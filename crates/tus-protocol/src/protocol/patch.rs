//! Core PATCH handler.
//!
//! Validates the request, acquires a lock on the upload, verifies the offset,
//! writes the request body to storage, and fires lifecycle hooks.

use http::StatusCode;

use crate::config::Extension;
use crate::error::Error;
use crate::hooks::{HookContext, HookEvent, HookExecutor, HookRequestInfo};
use crate::lifecycle::{
    ReceiveRequest, apply_receive_offset, ensure_active, ensure_committed_offset, prepare_receive,
    receive_body_size_limit, reconcile_state_offset, run_pre_finish, validate_receive_body,
};
use crate::locking::Locker;
use crate::state::StateStore;
use crate::storage::{ChunkStream, Storage};

use super::body::RequestBody;
use super::{Headers, Protocol, Response, UploadId};

/// Appends data to an upload.
#[allow(clippy::too_many_arguments)]
impl<'a, S, I, L, H> Protocol<'a, S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    /// Appends data to an upload.
    ///
    /// `body` determines how request bytes are collected. Collection happens
    /// only after PATCH preflight validation reaches the body collection point.
    ///
    /// # Errors
    ///
    /// Returns an error if the request content type or offset is invalid, the
    /// upload is missing, expired, final, or already complete, checksum
    /// validation fails, a hook rejects the write, or a lock, storage, or
    /// state-store operation fails.
    pub async fn patch(
        &self,
        headers: Headers,
        upload_id: &UploadId,
        body: RequestBody,
    ) -> Result<Response, Error> {
        headers.validate_patch_content_type()?;
        let upload_id = upload_id.as_str();

        let client_offset = headers
            .upload_offset
            .ok_or_else(|| Error::MissingHeader("Upload-Offset"))?;

        let _guard = self
            .locker
            .lock(upload_id, self.config.lock_timeout_duration())
            .await?;

        let mut state = self
            .state_store
            .get(upload_id)
            .await?
            .ok_or_else(|| Error::NotFound(upload_id.to_string()))?;

        ensure_active(&state)?;

        let request_info = make_hook_request_info(&headers, upload_id);

        reconcile_state_offset(
            self.storage,
            self.state_store,
            self.hooks,
            &request_info,
            &mut state,
        )
        .await?;

        prepare_receive(
            self.config,
            &mut state,
            ReceiveRequest {
                client_offset,
                upload_length: headers.upload_length,
            },
        )?;

        let pre_ctx = HookContext::new(HookEvent::PreReceive, state.clone(), request_info.clone());
        let pre_result = self.hooks.execute_pre(&pre_ctx).await?;

        if !pre_result.proceed {
            return Err(Error::HookRejected {
                status_code: pre_result.reject_status.unwrap_or(400),
                message: pre_result.reject_message.unwrap_or_default(),
            });
        }

        if let Some(modified_state) = pre_result.upload {
            state = modified_state;
        }

        let body = super::body::collect(
            self.config,
            &headers,
            receive_body_size_limit(self.config, &state),
            body,
        )
        .await?;
        let body_len = body.bytes.len() as u64;
        let receive_projection = validate_receive_body(self.config, &state, body_len)?;

        if receive_projection.completes_upload {
            let mut completed_state = state.clone();
            completed_state.set_offset(receive_projection.projected_offset);
            run_pre_finish(self.hooks, &request_info, completed_state).await?;
        }

        let new_offset = self
            .storage
            .append(&mut state, ChunkStream::Buffered(body.bytes))
            .await?;
        ensure_committed_offset(new_offset, receive_projection.projected_offset)?;
        apply_receive_offset(&mut state, new_offset);
        self.state_store.set(&state, false).await?;

        let post_receive_ctx =
            HookContext::new(HookEvent::PostReceive, state.clone(), request_info.clone());
        self.hooks.execute_post(&post_receive_ctx).await?;

        if state.is_complete() {
            let post_finish_ctx =
                HookContext::new(HookEvent::PostFinish, state.clone(), request_info);
            self.hooks.execute_post(&post_finish_ctx).await?;
        }

        let mut response = Response::new(StatusCode::NO_CONTENT)
            .with_header("upload-offset", new_offset.to_string());

        if self.config.has_extension(Extension::Expiration)
            && let Some(expires) = state.expires_header()
        {
            response = response.with_header("upload-expires", &expires);
        }

        for (name, value) in pre_result.response_headers {
            response = response.with_header_owned(name, value);
        }

        Ok(response)
    }
}

fn make_hook_request_info(headers: &Headers, upload_id: &str) -> HookRequestInfo {
    let mut hook_headers = std::collections::HashMap::new();
    if let Some(offset) = headers.upload_offset {
        hook_headers.insert("upload-offset".to_string(), offset.to_string());
    }
    if let Some(length) = headers.upload_length {
        hook_headers.insert("upload-length".to_string(), length.to_string());
    }
    if let Some(ct) = &headers.content_type {
        hook_headers.insert("content-type".to_string(), ct.clone());
    }
    if let Some(cl) = headers.content_length {
        hook_headers.insert("content-length".to_string(), cl.to_string());
    }
    HookRequestInfo {
        method: "PATCH".to_string(),
        path: format!("/files/{}", upload_id),
        remote_addr: None,
        headers: hook_headers,
    }
}

#[cfg(all(
    test,
    feature = "storage-memory",
    feature = "state-memory",
    not(feature = "local-futures")
))]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::hooks::{HookChain, NoopHookExecutor, PreHookResult};
    use crate::locking::NoopLocker;
    use crate::state::{UploadState, memory::MemoryStateStore};
    use crate::storage::ByteStream;
    use crate::storage::memory::MemoryStorage;
    use bytes::Bytes;
    use chrono::{Duration, Utc};

    struct WrongOffsetStorage {
        inner: MemoryStorage,
        returned_offset: u64,
    }

    impl WrongOffsetStorage {
        fn new(returned_offset: u64) -> Self {
            Self {
                inner: MemoryStorage::new(),
                returned_offset,
            }
        }
    }

    #[async_trait::async_trait]
    impl Storage for WrongOffsetStorage {
        fn name(&self) -> &'static str {
            "wrong-offset"
        }

        async fn create(&self, state: &mut UploadState) -> crate::error::Result<String> {
            self.inner.create(state).await
        }

        async fn append(
            &self,
            state: &mut UploadState,
            data: ChunkStream,
        ) -> crate::error::Result<u64> {
            self.inner.append(state, data).await?;
            Ok(self.returned_offset)
        }

        async fn get_stream(&self, state: &UploadState) -> crate::error::Result<ByteStream> {
            self.inner.get_stream(state).await
        }

        async fn concat(
            &self,
            target: &mut UploadState,
            parts: Vec<UploadState>,
        ) -> crate::error::Result<()> {
            self.inner.concat(target, parts).await
        }

        async fn delete(&self, state: &UploadState) -> crate::error::Result<()> {
            self.inner.delete(state).await
        }

        async fn size(&self, state: &UploadState) -> crate::error::Result<Option<u64>> {
            self.inner.size(state).await
        }
    }

    fn headers(offset: u64) -> Headers {
        Headers {
            upload_offset: Some(offset),
            content_type: Some("application/offset+octet-stream".to_string()),
            ..Default::default()
        }
    }

    fn body(data: &[u8]) -> ChunkStream {
        ChunkStream::from_bytes(Bytes::copy_from_slice(data))
    }

    async fn setup(state: UploadState) -> (MemoryStorage, MemoryStateStore) {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut st = state;
        storage.create(&mut st).await.unwrap();
        store.set(&st, true).await.unwrap();
        (storage, store)
    }

    async fn call(
        config: &Config,
        storage: &MemoryStorage,
        store: &MemoryStateStore,
        h: Headers,
        upload_id: &str,
        data: &[u8],
    ) -> Result<Response, Error> {
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = upload_id.parse().unwrap();
        Protocol::new(config, storage, store, &locker, &hooks)
            .patch(h, &upload_id, RequestBody::from_chunk_stream(body(data)))
            .await
    }

    #[tokio::test]
    async fn basic_write() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(100)).await;
        let response = call(
            &Config::default(),
            &storage,
            &store,
            headers(0),
            "test-id",
            b"Hello World",
        )
        .await
        .unwrap();
        assert_eq!(response.status, StatusCode::NO_CONTENT);
        assert_eq!(response.headers.get("upload-offset").unwrap(), "11");

        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 11);
    }

    #[tokio::test]
    async fn patch_rejects_storage_offset_that_does_not_match_projected_offset() {
        let storage = WrongOffsetStorage::new(0);
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(10);
        storage.create(&mut state).await.unwrap();
        store.set(&state, true).await.unwrap();
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = "test-id".parse().unwrap();

        let err = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .patch(
                headers(0),
                &upload_id,
                RequestBody::from_chunk_stream(body(b"hello")),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, Error::Internal(message) if message.contains("storage returned offset 0") && message.contains("projected offset 5"))
        );
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 0);
    }

    #[tokio::test]
    async fn recovers_offset_before_accepting_patch() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(10);
        storage.create(&mut state).await.unwrap();
        storage.append(&mut state, body(b"hello")).await.unwrap();
        state.set_offset(0);
        store.set(&state, true).await.unwrap();

        let response = call(
            &Config::default(),
            &storage,
            &store,
            headers(5),
            "test-id",
            b"world",
        )
        .await
        .unwrap();

        assert_eq!(response.headers.get("upload-offset").unwrap(), "10");
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 10);
    }

    #[tokio::test]
    async fn not_found() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let err = call(
            &Config::default(),
            &storage,
            &store,
            headers(0),
            "missing",
            b"data",
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn offset_mismatch() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(100);
        storage.create(&mut state).await.unwrap();
        storage.append(&mut state, body(&[b'x'; 50])).await.unwrap();
        store.set(&state, true).await.unwrap();
        let err = call(
            &Config::default(),
            &storage,
            &store,
            headers(0),
            "test-id",
            b"data",
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::OffsetMismatch { .. }));
    }

    #[tokio::test]
    async fn missing_offset_header() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(100)).await;
        let h = Headers {
            content_type: Some("application/offset+octet-stream".to_string()),
            ..Default::default()
        };
        let err = call(&Config::default(), &storage, &store, h, "test-id", b"data")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::MissingHeader(_)));
    }

    #[tokio::test]
    async fn invalid_content_type() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(100)).await;
        let h = Headers {
            upload_offset: Some(0),
            content_type: Some("text/plain".to_string()),
            ..Default::default()
        };
        let err = call(&Config::default(), &storage, &store, h, "test-id", b"data")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidContentType { .. }));
    }

    #[tokio::test]
    async fn expired() {
        let state = UploadState::new("test-id")
            .with_length(100)
            .with_expiration(Utc::now() - Duration::hours(1));
        let (storage, store) = setup(state).await;
        let config = Config::default().with_extension(Extension::Expiration);
        let err = call(&config, &storage, &store, headers(0), "test-id", b"data")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Expired(_)));
    }

    #[tokio::test]
    async fn expired_upload_is_rejected_before_reconciliation() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id")
            .with_length(10)
            .with_expiration(Utc::now() - Duration::hours(1));
        storage.create(&mut state).await.unwrap();
        storage.append(&mut state, body(b"hello")).await.unwrap();
        state.set_offset(0);
        store.set(&state, true).await.unwrap();

        let config = Config::default().with_extension(Extension::Expiration);
        let err = call(&config, &storage, &store, headers(0), "test-id", b"world")
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Expired(_)));
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 0);
    }

    #[tokio::test]
    async fn final_upload_forbidden() {
        let mut state = UploadState::new("test-id").with_length(100);
        state.mark_final(Vec::new());
        let (storage, store) = setup(state).await;
        let config = Config::default().with_extension(Extension::Concatenation);
        let err = call(&config, &storage, &store, headers(0), "test-id", b"data")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::FinalUploadModificationForbidden(_)));
    }

    #[tokio::test]
    async fn size_exceeded() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(10)).await;
        let h = Headers {
            upload_offset: Some(0),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(20),
            ..Default::default()
        };
        let err = call(
            &Config::default(),
            &storage,
            &store,
            h,
            "test-id",
            b"12345678901234567890",
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::SizeExceeded { .. }));
    }

    #[tokio::test]
    async fn actual_body_cannot_exceed_upload_length_when_content_length_missing() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(5)).await;
        let h = Headers {
            upload_offset: Some(0),
            content_type: Some("application/offset+octet-stream".to_string()),
            ..Default::default()
        };

        let err = call(
            &Config::default(),
            &storage,
            &store,
            h,
            "test-id",
            b"123456",
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::SizeExceeded { .. }));
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 0);
    }

    #[tokio::test]
    async fn actual_body_cannot_exceed_max_size_when_content_length_missing() {
        let (storage, store) = setup(UploadState::new("test-id")).await;
        let config = Config::default().max_size(5);
        let h = Headers {
            upload_offset: Some(0),
            content_type: Some("application/offset+octet-stream".to_string()),
            ..Default::default()
        };

        let err = call(&config, &storage, &store, h, "test-id", b"123456")
            .await
            .unwrap_err();

        assert!(matches!(err, Error::SizeExceeded { .. }));
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 0);
    }

    #[tokio::test]
    async fn actual_body_cannot_exceed_max_chunk_size_when_content_length_missing() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(100)).await;
        let config = Config::default().max_chunk_size(5);
        let h = Headers {
            upload_offset: Some(0),
            content_type: Some("application/offset+octet-stream".to_string()),
            ..Default::default()
        };

        let err = call(&config, &storage, &store, h, "test-id", b"123456")
            .await
            .unwrap_err();

        assert!(matches!(err, Error::SizeExceeded { .. }));
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 0);
    }

    #[tokio::test]
    async fn deferred_length_set() {
        // Upload created with no length (deferred); client provides it on PATCH.
        let (storage, store) = setup(UploadState::new("test-id")).await;
        let h = Headers {
            upload_offset: Some(0),
            upload_length: Some(100),
            content_type: Some("application/offset+octet-stream".to_string()),
            ..Default::default()
        };
        let response = call(&Config::default(), &storage, &store, h, "test-id", b"Hello")
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::NO_CONTENT);
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.length(), Some(100));
    }

    #[tokio::test]
    async fn cannot_change_length() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(100)).await;
        let h = Headers {
            upload_offset: Some(0),
            upload_length: Some(200),
            content_type: Some("application/offset+octet-stream".to_string()),
            ..Default::default()
        };
        let err = call(&Config::default(), &storage, &store, h, "test-id", b"Hello")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidHeader { .. }));
    }

    #[tokio::test]
    async fn completes_upload() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(5)).await;
        let response = call(
            &Config::default(),
            &storage,
            &store,
            headers(0),
            "test-id",
            b"Hello",
        )
        .await
        .unwrap();
        assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert!(stored.is_complete());
    }

    #[tokio::test]
    async fn pre_finish_rejection_blocks_completing_patch() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(5)).await;
        let locker = NoopLocker::new();
        let hooks = HookChain::new()
            .on_pre_finish(|_| async { Ok(PreHookResult::reject(403, "finish blocked")) });
        let upload_id: UploadId = "test-id".parse().unwrap();

        let err = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .patch(
                headers(0),
                &upload_id,
                RequestBody::from_chunk_stream(body(b"Hello")),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::HookRejected {
                status_code: 403,
                ..
            }
        ));
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 0);
        assert!(!stored.is_complete());
    }

    #[tokio::test]
    async fn stream_body_is_not_polled_before_offset_validation() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let (storage, store) = setup(UploadState::new("test-id").with_length(10)).await;
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = "test-id".parse().unwrap();
        let polled = Arc::new(AtomicBool::new(false));
        let polled_for_stream = Arc::clone(&polled);
        let stream: crate::BodyStream = Box::pin(futures::stream::once(async move {
            polled_for_stream.store(true, Ordering::SeqCst);
            Ok(crate::BodyFrame::Data(Bytes::from_static(b"test")))
        }));

        let err = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .patch(
                headers(5),
                &upload_id,
                crate::RequestBody::from_stream(stream),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::OffsetMismatch {
                expected: 0,
                actual: 5
            }
        ));
        assert!(!polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn completed_upload_forbidden() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(5)).await;
        call(
            &Config::default(),
            &storage,
            &store,
            headers(0),
            "test-id",
            b"Hello",
        )
        .await
        .unwrap();

        let err = call(
            &Config::default(),
            &storage,
            &store,
            headers(5),
            "test-id",
            b"",
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            Error::CompletedUploadModificationForbidden(_)
        ));
    }

    #[tokio::test]
    async fn max_chunk_size_enforced() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(1000)).await;
        let config = Config::default().max_chunk_size(5);
        let h = Headers {
            upload_offset: Some(0),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(10),
            ..Default::default()
        };
        let err = call(&config, &storage, &store, h, "test-id", b"1234567890")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::SizeExceeded { .. }));
    }

    #[tokio::test]
    async fn max_size_enforced_on_deferred_length() {
        // Upload created without length; PATCH has content-length exceeding
        // the server-wide max. Must reject even though Upload-Length isn't
        // set on the state.
        let (storage, store) = setup(UploadState::new("test-id")).await;
        let config = Config::default().max_size(10);
        let h = Headers {
            upload_offset: Some(0),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(20),
            ..Default::default()
        };
        let err = call(
            &config,
            &storage,
            &store,
            h,
            "test-id",
            b"12345678901234567890",
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::SizeExceeded { .. }));
    }

    #[tokio::test]
    async fn max_size_accumulates_across_patches() {
        // Two PATCHes, second exceeds max_size in aggregate.
        let (storage, store) = setup(UploadState::new("test-id")).await;
        let config = Config::default().max_size(8);
        let h = |offset: u64, cl: u64| Headers {
            upload_offset: Some(offset),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(cl),
            ..Default::default()
        };
        call(&config, &storage, &store, h(0, 5), "test-id", b"12345")
            .await
            .unwrap();
        let err = call(&config, &storage, &store, h(5, 5), "test-id", b"67890")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::SizeExceeded { .. }));
    }

    #[tokio::test]
    async fn multiple_chunks() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(20)).await;

        let response = call(
            &Config::default(),
            &storage,
            &store,
            headers(0),
            "test-id",
            b"Hello",
        )
        .await
        .unwrap();
        assert_eq!(response.headers.get("upload-offset").unwrap(), "5");

        let response = call(
            &Config::default(),
            &storage,
            &store,
            headers(5),
            "test-id",
            b" World",
        )
        .await
        .unwrap();
        assert_eq!(response.headers.get("upload-offset").unwrap(), "11");

        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 11);
    }
}
