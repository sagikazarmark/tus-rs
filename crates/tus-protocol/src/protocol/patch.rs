//! Core PATCH handler.
//!
//! Validates the request, acquires a lock on the upload, verifies the offset,
//! writes the request body to storage, and fires lifecycle hooks.

use http::StatusCode;

use crate::config::Extension;
use crate::error::Error;
use crate::hooks::HookExecutor;
use crate::lifecycle::{ByteReceiver, ReceiveRequest, prepare_upload_mutation_access};
use crate::locking::Locker;
use crate::state::StateStore;
use crate::storage::Storage;

use super::body::RequestBody;
use super::hook_context::{HookContextBuilder, HookRequestFacts};
use super::{Headers, Protocol, Response, UploadId};

/// Appends data to an upload.
#[allow(clippy::too_many_arguments)]
impl<'a, S, St, L, H> Protocol<'a, S, St, L, H>
where
    S: Storage + ?Sized,
    St: StateStore + ?Sized,
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
        let hook_contexts =
            HookContextBuilder::new(self.config, HookRequestFacts::patch(&headers, upload_id));
        let upload_id = upload_id.as_str();

        let client_offset = headers
            .upload_offset
            .ok_or_else(|| Error::MissingHeader("Upload-Offset"))?;

        // Cheap unlocked existence pre-check (DoS guard, not authoritative):
        // requests for unknown IDs must not exercise the locker, which may
        // allocate per-ID resources. The authoritative state load below still
        // happens under the lock.
        if self.state_store.get(upload_id).await?.is_none() {
            return Err(Error::NotFound(upload_id.to_string()));
        }

        let _guard = self
            .locker
            .lock(upload_id, self.config.lock_timeout())
            .await?;

        let mut state = self
            .state_store
            .get(upload_id)
            .await?
            .ok_or_else(|| Error::NotFound(upload_id.to_string()))?;

        prepare_upload_mutation_access(
            self.storage,
            self.state_store,
            self.hooks,
            hook_contexts.request_info(),
            &mut state,
        )
        .await?;

        let receiver = ByteReceiver::new(
            self.storage,
            self.state_store,
            self.hooks,
            self.config,
            hook_contexts.request_info(),
        );
        let received = receiver
            .receive_patch(
                &headers,
                &mut state,
                ReceiveRequest {
                    client_offset,
                    upload_length: headers.upload_length,
                },
                body,
            )
            .await?;
        let response_headers = received.response_headers;

        let mut response = Response::new(StatusCode::NO_CONTENT)
            .with_header("upload-offset", state.offset().to_string());

        if self.config.has_extension(Extension::Expiration)
            && let Some(expires) = state.expires_header()
        {
            response = response.with_header("upload-expires", &expires);
        }

        for (name, value) in response_headers {
            response = response.with_header(name, value);
        }

        Ok(response)
    }
}

#[cfg(all(
    test,
    feature = "storage-memory",
    feature = "state-memory",
    not(target_arch = "wasm32")
))]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::hooks::{HookChain, HookEvent, NoopHookExecutor, PreHookResult};
    use crate::locking::NoopLocker;
    use crate::state::{UploadMetadata, UploadState, WriteMode, memory::MemoryStateStore};
    use crate::storage::{AppendRequest, ChunkStream, memory::MemoryStorage};
    use bytes::Bytes;
    use chrono::{Duration, Utc};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct RecordingStorage {
        inner: MemoryStorage,
        append_received_stream: AtomicBool,
    }

    impl RecordingStorage {
        fn new() -> Self {
            Self {
                inner: MemoryStorage::new(),
                append_received_stream: AtomicBool::new(false),
            }
        }

        fn append_received_stream(&self) -> bool {
            self.append_received_stream.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Storage for RecordingStorage {
        fn name(&self) -> &'static str {
            "recording-memory"
        }

        async fn create(&self, upload_id: &str) -> crate::Result<crate::StorageHandle> {
            self.inner.create(upload_id).await
        }

        async fn append(&self, request: AppendRequest) -> crate::Result<crate::StorageHandle> {
            if matches!(&request.data, ChunkStream::Stream(_)) {
                self.append_received_stream.store(true, Ordering::SeqCst);
            }

            self.inner.append(request).await
        }

        async fn concat(
            &self,
            request: crate::ConcatRequest,
        ) -> crate::Result<crate::StorageHandle> {
            self.inner.concat(request).await
        }

        async fn delete(&self, handle: &crate::StorageHandle) -> crate::Result<()> {
            self.inner.delete(handle).await
        }

        async fn size(&self, handle: &crate::StorageHandle) -> crate::Result<Option<u64>> {
            self.inner.size(handle).await
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
        create_storage(&storage, &mut st).await;
        store.set(&st, WriteMode::CreateNew).await.unwrap();
        (storage, store)
    }

    async fn create_storage(storage: &MemoryStorage, state: &mut UploadState) {
        let handle = storage.create(state.id()).await.unwrap();
        state.set_storage_handle(handle);
    }

    async fn append_storage(storage: &MemoryStorage, state: &mut UploadState, data: &[u8]) {
        let projected_offset = state.offset().saturating_add(data.len() as u64);
        let completes_upload = state
            .length()
            .is_some_and(|length| projected_offset == length);
        let handle = storage
            .append(AppendRequest::new(
                state.require_storage_handle().unwrap(),
                state.offset(),
                body(data),
                completes_upload,
            ))
            .await
            .unwrap();
        state.set_storage_handle(handle);
        state.set_offset(projected_offset);
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

    /// Locker that fails every call; used to prove handlers do not exercise
    /// the locker for unknown upload IDs.
    struct RejectingLocker;

    #[async_trait::async_trait]
    impl crate::locking::Locker for RejectingLocker {
        fn name(&self) -> &'static str {
            "rejecting"
        }

        async fn lock(
            &self,
            _upload_id: &str,
            _timeout: std::time::Duration,
        ) -> Result<crate::locking::LockGuard, Error> {
            Err(Error::Internal("locker must not be exercised".to_string()))
        }

        async fn try_lock(
            &self,
            _upload_id: &str,
        ) -> Result<Option<crate::locking::LockGuard>, Error> {
            Err(Error::Internal("locker must not be exercised".to_string()))
        }
    }

    #[tokio::test]
    async fn patch_of_unknown_upload_does_not_touch_locker() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = "missing".parse().unwrap();

        let err = Protocol::new(
            &Config::default(),
            &storage,
            &store,
            &RejectingLocker,
            &hooks,
        )
        .patch(
            headers(0),
            &upload_id,
            RequestBody::from_chunk_stream(body(b"data")),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::NotFound(_)));
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
    async fn streamed_patch_reaches_storage_append_as_stream() {
        let storage = RecordingStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(10);
        let handle = storage.create(state.id()).await.unwrap();
        state.set_storage_handle(handle);
        store.set(&state, WriteMode::CreateNew).await.unwrap();
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = "test-id".parse().unwrap();
        let stream: crate::BodyStream = Box::pin(futures::stream::iter([
            Ok(crate::BodyFrame::Data(Bytes::from_static(b"he"))),
            Ok(crate::BodyFrame::Data(Bytes::from_static(b"llo"))),
        ]));
        let h = Headers {
            upload_offset: Some(0),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };

        let response = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .patch(h, &upload_id, RequestBody::from_stream(stream))
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::NO_CONTENT);
        assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
        assert!(storage.append_received_stream());
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 5);
        assert!(!stored.is_complete());
    }

    #[tokio::test]
    async fn streamed_patch_failure_does_not_accept_partial_bytes() {
        use std::io;

        let (storage, store) = setup(UploadState::new("test-id").with_length(10)).await;
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = "test-id".parse().unwrap();
        let stream: crate::BodyStream = Box::pin(futures::stream::iter(vec![
            Ok(crate::BodyFrame::Data(Bytes::from_static(b"part"))),
            Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "client gone",
            )),
        ]));
        let h = Headers {
            upload_offset: Some(0),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(8),
            ..Default::default()
        };

        let err = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .patch(h, &upload_id, RequestBody::from_stream(stream))
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Internal(_)));
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 0);
        assert_eq!(
            storage
                .size(&stored.require_storage_handle().unwrap())
                .await
                .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn streamed_patch_content_length_mismatch_does_not_accept_bytes() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(10)).await;
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = "test-id".parse().unwrap();
        let stream: crate::BodyStream = Box::pin(futures::stream::iter([Ok(
            crate::BodyFrame::Data(Bytes::from_static(b"part")),
        )]));
        let h = Headers {
            upload_offset: Some(0),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };

        let err = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .patch(h, &upload_id, RequestBody::from_stream(stream))
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::InvalidHeader {
                header: "Content-Length",
                ..
            }
        ));
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 0);
        assert_eq!(
            storage
                .size(&stored.require_storage_handle().unwrap())
                .await
                .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn streamed_completing_patch_content_length_mismatch_does_not_run_pre_finish() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(5)).await;
        let locker = NoopLocker::new();
        let pre_finish_calls = Arc::new(AtomicUsize::new(0));
        let hooks = HookChain::new().on_pre_finish({
            let pre_finish_calls = Arc::clone(&pre_finish_calls);
            move |_| {
                let pre_finish_calls = Arc::clone(&pre_finish_calls);
                async move {
                    pre_finish_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(PreHookResult::proceed())
                }
            }
        });
        let upload_id: UploadId = "test-id".parse().unwrap();
        let stream: crate::BodyStream = Box::pin(futures::stream::iter([Ok(
            crate::BodyFrame::Data(Bytes::from_static(b"part")),
        )]));
        let h = Headers {
            upload_offset: Some(0),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };

        let err = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .patch(h, &upload_id, RequestBody::from_stream(stream))
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::InvalidHeader {
                header: "Content-Length",
                ..
            }
        ));
        assert_eq!(pre_finish_calls.load(Ordering::SeqCst), 0);
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 0);
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn streamed_patch_checksum_trailer_mismatch_does_not_accept_bytes() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(10)).await;
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = "test-id".parse().unwrap();
        let mut trailers = http::HeaderMap::new();
        trailers.insert(
            "upload-checksum",
            "sha1 AAAAAAAAAAAAAAAAAAAAAAAAAAA=".parse().unwrap(),
        );
        let stream: crate::BodyStream = Box::pin(futures::stream::iter([
            Ok(crate::BodyFrame::Data(Bytes::from_static(b"hello"))),
            Ok(crate::BodyFrame::Trailers(trailers)),
        ]));
        let h = Headers {
            upload_offset: Some(0),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };
        let config = Config::default().with_extension(Extension::ChecksumTrailer);

        let err = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .patch(h, &upload_id, RequestBody::from_stream(stream))
            .await
            .unwrap_err();

        assert!(matches!(err, Error::ChecksumMismatch { .. }));
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 0);
        assert_eq!(
            storage
                .size(&stored.require_storage_handle().unwrap())
                .await
                .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn recovers_offset_before_accepting_patch() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(10);
        create_storage(&storage, &mut state).await;
        append_storage(&storage, &mut state, b"hello").await;
        state.set_offset(0);
        store.set(&state, WriteMode::CreateNew).await.unwrap();

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
        create_storage(&storage, &mut state).await;
        append_storage(&storage, &mut state, &[b'x'; 50]).await;
        store.set(&state, WriteMode::CreateNew).await.unwrap();
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
        create_storage(&storage, &mut state).await;
        append_storage(&storage, &mut state, b"hello").await;
        state.set_offset(0);
        store.set(&state, WriteMode::CreateNew).await.unwrap();

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
    async fn final_upload_patch_rejects_without_materializing_or_running_finish_hooks() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();

        let mut part = UploadState::new("part-1").with_length(4).with_partial();
        create_storage(&storage, &mut part).await;
        append_storage(&storage, &mut part, b"ABCD").await;
        store.set(&part, WriteMode::CreateNew).await.unwrap();

        let mut final_upload = UploadState::new("final-1").with_length(4);
        create_storage(&storage, &mut final_upload).await;
        final_upload.mark_final(vec!["part-1".to_string()]);
        store
            .set(&final_upload, WriteMode::CreateNew)
            .await
            .unwrap();

        let events = Arc::new(Mutex::new(Vec::new()));
        let hooks = HookChain::new()
            .on_pre_finish({
                let events = Arc::clone(&events);
                move |ctx| {
                    let events = Arc::clone(&events);
                    let method = ctx.request().method.clone();
                    let path = ctx.request().path.clone();
                    let offset = ctx.upload().offset();
                    async move {
                        events
                            .lock()
                            .unwrap()
                            .push((HookEvent::PreFinish, method, path, offset));
                        Ok(PreHookResult::proceed())
                    }
                }
            })
            .on_post_finish({
                let events = Arc::clone(&events);
                move |ctx| {
                    let events = Arc::clone(&events);
                    let method = ctx.request().method.clone();
                    let path = ctx.request().path.clone();
                    let offset = ctx.upload().offset();
                    async move {
                        events
                            .lock()
                            .unwrap()
                            .push((HookEvent::PostFinish, method, path, offset));
                        Ok(())
                    }
                }
            });
        let locker = NoopLocker::new();
        let upload_id: UploadId = "final-1".parse().unwrap();

        let err = Protocol::new(
            &Config::default().with_extension(Extension::Concatenation),
            &storage,
            &store,
            &locker,
            &hooks,
        )
        .patch(
            headers(0),
            &upload_id,
            RequestBody::from_chunk_stream(body(b"ignored")),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::FinalUploadModificationForbidden(_)));
        assert!(events.lock().unwrap().is_empty());

        let stored = store.get("final-1").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 0);
        assert_eq!(
            storage
                .size(&stored.require_storage_handle().unwrap())
                .await
                .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn final_upload_patch_rejection_precedes_pre_finish_hook_rejection() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();

        let mut part = UploadState::new("part-1").with_length(4).with_partial();
        create_storage(&storage, &mut part).await;
        append_storage(&storage, &mut part, b"ABCD").await;
        store.set(&part, WriteMode::CreateNew).await.unwrap();

        let mut final_upload = UploadState::new("final-1").with_length(4);
        create_storage(&storage, &mut final_upload).await;
        final_upload.mark_final(vec!["part-1".to_string()]);
        store
            .set(&final_upload, WriteMode::CreateNew)
            .await
            .unwrap();

        let events = Arc::new(Mutex::new(Vec::new()));
        let hooks = HookChain::new().on_pre_finish({
            let events = Arc::clone(&events);
            move |ctx| {
                let events = Arc::clone(&events);
                let method = ctx.request().method.clone();
                async move {
                    events.lock().unwrap().push(method);
                    Ok(PreHookResult::reject(403, "finish blocked"))
                }
            }
        });
        let locker = NoopLocker::new();
        let upload_id: UploadId = "final-1".parse().unwrap();

        let err = Protocol::new(
            &Config::default().with_extension(Extension::Concatenation),
            &storage,
            &store,
            &locker,
            &hooks,
        )
        .patch(
            headers(0),
            &upload_id,
            RequestBody::from_chunk_stream(body(b"ignored")),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::FinalUploadModificationForbidden(_)));
        assert!(events.lock().unwrap().is_empty());
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
        let config = Config::default().with_max_size(5);
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
        let config = Config::default().with_max_chunk_size(5);
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
    async fn pre_receive_can_replace_user_metadata() {
        let (storage, store) = setup(UploadState::new("test-id").with_length(10)).await;
        let locker = NoopLocker::new();
        let hooks = HookChain::new().on_pre_receive(|_| async {
            let mut metadata = UploadMetadata::new();
            metadata.insert("stage", "received");
            Ok(PreHookResult::proceed_with_metadata(metadata))
        });
        let upload_id: UploadId = "test-id".parse().unwrap();

        let response = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .patch(
                headers(0),
                &upload_id,
                RequestBody::from_chunk_stream(body(b"Hello")),
            )
            .await
            .unwrap();

        assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(
            stored.metadata().get("stage").and_then(|v| v.as_str()),
            Some("received")
        );
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn patch_accepts_checksum_trailer() {
        use crate::{BodyFrame, BodyStream};
        use base64::Engine;

        let (storage, store) = setup(UploadState::new("test-id").with_length(5)).await;
        let config = Config::default().with_extension(Extension::ChecksumTrailer);
        let checksum = base64::engine::general_purpose::STANDARD.encode(crate::calculate_checksum(
            crate::config::ChecksumAlgorithm::Sha1,
            b"hello",
        ));
        let mut trailers = http::HeaderMap::new();
        trailers.insert(
            "upload-checksum",
            format!("sha1 {checksum}").parse().unwrap(),
        );
        let stream: BodyStream = Box::pin(futures::stream::iter([
            Ok(BodyFrame::Data(Bytes::from_static(b"hello"))),
            Ok(BodyFrame::Trailers(trailers)),
        ]));
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = "test-id".parse().unwrap();

        let response = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .patch(headers(0), &upload_id, RequestBody::from_stream(stream))
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::NO_CONTENT);
        assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
    }

    #[tokio::test]
    async fn pre_finish_rejection_fails_completing_patch_response() {
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
        // The completing bytes were already durable when the gate ran: the
        // response fails, but the stored upload remains complete.
        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 5);
        assert!(stored.is_complete());
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
        let config = Config::default().with_max_chunk_size(5);
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
        let config = Config::default().with_max_size(10);
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
        let config = Config::default().with_max_size(8);
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

    #[tokio::test]
    async fn patch_recovers_storage_completed_upload_and_runs_finish_hooks() {
        // Storage reached the declared length but the completing state write
        // never landed. A PATCH preflight recovers the completion, runs the
        // finish gate, then rejects the write as targeting an already-complete
        // upload, so downstream processing wired to PostFinish still fires.
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(5);
        create_storage(&storage, &mut state).await;
        append_storage(&storage, &mut state, b"hello").await;
        state.set_offset(0);
        store.set(&state, WriteMode::CreateNew).await.unwrap();

        let events = Arc::new(Mutex::new(Vec::new()));
        let hooks = HookChain::new()
            .on_pre_finish({
                let events = Arc::clone(&events);
                move |_| {
                    let events = Arc::clone(&events);
                    async move {
                        events.lock().unwrap().push(HookEvent::PreFinish);
                        Ok(PreHookResult::proceed())
                    }
                }
            })
            .on_post_finish({
                let events = Arc::clone(&events);
                move |_| {
                    let events = Arc::clone(&events);
                    async move {
                        events.lock().unwrap().push(HookEvent::PostFinish);
                        Ok(())
                    }
                }
            });
        let locker = NoopLocker::new();
        let upload_id: UploadId = "test-id".parse().unwrap();

        let err = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .patch(
                headers(0),
                &upload_id,
                RequestBody::from_chunk_stream(body(b"hello")),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::CompletedUploadModificationForbidden(id) if id == "test-id"
        ));
        assert_eq!(
            *events.lock().unwrap(),
            vec![HookEvent::PreFinish, HookEvent::PostFinish]
        );

        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 5);
        assert!(stored.is_complete());
    }
}
