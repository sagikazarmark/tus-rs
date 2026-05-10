//! Core PATCH handler.
//!
//! Validates the request, acquires a lock on the upload, verifies the offset,
//! writes the request body to storage, and fires lifecycle hooks.

use std::future::Future;
use std::pin::Pin;

use futures::StreamExt;
use http::StatusCode;

use crate::config::{ChecksumAlgorithm, Config, Extension};
use crate::error::Error;
use crate::hooks::{HookContext, HookEvent, HookExecutor, HookRequestInfo};
use crate::locking::Locker;
use crate::state::StateStore;
use crate::storage::{ChunkStream, Storage};

use super::recovery::reconcile_state_offset;
use super::{Headers, Protocol, Response, UploadId};

/// Optional checksum value to verify after collecting a PATCH body.
pub type PatchChecksum = Option<(ChecksumAlgorithm, Vec<u8>)>;

/// Collected PATCH body data and the checksum the protocol should verify.
pub struct PatchBodyData {
    /// Collected body bytes.
    pub bytes: bytes::Bytes,
    /// Effective checksum from headers or trailers.
    pub checksum: PatchChecksum,
}

#[cfg(not(feature = "local-futures"))]
/// Future returned by a PATCH body collector.
pub type PatchBodyCollectorFuture =
    Pin<Box<dyn Future<Output = Result<PatchBodyData, Error>> + Send>>;

#[cfg(feature = "local-futures")]
/// Future returned by a PATCH body collector.
pub type PatchBodyCollectorFuture = Pin<Box<dyn Future<Output = Result<PatchBodyData, Error>>>>;

/// Collector for PATCH bodies that need protocol-computed size limits.
///
/// The collector is called after PATCH preflight validation and receives the
/// effective body size limit. Collectors should enforce that limit while
/// reading from their underlying transport so oversized bodies are rejected
/// before they are fully buffered.
pub trait PatchBodyCollector: crate::runtime::MaybeSend + 'static {
    /// Collects the body using the computed body size limit.
    fn collect(self, body_limit: Option<u64>) -> PatchBodyCollectorFuture;
}

impl<F, Fut> PatchBodyCollector for F
where
    F: FnOnce(Option<u64>) -> Fut + crate::runtime::MaybeSend + 'static,
    Fut: Future<Output = Result<PatchBodyData, Error>> + crate::runtime::MaybeSend + 'static,
{
    fn collect(self, body_limit: Option<u64>) -> PatchBodyCollectorFuture {
        Box::pin(self(body_limit))
    }
}

#[cfg(not(feature = "local-futures"))]
type CollectorFn = Box<dyn FnOnce(Option<u64>) -> PatchBodyCollectorFuture + Send>;

#[cfg(feature = "local-futures")]
type CollectorFn = Box<dyn FnOnce(Option<u64>) -> PatchBodyCollectorFuture>;

struct PatchBodyCollectorBox {
    collect: CollectorFn,
}

impl PatchBodyCollectorBox {
    fn new<C>(collector: C) -> Self
    where
        C: PatchBodyCollector,
    {
        Self {
            collect: Box::new(move |body_limit| collector.collect(body_limit)),
        }
    }

    async fn collect(self, body_limit: Option<u64>) -> Result<PatchBodyData, Error> {
        (self.collect)(body_limit).await
    }
}

/// PATCH request body input.
pub struct PatchBody {
    kind: PatchBodyKind,
}

enum PatchBodyKind {
    Stream {
        body: ChunkStream,
        checksum: PatchChecksum,
    },
    Collector(PatchBodyCollectorBox),
}

impl PatchBody {
    /// Creates a regular PATCH body from a stream and optional checksum.
    #[must_use]
    pub fn stream(body: ChunkStream, checksum: PatchChecksum) -> Self {
        Self {
            kind: PatchBodyKind::Stream { body, checksum },
        }
    }

    /// Creates a PATCH body from an adapter-provided collector.
    #[must_use]
    pub fn collector<C>(collector: C) -> Self
    where
        C: PatchBodyCollector,
    {
        Self {
            kind: PatchBodyKind::Collector(PatchBodyCollectorBox::new(collector)),
        }
    }

    async fn collect(self, body_limit: Option<u64>) -> Result<PatchBodyData, Error> {
        match self.kind {
            PatchBodyKind::Stream { body, checksum } => {
                let bytes = collect_chunk_stream(body, body_limit).await?;
                Ok(PatchBodyData { bytes, checksum })
            }
            PatchBodyKind::Collector(collector) => collector.collect(body_limit).await,
        }
    }
}

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
        body: PatchBody,
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

        if state.is_expired() {
            return Err(Error::Expired(upload_id.to_string()));
        }

        let request_info = make_hook_request_info(&headers, upload_id);

        reconcile_state_offset(
            self.storage,
            self.state_store,
            self.hooks,
            &request_info,
            &mut state,
        )
        .await?;

        if state.is_final() {
            return Err(Error::FinalUploadModificationForbidden(
                upload_id.to_string(),
            ));
        }

        if client_offset != state.offset() {
            return Err(Error::OffsetMismatch {
                expected: state.offset(),
                actual: client_offset,
            });
        }

        if state.is_complete() {
            return Err(Error::CompletedUploadModificationForbidden(
                upload_id.to_string(),
            ));
        }

        if let Some(length) = headers.upload_length {
            if let Some(existing) = state.length() {
                if length != existing {
                    return Err(Error::InvalidHeader {
                        header: "Upload-Length",
                        message: format!(
                            "cannot change Upload-Length after it is set (existing: {}, provided: {})",
                            existing, length
                        ),
                    });
                }
            } else {
                if let Some(max_size) = self.config.max_size_limit()
                    && length > max_size
                {
                    return Err(Error::SizeExceeded {
                        size: length,
                        max: max_size,
                    });
                }
                state.set_length(length);
            }
        }

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

        // Reject unsupported header checksum algorithms before body collection.
        #[cfg(feature = "checksum")]
        if let Some((algorithm, _)) = &headers.upload_checksum
            && self.config.has_extension(Extension::Checksum)
            && !self.config.supports_checksum_algorithm(*algorithm)
        {
            return Err(Error::UnsupportedChecksum(algorithm.as_str().to_string()));
        }

        let body = body.collect(body_size_limit(self.config, &state)).await?;
        let body_len = body.bytes.len() as u64;
        validate_content_length(&headers, body_len)?;
        validate_body_size(self.config, &state, body_len)?;

        // Validate checksum algorithm is advertised.
        #[cfg(feature = "checksum")]
        if let Some((algorithm, _)) = &body.checksum
            && self.config.has_extension(Extension::Checksum)
            && !self.config.supports_checksum_algorithm(*algorithm)
        {
            return Err(Error::UnsupportedChecksum(algorithm.as_str().to_string()));
        }

        #[cfg(feature = "checksum")]
        if let Some((algorithm, expected)) = body.checksum {
            let calculated = crate::checksum::calculate(algorithm, &body.bytes);
            if calculated != expected {
                use base64::Engine;
                return Err(Error::ChecksumMismatch {
                    expected: base64::engine::general_purpose::STANDARD.encode(&expected),
                    actual: base64::engine::general_purpose::STANDARD.encode(&calculated),
                });
            }
        }
        #[cfg(not(feature = "checksum"))]
        let _ = body.checksum;

        let projected_offset = state.offset().saturating_add(body_len);
        if state
            .length()
            .is_some_and(|length| projected_offset == length)
        {
            let mut completed_state = state.clone();
            completed_state.set_offset(projected_offset);
            let pre_finish_ctx =
                HookContext::new(HookEvent::PreFinish, completed_state, request_info.clone());
            let pre_finish_result = self.hooks.execute_pre(&pre_finish_ctx).await?;

            if !pre_finish_result.proceed {
                return Err(Error::HookRejected {
                    status_code: pre_finish_result.reject_status.unwrap_or(400),
                    message: pre_finish_result.reject_message.unwrap_or_default(),
                });
            }
        }

        let new_offset = self
            .storage
            .append(&mut state, ChunkStream::Buffered(body.bytes))
            .await?;
        state.set_offset(new_offset);
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

async fn collect_chunk_stream(
    stream: ChunkStream,
    body_limit: Option<u64>,
) -> Result<bytes::Bytes, Error> {
    match stream {
        ChunkStream::Buffered(b) => {
            enforce_body_limit(0, b.len(), body_limit)?;
            Ok(b)
        }
        ChunkStream::Stream(mut s) => {
            let mut buffer = bytes::BytesMut::new();
            while let Some(chunk) = s.next().await {
                let bytes = chunk.map_err(|e| Error::Internal(e.to_string()))?;
                enforce_body_limit(buffer.len(), bytes.len(), body_limit)?;
                buffer.extend_from_slice(&bytes);
            }
            Ok(buffer.freeze())
        }
    }
}

fn enforce_body_limit(
    current_len: usize,
    next_len: usize,
    body_limit: Option<u64>,
) -> Result<(), Error> {
    let Some(limit) = body_limit else {
        return Ok(());
    };
    let next_total = (current_len as u64).saturating_add(next_len as u64);
    if next_total > limit {
        return Err(Error::SizeExceeded {
            size: next_total,
            max: limit,
        });
    }

    Ok(())
}

fn body_size_limit(config: &Config, state: &crate::state::UploadState) -> Option<u64> {
    [
        config.max_chunk_size_limit(),
        config
            .max_size_limit()
            .map(|max_size| max_size.saturating_sub(state.offset())),
        state
            .length()
            .map(|length| length.saturating_sub(state.offset())),
    ]
    .into_iter()
    .flatten()
    .min()
}

fn validate_content_length(headers: &Headers, actual_len: u64) -> Result<(), Error> {
    if let Some(content_length) = headers.content_length
        && content_length != actual_len
    {
        return Err(Error::InvalidHeader {
            header: "Content-Length",
            message: format!(
                "declared content length {content_length} does not match body size {actual_len}"
            ),
        });
    }

    Ok(())
}

fn validate_body_size(
    config: &Config,
    state: &crate::state::UploadState,
    body_len: u64,
) -> Result<(), Error> {
    if let Some(max_chunk) = config.max_chunk_size_limit()
        && body_len > max_chunk
    {
        return Err(Error::SizeExceeded {
            size: body_len,
            max: max_chunk,
        });
    }

    if let Some(max_size) = config.max_size_limit() {
        let projected = state.offset().saturating_add(body_len);
        if projected > max_size {
            return Err(Error::SizeExceeded {
                size: projected,
                max: max_size,
            });
        }
    }

    if let Some(length) = state.length() {
        let projected = state.offset().saturating_add(body_len);
        if projected > length {
            return Err(Error::SizeExceeded {
                size: projected,
                max: length,
            });
        }
    }

    Ok(())
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
    use crate::storage::memory::MemoryStorage;
    use bytes::Bytes;
    use chrono::{Duration, Utc};

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
            .patch(h, &upload_id, PatchBody::stream(body(data), None))
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
                PatchBody::stream(body(b"Hello"), None),
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
    async fn collector_receives_limit_and_is_not_called_before_offset_validation() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let (storage, store) = setup(UploadState::new("test-id").with_length(10)).await;
        let config = Config::default().max_chunk_size(4);
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = "test-id".parse().unwrap();

        let called = Arc::new(AtomicBool::new(false));
        let called_for_collector = called.clone();
        let err = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .patch(
                headers(5),
                &upload_id,
                PatchBody::collector(move |_| async move {
                    called_for_collector.store(true, Ordering::SeqCst);
                    Ok(PatchBodyData {
                        bytes: Bytes::new(),
                        checksum: None,
                    })
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, Error::OffsetMismatch { .. }));
        assert!(!called.load(Ordering::SeqCst));

        let received_limit = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .patch(
                headers(0),
                &upload_id,
                PatchBody::collector(|body_limit| async move {
                    Ok(PatchBodyData {
                        bytes: Bytes::from_static(b"test"),
                        checksum: None,
                    })
                    .inspect(|_| assert_eq!(body_limit, Some(4)))
                }),
            )
            .await
            .unwrap();

        assert_eq!(received_limit.headers.get("upload-offset").unwrap(), "4");
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
