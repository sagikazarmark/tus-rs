//! Framework-neutral download helper.
//!
//! Download is not part of the core tus protocol. This module provides shared
//! server-side convenience behavior for framework adapters that want to expose a
//! GET endpoint for completed uploads.

use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};

use crate::config::TUS_RESUMABLE;
use crate::error::Error;
use crate::hooks::HookExecutor;
use crate::lifecycle::prepare_upload_download_access;
use crate::locking::Locker;
use crate::state::{StateStore, UploadState};
use crate::storage::{ByteStream, Storage, StorageReader};

use super::hook_context::{HookContextBuilder, HookRequestFacts};
use super::{Protocol, UploadId};

/// Request inputs for the non-standard download helper.
#[derive(Debug, Clone, Copy)]
pub struct DownloadRequest<'a> {
    upload_id: &'a UploadId,
    range: Option<&'a str>,
}

impl<'a> DownloadRequest<'a> {
    /// Creates a download request for an upload, without a byte range.
    #[must_use]
    pub fn new(upload_id: &'a UploadId) -> Self {
        Self {
            upload_id,
            range: None,
        }
    }

    /// Sets the raw HTTP `Range` header value, if present.
    #[must_use]
    pub fn with_range(mut self, range: Option<&'a str>) -> Self {
        self.range = range;
        self
    }
}

/// Streaming response produced by the non-standard download helper.
///
/// Construct with [`DownloadResponse::new`]. The struct is
/// `#[non_exhaustive]` so future response facts can be added without breaking
/// adapters; fields stay public for reading and destructuring with `..`.
#[non_exhaustive]
pub struct DownloadResponse {
    /// HTTP status code.
    pub status: StatusCode,
    /// Response headers.
    pub headers: HeaderMap,
    /// Streaming response body.
    pub body: ByteStream,
}

impl DownloadResponse {
    /// Creates a download response from its parts.
    #[must_use]
    pub fn new(status: StatusCode, headers: HeaderMap, body: ByteStream) -> Self {
        Self {
            status,
            headers,
            body,
        }
    }
}

impl std::fmt::Debug for DownloadResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadResponse")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .field("body", &"ByteStream(..)")
            .finish()
    }
}

impl<'a, S, I, L, H> Protocol<'a, S, I, L, H>
where
    S: Storage + StorageReader + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    /// Downloads a completed upload.
    ///
    /// This is a non-standard convenience endpoint for framework adapters that
    /// expose uploaded data with HTTP GET. It is not part of the core tus
    /// protocol.
    ///
    /// # Errors
    ///
    /// Returns an error if downloads are disabled, the upload does not exist,
    /// the upload is expired or incomplete, the requested byte range is invalid,
    /// or the backing storage cannot provide the requested stream.
    pub async fn download(&self, request: DownloadRequest<'_>) -> Result<DownloadResponse, Error> {
        if self.config.is_download_disabled() {
            return Err(Error::MethodNotAllowed("GET".to_string()));
        }

        let hook_contexts =
            HookContextBuilder::new(self.config, HookRequestFacts::get(request.upload_id));
        let upload_id = request.upload_id.as_str();

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

        prepare_upload_download_access(
            self.storage,
            self.state_store,
            self.locker,
            self.hooks,
            self.config,
            hook_contexts.request_info(),
            &mut state,
        )
        .await?;

        let size = state.offset();
        let range = parse_range(request.range, size)?;
        let mut headers = download_headers(&state)?;

        let (status, body) = if let Some((start, end)) = range {
            let handle = state.require_storage_handle()?;
            insert_header(
                &mut headers,
                "content-length",
                (end - start + 1).to_string(),
            )?;
            insert_header(
                &mut headers,
                "content-range",
                format!("bytes {start}-{end}/{size}"),
            )?;
            (
                StatusCode::PARTIAL_CONTENT,
                self.storage
                    .stream_range(&handle, start, Some(end + 1))
                    .await?,
            )
        } else if size == 0 {
            insert_header(&mut headers, "content-length", "0")?;
            let body: ByteStream = Box::pin(futures::stream::empty());
            (StatusCode::OK, body)
        } else {
            let handle = state.require_storage_handle()?;
            insert_header(&mut headers, "content-length", size.to_string())?;
            (StatusCode::OK, self.storage.stream(&handle).await?)
        };

        Ok(DownloadResponse::new(status, headers, body))
    }
}

fn parse_range(value: Option<&str>, size: u64) -> Result<Option<(u64, u64)>, Error> {
    let Some(value) = value else {
        return Ok(None);
    };

    let spec = value
        .strip_prefix("bytes=")
        .ok_or_else(|| Error::InvalidHeader {
            header: "Range",
            message: "only bytes ranges are supported".to_string(),
        })?;

    if spec.contains(',') {
        return Err(Error::InvalidHeader {
            header: "Range",
            message: "multiple ranges are not supported".to_string(),
        });
    }

    if size == 0 {
        return Err(Error::RangeNotSatisfiable { size });
    }

    let (start, end) = spec.split_once('-').ok_or_else(|| Error::InvalidHeader {
        header: "Range",
        message: "range must be in the form bytes=start-end".to_string(),
    })?;

    if start.is_empty() {
        let suffix_len = end.parse::<u64>().map_err(|_| Error::InvalidHeader {
            header: "Range",
            message: "invalid suffix byte count".to_string(),
        })?;
        if suffix_len == 0 {
            return Err(Error::RangeNotSatisfiable { size });
        }

        let start = size.saturating_sub(suffix_len.min(size));
        return Ok(Some((start, size - 1)));
    }

    let start = start.parse::<u64>().map_err(|_| Error::InvalidHeader {
        header: "Range",
        message: "invalid range start".to_string(),
    })?;
    if start >= size {
        return Err(Error::RangeNotSatisfiable { size });
    }

    let end = if end.is_empty() {
        size - 1
    } else {
        let parsed = end.parse::<u64>().map_err(|_| Error::InvalidHeader {
            header: "Range",
            message: "invalid range end".to_string(),
        })?;
        parsed.min(size - 1)
    };

    if start > end {
        return Err(Error::InvalidHeader {
            header: "Range",
            message: "range start must be less than or equal to range end".to_string(),
        });
    }

    Ok(Some((start, end)))
}

fn download_headers(state: &UploadState) -> Result<HeaderMap, Error> {
    let mut headers = HeaderMap::new();
    headers.insert("tus-resumable", HeaderValue::from_static(TUS_RESUMABLE));
    headers.insert("cache-control", HeaderValue::from_static("no-store"));
    headers.insert("accept-ranges", HeaderValue::from_static("bytes"));
    insert_header(&mut headers, "content-type", content_type(state))?;
    Ok(headers)
}

fn insert_header(
    headers: &mut HeaderMap,
    name: &'static str,
    value: impl AsRef<str>,
) -> Result<(), Error> {
    let value = HeaderValue::from_str(value.as_ref())
        .map_err(|err| Error::Internal(format!("failed to build download response: {err}")))?;
    headers.insert(HeaderName::from_static(name), value);
    Ok(())
}

fn content_type(state: &UploadState) -> &str {
    state
        .metadata()
        .get("content-type")
        .or_else(|| state.metadata().get("mimetype"))
        .and_then(|value| value.as_str())
        .unwrap_or("application/octet-stream")
}

#[cfg(all(
    test,
    feature = "state-memory",
    feature = "storage-memory",
    not(target_arch = "wasm32")
))]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::StreamExt;

    use crate::config::{Config, Extension};
    use crate::hooks::NoopHookExecutor;
    use crate::locking::NoopLocker;
    use crate::protocol::Protocol;
    use crate::state::{StateStore, UploadMetadata, UploadState, memory::MemoryStateStore};
    use crate::storage::{AppendRequest, ChunkStream, Storage, memory::MemoryStorage};

    async fn completed_upload(bytes: &'static [u8]) -> (MemoryStorage, MemoryStateStore) {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(bytes.len() as u64);
        let mut metadata = UploadMetadata::new();
        metadata.insert("mimetype".to_string(), "text/plain");
        state.set_metadata(metadata);

        let handle = storage.create(state.id()).await.unwrap();
        state.set_storage_handle(handle);
        let handle = storage
            .append(AppendRequest::new(
                state.require_storage_handle().unwrap(),
                state.offset(),
                ChunkStream::from_bytes(Bytes::from_static(bytes)),
                true,
            ))
            .await
            .unwrap();
        state.set_storage_handle(handle);
        state.set_offset(bytes.len() as u64);
        store.set(&state, true).await.unwrap();

        (storage, store)
    }

    async fn append_storage(storage: &MemoryStorage, state: &mut UploadState, bytes: Bytes) {
        let projected_offset = state.offset().saturating_add(bytes.len() as u64);
        let completes_upload = state
            .length()
            .is_some_and(|length| projected_offset == length);
        let handle = storage
            .append(AppendRequest::new(
                state.require_storage_handle().unwrap(),
                state.offset(),
                ChunkStream::from_bytes(bytes),
                completes_upload,
            ))
            .await
            .unwrap();
        state.set_storage_handle(handle);
        state.set_offset(projected_offset);
    }

    async fn body_bytes(response: DownloadResponse) -> Bytes {
        let mut stream = response.body;
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            body.extend_from_slice(&chunk.expect("download stream should succeed"));
        }
        Bytes::from(body)
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
        ) -> Result<crate::locking::LockGuard, crate::Error> {
            Err(crate::Error::Internal(
                "locker must not be exercised".to_string(),
            ))
        }

        async fn try_lock(
            &self,
            _upload_id: &str,
        ) -> Result<Option<crate::locking::LockGuard>, crate::Error> {
            Err(crate::Error::Internal(
                "locker must not be exercised".to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn download_of_unknown_upload_does_not_touch_locker() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let hooks = NoopHookExecutor::new();
        let upload_id = "missing".parse().unwrap();

        let err = Protocol::new(
            &Config::default(),
            &storage,
            &store,
            &RejectingLocker,
            &hooks,
        )
        .download(DownloadRequest::new(&upload_id))
        .await
        .unwrap_err();

        assert!(matches!(err, crate::Error::NotFound(_)));
    }

    #[tokio::test]
    async fn download_streams_completed_upload() {
        let (storage, store) = completed_upload(b"hello").await;
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id = "test-id".parse().unwrap();

        let response = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .download(DownloadRequest::new(&upload_id))
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.headers.get("content-type").unwrap(), "text/plain");
        assert_eq!(response.headers.get("content-length").unwrap(), "5");
        assert_eq!(body_bytes(response).await.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn download_serves_single_byte_range() {
        let (storage, store) = completed_upload(b"hello world").await;
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id = "test-id".parse().unwrap();

        let response = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .download(DownloadRequest::new(&upload_id).with_range(Some("bytes=6-10")))
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers.get("content-range").unwrap(),
            "bytes 6-10/11"
        );
        assert_eq!(response.headers.get("content-length").unwrap(), "5");
        assert_eq!(body_bytes(response).await.as_ref(), b"world");
    }

    #[tokio::test]
    async fn download_materializes_final_upload_once_partials_complete() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();

        let mut part1 = UploadState::new("part-1").with_length(4).as_partial();
        let handle = storage.create(part1.id()).await.unwrap();
        part1.set_storage_handle(handle);
        append_storage(&storage, &mut part1, Bytes::from_static(b"ABCD")).await;
        store.set(&part1, true).await.unwrap();

        let mut part2 = UploadState::new("part-2").with_length(4).as_partial();
        let handle = storage.create(part2.id()).await.unwrap();
        part2.set_storage_handle(handle);
        append_storage(&storage, &mut part2, Bytes::from_static(b"EFGH")).await;
        store.set(&part2, true).await.unwrap();

        let mut final_upload = UploadState::new("final-1");
        let handle = storage.create(final_upload.id()).await.unwrap();
        final_upload.set_storage_handle(handle);
        final_upload.mark_final(vec!["part-1".to_string(), "part-2".to_string()]);
        final_upload.set_length(8);
        final_upload.set_offset(4);
        store.set(&final_upload, true).await.unwrap();

        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id = "final-1".parse().unwrap();
        let response = Protocol::new(
            &Config::default().with_extension(Extension::Concatenation),
            &storage,
            &store,
            &locker,
            &hooks,
        )
        .download(DownloadRequest::new(&upload_id))
        .await
        .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.headers.get("content-length").unwrap(), "8");
        assert_eq!(body_bytes(response).await.as_ref(), b"ABCDEFGH");

        let stored = store.get("final-1").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 8);
    }

    #[tokio::test]
    async fn download_rejects_planned_final_upload_with_missing_part_as_expired() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();

        let mut final_upload = UploadState::new("final-1").with_length(4);
        let handle = storage.create(final_upload.id()).await.unwrap();
        final_upload.set_storage_handle(handle);
        final_upload.mark_final(vec!["missing-part".to_string()]);
        store.set(&final_upload, true).await.unwrap();

        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id = "final-1".parse().unwrap();
        let result = Protocol::new(
            &Config::default().with_extension(Extension::Concatenation),
            &storage,
            &store,
            &locker,
            &hooks,
        )
        .download(DownloadRequest::new(&upload_id))
        .await;

        assert!(matches!(result, Err(Error::Expired(id)) if id == "final-1"));
    }

    #[test]
    fn parse_range_rejects_unsatisfiable_start() {
        let err = parse_range(Some("bytes=10-20"), 5).unwrap_err();
        assert!(matches!(err, Error::RangeNotSatisfiable { size: 5 }));
    }
}
