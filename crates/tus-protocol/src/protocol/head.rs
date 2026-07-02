//! Core HEAD handler.
//!
//! Returns the current upload status: offset, length, metadata, and
//! expiration / concatenation information where applicable.

use http::StatusCode;

use crate::config::Extension;
use crate::error::Error;
use crate::hooks::HookExecutor;
use crate::lifecycle::prepare_upload_access;
use crate::locking::Locker;
use crate::state::{StateStore, UploadMetadata};
use crate::storage::Storage;

use super::hook_context::{HookContextBuilder, HookRequestFacts};
use super::{Protocol, Response, UploadId};

/// Returns the status of an upload identified by `upload_id`.
///
/// Errors:
/// - [`Error::NotFound`] if the upload doesn't exist.
/// - [`Error::Expired`] if the upload is protocol-expired.
impl<'a, S, I, L, H> Protocol<'a, S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    /// Returns the status of an upload identified by `upload_id`.
    ///
    /// The response includes the current offset, length, metadata, expiration,
    /// and concatenation headers where applicable.
    ///
    /// # Errors
    ///
    /// Returns an error if the upload does not exist, is protocol-expired, or
    /// if state reconciliation against the storage backend fails.
    pub async fn head(&self, upload_id: &UploadId) -> Result<Response, Error> {
        let hook_contexts = HookContextBuilder::new(self.config, HookRequestFacts::head(upload_id));
        let upload_id = upload_id.as_str();
        let _guard = self
            .locker
            .lock(upload_id, self.config.lock_timeout_duration())
            .await?;

        let mut upload_state = self
            .state_store
            .get(upload_id)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?
            .ok_or_else(|| Error::NotFound(upload_id.to_string()))?;

        let prepared = prepare_upload_access(
            self.storage,
            self.state_store,
            self.hooks,
            self.config,
            hook_contexts.request_info(),
            &mut upload_state,
        )
        .await?;
        let facts = prepared.facts;

        let mut response = Response::new(StatusCode::OK).with_header("cache-control", "no-store");

        if let Some(offset) = facts.offset {
            response = response.with_header("upload-offset", offset.to_string());
        }

        if let Some(length) = facts.length {
            response = response.with_header("upload-length", length.to_string());
        } else if facts.defer_length {
            // `Upload-Defer-Length` is a creation-time signal for non-final
            // uploads. Final uploads always have a known length (sum of parts)
            // or are unfinished with length-not-yet-known, which we simply omit.
            response = response.with_header("upload-defer-length", "1");
        }

        if !upload_state.metadata().is_empty() {
            response = response.with_header(
                "upload-metadata",
                encode_upload_metadata(upload_state.metadata()),
            );
        }

        if self.config.has_extension(Extension::Expiration)
            && let Some(expires) = upload_state.expires_header()
        {
            response = response.with_header("upload-expires", expires);
        }

        if self.config.has_extension(Extension::Concatenation) {
            if upload_state.is_partial() {
                response = response.with_header("upload-concat", "partial");
            } else if upload_state.is_final() {
                // Include the part URLs (as paths relative to `base_path`) so
                // clients can reconstruct the composition.
                let concat_value = match upload_state.parts() {
                    Some(parts) if !parts.is_empty() => {
                        let urls: Vec<String> = parts
                            .iter()
                            .map(|id| format!("{}/{}", self.config.base_path_str(), id))
                            .collect();
                        format!("final;{}", urls.join(" "))
                    }
                    _ => "final".to_string(),
                };
                response = response.with_header("upload-concat", concat_value);
            }
        }

        Ok(response)
    }
}

/// Encodes metadata for the Upload-Metadata response header.
fn encode_upload_metadata(metadata: &UploadMetadata) -> String {
    if metadata.is_empty() {
        return String::new();
    }

    use base64::Engine;
    metadata
        .iter()
        .map(|(k, v)| {
            let encoded = base64::engine::general_purpose::STANDARD.encode(v.as_bytes());
            format!("{} {}", k, encoded)
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(all(
    test,
    feature = "state-memory",
    feature = "storage-memory",
    not(feature = "local-futures")
))]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::hooks::{HookChain, NoopHookExecutor, PreHookResult};
    use crate::locking::{LockGuard, Locker, NoopLocker};
    use crate::state::{UploadState, memory::MemoryStateStore};
    use crate::storage::{
        AppendRequest, ChunkStream, Storage, StorageReader, memory::MemoryStorage,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::{Duration, TimeZone, Utc};
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration as StdDuration;

    struct RecordingLocker {
        lock_calls: AtomicUsize,
    }

    impl RecordingLocker {
        fn new() -> Self {
            Self {
                lock_calls: AtomicUsize::new(0),
            }
        }

        fn lock_calls(&self) -> usize {
            self.lock_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Locker for RecordingLocker {
        fn name(&self) -> &'static str {
            "recording"
        }

        async fn lock(&self, upload_id: &str, _timeout: StdDuration) -> Result<LockGuard, Error> {
            self.lock_calls.fetch_add(1, Ordering::SeqCst);
            Ok(LockGuard::new(upload_id))
        }

        async fn try_lock(&self, upload_id: &str) -> Result<Option<LockGuard>, Error> {
            self.lock_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(LockGuard::new(upload_id)))
        }

        async fn unlock(&self, _upload_id: &str) -> Result<(), Error> {
            Ok(())
        }

        async fn is_locked(&self, _upload_id: &str) -> Result<bool, Error> {
            Ok(false)
        }
    }

    async fn store_with(state: UploadState) -> MemoryStateStore {
        let store = MemoryStateStore::new();
        store.set(&state, true).await.unwrap();
        store
    }

    async fn call(
        config: &Config,
        storage: &MemoryStorage,
        state_store: &MemoryStateStore,
        upload_id: &str,
    ) -> Result<Response, Error> {
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = upload_id.parse().unwrap();
        Protocol::new(config, storage, state_store, &locker, &hooks)
            .head(&upload_id)
            .await
    }

    async fn create_storage(storage: &MemoryStorage, state: &mut UploadState) {
        let handle = storage.create(state.id()).await.unwrap();
        state.set_storage_handle(handle);
    }

    async fn append_storage(storage: &MemoryStorage, state: &mut UploadState, bytes: Bytes) {
        let projected_offset = state.offset().saturating_add(bytes.len() as u64);
        let completes_upload = state
            .length()
            .is_some_and(|length| projected_offset == length);
        let handle = storage
            .append(AppendRequest {
                handle: state.require_storage_handle().unwrap(),
                expected_offset: state.offset(),
                data: ChunkStream::from_bytes(bytes),
                completes_upload,
            })
            .await
            .unwrap();
        state.set_storage_handle(handle);
        state.set_offset(projected_offset);
    }

    async fn body_bytes(storage: &MemoryStorage, state: &UploadState) -> Bytes {
        let body = storage
            .get_stream(&state.require_storage_handle().unwrap())
            .await
            .unwrap();
        let chunks = body.collect::<Vec<_>>().await;
        chunks
            .into_iter()
            .map(|chunk| chunk.unwrap())
            .fold(bytes::BytesMut::new(), |mut acc, chunk| {
                acc.extend_from_slice(&chunk);
                acc
            })
            .freeze()
    }

    #[tokio::test]
    async fn basic() {
        let storage = MemoryStorage::new();
        let store = store_with(UploadState::new("test-id").with_length(1000)).await;
        let response = call(&Config::default(), &storage, &store, "test-id")
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.headers.get("upload-offset").unwrap(), "0");
        assert_eq!(response.headers.get("upload-length").unwrap(), "1000");
    }

    #[tokio::test]
    async fn head_acquires_upload_lock_before_reconciliation() {
        let storage = MemoryStorage::new();
        let store = store_with(UploadState::new("test-id").with_length(1000)).await;
        let locker = RecordingLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = "test-id".parse().unwrap();

        Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .head(&upload_id)
            .await
            .unwrap();

        assert_eq!(locker.lock_calls(), 1);
    }

    #[tokio::test]
    async fn with_offset() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(1000);
        create_storage(&storage, &mut state).await;
        append_storage(&storage, &mut state, Bytes::from(vec![0; 500])).await;
        store.set(&state, true).await.unwrap();
        let response = call(&Config::default(), &storage, &store, "test-id")
            .await
            .unwrap();
        assert_eq!(response.headers.get("upload-offset").unwrap(), "500");
    }

    #[tokio::test]
    async fn not_found() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let err = call(&Config::default(), &storage, &store, "missing")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn deferred_length() {
        let storage = MemoryStorage::new();
        let store = store_with(UploadState::new("test-id")).await;
        let response = call(&Config::default(), &storage, &store, "test-id")
            .await
            .unwrap();
        assert!(response.headers.get("upload-length").is_none());
        assert_eq!(response.headers.get("upload-defer-length").unwrap(), "1");
    }

    #[tokio::test]
    async fn expired() {
        let storage = MemoryStorage::new();
        let state = UploadState::new("test-id")
            .with_length(1000)
            .with_expiration(Utc::now() - Duration::hours(1));
        let store = store_with(state).await;
        let config = Config::default().with_extension(Extension::Expiration);
        let err = call(&config, &storage, &store, "test-id")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Expired(_)));
    }

    #[tokio::test]
    async fn metadata_is_encoded() {
        let storage = MemoryStorage::new();
        let mut metadata = UploadMetadata::new();
        metadata.insert("filename".to_string(), "test.txt");
        let state = UploadState::new("test-id")
            .with_length(1000)
            .with_metadata(metadata);
        let store = store_with(state).await;
        let response = call(&Config::default(), &storage, &store, "test-id")
            .await
            .unwrap();
        let header = response
            .headers
            .get("upload-metadata")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(header.contains("filename"));
    }

    #[tokio::test]
    async fn partial_concat_header() {
        let storage = MemoryStorage::new();
        let state = UploadState::new("test-id").with_length(1000).as_partial();
        let store = store_with(state).await;
        let config = Config::default().with_extension(Extension::Concatenation);
        let response = call(&config, &storage, &store, "test-id").await.unwrap();
        assert_eq!(response.headers.get("upload-concat").unwrap(), "partial");
    }

    #[tokio::test]
    async fn upload_expires_header_is_rfc7231() {
        let storage = MemoryStorage::new();
        let expires_at = Utc.with_ymd_and_hms(2030, 6, 25, 14, 30, 0).unwrap();
        let state = UploadState::new("test-id")
            .with_length(1000)
            .with_expiration(expires_at);
        let store = store_with(state).await;
        let config = Config::default().with_extension(Extension::Expiration);
        let response = call(&config, &storage, &store, "test-id").await.unwrap();
        let value = response
            .headers
            .get("upload-expires")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(value, "Tue, 25 Jun 2030 14:30:00 GMT");
    }

    #[tokio::test]
    async fn cache_control_is_no_store() {
        let storage = MemoryStorage::new();
        let store = store_with(UploadState::new("test-id").with_length(1000)).await;
        let response = call(&Config::default(), &storage, &store, "test-id")
            .await
            .unwrap();
        assert_eq!(response.headers.get("cache-control").unwrap(), "no-store");
    }

    #[tokio::test]
    async fn unfinished_final_omits_upload_offset() {
        let storage = MemoryStorage::new();
        // Partial not yet complete.
        let mut partial = UploadState::new("part-1").with_length(1000).as_partial();
        partial.set_offset(250);
        let store = MemoryStateStore::new();
        store.set(&partial, true).await.unwrap();

        // Final pointing at it (not complete).
        let mut final_upload = UploadState::new("final-1");
        final_upload.mark_final(vec!["part-1".to_string()]);
        final_upload.set_length(1000);
        final_upload.set_offset(0);
        store.set(&final_upload, true).await.unwrap();

        let config = Config::default().with_extension(Extension::Concatenation);
        let response = call(&config, &storage, &store, "final-1").await.unwrap();
        assert!(response.headers.get("upload-offset").is_none());
        let concat = response
            .headers
            .get("upload-concat")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(concat.starts_with("final;"), "got {:?}", concat);
        assert!(concat.contains("/files/part-1"), "got {:?}", concat);

        let stored = store.get("final-1").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 250);
    }

    #[tokio::test]
    async fn head_materializes_final_upload_once_partials_complete() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();

        let mut part1 = UploadState::new("part-1").with_length(4).as_partial();
        create_storage(&storage, &mut part1).await;
        append_storage(&storage, &mut part1, Bytes::from_static(b"ABCD")).await;
        store.set(&part1, true).await.unwrap();

        let mut part2 = UploadState::new("part-2").with_length(4).as_partial();
        create_storage(&storage, &mut part2).await;
        append_storage(&storage, &mut part2, Bytes::from_static(b"EFGH")).await;
        store.set(&part2, true).await.unwrap();

        let mut final_upload = UploadState::new("final-1");
        create_storage(&storage, &mut final_upload).await;
        final_upload.mark_final(vec!["part-1".to_string(), "part-2".to_string()]);
        final_upload.set_length(8);
        final_upload.set_offset(4);
        store.set(&final_upload, true).await.unwrap();

        let config = Config::default().with_extension(Extension::Concatenation);
        let response = call(&config, &storage, &store, "final-1").await.unwrap();

        assert_eq!(response.headers.get("upload-offset").unwrap(), "8");

        let stored = store.get("final-1").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 8);

        let bytes = body_bytes(&storage, &stored).await;
        assert_eq!(&bytes[..], b"ABCDEFGH");
    }

    #[tokio::test]
    async fn pre_finish_rejection_blocks_head_materialization() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();

        let mut part = UploadState::new("part-1").with_length(4).as_partial();
        create_storage(&storage, &mut part).await;
        append_storage(&storage, &mut part, Bytes::from_static(b"ABCD")).await;
        store.set(&part, true).await.unwrap();

        let mut final_upload = UploadState::new("final-1");
        create_storage(&storage, &mut final_upload).await;
        final_upload.mark_final(vec!["part-1".to_string()]);
        final_upload.set_length(4);
        final_upload.set_offset(0);
        store.set(&final_upload, true).await.unwrap();

        let config = Config::default().with_extension(Extension::Concatenation);
        let locker = NoopLocker::new();
        let hooks = HookChain::new()
            .on_pre_finish(|_| async { Ok(PreHookResult::reject(403, "finish blocked")) });
        let upload_id: UploadId = "final-1".parse().unwrap();

        let err = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .head(&upload_id)
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::HookRejected {
                status_code: 403,
                ..
            }
        ));
        let stored = store.get("final-1").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 0);
        assert!(!stored.is_complete());
        assert_eq!(
            storage
                .size(&stored.require_storage_handle().unwrap())
                .await
                .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn pre_finish_rejection_blocks_repairing_complete_final_record() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();

        let mut part = UploadState::new("part-1").with_length(4).as_partial();
        create_storage(&storage, &mut part).await;
        append_storage(&storage, &mut part, Bytes::from_static(b"ABCD")).await;
        store.set(&part, true).await.unwrap();

        let mut final_upload = UploadState::new("final-1");
        create_storage(&storage, &mut final_upload).await;
        final_upload.mark_final(vec!["part-1".to_string()]);
        final_upload.set_length(4);
        final_upload.set_offset(4);
        store.set(&final_upload, true).await.unwrap();

        let config = Config::default().with_extension(Extension::Concatenation);
        let locker = NoopLocker::new();
        let hooks = HookChain::new()
            .on_pre_finish(|_| async { Ok(PreHookResult::reject(403, "finish blocked")) });
        let upload_id: UploadId = "final-1".parse().unwrap();

        let err = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .head(&upload_id)
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::HookRejected {
                status_code: 403,
                ..
            }
        ));
        let stored = store.get("final-1").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 4);
        assert!(stored.is_complete());
        assert_eq!(
            storage
                .size(&stored.require_storage_handle().unwrap())
                .await
                .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn completed_final_emits_upload_offset() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut final_upload = UploadState::new("final-1");
        create_storage(&storage, &mut final_upload).await;
        append_storage(&storage, &mut final_upload, Bytes::from(vec![0; 1000])).await;
        final_upload.mark_final(vec!["a".to_string(), "b".to_string()]);
        final_upload.set_length(1000);
        final_upload.set_offset(1000);
        store.set(&final_upload, true).await.unwrap();

        let config = Config::default().with_extension(Extension::Concatenation);
        let response = call(&config, &storage, &store, "final-1").await.unwrap();
        assert_eq!(response.headers.get("upload-offset").unwrap(), "1000");
        let concat = response
            .headers
            .get("upload-concat")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(concat, "final;/files/a /files/b");
    }

    #[tokio::test]
    async fn materialized_final_upload_does_not_require_partial_state_records() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut final_upload = UploadState::new("final-1");
        create_storage(&storage, &mut final_upload).await;
        append_storage(&storage, &mut final_upload, Bytes::from_static(b"ABCD")).await;
        final_upload.mark_final(vec!["missing-part".to_string()]);
        final_upload.set_length(4);
        final_upload.set_offset(4);
        store.set(&final_upload, true).await.unwrap();

        let config = Config::default().with_extension(Extension::Concatenation);
        let response = call(&config, &storage, &store, "final-1").await.unwrap();

        assert_eq!(response.headers.get("upload-offset").unwrap(), "4");
    }

    #[tokio::test]
    async fn head_reconciles_offset_from_storage() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(10);
        create_storage(&storage, &mut state).await;
        append_storage(&storage, &mut state, Bytes::from_static(b"hello")).await;
        state.set_offset(0);
        store.set(&state, true).await.unwrap();

        let response = call(&Config::default(), &storage, &store, "test-id")
            .await
            .unwrap();
        assert_eq!(response.headers.get("upload-offset").unwrap(), "5");

        let stored = store.get("test-id").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 5);
    }
}
