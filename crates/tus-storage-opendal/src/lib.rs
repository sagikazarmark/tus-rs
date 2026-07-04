//! OpenDAL storage implementation.
//!
//! This storage backend wraps a caller-provided Apache OpenDAL `Operator`.
//! Configure the operator in application code, then pass it to
//! [`OpendalStorage::new`]. Construct the operator from this crate's
//! [`opendal`] re-export so it matches the exact `opendal` version this crate
//! links against.
//!
//! # Append strategy
//!
//! Many OpenDAL backends do not support native append for object writes. Rather
//! than the previous read-modify-write fallback (which is O(n²) in the
//! number of PATCH chunks), this implementation writes each PATCH into its
//! own staging object:
//!
//! ```text
//! uploads/<id>                      // final/main object (only populated on finalize)
//! uploads/<id>.parts/0000000001     // chunk from PATCH 1
//! uploads/<id>.parts/0000000002     // chunk from PATCH 2
//! ...
//! ```
//!
//! Each PATCH is O(1) in terms of storage cost (one PUT). On finalize,
//! the append that either completes the declared `Upload-Length` or is
//! triggered by `concat()`, all staging objects are streamed into the
//! main key in part-number order via a temporary object that is promoted only
//! after the complete object is written, then the staging prefix is removed.
//! Completion requires an OpenDAL service that supports `rename` or `copy` so
//! partially materialized target objects are not exposed.
//!
//! # Single-writer expectation
//!
//! This backend expects a single writer per upload at a time. The staging
//! scheme's check-then-put (stat the next part key, then write it) is not
//! atomic across processes, so concurrent processes appending to the same
//! upload can both claim the same part object. Deployments where multiple
//! processes may serve PATCH requests for the same upload must serialize them
//! through an external cross-process locker; the in-process locker shipped
//! with `tus-protocol` is not sufficient.

#![warn(missing_docs)]

use async_trait::async_trait;
use futures::StreamExt;
use std::io;

/// Re-export of the [`opendal`] crate this backend is built against.
///
/// [`OpendalStorage::new`] takes an [`opendal::Operator`]. `opendal` is a
/// fast-moving 0.x crate, so construct the operator through this re-export to
/// guarantee the `Operator` comes from the exact version this crate links
/// against.
pub use opendal;
use opendal::Operator;

use tus_protocol::{AppendRequest, ByteStream, ConcatRequest, Error, Result, StorageHandle};

mod staging;

/// OpenDAL-based storage backend.
///
/// The caller provides the configured OpenDAL operator. This crate only maps
/// the TUS storage operations onto OpenDAL object operations.
pub struct OpendalStorage {
    operator: Operator,
    prefix: String,
}

impl OpendalStorage {
    /// Creates a new OpenDAL storage with the given operator.
    ///
    /// Upload objects are keyed directly by upload id; use
    /// [`with_prefix`](Self::with_prefix) to nest them under a key prefix.
    pub fn new(operator: Operator) -> Self {
        Self {
            operator,
            prefix: String::new(),
        }
    }

    /// Returns the storage with upload keys nested under the given prefix.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Generates a storage key for an upload.
    fn make_key(&self, id: &str) -> String {
        if self.prefix.is_empty() {
            id.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), id)
        }
    }
}

impl std::fmt::Debug for OpendalStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpendalStorage")
            .field("prefix", &self.prefix)
            .finish()
    }
}

#[async_trait]
impl tus_protocol::Storage for OpendalStorage {
    fn name(&self) -> &'static str {
        "opendal"
    }

    async fn create(&self, upload_id: &str) -> Result<StorageHandle> {
        let key = self.make_key(upload_id);
        let mut handle = StorageHandle::new(key);
        staging::UploadObjects::initialize_handle(&mut handle);
        Ok(handle)
    }

    async fn append(&self, request: AppendRequest) -> Result<StorageHandle> {
        let AppendRequest {
            mut handle,
            expected_offset,
            data,
            completes_upload,
        } = request;
        let key = handle.key().to_string();
        let upload = staging::UploadObjects::new(&self.operator, &key);

        // Staging validates the offset, then streams the body into the next
        // part object without buffering it; a mid-stream failure discards the
        // partial part so a failed PATCH changes nothing.
        upload
            .append_part(&mut handle, expected_offset, data)
            .await?;

        // Lifecycle owns completion detection. Deferred-length uploads stay in
        // staging until the PATCH that declares and reaches the length.
        if completes_upload {
            upload.finalize().await?;
        }

        Ok(handle)
    }

    async fn concat(&self, request: ConcatRequest) -> Result<StorageHandle> {
        let ConcatRequest { target, parts } = request;
        let target_key = target.key();

        let part_keys: Vec<String> = parts.iter().map(|part| part.key().to_string()).collect();
        staging::UploadObjects::new(&self.operator, target_key)
            .concat(&part_keys)
            .await?;

        Ok(target)
    }

    async fn delete(&self, handle: &StorageHandle) -> Result<()> {
        staging::UploadObjects::new(&self.operator, handle.key())
            .delete_all()
            .await
    }

    async fn size(&self, handle: &StorageHandle) -> Result<Option<u64>> {
        staging::UploadObjects::new(&self.operator, handle.key())
            .stored_size()
            .await
    }
}

#[async_trait]
impl tus_protocol::StorageReader for OpendalStorage {
    async fn stream(&self, handle: &StorageHandle) -> Result<ByteStream> {
        let key = handle.key();

        let reader = self.operator.reader(key).await.map_err(Error::storage)?;

        // Convert OpenDAL reader to our ByteStream
        let stream = reader
            .into_bytes_stream(0..)
            .await
            .map_err(Error::storage)?;

        Ok(Box::pin(
            stream.map(|result| result.map_err(io::Error::other)),
        ))
    }

    async fn stream_range(
        &self,
        handle: &StorageHandle,
        start: u64,
        end: Option<u64>,
    ) -> Result<ByteStream> {
        let key = handle.key();

        let reader = self.operator.reader(key).await.map_err(Error::storage)?;
        let stream = match end {
            Some(end) => reader
                .into_bytes_stream(start..end)
                .await
                .map_err(Error::storage)?,
            None => reader
                .into_bytes_stream(start..)
                .await
                .map_err(Error::storage)?,
        };

        Ok(Box::pin(
            stream.map(|result| result.map_err(io::Error::other)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use opendal::services::Fs;
    use tus_protocol::storage::conformance;
    use tus_protocol::{ChunkStream, Storage, StorageReader};

    struct TestStorage {
        storage: OpendalStorage,
        _tempdir: tempfile::TempDir,
    }

    impl TestStorage {
        fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
            self.storage = self.storage.with_prefix(prefix);
            self
        }
    }

    impl std::ops::Deref for TestStorage {
        type Target = OpendalStorage;

        fn deref(&self) -> &Self::Target {
            &self.storage
        }
    }

    fn create_test_storage() -> TestStorage {
        let tempdir = tempfile::tempdir().unwrap();
        let operator = Operator::new(Fs::default().root(tempdir.path().to_str().unwrap()))
            .unwrap()
            .finish();

        TestStorage {
            storage: OpendalStorage::new(operator),
            _tempdir: tempdir,
        }
    }

    #[tokio::test]
    async fn storage_conformance() {
        let storage = create_test_storage();

        conformance::assert_full_semantics(&storage.storage).await;
    }

    #[tokio::test]
    async fn append_that_completes_upload_finalizes_content() {
        let storage = create_test_storage();

        // Create
        let handle = storage.create("test-upload").await.unwrap();
        assert!(!handle.key().is_empty());

        // Append the full upload in one shot; finalizes because lifecycle says
        // this write completes the upload.
        let data = ChunkStream::from_bytes(Bytes::from("hello world"));
        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data,
                completes_upload: true,
            })
            .await
            .unwrap();

        // After finalize the main key holds the bytes.
        assert_eq!(storage.size(&handle).await.unwrap(), Some(11));
    }

    #[tokio::test]
    async fn test_multiple_appends() {
        let storage = create_test_storage();

        let handle = storage.create("test-upload-2").await.unwrap();

        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("hello ")),
                completes_upload: false,
            })
            .await
            .unwrap();

        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 6,
                data: ChunkStream::from_bytes(Bytes::from("world")),
                completes_upload: true,
            })
            .await
            .unwrap();

        assert_eq!(storage.size(&handle).await.unwrap(), Some(11));
    }

    #[tokio::test]
    async fn test_deferred_length_size_reports_offset() {
        // Without a declared length, we never finalize, but size() should
        // still report the staged bytes so HEAD responses are useful.
        let storage = create_test_storage();

        let handle = storage.create("deferred-upload").await.unwrap();
        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("hello")),
                completes_upload: false,
            })
            .await
            .unwrap();

        assert_eq!(storage.size(&handle).await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn append_advances_part_cursor() {
        let storage = create_test_storage();
        let handle = storage.create("handle-internals").await.unwrap();

        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("hello")),
                completes_upload: false,
            })
            .await
            .unwrap();

        assert_eq!(handle.internal(staging::INTERNAL_NEXT_PART), Some("2"));
        assert_eq!(handle.internal(staging::INTERNAL_STAGED_SIZE), Some("5"));
    }

    #[tokio::test]
    async fn failed_stream_append_discards_partial_part() {
        let storage = create_test_storage();

        let handle = storage.create("failed-stream-upload").await.unwrap();
        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("hello")),
                completes_upload: false,
            })
            .await
            .unwrap();

        let failing: ByteStream = Box::pin(futures::stream::iter(vec![
            Ok(Bytes::from("wor")),
            Err(io::Error::other("client went away")),
        ]));
        let error = storage
            .append(AppendRequest {
                handle: handle.clone(),
                expected_offset: 5,
                data: ChunkStream::from_stream(failing),
                completes_upload: false,
            })
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Io(_)));
        // The failed PATCH must leave no partial staged part behind: the
        // offset is unchanged and the next part object does not exist.
        assert_eq!(storage.size(&handle).await.unwrap(), Some(5));
        let part2 = format!("{}.parts/{:010}", handle.key(), 2);
        assert!(matches!(
            storage.operator.stat(&part2).await,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound
        ));
    }

    #[tokio::test]
    async fn test_deferred_length_size_uses_staged_bytes_not_state_offset() {
        let storage = create_test_storage();

        let handle = storage.create("stale-offset-upload").await.unwrap();
        let mut handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("hello")),
                completes_upload: false,
            })
            .await
            .unwrap();

        handle.set_internal(staging::INTERNAL_NEXT_PART, "99");
        assert_eq!(storage.size(&handle).await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn size_uses_staged_bytes_when_main_object_is_partial() {
        let storage = create_test_storage();

        let handle = storage.create("partial-main-upload").await.unwrap();
        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("hello")),
                completes_upload: false,
            })
            .await
            .unwrap();

        storage
            .operator
            .write(handle.key(), Bytes::from("he"))
            .await
            .unwrap();

        assert_eq!(storage.size(&handle).await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn append_with_stale_part_cursor_does_not_overwrite_existing_staged_part() {
        let storage = create_test_storage();

        let active_handle = storage.create("recovered-upload").await.unwrap();

        // Simulates the state persisted before a crash: create() stored the
        // first part cursor, then the process crashed after writing part 1
        // but before persisting the incremented cursor and offset.
        let recovered_handle = active_handle.clone();

        let _active_handle = storage
            .append(AppendRequest {
                handle: active_handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("hello")),
                completes_upload: false,
            })
            .await
            .unwrap();

        let recovered_offset = storage.size(&recovered_handle).await.unwrap().unwrap();

        let recovered_handle = storage
            .append(AppendRequest {
                handle: recovered_handle,
                expected_offset: recovered_offset,
                data: ChunkStream::from_bytes(Bytes::from("world")),
                completes_upload: true,
            })
            .await
            .unwrap();

        let mut stream = storage.stream(&recovered_handle).await.unwrap();
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            data.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(data, b"helloworld");
    }

    #[tokio::test]
    async fn test_get_stream() {
        let storage = create_test_storage();

        let handle = storage.create("test-upload-3").await.unwrap();
        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("test content")),
                completes_upload: true,
            })
            .await
            .unwrap();

        let mut stream = storage.stream(&handle).await.unwrap();
        let mut data = Vec::new();

        while let Some(chunk) = stream.next().await {
            data.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(String::from_utf8(data).unwrap(), "test content");
    }

    #[tokio::test]
    async fn test_concat() {
        let storage = create_test_storage();

        // Create parts; each declares its length so finalize runs.
        let part1 = storage.create("part1").await.unwrap();
        let part1 = storage
            .append(AppendRequest {
                handle: part1,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("Hello ")),
                completes_upload: true,
            })
            .await
            .unwrap();

        let part2 = storage.create("part2").await.unwrap();
        let part2 = storage
            .append(AppendRequest {
                handle: part2,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("World")),
                completes_upload: true,
            })
            .await
            .unwrap();

        // Create target and concat
        let target = storage.create("final").await.unwrap();
        let target = storage
            .concat(ConcatRequest {
                target,
                parts: vec![part1, part2],
            })
            .await
            .unwrap();

        // Read result
        let mut stream = storage.stream(&target).await.unwrap();
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            data.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(String::from_utf8(data).unwrap(), "Hello World");
    }

    #[tokio::test]
    async fn failed_concat_does_not_expose_partial_target() {
        let storage = create_test_storage();

        let part = storage.create("concat-good-part").await.unwrap();
        let part = storage
            .append(AppendRequest {
                handle: part,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("Hello ")),
                completes_upload: true,
            })
            .await
            .unwrap();

        let missing_part = storage.create("concat-missing-part").await.unwrap();
        let target = storage.create("concat-failed-target").await.unwrap();
        let error = storage
            .concat(ConcatRequest {
                target: target.clone(),
                parts: vec![part, missing_part],
            })
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Storage(_)));
        assert_eq!(storage.size(&target).await.unwrap(), None);
    }

    #[tokio::test]
    async fn concat_preserves_existing_target_handle_internals() {
        let storage = create_test_storage();
        let part = storage.create("part-internals").await.unwrap();
        let part = storage
            .append(AppendRequest {
                handle: part,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("part")),
                completes_upload: true,
            })
            .await
            .unwrap();
        let mut target = storage.create("target-internals").await.unwrap();
        target.set_internal("target_fact", "keep-me");

        let target = storage
            .concat(ConcatRequest {
                target,
                parts: vec![part],
            })
            .await
            .unwrap();

        assert_eq!(target.internal("target_fact"), Some("keep-me"));
    }

    #[tokio::test]
    async fn test_delete() {
        let storage = create_test_storage();

        let handle = storage.create("test-delete").await.unwrap();
        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("hello")),
                completes_upload: true,
            })
            .await
            .unwrap();
        assert!(storage.size(&handle).await.unwrap().is_some());

        storage.delete(&handle).await.unwrap();
        assert!(storage.size(&handle).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_cleans_up_staging() {
        // Staging parts from an un-finalized upload should also be removed
        // on delete.
        let storage = create_test_storage();

        let handle = storage.create("stale-upload").await.unwrap();
        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("abc")),
                completes_upload: false,
            })
            .await
            .unwrap();

        assert_eq!(storage.size(&handle).await.unwrap(), Some(3));

        storage.delete(&handle).await.unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_with_prefix() {
        let storage = create_test_storage().with_prefix("uploads/2024");

        let handle = storage.create("test-id").await.unwrap();

        assert!(handle.key().starts_with("uploads/2024/"));
    }
}
