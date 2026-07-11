//! OpenDAL storage implementation.
//!
//! This storage backend wraps a caller-provided Apache OpenDAL `Operator`.
//! Configure the operator in application code, then pass it to
//! [`OpendalStorage::new`]. Construct the operator from this crate's
//! [`opendal`] re-export so it matches the exact `opendal` version this crate
//! links against. Cargo features named `services-*` (for example
//! `services-s3`) forward to the same-named `opendal` features so downstream
//! crates can enable backends without depending on `opendal` directly.
//!
//! Only a curated subset of OpenDAL services has a passthrough feature here
//! (currently `services-azblob`, `services-fs`, `services-gcs`,
//! `services-memory`, and `services-s3`). To use any other OpenDAL backend,
//! add a direct `opendal` dependency (matching the re-exported version) and
//! enable its `services-*` feature there; `OpendalStorage` works with any
//! [`opendal::Operator`] regardless of how the backend feature was enabled.
//!
//! # Append strategy
//!
//! Many OpenDAL backends do not support native append for object writes. Rather
//! than the previous read-modify-write fallback (which is O(n²) in the
//! number of PATCH chunks), this implementation writes each PATCH into its
//! own staging object. Every object for an upload lives strictly under the
//! upload's own `{id}/` prefix:
//!
//! ```text
//! uploads/<id>/data                 // final/main object (only populated on finalize)
//! uploads/<id>/parts/0000000001     // chunk from PATCH 1
//! uploads/<id>/parts/0000000002     // chunk from PATCH 2
//! uploads/<id>/tmp/...              // temporary objects promoted into place
//! uploads/<id>/complete             // completion marker (final part number)
//! ...
//! ```
//!
//! The `/`-nested layout is collision-proof: upload ids may contain dots (so
//! `report`, `report.complete`, and `report.parts` are all valid ids) but can
//! never contain `/`, so each upload owns a disjoint `{id}/` keyspace. A
//! dot-suffixed sibling layout (`<id>.parts`, `<id>.complete`) would instead
//! alias one upload's staging objects onto another upload's main object.
//!
//! Each PATCH is O(1) in terms of storage cost (one PUT). On finalize,
//! the append that either completes the declared `Upload-Length` or is
//! triggered by `concat()`, all staging objects are streamed into the
//! deliverable object in part-number order via a temporary object that is
//! promoted only after the complete object is written, then the staging prefix
//! is removed. Completion requires an OpenDAL service that supports `rename` or
//! `copy` so partially materialized target objects are not exposed.
//!
//! # Interrupted finalize and lazy repair
//!
//! The completing PATCH first writes a durable completion marker
//! (`uploads/<id>/complete`, recording the final part number), then stages the
//! final part, then finalizes. Finalize is retried a bounded number of times
//! inline; if it still fails (or the process crashes before it runs), the
//! upload is fully staged but the main object does not exist yet. The read
//! paths ([`StorageReader::stream`](tus_protocol::StorageReader::stream),
//! [`StorageReader::stream_range`](tus_protocol::StorageReader::stream_range)) and the
//! part-read side of [`Storage::concat`](tus_protocol::Storage::concat)
//! detect this state via the marker and lazily re-drive materialization
//! before serving. Repair never deletes staged parts, so concurrent repairs
//! promote byte-identical content (idempotent last-writer-wins); leftover
//! staging objects are removed when the upload is deleted.
//!
//! # Reads of incomplete uploads
//!
//! Unlike `FileStorage` in `tus-protocol` (which serves the partial bytes
//! written so far), this backend returns [`Error::NotFound`] from
//! [`StorageReader::stream`](tus_protocol::StorageReader::stream) and
//! [`StorageReader::stream_range`](tus_protocol::StorageReader::stream_range) for uploads
//! that are still incomplete: their bytes live only in staging objects and no
//! main object exists to read. Complete uploads whose finalize was
//! interrupted are repaired transparently as described above.
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

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use async_trait::async_trait;
use futures_util::StreamExt;
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

/// Extra inline `finalize` attempts after the first failure.
///
/// A failed finalize leaves a completed upload staged but unreadable until a
/// read repairs it lazily, so it is worth a couple of immediate retries to
/// shrink that window.
const FINALIZE_RETRIES: u32 = 2;

/// OpenDAL-based storage backend.
///
/// The caller provides the configured OpenDAL operator. This crate only maps
/// the TUS storage operations onto OpenDAL object operations.
///
/// Cloning is cheap: `opendal::Operator` is itself a shared handle.
#[derive(Clone)]
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
    ///
    /// Trailing slashes are normalized away once here, so per-upload key
    /// construction does not have to re-trim on every call.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        self.prefix = prefix.trim_end_matches('/').to_string();
        self
    }

    /// Generates a storage key for an upload.
    fn make_key(&self, id: &str) -> String {
        if self.prefix.is_empty() {
            id.to_string()
        } else {
            format!("{}/{}", self.prefix, id)
        }
    }

    /// Opens a byte stream over the main object, repairing an interrupted
    /// finalize first.
    async fn stream_from(&self, key: &str, start: u64, end: Option<u64>) -> Result<ByteStream> {
        // A completed upload whose finalize was interrupted has no main
        // object yet; re-drive materialization before reading. Genuinely
        // incomplete uploads only have staging bytes and are not readable.
        let upload = staging::UploadObjects::new(&self.operator, key);
        if !upload.ensure_materialized().await? {
            return Err(Error::NotFound(key.to_string()));
        }

        let reader = self
            .operator
            .reader(&upload.main_key())
            .await
            .map_err(Error::storage)?;
        let stream = match end {
            Some(end) => reader.into_bytes_stream(start..end).await,
            None => reader.into_bytes_stream(start..).await,
        }
        .map_err(Error::storage)?;

        Ok(Box::pin(
            stream.map(|result| result.map_err(io::Error::other)),
        ))
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

        // A reused key must not inherit staged parts, temporary objects, or a
        // completion marker from an earlier upload; stale staged bytes would
        // otherwise splice into the new upload via append-position recovery.
        staging::UploadObjects::new(&self.operator, &key)
            .prepare_for_new_upload()
            .await?;

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
            ..
        } = request;
        let key = handle.key().to_string();
        let upload = staging::UploadObjects::new(&self.operator, &key);

        // Staging validates the offset, then streams the body into the next
        // part object without buffering it; a mid-stream failure discards the
        // partial part so a failed PATCH changes nothing.
        upload
            .append_part(&mut handle, expected_offset, data, completes_upload)
            .await?;

        // Lifecycle owns completion detection. Deferred-length uploads stay in
        // staging until the PATCH that declares and reaches the length.
        if completes_upload {
            finalize_with_retry(&upload, &key).await?;
        }

        Ok(handle)
    }

    async fn concat(&self, request: ConcatRequest) -> Result<StorageHandle> {
        let ConcatRequest { target, parts, .. } = request;
        let target_key = target.key();

        let part_keys: Vec<String> = parts.iter().map(|part| part.key().to_string()).collect();

        // A partial upload whose finalize was interrupted has its bytes fully
        // staged but no main object; repair it so concatenation can read it.
        // Genuinely missing parts still fail below when the read runs.
        for part_key in &part_keys {
            staging::UploadObjects::new(&self.operator, part_key)
                .ensure_materialized()
                .await?;
        }

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

/// Reads of stored upload bytes.
///
/// Incomplete uploads are not readable through this backend: their bytes live
/// only in staging objects, so `stream`/`stream_range` return
/// [`Error::NotFound`] until the upload completes. This diverges from
/// `FileStorage`, which serves the partial bytes written so far. Completed
/// uploads whose finalize was interrupted are repaired transparently before
/// serving.
#[async_trait]
impl tus_protocol::StorageReader for OpendalStorage {
    async fn stream(&self, handle: &StorageHandle) -> Result<ByteStream> {
        self.stream_from(handle.key(), 0, None).await
    }

    async fn stream_range(
        &self,
        handle: &StorageHandle,
        start: u64,
        end: Option<u64>,
    ) -> Result<ByteStream> {
        self.stream_from(handle.key(), start, end).await
    }
}

/// Runs `finalize` with a small number of immediate retries.
async fn finalize_with_retry(upload: &staging::UploadObjects<'_>, key: &str) -> Result<()> {
    let mut error = match upload.finalize().await {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };

    for attempt in 1..=FINALIZE_RETRIES {
        tracing::warn!(
            key,
            attempt,
            error = %error,
            "finalize failed; retrying inline"
        );
        error = match upload.finalize().await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
    }

    tracing::warn!(
        key,
        error = %error,
        "finalize failed after retries; upload stays staged until a read repairs it"
    );
    Err(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use opendal::services::{Fs, Memory};
    use tus_protocol::storage::conformance;
    use tus_protocol::{ChunkStream, Storage, StorageReader};

    struct TestStorage {
        storage: OpendalStorage,
        tempdir: tempfile::TempDir,
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
        let operator = Operator::new(Fs::default().root(tempdir.path().to_str().unwrap())).unwrap();

        TestStorage {
            storage: OpendalStorage::new(operator),
            tempdir,
        }
    }

    async fn read_all(mut stream: ByteStream) -> Vec<u8> {
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            data.extend_from_slice(&chunk.unwrap());
        }
        data
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
            .append(AppendRequest::new(handle, 0, data, true))
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
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("hello ")),
                false,
            ))
            .await
            .unwrap();

        let handle = storage
            .append(AppendRequest::new(
                handle,
                6,
                ChunkStream::from_bytes(Bytes::from("world")),
                true,
            ))
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
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("hello")),
                false,
            ))
            .await
            .unwrap();

        assert_eq!(storage.size(&handle).await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn append_advances_part_cursor() {
        let storage = create_test_storage();
        let handle = storage.create("handle-internals").await.unwrap();

        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("hello")),
                false,
            ))
            .await
            .unwrap();

        assert_eq!(handle.internal(staging::INTERNAL_NEXT_PART), Some("2"));
        assert_eq!(handle.internal(staging::INTERNAL_STAGED_SIZE), Some("5"));
    }

    #[tokio::test]
    async fn stale_expected_offset_returns_internal_error() {
        let storage = create_test_storage();

        let handle = storage.create("offset-mismatch-upload").await.unwrap();
        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("hello")),
                false,
            ))
            .await
            .unwrap();

        // Storage holds 5 bytes. The lifecycle reconciles the recorded offset
        // against size() and validates the client's offset before append runs,
        // so a divergence at append time is a server-side storage/state
        // inconsistency (500), matching FileStorage/MemoryStorage, not a
        // client-recoverable 409 conflict.
        let error = storage
            .append(AppendRequest::new(
                handle.clone(),
                3,
                ChunkStream::from_bytes(Bytes::from("xyz")),
                false,
            ))
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Internal(_)));
        assert_eq!(storage.size(&handle).await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn failed_stream_append_discards_partial_part() {
        let storage = create_test_storage();

        let handle = storage.create("failed-stream-upload").await.unwrap();
        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("hello")),
                false,
            ))
            .await
            .unwrap();

        let failing: ByteStream = Box::pin(futures::stream::iter(vec![
            Ok(Bytes::from("wor")),
            Err(io::Error::other("client went away")),
        ]));
        let error = storage
            .append(AppendRequest::new(
                handle.clone(),
                5,
                ChunkStream::from_stream(failing),
                false,
            ))
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Io(_)));
        // The failed PATCH must leave no partial staged part behind: the
        // offset is unchanged and the next part object does not exist.
        assert_eq!(storage.size(&handle).await.unwrap(), Some(5));
        let part2 = format!("{}/parts/{:010}", handle.key(), 2);
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
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("hello")),
                false,
            ))
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
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("hello")),
                false,
            ))
            .await
            .unwrap();

        storage
            .operator
            .write(&format!("{}/data", handle.key()), Bytes::from("he"))
            .await
            .unwrap();

        assert_eq!(storage.size(&handle).await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn staged_size_on_memory_backend_falls_back_to_stat() {
        // The memory service's listings do not include content lengths, so
        // size() must fall back to stat-ing the parts it cannot size from the
        // listing alone.
        let operator = Operator::new(Memory::default()).unwrap();
        let storage = OpendalStorage::new(operator);

        let handle = storage.create("memory-upload").await.unwrap();
        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("hello ")),
                false,
            ))
            .await
            .unwrap();
        let handle = storage
            .append(AppendRequest::new(
                handle,
                6,
                ChunkStream::from_bytes(Bytes::from("world")),
                false,
            ))
            .await
            .unwrap();

        assert_eq!(storage.size(&handle).await.unwrap(), Some(11));
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
            .append(AppendRequest::new(
                active_handle,
                0,
                ChunkStream::from_bytes(Bytes::from("hello")),
                false,
            ))
            .await
            .unwrap();

        let recovered_offset = storage.size(&recovered_handle).await.unwrap().unwrap();

        let recovered_handle = storage
            .append(AppendRequest::new(
                recovered_handle,
                recovered_offset,
                ChunkStream::from_bytes(Bytes::from("world")),
                true,
            ))
            .await
            .unwrap();

        let data = read_all(storage.stream(&recovered_handle).await.unwrap()).await;

        assert_eq!(data, b"helloworld");
    }

    #[tokio::test]
    async fn test_get_stream() {
        let storage = create_test_storage();

        let handle = storage.create("test-upload-3").await.unwrap();
        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("test content")),
                true,
            ))
            .await
            .unwrap();

        let data = read_all(storage.stream(&handle).await.unwrap()).await;

        assert_eq!(String::from_utf8(data).unwrap(), "test content");
    }

    #[tokio::test]
    async fn stream_of_incomplete_upload_returns_not_found() {
        // Incomplete uploads only have staging bytes; unlike FileStorage
        // (which serves partial bytes), this backend reports NotFound until
        // the upload completes.
        let storage = create_test_storage();

        let handle = storage.create("incomplete-read").await.unwrap();
        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("hello")),
                false,
            ))
            .await
            .unwrap();

        assert!(matches!(
            storage.stream(&handle).await.map(|_| ()),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            storage.stream_range(&handle, 0, Some(2)).await.map(|_| ()),
            Err(Error::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn interrupted_finalize_is_repaired_on_stream() {
        let storage = create_test_storage();

        let handle = storage.create("stranded-upload").await.unwrap();
        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("hello ")),
                false,
            ))
            .await
            .unwrap();

        // Block promotion: a directory at the deliverable key makes the fs
        // service's rename fail, so the completing append stages its part
        // durably but every finalize attempt (including the inline retries)
        // fails.
        storage
            .operator
            .create_dir("stranded-upload/data/")
            .await
            .unwrap();

        let error = storage
            .append(AppendRequest::new(
                handle.clone(),
                6,
                ChunkStream::from_bytes(Bytes::from("world")),
                true,
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Storage(_)));

        // Promotion becomes possible again; the read path must repair the
        // stranded upload instead of returning NotFound forever.
        storage
            .operator
            .delete("stranded-upload/data/")
            .await
            .unwrap();

        // HEAD reconciliation reads size(), sees all bytes accounted for, and
        // marks the upload complete even though no main object exists yet.
        assert_eq!(storage.size(&handle).await.unwrap(), Some(11));

        let body = read_all(storage.stream(&handle).await.unwrap()).await;
        assert_eq!(body, b"hello world");

        let range = read_all(storage.stream_range(&handle, 6, None).await.unwrap()).await;
        assert_eq!(range, b"world");

        // The repaired upload stays consistent for follow-up reads.
        assert_eq!(storage.size(&handle).await.unwrap(), Some(11));
    }

    #[tokio::test]
    async fn concat_repairs_stranded_partial_upload() {
        let storage = create_test_storage();

        // Strand a partial upload: its completing append stages everything
        // but finalize cannot promote the main object.
        let stranded = storage.create("stranded-part").await.unwrap();
        storage
            .operator
            .create_dir("stranded-part/data/")
            .await
            .unwrap();
        let error = storage
            .append(AppendRequest::new(
                stranded.clone(),
                0,
                ChunkStream::from_bytes(Bytes::from("Hello ")),
                true,
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Storage(_)));
        storage
            .operator
            .delete("stranded-part/data/")
            .await
            .unwrap();

        let healthy = storage.create("healthy-part").await.unwrap();
        let healthy = storage
            .append(AppendRequest::new(
                healthy,
                0,
                ChunkStream::from_bytes(Bytes::from("World")),
                true,
            ))
            .await
            .unwrap();

        let target = storage.create("concat-repair-target").await.unwrap();
        let target = storage
            .concat(ConcatRequest::new(target, vec![stranded, healthy]))
            .await
            .unwrap();

        let body = read_all(storage.stream(&target).await.unwrap()).await;
        assert_eq!(body, b"Hello World");
    }

    #[tokio::test]
    async fn test_concat() {
        let storage = create_test_storage();

        // Create parts; each declares its length so finalize runs.
        let part1 = storage.create("part1").await.unwrap();
        let part1 = storage
            .append(AppendRequest::new(
                part1,
                0,
                ChunkStream::from_bytes(Bytes::from("Hello ")),
                true,
            ))
            .await
            .unwrap();

        let part2 = storage.create("part2").await.unwrap();
        let part2 = storage
            .append(AppendRequest::new(
                part2,
                0,
                ChunkStream::from_bytes(Bytes::from("World")),
                true,
            ))
            .await
            .unwrap();

        // Create target and concat
        let target = storage.create("final").await.unwrap();
        let target = storage
            .concat(ConcatRequest::new(target, vec![part1, part2]))
            .await
            .unwrap();

        // Read result
        let data = read_all(storage.stream(&target).await.unwrap()).await;

        assert_eq!(String::from_utf8(data).unwrap(), "Hello World");
    }

    #[tokio::test]
    async fn failed_concat_does_not_expose_partial_target() {
        let storage = create_test_storage();

        let part = storage.create("concat-good-part").await.unwrap();
        let part = storage
            .append(AppendRequest::new(
                part,
                0,
                ChunkStream::from_bytes(Bytes::from("Hello ")),
                true,
            ))
            .await
            .unwrap();

        let missing_part = storage.create("concat-missing-part").await.unwrap();
        let target = storage.create("concat-failed-target").await.unwrap();
        let error = storage
            .concat(ConcatRequest::new(target.clone(), vec![part, missing_part]))
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
            .append(AppendRequest::new(
                part,
                0,
                ChunkStream::from_bytes(Bytes::from("part")),
                true,
            ))
            .await
            .unwrap();
        let mut target = storage.create("target-internals").await.unwrap();
        target.set_internal("target_fact", "keep-me");

        let target = storage
            .concat(ConcatRequest::new(target, vec![part]))
            .await
            .unwrap();

        assert_eq!(target.internal("target_fact"), Some("keep-me"));
    }

    #[tokio::test]
    async fn test_delete() {
        let storage = create_test_storage();

        let handle = storage.create("test-delete").await.unwrap();
        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("hello")),
                true,
            ))
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
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("abc")),
                false,
            ))
            .await
            .unwrap();

        assert_eq!(storage.size(&handle).await.unwrap(), Some(3));

        storage.delete(&handle).await.unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_propagates_staging_cleanup_failures() {
        use std::os::unix::fs::PermissionsExt;

        let storage = create_test_storage();

        let handle = storage.create("undeletable-upload").await.unwrap();
        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("abc")),
                false,
            ))
            .await
            .unwrap();

        // Make the staged part undeletable; DELETE must surface the failure
        // (so the client can retry) instead of reporting success while the
        // bytes remain orphaned.
        let parts_dir = storage.tempdir.path().join("undeletable-upload/parts");
        std::fs::set_permissions(&parts_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        // Root (as in CI containers) and some filesystems ignore permission
        // bits, so the read-only directory would not actually block deletion.
        // When the injection can't take effect, the failure path is untestable
        // here; restore the mode and skip rather than assert a failure that
        // can't happen.
        if std::fs::File::create(parts_dir.join(".probe")).is_ok() {
            let _ = std::fs::remove_file(parts_dir.join(".probe"));
            std::fs::set_permissions(&parts_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
            eprintln!(
                "skipping delete_propagates_staging_cleanup_failures: \
                 permission bits are not enforced (running as root?)"
            );
            return;
        }

        let error = storage.delete(&handle).await.unwrap_err();
        assert!(matches!(error, Error::Storage(_)));

        // Once the failure clears, the retried DELETE succeeds and removes
        // everything.
        std::fs::set_permissions(&parts_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        storage.delete(&handle).await.unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), None);
    }

    #[tokio::test]
    async fn create_removes_stale_staging_objects_at_reused_key() {
        let storage = create_test_storage();

        // Leftovers from a previous upload at the same key: staged parts, a
        // temporary object, and a completion marker.
        storage
            .operator
            .write("reused/parts/0000000001", Bytes::from("stale"))
            .await
            .unwrap();
        storage
            .operator
            .write("reused/tmp/finalize-deadbeef", Bytes::from("stale-tmp"))
            .await
            .unwrap();
        storage
            .operator
            .write("reused/complete", Bytes::from("1"))
            .await
            .unwrap();

        let handle = storage.create("reused").await.unwrap();

        // The fresh upload starts empty and stale bytes never splice in.
        assert_eq!(storage.size(&handle).await.unwrap(), None);

        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("fresh")),
                true,
            ))
            .await
            .unwrap();

        assert_eq!(storage.size(&handle).await.unwrap(), Some(5));
        let body = read_all(storage.stream(&handle).await.unwrap()).await;
        assert_eq!(body, b"fresh");
    }

    #[tokio::test]
    async fn create_skips_cleanup_sweep_for_fresh_key() {
        // Upload ids are UUIDs, so the common path is a genuinely-fresh key.
        // create() must not pay the mutating delete+list sweep there: an inert
        // leftover temp object (never read as upload data) is left untouched by
        // the fast path, proving the sweep did not run. A stale marker/main/
        // staged part would instead trigger it.
        let storage = create_test_storage();
        storage
            .operator
            .write("fresh-key/tmp/orphan-abcdef", Bytes::from("inert"))
            .await
            .unwrap();

        let handle = storage.create("fresh-key").await.unwrap();

        // The fast path skipped the sweep, so the inert temp object survives.
        assert!(
            storage
                .operator
                .stat("fresh-key/tmp/orphan-abcdef")
                .await
                .is_ok()
        );

        // The fresh upload still works and never inherits stale bytes.
        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("data")),
                true,
            ))
            .await
            .unwrap();
        let body = read_all(storage.stream(&handle).await.unwrap()).await;
        assert_eq!(body, b"data");

        // Delete reclaims leftover temp objects. The completing append above
        // finalized the upload and already swept the original orphan, so seed a
        // fresh leftover temp object to exercise delete's cleanup directly.
        storage
            .operator
            .write("fresh-key/tmp/orphan-ghijkl", Bytes::from("inert"))
            .await
            .unwrap();
        storage.delete(&handle).await.unwrap();
        assert!(matches!(
            storage.operator.stat("fresh-key/tmp/orphan-ghijkl").await,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound
        ));
    }

    #[tokio::test]
    async fn dot_suffixed_sibling_ids_do_not_corrupt_each_other() {
        // Upload ids may contain dots, so `report` and `report.complete` are
        // both valid, distinct ids. Under the previous dot-suffixed staging
        // layout `report`'s completion marker lived at `report.complete` (the
        // exact key holding upload `report.complete`'s main object), so
        // finalizing `report` (which writes then deletes that marker) would
        // clobber the sibling upload's data. The `/`-nested layout gives each
        // upload a disjoint `{id}/` keyspace (ids can never contain `/`), so
        // they cannot alias.
        let storage = create_test_storage();

        // Complete the sibling first so its main object is already in place
        // when the colliding upload runs its completing append.
        let sibling = storage.create("report.complete").await.unwrap();
        let sibling = storage
            .append(AppendRequest::new(
                sibling,
                0,
                ChunkStream::from_bytes(Bytes::from("sibling body")),
                true,
            ))
            .await
            .unwrap();

        let report = storage.create("report").await.unwrap();
        let report = storage
            .append(AppendRequest::new(
                report,
                0,
                ChunkStream::from_bytes(Bytes::from("REPORT BODY")),
                true,
            ))
            .await
            .unwrap();

        // Both uploads keep their own bytes; neither clobbered the other.
        assert_eq!(
            read_all(storage.stream(&report).await.unwrap()).await,
            b"REPORT BODY"
        );
        assert_eq!(
            read_all(storage.stream(&sibling).await.unwrap()).await,
            b"sibling body"
        );

        // Deleting one upload must not touch the other's keyspace.
        storage.delete(&report).await.unwrap();
        assert_eq!(storage.size(&report).await.unwrap(), None);
        assert_eq!(
            read_all(storage.stream(&sibling).await.unwrap()).await,
            b"sibling body"
        );
        assert_eq!(storage.size(&sibling).await.unwrap(), Some(12));
    }

    #[tokio::test]
    async fn test_with_prefix() {
        let storage = create_test_storage().with_prefix("uploads/2024");

        let handle = storage.create("test-id").await.unwrap();

        assert!(handle.key().starts_with("uploads/2024/"));
    }
}
