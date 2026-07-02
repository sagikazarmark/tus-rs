//! OpenDAL storage implementation.
//!
//! This storage backend wraps a caller-provided Apache OpenDAL `Operator`.
//! Configure the operator in application code, then pass it to [`Storage::new`].
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

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use opendal::Operator;
use std::io;

use tus_protocol::{
    AppendRequest, ByteStream, ChunkStream, ConcatRequest, Error, Result, StorageHandle,
};

const INTERNAL_NEXT_PART: &str = "opendal_next_part";

/// OpenDAL-based storage backend.
///
/// The caller provides the configured OpenDAL operator. This crate only maps
/// the TUS storage operations onto OpenDAL object operations.
pub struct Storage {
    operator: Operator,
    prefix: String,
}

impl Storage {
    /// Creates a new OpenDAL storage with the given operator and storage-key prefix.
    pub fn new(operator: Operator, prefix: impl Into<String>) -> Self {
        Self {
            operator,
            prefix: prefix.into(),
        }
    }

    /// Sets the prefix for storage keys.
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

    /// Directory prefix under which staging parts for an upload live.
    fn parts_prefix(key: &str) -> String {
        format!("{}.parts/", key)
    }

    /// Generates the staging key for a particular part number. Zero-padded
    /// so lexicographic listing returns parts in order.
    fn part_key(key: &str, part_number: u64) -> String {
        format!("{}.parts/{:010}", key, part_number)
    }

    /// Directory prefix under which temporary materializations live.
    fn temp_prefix(key: &str) -> String {
        format!("{}.tmp/", key)
    }

    /// Generates a unique temporary key for materializing an object.
    fn temp_key(key: &str, purpose: &str) -> String {
        format!(
            "{}{}-{}",
            Self::temp_prefix(key),
            purpose,
            uuid::Uuid::new_v4().simple()
        )
    }

    /// Streams an object into an already-open writer.
    async fn copy_object_into_writer(
        &self,
        source_key: &str,
        writer: &mut opendal::Writer,
    ) -> Result<()> {
        let reader = self
            .operator
            .reader(source_key)
            .await
            .map_err(Error::storage)?;
        let mut stream = reader
            .into_bytes_stream(0..)
            .await
            .map_err(Error::storage)?;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(Error::storage)?;
            writer.write(chunk).await.map_err(Error::storage)?;
        }
        Ok(())
    }

    /// Streams source objects into `target_key`, replacing it only when closed.
    async fn write_objects_to_key(&self, target_key: &str, source_keys: &[String]) -> Result<()> {
        let mut writer = self
            .operator
            .writer(target_key)
            .await
            .map_err(Error::storage)?;
        for source_key in source_keys {
            self.copy_object_into_writer(source_key, &mut writer)
                .await?;
        }
        writer.close().await.map(|_| ()).map_err(Error::storage)
    }

    /// Promotes a complete temporary object to its public target key.
    async fn promote_temp(&self, temp_key: &str, target_key: &str) -> Result<()> {
        let capability = self.operator.info().full_capability();

        if capability.rename {
            return self
                .operator
                .rename(temp_key, target_key)
                .await
                .map_err(Error::storage);
        }

        if capability.copy {
            self.operator
                .copy(temp_key, target_key)
                .await
                .map_err(Error::storage)?;
            let _ = self.operator.delete(temp_key).await;
            return Ok(());
        }

        Err(Error::storage(
            opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "OpenDAL service must support rename or copy to promote materialized uploads",
            )
            .with_operation("tus_storage_opendal::Storage::promote_temp")
            .with_context("service", self.operator.info().scheme()),
        ))
    }

    /// Lists the staging parts for an upload, sorted by part number
    /// (ascending), returning their storage keys.
    async fn list_parts(&self, key: &str) -> Result<Vec<String>> {
        let prefix = Self::parts_prefix(key);
        let entries = match self.operator.list(&prefix).await {
            Ok(e) => e,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::storage(e)),
        };
        let mut part_keys: Vec<String> = entries
            .into_iter()
            .filter(|e| e.metadata().is_file())
            .map(|e| e.path().to_string())
            .collect();
        // Zero-padded part numbers mean lex sort == numeric sort.
        part_keys.sort();
        Ok(part_keys)
    }

    /// Returns the accumulated size of all staged parts for an upload.
    async fn staged_size(&self, key: &str) -> Result<Option<u64>> {
        let part_keys = self.list_parts(key).await?;
        if part_keys.is_empty() {
            return Ok(None);
        }

        let mut total = 0_u64;
        for part_key in part_keys {
            let stat = self
                .operator
                .stat(&part_key)
                .await
                .map_err(Error::storage)?;
            total = total.saturating_add(stat.content_length());
        }

        Ok(Some(total))
    }

    /// Lists leftover temporary materialization objects for an upload.
    async fn list_temp_objects(&self, key: &str) -> Result<Vec<String>> {
        let prefix = Self::temp_prefix(key);
        let entries = match self.operator.list(&prefix).await {
            Ok(e) => e,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::storage(e)),
        };
        Ok(entries
            .into_iter()
            .filter(|e| e.metadata().is_file())
            .map(|e| e.path().to_string())
            .collect())
    }

    /// Concatenates all staging parts into the main key and removes them.
    async fn finalize(&self, key: &str) -> Result<()> {
        let part_keys = self.list_parts(key).await?;
        let temp_key = Self::temp_key(key, "finalize");

        if let Err(error) = self.write_objects_to_key(&temp_key, &part_keys).await {
            let _ = self.operator.delete(&temp_key).await;
            return Err(error);
        }

        if let Err(error) = self.promote_temp(&temp_key, key).await {
            let _ = self.operator.delete(&temp_key).await;
            return Err(error);
        }

        for part_key in &part_keys {
            let _ = self.operator.delete(part_key).await;
        }
        let _ = self.operator.delete(&Self::parts_prefix(key)).await;
        let _ = self.operator.delete(&Self::temp_prefix(key)).await;

        Ok(())
    }

    fn next_part(handle: &StorageHandle) -> u64 {
        handle
            .get_internal(INTERNAL_NEXT_PART)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1)
    }

    fn part_number(part_key: &str) -> Option<u64> {
        part_key.rsplit('/').next()?.parse::<u64>().ok()
    }

    async fn next_part_for_append(&self, handle: &StorageHandle, key: &str) -> Result<u64> {
        let candidate = Self::next_part(handle);
        let candidate_key = Self::part_key(key, candidate);

        match self.operator.stat(&candidate_key).await {
            Ok(_) => {
                let next_existing = self
                    .list_parts(key)
                    .await?
                    .iter()
                    .filter_map(|part_key| Self::part_number(part_key))
                    .max()
                    .unwrap_or(0)
                    .saturating_add(1);

                Ok(candidate.max(next_existing))
            }
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(candidate),
            Err(e) => Err(Error::storage(e)),
        }
    }
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field("prefix", &self.prefix)
            .finish()
    }
}

#[async_trait]
impl tus_protocol::Storage for Storage {
    fn name(&self) -> &'static str {
        "opendal"
    }

    async fn create(&self, upload_id: &str) -> Result<StorageHandle> {
        let key = self.make_key(upload_id);
        let mut handle = StorageHandle::new(key);
        handle.set_internal(INTERNAL_NEXT_PART, "1");
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

        let current_size = self.size(&handle).await?.unwrap_or(0);
        if current_size != expected_offset {
            return Err(Error::Internal(format!(
                "opendal storage size {current_size} does not match expected offset {expected_offset} for key {key}"
            )));
        }

        // Collect the chunk into bytes. OpenDAL's writer accepts Bytes, and
        // a single staging-object PUT must be atomic; we can't split it.
        // Callers bound this by `config.max_chunk_size`.
        let bytes = collect_chunk_stream(data).await?;

        let part_number = self.next_part_for_append(&handle, &key).await?;
        let part_key = Self::part_key(&key, part_number);

        self.operator
            .write(&part_key, bytes)
            .await
            .map_err(Error::storage)?;

        handle.set_internal(INTERNAL_NEXT_PART, (part_number + 1).to_string());

        // Lifecycle owns completion detection. Deferred-length uploads stay in
        // staging until the PATCH that declares and reaches the length.
        if completes_upload {
            self.finalize(&key).await?;
        }

        Ok(handle)
    }

    async fn concat(&self, request: ConcatRequest) -> Result<StorageHandle> {
        let ConcatRequest { target, parts } = request;
        let target_key = target.key();

        let part_keys: Vec<String> = parts.iter().map(|part| part.key().to_string()).collect();
        let temp_key = Self::temp_key(target_key, "concat");

        if let Err(error) = self.write_objects_to_key(&temp_key, &part_keys).await {
            let _ = self.operator.delete(&temp_key).await;
            return Err(error);
        }

        if let Err(error) = self.promote_temp(&temp_key, target_key).await {
            let _ = self.operator.delete(&temp_key).await;
            return Err(error);
        }

        Ok(target)
    }

    async fn delete(&self, handle: &StorageHandle) -> Result<()> {
        let key = handle.key();

        // Best-effort cleanup of any staging parts left over from an
        // unfinished upload.
        if let Ok(parts) = self.list_parts(key).await {
            for p in parts {
                let _ = self.operator.delete(&p).await;
            }
            let _ = self.operator.delete(&Self::parts_prefix(key)).await;
        }

        if let Ok(temp_objects) = self.list_temp_objects(key).await {
            for temp_key in temp_objects {
                let _ = self.operator.delete(&temp_key).await;
            }
            let _ = self.operator.delete(&Self::temp_prefix(key)).await;
        }

        match self.operator.delete(key).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::storage(e)),
        }
    }

    async fn size(&self, handle: &StorageHandle) -> Result<Option<u64>> {
        let key = handle.key();

        let staged_size = self.staged_size(key).await?;

        // If both main and staged objects exist, use the larger size. This is
        // safe for recovery from old direct-to-main finalize failures (partial
        // main object plus complete staged parts) and from crashes after temp
        // promotion but before all staged parts were cleaned up.
        match self.operator.stat(key).await {
            Ok(stat) => Ok(Some(match staged_size {
                Some(staged_size) => staged_size.max(stat.content_length()),
                None => stat.content_length(),
            })),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(staged_size),
            Err(e) => Err(Error::storage(e)),
        }
    }
}

#[async_trait]
impl tus_protocol::StorageReader for Storage {
    async fn get_stream(&self, handle: &StorageHandle) -> Result<ByteStream> {
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

    async fn get_range(
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

/// Collects a `ChunkStream` into a contiguous `Bytes` buffer.
async fn collect_chunk_stream(data: ChunkStream) -> Result<Bytes> {
    match data {
        ChunkStream::Buffered(b) => Ok(b),
        ChunkStream::Stream(mut stream) => {
            let mut out: Vec<u8> = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(Error::Io)?;
                out.extend_from_slice(&chunk);
            }
            Ok(Bytes::from(out))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::services::Fs;
    use tus_protocol::storage::conformance;
    use tus_protocol::{Storage as StorageBackend, StorageReader};

    struct TestStorage {
        storage: Storage,
        _tempdir: tempfile::TempDir,
    }

    impl TestStorage {
        fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
            self.storage.prefix = prefix.into();
            self
        }
    }

    impl std::ops::Deref for TestStorage {
        type Target = Storage;

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
            storage: Storage::new(operator, ""),
            _tempdir: tempdir,
        }
    }

    #[tokio::test]
    async fn storage_conformance() {
        let storage = create_test_storage();

        conformance::assert_full_semantics(&storage.storage).await;
    }

    #[tokio::test]
    async fn test_create_and_append() {
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
        assert!(storage.list_parts(handle.key()).await.unwrap().is_empty());
        assert!(
            storage
                .list_temp_objects(handle.key())
                .await
                .unwrap()
                .is_empty()
        );
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

        assert_eq!(handle.get_internal(INTERNAL_NEXT_PART), Some("2"));
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

        handle.set_internal(INTERNAL_NEXT_PART, "99");
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

        let mut stream = storage.get_stream(&recovered_handle).await.unwrap();
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

        let mut stream = storage.get_stream(&handle).await.unwrap();
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
        let mut stream = storage.get_stream(&target).await.unwrap();
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            data.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(String::from_utf8(data).unwrap(), "Hello World");
        assert!(
            storage
                .list_temp_objects(target.key())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn failed_concat_does_not_expose_partial_target_or_leave_temp_object() {
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
        let target_key = target.key().to_string();

        let error = storage
            .concat(ConcatRequest {
                target: target.clone(),
                parts: vec![part, missing_part],
            })
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Storage(_)));
        assert_eq!(storage.size(&target).await.unwrap(), None);
        assert!(
            storage
                .list_temp_objects(&target_key)
                .await
                .unwrap()
                .is_empty()
        );
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

        assert_eq!(target.get_internal("target_fact"), Some("keep-me"));
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

        let key = handle.key().to_string();
        assert!(!storage.list_parts(&key).await.unwrap().is_empty());

        storage.delete(&handle).await.unwrap();
        assert!(storage.list_parts(&key).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_with_prefix() {
        let storage = create_test_storage().with_prefix("uploads/2024");

        let handle = storage.create("test-id").await.unwrap();

        assert!(handle.key().starts_with("uploads/2024/"));
    }
}
