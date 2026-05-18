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
//! main key in part-number order and the staging prefix is removed.

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use opendal::Operator;
use std::io;

use tus_protocol::{ByteStream, ChunkStream, Error, Result, UploadState};

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

    /// Streams a staging part into an already-open writer.
    async fn copy_part_into_writer(
        &self,
        part_key: &str,
        writer: &mut opendal::Writer,
    ) -> Result<()> {
        let reader = self
            .operator
            .reader(part_key)
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

    /// Concatenates all staging parts into the main key and removes them.
    async fn finalize(&self, key: &str) -> Result<()> {
        let part_keys = self.list_parts(key).await?;
        let mut writer = self.operator.writer(key).await.map_err(Error::storage)?;
        for part_key in &part_keys {
            self.copy_part_into_writer(part_key, &mut writer).await?;
        }
        writer.close().await.map_err(Error::storage)?;

        for part_key in &part_keys {
            let _ = self.operator.delete(part_key).await;
        }
        let _ = self.operator.delete(&Self::parts_prefix(key)).await;

        Ok(())
    }

    fn next_part(state: &UploadState) -> u64 {
        state
            .get_internal(INTERNAL_NEXT_PART)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1)
    }

    fn part_number(part_key: &str) -> Option<u64> {
        part_key.rsplit('/').next()?.parse::<u64>().ok()
    }

    async fn next_part_for_append(&self, state: &UploadState, key: &str) -> Result<u64> {
        let candidate = Self::next_part(state);
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

    async fn create(&self, state: &mut UploadState) -> Result<String> {
        let key = self.make_key(state.id());
        state.set_storage_key(key.clone());
        state.set_internal(INTERNAL_NEXT_PART, "1");
        Ok(key)
    }

    async fn append(&self, state: &mut UploadState, data: ChunkStream) -> Result<u64> {
        let key = state
            .storage_key()
            .ok_or(Error::StorageKeyMissing)?
            .to_string();

        // Collect the chunk into bytes. OpenDAL's writer accepts Bytes, and
        // a single staging-object PUT must be atomic; we can't split it.
        // Callers bound this by `config.max_chunk_size`.
        let bytes = collect_chunk_stream(data).await?;
        let incoming_len = bytes.len() as u64;

        let part_number = self.next_part_for_append(state, &key).await?;
        let part_key = Self::part_key(&key, part_number);

        self.operator
            .write(&part_key, bytes)
            .await
            .map_err(Error::storage)?;

        state.set_internal(INTERNAL_NEXT_PART, (part_number + 1).to_string());
        let new_offset = state.offset().saturating_add(incoming_len);
        state.set_offset(new_offset);

        // Finalize once the declared length is reached. Deferred-length
        // uploads stay in staging until the client declares a length via
        // a subsequent PATCH.
        if let Some(length) = state.length()
            && new_offset >= length
        {
            self.finalize(&key).await?;
        }

        Ok(new_offset)
    }

    async fn get_stream(&self, state: &UploadState) -> Result<ByteStream> {
        let key = state.storage_key().ok_or(Error::StorageKeyMissing)?;

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
        state: &UploadState,
        start: u64,
        end: Option<u64>,
    ) -> Result<ByteStream> {
        let key = state.storage_key().ok_or(Error::StorageKeyMissing)?;

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

    async fn concat(&self, target: &mut UploadState, parts: Vec<UploadState>) -> Result<()> {
        let target_key = target.storage_key().ok_or(Error::StorageKeyMissing)?;

        let mut writer = self
            .operator
            .writer(target_key)
            .await
            .map_err(Error::storage)?;

        for part in &parts {
            let part_key = part.storage_key().ok_or(Error::StorageKeyMissing)?;

            // Each partial is itself a finalized OpenDAL object (its own
            // main key). Stream its bytes into the target writer without
            // buffering the whole thing in memory.
            let reader = self
                .operator
                .reader(part_key)
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
        }

        writer.close().await.map_err(Error::storage)?;

        Ok(())
    }

    async fn delete(&self, state: &UploadState) -> Result<()> {
        if let Some(key) = state.storage_key() {
            // Best-effort cleanup of any staging parts left over from an
            // unfinished upload.
            if let Ok(parts) = self.list_parts(key).await {
                for p in parts {
                    let _ = self.operator.delete(&p).await;
                }
                let _ = self.operator.delete(&Self::parts_prefix(key)).await;
            }

            match self.operator.delete(key).await {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(Error::storage(e)),
            }
        } else {
            Ok(())
        }
    }

    async fn size(&self, state: &UploadState) -> Result<Option<u64>> {
        let key = match state.storage_key() {
            Some(k) => k,
            None => return Ok(None),
        };

        // Prefer the main key's size (post-finalize). If the main key
        // doesn't exist yet, sum the staged parts so recovery can reconcile
        // state even after a crash before the offset update was persisted.
        match self.operator.stat(key).await {
            Ok(stat) => Ok(Some(stat.content_length())),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => self.staged_size(key).await,
            Err(e) => Err(Error::storage(e)),
        }
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
    use opendal::services::Memory;
    use tus_protocol::Storage as StorageBackend;

    fn create_test_storage() -> Storage {
        let operator = Operator::new(Memory::default()).unwrap().finish();
        Storage::new(operator, "")
    }

    #[tokio::test]
    async fn test_create_and_append() {
        let storage = create_test_storage();
        let mut state = UploadState::new("test-upload").with_length(11);

        // Create
        let key = storage.create(&mut state).await.unwrap();
        assert!(!key.is_empty());

        // Append the full upload in one shot; finalizes because offset
        // reaches the declared length.
        let data = ChunkStream::from_bytes(Bytes::from("hello world"));
        let offset = storage.append(&mut state, data).await.unwrap();
        assert_eq!(offset, 11);

        // After finalize the main key holds the bytes.
        assert_eq!(storage.size(&state).await.unwrap(), Some(11));
    }

    #[tokio::test]
    async fn test_multiple_appends() {
        let storage = create_test_storage();
        let mut state = UploadState::new("test-upload-2").with_length(11);

        storage.create(&mut state).await.unwrap();

        storage
            .append(&mut state, ChunkStream::from_bytes(Bytes::from("hello ")))
            .await
            .unwrap();

        let offset = storage
            .append(&mut state, ChunkStream::from_bytes(Bytes::from("world")))
            .await
            .unwrap();

        assert_eq!(offset, 11);
        assert_eq!(storage.size(&state).await.unwrap(), Some(11));
    }

    #[tokio::test]
    async fn test_deferred_length_size_reports_offset() {
        // Without a declared length, we never finalize, but size() should
        // still report the staged bytes so HEAD responses are useful.
        let storage = create_test_storage();
        let mut state = UploadState::new("deferred-upload");

        storage.create(&mut state).await.unwrap();
        storage
            .append(&mut state, ChunkStream::from_bytes(Bytes::from("hello")))
            .await
            .unwrap();

        assert_eq!(storage.size(&state).await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn test_deferred_length_size_uses_staged_bytes_not_state_offset() {
        let storage = create_test_storage();
        let mut state = UploadState::new("stale-offset-upload");

        storage.create(&mut state).await.unwrap();
        storage
            .append(&mut state, ChunkStream::from_bytes(Bytes::from("hello")))
            .await
            .unwrap();

        state.set_offset(0);
        assert_eq!(storage.size(&state).await.unwrap(), Some(5));

        state.set_offset(99);
        assert_eq!(storage.size(&state).await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn append_with_stale_part_cursor_does_not_overwrite_existing_staged_part() {
        let storage = create_test_storage();
        let mut active_state = UploadState::new("recovered-upload").with_length(10);

        storage.create(&mut active_state).await.unwrap();

        // Simulates the state persisted before a crash: create() stored the
        // first part cursor, then the process crashed after writing part 1
        // but before persisting the incremented cursor and offset.
        let mut recovered_state = active_state.clone();

        storage
            .append(
                &mut active_state,
                ChunkStream::from_bytes(Bytes::from("hello")),
            )
            .await
            .unwrap();

        let recovered_offset = storage.size(&recovered_state).await.unwrap().unwrap();
        recovered_state.set_offset(recovered_offset);

        storage
            .append(
                &mut recovered_state,
                ChunkStream::from_bytes(Bytes::from("world")),
            )
            .await
            .unwrap();

        let mut stream = storage.get_stream(&recovered_state).await.unwrap();
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            data.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(data, b"helloworld");
    }

    #[tokio::test]
    async fn test_get_stream() {
        let storage = create_test_storage();
        let mut state = UploadState::new("test-upload-3").with_length(12);

        storage.create(&mut state).await.unwrap();
        storage
            .append(
                &mut state,
                ChunkStream::from_bytes(Bytes::from("test content")),
            )
            .await
            .unwrap();

        let mut stream = storage.get_stream(&state).await.unwrap();
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
        let mut part1 = UploadState::new("part1").with_length(6);
        storage.create(&mut part1).await.unwrap();
        storage
            .append(&mut part1, ChunkStream::from_bytes(Bytes::from("Hello ")))
            .await
            .unwrap();

        let mut part2 = UploadState::new("part2").with_length(5);
        storage.create(&mut part2).await.unwrap();
        storage
            .append(&mut part2, ChunkStream::from_bytes(Bytes::from("World")))
            .await
            .unwrap();

        // Create target and concat
        let mut target = UploadState::new("final");
        storage.create(&mut target).await.unwrap();
        storage
            .concat(&mut target, vec![part1, part2])
            .await
            .unwrap();

        // Read result
        let mut stream = storage.get_stream(&target).await.unwrap();
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            data.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(String::from_utf8(data).unwrap(), "Hello World");
    }

    #[tokio::test]
    async fn test_delete() {
        let storage = create_test_storage();
        let mut state = UploadState::new("test-delete").with_length(5);

        storage.create(&mut state).await.unwrap();
        storage
            .append(&mut state, ChunkStream::from_bytes(Bytes::from("hello")))
            .await
            .unwrap();
        assert!(storage.size(&state).await.unwrap().is_some());

        storage.delete(&state).await.unwrap();
        assert!(storage.size(&state).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_cleans_up_staging() {
        // Staging parts from an un-finalized upload should also be removed
        // on delete.
        let storage = create_test_storage();
        let mut state = UploadState::new("stale-upload"); // no length → never finalizes

        storage.create(&mut state).await.unwrap();
        storage
            .append(&mut state, ChunkStream::from_bytes(Bytes::from("abc")))
            .await
            .unwrap();

        let key = state.storage_key().unwrap().to_string();
        assert!(!storage.list_parts(&key).await.unwrap().is_empty());

        storage.delete(&state).await.unwrap();
        assert!(storage.list_parts(&key).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_with_prefix() {
        let storage = create_test_storage().with_prefix("uploads/2024");

        let mut state = UploadState::new("test-id");
        let key = storage.create(&mut state).await.unwrap();

        assert!(key.starts_with("uploads/2024/"));
    }
}
