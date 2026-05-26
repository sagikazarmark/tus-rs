//! In-memory storage implementation.
//!
//! This storage backend keeps all data in memory. Useful for testing
//! and development, but data is lost when the process exits.

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::error::{Error, Result};
use crate::state::UploadState;
use crate::storage::ByteStream;
use crate::storage::{ChunkStream, Storage};

/// In-memory storage backend.
///
/// Stores all upload data in a HashMap protected by a RwLock.
/// Thread-safe and suitable for single-process use.
pub struct MemoryStorage {
    data: RwLock<HashMap<String, BytesMut>>,
}

impl MemoryStorage {
    /// Creates a new empty memory storage.
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    /// Returns the number of uploads currently stored.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.data.read().unwrap().len()
    }

    /// Returns true if no uploads are stored.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.data.read().unwrap().is_empty()
    }

    /// Gets the data for an upload (for testing).
    #[cfg(test)]
    pub fn get_data(&self, key: &str) -> Option<Bytes> {
        self.data
            .read()
            .unwrap()
            .get(key)
            .map(|b| b.clone().freeze())
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MemoryStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.data.read().unwrap();
        f.debug_struct("MemoryStorage")
            .field("uploads", &data.len())
            .finish()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    fn name(&self) -> &'static str {
        "memory"
    }

    async fn create(&self, state: &mut UploadState) -> Result<String> {
        let key = format!("memory://{}", state.id());
        self.data
            .write()
            .unwrap()
            .insert(key.clone(), BytesMut::new());
        state.set_storage_key(key.clone());
        Ok(key)
    }

    async fn append(&self, state: &mut UploadState, data: ChunkStream) -> Result<u64> {
        let key = state.storage_key().ok_or(Error::StorageKeyMissing)?;

        let bytes = match data {
            ChunkStream::Buffered(b) => b,
            ChunkStream::Stream(mut stream) => {
                let mut buffer = BytesMut::new();
                while let Some(chunk) = stream.next().await {
                    buffer.extend_from_slice(&chunk.map_err(Error::Io)?);
                }
                buffer.freeze()
            }
        };

        let mut storage = self.data.write().unwrap();
        let entry = storage
            .get_mut(key)
            .ok_or_else(|| Error::NotFound(key.to_string()))?;

        entry.extend_from_slice(&bytes);
        Ok(entry.len() as u64)
    }

    async fn get_stream(&self, state: &UploadState) -> Result<ByteStream> {
        let key = state.storage_key().ok_or(Error::StorageKeyMissing)?;

        let storage = self.data.read().unwrap();
        let data = storage
            .get(key)
            .ok_or_else(|| Error::NotFound(key.to_string()))?
            .clone()
            .freeze();

        Ok(Box::pin(futures::stream::once(async move { Ok(data) })))
    }

    async fn concat(&self, target: &mut UploadState, parts: Vec<UploadState>) -> Result<()> {
        let target_key = target
            .storage_key()
            .ok_or(Error::StorageKeyMissing)?
            .to_string();

        let mut combined = BytesMut::new();

        {
            let storage = self.data.read().unwrap();
            for part in &parts {
                let part_key = part.storage_key().ok_or(Error::StorageKeyMissing)?;
                let part_data = storage
                    .get(part_key)
                    .ok_or_else(|| Error::NotFound(part_key.to_string()))?;
                combined.extend_from_slice(part_data);
            }
        }

        let mut storage = self.data.write().unwrap();
        storage.insert(target_key.to_string(), combined);

        Ok(())
    }

    async fn delete(&self, state: &UploadState) -> Result<()> {
        if let Some(key) = state.storage_key() {
            self.data.write().unwrap().remove(key);
        }
        Ok(())
    }

    async fn size(&self, state: &UploadState) -> Result<Option<u64>> {
        let key = match state.storage_key() {
            Some(k) => k,
            None => return Ok(None),
        };

        let storage = self.data.read().unwrap();
        Ok(storage.get(key).map(|d| d.len() as u64))
    }

    async fn get_range(
        &self,
        state: &UploadState,
        start: u64,
        end: Option<u64>,
    ) -> Result<ByteStream> {
        let key = state.storage_key().ok_or(Error::StorageKeyMissing)?;

        let storage = self.data.read().unwrap();
        let data = storage
            .get(key)
            .ok_or_else(|| Error::NotFound(key.to_string()))?
            .clone()
            .freeze();

        let len = data.len() as u64;
        let start = start.min(len);
        let end = end.unwrap_or(len).min(len);
        let slice = if start <= end {
            data.slice(start as usize..end as usize)
        } else {
            Bytes::new()
        };

        Ok(Box::pin(futures::stream::once(async move { Ok(slice) })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_append() {
        let storage = MemoryStorage::new();
        let mut state = UploadState::new("test-1");

        // Create
        let key = storage.create(&mut state).await.unwrap();
        assert!(key.contains("test-1"));
        assert_eq!(state.storage_key(), Some(key.as_str()));

        // Append
        let data = ChunkStream::from_bytes(Bytes::from("hello "));
        let offset = storage.append(&mut state, data).await.unwrap();
        assert_eq!(offset, 6);
        assert_eq!(state.offset(), 0);

        let data2 = ChunkStream::from_bytes(Bytes::from("world"));
        let offset2 = storage.append(&mut state, data2).await.unwrap();
        assert_eq!(offset2, 11);

        // Verify content
        let stored = storage.get_data(&key).unwrap();
        assert_eq!(stored.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn test_get_stream() {
        let storage = MemoryStorage::new();
        let mut state = UploadState::new("test-2");

        storage.create(&mut state).await.unwrap();
        storage
            .append(
                &mut state,
                ChunkStream::from_bytes(Bytes::from("test data")),
            )
            .await
            .unwrap();

        let mut stream = storage.get_stream(&state).await.unwrap();
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk.as_ref(), b"test data");
    }

    #[tokio::test]
    async fn test_concat() {
        let storage = MemoryStorage::new();

        // Create parts
        let mut part1 = UploadState::new("part-1");
        storage.create(&mut part1).await.unwrap();
        storage
            .append(&mut part1, ChunkStream::from_bytes(Bytes::from("Hello ")))
            .await
            .unwrap();

        let mut part2 = UploadState::new("part-2");
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

        // Verify
        let key = target.storage_key().unwrap();
        let data = storage.get_data(key).unwrap();
        assert_eq!(data.as_ref(), b"Hello World");
    }

    #[tokio::test]
    async fn test_delete() {
        let storage = MemoryStorage::new();
        let mut state = UploadState::new("test-3");

        storage.create(&mut state).await.unwrap();
        assert_eq!(storage.len(), 1);

        storage.delete(&state).await.unwrap();
        assert_eq!(storage.len(), 0);
    }

    #[tokio::test]
    async fn test_size() {
        let storage = MemoryStorage::new();
        let mut state = UploadState::new("test-4");

        // No storage key yet
        assert_eq!(storage.size(&state).await.unwrap(), None);

        storage.create(&mut state).await.unwrap();
        assert_eq!(storage.size(&state).await.unwrap(), Some(0));

        storage
            .append(&mut state, ChunkStream::from_bytes(Bytes::from("12345")))
            .await
            .unwrap();
        assert_eq!(storage.size(&state).await.unwrap(), Some(5));
    }

    /// Per-PATCH atomicity invariant: if the body stream errors before
    /// it is fully drained, no bytes are committed to storage. This is
    /// not a happy-path optimisation -- it is the invariant the protocol
    /// relies on. Resumability is provided at the PATCH boundary, never
    /// inside one. Backends that buffered partial bytes on error would
    /// silently violate the offset contract: the state store would
    /// report N bytes, the backend would hold N+K, and the next PATCH
    /// at offset N would write the K bytes again. Don't "fix" this.
    ///
    /// Other storage backends should preserve the same invariant for the
    /// same reason.
    #[tokio::test]
    async fn append_rolls_back_when_body_stream_errors_mid_stream() {
        use futures::stream;
        use std::io;

        let storage = MemoryStorage::new();
        let mut state = UploadState::new("test-rollback");
        let key = storage.create(&mut state).await.unwrap();

        // First PATCH commits cleanly.
        let baseline = storage
            .append(&mut state, ChunkStream::from_bytes(Bytes::from("intact ")))
            .await
            .unwrap();
        assert_eq!(baseline, 7);

        // Second PATCH: a stream that yields some bytes then errors.
        // The error is io::ErrorKind::ConnectionReset, modelling a TCP
        // RST mid-upload. Storage layer should propagate the error
        // and leave nothing behind.
        let stream: ByteStream = Box::pin(stream::iter(vec![
            Ok(Bytes::from("partial-")),
            Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "client gone",
            )),
            // Anything after the first Err is unreachable, but include
            // a "good" trailer to prove we don't accidentally pick it up.
            Ok(Bytes::from("...trailer-that-must-not-commit")),
        ]));
        let err = storage
            .append(&mut state, ChunkStream::from_stream(stream))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Io(_)),
            "expected Error::Io, got {err:?}"
        );

        // Storage size is unchanged from the first (clean) PATCH --
        // the partial bytes are NOT visible.
        let stored = storage.get_data(&key).unwrap();
        assert_eq!(stored.as_ref(), b"intact ");
        assert_eq!(storage.size(&state).await.unwrap(), Some(7));
    }
}
