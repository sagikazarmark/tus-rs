//! In-memory storage implementation.
//!
//! This storage backend keeps all data in memory. Useful for testing
//! and development, but data is lost when the process exits.

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::error::{Error, Result};
use crate::storage::ByteStream;
use crate::storage::{
    AppendRequest, ChunkStream, ConcatRequest, Storage, StorageHandle, StorageReader,
};

/// In-memory storage backend.
///
/// Stores all upload data in a HashMap protected by a RwLock.
/// Thread-safe and suitable for single-process use.
///
/// # Memory use with untrusted input
///
/// This backend buffers each upload's bytes fully in memory, and `append`
/// collects a streamed chunk into memory before committing it. The protocol
/// only bounds that intake by the client-declared `Content-Length` (or the
/// configured chunk/size limits). For deployments that accept untrusted
/// uploads, configure [`Config::with_max_chunk_size`](crate::Config::with_max_chunk_size)
/// and a maximum upload size, or use a streaming backend such as `FileStorage`
/// — otherwise a client can drive allocation up to its declared body size.
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

    async fn create(&self, upload_id: &str) -> Result<StorageHandle> {
        let key = format!("memory://{}", upload_id);
        self.data
            .write()
            .unwrap()
            .insert(key.clone(), BytesMut::new());
        Ok(StorageHandle::new(key))
    }

    async fn append(&self, request: AppendRequest) -> Result<StorageHandle> {
        let AppendRequest {
            handle,
            expected_offset,
            data,
            completes_upload: _,
        } = request;
        let key = handle.key().to_string();

        {
            let storage = self.data.read().unwrap();
            let entry = storage
                .get(&key)
                .ok_or_else(|| Error::NotFound(key.clone()))?;
            if entry.len() as u64 != expected_offset {
                return Err(Error::Internal(format!(
                    "memory storage size {} does not match expected offset {expected_offset} for key {key}",
                    entry.len()
                )));
            }
        }

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
            .get_mut(&key)
            .ok_or_else(|| Error::NotFound(key.clone()))?;
        if entry.len() as u64 != expected_offset {
            return Err(Error::Internal(format!(
                "memory storage size {} does not match expected offset {expected_offset} for key {key}",
                entry.len()
            )));
        }

        entry.extend_from_slice(&bytes);
        Ok(handle)
    }

    async fn concat(&self, request: ConcatRequest) -> Result<StorageHandle> {
        let ConcatRequest { target, parts } = request;
        let target_key = target.key().to_string();

        let mut combined = BytesMut::new();

        {
            let storage = self.data.read().unwrap();
            for part in &parts {
                let part_key = part.key();
                let part_data = storage
                    .get(part_key)
                    .ok_or_else(|| Error::NotFound(part_key.to_string()))?;
                combined.extend_from_slice(part_data);
            }
        }

        let mut storage = self.data.write().unwrap();
        storage.insert(target_key.to_string(), combined);

        Ok(target)
    }

    async fn delete(&self, handle: &StorageHandle) -> Result<()> {
        self.data.write().unwrap().remove(handle.key());
        Ok(())
    }

    async fn size(&self, handle: &StorageHandle) -> Result<Option<u64>> {
        let storage = self.data.read().unwrap();
        Ok(storage.get(handle.key()).map(|d| d.len() as u64))
    }
}

#[async_trait]
impl StorageReader for MemoryStorage {
    async fn stream(&self, handle: &StorageHandle) -> Result<ByteStream> {
        let key = handle.key();

        let storage = self.data.read().unwrap();
        let data = storage
            .get(key)
            .ok_or_else(|| Error::NotFound(key.to_string()))?
            .clone()
            .freeze();

        Ok(Box::pin(futures_util::stream::once(
            async move { Ok(data) },
        )))
    }

    async fn stream_range(
        &self,
        handle: &StorageHandle,
        start: u64,
        end: Option<u64>,
    ) -> Result<ByteStream> {
        let key = handle.key();

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

        Ok(Box::pin(futures_util::stream::once(
            async move { Ok(slice) },
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn storage_conformance() {
        let storage = MemoryStorage::new();

        crate::storage::conformance::assert_full_semantics(&storage).await;
    }

    #[tokio::test]
    async fn test_create_and_append() {
        let storage = MemoryStorage::new();

        // Create
        let mut handle = storage.create("test-1").await.unwrap();
        assert!(handle.key().contains("test-1"));

        // Append
        let data = ChunkStream::from_bytes(Bytes::from("hello "));
        handle = storage
            .append(AppendRequest::new(handle, 0, data, false))
            .await
            .unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), Some(6));

        let data2 = ChunkStream::from_bytes(Bytes::from("world"));
        handle = storage
            .append(AppendRequest::new(handle, 6, data2, true))
            .await
            .unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), Some(11));

        // Verify content
        let stored = storage.get_data(handle.key()).unwrap();
        assert_eq!(stored.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn test_get_stream() {
        let storage = MemoryStorage::new();

        let handle = storage.create("test-2").await.unwrap();
        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("test data")),
                true,
            ))
            .await
            .unwrap();

        let mut stream = storage.stream(&handle).await.unwrap();
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk.as_ref(), b"test data");
    }

    #[tokio::test]
    async fn test_concat() {
        let storage = MemoryStorage::new();

        // Create parts
        let part1 = storage.create("part-1").await.unwrap();
        let part1 = storage
            .append(AppendRequest::new(
                part1,
                0,
                ChunkStream::from_bytes(Bytes::from("Hello ")),
                true,
            ))
            .await
            .unwrap();

        let part2 = storage.create("part-2").await.unwrap();
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

        // Verify
        let data = storage.get_data(target.key()).unwrap();
        assert_eq!(data.as_ref(), b"Hello World");
    }

    #[tokio::test]
    async fn test_delete() {
        let storage = MemoryStorage::new();

        let handle = storage.create("test-3").await.unwrap();
        assert_eq!(storage.len(), 1);

        storage.delete(&handle).await.unwrap();
        assert_eq!(storage.len(), 0);
    }

    #[tokio::test]
    async fn test_size() {
        let storage = MemoryStorage::new();

        let handle = storage.create("test-4").await.unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), Some(0));

        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("12345")),
                true,
            ))
            .await
            .unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn concat_preserves_existing_target_handle_internals() {
        let storage = MemoryStorage::new();
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
        let handle = storage.create("test-rollback").await.unwrap();
        let key = handle.key().to_string();

        // First PATCH commits cleanly.
        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("intact ")),
                false,
            ))
            .await
            .unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), Some(7));

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
            .append(AppendRequest::new(
                handle.clone(),
                7,
                ChunkStream::from_stream(stream),
                false,
            ))
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
        assert_eq!(storage.size(&handle).await.unwrap(), Some(7));
    }

    #[tokio::test]
    async fn append_rejects_stale_offset_before_reading_body() {
        let storage = MemoryStorage::new();
        let handle = storage.create("test-stale-offset").await.unwrap();
        let handle = storage
            .append(AppendRequest::new(
                handle,
                0,
                ChunkStream::from_bytes(Bytes::from("seed")),
                false,
            ))
            .await
            .unwrap();

        let stream: ByteStream = Box::pin(futures::stream::once(async {
            panic!("body stream should not be read when storage offset is stale");
            #[allow(unreachable_code)]
            Ok(Bytes::from("must not be consumed"))
        }));

        let error = storage
            .append(AppendRequest::new(
                handle.clone(),
                0,
                ChunkStream::from_stream(stream),
                false,
            ))
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Internal(_)));
        assert_eq!(storage.size(&handle).await.unwrap(), Some(4));
    }
}
