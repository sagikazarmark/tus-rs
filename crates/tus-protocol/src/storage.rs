//! Storage trait for storing upload data.
//!
//! This module defines the `Storage` trait that abstracts over different
//! storage backends (filesystem, S3, R2, etc.).
//!
//! # Implementations
//!
//! - `file::FileStorage` - File-based storage (feature: `storage-file`)
//! - `memory::MemoryStorage` - In-memory storage (feature: `storage-memory`)
//!
//! First-party integration crates can provide production storage backends
//! against this trait.

// Feature-gated implementations
// Native implementations are not available in local-futures builds.
#[cfg(all(feature = "storage-file", not(feature = "local-futures")))]
pub mod file;

#[cfg(all(feature = "storage-memory", not(feature = "local-futures")))]
pub mod memory;

use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::error::Result;
use crate::runtime::MaybeSendSync;
use crate::state::UploadState;

/// Trait for storing upload file data.
///
/// Implementors must handle the actual storage of upload bytes.
/// The `StateStore` handles metadata; this trait handles only the data itself.
/// Successful write methods should return only after bytes are accepted by the
/// backend. If a backend can partially write and then fail, it should either
/// roll the write back or report enough actual size through [`Storage::size`]
/// for protocol recovery to reconcile state on the next request.
///
/// # Platform Support
///
/// This trait uses conditional bounds:
/// - On native platforms: implementations and returned futures must be `Send + Sync`
/// - With `local-futures`: `Send + Sync` is not required
#[cfg_attr(not(feature = "local-futures"), async_trait)]
#[cfg_attr(feature = "local-futures", async_trait(?Send))]
pub trait Storage: MaybeSendSync {
    /// Returns the storage backend name for logging/debugging.
    fn name(&self) -> &'static str;

    /// Creates storage for a new upload.
    ///
    /// This should allocate any necessary resources (file handles, etc.)
    /// and set `state.storage_key` to the storage identifier. Returns the
    /// storage key/path that was created. The key should be opaque to clients
    /// and safe to pass back to this storage implementation later.
    async fn create(&self, state: &mut UploadState) -> Result<String>;

    /// Appends data to an existing upload.
    ///
    /// Data should be appended starting at `state.offset`. After successful
    /// append, the new offset should be returned. Implementations may modify
    /// backend-specific values through `UploadState` internal-state helpers, but
    /// must not advance protocol fields such as offset or length directly. The
    /// protocol lifecycle verifies and applies the returned offset after the
    /// append succeeds.
    async fn append(&self, state: &mut UploadState, data: ChunkStream) -> Result<u64>;

    /// Retrieves a stream of the upload data for download.
    async fn get_stream(&self, state: &UploadState) -> Result<ByteStream>;

    /// Concatenates multiple partial uploads into a final upload.
    ///
    /// Used by the Concatenation extension. The parts should be concatenated
    /// in the order provided. Backends should avoid exposing a partially
    /// concatenated target as complete if copying fails midway.
    async fn concat(&self, target: &mut UploadState, parts: Vec<UploadState>) -> Result<()>;

    /// Deletes an upload's data from storage.
    ///
    /// Implementations should treat missing storage as success so termination
    /// cleanup can be retried safely.
    async fn delete(&self, state: &UploadState) -> Result<()>;

    /// Returns the current size of an upload in storage.
    ///
    /// This is useful for recovery after crashes - checking actual storage
    /// vs recorded offset.
    ///
    /// Returns the actual size in bytes, or `None` if the storage key doesn't
    /// exist.
    async fn size(&self, state: &UploadState) -> Result<Option<u64>>;

    /// Retrieves a range of bytes from the upload.
    ///
    /// `start` is inclusive and `end` is exclusive. `None` for `end` means the
    /// end of the upload. The default implementation clamps the range to the
    /// current object size.
    ///
    /// The default implementation buffers the full upload and slices it in
    /// memory. Backends with native range support should override this.
    async fn get_range(
        &self,
        state: &UploadState,
        start: u64,
        end: Option<u64>,
    ) -> Result<ByteStream> {
        let mut stream = self.get_stream(state).await?;
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(crate::error::Error::Io)?;
            body.extend_from_slice(&chunk);
        }

        let len = body.len() as u64;
        let start = start.min(len);
        let end = end.unwrap_or(len).min(len);
        let slice = if start <= end {
            Bytes::copy_from_slice(&body[start as usize..end as usize])
        } else {
            Bytes::new()
        };

        Ok(Box::pin(futures::stream::once(async move { Ok(slice) })))
    }
}

/// A stream of data chunks for upload.
pub enum ChunkStream {
    /// Buffered data (small uploads or pre-buffered).
    Buffered(Bytes),
    /// Async stream of chunks.
    Stream(ByteStream),
}

impl ChunkStream {
    /// Creates a chunk stream from buffered bytes.
    #[must_use]
    pub fn from_bytes(bytes: Bytes) -> Self {
        ChunkStream::Buffered(bytes)
    }

    /// Creates a chunk stream from an async stream.
    #[must_use]
    pub fn from_stream(stream: ByteStream) -> Self {
        ChunkStream::Stream(stream)
    }

    /// Creates an empty chunk stream.
    #[must_use]
    pub fn empty() -> Self {
        ChunkStream::Buffered(Bytes::new())
    }
}

impl std::fmt::Debug for ChunkStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkStream::Buffered(b) => write!(f, "ChunkStream::Buffered({} bytes)", b.len()),
            ChunkStream::Stream(_) => write!(f, "ChunkStream::Stream(..)"),
        }
    }
}

/// A stream of bytes for request/response bodies.
#[cfg(not(feature = "local-futures"))]
pub type ByteStream = Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send>>;

/// A stream of bytes for request/response bodies.
#[cfg(feature = "local-futures")]
pub type ByteStream = Pin<Box<dyn Stream<Item = std::io::Result<Bytes>>>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_stream_from_bytes() {
        let bytes = Bytes::from("hello world");
        let stream = ChunkStream::from_bytes(bytes.clone());
        match stream {
            ChunkStream::Buffered(b) => assert_eq!(b, bytes),
            _ => panic!("expected Buffered variant"),
        }
    }

    #[test]
    fn test_chunk_stream_debug() {
        let stream = ChunkStream::from_bytes(Bytes::from("test"));
        let debug_str = format!("{:?}", stream);
        assert!(debug_str.contains("4 bytes"));
    }

    #[cfg(feature = "local-futures")]
    #[test]
    fn test_byte_stream_accepts_non_send_streams_in_local_mode() {
        use std::rc::Rc;

        let marker = Rc::new(());
        let stream: ByteStream = Box::pin(futures::stream::once({
            let marker = marker.clone();
            async move {
                let _marker = marker;
                Ok(Bytes::from_static(b"local"))
            }
        }));

        let chunk = ChunkStream::from_stream(stream);
        assert!(matches!(chunk, ChunkStream::Stream(_)));
    }
}
