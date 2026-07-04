//! Storage traits for upload bytes.
//!
//! This module defines the required [`Storage`] trait for upload lifecycle
//! writes and the optional [`StorageReader`] trait for integrations that expose
//! stored bytes through download or inspection paths.
//!
//! # Implementations
//!
//! - `file::FileStorage` - File-based storage (feature: `storage-file`)
//! - `memory::MemoryStorage` - In-memory storage (feature: `storage-memory`)
//!
//! First-party integration crates can provide production storage backends
//! against these traits.

// Feature-gated implementations
// Native implementations are not available on wasm32.
#[cfg(any(test, feature = "conformance-storage"))]
pub mod conformance;

#[cfg(all(feature = "storage-file", not(target_arch = "wasm32")))]
pub mod file;

#[cfg(all(feature = "storage-memory", not(target_arch = "wasm32")))]
pub mod memory;

use std::collections::HashMap;
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

use crate::error::Result;
use crate::runtime::MaybeSendSync;

/// Trait for storing upload file data.
///
/// Implementors own upload bytes and any storage-local locator or bookkeeping
/// encoded in [`StorageHandle`]. The `StateStore` persists protocol upload
/// state and the opaque handle snapshot, but storage adapters are the only code
/// that should interpret handle internals.
/// Successful write methods should return only after bytes are accepted by the
/// backend. If a backend can partially write and then fail, it should either
/// roll the write back or report enough actual size through [`Storage::size`]
/// for protocol recovery to reconcile state on the next request.
///
/// # Platform Support
///
/// This trait uses conditional bounds:
/// - On native platforms: implementations and returned futures must be `Send + Sync`
/// - On `wasm32`: `Send + Sync` is not required
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Storage: MaybeSendSync {
    /// Returns the storage backend name for logging/debugging.
    fn name(&self) -> &'static str;

    /// Creates storage for a new upload.
    ///
    /// This should allocate any necessary resources (file handles, etc.) and
    /// return an opaque handle that is safe to pass back to this storage
    /// implementation later. The lifecycle module persists the returned handle
    /// alongside upload state.
    async fn create(&self, upload_id: &str) -> Result<StorageHandle>;

    /// Appends data to an existing upload.
    ///
    /// Data should be committed at `request.expected_offset`. Implementations
    /// may update the returned [`StorageHandle`] with backend-specific
    /// bookkeeping, but protocol lifecycle code owns upload offset and length
    /// advancement after this method succeeds. The returned handle replaces the
    /// persisted handle, so implementations must preserve every existing
    /// internal fact from `request.handle` that remains valid after the append.
    async fn append(&self, request: AppendRequest) -> Result<StorageHandle>;

    /// Concatenates multiple partial uploads into a final upload.
    ///
    /// Used by the Concatenation extension. The parts should be concatenated
    /// in the order provided. Backends should avoid exposing a partially
    /// concatenated target as complete if copying fails midway. The returned
    /// handle replaces the persisted target handle, so implementations must
    /// preserve every existing target internal fact that remains valid after
    /// concatenation.
    async fn concat(&self, request: ConcatRequest) -> Result<StorageHandle>;

    /// Deletes an upload's data from storage.
    ///
    /// Implementations should treat missing storage as success so termination
    /// cleanup can be retried safely.
    async fn delete(&self, handle: &StorageHandle) -> Result<()>;

    /// Returns the current size of an upload in storage.
    ///
    /// This is useful for recovery after crashes - checking actual storage
    /// vs recorded offset.
    ///
    /// Returns the actual size in bytes, or `None` if the storage key doesn't
    /// exist.
    async fn size(&self, handle: &StorageHandle) -> Result<Option<u64>>;
}

/// Optional trait for reading stored upload bytes.
///
/// Download is not part of the core TUS protocol. Adapters that expose stored
/// bytes back to callers implement this trait in addition to [`Storage`], while
/// upload-only adapters can satisfy the protocol lifecycle with [`Storage`]
/// alone.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait StorageReader: MaybeSendSync {
    /// Retrieves a stream of the upload data for download.
    async fn stream(&self, handle: &StorageHandle) -> Result<ByteStream>;

    /// Retrieves a range of bytes from the upload.
    ///
    /// `start` is inclusive and `end` is exclusive. `None` for `end` means the
    /// end of the upload. Implementations should clamp the range to the
    /// current object size.
    ///
    /// This method is deliberately required (no buffering default): range
    /// requests must not silently degrade into reading the whole object into
    /// memory. Backends without native range support should stream and skip
    /// instead of buffering everything.
    async fn stream_range(
        &self,
        handle: &StorageHandle,
        start: u64,
        end: Option<u64>,
    ) -> Result<ByteStream>;
}

/// Opaque storage addressing and backend-specific persisted facts.
///
/// Lifecycle code stores this handle on [`UploadState`](crate::UploadState),
/// but storage adapters are the only code that should interpret the key or
/// internal values. When a storage operation returns an updated handle, it is
/// returning the complete persisted storage facts for the upload. Application
/// code may persist and pass handles around, but should not derive protocol
/// behavior from their contents.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageHandle {
    key: String,
    internal: HashMap<String, String>,
}

impl StorageHandle {
    /// Creates a storage handle from an opaque backend key.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            internal: HashMap::new(),
        }
    }

    /// Creates a storage handle from persisted parts.
    pub fn from_parts(key: impl Into<String>, internal: HashMap<String, String>) -> Self {
        Self {
            key: key.into(),
            internal,
        }
    }

    /// Returns the opaque backend key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Consumes the handle and returns its persisted parts.
    pub fn into_parts(self) -> (String, HashMap<String, String>) {
        (self.key, self.internal)
    }

    /// Stashes backend-specific bookkeeping in the handle.
    pub fn set_internal(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.internal.insert(key.into(), value.into());
    }

    /// Reads a backend-specific value previously stored on the handle.
    pub fn internal(&self, key: &str) -> Option<&str> {
        self.internal.get(key).map(String::as_str)
    }

    /// Removes a backend-specific value from the handle.
    pub fn remove_internal(&mut self, key: &str) -> Option<String> {
        self.internal.remove(key)
    }
}

/// Storage append request facts.
///
/// Construct with [`AppendRequest::new`]. The struct is `#[non_exhaustive]`
/// so future protocol facts can be added without breaking storage backends;
/// fields stay public for reading and destructuring with `..`.
#[derive(Debug)]
#[non_exhaustive]
pub struct AppendRequest {
    /// Opaque storage handle for the upload being appended to.
    pub handle: StorageHandle,
    /// Byte offset where this append is expected to start.
    pub expected_offset: u64,
    /// Upload bytes to append.
    pub data: ChunkStream,
    /// Whether lifecycle has determined this append completes the upload.
    pub completes_upload: bool,
}

impl AppendRequest {
    /// Creates an append request from its protocol facts.
    #[must_use]
    pub fn new(
        handle: StorageHandle,
        expected_offset: u64,
        data: ChunkStream,
        completes_upload: bool,
    ) -> Self {
        Self {
            handle,
            expected_offset,
            data,
            completes_upload,
        }
    }
}

/// Storage concatenation request facts.
///
/// Construct with [`ConcatRequest::new`]. The struct is `#[non_exhaustive]`
/// so future protocol facts can be added without breaking storage backends;
/// fields stay public for reading and destructuring with `..`.
#[derive(Debug)]
#[non_exhaustive]
pub struct ConcatRequest {
    /// Opaque storage handle for the final upload target.
    pub target: StorageHandle,
    /// Opaque storage handles for partial uploads, in concatenation order.
    pub parts: Vec<StorageHandle>,
}

impl ConcatRequest {
    /// Creates a concatenation request from its protocol facts.
    #[must_use]
    pub fn new(target: StorageHandle, parts: Vec<StorageHandle>) -> Self {
        Self { target, parts }
    }
}

/// A stream of data chunks for upload.
///
/// This enum is deliberately exhaustive (not `#[non_exhaustive]`): storage
/// backends must handle every delivery mode, so adding a variant is a
/// breaking change by design.
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
#[cfg(not(target_arch = "wasm32"))]
pub type ByteStream = Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send>>;

/// A stream of bytes for request/response bodies.
#[cfg(target_arch = "wasm32")]
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

    #[cfg(target_arch = "wasm32")]
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
