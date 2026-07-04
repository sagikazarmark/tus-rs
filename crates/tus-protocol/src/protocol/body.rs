//! Framework-neutral request body intake.

use std::pin::Pin;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::HeaderMap;

use crate::storage::ChunkStream;

/// A stream of request body frames supplied by framework adapters.
#[cfg(not(target_arch = "wasm32"))]
pub type BodyStream = Pin<Box<dyn Stream<Item = std::io::Result<BodyFrame>> + Send>>;

/// A stream of request body frames supplied by framework adapters.
#[cfg(target_arch = "wasm32")]
pub type BodyStream = Pin<Box<dyn Stream<Item = std::io::Result<BodyFrame>>>>;

/// A request body frame observed by a framework adapter.
#[derive(Debug)]
#[non_exhaustive]
pub enum BodyFrame {
    /// Request body bytes.
    Data(Bytes),
    /// Request trailer headers observed after the body.
    Trailers(HeaderMap),
}

/// Framework-neutral request body input for protocol handlers.
///
/// This enum is deliberately exhaustive (not `#[non_exhaustive]`): consumers
/// must handle every body delivery mode, so adding a variant is a breaking
/// change by design.
pub enum RequestBody {
    /// No request body was supplied.
    Absent,
    /// Buffered body bytes.
    Bytes(Bytes),
    /// Streamed body frames.
    Stream(BodyStream),
}

impl RequestBody {
    /// Creates a request body from buffered bytes.
    #[must_use]
    pub fn from_bytes(bytes: Bytes) -> Self {
        Self::Bytes(bytes)
    }

    /// Creates an empty request body that was supplied by the client.
    #[must_use]
    pub fn empty() -> Self {
        Self::Bytes(Bytes::new())
    }

    /// Creates an explicitly absent request body.
    #[must_use]
    pub fn absent() -> Self {
        Self::Absent
    }

    /// Creates a request body from a body-frame stream.
    #[must_use]
    pub fn from_stream(stream: BodyStream) -> Self {
        Self::Stream(stream)
    }

    /// Returns whether the request supplied a body without polling streams.
    #[must_use]
    pub fn is_supplied(&self) -> bool {
        !matches!(self, Self::Absent)
    }

    /// Creates a data-only request body from an existing storage chunk stream.
    #[must_use]
    pub fn from_chunk_stream(stream: ChunkStream) -> Self {
        match stream {
            ChunkStream::Buffered(bytes) => Self::Bytes(bytes),
            ChunkStream::Stream(stream) => {
                Self::Stream(Box::pin(stream.map(|chunk| chunk.map(BodyFrame::Data))))
            }
        }
    }
}

impl std::fmt::Debug for RequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestBody::Absent => write!(f, "RequestBody::Absent"),
            RequestBody::Bytes(bytes) => write!(f, "RequestBody::Bytes({} bytes)", bytes.len()),
            RequestBody::Stream(_) => write!(f, "RequestBody::Stream(..)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ByteStream;
    use bytes::Bytes;
    use futures::StreamExt;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[test]
    fn buffered_chunk_stream_becomes_buffered_request_body() {
        let body =
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"abc")));

        assert!(matches!(body, RequestBody::Bytes(bytes) if bytes == Bytes::from_static(b"abc")));
    }

    #[test]
    fn empty_chunk_stream_becomes_supplied_empty_request_body() {
        let body = RequestBody::from_chunk_stream(ChunkStream::empty());

        assert!(matches!(&body, RequestBody::Bytes(bytes) if bytes.is_empty()));
        assert!(body.is_supplied());
    }

    #[test]
    fn supplied_body_presence_does_not_poll_streams() {
        let polled = Arc::new(AtomicBool::new(false));
        let polled_for_stream = Arc::clone(&polled);
        let stream: BodyStream = Box::pin(futures::stream::once(async move {
            polled_for_stream.store(true, Ordering::SeqCst);
            Ok(BodyFrame::Data(Bytes::from_static(b"abc")))
        }));
        let body = RequestBody::from_stream(stream);

        assert!(body.is_supplied());
        assert!(!polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn streamed_chunk_stream_becomes_data_frames() {
        let stream: ByteStream = Box::pin(futures::stream::iter([
            Ok(Bytes::from_static(b"ab")),
            Ok(Bytes::from_static(b"cd")),
        ]));
        let RequestBody::Stream(mut body) =
            RequestBody::from_chunk_stream(ChunkStream::from_stream(stream))
        else {
            panic!("expected streamed request body");
        };

        let first = body.next().await.unwrap().unwrap();
        let second = body.next().await.unwrap().unwrap();

        assert!(matches!(first, BodyFrame::Data(bytes) if bytes == Bytes::from_static(b"ab")));
        assert!(matches!(second, BodyFrame::Data(bytes) if bytes == Bytes::from_static(b"cd")));
    }
}
