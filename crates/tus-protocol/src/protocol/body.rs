//! Framework-neutral request body intake.

use std::pin::Pin;

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use http::HeaderMap;

use crate::config::{Config, Extension};
use crate::error::Error;
use crate::protocol::Headers;
use crate::protocol::headers::parse_upload_checksum;
use crate::storage::ChunkStream;

/// Optional checksum selected by body intake.
type BodyChecksum = Option<(crate::config::ChecksumAlgorithm, Vec<u8>)>;

/// A stream of request body frames supplied by framework adapters.
#[cfg(not(feature = "local-futures"))]
pub type BodyStream = Pin<Box<dyn Stream<Item = std::io::Result<BodyFrame>> + Send>>;

/// A stream of request body frames supplied by framework adapters.
#[cfg(feature = "local-futures")]
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
pub enum RequestBody {
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

    /// Creates an empty request body.
    #[must_use]
    pub fn empty() -> Self {
        Self::Bytes(Bytes::new())
    }

    /// Creates a request body from a body-frame stream.
    #[must_use]
    pub fn from_stream(stream: BodyStream) -> Self {
        Self::Stream(stream)
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

/// Collected and validated request body bytes.
#[derive(Debug)]
pub(crate) struct CollectedBody {
    pub(crate) bytes: Bytes,
}

/// Collects a request body according to protocol-owned body intake policy.
pub(crate) async fn collect(
    config: &Config,
    headers: &Headers,
    body_limit: Option<u64>,
    body: RequestBody,
) -> Result<CollectedBody, Error> {
    if let Some(checksum) = headers.upload_checksum.as_ref() {
        validate_checksum_algorithm(config, checksum.0)?;
    }

    if let Some(content_length) = headers.content_length
        && let Some(limit) = body_limit
        && content_length > limit
    {
        return Err(Error::SizeExceeded {
            size: content_length,
            max: limit,
        });
    }

    let (bytes, trailers) = collect_frames(body, body_limit).await?;
    validate_content_length(headers, bytes.len() as u64)?;

    let checksum = effective_checksum(config, headers, trailers.as_ref())?;
    verify_checksum(config, checksum, &bytes)?;

    Ok(CollectedBody { bytes })
}

async fn collect_frames(
    body: RequestBody,
    body_limit: Option<u64>,
) -> Result<(Bytes, Option<HeaderMap>), Error> {
    match body {
        RequestBody::Bytes(bytes) => {
            enforce_body_limit(0, bytes.len(), body_limit)?;
            Ok((bytes, None))
        }
        RequestBody::Stream(mut stream) => {
            let mut buffer = BytesMut::new();
            let mut trailers = None;
            while let Some(frame) = stream.next().await {
                match frame.map_err(|err| Error::Internal(err.to_string()))? {
                    BodyFrame::Data(bytes) => {
                        enforce_body_limit(buffer.len(), bytes.len(), body_limit)?;
                        buffer.extend_from_slice(&bytes);
                    }
                    BodyFrame::Trailers(headers) => {
                        trailers = Some(headers);
                    }
                }
            }
            Ok((buffer.freeze(), trailers))
        }
    }
}

fn enforce_body_limit(
    current_len: usize,
    next_len: usize,
    body_limit: Option<u64>,
) -> Result<(), Error> {
    let Some(limit) = body_limit else {
        return Ok(());
    };
    let next_total = (current_len as u64).saturating_add(next_len as u64);
    if next_total > limit {
        return Err(Error::SizeExceeded {
            size: next_total,
            max: limit,
        });
    }

    Ok(())
}

fn validate_content_length(headers: &Headers, actual_len: u64) -> Result<(), Error> {
    if let Some(content_length) = headers.content_length
        && content_length != actual_len
    {
        return Err(Error::InvalidHeader {
            header: "Content-Length",
            message: format!(
                "declared content length {content_length} does not match body size {actual_len}"
            ),
        });
    }

    Ok(())
}

fn effective_checksum(
    config: &Config,
    headers: &Headers,
    trailers: Option<&HeaderMap>,
) -> Result<BodyChecksum, Error> {
    if headers.upload_checksum.is_some() {
        return Ok(headers.upload_checksum.clone());
    }

    if !config.has_extension(Extension::ChecksumTrailer) {
        return Ok(None);
    }

    trailers
        .map(parse_upload_checksum)
        .transpose()
        .map(Option::flatten)
}

fn validate_checksum_algorithm(
    config: &Config,
    algorithm: crate::config::ChecksumAlgorithm,
) -> Result<(), Error> {
    #[cfg(feature = "checksum")]
    {
        if config.has_extension(Extension::Checksum)
            && !config.supports_checksum_algorithm(algorithm)
        {
            return Err(Error::UnsupportedChecksum(algorithm.as_str().to_string()));
        }
    }
    #[cfg(not(feature = "checksum"))]
    let _ = (config, algorithm);

    Ok(())
}

fn verify_checksum(config: &Config, checksum: BodyChecksum, bytes: &[u8]) -> Result<(), Error> {
    #[cfg(feature = "checksum")]
    if let Some((algorithm, expected)) = checksum {
        if !config.has_extension(Extension::Checksum) {
            return Ok(());
        }

        validate_checksum_algorithm(config, algorithm)?;
        let calculated = crate::checksum::calculate(algorithm, bytes);
        if calculated != expected {
            use base64::Engine;
            return Err(Error::ChecksumMismatch {
                expected: base64::engine::general_purpose::STANDARD.encode(&expected),
                actual: base64::engine::general_purpose::STANDARD.encode(&calculated),
            });
        }
    }

    #[cfg(not(feature = "checksum"))]
    let _ = (config, checksum, bytes);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ByteStream;
    use bytes::Bytes;
    use futures::StreamExt;

    #[test]
    fn buffered_chunk_stream_becomes_buffered_request_body() {
        let body =
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"abc")));

        assert!(matches!(body, RequestBody::Bytes(bytes) if bytes == Bytes::from_static(b"abc")));
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

#[cfg(all(test, feature = "checksum", not(feature = "local-futures")))]
mod intake_tests {
    use super::*;
    use crate::config::{ChecksumAlgorithm, Config, Extension};
    use crate::error::Error;
    use crate::protocol::Headers;
    use bytes::Bytes;
    use http::{HeaderMap, HeaderValue};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    fn sha1_header_for(data: &[u8]) -> String {
        use base64::Engine;
        let checksum = crate::checksum::calculate(ChecksumAlgorithm::Sha1, data);
        format!(
            "sha1 {}",
            base64::engine::general_purpose::STANDARD.encode(checksum)
        )
    }

    fn trailers(value: &str) -> HeaderMap {
        let mut trailers = HeaderMap::new();
        trailers.insert("upload-checksum", HeaderValue::from_str(value).unwrap());
        trailers
    }

    #[tokio::test]
    async fn declared_content_length_over_limit_fails_before_polling_stream() {
        let polled = Arc::new(AtomicBool::new(false));
        let polled_for_stream = Arc::clone(&polled);
        let stream: BodyStream = Box::pin(futures::stream::once(async move {
            polled_for_stream.store(true, Ordering::SeqCst);
            Ok(BodyFrame::Data(Bytes::from_static(b"abcdef")))
        }));
        let headers = Headers {
            content_length: Some(6),
            ..Default::default()
        };

        let err = collect(
            &Config::default(),
            &headers,
            Some(5),
            RequestBody::from_stream(stream),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::SizeExceeded { size: 6, max: 5 }));
        assert!(!polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn content_length_mismatch_fails_after_collection() {
        let headers = Headers {
            content_length: Some(4),
            ..Default::default()
        };

        let err = collect(
            &Config::default(),
            &headers,
            Some(10),
            RequestBody::from_bytes(Bytes::from_static(b"abc")),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            Error::InvalidHeader {
                header: "Content-Length",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn checksum_trailer_is_accepted_when_extension_enabled() {
        let config = Config::default().with_extension(Extension::ChecksumTrailer);
        let headers = Headers {
            content_length: Some(5),
            ..Default::default()
        };
        let trailer_value = sha1_header_for(b"hello");
        let stream: BodyStream = Box::pin(futures::stream::iter([
            Ok(BodyFrame::Data(Bytes::from_static(b"hello"))),
            Ok(BodyFrame::Trailers(trailers(&trailer_value))),
        ]));

        let collected = collect(
            &config,
            &headers,
            Some(10),
            RequestBody::from_stream(stream),
        )
        .await
        .unwrap();

        assert_eq!(collected.bytes, Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn header_checksum_wins_over_malformed_trailer() {
        let config = Config::default().with_extension(Extension::ChecksumTrailer);
        let headers = Headers {
            content_length: Some(5),
            upload_checksum: Some((
                ChecksumAlgorithm::Sha1,
                crate::checksum::calculate(ChecksumAlgorithm::Sha1, b"hello"),
            )),
            ..Default::default()
        };
        let stream: BodyStream = Box::pin(futures::stream::iter([
            Ok(BodyFrame::Data(Bytes::from_static(b"hello"))),
            Ok(BodyFrame::Trailers(trailers("sha1 not-base64"))),
        ]));

        let collected = collect(
            &config,
            &headers,
            Some(10),
            RequestBody::from_stream(stream),
        )
        .await
        .unwrap();

        assert_eq!(collected.bytes, Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn unsupported_header_checksum_algorithm_fails_before_polling_stream() {
        let config = Config::default().with_extension(Extension::Checksum);
        let polled = Arc::new(AtomicBool::new(false));
        let polled_for_stream = Arc::clone(&polled);
        let stream: BodyStream = Box::pin(futures::stream::once(async move {
            polled_for_stream.store(true, Ordering::SeqCst);
            Ok(BodyFrame::Data(Bytes::from_static(b"hello")))
        }));
        let headers = Headers {
            upload_checksum: Some((ChecksumAlgorithm::Sha256, vec![0u8; 32])),
            ..Default::default()
        };

        let err = collect(
            &config,
            &headers,
            Some(10),
            RequestBody::from_stream(stream),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::UnsupportedChecksum(algorithm) if algorithm == "sha256"));
        assert!(!polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn checksum_mismatch_fails_after_collection() {
        let config = Config::default().with_extension(Extension::Checksum);
        let headers = Headers {
            upload_checksum: Some((ChecksumAlgorithm::Sha1, vec![0u8; 20])),
            ..Default::default()
        };

        let err = collect(
            &config,
            &headers,
            Some(10),
            RequestBody::from_bytes(Bytes::from_static(b"hello")),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::ChecksumMismatch { .. }));
    }
}
