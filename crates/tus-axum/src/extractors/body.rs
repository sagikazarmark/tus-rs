//! TUS body extractor.
//!
//! This module maps Axum request body frames into framework-neutral protocol
//! body frames. Protocol policy remains in `tus_protocol`.

use axum::{
    body::Body,
    extract::FromRequest,
    http::{HeaderMap, Request},
};
use bytes::Bytes;
use futures_util::stream;
use http_body::Body as _;
use http_body_util::BodyExt;

use tus_protocol::{BodyFrame, RequestBody};

use crate::error::TusRejection as AxumError;

/// Extracted TUS body mapped to protocol body frames.
#[non_exhaustive]
pub struct TusBody {
    body: RequestBody,
}

impl TusBody {
    /// Creates a new TusBody for requests where no body was supplied.
    pub fn absent() -> Self {
        Self {
            body: RequestBody::absent(),
        }
    }

    /// Creates a new TusBody with buffered data and optional trailers.
    pub fn buffered(bytes: Bytes, trailers: Option<HeaderMap>) -> Self {
        let body = match trailers {
            Some(trailers) => {
                let frames = stream::iter([
                    Ok(BodyFrame::Data(bytes)),
                    Ok(BodyFrame::Trailers(trailers)),
                ]);
                RequestBody::from_stream(Box::pin(frames))
            }
            None => RequestBody::from_bytes(bytes),
        };

        Self { body }
    }

    /// Returns the protocol request body.
    pub fn into_body(self) -> RequestBody {
        self.body
    }
}

// Manual Debug implementation - the wrapped body may be an opaque stream.
impl std::fmt::Debug for TusBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TusBody").finish_non_exhaustive()
    }
}

impl<S> FromRequest<S> for TusBody
where
    S: Send + Sync,
{
    type Rejection = AxumError;

    async fn from_request(req: Request<Body>, _state: &S) -> Result<Self, Self::Rejection> {
        let (parts, body) = req.into_parts();
        let supplied = body_is_supplied(&parts.headers, &body);
        if !supplied {
            return Ok(TusBody::absent());
        }

        let stream = stream::unfold(body, |mut body| async {
            body.frame().await.map(|frame| {
                // Body read failures pass through as io errors with their
                // source chain intact. The protocol surfaces them as
                // `tus_protocol::Error::Io` (client disconnects stay 500-class
                // IO errors), and the axum error bridge (`crate::error`)
                // walks the preserved chain to answer 413 instead when a
                // transport body-limit layer tripped mid-stream
                // (`http_body_util::LengthLimitError`).
                let frame = frame.map_err(std::io::Error::other).and_then(|frame| {
                    match frame.into_data() {
                        Ok(bytes) => Ok(BodyFrame::Data(bytes)),
                        Err(frame) => frame
                            .into_trailers()
                            .map(BodyFrame::Trailers)
                            .map_err(|_| std::io::Error::other("unsupported body frame")),
                    }
                });

                (frame, body)
            })
        });

        Ok(TusBody {
            body: RequestBody::from_stream(Box::pin(stream)),
        })
    }
}

fn body_is_supplied(headers: &HeaderMap, body: &Body) -> bool {
    if has_offset_content_type(headers) {
        return true;
    }

    if let Some(content_length) = content_length(headers) {
        return content_length > 0;
    }

    has_chunked_transfer_encoding(headers) || !body.is_end_stream()
}

fn has_offset_content_type(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|content_type| content_type.starts_with("application/offset+octet-stream"))
}

fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn has_chunked_transfer_encoding(headers: &HeaderMap) -> bool {
    headers.get_all("transfer-encoding").iter().any(|value| {
        value.to_str().ok().is_some_and(|value| {
            value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use futures::StreamExt;
    use http_body_util::{BodyExt, Full};
    use std::convert::Infallible;
    use tus_protocol::{BodyFrame, RequestBody};

    #[test]
    fn buffered_empty_body_is_supplied() {
        let body = TusBody::buffered(Bytes::new(), None).into_body();

        assert!(body.is_supplied());
        assert!(matches!(body, RequestBody::Bytes(bytes) if bytes.is_empty()));
    }

    #[tokio::test]
    async fn buffered_body_can_include_trailers() {
        let mut trailers = HeaderMap::new();
        trailers.insert(
            "upload-checksum",
            HeaderValue::from_static("sha1 qvTGHdzF6KLavt4PO0gs2a6pQ00="),
        );

        let body = TusBody::buffered(Bytes::from_static(b"hello"), Some(trailers)).into_body();
        let RequestBody::Stream(mut stream) = body else {
            panic!("buffered body with trailers should be streamed as protocol frames");
        };

        let first = stream.next().await.unwrap().unwrap();
        assert!(matches!(first, BodyFrame::Data(bytes) if bytes == Bytes::from_static(b"hello")));

        let second = stream.next().await.unwrap().unwrap();
        assert!(
            matches!(second, BodyFrame::Trailers(headers) if headers.contains_key("upload-checksum"))
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn empty_axum_body_without_body_headers_is_absent() {
        let request = Request::builder().body(Body::empty()).unwrap();

        let body = TusBody::from_request(request, &())
            .await
            .unwrap()
            .into_body();

        assert!(matches!(body, RequestBody::Absent));
    }

    #[tokio::test]
    async fn content_length_zero_axum_body_is_absent_even_before_end_stream() {
        let empty_stream = futures::stream::empty::<Result<Bytes, Infallible>>();
        let request = Request::builder()
            .header("content-length", "0")
            .body(Body::from_stream(empty_stream))
            .unwrap();

        let body = TusBody::from_request(request, &())
            .await
            .unwrap()
            .into_body();

        assert!(matches!(body, RequestBody::Absent));
    }

    #[tokio::test]
    async fn offset_content_type_marks_empty_axum_body_supplied() {
        let request = Request::builder()
            .header("content-type", "application/offset+octet-stream")
            .body(Body::empty())
            .unwrap();

        let body = TusBody::from_request(request, &())
            .await
            .unwrap()
            .into_body();

        assert!(body.is_supplied());
        let RequestBody::Stream(mut stream) = body else {
            panic!("empty offset-content body should be represented as supplied stream");
        };
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn axum_body_frames_are_mapped_to_protocol_frames() {
        let mut trailers = HeaderMap::new();
        trailers.insert(
            "upload-checksum",
            HeaderValue::from_static("sha1 qvTGHdzF6KLavt4PO0gs2a6pQ00="),
        );
        let body = Full::new(Bytes::from_static(b"hello"))
            .with_trailers(std::future::ready(Some(Ok::<_, Infallible>(trailers))))
            .map_err(|never| match never {});
        let request = Request::builder().body(Body::new(body)).unwrap();

        let body = TusBody::from_request(request, &())
            .await
            .unwrap()
            .into_body();
        let RequestBody::Stream(mut stream) = body else {
            panic!("extracted axum body should be streamed as protocol frames");
        };

        let first = stream.next().await.unwrap().unwrap();
        assert!(matches!(first, BodyFrame::Data(bytes) if bytes == Bytes::from_static(b"hello")));

        let second = stream.next().await.unwrap().unwrap();
        assert!(
            matches!(second, BodyFrame::Trailers(headers) if headers.contains_key("upload-checksum"))
        );
        assert!(stream.next().await.is_none());
    }
}
