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
use futures::stream;
use http_body_util::BodyExt;

use tus_protocol::{BodyFrame, RequestBody};

use crate::error::Error as AxumError;

/// Extracted TUS body mapped to protocol body frames.
#[non_exhaustive]
pub struct TusBody {
    body: RequestBody,
}

impl TusBody {
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

impl<S> FromRequest<S> for TusBody
where
    S: Send + Sync,
{
    type Rejection = AxumError;

    async fn from_request(req: Request<Body>, _state: &S) -> Result<Self, Self::Rejection> {
        let (_parts, body) = req.into_parts();
        let stream = stream::unfold(body, |mut body| async {
            body.frame().await.map(|frame| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use futures::StreamExt;
    use http_body_util::{BodyExt, Full};
    use std::convert::Infallible;
    use tus_protocol::{BodyFrame, RequestBody};

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
