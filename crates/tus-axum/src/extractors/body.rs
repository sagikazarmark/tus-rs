//! TUS body extractor.
//!
//! This module provides the [`TusBody`] extractor that handles both streaming
//! bodies and checksum-trailer intent detection.

use axum::{
    body::Body,
    extract::FromRequest,
    http::{HeaderMap, Request},
};
use bytes::Bytes;

use tus_protocol::{ChecksumAlgorithm, Error};

use crate::error::Error as AxumError;

/// The type of body data extracted.
#[derive(Debug)]
#[non_exhaustive]
pub enum BodyData {
    /// Streaming body - data is read on demand.
    Stream(Body),
    /// Buffered body - data was collected to extract trailers.
    Buffered(Bytes),
}

/// Extracted TUS body with optional trailers.
///
/// This extractor handles the TUS checksum-trailer extension by:
/// - Detecting the `Trailer: Upload-Checksum` header
/// - Recording checksum-trailer intent without buffering the body
/// - Returning the body as a stream so handlers can enforce protocol limits
#[derive(Debug)]
#[non_exhaustive]
pub struct TusBody {
    /// The body data (streaming or buffered).
    pub data: BodyData,
    /// Trailer headers extracted from the body.
    pub trailers: Option<HeaderMap>,
    /// Whether the request declared or supplied an Upload-Checksum trailer.
    pub checksum_trailer_declared: bool,
}

impl TusBody {
    /// Creates a new TusBody with buffered data and optional trailers.
    pub fn buffered(bytes: Bytes, trailers: Option<HeaderMap>) -> Self {
        let checksum_trailer_declared = trailers
            .as_ref()
            .map(|trailers| trailers.contains_key("upload-checksum"))
            .unwrap_or(false)
            || trailers.is_some();

        Self {
            data: BodyData::Buffered(bytes),
            trailers,
            checksum_trailer_declared,
        }
    }

    /// Returns true if the request declared checksum trailers or has trailers.
    pub fn has_trailers(&self) -> bool {
        self.checksum_trailer_declared || self.trailers.is_some()
    }

    /// Extracts the Upload-Checksum from trailers if present.
    pub fn trailer_checksum(&self) -> Result<Option<(ChecksumAlgorithm, Vec<u8>)>, Error> {
        let trailers = match &self.trailers {
            Some(t) => t,
            None => return Ok(None),
        };

        let value = match trailers
            .get("upload-checksum")
            .and_then(|v| v.to_str().ok())
        {
            Some(v) => v,
            None => return Ok(None),
        };

        let parts: Vec<&str> = value.splitn(2, ' ').collect();
        if parts.len() != 2 {
            return Err(Error::InvalidHeader {
                header: "Upload-Checksum",
                message: "expected 'algorithm checksum' format".to_string(),
            });
        }

        let algorithm = ChecksumAlgorithm::parse(parts[0])
            .ok_or_else(|| Error::UnsupportedChecksum(parts[0].to_string()))?;

        use base64::Engine;
        let checksum = base64::engine::general_purpose::STANDARD
            .decode(parts[1])
            .map_err(|e| Error::InvalidHeader {
                header: "Upload-Checksum",
                message: format!("invalid base64: {}", e),
            })?;

        Ok(Some((algorithm, checksum)))
    }
}

/// Checks if the request has a Trailer header indicating Upload-Checksum.
fn has_checksum_trailer(headers: &HeaderMap) -> bool {
    headers
        .get("trailer")
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_lowercase())
                .any(|s| s == "upload-checksum")
        })
        .unwrap_or(false)
}

impl<S> FromRequest<S> for TusBody
where
    S: Send + Sync,
{
    type Rejection = AxumError;

    async fn from_request(req: Request<Body>, _state: &S) -> Result<Self, Self::Rejection> {
        let (parts, body) = req.into_parts();

        Ok(TusBody {
            data: BodyData::Stream(body),
            trailers: None,
            checksum_trailer_declared: has_checksum_trailer(&parts.headers),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::HeaderValue;

    #[test]
    fn test_has_checksum_trailer() {
        let mut headers = HeaderMap::new();
        headers.insert("trailer", HeaderValue::from_static("Upload-Checksum"));
        assert!(has_checksum_trailer(&headers));

        let mut headers = HeaderMap::new();
        headers.insert("trailer", HeaderValue::from_static("upload-checksum"));
        assert!(has_checksum_trailer(&headers));

        let mut headers = HeaderMap::new();
        headers.insert(
            "trailer",
            HeaderValue::from_static("Content-MD5, Upload-Checksum"),
        );
        assert!(has_checksum_trailer(&headers));

        let headers = HeaderMap::new();
        assert!(!has_checksum_trailer(&headers));

        let mut headers = HeaderMap::new();
        headers.insert("trailer", HeaderValue::from_static("Content-MD5"));
        assert!(!has_checksum_trailer(&headers));
    }

    #[tokio::test]
    async fn checksum_trailer_request_remains_streaming() {
        let request = Request::builder()
            .header("trailer", "Upload-Checksum")
            .body(Body::from(Bytes::from_static(b"hello")))
            .unwrap();

        let body = TusBody::from_request(request, &()).await.unwrap();

        assert!(matches!(body.data, BodyData::Stream(_)));
        assert!(body.checksum_trailer_declared);
        assert!(body.has_trailers());
        assert!(body.trailers.is_none());
    }
}
