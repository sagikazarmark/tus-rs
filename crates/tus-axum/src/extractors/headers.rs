//! Axum extractors that wrap the framework-neutral [`tus_protocol::Headers`] parsing.
//!
//! [`Headers`] is a newtype around [`tus_protocol::Headers`] so the
//! [`FromRequestParts`] impl can live here without violating the orphan rule.

use axum::{extract::FromRequestParts, http::request::Parts};

use crate::error::Error;

/// Axum extractor that parses TUS request headers and validates
/// `Tus-Resumable: 1.0.0`.
///
/// Use for handlers (POST, PATCH, HEAD, DELETE) that require the version
/// header. Destructure inside the handler signature to reach the inner
/// [`tus_protocol::Headers`]:
///
/// ```rust,no_run
/// # use tus_axum::Headers;
/// async fn handler(Headers(_headers): Headers) {
///     // handler body
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Headers(pub tus_protocol::Headers);

impl<S> FromRequestParts<S> for Headers
where
    S: Send + Sync,
{
    type Rejection = Error;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        tus_protocol::Headers::from_headers(&parts.headers)
            .map(Self)
            .map_err(Error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap as AxumHeaderMap, HeaderValue, Request};

    fn create_request_parts(headers: AxumHeaderMap) -> Parts {
        let request = Request::builder().body(()).unwrap();
        let (mut parts, _) = request.into_parts();
        parts.headers = headers;
        parts
    }

    #[tokio::test]
    async fn extractor_delegates_to_protocol_layer() {
        let mut headers = AxumHeaderMap::new();
        headers.insert("tus-resumable", HeaderValue::from_static("1.0.0"));
        headers.insert("upload-offset", HeaderValue::from_static("100"));

        let mut parts = create_request_parts(headers);
        let Headers(tus) = Headers::from_request_parts(&mut parts, &()).await.unwrap();
        assert_eq!(tus.upload_offset, Some(100));
    }

    #[tokio::test]
    async fn extractor_rejects_missing_tus_resumable() {
        let mut parts = create_request_parts(AxumHeaderMap::new());
        let result = Headers::from_request_parts(&mut parts, &()).await;
        assert!(result.is_err());
    }
}
