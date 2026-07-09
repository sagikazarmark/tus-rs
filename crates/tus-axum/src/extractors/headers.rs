//! Axum extractors that wrap the framework-neutral [`tus_protocol::Headers`] parsing.
//!
//! [`TusHeaders`] is a newtype around [`tus_protocol::Headers`] so the
//! [`FromRequestParts`] impl can live here without violating the orphan rule.

use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, request::Parts},
};

use crate::error::TusRejection;

/// Axum extractor that parses TUS request headers and validates
/// `Tus-Resumable: 1.0.0`.
///
/// A thin newtype over [`tus_protocol::Headers`], existing only so the
/// [`FromRequestParts`] impl can live in this crate (orphan rule). The
/// `Tus` prefix keeps it from colliding with the protocol type when both are
/// in scope.
///
/// Use for handlers (POST, PATCH, HEAD, DELETE) that require the version
/// header. Destructure inside the handler signature to reach the inner
/// [`tus_protocol::Headers`]:
///
/// ```rust,no_run
/// # use tus_axum::TusHeaders;
/// async fn handler(TusHeaders(_headers): TusHeaders) {
///     // handler body
/// }
/// ```
///
/// The `pub` field is a deliberate 1.0 commitment so this destructuring works,
/// matching every other axum extractor. See ADR 0006.
#[derive(Debug, Clone)]
pub struct TusHeaders(pub tus_protocol::Headers);

impl TusHeaders {
    pub(crate) fn from_header_map(headers: &HeaderMap) -> Result<Self, TusRejection> {
        tus_protocol::Headers::from_headers(headers)
            .map(Self)
            .map_err(TusRejection::from)
    }
}

impl<S> FromRequestParts<S> for TusHeaders
where
    S: Send + Sync,
{
    type Rejection = TusRejection;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Self::from_header_map(&parts.headers)
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
        let TusHeaders(tus) = TusHeaders::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        assert_eq!(tus.upload_offset, Some(100));
    }

    #[tokio::test]
    async fn extractor_rejects_missing_tus_resumable() {
        let mut parts = create_request_parts(AxumHeaderMap::new());
        let result = TusHeaders::from_request_parts(&mut parts, &()).await;
        assert!(result.is_err());
    }
}
