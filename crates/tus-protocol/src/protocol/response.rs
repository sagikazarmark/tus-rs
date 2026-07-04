//! Framework-neutral response type for TUS protocol handlers.
//!
//! Core handlers in the [`protocol`](super) module return [`Response`].
//! Adapters convert it into their framework's response type (axum, worker,
//! hyper, ...).
//!
//! Response bodies in TUS are always small (location URL, error text, or
//! empty); there is no streaming on the response side, so a plain
//! [`Bytes`] buffer is sufficient.

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};

use crate::config::TUS_RESUMABLE;

/// Protocol-owned success response headers that browser clients may need to read.
///
/// Framework adapters should use this list for CORS exposure instead of
/// maintaining their own copy. Hook-added headers are application-specific and
/// intentionally excluded.
pub const TUS_SUCCESS_RESPONSE_HEADERS: &[&str] = &[
    "tus-resumable",
    "tus-version",
    "tus-extension",
    "tus-max-size",
    "tus-checksum-algorithm",
    "upload-offset",
    "upload-length",
    "upload-defer-length",
    "upload-concat",
    "upload-expires",
    "upload-metadata",
    "location",
];

/// A framework-neutral HTTP response produced by a TUS protocol handler.
///
/// The adapter layer is responsible for turning this into a
/// framework-specific response (e.g. `axum::response::Response`).
#[derive(Debug)]
#[non_exhaustive]
pub struct Response {
    /// HTTP status code.
    pub status: StatusCode,
    /// Response headers. Always includes `Tus-Resumable` when constructed via
    /// [`Response::new`].
    pub headers: HeaderMap,
    /// Response body. Empty for most TUS responses.
    pub body: Bytes,
}

impl Response {
    /// Builds a new response with the given status and the mandatory
    /// `Tus-Resumable: 1.0.0` header pre-set. Body is empty.
    pub fn new(status: StatusCode) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert("tus-resumable", HeaderValue::from_static(TUS_RESUMABLE));
        Self {
            status,
            headers,
            body: Bytes::new(),
        }
    }

    /// Sets a header, replacing any existing value.
    ///
    /// Drops the header (with a warning log) if the value is invalid; header
    /// values must never carry unvalidated bytes into the response.
    #[must_use]
    pub fn with_header(mut self, name: &'static str, value: impl AsRef<str>) -> Self {
        match HeaderValue::from_str(value.as_ref()) {
            Ok(v) => {
                self.headers.insert(HeaderName::from_static(name), v);
            }
            Err(_) => {
                tracing::warn!(header = name, "dropping response header with invalid value");
            }
        }
        self
    }

    /// Sets a header by owned name (for caller-supplied strings from hooks).
    ///
    /// Drops the header (with a warning log) if the name or value is invalid;
    /// hook-supplied strings must never carry unvalidated bytes into the
    /// response.
    #[must_use]
    pub fn with_header_owned(mut self, name: String, value: String) -> Self {
        match (
            HeaderName::try_from(name.as_str()),
            HeaderValue::try_from(value.as_str()),
        ) {
            (Ok(n), Ok(v)) => {
                self.headers.insert(n, v);
            }
            _ => {
                tracing::warn!(header = %name, "dropping response header with invalid name or value");
            }
        }
        self
    }

    /// Sets the response body.
    #[must_use]
    pub fn with_body(mut self, body: Bytes) -> Self {
        self.body = body;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_includes_tus_resumable() {
        let response = Response::new(StatusCode::CREATED);
        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(
            response.headers.get("tus-resumable").unwrap(),
            TUS_RESUMABLE
        );
    }

    #[test]
    fn with_header_sets_value() {
        let response = Response::new(StatusCode::OK)
            .with_header("upload-offset", "42")
            .with_header("location", "/files/abc");
        assert_eq!(response.headers.get("upload-offset").unwrap(), "42");
        assert_eq!(response.headers.get("location").unwrap(), "/files/abc");
    }

    #[test]
    fn with_body_sets_body() {
        let response = Response::new(StatusCode::OK).with_body(Bytes::from_static(b"hello"));
        assert_eq!(response.body.as_ref(), b"hello");
    }
}
