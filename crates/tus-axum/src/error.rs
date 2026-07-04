//! Error conversion between [`tus_protocol::Error`] and axum responses.

use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
};

/// Wrapper around [`tus_protocol::Error`] that carries axum's [`IntoResponse`] impl.
///
/// Construct through `From<tus_protocol::Error>`; the conversion is where
/// transport-level body-limit failures are remapped to a 413 response (see
/// [`Error::into_inner`]). The inner error is private so the remap invariant
/// is enforced by construction — read it through [`Error::inner`] or
/// [`Error::into_inner`].
#[derive(Debug)]
pub struct Error {
    inner: tus_protocol::Error,
    /// Response body override used when the inner error was remapped from a
    /// transport body-limit failure and the variant's own message would be
    /// misleading (the transport does not know the real sizes).
    body_override: Option<&'static str>,
}

/// Response body for requests rejected by a transport-level body limit.
///
/// The transport layer does not expose the configured limit or the observed
/// size, so the body carries a plain description instead of the protocol's
/// "upload size N exceeds maximum M" message.
const BODY_LIMIT_EXCEEDED_BODY: &str = "request body exceeds the configured body size limit";

impl Error {
    /// Returns a reference to the wrapped protocol error.
    pub fn inner(&self) -> &tus_protocol::Error {
        &self.inner
    }

    /// Consumes the wrapper and returns the protocol error.
    ///
    /// If the error was remapped from a transport body-limit failure, this is
    /// [`tus_protocol::Error::SizeExceeded`] with zeroed sizes: the transport
    /// layer does not know the configured limit or the observed size, and the
    /// 413 status is the meaningful part.
    pub fn into_inner(self) -> tus_protocol::Error {
        self.inner
    }
}

impl AsRef<tus_protocol::Error> for Error {
    fn as_ref(&self) -> &tus_protocol::Error {
        &self.inner
    }
}

impl From<tus_protocol::Error> for Error {
    fn from(err: tus_protocol::Error) -> Self {
        match map_transport_body_error(err) {
            (inner, true) => Self {
                inner,
                body_override: Some(BODY_LIMIT_EXCEEDED_BODY),
            },
            (inner, false) => Self {
                inner,
                body_override: None,
            },
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.body_override {
            Some(body) => f.write_str(body),
            None => self.inner.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.body_override {
            // The remapped variant carries no source; point at the protocol
            // error itself so the chain stays inspectable.
            Some(_) => Some(&self.inner),
            None => std::error::Error::source(&self.inner),
        }
    }
}

/// Remaps transport-level body read failures that reach the protocol as
/// opaque IO errors. Returns the (possibly remapped) error and whether the
/// remap happened.
///
/// Body-limiting middleware such as `tower_http::limit::RequestBodyLimitLayer`
/// enforces its byte cap inside the request body: a chunked upload that trips
/// the cap mid-stream surfaces as an [`http_body_util::LengthLimitError`]
/// buried in the body read error chain, which the protocol classifies as an
/// internal error (500). The client sent too many bytes, so the correct
/// answer is 413 ([`tus_protocol::Error::SizeExceeded`]).
///
/// The transport layer does not expose the configured limit or the observed
/// size, so the remapped variant carries zeroes and the response body is
/// overridden with [`BODY_LIMIT_EXCEEDED_BODY`]; the status code is the
/// meaningful part. All other errors (including plain client-disconnect read
/// errors, which arrive as [`tus_protocol::Error::Io`]) pass through
/// unchanged.
fn map_transport_body_error(err: tus_protocol::Error) -> (tus_protocol::Error, bool) {
    match err {
        tus_protocol::Error::Io(io_err) if chain_contains_length_limit(&io_err) => {
            (tus_protocol::Error::SizeExceeded { size: 0, max: 0 }, true)
        }
        other => (other, false),
    }
}

/// Returns whether the error's source chain contains an
/// [`http_body_util::LengthLimitError`].
fn chain_contains_length_limit(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(err) = current {
        if err.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        current = err.source();
    }
    false
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, headers, body) = self.inner.response_parts();
        let body = match self.body_override {
            Some(body) => body.to_string(),
            None => body,
        };

        let mut builder = Response::builder()
            .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR));

        for (name, value) in headers {
            builder = builder.header(name, value);
        }

        builder
            .body(Body::from(body))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;
    use tus_protocol::Error as ProtocolError;

    #[test]
    fn from_protocol_error_wraps_inner() {
        let err: Error = ProtocolError::NotFound("upload-7".into()).into();
        assert!(matches!(err.inner(), ProtocolError::NotFound(s) if s == "upload-7"));
        assert!(matches!(err.as_ref(), ProtocolError::NotFound(_)));
        assert!(matches!(err.into_inner(), ProtocolError::NotFound(s) if s == "upload-7"));
    }

    #[test]
    fn length_required_maps_to_411() {
        let response =
            <Error as From<ProtocolError>>::from(ProtocolError::LengthRequired).into_response();
        assert_eq!(response.status(), StatusCode::LENGTH_REQUIRED);
    }

    /// Produces the `LengthLimitError` a limited transport body yields when
    /// its byte cap trips. The error type is `#[non_exhaustive]`, so the only
    /// way to obtain one is to actually poll an over-limit body.
    async fn length_limit_error() -> Box<dyn std::error::Error + Send + Sync> {
        use http_body_util::{BodyExt, Full, Limited};

        let mut body = Limited::new(Full::new(bytes::Bytes::from_static(b"hello")), 1);
        match body.frame().await {
            Some(Err(err)) => err,
            other => panic!("expected a length limit error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn io_error_wrapping_length_limit_maps_to_413() {
        // Mirrors the exact chain the body bridge produces when
        // tower_http::limit::RequestBodyLimitLayer trips mid-stream:
        // io::Error -> axum::Error -> LengthLimitError.
        let io_err = std::io::Error::other(axum::Error::new(length_limit_error().await));

        let err: Error = ProtocolError::Io(io_err).into();
        assert!(matches!(
            err.inner(),
            ProtocolError::SizeExceeded { size: 0, max: 0 }
        ));

        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    /// The remapped 413 must not claim "upload size 0 exceeds maximum 0";
    /// the transport does not know the sizes, so the body carries a plain
    /// description instead.
    #[tokio::test]
    async fn transport_body_limit_413_has_sensible_body() {
        let io_err = std::io::Error::other(axum::Error::new(length_limit_error().await));
        let err: Error = ProtocolError::Io(io_err).into();
        assert_eq!(err.to_string(), BODY_LIMIT_EXCEEDED_BODY);

        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            BODY_LIMIT_EXCEEDED_BODY
        );
    }

    /// The remapped 413 keeps the protocol's error response headers
    /// (`Tus-Resumable` and friends) even though the body is overridden.
    #[tokio::test]
    async fn transport_body_limit_413_keeps_protocol_headers() {
        let io_err = std::io::Error::other(axum::Error::new(length_limit_error().await));
        let err: Error = ProtocolError::Io(io_err).into();

        let (_, expected_headers, _) =
            ProtocolError::SizeExceeded { size: 0, max: 0 }.response_parts();
        let response = err.into_response();
        for (name, value) in &expected_headers {
            assert_eq!(
                response.headers().get(*name).unwrap().to_str().unwrap(),
                value,
                "header {name} mismatch"
            );
        }
    }

    #[test]
    fn plain_io_error_is_not_remapped() {
        let err: Error = ProtocolError::Io(std::io::Error::other("connection reset")).into();
        assert!(matches!(err.inner(), ProtocolError::Io(_)));

        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn display_and_source_delegate_to_inner_for_plain_errors() {
        let err: Error = ProtocolError::NotFound("upload-7".into()).into();
        assert_eq!(
            err.to_string(),
            ProtocolError::NotFound("upload-7".into()).to_string()
        );
    }

    /// Constructors for every ProtocolError variant. Mirrors the helper in
    /// tus-protocol so adding a new variant forces a thoughtful update here
    /// when the parity test catches a missing case.
    fn variant_constructors() -> Vec<Box<dyn Fn() -> ProtocolError>> {
        vec![
            Box::new(|| ProtocolError::NotFound("x".into())),
            Box::new(|| ProtocolError::AlreadyExists("x".into())),
            Box::new(|| ProtocolError::OffsetMismatch {
                expected: 5,
                actual: 3,
            }),
            Box::new(|| ProtocolError::SizeExceeded { size: 100, max: 50 }),
            Box::new(|| ProtocolError::InvalidContentType {
                expected: "application/offset+octet-stream".into(),
                actual: "text/plain".into(),
            }),
            Box::new(|| ProtocolError::MissingHeader("Upload-Offset")),
            Box::new(|| ProtocolError::MissingTusResumable),
            Box::new(|| ProtocolError::UnsupportedTusVersion("9.9.9".into())),
            Box::new(|| ProtocolError::InvalidHeader {
                header: "Upload-Length",
                message: "not a number".into(),
            }),
            Box::new(|| ProtocolError::Locked("x".into())),
            Box::new(|| ProtocolError::LockTimeout("x".into())),
            Box::new(|| ProtocolError::Expired("x".into())),
            Box::new(|| ProtocolError::ChecksumMismatch {
                expected: "abc".into(),
                actual: "def".into(),
            }),
            Box::new(|| ProtocolError::UnsupportedChecksum("xyz".into())),
            Box::new(|| ProtocolError::ExtensionNotSupported("foo".into())),
            Box::new(|| ProtocolError::InvalidMetadata("bad".into())),
            Box::new(|| ProtocolError::ConcatenationError("bad".into())),
            Box::new(|| ProtocolError::NotPartialUpload("x".into())),
            Box::new(|| ProtocolError::IncompleteUpload("x".into())),
            Box::new(|| ProtocolError::RangeNotSatisfiable { size: 1024 }),
            Box::new(|| ProtocolError::StorageKeyMissing),
            Box::new(|| ProtocolError::Storage(Box::new(std::io::Error::other("x")))),
            Box::new(|| ProtocolError::StateStore(Box::new(std::io::Error::other("x")))),
            Box::new(|| ProtocolError::Hook(Box::new(std::io::Error::other("x")))),
            Box::new(|| ProtocolError::HookRejected {
                status_code: 418,
                message: "teapot".into(),
            }),
            Box::new(|| ProtocolError::LengthRequired),
            Box::new(|| ProtocolError::Io(std::io::Error::other("x"))),
            Box::new(|| ProtocolError::Internal("x".into())),
            Box::new(|| ProtocolError::MethodNotAllowed("PATCH".into())),
            Box::new(|| ProtocolError::FinalUploadModificationForbidden("x".into())),
            Box::new(|| ProtocolError::CompletedUploadModificationForbidden("x".into())),
            Box::new(|| ProtocolError::InvalidUploadId("contains NUL".into())),
        ]
    }

    /// End-to-end parity: every variant routed through Error's
    /// IntoResponse impl must produce a Response whose status, header set,
    /// and body bytes exactly match the framework-neutral tuple from
    /// ProtocolError::response_parts(). This proves the axum bridge does not
    /// add or lose information. (The single deliberate exception — the body
    /// override for transport body-limit remaps — is covered by
    /// `transport_body_limit_413_has_sensible_body`; none of the variants
    /// here trigger it.)
    #[tokio::test]
    async fn into_response_matches_response_parts_for_all_variants() {
        for make in variant_constructors() {
            let parts_err = make();
            let (expected_status, expected_headers, expected_body) = parts_err.response_parts();

            let response = <Error as From<ProtocolError>>::from(make()).into_response();
            let actual_status = response.status().as_u16();
            assert_eq!(actual_status, expected_status, "status mismatch");

            assert_eq!(
                response.headers().len(),
                expected_headers.len(),
                "header count mismatch (expected {:?}, got {:?})",
                expected_headers,
                response.headers(),
            );
            for (name, value) in &expected_headers {
                let actual = response
                    .headers()
                    .get(*name)
                    .unwrap_or_else(|| panic!("response missing header {name}"))
                    .to_str()
                    .expect("header value not ASCII");
                assert_eq!(actual, value, "header {name} value mismatch");
            }

            let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
            let body_str = std::str::from_utf8(&body_bytes).unwrap();
            assert_eq!(body_str, expected_body, "body bytes mismatch");
        }
    }
}
