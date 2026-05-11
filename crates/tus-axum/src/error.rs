//! Error conversion between [`tus_protocol::Error`] and axum responses.

use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
};

/// Newtype around [`tus_protocol::Error`] that carries axum's [`IntoResponse`] impl.
#[derive(Debug)]
pub struct Error(pub tus_protocol::Error);

impl From<tus_protocol::Error> for Error {
    fn from(err: tus_protocol::Error) -> Self {
        Self(err)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, headers, body) = self.0.response_parts();

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
        assert!(matches!(err.0, ProtocolError::NotFound(ref s) if s == "upload-7"));
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
            Box::new(|| ProtocolError::Lock("x".into())),
            Box::new(|| ProtocolError::State("x".into())),
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
    /// add or lose information.
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
