//! TUS protocol error types.
//!
//! This module defines all error types that can occur during TUS operations.
//! Each error variant includes the appropriate HTTP status code to return to clients.
//!
//! Framework adapters (`tus-axum`, `tus-worker-example`, etc.) build their
//! HTTP responses from [`Error::response_parts`]. This crate intentionally
//! has no dependency on a specific HTTP framework.

/// Errors that can occur during TUS protocol operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Upload not found (404 Not Found).
    #[error("upload not found: {0}")]
    NotFound(String),

    /// Upload already exists (409 Conflict).
    #[error("upload already exists: {0}")]
    AlreadyExists(String),

    /// Offset mismatch (409 Conflict).
    #[error("offset mismatch: expected {expected}, got {actual}")]
    OffsetMismatch {
        /// Offset the server expected the client to upload next.
        expected: u64,
        /// Offset the client actually supplied.
        actual: u64,
    },

    /// Upload size exceeds maximum (413 Payload Too Large).
    #[error("upload size {size} exceeds maximum {max}")]
    SizeExceeded {
        /// Declared or observed upload size.
        size: u64,
        /// Maximum size allowed by the server configuration.
        max: u64,
    },

    /// Invalid content type (415 Unsupported Media Type).
    #[error("invalid content type: expected {expected}, got {actual}")]
    InvalidContentType {
        /// Content type the server required.
        expected: String,
        /// Content type the client supplied.
        actual: String,
    },

    /// Missing required header (400 Bad Request).
    #[error("missing required header: {0}")]
    MissingHeader(&'static str),

    /// Missing Tus-Resumable header (412 Precondition Failed).
    #[error("missing Tus-Resumable header")]
    MissingTusResumable,

    /// Unsupported TUS version (412 Precondition Failed).
    #[error("unsupported TUS version: {0}")]
    UnsupportedTusVersion(String),

    /// Invalid header value (400 Bad Request).
    #[error("invalid header value for {header}: {message}")]
    InvalidHeader {
        /// Name of the header that failed validation.
        header: &'static str,
        /// Human-readable description of why the value was rejected.
        message: String,
    },

    /// Upload is locked by another operation (423 Locked).
    #[error("upload is locked: {0}")]
    Locked(String),

    /// Lock acquisition timeout (423 Locked).
    #[error("lock acquisition timeout for upload: {0}")]
    LockTimeout(String),

    /// Lock operation failed (500 Internal Server Error).
    #[error("lock error: {0}")]
    Lock(String),

    /// State operation failed (500 Internal Server Error).
    #[error("state error: {0}")]
    State(String),

    /// Upload has expired (410 Gone).
    #[error("upload has expired: {0}")]
    Expired(String),

    /// Checksum mismatch (460 Checksum Mismatch - TUS specific).
    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch {
        /// Checksum value supplied by the client (base64).
        expected: String,
        /// Checksum value computed by the server (base64).
        actual: String,
    },

    /// Unsupported checksum algorithm (400 Bad Request).
    #[error("unsupported checksum algorithm: {0}")]
    UnsupportedChecksum(String),

    /// Extension not supported (400 Bad Request).
    #[error("extension not supported: {0}")]
    ExtensionNotSupported(String),

    /// Invalid metadata format (400 Bad Request).
    #[error("invalid metadata: {0}")]
    InvalidMetadata(String),

    /// Concatenation error (400 Bad Request).
    #[error("concatenation error: {0}")]
    ConcatenationError(String),

    /// Partial upload required for concatenation (400 Bad Request).
    #[error("upload {0} is not a partial upload")]
    NotPartialUpload(String),

    /// Upload is incomplete (400 Bad Request).
    #[error("upload {0} is incomplete")]
    IncompleteUpload(String),

    /// Requested byte range cannot be satisfied (416 Range Not Satisfiable).
    #[error("range not satisfiable for resource of size {size}")]
    RangeNotSatisfiable {
        /// Total resource size in bytes.
        size: u64,
    },

    /// Storage key not set (500 Internal Server Error).
    #[error("storage key not set for upload")]
    StorageKeyMissing,

    /// Storage operation failed (500 Internal Server Error).
    #[error("storage error: {0}")]
    Storage(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// State store operation failed (500 Internal Server Error).
    #[error("state store error: {0}")]
    StateStore(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Hook execution failed (500 Internal Server Error or hook-determined).
    #[error("hook error: {0}")]
    Hook(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Hook rejected the operation (hook-determined status code).
    #[error("hook rejected: {message}")]
    HookRejected {
        /// HTTP status code the hook wants the server to return.
        status_code: u16,
        /// Human-readable rejection message.
        message: String,
    },

    /// IO error (500 Internal Server Error).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Internal error (500 Internal Server Error).
    #[error("internal error: {0}")]
    Internal(String),

    /// Method not allowed (405 Method Not Allowed).
    #[error("method not allowed: {0}")]
    MethodNotAllowed(String),

    /// Cannot modify a final concatenated upload (403 Forbidden).
    #[error("cannot modify final upload: {0}")]
    FinalUploadModificationForbidden(String),

    /// Cannot modify an already-completed upload (403 Forbidden).
    #[error("cannot modify completed upload: {0}")]
    CompletedUploadModificationForbidden(String),

    /// Upload id failed shape validation (400 Bad Request).
    ///
    /// The id was either empty, too long, contained a path
    /// separator, or contained a control character (NUL, etc.).
    /// Returned by [`UploadId`](crate::protocol::UploadId) parsing.
    #[error("invalid upload id: {0}")]
    InvalidUploadId(String),
}

impl Error {
    /// Returns the HTTP status code for this error.
    pub fn status_code(&self) -> u16 {
        match self {
            Error::NotFound(_) => 404,
            Error::AlreadyExists(_) => 409,
            Error::OffsetMismatch { .. } => 409,
            Error::SizeExceeded { .. } => 413,
            Error::InvalidContentType { .. } => 415,
            Error::MissingHeader(_) => 400,
            Error::MissingTusResumable => 412,
            Error::UnsupportedTusVersion(_) => 412,
            Error::InvalidHeader { .. } => 400,
            Error::Locked(_) => 423,
            Error::LockTimeout(_) => 423,
            Error::Lock(_) => 500,
            Error::State(_) => 500,
            Error::Expired(_) => 410,
            Error::ChecksumMismatch { .. } => 460, // TUS-specific status code
            Error::UnsupportedChecksum(_) => 400,
            Error::ExtensionNotSupported(_) => 400,
            Error::InvalidMetadata(_) => 400,
            Error::ConcatenationError(_) => 400,
            Error::NotPartialUpload(_) => 400,
            Error::IncompleteUpload(_) => 400,
            Error::RangeNotSatisfiable { .. } => 416,
            Error::StorageKeyMissing => 500,
            Error::Storage(_) => 500,
            Error::StateStore(_) => 500,
            Error::Hook(_) => 500,
            Error::HookRejected { status_code, .. } => *status_code,
            Error::Io(_) => 500,
            Error::Internal(_) => 500,
            Error::MethodNotAllowed(_) => 405,
            Error::FinalUploadModificationForbidden(_) => 403,
            Error::CompletedUploadModificationForbidden(_) => 403,
            Error::InvalidUploadId(_) => 400,
        }
    }

    /// Returns whether this error should include details in the response body.
    ///
    /// Some errors (like internal server errors) should not expose details to clients.
    pub fn should_expose_details(&self) -> bool {
        !matches!(
            self,
            Error::Storage(_)
                | Error::StateStore(_)
                | Error::Hook(_)
                | Error::Lock(_)
                | Error::State(_)
                | Error::Io(_)
                | Error::Internal(_)
        )
    }

    /// Creates a storage error from any error type.
    pub fn storage<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Error::Storage(Box::new(err))
    }

    /// Creates a state store error from any error type.
    pub fn state_store<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Error::StateStore(Box::new(err))
    }

    /// Creates a hook error from any error type.
    pub fn hook<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Error::Hook(Box::new(err))
    }

    /// Returns the framework-neutral pieces of a TUS-spec-compliant error
    /// response: `(status, headers, body)`.
    ///
    /// The headers vec always contains `tus-resumable`. Some variants append
    /// further headers required by the TUS spec (`tus-version` on version
    /// errors, `upload-offset` on offset mismatches, `content-range` on range
    /// errors). Internal-detail variants return a redacted body string.
    ///
    /// This is the single source of truth for the TUS error→response mapping.
    /// Framework adapters (axum, Cloudflare Workers) build their concrete
    /// `Response` types from this tuple. Keeping the mapping in one place
    /// stops the axum and Worker code paths from drifting.
    pub fn response_parts(&self) -> (u16, Vec<(&'static str, String)>, String) {
        let status = self.status_code();
        let body = if self.should_expose_details() {
            self.to_string()
        } else {
            "Internal server error".to_string()
        };
        let mut headers: Vec<(&'static str, String)> =
            vec![("tus-resumable", crate::config::TUS_RESUMABLE.to_string())];
        match self {
            Error::MissingTusResumable | Error::UnsupportedTusVersion(_) => {
                headers.push(("tus-version", crate::config::TUS_RESUMABLE.to_string()));
            }
            Error::OffsetMismatch { expected, .. } => {
                headers.push(("upload-offset", expected.to_string()));
            }
            Error::RangeNotSatisfiable { size } => {
                headers.push(("content-range", format!("bytes */{size}")));
            }
            _ => {}
        }
        (status, headers, body)
    }
}

/// Result type alias for TUS operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_codes() {
        assert_eq!(Error::NotFound("test".into()).status_code(), 404);
        assert_eq!(Error::AlreadyExists("test".into()).status_code(), 409);
        assert_eq!(
            Error::OffsetMismatch {
                expected: 0,
                actual: 100
            }
            .status_code(),
            409
        );
        assert_eq!(
            Error::SizeExceeded { size: 100, max: 50 }.status_code(),
            413
        );
        assert_eq!(
            Error::CompletedUploadModificationForbidden("test".into()).status_code(),
            403
        );
        assert_eq!(Error::Locked("test".into()).status_code(), 423);
        assert_eq!(Error::Expired("test".into()).status_code(), 410);
        assert_eq!(
            Error::ChecksumMismatch {
                expected: "a".into(),
                actual: "b".into()
            }
            .status_code(),
            460
        );
    }

    #[test]
    fn test_should_expose_details() {
        assert!(Error::NotFound("test".into()).should_expose_details());
        assert!(
            Error::OffsetMismatch {
                expected: 0,
                actual: 100
            }
            .should_expose_details()
        );
        assert!(!Error::Internal("secret".into()).should_expose_details());
    }

    fn header(headers: &[(&'static str, String)], name: &str) -> Option<String> {
        headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    }

    #[test]
    fn response_parts_status_matches_status_code() {
        for make in variant_constructors() {
            let err = make();
            let expected = err.status_code();
            let (actual, _, _) = err.response_parts();
            assert_eq!(actual, expected, "status mismatch for variant");
        }
    }

    #[test]
    fn response_parts_always_includes_tus_resumable() {
        for make in variant_constructors() {
            let err = make();
            let (_, headers, _) = err.response_parts();
            assert_eq!(
                header(&headers, "tus-resumable").as_deref(),
                Some(crate::config::TUS_RESUMABLE),
                "tus-resumable missing or wrong",
            );
        }
    }

    #[test]
    fn response_parts_version_errors_include_tus_version() {
        let (_, headers, _) = Error::MissingTusResumable.response_parts();
        assert_eq!(
            header(&headers, "tus-version").as_deref(),
            Some(crate::config::TUS_RESUMABLE),
        );
        let (_, headers, _) = Error::UnsupportedTusVersion("9.9.9".into()).response_parts();
        assert_eq!(
            header(&headers, "tus-version").as_deref(),
            Some(crate::config::TUS_RESUMABLE),
        );
    }

    #[test]
    fn response_parts_offset_mismatch_includes_upload_offset() {
        let (_, headers, _) = Error::OffsetMismatch {
            expected: 4096,
            actual: 0,
        }
        .response_parts();
        assert_eq!(header(&headers, "upload-offset").as_deref(), Some("4096"),);
    }

    #[test]
    fn response_parts_range_not_satisfiable_includes_content_range() {
        let (_, headers, _) = Error::RangeNotSatisfiable { size: 1024 }.response_parts();
        assert_eq!(
            header(&headers, "content-range").as_deref(),
            Some("bytes */1024"),
        );
    }

    #[test]
    fn response_parts_redacts_internal_error_bodies() {
        let cases = [
            Error::Internal("secret".into()),
            Error::Storage(Box::new(std::io::Error::other("disk on fire"))),
            Error::StateStore(Box::new(std::io::Error::other("redis ate it"))),
            Error::Hook(Box::new(std::io::Error::other("hook crashed"))),
            Error::Lock("oops".into()),
            Error::State("oops".into()),
            Error::Io(std::io::Error::other("eio")),
        ];
        for err in cases {
            let (_, _, body) = err.response_parts();
            assert_eq!(body, "Internal server error", "leaked details");
        }
    }

    #[test]
    fn response_parts_exposes_safe_error_bodies() {
        let err = Error::NotFound("upload-123".into());
        let display = err.to_string();
        let (_, _, body) = err.response_parts();
        assert_eq!(body, display);
    }

    #[test]
    fn response_parts_hook_rejected_uses_provided_status() {
        let (status, _, _) = Error::HookRejected {
            status_code: 451,
            message: "legal".into(),
        }
        .response_parts();
        assert_eq!(status, 451);
    }

    #[test]
    fn response_parts_no_extra_headers_for_unrelated_variants() {
        let (_, headers, _) = Error::NotFound("x".into()).response_parts();
        let names: Vec<&str> = headers.iter().map(|(n, _)| *n).collect();
        assert_eq!(names, vec!["tus-resumable"]);
    }

    /// Constructors for every Error variant. Used by parity and coverage tests
    /// so adding a new variant forces a thoughtful update here.
    fn variant_constructors() -> Vec<Box<dyn Fn() -> Error>> {
        vec![
            Box::new(|| Error::NotFound("x".into())),
            Box::new(|| Error::AlreadyExists("x".into())),
            Box::new(|| Error::OffsetMismatch {
                expected: 5,
                actual: 3,
            }),
            Box::new(|| Error::SizeExceeded { size: 100, max: 50 }),
            Box::new(|| Error::InvalidContentType {
                expected: "application/offset+octet-stream".into(),
                actual: "text/plain".into(),
            }),
            Box::new(|| Error::MissingHeader("Upload-Offset")),
            Box::new(|| Error::MissingTusResumable),
            Box::new(|| Error::UnsupportedTusVersion("9.9.9".into())),
            Box::new(|| Error::InvalidHeader {
                header: "Upload-Length",
                message: "not a number".into(),
            }),
            Box::new(|| Error::Locked("x".into())),
            Box::new(|| Error::LockTimeout("x".into())),
            Box::new(|| Error::Lock("x".into())),
            Box::new(|| Error::State("x".into())),
            Box::new(|| Error::Expired("x".into())),
            Box::new(|| Error::ChecksumMismatch {
                expected: "abc".into(),
                actual: "def".into(),
            }),
            Box::new(|| Error::UnsupportedChecksum("xyz".into())),
            Box::new(|| Error::ExtensionNotSupported("foo".into())),
            Box::new(|| Error::InvalidMetadata("bad".into())),
            Box::new(|| Error::ConcatenationError("bad".into())),
            Box::new(|| Error::NotPartialUpload("x".into())),
            Box::new(|| Error::IncompleteUpload("x".into())),
            Box::new(|| Error::RangeNotSatisfiable { size: 1024 }),
            Box::new(|| Error::StorageKeyMissing),
            Box::new(|| Error::Storage(Box::new(std::io::Error::other("x")))),
            Box::new(|| Error::StateStore(Box::new(std::io::Error::other("x")))),
            Box::new(|| Error::Hook(Box::new(std::io::Error::other("x")))),
            Box::new(|| Error::HookRejected {
                status_code: 418,
                message: "teapot".into(),
            }),
            Box::new(|| Error::Io(std::io::Error::other("x"))),
            Box::new(|| Error::Internal("x".into())),
            Box::new(|| Error::MethodNotAllowed("PATCH".into())),
            Box::new(|| Error::FinalUploadModificationForbidden("x".into())),
            Box::new(|| Error::CompletedUploadModificationForbidden("x".into())),
            Box::new(|| Error::InvalidUploadId("contains NUL".into())),
        ]
    }
}
