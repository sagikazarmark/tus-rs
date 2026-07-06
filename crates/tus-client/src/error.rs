//! Error types for the TUS client.

use std::time::Duration;

/// Result type returned by the client.
pub type Result<T> = std::result::Result<T, Error>;

/// Boxed error type carried by [`Error::Transport`].
///
/// On native targets the boxed error must be `Send + Sync` so client futures
/// stay `Send`; on `wasm32` (where futures are not `Send`) the bounds are
/// relaxed, mirroring the rest of the crate.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Boxed error type carried by [`Error::Transport`].
///
/// On native targets the boxed error must be `Send + Sync` so client futures
/// stay `Send`; on `wasm32` (where futures are not `Send`) the bounds are
/// relaxed, mirroring the rest of the crate.
#[cfg(target_arch = "wasm32")]
pub type BoxError = Box<dyn std::error::Error + 'static>;

/// Errors returned by the client.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Local file I/O error.
    #[error("io failed: {0}")]
    Io(#[from] std::io::Error),

    /// URL parsing/joining error.
    #[error("invalid upload url: {0}")]
    Url(#[from] url::ParseError),

    /// The server did not send a required response header.
    #[error("missing required `{0}` header")]
    MissingHeader(&'static str),

    /// The server sent a response header the client could not parse — a
    /// non-UTF-8 value, a non-numeric `Upload-Offset`/`Upload-Length`, or a
    /// malformed `Upload-Metadata`. Returned only while decoding a server
    /// response; client-built request headers surface as
    /// [`InvalidRequestHeader`](Error::InvalidRequestHeader).
    #[error("invalid `{header}` header value `{value}`")]
    #[non_exhaustive]
    InvalidHeader {
        /// Name of the offending response header.
        header: &'static str,
        /// The malformed value the server sent.
        value: String,
    },

    /// A request header the client constructs could not be built — a
    /// protocol header (`Upload-Offset`, `Upload-Length`, `Upload-Concat`),
    /// the encoded `Upload-Metadata`, or the `Upload-Checksum` value. This
    /// is a client-side construction failure (e.g. a metadata key or value
    /// that cannot form a valid header), never a response the server sent.
    #[error("invalid `{name}` header: {value}")]
    #[non_exhaustive]
    InvalidRequestHeader {
        /// Name of the header.
        name: String,
        /// The invalid value.
        value: String,
    },

    /// The remote offset is beyond the local source size.
    #[error("server offset {offset} exceeds local source size {source_len}")]
    #[non_exhaustive]
    OffsetBeyondSource {
        /// Offset reported by the server.
        offset: u64,
        /// Size of the local upload source.
        source_len: u64,
    },

    /// The remote upload length does not match the local source size.
    #[error("server length {remote} does not match local source size {local}")]
    #[non_exhaustive]
    LengthMismatch {
        /// Upload length reported by the server.
        remote: u64,
        /// Size of the local upload source.
        local: u64,
    },

    /// The server acknowledged an offset inconsistent with what the client
    /// sent — either behind the previous offset or beyond the bytes actually
    /// transmitted — which indicates a protocol bug on one side rather than
    /// a transient network failure. Never retried.
    #[error("server offset {actual} does not match expected offset {expected}")]
    #[non_exhaustive]
    OffsetDesync {
        /// Offset the client expected after the write.
        expected: u64,
        /// Offset the server acknowledged.
        actual: u64,
    },

    /// The upload source misbehaved (for example a short or oversized read,
    /// or content that changed underneath the client). Deterministic and
    /// never retried.
    #[error("upload source failed: {message}")]
    #[non_exhaustive]
    Source {
        /// Description of the source misbehavior.
        message: String,
    },

    /// The server returned an unexpected HTTP response.
    #[error("unexpected {operation} response: status {}, body `{body}`", .status.as_u16())]
    #[non_exhaustive]
    UnexpectedResponse {
        /// The client operation that received the response.
        operation: &'static str,
        /// HTTP status code of the response.
        status: http::StatusCode,
        /// Response body, for diagnostics. Bodies are truncated to a few
        /// KiB; a truncated body ends with `...[truncated]`.
        body: String,
        /// The delay requested by a valid `Retry-After` response header, if
        /// present. Populated for every response but only meaningful on
        /// retryable statuses (429/503/408), where the client honors it in
        /// place of its computed backoff.
        retry_after: Option<Duration>,
    },

    /// A transport failed to execute a request.
    ///
    /// `retryable` reports whether retrying could plausibly succeed
    /// (connection reset, DNS hiccup) or whether the failure is
    /// deterministic (request construction bug, redirect-policy violation).
    /// Custom transports should build this variant through
    /// [`Error::transport`] or [`Error::transport_permanent`] rather than
    /// constructing it directly; the underlying failure is preserved as the
    /// error [`source`](std::error::Error::source).
    #[error("{}", transport_failure_message(.retryable))]
    #[non_exhaustive]
    Transport {
        /// The underlying transport failure.
        #[source]
        source: BoxError,
        /// Whether retrying the request could plausibly succeed.
        retryable: bool,
    },

    /// The server does not advertise a TUS extension required by the
    /// requested operation.
    #[error("server does not advertise the `{0}` tus extension")]
    UnsupportedExtension(&'static str),

    /// An internal client error, such as a panicked upload task.
    #[error("internal client error: {0}")]
    Internal(String),
}

impl Error {
    /// Wraps a transport failure that may succeed on retry (connection
    /// reset, DNS hiccup, timeout).
    ///
    /// Custom [`Transport`](crate::Transport) implementations should use
    /// this for transient failures and [`Error::transport_permanent`] for
    /// deterministic ones. Accepts any error type (or a plain message
    /// string):
    ///
    /// ```
    /// use tus_client::Error;
    ///
    /// let io = std::io::Error::other("connection reset");
    /// let error = Error::transport(io);
    /// assert!(error.is_retryable());
    /// ```
    pub fn transport(source: impl Into<BoxError>) -> Self {
        Error::Transport {
            source: source.into(),
            retryable: true,
        }
    }

    /// Wraps a deterministic transport failure that is never retried, such
    /// as a request that cannot be constructed.
    pub fn transport_permanent(source: impl Into<BoxError>) -> Self {
        Error::Transport {
            source: source.into(),
            retryable: false,
        }
    }

    /// Reports whether retrying the failed operation could plausibly
    /// succeed.
    ///
    /// Transient failures (5xx/408/409/429/460 responses and retryable
    /// [`Error::Transport`] failures) are retryable. Deterministic
    /// failures — source misbehavior, offset desync, request construction
    /// errors, and permanent transport failures — are not.
    ///
    /// A 460 (Checksum Mismatch, TUS checksum extension) is retryable
    /// because in-transit corruption of a chunk is exactly the transient
    /// failure the extension exists to detect: the server discarded the
    /// chunk, so resending it is safe and likely to succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::UnexpectedResponse { status, .. } => {
                let status = status.as_u16();
                status >= 500 || matches!(status, 408 | 409 | 429 | 460)
            }
            Error::Transport { retryable, .. } => *retryable,
            _ => false,
        }
    }

    /// Returns the delay a server requested via a valid `Retry-After`
    /// response header, when the error carries one. Only
    /// [`UnexpectedResponse`](Error::UnexpectedResponse) can; every other
    /// variant returns `None`. The client uses this to honor server-driven
    /// backoff on retryable responses instead of its own jittered delay.
    ///
    /// Exposed so a custom [`RetryHook`](crate::RetryHook) can honor the same
    /// server-driven backoff the built-in retry loop uses.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Error::UnexpectedResponse { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

// Takes `&bool` because thiserror's `.retryable` format shorthand expands to
// `&self.retryable`.
fn transport_failure_message(retryable: &bool) -> &'static str {
    if *retryable {
        "transport failed"
    } else {
        "transport failed permanently"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn offset_beyond_source_error_uses_public_client_error_name() {
        let error = Error::OffsetBeyondSource {
            offset: 5,
            source_len: 4,
        };

        assert_eq!(
            error.to_string(),
            "server offset 5 exceeds local source size 4"
        );
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn transport_errors_preserve_the_source_chain() {
        let inner = std::io::Error::other("connection reset");
        let error = Error::transport(inner);

        assert_eq!(error.to_string(), "transport failed");
        let source = std::error::Error::source(&error).expect("source must be preserved");
        assert_eq!(source.to_string(), "connection reset");

        let permanent = Error::transport_permanent("bad request line");
        assert_eq!(permanent.to_string(), "transport failed permanently");
        let source = std::error::Error::source(&permanent).expect("source must be preserved");
        assert_eq!(source.to_string(), "bad request line");
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn retryability_is_a_typed_property() {
        let retryable = [
            Error::transport("connection reset"),
            Error::UnexpectedResponse {
                operation: "patch upload",
                status: StatusCode::SERVICE_UNAVAILABLE,
                body: String::new(),
                retry_after: None,
            },
            Error::UnexpectedResponse {
                operation: "patch upload",
                status: StatusCode::TOO_MANY_REQUESTS,
                body: String::new(),
                retry_after: None,
            },
            Error::UnexpectedResponse {
                operation: "patch upload",
                status: StatusCode::REQUEST_TIMEOUT,
                body: String::new(),
                retry_after: None,
            },
            Error::UnexpectedResponse {
                operation: "patch upload",
                status: StatusCode::CONFLICT,
                body: String::new(),
                retry_after: None,
            },
            Error::UnexpectedResponse {
                operation: "patch upload",
                status: StatusCode::from_u16(460).unwrap(),
                body: String::new(),
                retry_after: None,
            },
        ];
        for error in retryable {
            assert!(error.is_retryable(), "expected retryable: {error}");
        }

        let permanent = [
            Error::transport_permanent("bad credentials"),
            Error::Source {
                message: "short read".into(),
            },
            Error::OffsetDesync {
                expected: 4,
                actual: 2,
            },
            Error::OffsetBeyondSource {
                offset: 5,
                source_len: 4,
            },
            Error::UnexpectedResponse {
                operation: "patch upload",
                status: StatusCode::BAD_REQUEST,
                body: String::new(),
                retry_after: None,
            },
            Error::UnsupportedExtension("concatenation"),
            Error::Internal("task panicked".into()),
        ];
        for error in permanent {
            assert!(!error.is_retryable(), "expected permanent: {error}");
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn retry_after_is_carried_only_by_unexpected_response() {
        let with_hint = Error::UnexpectedResponse {
            operation: "patch upload",
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: String::new(),
            retry_after: Some(Duration::from_secs(7)),
        };
        assert_eq!(with_hint.retry_after(), Some(Duration::from_secs(7)));

        let without_hint = Error::UnexpectedResponse {
            operation: "patch upload",
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: String::new(),
            retry_after: None,
        };
        assert_eq!(without_hint.retry_after(), None);

        assert_eq!(Error::transport("connection reset").retry_after(), None);
    }
}
