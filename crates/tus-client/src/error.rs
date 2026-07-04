//! Error types for the TUS client.

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

    /// The server sent a malformed response header.
    #[error("invalid `{header}` header value `{value}`")]
    InvalidHeader {
        /// Name of the offending response header.
        header: &'static str,
        /// The malformed value the server sent.
        value: String,
    },

    /// The configured default header is invalid.
    #[error("invalid default header `{name}`: {value}")]
    InvalidDefaultHeader {
        /// Name of the configured header.
        name: String,
        /// The invalid configured value.
        value: String,
    },

    /// The remote offset is beyond the local source size.
    #[error("server offset {offset} exceeds local source size {source_len}")]
    OffsetBeyondSource {
        /// Offset reported by the server.
        offset: u64,
        /// Size of the local upload source.
        source_len: u64,
    },

    /// The remote upload length does not match the local source size.
    #[error("server length {remote} does not match local source size {local}")]
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
    Source {
        /// Description of the source misbehavior.
        message: String,
    },

    /// The server returned an unexpected HTTP response.
    #[error("unexpected {operation} response: status {status}, body `{body}`")]
    UnexpectedResponse {
        /// The client operation that received the response.
        operation: &'static str,
        /// HTTP status code of the response.
        status: u16,
        /// Response body, for diagnostics. Bodies are truncated to a few
        /// KiB; a truncated body ends with `...[truncated]`.
        body: String,
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
                *status >= 500 || matches!(*status, 408 | 409 | 429 | 460)
            }
            Error::Transport { retryable, .. } => *retryable,
            _ => false,
        }
    }
}

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
                status: 503,
                body: String::new(),
            },
            Error::UnexpectedResponse {
                operation: "patch upload",
                status: 429,
                body: String::new(),
            },
            Error::UnexpectedResponse {
                operation: "patch upload",
                status: 408,
                body: String::new(),
            },
            Error::UnexpectedResponse {
                operation: "patch upload",
                status: 409,
                body: String::new(),
            },
            Error::UnexpectedResponse {
                operation: "patch upload",
                status: 460,
                body: String::new(),
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
                status: 400,
                body: String::new(),
            },
            Error::UnsupportedExtension("concatenation"),
            Error::Internal("task panicked".into()),
        ];
        for error in permanent {
            assert!(!error.is_retryable(), "expected permanent: {error}");
        }
    }
}
