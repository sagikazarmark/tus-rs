//! Error types for the TUS client.

/// Result type returned by the client.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the client.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// HTTP client error from the reqwest transport.
    #[cfg(feature = "transport-reqwest")]
    #[error("http request failed: {0}")]
    Reqwest(#[from] reqwest::Error),

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

    /// The server acknowledged an offset that fell behind what the client
    /// expects, which indicates a protocol bug on one side rather than a
    /// transient network failure. Never retried.
    #[error("server offset {actual} is behind expected offset {expected}")]
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
        /// Response body, for diagnostics.
        body: String,
    },

    /// A pluggable transport returned a (possibly transient) error.
    ///
    /// Custom transports should use this for failures that may succeed on
    /// retry (connection reset, DNS hiccup). Use
    /// [`Error::TransportPermanent`] for deterministic failures.
    #[error("transport failed: {0}")]
    Transport(String),

    /// A pluggable transport failed permanently. Never retried.
    #[error("transport failed permanently: {0}")]
    TransportPermanent(String),

    /// The server does not advertise a TUS extension required by the
    /// requested operation.
    #[error("server does not advertise the `{0}` tus extension")]
    UnsupportedExtension(&'static str),

    /// An internal client error, such as a panicked upload task.
    #[error("internal client error: {0}")]
    Internal(String),
}

impl Error {
    /// Reports whether retrying the failed operation could plausibly
    /// succeed.
    ///
    /// Transient failures (connection drops, timeouts, 5xx/408/409/429
    /// responses, and generic [`Error::Transport`] failures) are retryable.
    /// Deterministic failures — source misbehavior, offset desync, request
    /// construction errors, and [`Error::TransportPermanent`] — are not.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::UnexpectedResponse { status, .. } => {
                *status >= 500 || matches!(*status, 408 | 409 | 429)
            }
            Error::Transport(_) => true,
            #[cfg(all(feature = "transport-reqwest", not(target_arch = "wasm32")))]
            Error::Reqwest(error) => error.is_connect() || error.is_timeout(),
            // Browser fetch reports dropped connections as generic request
            // errors, so on wasm anything short of a request-construction
            // (builder) bug is worth retrying.
            #[cfg(all(feature = "transport-reqwest", target_arch = "wasm32"))]
            Error::Reqwest(error) => !error.is_builder(),
            _ => false,
        }
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
    fn retryability_is_a_typed_property() {
        let retryable = [
            Error::Transport("connection reset".into()),
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
        ];
        for error in retryable {
            assert!(error.is_retryable(), "expected retryable: {error}");
        }

        let permanent = [
            Error::TransportPermanent("bad credentials".into()),
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
