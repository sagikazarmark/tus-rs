//! Error types for the TUS client.

/// Result type returned by the client.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the client.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// HTTP client error.
    #[cfg(feature = "transport-reqwest")]
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),

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
    InvalidHeader { header: &'static str, value: String },

    /// The configured default header is invalid.
    #[error("invalid default header `{name}`: {value}")]
    InvalidDefaultHeader { name: String, value: String },

    /// The remote offset is beyond the local source size.
    #[error("server offset {offset} exceeds local source size {source_len}")]
    OffsetBeyondSource { offset: u64, source_len: u64 },

    /// The remote upload length does not match the local source size.
    #[error("server length {remote} does not match local source size {local}")]
    LengthMismatch { remote: u64, local: u64 },

    /// The server returned an unexpected HTTP response.
    #[error("unexpected {operation} response: status {status}, body `{body}`")]
    UnexpectedResponse {
        operation: &'static str,
        status: u16,
        body: String,
    },

    /// A pluggable transport returned an error.
    #[error("transport failed: {0}")]
    Transport(String),
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
}
