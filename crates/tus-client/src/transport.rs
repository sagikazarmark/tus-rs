//! Transport abstractions for the TUS client.

use async_trait::async_trait;
use http::header::HeaderName;

use crate::error::Result;
use crate::runtime::MaybeSendSync;

#[cfg(feature = "transport-reqwest")]
mod reqwest;

#[cfg(feature = "transport-reqwest")]
pub use reqwest::ReqwestTransport;

/// Pluggable HTTP transport used by the client core.
#[cfg_attr(
    all(not(feature = "local-futures"), not(target_arch = "wasm32")),
    async_trait
)]
#[cfg_attr(any(feature = "local-futures", target_arch = "wasm32"), async_trait(?Send))]
pub trait Transport: Clone + MaybeSendSync + 'static {
    /// Executes a request and returns a buffered response.
    async fn send(&self, request: TransportRequest) -> Result<TransportResponse>;
}

/// A transport request emitted by the client core.
pub type TransportRequest = http::Request<TransportBody>;

/// A transport response returned to the client core.
pub type TransportResponse = http::Response<Vec<u8>>;

/// A transport-level request body.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TransportBody {
    /// No request body.
    Empty,
    /// A buffered body with a known length.
    Bytes(Vec<u8>),
    /// A buffered body whose checksum is sent as a trailer.
    BytesWithTrailer {
        /// The request bytes.
        body: Vec<u8>,
        /// Trailer header name.
        trailer_name: HeaderName,
        /// Trailer header value.
        trailer_value: String,
    },
}
