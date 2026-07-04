//! Transport abstractions for the TUS client.

use std::sync::Arc;

use async_trait::async_trait;
use http::header::HeaderName;

use crate::error::Result;
use crate::runtime::MaybeSendSync;

#[cfg(feature = "transport-reqwest")]
mod reqwest;

#[cfg(feature = "transport-reqwest")]
pub use reqwest::ReqwestTransport;

/// Pluggable HTTP transport used by the client core.
///
/// Implementations turn a [`TransportRequest`] into a buffered
/// [`TransportResponse`]. Failures should be reported through
/// [`Error::transport`](crate::Error::transport) (transient, retried) or
/// [`Error::transport_permanent`](crate::Error::transport_permanent)
/// (deterministic, never retried).
///
/// # Evolution
///
/// This trait is expected to grow: future methods will be added with
/// default implementations, so existing implementations keep compiling.
/// Only `send` will ever be required.
///
/// The trait is not dyn-compatible (it requires `Clone`). For runtime
/// transport selection, wrap any transport in [`BoxTransport`] and use
/// `Client<BoxTransport>`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Transport: Clone + MaybeSendSync + 'static {
    /// Executes a request and returns a buffered response.
    async fn send(&self, request: TransportRequest) -> Result<TransportResponse>;
}

/// Sharing a transport through an [`Arc`] is itself a transport.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<T> Transport for Arc<T>
where
    T: Transport,
{
    async fn send(&self, request: TransportRequest) -> Result<TransportResponse> {
        (**self).send(request).await
    }
}

/// A type-erased [`Transport`].
///
/// `Transport` is not dyn-compatible, so a `Client<T>` is normally
/// monomorphized over one transport type. `BoxTransport` erases the
/// concrete type behind a cheaply clonable handle, letting callers pick a
/// transport at runtime while working with a single `Client<BoxTransport>`
/// type:
///
/// ```no_run
/// use tus_client::{BoxTransport, Client, Transport};
///
/// fn build_client<T: Transport>(endpoint: url::Url, transport: T) -> Client<BoxTransport> {
///     Client::with_transport(endpoint, BoxTransport::new(transport))
/// }
/// ```
#[derive(Clone)]
pub struct BoxTransport {
    inner: Arc<dyn ErasedTransport>,
}

impl BoxTransport {
    /// Erases the concrete type of the given transport.
    pub fn new<T>(transport: T) -> Self
    where
        T: Transport,
    {
        Self {
            inner: Arc::new(transport),
        }
    }
}

impl std::fmt::Debug for BoxTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxTransport").finish_non_exhaustive()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Transport for BoxTransport {
    async fn send(&self, request: TransportRequest) -> Result<TransportResponse> {
        self.inner.send_erased(request).await
    }
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

/// Dyn-compatible mirror of [`Transport`] backing [`BoxTransport`].
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
trait ErasedTransport: MaybeSendSync {
    async fn send_erased(&self, request: TransportRequest) -> Result<TransportResponse>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<T> ErasedTransport for T
where
    T: Transport,
{
    async fn send_erased(&self, request: TransportRequest) -> Result<TransportResponse> {
        Transport::send(self, request).await
    }
}
