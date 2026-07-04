//! TUS client implementation.

use async_trait::async_trait;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::{Method, Uri};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use url::Url;

use crate::error::{Error, Result};
#[cfg(feature = "checksum")]
use crate::helpers::encode_checksum;
use crate::runtime::{MaybeSend, MaybeSendSync};
use crate::transport::{Transport, TransportBody, TransportRequest};

mod handle;
mod protocol;
mod upload;

pub use handle::Upload;
pub use protocol::{NewUpload, ServerCapabilities, UploadInfo};
#[cfg(all(feature = "source-file", not(target_arch = "wasm32")))]
pub use upload::FileSource;
pub use upload::{ParallelUpload, UploadProgress, UploadSource};

#[cfg(feature = "transport-reqwest")]
use crate::transport::ReqwestTransport;

/// Async TUS client.
///
/// # Async runtime
///
/// On native targets the client depends on tokio: retrying operations
/// (`upload_from`, [`Upload::upload`], …) sleep between attempts via tokio
/// timers (see `crate::runtime`), and [`Client::upload_parallel`] spawns
/// tokio tasks. Client operations must therefore run inside a tokio
/// runtime. On `wasm32` the client is runtime-agnostic and uses the
/// browser event loop for timers.
#[derive(Clone)]
pub struct Client<
    #[cfg(feature = "transport-reqwest")] T = ReqwestTransport,
    #[cfg(not(feature = "transport-reqwest"))] T,
> {
    endpoint: Url,
    transport: T,
    headers: HeaderMap,
    max_retries: usize,
    retry_delay: Duration,
    max_chunk_size: usize,
    max_initial_upload_size: usize,
    #[cfg(feature = "checksum")]
    checksum: Option<ChecksumMode>,
    header_provider: Option<Arc<dyn HeaderProvider>>,
    retry_hook: Option<Arc<dyn RetryHook>>,
    /// Capabilities discovered via OPTIONS, shared across clones of this
    /// client. A `std::sync::Mutex` (never held across an await) keeps the
    /// cache usable on both native and wasm targets.
    capabilities: Arc<Mutex<Option<ServerCapabilities>>>,
}

/// Checksum mode applied to each upload chunk.
#[cfg(feature = "checksum")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChecksumMode {
    /// Send the checksum in the `Upload-Checksum` request header.
    Header(tus_protocol::ChecksumAlgorithm),
    /// Send the checksum in an `Upload-Checksum` request trailer.
    Trailer(tus_protocol::ChecksumAlgorithm),
}

#[cfg(feature = "checksum")]
impl ChecksumMode {
    fn algorithm(self) -> tus_protocol::ChecksumAlgorithm {
        match self {
            ChecksumMode::Header(algorithm) | ChecksumMode::Trailer(algorithm) => algorithm,
        }
    }
}

#[cfg(feature = "checksum")]
impl From<tus_protocol::ChecksumAlgorithm> for ChecksumMode {
    fn from(algorithm: tus_protocol::ChecksumAlgorithm) -> Self {
        Self::Header(algorithm)
    }
}

#[cfg(feature = "transport-reqwest")]
impl Client<ReqwestTransport> {
    /// Creates a new client targeting the given collection endpoint.
    pub fn new(endpoint: Url) -> Self {
        Self::with_transport(endpoint, ReqwestTransport::new())
    }
}

impl<T> std::fmt::Debug for Client<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("Client");
        debug
            .field("endpoint", &self.endpoint)
            .field("max_retries", &self.max_retries)
            .field("retry_delay", &self.retry_delay)
            .field("max_chunk_size", &self.max_chunk_size)
            .field("max_initial_upload_size", &self.max_initial_upload_size);
        #[cfg(feature = "checksum")]
        debug.field("checksum", &self.checksum);
        debug.finish()
    }
}

impl<T> Client<T>
where
    T: Transport,
{
    /// Creates a new client using the supplied transport.
    pub fn with_transport(endpoint: Url, transport: T) -> Self {
        Self {
            endpoint,
            transport,
            headers: HeaderMap::new(),
            max_retries: 3,
            retry_delay: Duration::from_millis(200),
            max_chunk_size: 8 * 1024 * 1024,
            max_initial_upload_size: 256 * 1024,
            #[cfg(feature = "checksum")]
            checksum: None,
            header_provider: None,
            retry_hook: None,
            capabilities: Arc::new(Mutex::new(None)),
        }
    }

    /// Sets headers added to every request.
    pub fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Sets the number of PATCH retries on transport or 5xx failures.
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the base retry delay used for resumable PATCH retries.
    pub fn with_retry_delay(mut self, retry_delay: Duration) -> Self {
        self.retry_delay = retry_delay;
        self
    }

    /// Sets the maximum PATCH request body size.
    pub fn with_max_chunk_size(mut self, max_chunk_size: usize) -> Self {
        self.max_chunk_size = max_chunk_size.max(1);
        self
    }

    /// Sets the maximum body size sent in the initial POST request when the
    /// server advertises creation-with-upload.
    pub fn with_max_initial_upload_size(mut self, max_initial_upload_size: usize) -> Self {
        self.max_initial_upload_size = max_initial_upload_size;
        self
    }

    /// Enables per-chunk checksum verification.
    ///
    /// [`ChecksumMode::Trailer`] is not supported on `wasm32` because the
    /// browser `fetch` API cannot send request trailers; the first request
    /// fails with a permanent, non-retried [`Error::Transport`].
    #[cfg(feature = "checksum")]
    pub fn with_checksum(mut self, mode: impl Into<ChecksumMode>) -> Self {
        self.checksum = Some(mode.into());
        self
    }

    /// Adds a dynamic header provider consulted for every request.
    ///
    /// Provider headers *replace* any statically configured
    /// [`with_headers`](Client::with_headers) values of the same header
    /// name — including all appended multi-values of that name — so a
    /// refreshed credential can never be sent alongside a stale configured
    /// one. Configured headers with other names are unaffected. If the
    /// provider itself yields the same name more than once, the last value
    /// wins.
    pub fn with_header_provider<P>(mut self, provider: P) -> Self
    where
        P: HeaderProvider + 'static,
    {
        self.header_provider = Some(Arc::new(provider));
        self
    }

    /// Adds a retry hook invoked before the next retry attempt.
    pub fn with_retry_hook<H>(mut self, hook: H) -> Self
    where
        H: RetryHook + 'static,
    {
        self.retry_hook = Some(Arc::new(hook));
        self
    }

    async fn request(&self, method: Method, url: &str) -> Result<TransportRequest> {
        let url = Url::parse(url)?;
        let uri: Uri = url.as_str().parse().map_err(|err| {
            Error::transport_permanent(format!("invalid transport URI {}: {err}", url.as_str()))
        })?;
        let mut request = http::Request::builder()
            .method(method)
            .uri(uri)
            .body(TransportBody::Empty)
            .map_err(|err| Error::transport_permanent(format!("failed to build request: {err}")))?;

        // `append` keeps every value of repeated default header names
        // (e.g. multiple `Cookie` or custom multi-valued headers) instead of
        // keeping only the last one.
        for (name, value) in &self.headers {
            request.headers_mut().append(name.clone(), value.clone());
        }
        if let Some(provider) = &self.header_provider {
            // `insert` (not `append`): provider headers replace all
            // configured values of the same name, so refreshed credentials
            // never travel next to stale configured ones. Documented on
            // `with_header_provider`.
            for (name, value) in provider.headers().await? {
                insert_request_header(&mut request, name, value)?;
            }
        }
        request.headers_mut().insert(
            HeaderName::from_static("tus-resumable"),
            HeaderValue::from_static(tus_protocol::TUS_RESUMABLE),
        );
        Ok(request)
    }

    /// Returns the capabilities previously discovered via OPTIONS, if any.
    pub(crate) fn known_capabilities(&self) -> Option<ServerCapabilities> {
        self.capabilities.lock().unwrap().clone()
    }

    pub(crate) fn store_capabilities(&self, capabilities: &ServerCapabilities) {
        *self.capabilities.lock().unwrap() = Some(capabilities.clone());
    }

    #[cfg(feature = "checksum")]
    fn apply_checksum(
        &self,
        mut request: TransportRequest,
        body: Vec<u8>,
    ) -> Result<TransportRequest> {
        let Some(mode) = self.checksum else {
            *request.body_mut() = TransportBody::Bytes(body);
            return Ok(request);
        };

        let checksum = encode_checksum(mode.algorithm(), &body);

        match mode {
            ChecksumMode::Header(algorithm) => {
                request.headers_mut().insert(
                    HeaderName::from_static("upload-checksum"),
                    HeaderValue::from_str(&format!("{} {}", algorithm.as_str(), checksum))
                        .map_err(|_| Error::InvalidHeader {
                            header: "Upload-Checksum",
                            value: checksum.clone(),
                        })?,
                );
                *request.body_mut() = TransportBody::Bytes(body);
                Ok(request)
            }
            #[cfg(target_arch = "wasm32")]
            ChecksumMode::Trailer(_) => Err(Error::transport_permanent(
                "checksum trailers are not supported on wasm32 (browser fetch cannot send \
                 request trailers); use ChecksumMode::Header instead",
            )),
            #[cfg(not(target_arch = "wasm32"))]
            ChecksumMode::Trailer(algorithm) => {
                request.headers_mut().insert(
                    HeaderName::from_static("trailer"),
                    HeaderValue::from_static("upload-checksum"),
                );
                *request.body_mut() = TransportBody::BytesWithTrailer {
                    body,
                    trailer_name: HeaderName::from_static("upload-checksum"),
                    trailer_value: format!("{} {}", algorithm.as_str(), checksum),
                };
                Ok(request)
            }
        }
    }

    #[cfg(not(feature = "checksum"))]
    fn apply_checksum(
        &self,
        mut request: TransportRequest,
        body: Vec<u8>,
    ) -> Result<TransportRequest> {
        *request.body_mut() = TransportBody::Bytes(body);
        Ok(request)
    }

    /// Applies the configured checksum to a creation-with-upload POST body.
    ///
    /// The TUS checksum extension defines `Upload-Checksum` for PATCH
    /// requests, so the header is only attached to the creation POST when
    /// the server is known (from cached OPTIONS capabilities) to advertise
    /// the `checksum` extension. Otherwise the body is sent unadorned.
    #[cfg(feature = "checksum")]
    fn apply_creation_checksum(
        &self,
        mut request: TransportRequest,
        body: Vec<u8>,
    ) -> Result<TransportRequest> {
        let server_advertises_checksum = self
            .known_capabilities()
            .is_some_and(|capabilities| capabilities.has_extension("checksum"));
        if server_advertises_checksum {
            self.apply_checksum(request, body)
        } else {
            *request.body_mut() = TransportBody::Bytes(body);
            Ok(request)
        }
    }

    #[cfg(not(feature = "checksum"))]
    fn apply_creation_checksum(
        &self,
        request: TransportRequest,
        body: Vec<u8>,
    ) -> Result<TransportRequest> {
        self.apply_checksum(request, body)
    }
}

/// Hook for providing dynamic request headers.
///
/// The hook is asynchronous so implementations can refresh credentials
/// (e.g. renew an OAuth token) before each request.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait HeaderProvider: MaybeSendSync {
    /// Produces headers to append to the next request.
    async fn headers(&self) -> Result<Vec<(String, String)>>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<F, Fut> HeaderProvider for F
where
    F: Fn() -> Fut + MaybeSendSync,
    Fut: std::future::Future<Output = Result<Vec<(String, String)>>> + MaybeSend,
{
    async fn headers(&self) -> Result<Vec<(String, String)>> {
        self().await
    }
}

/// Hook invoked before a failed request is retried.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait RetryHook: MaybeSendSync {
    /// Returns true if the client should retry the failed operation.
    async fn before_retry(&self, attempt: usize, error: &Error) -> Result<bool>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<F, Fut> RetryHook for F
where
    F: Fn(usize, &Error) -> Fut + MaybeSendSync,
    Fut: std::future::Future<Output = Result<bool>> + MaybeSend,
{
    async fn before_retry(&self, attempt: usize, error: &Error) -> Result<bool> {
        self(attempt, error).await
    }
}

fn insert_request_header(
    request: &mut TransportRequest,
    name: impl AsRef<str>,
    value: impl ToString,
) -> Result<()> {
    let name = name.as_ref();
    let value = value.to_string();
    let header_name =
        HeaderName::from_bytes(name.as_bytes()).map_err(|_| Error::InvalidDefaultHeader {
            name: name.to_string(),
            value: value.clone(),
        })?;
    let header_value = HeaderValue::from_str(&value).map_err(|_| Error::InvalidDefaultHeader {
        name: header_name.as_str().to_string(),
        value: value.clone(),
    })?;
    request.headers_mut().insert(header_name, header_value);
    Ok(())
}

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::test_support::{MockTransport, endpoint_url, transport_response};
    use http::header::{HeaderMap, HeaderName, HeaderValue, LOCATION};
    use tus_protocol::UploadMetadata;

    #[cfg(not(target_arch = "wasm32"))]
    use tokio::test as async_test;
    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as async_test;

    #[async_test]
    async fn headers_are_applied_to_transport_requests() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(LOCATION, HeaderValue::from_static("/files/mock-id"));
                    headers
                },
                Vec::new(),
            )));

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-tenant-id"),
            HeaderValue::from_static("team-a"),
        );
        let client =
            Client::with_transport(endpoint_url(), transport.clone()).with_headers(headers);

        let (upload, _info) = client
            .create_upload(NewUpload::new(5, UploadMetadata::new()))
            .await
            .unwrap();
        assert_eq!(upload.url().as_str(), "http://example.test/files/mock-id");

        let requests = transport.requests.lock().unwrap();
        let request = requests.first().unwrap();
        assert_eq!(
            request
                .headers()
                .get("x-tenant-id")
                .and_then(|value| value.to_str().ok()),
            Some("team-a")
        );
    }

    #[async_test]
    async fn repeated_default_header_values_are_all_sent() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(LOCATION, HeaderValue::from_static("/files/mock-id"));
                    headers
                },
                Vec::new(),
            )));

        let mut headers = HeaderMap::new();
        headers.append(
            HeaderName::from_static("x-trace-tag"),
            HeaderValue::from_static("alpha"),
        );
        headers.append(
            HeaderName::from_static("x-trace-tag"),
            HeaderValue::from_static("beta"),
        );
        let client =
            Client::with_transport(endpoint_url(), transport.clone()).with_headers(headers);

        client
            .create_upload(NewUpload::new(5, UploadMetadata::new()))
            .await
            .unwrap();

        let requests = transport.requests.lock().unwrap();
        let values: Vec<_> = requests
            .first()
            .unwrap()
            .headers()
            .get_all("x-trace-tag")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .collect();
        assert_eq!(values, vec!["alpha", "beta"]);
    }

    #[async_test]
    async fn header_provider_is_applied_to_transport_requests() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(LOCATION, HeaderValue::from_static("/files/mock-id"));
                    headers
                },
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_header_provider(|| {
                // Async providers can refresh tokens before answering.
                std::future::ready(Ok(vec![("x-tenant-id".to_string(), "team-a".to_string())]))
            });

        let (upload, _info) = client
            .create_upload(NewUpload::new(5, UploadMetadata::new()))
            .await
            .unwrap();
        assert_eq!(upload.url().as_str(), "http://example.test/files/mock-id");

        let requests = transport.requests.lock().unwrap();
        let request = requests.first().unwrap();
        assert_eq!(
            request
                .headers()
                .get("x-tenant-id")
                .and_then(|value| value.to_str().ok()),
            Some("team-a")
        );
    }

    /// Provider headers replace *all* configured multi-values of the same
    /// name (documented on `with_header_provider`), while configured
    /// headers with other names survive untouched.
    #[async_test]
    async fn header_provider_replaces_configured_multi_values_of_same_name() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(LOCATION, HeaderValue::from_static("/files/mock-id"));
                    headers
                },
                Vec::new(),
            )));

        let mut headers = HeaderMap::new();
        headers.append(
            HeaderName::from_static("x-trace-tag"),
            HeaderValue::from_static("alpha"),
        );
        headers.append(
            HeaderName::from_static("x-trace-tag"),
            HeaderValue::from_static("beta"),
        );
        headers.insert(
            HeaderName::from_static("x-tenant-id"),
            HeaderValue::from_static("team-a"),
        );
        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_headers(headers)
            .with_header_provider(|| {
                std::future::ready(Ok(vec![("x-trace-tag".to_string(), "fresh".to_string())]))
            });

        client
            .create_upload(NewUpload::new(5, UploadMetadata::new()))
            .await
            .unwrap();

        let requests = transport.requests.lock().unwrap();
        let request = requests.first().unwrap();
        let trace_tags: Vec<_> = request
            .headers()
            .get_all("x-trace-tag")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .collect();
        assert_eq!(
            trace_tags,
            vec!["fresh"],
            "provider value must replace every configured value of the same name"
        );
        assert_eq!(
            request
                .headers()
                .get("x-tenant-id")
                .and_then(|value| value.to_str().ok()),
            Some("team-a"),
            "configured headers with other names must survive"
        );
    }

    #[async_test]
    async fn box_transport_erases_the_concrete_transport_type() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(LOCATION, HeaderValue::from_static("/files/mock-id"));
                    headers
                },
                Vec::new(),
            )));

        let boxed = crate::BoxTransport::new(transport.clone());
        let client: Client<crate::BoxTransport> = Client::with_transport(endpoint_url(), boxed);

        let (upload, _info) = client
            .create_upload(NewUpload::new(5, UploadMetadata::new()))
            .await
            .unwrap();

        assert_eq!(upload.url().as_str(), "http://example.test/files/mock-id");
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
    }

    #[async_test]
    async fn arc_wrapped_transports_are_transports() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(LOCATION, HeaderValue::from_static("/files/mock-id"));
                    headers
                },
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), Arc::new(transport.clone()));

        let (upload, _info) = client
            .create_upload(NewUpload::new(5, UploadMetadata::new()))
            .await
            .unwrap();

        assert_eq!(upload.url().as_str(), "http://example.test/files/mock-id");
        assert_eq!(transport.requests.lock().unwrap().len(), 1);
    }

    #[async_test]
    async fn protocol_headers_override_configured_headers() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(LOCATION, HeaderValue::from_static("/files/mock-id"));
                    headers
                },
                Vec::new(),
            )));

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("tus-resumable"),
            HeaderValue::from_static("0.0.0"),
        );
        let client =
            Client::with_transport(endpoint_url(), transport.clone()).with_headers(headers);

        client
            .create_upload(NewUpload::new(5, UploadMetadata::new()))
            .await
            .unwrap();

        let requests = transport.requests.lock().unwrap();
        assert_eq!(
            requests
                .first()
                .unwrap()
                .headers()
                .get("tus-resumable")
                .and_then(|value| value.to_str().ok()),
            Some(tus_protocol::TUS_RESUMABLE)
        );
    }
}
