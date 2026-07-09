//! Reqwest-backed TUS client transports.

use async_trait::async_trait;
use http::{HeaderName, HeaderValue};

use crate::error::{Error, Result};
use crate::transport::{Transport, TransportBody, TransportRequest, TransportResponse};

#[cfg(not(target_arch = "wasm32"))]
use {
    ::reqwest::Body,
    bytes::Bytes,
    http::header::CONTENT_LENGTH,
    http_body_util::{BodyExt, Full},
};

/// Default `reqwest`-backed transport.
///
/// Always backed by a plain `reqwest::Client`. The middleware variant is a
/// separate type (`ReqwestMiddlewareTransport`, under the
/// `transport-reqwest-middleware` feature) so enabling that feature anywhere in
/// the dependency graph can never reshape this transport.
#[derive(Clone, Debug)]
pub struct ReqwestTransport {
    client: ::reqwest::Client,
}

impl ReqwestTransport {
    /// Creates a new transport using reqwest's default client.
    ///
    /// To wrap a configured client, use the [`From`] impl
    /// (`ReqwestTransport::from(client)` or `client.into()`).
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: ::reqwest::Client::new(),
        }
    }
}

impl From<::reqwest::Client> for ReqwestTransport {
    fn from(client: ::reqwest::Client) -> Self {
        Self { client }
    }
}

impl Default for ReqwestTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Transport for ReqwestTransport {
    async fn send(&self, request: TransportRequest) -> Result<TransportResponse> {
        let (parts, body) = request.into_parts();
        let builder = self.client.request(parts.method, parts.uri.to_string());
        send_request(builder, &parts.headers, body).await
    }
}

/// `reqwest`-backed transport that runs a configured
/// [`reqwest_middleware`](crate::reqwest_middleware) chain.
///
/// A middleware transport with no middleware is just [`ReqwestTransport`], so
/// this type is constructed only from an already-built
/// [`ClientWithMiddleware`](reqwest_middleware::ClientWithMiddleware) — there is
/// deliberately no `Default` or no-argument constructor.
#[cfg(feature = "transport-reqwest-middleware")]
#[derive(Clone, Debug)]
pub struct ReqwestMiddlewareTransport {
    client: reqwest_middleware::ClientWithMiddleware,
}

#[cfg(feature = "transport-reqwest-middleware")]
impl From<reqwest_middleware::ClientWithMiddleware> for ReqwestMiddlewareTransport {
    fn from(client: reqwest_middleware::ClientWithMiddleware) -> Self {
        Self { client }
    }
}

#[cfg(feature = "transport-reqwest-middleware")]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Transport for ReqwestMiddlewareTransport {
    async fn send(&self, request: TransportRequest) -> Result<TransportResponse> {
        let (parts, body) = request.into_parts();
        let builder = self.client.request(parts.method, parts.uri.to_string());
        send_request(builder, &parts.headers, body).await
    }
}

/// Assembles `body` onto `builder`, applies `headers`, sends the request, and
/// reads the capped response. Written once and shared by both reqwest
/// transports through the [`ReqwestRequestBuilder`] adapter, since the plain
/// and middleware builder types expose identical methods but share no trait.
async fn send_request<B: ReqwestRequestBuilder>(
    mut builder: B,
    headers: &http::HeaderMap,
    body: TransportBody,
) -> Result<TransportResponse> {
    for (name, value) in headers {
        builder = builder.with_header(name, value);
    }

    builder = match body {
        TransportBody::Empty => builder,
        TransportBody::Bytes(body) => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                builder
                    .with_header(&CONTENT_LENGTH, &HeaderValue::from(body.len()))
                    .with_body(Body::from(body))
            }

            #[cfg(target_arch = "wasm32")]
            {
                builder.with_body(body)
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        TransportBody::BytesWithTrailer {
            body,
            trailer_name,
            trailer_value,
        } => {
            let mut trailers = http::HeaderMap::new();
            trailers.insert(
                trailer_name.clone(),
                HeaderValue::from_str(&trailer_value).map_err(|_| {
                    Error::transport_permanent(format!(
                        "invalid trailer value for {}",
                        trailer_name.as_str()
                    ))
                })?,
            );
            let body = Full::new(Bytes::from(body))
                .with_trailers(std::future::ready(Some(Ok::<_, std::convert::Infallible>(
                    trailers,
                ))))
                .map_err(|never| -> std::io::Error { match never {} });
            builder.with_body(Body::wrap(body))
        }
        #[cfg(target_arch = "wasm32")]
        TransportBody::BytesWithTrailer { .. } => {
            return Err(Error::transport_permanent(
                "reqwest transport does not support request trailers on wasm32",
            ));
        }
    };

    let response = builder.execute().await?;
    finish_response(response).await
}

/// Turns a completed `reqwest` response into a [`TransportResponse`], reading
/// the body under [`MAX_RESPONSE_BODY_BYTES`].
async fn finish_response(response: ::reqwest::Response) -> Result<TransportResponse> {
    let status = response.status().as_u16();
    let headers = response.headers().clone();

    // TUS responses carry no significant body (Location, headers, or a short
    // error string), so bound the read. Without a cap, a misbehaving or
    // malicious server — including one reached via a redirect — could return
    // a multi-gigabyte body and exhaust client memory, since the transport
    // buffers the whole response before the caller inspects it.
    #[cfg(not(target_arch = "wasm32"))]
    let body = read_body_capped(response).await?;
    #[cfg(target_arch = "wasm32")]
    let body = response.bytes().await.map_err(reqwest_error)?.to_vec();

    let mut response = http::Response::builder()
        .status(status)
        .body(body)
        .map_err(|err| Error::transport(format!("failed to build response: {err}")))?;
    *response.headers_mut() = headers;
    Ok(response)
}

/// Adapter over the plain and middleware `reqwest` request builders so the
/// request assembly in [`send_request`] is written once. The two builder types
/// expose identical inherent methods but share no common trait; each impl
/// differs only in how transport errors are classified on `execute`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
trait ReqwestRequestBuilder: Sized {
    fn with_header(self, name: &HeaderName, value: &HeaderValue) -> Self;

    #[cfg(not(target_arch = "wasm32"))]
    fn with_body(self, body: Body) -> Self;

    #[cfg(target_arch = "wasm32")]
    fn with_body(self, body: Vec<u8>) -> Self;

    async fn execute(self) -> Result<::reqwest::Response>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ReqwestRequestBuilder for ::reqwest::RequestBuilder {
    fn with_header(self, name: &HeaderName, value: &HeaderValue) -> Self {
        self.header(name, value)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn with_body(self, body: Body) -> Self {
        self.body(body)
    }

    #[cfg(target_arch = "wasm32")]
    fn with_body(self, body: Vec<u8>) -> Self {
        self.body(body)
    }

    async fn execute(self) -> Result<::reqwest::Response> {
        self.send().await.map_err(reqwest_error)
    }
}

#[cfg(feature = "transport-reqwest-middleware")]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ReqwestRequestBuilder for reqwest_middleware::RequestBuilder {
    fn with_header(self, name: &HeaderName, value: &HeaderValue) -> Self {
        self.header(name, value)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn with_body(self, body: Body) -> Self {
        self.body(body)
    }

    #[cfg(target_arch = "wasm32")]
    fn with_body(self, body: Vec<u8>) -> Self {
        self.body(body)
    }

    async fn execute(self) -> Result<::reqwest::Response> {
        self.send().await.map_err(reqwest_middleware_error)
    }
}

/// Maximum response body the transport will buffer before failing. TUS
/// responses are tiny; this only exists to bound memory against a server that
/// returns an oversized body.
#[cfg(not(target_arch = "wasm32"))]
const MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;

/// Reads a reqwest response body into memory, failing if it exceeds
/// [`MAX_RESPONSE_BODY_BYTES`]. Streams chunk-by-chunk so an oversized body is
/// rejected without being fully buffered first.
#[cfg(not(target_arch = "wasm32"))]
async fn read_body_capped(mut response: ::reqwest::Response) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(reqwest_error)? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BODY_BYTES {
            return Err(Error::transport_permanent(format!(
                "response body exceeds {MAX_RESPONSE_BODY_BYTES} byte limit"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Maps a reqwest error into [`Error::Transport`], preserving the error as
/// the source and classifying its retryability.
///
/// Mid-transfer connection resets surface as request/body errors, not
/// `is_connect()` failures, so anything short of a deterministic
/// request-construction (builder) or redirect-policy error is worth
/// retrying for a resumable upload. Browser `fetch` reports dropped
/// connections as generic request errors, so on wasm anything short of a
/// builder bug is retryable.
fn reqwest_error(error: ::reqwest::Error) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    let retryable = !(error.is_builder() || error.is_redirect());
    #[cfg(target_arch = "wasm32")]
    let retryable = !error.is_builder();

    Error::Transport {
        source: Box::new(error),
        retryable,
    }
}

#[cfg(feature = "transport-reqwest-middleware")]
fn reqwest_middleware_error(error: reqwest_middleware::Error) -> Error {
    match error {
        reqwest_middleware::Error::Reqwest(error) => reqwest_error(error),
        // Middleware failures are opaque (`anyhow::Error`), so they get the
        // benefit of the doubt: retryable, like other generic transport
        // failures.
        reqwest_middleware::Error::Middleware(error) => Error::transport(error),
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use http::{Method, StatusCode, header::CONTENT_TYPE};
    #[cfg(feature = "checksum")]
    use tus_protocol::ChecksumAlgorithm;
    use tus_protocol::{
        Config, Extension, NoopHookExecutor, ProtocolHandle, UploadMetadata,
        locking::memory::MemoryLocker, state::memory::MemoryStateStore,
        storage::memory::MemoryStorage,
    };

    #[cfg(feature = "checksum")]
    use crate::ChecksumMode;
    use crate::{Client, NewUpload, ParallelUpload, Transport, TransportBody, TransportRequest};

    async fn spawn_test_server() -> (String, tokio::task::JoinHandle<()>) {
        spawn_test_server_with_config(Config::default()).await
    }

    fn endpoint_url(endpoint: &str) -> url::Url {
        url::Url::parse(endpoint).unwrap()
    }

    async fn spawn_test_server_with_config(
        config: Config,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let state = tus_axum::TusState::new(ProtocolHandle::new(
            config,
            MemoryStorage::new(),
            MemoryStateStore::new(),
            MemoryLocker::new(),
            NoopHookExecutor::new(),
        ));
        let app: axum::Router =
            tus_axum::create_router(state, tus_axum::RouterOptions::default()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}/files"), handle)
    }

    #[derive(Clone)]
    struct OneShotPatchFailureTransport {
        inner: ReqwestTransport,
        failures_remaining: Arc<AtomicUsize>,
    }

    impl OneShotPatchFailureTransport {
        fn new(failures: usize) -> Self {
            Self {
                inner: ReqwestTransport::new(),
                failures_remaining: Arc::new(AtomicUsize::new(failures)),
            }
        }

        fn injected_failures(&self) -> usize {
            self.failures_remaining.load(Ordering::Relaxed)
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(
        target_arch = "wasm32",
        async_trait(?Send)
    )]
    impl Transport for OneShotPatchFailureTransport {
        async fn send(&self, request: TransportRequest) -> Result<TransportResponse> {
            if request.method() == Method::PATCH
                && self
                    .failures_remaining
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                        if remaining > 0 {
                            Some(remaining - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
            {
                return Ok(transport_response(
                    503,
                    http::HeaderMap::new(),
                    b"temporary failure".to_vec(),
                ));
            }

            self.inner.send(request).await
        }
    }

    #[derive(Clone)]
    struct MidBodyDropTransport {
        inner: ReqwestTransport,
        drops_remaining: Arc<AtomicUsize>,
        accepted_bytes: usize,
        partial_acks: Arc<AtomicUsize>,
    }

    impl MidBodyDropTransport {
        fn new(accepted_bytes: usize) -> Self {
            Self {
                inner: ReqwestTransport::new(),
                drops_remaining: Arc::new(AtomicUsize::new(1)),
                accepted_bytes,
                partial_acks: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn partial_acks(&self) -> usize {
            self.partial_acks.load(Ordering::Relaxed)
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(
        target_arch = "wasm32",
        async_trait(?Send)
    )]
    impl Transport for MidBodyDropTransport {
        async fn send(&self, mut request: TransportRequest) -> Result<TransportResponse> {
            if request.method() == Method::PATCH
                && self
                    .drops_remaining
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                        if remaining > 0 {
                            Some(remaining - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
            {
                // Truncate the body to simulate a TCP reset that still delivered some bytes.
                if let TransportBody::Bytes(body) = request.body().clone() {
                    let take = body.len().min(self.accepted_bytes);
                    if take > 0 {
                        let mut partial = request.clone();
                        *partial.body_mut() = TransportBody::Bytes(body[..take].to_vec());
                        if self.inner.send(partial).await.is_ok() {
                            self.partial_acks.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                *request.body_mut() = TransportBody::Empty;
                return Ok(transport_response(
                    500,
                    http::HeaderMap::new(),
                    b"simulated mid-body connection drop".to_vec(),
                ));
            }

            self.inner.send(request).await
        }
    }

    #[cfg(feature = "transport-reqwest-middleware")]
    struct CountingMiddleware {
        calls: Arc<AtomicUsize>,
    }

    #[cfg(feature = "transport-reqwest-middleware")]
    #[async_trait]
    impl reqwest_middleware::Middleware for CountingMiddleware {
        async fn handle(
            &self,
            request: reqwest::Request,
            extensions: &mut http::Extensions,
            next: reqwest_middleware::Next<'_>,
        ) -> reqwest_middleware::Result<reqwest::Response> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            next.run(request, extensions).await
        }
    }

    #[tokio::test]
    async fn upload_creates_and_completes_remote_upload_from_source() {
        let (endpoint, handle) = spawn_test_server().await;

        let mut metadata = HashMap::new();
        metadata.insert("filename".to_string(), "hello.txt".to_string());

        let client = Client::new(endpoint_url(&endpoint));
        let upload = client
            .upload_from(b"hello world".to_vec(), &metadata)
            .await
            .unwrap();

        assert_eq!(upload.offset, 11);
        assert_eq!(upload.length, Some(11));
        assert_eq!(
            upload.metadata.get("filename").and_then(|v| v.as_str()),
            Some("hello.txt")
        );

        handle.abort();
    }

    #[cfg(feature = "transport-reqwest-middleware")]
    #[tokio::test]
    async fn reqwest_transport_runs_configured_middleware() {
        let (endpoint, handle) = spawn_test_server().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let middleware_client = reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
            .with(CountingMiddleware {
                calls: calls.clone(),
            })
            .build();
        let transport = ReqwestMiddlewareTransport::from(middleware_client);
        let client = Client::with_transport(endpoint_url(&endpoint), transport);

        client.server_capabilities().await.unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        handle.abort();
    }

    #[tokio::test]
    async fn reqwest_transport_accepts_configured_reqwest_client() {
        let (endpoint, handle) = spawn_test_server().await;
        let transport = ReqwestTransport::from(reqwest::Client::new());
        let client = Client::with_transport(endpoint_url(&endpoint), transport);

        let info = client.server_capabilities().await.unwrap();

        assert!(info.supports_version("1.0.0"));
        handle.abort();
    }

    #[tokio::test]
    async fn upload_uses_creation_with_upload_for_small_sources() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let (endpoint, handle) = spawn_test_server_with_config(config).await;
        let mut metadata = HashMap::new();
        metadata.insert("filename".to_string(), "tiny.txt".to_string());

        let client = Client::new(endpoint_url(&endpoint)).with_max_initial_upload_size(1024);
        let upload = client
            .upload_from(b"tiny-body".to_vec(), &metadata)
            .await
            .unwrap();

        assert_eq!(upload.offset, 9);
        assert_eq!(upload.length, Some(9));

        handle.abort();
    }

    #[tokio::test]
    async fn resume_continues_from_server_offset() {
        let (endpoint, handle) = spawn_test_server().await;

        let client = Client::new(endpoint_url(&endpoint)).with_max_retries(0);
        let (upload, _info) = client
            .create_upload(NewUpload::new(10, UploadMetadata::new()))
            .await
            .unwrap();

        let partial = reqwest::Client::new()
            .patch(upload.url().as_str())
            .header("tus-resumable", tus_protocol::TUS_RESUMABLE)
            .header("upload-offset", "0")
            .header(CONTENT_TYPE, "application/offset+octet-stream")
            .body("abcde")
            .send()
            .await
            .unwrap();
        assert!(partial.status().is_success());

        let resumed = client
            .upload_at(upload.url().clone())
            .unwrap()
            .upload(b"abcdefghij".to_vec())
            .await
            .unwrap();
        assert_eq!(resumed.offset, 10);
        assert_eq!(resumed.length, Some(10));

        handle.abort();
    }

    #[tokio::test]
    async fn progress_callback_observes_offset_advances() {
        let (endpoint, handle) = spawn_test_server().await;
        let client = Client::new(endpoint_url(&endpoint))
            .with_max_chunk_size(4)
            .with_max_initial_upload_size(0);

        let mut updates = Vec::new();
        let upload = client
            .upload_from_with_progress(
                b"abcdefghijkl".to_vec(),
                UploadMetadata::new(),
                &mut |sent, total| {
                    updates.push((sent, total));
                },
            )
            .await
            .unwrap();

        assert_eq!(upload.offset, 12);
        assert_eq!(updates, vec![(4, 12), (8, 12), (12, 12)]);

        handle.abort();
    }

    #[tokio::test]
    #[cfg(feature = "checksum")]
    async fn checksum_header_uploads_are_accepted() {
        let config = Config::default().with_checksum(ChecksumAlgorithm::Sha256);
        let (endpoint, handle) = spawn_test_server_with_config(config).await;
        let client = Client::new(endpoint_url(&endpoint))
            .with_checksum(ChecksumAlgorithm::Sha256)
            .with_max_chunk_size(4)
            .with_max_initial_upload_size(0);
        let upload = client
            .upload_from(b"checksum-data".to_vec(), UploadMetadata::new())
            .await
            .unwrap();

        assert_eq!(upload.offset, 13);
        assert_eq!(upload.length, Some(13));

        handle.abort();
    }

    #[tokio::test]
    #[cfg(feature = "checksum")]
    async fn checksum_trailer_uploads_are_accepted() {
        let config = Config::default().with_checksum(ChecksumAlgorithm::Sha1);
        let (endpoint, handle) = spawn_test_server_with_config(config).await;
        let client = Client::new(endpoint_url(&endpoint))
            .with_checksum(ChecksumMode::Trailer(ChecksumAlgorithm::Sha1))
            .with_max_chunk_size(5)
            .with_max_initial_upload_size(0);
        let upload = client
            .upload_from(b"trailer-data".to_vec(), UploadMetadata::new())
            .await
            .unwrap();

        assert_eq!(upload.offset, 12);
        assert_eq!(upload.length, Some(12));

        handle.abort();
    }

    #[tokio::test]
    async fn parallel_uploads_can_be_concatenated() {
        let config = Config::default().with_extension(Extension::Concatenation);
        let (endpoint, handle) = spawn_test_server_with_config(config).await;
        let client = Client::new(endpoint_url(&endpoint))
            .with_max_initial_upload_size(0)
            .with_max_chunk_size(4);
        let upload = client
            .upload_parallel(
                b"abcdefghijklmnop".to_vec(),
                UploadMetadata::new(),
                ParallelUpload::new(4),
            )
            .await
            .unwrap();

        assert_eq!(upload.offset, 16);
        assert_eq!(upload.length, Some(16));

        handle.abort();
    }

    #[tokio::test]
    async fn terminate_upload_terminates_remote_upload() {
        let (endpoint, server_handle) = spawn_test_server().await;
        let client = Client::new(endpoint_url(&endpoint));
        let (upload, _info) = client
            .create_upload(NewUpload::new(5, UploadMetadata::new()))
            .await
            .unwrap();

        let handle = client.upload_at(upload.url().clone()).unwrap();
        handle.terminate().await.unwrap();

        let err = handle.info().await.unwrap_err();
        assert!(
            matches!(err, Error::UnexpectedResponse { status, .. } if status == StatusCode::NOT_FOUND)
        );

        server_handle.abort();
    }

    #[tokio::test]
    async fn upload_retries_after_injected_patch_failure() {
        let (endpoint, handle) = spawn_test_server().await;

        let transport = OneShotPatchFailureTransport::new(1);
        let client = Client::with_transport(endpoint_url(&endpoint), transport.clone())
            .with_max_initial_upload_size(0)
            .with_max_chunk_size(4)
            .with_max_retries(1)
            .with_retry_delay(Duration::from_millis(0));

        let upload = client
            .upload_from(b"retry-data".to_vec(), UploadMetadata::new())
            .await
            .unwrap();

        assert_eq!(upload.offset, 10);
        assert_eq!(upload.length, Some(10));
        assert_eq!(transport.injected_failures(), 0);

        handle.abort();
    }

    #[tokio::test]
    async fn resume_after_mid_body_drop_completes_the_upload() {
        let (endpoint, handle) = spawn_test_server().await;

        let contents: Vec<u8> = (0..32u8).collect();
        let transport = MidBodyDropTransport::new(16);
        let client = Client::with_transport(endpoint_url(&endpoint), transport.clone())
            .with_max_initial_upload_size(0)
            .with_max_chunk_size(contents.len())
            .with_max_retries(3)
            .with_retry_delay(Duration::from_millis(10));

        let upload = client
            .upload_from(contents.clone(), UploadMetadata::new())
            .await
            .expect("upload must eventually complete via resume");

        assert_eq!(upload.offset, contents.len() as u64);
        assert_eq!(upload.length, Some(contents.len() as u64));
        assert_eq!(
            transport.partial_acks(),
            1,
            "fault transport should have delivered one partial chunk to the server"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn server_capabilities_returns_advertised_extensions() {
        let (endpoint, _handle) =
            spawn_test_server_with_config(Config::all_extensions().with_max_size(1024 * 1024))
                .await;
        let client = Client::new(endpoint_url(&endpoint));
        let info = client
            .server_capabilities()
            .await
            .expect("server capabilities");
        assert!(
            info.supports_version("1.0.0"),
            "expected Tus-Version 1.0.0, got {:?}",
            info.versions
        );
        assert!(info.has_extension("creation"));
        assert!(info.has_extension("termination"));
        assert_eq!(info.max_size, Some(1024 * 1024));
        assert!(!info.checksum_algorithms.is_empty());
    }

    #[tokio::test]
    async fn server_capabilities_returns_empty_extensions_for_minimal_config() {
        let (endpoint, _handle) = spawn_test_server().await;
        let client = Client::new(endpoint_url(&endpoint));
        let info = client
            .server_capabilities()
            .await
            .expect("server capabilities");
        assert!(info.has_extension("creation"));
        assert!(!info.has_extension("checksum"));
    }

    fn transport_response(
        status: u16,
        headers: http::HeaderMap,
        body: Vec<u8>,
    ) -> TransportResponse {
        let mut response = http::Response::builder().status(status).body(body).unwrap();
        *response.headers_mut() = headers;
        response
    }
}
