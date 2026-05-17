//! Reqwest-backed TUS client transport.

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::transport::{Transport, TransportBody, TransportRequest, TransportResponse};

#[cfg(not(target_arch = "wasm32"))]
use {
    ::reqwest::Body,
    bytes::Bytes,
    http::header::{CONTENT_LENGTH, HeaderValue},
    http_body_util::{BodyExt, Full},
};

#[cfg(feature = "transport-reqwest-middleware")]
type ReqwestClient = reqwest_middleware::ClientWithMiddleware;

#[cfg(not(feature = "transport-reqwest-middleware"))]
type ReqwestClient = ::reqwest::Client;

/// Default `reqwest`-backed transport.
#[derive(Clone, Debug)]
pub struct ReqwestTransport {
    client: ReqwestClient,
}

impl ReqwestTransport {
    /// Creates a new transport using reqwest's default client.
    pub fn new() -> Self {
        Self {
            client: default_reqwest_client(),
        }
    }

    /// Creates a new transport using a configured reqwest or middleware client.
    pub fn with_client(client: impl Into<Self>) -> Self {
        client.into()
    }
}

impl From<::reqwest::Client> for ReqwestTransport {
    fn from(client: ::reqwest::Client) -> Self {
        Self {
            client: reqwest_client(client),
        }
    }
}

#[cfg(feature = "transport-reqwest-middleware")]
impl From<reqwest_middleware::ClientWithMiddleware> for ReqwestTransport {
    fn from(client: reqwest_middleware::ClientWithMiddleware) -> Self {
        Self { client }
    }
}

impl Default for ReqwestTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg_attr(
    all(not(feature = "local-futures"), not(target_arch = "wasm32")),
    async_trait
)]
#[cfg_attr(any(feature = "local-futures", target_arch = "wasm32"), async_trait(?Send))]
impl Transport for ReqwestTransport {
    async fn send(&self, request: TransportRequest) -> Result<TransportResponse> {
        let (parts, body) = request.into_parts();
        let mut builder = self.client.request(parts.method, parts.uri.to_string());
        for (name, value) in &parts.headers {
            builder = builder.header(name, value);
        }

        builder = match body {
            TransportBody::Empty => builder,
            TransportBody::Bytes(body) => {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    builder.header(CONTENT_LENGTH, body.len()).body(body)
                }

                #[cfg(target_arch = "wasm32")]
                {
                    builder.body(body)
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
                        Error::Transport(format!(
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
                builder.body(Body::wrap(body))
            }
            #[cfg(target_arch = "wasm32")]
            TransportBody::BytesWithTrailer { .. } => {
                return Err(Error::Transport(
                    "reqwest transport does not support request trailers on wasm32".to_string(),
                ));
            }
        };

        let response = send_request(builder).await?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.bytes().await?.to_vec();

        let mut response = http::Response::builder()
            .status(status)
            .body(body)
            .map_err(|err| Error::Transport(format!("failed to build response: {err}")))?;
        *response.headers_mut() = headers;
        Ok(response)
    }
}

#[cfg(feature = "transport-reqwest-middleware")]
fn default_reqwest_client() -> ReqwestClient {
    reqwest_client(::reqwest::Client::new())
}

#[cfg(not(feature = "transport-reqwest-middleware"))]
fn default_reqwest_client() -> ReqwestClient {
    ::reqwest::Client::new()
}

#[cfg(feature = "transport-reqwest-middleware")]
fn reqwest_client(client: ::reqwest::Client) -> ReqwestClient {
    reqwest_middleware::ClientBuilder::new(client).build()
}

#[cfg(not(feature = "transport-reqwest-middleware"))]
fn reqwest_client(client: ::reqwest::Client) -> ReqwestClient {
    client
}

#[cfg(feature = "transport-reqwest-middleware")]
async fn send_request(builder: reqwest_middleware::RequestBuilder) -> Result<::reqwest::Response> {
    builder.send().await.map_err(reqwest_middleware_error)
}

#[cfg(not(feature = "transport-reqwest-middleware"))]
async fn send_request(builder: ::reqwest::RequestBuilder) -> Result<::reqwest::Response> {
    Ok(builder.send().await?)
}

#[cfg(feature = "transport-reqwest-middleware")]
fn reqwest_middleware_error(error: reqwest_middleware::Error) -> Error {
    match error {
        reqwest_middleware::Error::Reqwest(error) => Error::Http(error),
        reqwest_middleware::Error::Middleware(error) => {
            Error::Transport(format!("reqwest middleware failed: {error}"))
        }
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

    use http::{Method, header::CONTENT_TYPE};
    #[cfg(feature = "checksum")]
    use tus_protocol::ChecksumAlgorithm;
    use tus_protocol::{
        Config, Extension, ProtocolHandle, UploadMetadata, hooks::NoopHookExecutor,
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
        let app: axum::Router = tus_axum::create_router(state);
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

    #[cfg_attr(
        all(not(feature = "local-futures"), not(target_arch = "wasm32")),
        async_trait
    )]
    #[cfg_attr(
        any(feature = "local-futures", target_arch = "wasm32"),
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

    #[cfg_attr(
        all(not(feature = "local-futures"), not(target_arch = "wasm32")),
        async_trait
    )]
    #[cfg_attr(
        any(feature = "local-futures", target_arch = "wasm32"),
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
        let transport = ReqwestTransport::with_client(middleware_client);
        let client = Client::with_transport(endpoint_url(&endpoint), transport);

        client.server_capabilities().await.unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        handle.abort();
    }

    #[tokio::test]
    async fn reqwest_transport_accepts_configured_reqwest_client() {
        let (endpoint, handle) = spawn_test_server().await;
        let transport = ReqwestTransport::with_client(reqwest::Client::new());
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
        let upload = client
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
            .upload(upload.url().clone())
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
    async fn delete_upload_terminates_remote_upload() {
        let (endpoint, server_handle) = spawn_test_server().await;
        let client = Client::new(endpoint_url(&endpoint));
        let upload = client
            .create_upload(NewUpload::new(5, UploadMetadata::new()))
            .await
            .unwrap();

        let handle = client.upload(upload.url().clone());
        handle.delete().await.unwrap();

        let err = handle.info().await.unwrap_err();
        assert!(matches!(err, Error::UnexpectedResponse { status: 404, .. }));

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
            spawn_test_server_with_config(Config::with_all_extensions().max_size(1024 * 1024))
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
