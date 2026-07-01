//! Router configuration for TUS endpoints.
//!
//! This module provides functions to create an axum Router with all TUS
//! endpoints configured with the appropriate handlers, CORS, and the
//! `X-HTTP-Method-Override` POST fallback for proxy-constrained clients.

use axum::{
    Router,
    http::{HeaderName, HeaderValue, Method},
    routing::{delete, get, head, options, patch, post},
};
use tower_http::cors::{Any, CorsLayer};

use tus_protocol::{Config, HookExecutor, Locker, StateStore, Storage, StorageReader};

use crate::handlers;
use crate::state::TusState;

/// Creates an axum Router for TUS endpoints.
///
/// The router is configured with:
/// - All TUS protocol endpoints (OPTIONS, POST, HEAD, PATCH, DELETE)
/// - CORS middleware based on configuration
/// - The provided TusState as application state
///
/// # Example
///
/// ```rust,no_run
/// # use tus_axum::{create_router, TusState};
/// # use tus_protocol::{
/// #     Config, NoopHookExecutor, ProtocolHandle,
/// #     locking::memory::MemoryLocker,
/// #     state::memory::MemoryStateStore,
/// #     storage::memory::MemoryStorage,
/// # };
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let protocol = ProtocolHandle::new(
///     Config::default(),
///     MemoryStorage::new(),
///     MemoryStateStore::new(),
///     MemoryLocker::new(),
///     NoopHookExecutor::new(),
/// );
/// let state = TusState::new(protocol);
/// let router = create_router(state);
/// let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
/// axum::serve(listener, router).await?;
/// # Ok(())
/// # }
/// ```
pub fn create_router<S, I, L, H>(state: TusState<S, I, L, H>) -> Router
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    finish_router(build_upload_router(state.config()), state)
}

/// Creates an axum Router for TUS endpoints plus non-standard GET downloads.
///
/// GET download is a convenience endpoint outside the core TUS protocol. Use
/// this router when the storage adapter implements [`StorageReader`] and should
/// expose completed upload bytes through `GET /{upload_id}`.
pub fn create_router_with_download<S, I, L, H>(state: TusState<S, I, L, H>) -> Router
where
    S: Storage + StorageReader + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let upload_path = upload_path(state.config());
    let router = build_upload_router(state.config())
        .route(&upload_path, get(handlers::handle_get::<S, I, L, H>));

    finish_router(router, state)
}

fn build_upload_router<S, I, L, H>(config: &Config) -> Router<TusState<S, I, L, H>>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let base_path = config.base_path_str().to_string();

    // Create router with all TUS endpoints
    let upload_path = upload_path(config);
    Router::new()
        // Base path endpoints
        .route(&base_path, options(handlers::handle_options::<S, I, L, H>))
        .route(&base_path, post(handlers::handle_post::<S, I, L, H>))
        // Upload-specific endpoints
        .route(
            &upload_path,
            options(handlers::handle_options::<S, I, L, H>),
        )
        .route(&upload_path, head(handlers::handle_head::<S, I, L, H>))
        .route(&upload_path, patch(handlers::handle_patch::<S, I, L, H>))
        .route(&upload_path, delete(handlers::handle_delete::<S, I, L, H>))
        // Fallback for X-HTTP-Method-Override: a POST on the item resource
        // is rewritten to PATCH or DELETE according to the override header.
        // (Implemented as a POST handler rather than routing middleware
        // because axum's `Router::layer` wraps per-endpoint, including the
        // 405 fallback — so rewriting `req.method()` from middleware does
        // not re-dispatch to a different method handler.)
        .route(
            &upload_path,
            post(handlers::handle_post_with_override::<S, I, L, H>),
        )
}

fn upload_path(config: &Config) -> String {
    format!("{}/{{upload_id}}", config.base_path_str())
}

fn finish_router<S, I, L, H>(
    router: Router<TusState<S, I, L, H>>,
    state: TusState<S, I, L, H>,
) -> Router
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let cors_enabled = !state.config().cors_allowed_origins().is_empty();

    // Only apply CORS layer when CORS is explicitly configured.
    // CorsLayer intercepts OPTIONS requests for preflight handling, which would
    // prevent the TUS OPTIONS handler from running and returning TUS headers.
    if cors_enabled {
        router
            .layer(build_cors_layer(state.config()))
            .with_state(state)
    } else {
        router.with_state(state)
    }
}

/// Builds the CORS layer based on configuration.
///
/// Note: This function should only be called when CORS is enabled (cors_origins is non-empty).
/// The CorsLayer intercepts OPTIONS requests for preflight handling.
pub fn build_cors_layer(config: &Config) -> CorsLayer {
    let cors = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::DELETE,
            Method::HEAD,
            Method::OPTIONS,
        ])
        .allow_headers(vec![
            // Request-side headers the client sends.
            HeaderName::from_static("authorization"),
            HeaderName::from_static("tus-resumable"),
            HeaderName::from_static("upload-length"),
            HeaderName::from_static("upload-offset"),
            HeaderName::from_static("upload-metadata"),
            HeaderName::from_static("upload-defer-length"),
            HeaderName::from_static("upload-concat"),
            HeaderName::from_static("upload-checksum"),
            HeaderName::from_static("trailer"),
            HeaderName::from_static("content-type"),
            HeaderName::from_static("content-length"),
            // Response-side headers clients may echo via preflight.
            HeaderName::from_static("tus-version"),
            HeaderName::from_static("tus-extension"),
            HeaderName::from_static("tus-max-size"),
            HeaderName::from_static("tus-checksum-algorithm"),
            HeaderName::from_static("upload-expires"),
            // For clients behind proxies that block PATCH/DELETE.
            HeaderName::from_static("x-http-method-override"),
        ])
        .expose_headers(vec![
            HeaderName::from_static("tus-resumable"),
            HeaderName::from_static("tus-version"),
            HeaderName::from_static("tus-extension"),
            HeaderName::from_static("tus-max-size"),
            HeaderName::from_static("tus-checksum-algorithm"),
            HeaderName::from_static("upload-offset"),
            HeaderName::from_static("upload-length"),
            HeaderName::from_static("upload-defer-length"),
            HeaderName::from_static("upload-concat"),
            HeaderName::from_static("upload-expires"),
            HeaderName::from_static("upload-metadata"),
            HeaderName::from_static("location"),
        ])
        .max_age(std::time::Duration::from_secs(86400));

    if config
        .cors_allowed_origins()
        .iter()
        .any(|origin| origin == "*")
    {
        cors.allow_origin(Any)
    } else {
        // Parse origins
        let origins: Vec<HeaderValue> = config
            .cors_allowed_origins()
            .iter()
            .filter_map(|o| o.parse::<HeaderValue>().ok())
            .collect();
        cors.allow_origin(origins)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::StatusCode;
    use http_body_util::BodyExt;
    use tus_protocol::state::memory::MemoryStateStore;
    use tus_protocol::storage::memory::MemoryStorage;
    use tus_protocol::{
        AppendRequest, ChunkStream, ConcatRequest, NoopHookExecutor, NoopLocker, ProtocolHandle,
        Result, StorageHandle, UploadState,
    };

    fn upload_only_router() -> Router {
        let protocol = ProtocolHandle::new(
            Config::default(),
            UploadOnlyStorage,
            MemoryStateStore::new(),
            NoopLocker::new(),
            NoopHookExecutor::new(),
        );

        create_router(TusState::new(protocol))
    }

    struct UploadOnlyStorage;

    #[async_trait::async_trait]
    impl Storage for UploadOnlyStorage {
        fn name(&self) -> &'static str {
            "upload-only"
        }

        async fn create(&self, upload_id: &str) -> Result<StorageHandle> {
            Ok(StorageHandle::new(upload_id))
        }

        async fn append(&self, request: AppendRequest) -> Result<StorageHandle> {
            Ok(request.handle)
        }

        async fn concat(&self, request: ConcatRequest) -> Result<StorageHandle> {
            Ok(request.target)
        }

        async fn delete(&self, _handle: &StorageHandle) -> Result<()> {
            Ok(())
        }

        async fn size(&self, _handle: &StorageHandle) -> Result<Option<u64>> {
            Ok(Some(0))
        }
    }

    #[test]
    fn create_router_accepts_upload_only_storage() {
        let _router = upload_only_router();
    }

    #[tokio::test]
    async fn create_router_does_not_register_download_route() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let response = upload_only_router()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/files/test-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn create_router_with_download_registers_download_route() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let storage = MemoryStorage::new();
        let state_store = MemoryStateStore::new();
        let mut upload = UploadState::new("test-id").with_length(5);
        let handle = storage.create(upload.id()).await.unwrap();
        upload.set_storage_handle(handle);
        let handle = storage
            .append(AppendRequest {
                handle: upload.storage_handle().unwrap(),
                expected_offset: upload.offset(),
                data: ChunkStream::from_bytes(Bytes::from_static(b"hello")),
                completes_upload: true,
            })
            .await
            .unwrap();
        upload.set_storage_handle(handle);
        state_store.set(&upload, true).await.unwrap();

        let protocol = ProtocolHandle::new(
            Config::default(),
            storage,
            state_store,
            NoopLocker::new(),
            NoopHookExecutor::new(),
        );
        let router = create_router_with_download(TusState::new(protocol));

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/files/test-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"hello");
    }

    #[test]
    fn test_build_cors_layer_wildcard() {
        let config = Config::default().cors_all();
        let _ = build_cors_layer(&config);
    }

    #[test]
    fn test_build_cors_layer_specific_origins() {
        let config = Config::default().cors(vec![
            "http://localhost:3000".to_string(),
            "https://example.com".to_string(),
        ]);
        let _ = build_cors_layer(&config);
    }

    #[test]
    fn test_cors_not_applied_when_empty() {
        // When cors_origins is empty, CORS layer should not be applied
        // This is verified by the create_router logic, not build_cors_layer
        let config = Config::default();
        assert!(config.cors_allowed_origins().is_empty());
    }

    #[tokio::test]
    async fn cors_preflight_allows_checksum_trailer_header() {
        use axum::body::Body;
        use axum::http::{Request, Response, StatusCode};
        use tower::{ServiceBuilder, ServiceExt, service_fn};

        let service = ServiceBuilder::new()
            .layer(build_cors_layer(&Config::default().cors_all()))
            .service(service_fn(|_req: Request<Body>| async {
                Ok::<_, std::convert::Infallible>(Response::new(Body::empty()))
            }));

        let response = service
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/files")
                    .header("origin", "https://example.com")
                    .header("access-control-request-method", "PATCH")
                    .header("access-control-request-headers", "trailer, upload-checksum")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let allow_headers = response
            .headers()
            .get("access-control-allow-headers")
            .unwrap()
            .to_str()
            .unwrap()
            .to_ascii_lowercase();
        assert!(allow_headers.contains("trailer"));
    }

    #[tokio::test]
    async fn cors_preflight_allows_authorization_header() {
        use axum::body::Body;
        use axum::http::{Request, Response, StatusCode};
        use tower::{ServiceBuilder, ServiceExt, service_fn};

        let service = ServiceBuilder::new()
            .layer(build_cors_layer(&Config::default().cors_all()))
            .service(service_fn(|_req: Request<Body>| async {
                Ok::<_, std::convert::Infallible>(Response::new(Body::empty()))
            }));

        let response = service
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/files")
                    .header("origin", "https://example.com")
                    .header("access-control-request-method", "PATCH")
                    .header(
                        "access-control-request-headers",
                        "authorization, tus-resumable, upload-offset",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let allow_headers = response
            .headers()
            .get("access-control-allow-headers")
            .unwrap()
            .to_str()
            .unwrap()
            .to_ascii_lowercase();
        assert!(allow_headers.contains("authorization"));
    }
}
