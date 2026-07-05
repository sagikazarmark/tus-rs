//! Router configuration for TUS endpoints.
//!
//! This module provides functions to create an axum Router with all TUS
//! endpoints configured with the appropriate handlers and the
//! `X-HTTP-Method-Override` POST fallback for proxy-constrained clients.
//! CORS is opt-in through [`RouterOptions`] (an HTTP-adapter concern), not
//! part of [`tus_protocol::Config`].

use axum::{
    Router,
    http::{HeaderName, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, options},
};
use tower_http::cors::{Any, CorsLayer};

use tus_protocol::{
    Config, HookExecutor, Locker, StateStore, Storage, StorageReader, TUS_RESUMABLE,
    TUS_SUCCESS_RESPONSE_HEADERS,
};

use crate::handlers;
use crate::state::TusState;

/// `Allow` header value for the collection route (`POST`/`OPTIONS`).
pub(crate) const BASE_ALLOW: &str = "OPTIONS, POST";

/// `Allow` header value for the upload item route without the download
/// endpoint. POST is listed because of the `X-HTTP-Method-Override` fallback.
pub(crate) const UPLOAD_ALLOW: &str = "OPTIONS, HEAD, POST, PATCH, DELETE";

/// `Allow` header value for the upload item route when the non-standard GET
/// download endpoint is registered.
pub(crate) const UPLOAD_ALLOW_WITH_DOWNLOAD: &str = "OPTIONS, GET, HEAD, POST, PATCH, DELETE";

/// Builds the 405 response for methods a TUS route does not accept.
///
/// The tus spec requires every response to carry `Tus-Resumable`, and
/// RFC 9110 requires 405 responses to carry `Allow`; axum's default
/// method-not-allowed fallback provides neither.
fn method_not_allowed_response(allow: &'static str) -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [
            (header::ALLOW, HeaderValue::from_static(allow)),
            (
                HeaderName::from_static("tus-resumable"),
                HeaderValue::from_static(TUS_RESUMABLE),
            ),
        ],
    )
        .into_response()
}

/// Creates an axum Router for TUS endpoints.
///
/// The router is configured with:
/// - All TUS protocol endpoints (OPTIONS, POST, HEAD, PATCH, DELETE)
/// - The provided TusState as application state
///
/// CORS is an HTTP-adapter concern and is configured through the passed
/// [`RouterOptions`], not through [`tus_protocol::Config`]. Pass
/// [`RouterOptions::default()`] (or [`RouterOptions::new`]) to apply no CORS
/// layer.
///
/// The upload routes are mounted under [`Config::base_path`]
/// (`tus_protocol::Config::with_base_path`). The base path must start with
/// `/`; a single trailing slash is stripped so `/files/` and `/files` mount
/// the same routes.
///
/// # Errors
///
/// Returns [`RouterError::InvalidCorsOrigin`] when a configured CORS origin is
/// not a valid header value; a misconfigured origin list must fail startup
/// instead of silently shrinking the allowlist. Returns
/// [`RouterError::InvalidBasePath`] when the configured base path is empty or
/// does not start with `/`. Routing such a path would otherwise panic inside
/// axum at startup.
///
/// # Example
///
/// ```rust,no_run
/// # use tus_axum::{create_router, RouterOptions, TusState};
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
/// let router = create_router(state, &RouterOptions::default())?;
/// let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
/// axum::serve(listener, router).await?;
/// # Ok(())
/// # }
/// ```
pub fn create_router<S, I, L, H>(
    state: TusState<S, I, L, H>,
    options: &RouterOptions,
) -> Result<Router, RouterError>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let router = build_upload_router(state.config(), UPLOAD_ALLOW)?;
    finish_router(router, state, options, false)
}

/// Creates an axum Router for TUS endpoints plus non-standard GET downloads.
///
/// GET download is a convenience endpoint outside the core TUS protocol. Use
/// this router when the storage adapter implements [`StorageReader`] and should
/// expose completed upload bytes through `GET /{upload_id}`.
///
/// CORS is configured through the passed [`RouterOptions`]; pass
/// [`RouterOptions::default()`] to apply no CORS layer.
///
/// # Errors
///
/// Returns [`RouterError::InvalidCorsOrigin`] when a configured CORS origin is
/// not a valid header value, and [`RouterError::InvalidBasePath`] when the
/// configured base path is empty or does not start with `/`.
pub fn create_router_with_download<S, I, L, H>(
    state: TusState<S, I, L, H>,
    options: &RouterOptions,
) -> Result<Router, RouterError>
where
    S: Storage + StorageReader + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let paths = route_paths(state.config())?;
    let router = build_upload_router(state.config(), UPLOAD_ALLOW_WITH_DOWNLOAD)?
        .route(&paths.upload, get(handlers::handle_get::<S, I, L, H>));

    finish_router(router, state, options, true)
}

/// Router-level options for the TUS axum integration.
///
/// HTTP-adapter concerns (such as CORS) live here rather than in the
/// framework-neutral [`tus_protocol::Config`].
#[derive(Debug, Clone, Default)]
pub struct RouterOptions {
    cors_allowed_origins: Vec<String>,
}

impl RouterOptions {
    /// Creates empty options: no CORS layer is applied.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Allows the given CORS origins. Pass `"*"` to allow any origin.
    #[must_use]
    pub fn with_cors_allowed_origins<I, S>(mut self, origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.cors_allowed_origins = origins.into_iter().map(Into::into).collect();
        self
    }

    /// Allows any CORS origin.
    #[must_use]
    pub fn with_cors_any_origin(mut self) -> Self {
        self.cors_allowed_origins = vec!["*".to_string()];
        self
    }

    fn cors_enabled(&self) -> bool {
        !self.cors_allowed_origins.is_empty()
    }
}

/// Error building a TUS router from [`tus_protocol::Config`] and [`RouterOptions`].
#[derive(Debug)]
#[non_exhaustive]
pub enum RouterError {
    /// A configured CORS origin is not a valid header value.
    InvalidCorsOrigin(String),
    /// The configured base path is empty or does not start with `/`.
    ///
    /// Feeding such a path into `axum::Router::route` would panic at
    /// startup, so router construction rejects it up front.
    InvalidBasePath(String),
}

impl std::fmt::Display for RouterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouterError::InvalidCorsOrigin(origin) => {
                write!(f, "invalid CORS origin: {origin:?}")
            }
            RouterError::InvalidBasePath(path) => {
                write!(f, "invalid TUS base path {path:?}: must start with '/'")
            }
        }
    }
}

impl std::error::Error for RouterError {}

/// Validated axum route templates derived from the configured base path.
struct RoutePaths {
    /// Collection route (`POST`/`OPTIONS`), e.g. `/files`.
    base: String,
    /// Item route with the `{upload_id}` capture, e.g. `/files/{upload_id}`.
    upload: String,
}

/// Validates and normalizes the configured base path into route templates.
///
/// Rejects empty paths and paths that do not start with `/` (axum would
/// panic when routing them). A single trailing slash is stripped so
/// `/files/` produces `/files/{upload_id}` rather than the broken
/// `/files//{upload_id}`. A bare `/` mounts the routes at the server root.
fn route_paths(config: &Config) -> Result<RoutePaths, RouterError> {
    let raw = config.base_path();
    // Braces would be parsed as axum capture syntax (or panic inside
    // `Router::route` when malformed), and empty segments produce broken
    // `//`-routes; both must fail construction instead.
    if !raw.starts_with('/') || raw.contains(['{', '}']) || raw.contains("//") {
        return Err(RouterError::InvalidBasePath(raw.to_string()));
    }

    let base = if raw.len() > 1 {
        raw.strip_suffix('/').unwrap_or(raw)
    } else {
        raw
    };

    let upload = if base == "/" {
        "/{upload_id}".to_string()
    } else {
        format!("{base}/{{upload_id}}")
    };

    Ok(RoutePaths {
        base: base.to_string(),
        upload,
    })
}

fn build_upload_router<S, I, L, H>(
    config: &Config,
    upload_allow: &'static str,
) -> Result<Router<TusState<S, I, L, H>>, RouterError>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let RoutePaths {
        base: base_path,
        upload: upload_path,
    } = route_paths(config)?;

    // Create router with all TUS endpoints. Each path gets a single
    // MethodRouter whose fallback replaces axum's bare 405 with a
    // spec-compliant one (`Tus-Resumable` + `Allow`).
    let router = Router::new()
        // Base path endpoints
        .route(
            &base_path,
            options(handlers::handle_options::<S, I, L, H>)
                .post(handlers::handle_post::<S, I, L, H>)
                .fallback(|| async { method_not_allowed_response(BASE_ALLOW) }),
        )
        // Upload-specific endpoints. POST is the X-HTTP-Method-Override
        // fallback: a POST on the item resource is rewritten to PATCH or
        // DELETE according to the override header. (Implemented as a POST
        // handler rather than routing middleware because axum's
        // `Router::layer` wraps per-endpoint, including the 405 fallback —
        // so rewriting `req.method()` from middleware does not re-dispatch
        // to a different method handler.)
        .route(
            &upload_path,
            options(handlers::handle_options::<S, I, L, H>)
                .head(handlers::handle_head::<S, I, L, H>)
                .patch(handlers::handle_patch::<S, I, L, H>)
                .delete(handlers::handle_delete::<S, I, L, H>)
                .post(handlers::handle_post_with_override::<S, I, L, H>)
                .fallback(move || async move { method_not_allowed_response(upload_allow) }),
        );

    Ok(router)
}

fn finish_router<S, I, L, H>(
    router: Router<TusState<S, I, L, H>>,
    state: TusState<S, I, L, H>,
    options: &RouterOptions,
    download: bool,
) -> Result<Router, RouterError>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    // Only apply CORS layer when CORS is explicitly configured.
    // CorsLayer intercepts OPTIONS requests for preflight handling, which would
    // prevent the TUS OPTIONS handler from running and returning TUS headers.
    if options.cors_enabled() {
        Ok(router
            .layer(build_cors_layer(options, download)?)
            .with_state(state))
    } else {
        Ok(router.with_state(state))
    }
}

/// Response headers exposed to CORS clients.
///
/// Always exposes the protocol's success response headers. When the
/// non-standard GET download route is registered (`download` is true), also
/// exposes the range-related download headers (`Content-Range`,
/// `Accept-Ranges`); they are not CORS-safelisted and would otherwise be
/// invisible to browser clients issuing range requests. (`Content-Disposition`
/// is not listed because the download path never sets it.)
fn exposed_headers(download: bool) -> Vec<HeaderName> {
    let mut headers: Vec<HeaderName> = TUS_SUCCESS_RESPONSE_HEADERS
        .iter()
        .copied()
        .map(HeaderName::from_static)
        .collect();

    if download {
        headers.push(HeaderName::from_static("content-range"));
        headers.push(HeaderName::from_static("accept-ranges"));
    }

    headers
}

/// Builds the CORS layer from router options.
///
/// Only called when CORS is enabled (the origin list is non-empty). The
/// CorsLayer intercepts OPTIONS requests for preflight handling.
fn build_cors_layer(options: &RouterOptions, download: bool) -> Result<CorsLayer, RouterError> {
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
        .expose_headers(exposed_headers(download))
        .max_age(std::time::Duration::from_secs(86400));

    if options
        .cors_allowed_origins
        .iter()
        .any(|origin| origin == "*")
    {
        Ok(cors.allow_origin(Any))
    } else {
        let origins = options
            .cors_allowed_origins
            .iter()
            .map(|origin| {
                origin
                    .parse::<HeaderValue>()
                    .map_err(|_| RouterError::InvalidCorsOrigin(origin.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(cors.allow_origin(origins))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use bytes::Bytes;
    use http::HeaderMap;
    use http::StatusCode;
    use http_body_util::{BodyExt, Full};
    use std::convert::Infallible;
    use std::sync::Arc;
    use tower::ServiceExt;
    use tus_protocol::locking::memory::MemoryLocker;
    use tus_protocol::state::memory::MemoryStateStore;
    use tus_protocol::storage::memory::MemoryStorage;
    use tus_protocol::{
        AppendRequest, ChunkStream, ConcatRequest, Extension, HookChain, NoopHookExecutor,
        NoopLocker, PreHookResult, ProtocolHandle, Result, StorageHandle, UploadState, WriteMode,
    };

    fn upload_only_router() -> Router {
        let protocol = ProtocolHandle::new(
            Config::default(),
            UploadOnlyStorage,
            MemoryStateStore::new(),
            NoopLocker::new(),
            NoopHookExecutor::new(),
        );

        create_router(TusState::new(protocol), &RouterOptions::default()).unwrap()
    }

    fn router_with_parts(
        config: Config,
        storage: Arc<MemoryStorage>,
        state_store: Arc<MemoryStateStore>,
    ) -> Router {
        let protocol = ProtocolHandle::from_arcs(
            Arc::new(config),
            storage,
            state_store,
            Arc::new(MemoryLocker::new()),
            Arc::new(NoopHookExecutor::new()),
        );

        create_router(TusState::new(protocol), &RouterOptions::default()).unwrap()
    }

    fn download_router_with_parts(
        config: Config,
        storage: Arc<MemoryStorage>,
        state_store: Arc<MemoryStateStore>,
    ) -> Router {
        let protocol = ProtocolHandle::from_arcs(
            Arc::new(config),
            storage,
            state_store,
            Arc::new(MemoryLocker::new()),
            Arc::new(NoopHookExecutor::new()),
        );

        create_router_with_download(TusState::new(protocol), &RouterOptions::default()).unwrap()
    }

    async fn seed_upload(
        storage: &MemoryStorage,
        state_store: &MemoryStateStore,
        id: &str,
        length: u64,
        bytes: Option<Bytes>,
    ) {
        let mut upload = UploadState::new(id).with_length(length);
        let handle = storage.create(upload.id()).await.unwrap();
        upload.set_storage_handle(handle);

        if let Some(bytes) = bytes {
            let projected_offset = upload.offset().saturating_add(bytes.len() as u64);
            let handle = storage
                .append(AppendRequest::new(
                    upload.storage_handle().unwrap(),
                    upload.offset(),
                    ChunkStream::from_bytes(bytes),
                    projected_offset == length,
                ))
                .await
                .unwrap();
            upload.set_storage_handle(handle);
        }

        state_store.set(&upload, WriteMode::CreateNew).await.unwrap();
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

    #[test]
    fn create_router_rejects_base_path_without_leading_slash() {
        let protocol = ProtocolHandle::new(
            Config::default().with_base_path("files"),
            UploadOnlyStorage,
            MemoryStateStore::new(),
            NoopLocker::new(),
            NoopHookExecutor::new(),
        );

        let err = create_router(TusState::new(protocol), &RouterOptions::default()).unwrap_err();
        assert!(matches!(err, RouterError::InvalidBasePath(ref path) if path == "files"));
    }

    #[test]
    fn create_router_rejects_base_path_with_braces() {
        let protocol = ProtocolHandle::new(
            Config::default().with_base_path("/files/{tenant}"),
            UploadOnlyStorage,
            MemoryStateStore::new(),
            NoopLocker::new(),
            NoopHookExecutor::new(),
        );

        let err = create_router(TusState::new(protocol), &RouterOptions::default()).unwrap_err();
        assert!(matches!(err, RouterError::InvalidBasePath(_)));
    }

    #[test]
    fn create_router_rejects_base_path_with_empty_segment() {
        let protocol = ProtocolHandle::new(
            Config::default().with_base_path("/files//nested"),
            UploadOnlyStorage,
            MemoryStateStore::new(),
            NoopLocker::new(),
            NoopHookExecutor::new(),
        );

        let err = create_router(TusState::new(protocol), &RouterOptions::default()).unwrap_err();
        assert!(matches!(err, RouterError::InvalidBasePath(_)));
    }

    #[test]
    fn create_router_rejects_empty_base_path() {
        let protocol = ProtocolHandle::new(
            Config::default().with_base_path(""),
            UploadOnlyStorage,
            MemoryStateStore::new(),
            NoopLocker::new(),
            NoopHookExecutor::new(),
        );

        let err = create_router(TusState::new(protocol), &RouterOptions::default()).unwrap_err();
        assert!(matches!(err, RouterError::InvalidBasePath(ref path) if path.is_empty()));
    }

    #[tokio::test]
    async fn create_router_strips_single_trailing_slash_from_base_path() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(&storage, &state_store, "test-id", 1000, None).await;
        let router = router_with_parts(
            Config::default().with_base_path("/files/"),
            storage,
            state_store,
        );

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri("/files/test-id")
                    .header("tus-resumable", "1.0.0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn create_router_mounts_at_root_base_path() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(&storage, &state_store, "test-id", 1000, None).await;
        let router = router_with_parts(Config::default().with_base_path("/"), storage, state_store);

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri("/test-id")
                    .header("tus-resumable", "1.0.0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    /// A chunked PATCH whose transport body limit trips mid-stream (as
    /// `tower_http::limit::RequestBodyLimitLayer` does) must answer 413,
    /// not 500. `http_body_util::Limited` produces the same
    /// `LengthLimitError`-wrapped body read error as the tower-http layer.
    #[tokio::test]
    async fn chunked_patch_over_transport_body_limit_answers_413() {
        use http_body_util::Limited;

        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(&storage, &state_store, "test-id", 100, None).await;
        let router = router_with_parts(Config::default(), storage, state_store);

        let limited = Limited::new(Full::new(Bytes::from_static(b"Hello World")), 4);

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::PATCH)
                    .uri("/files/test-id")
                    .header("tus-resumable", "1.0.0")
                    .header("upload-offset", "0")
                    .header("content-type", "application/offset+octet-stream")
                    .body(Body::new(limited))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn create_router_does_not_register_download_route() {
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
        // The tus spec requires Tus-Resumable on every response, and RFC 9110
        // requires Allow on 405s; axum's bare fallback provides neither.
        assert_eq!(
            response.headers().get("tus-resumable").unwrap(),
            tus_protocol::TUS_RESUMABLE
        );
        assert_eq!(response.headers().get("allow").unwrap(), UPLOAD_ALLOW);
    }

    #[tokio::test]
    async fn unhandled_method_on_base_path_answers_405_with_tus_and_allow_headers() {
        let response = upload_only_router()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/files")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            response.headers().get("tus-resumable").unwrap(),
            tus_protocol::TUS_RESUMABLE
        );
        assert_eq!(response.headers().get("allow").unwrap(), BASE_ALLOW);
    }

    #[tokio::test]
    async fn unhandled_method_on_download_router_lists_get_in_allow() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        let router = download_router_with_parts(Config::default(), storage, state_store);

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/files/test-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            response.headers().get("tus-resumable").unwrap(),
            tus_protocol::TUS_RESUMABLE
        );
        assert_eq!(
            response.headers().get("allow").unwrap(),
            UPLOAD_ALLOW_WITH_DOWNLOAD
        );
    }

    /// Downloads registered on the router but disabled in [`Config`] answer
    /// 405; that deliberate 405 must also carry `Allow` (without GET, which
    /// the config rejects).
    #[tokio::test]
    async fn download_disabled_in_config_answers_405_with_allow_header() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(&storage, &state_store, "test-id", 1000, None).await;
        let router =
            download_router_with_parts(Config::default().without_download(), storage, state_store);

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

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers().get("allow").unwrap(), UPLOAD_ALLOW);
        assert_eq!(
            response.headers().get("tus-resumable").unwrap(),
            tus_protocol::TUS_RESUMABLE
        );
    }

    #[tokio::test]
    async fn router_options_returns_tus_headers() {
        let router = upload_only_router();

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/files")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("tus-resumable").unwrap(), "1.0.0");
    }

    #[tokio::test]
    async fn router_creates_upload() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        let router = router_with_parts(Config::default(), storage, state_store.clone());

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/files")
                    .header("tus-resumable", "1.0.0")
                    .header("upload-length", "1000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let location = response
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap();
        let id = location.rsplit('/').next().unwrap();
        let stored = state_store.get(id).await.unwrap().unwrap();
        assert_eq!(stored.length(), Some(1000));
    }

    #[tokio::test]
    async fn router_reports_upload_status() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(&storage, &state_store, "test-id", 1000, None).await;
        let router = router_with_parts(Config::default(), storage, state_store);

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri("/files/test-id")
                    .header("tus-resumable", "1.0.0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("upload-length").unwrap(), "1000");
    }

    #[tokio::test]
    async fn router_writes_patch_body() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(&storage, &state_store, "test-id", 100, None).await;
        let router = router_with_parts(Config::default(), storage, state_store.clone());

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::PATCH)
                    .uri("/files/test-id")
                    .header("tus-resumable", "1.0.0")
                    .header("upload-offset", "0")
                    .header("content-type", "application/offset+octet-stream")
                    .body(Body::from(Bytes::from_static(b"Hello World")))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers().get("upload-offset").unwrap(), "11");
        assert_eq!(
            state_store.get("test-id").await.unwrap().unwrap().offset(),
            11
        );
    }

    #[tokio::test]
    async fn router_passes_checksum_trailers_to_protocol() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(&storage, &state_store, "test-id", 100, None).await;
        let router = router_with_parts(
            Config::default().with_extension(Extension::ChecksumTrailer),
            storage,
            state_store,
        );
        let mut trailers = HeaderMap::new();
        trailers.insert(
            "upload-checksum",
            "sha1 qvTGHdzF6KLavt4PO0gs2a6pQ00=".parse().unwrap(),
        );
        let body = Full::new(Bytes::from_static(b"hello"))
            .with_trailers(std::future::ready(Some(Ok::<_, Infallible>(trailers))))
            .map_err(|never| match never {});

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::PATCH)
                    .uri("/files/test-id")
                    .header("tus-resumable", "1.0.0")
                    .header("upload-offset", "0")
                    .header("content-type", "application/offset+octet-stream")
                    .body(Body::new(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers().get("upload-offset").unwrap(), "5");
    }

    #[tokio::test]
    async fn router_deletes_upload() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(&storage, &state_store, "test-id", 1000, None).await;
        let router = router_with_parts(
            Config::default().with_extension(Extension::Termination),
            storage,
            state_store.clone(),
        );

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/files/test-id")
                    .header("tus-resumable", "1.0.0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(state_store.get("test-id").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn router_post_override_requires_supported_method() {
        let router = upload_only_router();

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/files/test-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers().get("allow").unwrap(), UPLOAD_ALLOW);
        assert_eq!(
            response.headers().get("tus-resumable").unwrap(),
            tus_protocol::TUS_RESUMABLE
        );

        let router = upload_only_router();
        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/files/test-id")
                    .header("x-http-method-override", "GET")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers().get("allow").unwrap(), UPLOAD_ALLOW);
    }

    /// HTTP method names in `X-HTTP-Method-Override` are matched
    /// case-insensitively: `"patch"` would otherwise parse as an extension
    /// method distinct from `Method::PATCH` and be rejected with 405.
    #[tokio::test]
    async fn router_post_override_accepts_lowercase_method() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(&storage, &state_store, "upload-1", 100, None).await;
        let router = router_with_parts(Config::all_extensions(), storage, state_store.clone());

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/files/upload-1")
                    .header("x-http-method-override", "patch")
                    .header("tus-resumable", "1.0.0")
                    .header("upload-offset", "0")
                    .header("content-type", "application/offset+octet-stream")
                    .body(Body::from(Bytes::from_static(b"Hello")))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers().get("upload-offset").unwrap(), "5");
    }

    #[tokio::test]
    async fn router_post_override_accepts_mixed_case_method() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(&storage, &state_store, "upload-1", 100, None).await;
        let router = router_with_parts(Config::all_extensions(), storage, state_store.clone());

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/files/upload-1")
                    .header("x-http-method-override", "Delete")
                    .header("tus-resumable", "1.0.0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(state_store.get("upload-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn router_post_override_dispatches_delete() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(&storage, &state_store, "upload-1", 100, None).await;
        let router = router_with_parts(Config::all_extensions(), storage, state_store.clone());

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/files/upload-1")
                    .header("x-http-method-override", "DELETE")
                    .header("tus-resumable", "1.0.0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(state_store.get("upload-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn router_post_override_dispatches_patch_body() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(&storage, &state_store, "upload-1", 100, None).await;
        let router = router_with_parts(Config::all_extensions(), storage, state_store.clone());

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/files/upload-1")
                    .header("x-http-method-override", "PATCH")
                    .header("tus-resumable", "1.0.0")
                    .header("upload-offset", "0")
                    .header("content-type", "application/offset+octet-stream")
                    .body(Body::from(Bytes::from_static(b"Hello")))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers().get("upload-offset").unwrap(), "5");
        assert_eq!(
            state_store.get("upload-1").await.unwrap().unwrap().offset(),
            5
        );
    }

    #[tokio::test]
    async fn create_router_with_download_registers_download_route() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        seed_upload(
            &storage,
            &state_store,
            "test-id",
            5,
            Some(Bytes::from_static(b"hello")),
        )
        .await;
        let router = download_router_with_parts(Config::default(), storage, state_store);

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
        let options = RouterOptions::new().with_cors_any_origin();
        let _ = build_cors_layer(&options, false).unwrap();
    }

    #[test]
    fn test_build_cors_layer_specific_origins() {
        let options = RouterOptions::new()
            .with_cors_allowed_origins(["http://localhost:3000", "https://example.com"]);
        let _ = build_cors_layer(&options, false).unwrap();
    }

    #[test]
    fn test_build_cors_layer_rejects_invalid_origin() {
        let options =
            RouterOptions::new().with_cors_allowed_origins(["http://ok.example", "bad\norigin"]);
        let err = build_cors_layer(&options, false).unwrap_err();
        assert!(matches!(err, RouterError::InvalidCorsOrigin(origin) if origin.contains("bad")));
    }

    #[tokio::test]
    async fn cors_preflight_allows_checksum_trailer_header() {
        use axum::body::Body;
        use axum::http::{Request, Response, StatusCode};
        use tower::{ServiceBuilder, ServiceExt, service_fn};

        let service = ServiceBuilder::new()
            .layer(build_cors_layer(&RouterOptions::new().with_cors_any_origin(), false).unwrap())
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
            .layer(build_cors_layer(&RouterOptions::new().with_cors_any_origin(), false).unwrap())
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

    #[tokio::test]
    async fn cors_exposes_protocol_success_response_headers() {
        use axum::body::Body;
        use axum::http::{Request, Response, StatusCode};
        use tower::{ServiceBuilder, ServiceExt, service_fn};

        let service = ServiceBuilder::new()
            .layer(build_cors_layer(&RouterOptions::new().with_cors_any_origin(), false).unwrap())
            .service(service_fn(|_req: Request<Body>| async {
                Ok::<_, std::convert::Infallible>(Response::new(Body::empty()))
            }));

        let response = service
            .oneshot(
                Request::builder()
                    .method(Method::PATCH)
                    .uri("/files/test-id")
                    .header("origin", "https://example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let expose_headers = response
            .headers()
            .get("access-control-expose-headers")
            .unwrap()
            .to_str()
            .unwrap();

        for expected in tus_protocol::TUS_SUCCESS_RESPONSE_HEADERS {
            assert!(
                expose_headers
                    .split(',')
                    .map(str::trim)
                    .any(|actual| actual.eq_ignore_ascii_case(expected)),
                "{expected} missing from Access-Control-Expose-Headers: {expose_headers}",
            );
        }
    }

    /// With the download route registered, browser clients need to read the
    /// range-related response headers, which are not CORS-safelisted.
    #[tokio::test]
    async fn cors_exposes_download_headers_when_download_route_enabled() {
        use axum::body::Body;
        use axum::http::{Request, Response, StatusCode};
        use tower::{ServiceBuilder, ServiceExt, service_fn};

        let service = ServiceBuilder::new()
            .layer(build_cors_layer(&RouterOptions::new().with_cors_any_origin(), true).unwrap())
            .service(service_fn(|_req: Request<Body>| async {
                Ok::<_, std::convert::Infallible>(Response::new(Body::empty()))
            }));

        let response = service
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/files/test-id")
                    .header("origin", "https://example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let expose_headers = response
            .headers()
            .get("access-control-expose-headers")
            .unwrap()
            .to_str()
            .unwrap();

        for expected in ["content-range", "accept-ranges"] {
            assert!(
                expose_headers
                    .split(',')
                    .map(str::trim)
                    .any(|actual| actual.eq_ignore_ascii_case(expected)),
                "{expected} missing from Access-Control-Expose-Headers: {expose_headers}",
            );
        }
        // The protocol success headers stay exposed too.
        for expected in tus_protocol::TUS_SUCCESS_RESPONSE_HEADERS {
            assert!(
                expose_headers
                    .split(',')
                    .map(str::trim)
                    .any(|actual| actual.eq_ignore_ascii_case(expected)),
                "{expected} missing from Access-Control-Expose-Headers: {expose_headers}",
            );
        }
    }

    /// Without the download route, the exposed-headers list stays exactly the
    /// protocol success set: no download headers leak in.
    #[tokio::test]
    async fn cors_exposure_excludes_download_headers_without_download_route() {
        use axum::body::Body;
        use axum::http::{Request, Response};
        use tower::{ServiceBuilder, ServiceExt, service_fn};

        let service = ServiceBuilder::new()
            .layer(build_cors_layer(&RouterOptions::new().with_cors_any_origin(), false).unwrap())
            .service(service_fn(|_req: Request<Body>| async {
                Ok::<_, std::convert::Infallible>(Response::new(Body::empty()))
            }));

        let response = service
            .oneshot(
                Request::builder()
                    .method(Method::PATCH)
                    .uri("/files/test-id")
                    .header("origin", "https://example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let expose_headers = response
            .headers()
            .get("access-control-expose-headers")
            .unwrap()
            .to_str()
            .unwrap();

        for excluded in ["content-range", "accept-ranges"] {
            assert!(
                !expose_headers
                    .split(',')
                    .map(str::trim)
                    .any(|actual| actual.eq_ignore_ascii_case(excluded)),
                "{excluded} unexpectedly exposed without download route: {expose_headers}",
            );
        }
    }

    #[tokio::test]
    async fn cors_exposure_excludes_hook_added_response_headers() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        let hooks = HookChain::new().on_pre_create(|_| async {
            Ok(PreHookResult::proceed().with_header("x-hook", "created"))
        });
        let protocol = ProtocolHandle::from_arcs(
            Arc::new(Config::default()),
            storage,
            state_store,
            Arc::new(MemoryLocker::new()),
            Arc::new(hooks),
        );
        let router = create_router(
            TusState::new(protocol),
            &RouterOptions::new().with_cors_any_origin(),
        )
        .unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/files")
                    .header("origin", "https://example.com")
                    .header("tus-resumable", "1.0.0")
                    .header("upload-length", "1000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers().get("x-hook").unwrap(), "created");
        let expose_headers = response
            .headers()
            .get("access-control-expose-headers")
            .unwrap()
            .to_str()
            .unwrap();

        assert!(
            !expose_headers
                .split(',')
                .map(str::trim)
                .any(|actual| actual.eq_ignore_ascii_case("x-hook")),
            "hook header leaked into Access-Control-Expose-Headers: {expose_headers}",
        );
    }
}
