use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::{
    Router,
    body::{Body, HttpBody as _},
    extract::{Request, State},
    http::{HeaderValue, Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
};
use tus_axum::TusState;
use tus_protocol::{HookExecutor, Locker, ProtocolHandle, StateStore, Storage, StorageReader};

#[derive(Clone, Debug, Default)]
pub(crate) struct AppSettings {
    pub(crate) auth_token: Vec<String>,
    pub(crate) max_request_body_bytes: usize,
    pub(crate) request_body_read_timeout: u64,
    pub(crate) cors_origins: Vec<String>,
}

pub(crate) fn build_app<S, I, L, H>(
    protocol: ProtocolHandle<S, I, L, H>,
    settings: &AppSettings,
    draining: Arc<AtomicBool>,
) -> anyhow::Result<Router>
where
    S: Storage + StorageReader + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let state = TusState::new(protocol);
    let options = tus_axum::RouterOptions::new()
        .with_cors_allowed_origins(settings.cors_origins.iter().cloned());
    let mut tus_router = tus_axum::create_router_with_download(state, &options)?;
    if !settings.auth_token.is_empty() {
        tracing::info!(
            tokens = settings.auth_token.len(),
            "enabling bearer-token auth on TUS routes"
        );
        tus_router = tus_router.layer(axum::middleware::from_fn_with_state(
            Arc::new(BearerAuthConfig {
                tokens: settings.auth_token.clone(),
                cors_enabled: !settings.cors_origins.is_empty(),
            }),
            bearer_auth,
        ));
    }

    let mut app = tus_router.merge(build_health_router(draining));

    if settings.max_request_body_bytes > 0 {
        tracing::info!(
            max_bytes = settings.max_request_body_bytes,
            "enforcing HTTP request body size limit"
        );
        app = app.layer(tower_http::limit::RequestBodyLimitLayer::new(
            settings.max_request_body_bytes,
        ));
    }

    if settings.request_body_read_timeout > 0 {
        let timeout = Duration::from_secs(settings.request_body_read_timeout);
        tracing::info!(
            idle_timeout_secs = timeout.as_secs(),
            "enforcing request body idle timeout"
        );
        app = app.layer(axum::middleware::from_fn_with_state(
            timeout,
            body_read_timeout,
        ));
    }

    Ok(app)
}

// Applies an idle timeout between successive request-body frames.
//
// tower_http's RequestBodyTimeoutLayer wraps every body in TimeoutBody,
// which does not forward `is_end_stream`. That makes already-finished
// (empty) bodies look supplied to the TUS body extractor, turning
// bodiless POST creations into 415s for clients that omit
// Content-Length. Wrapping only bodies that still have frames to read
// preserves end-of-stream detection while keeping the slowloris
// protection for real payloads.
async fn body_read_timeout(State(timeout): State<Duration>, req: Request, next: Next) -> Response {
    let (parts, body) = req.into_parts();
    let body = if body.is_end_stream() {
        body
    } else {
        Body::new(tower_http::timeout::TimeoutBody::new(timeout, body))
    };
    next.run(Request::from_parts(parts, body)).await
}

// Constant-time equality for equal-length byte slices. The upfront
// length check returns early and therefore leaks token length via
// timing, which is standard and acceptable; what this prevents is
// learning a token's bytes through early-exit comparison timing in a
// naive `==`.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// Bearer-auth middleware state: the accepted tokens plus whether CORS
// is configured, which decides if preflights may bypass auth.
struct BearerAuthConfig {
    tokens: Vec<String>,
    cors_enabled: bool,
}

async fn bearer_auth(
    State(config): State<Arc<BearerAuthConfig>>,
    req: Request,
    next: Next,
) -> Response {
    // Browser CORS preflights are sent without credentials, so when
    // CORS is enabled they must bypass auth for the CorsLayer inside
    // the TUS router to answer them. When CORS is disabled no
    // legitimate preflight can succeed anyway, and letting a forged
    // one through would hand unauthenticated clients the TUS OPTIONS
    // capability disclosure.
    if config.cors_enabled && is_cors_preflight(&req) {
        return next.run(req).await;
    }

    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(strip_bearer_scheme)
        .unwrap_or("");

    if !presented.is_empty()
        && config
            .tokens
            .iter()
            .any(|t| ct_eq(t.as_bytes(), presented.as_bytes()))
    {
        next.run(req).await
    } else {
        unauthorized_response()
    }
}

fn is_cors_preflight(req: &Request) -> bool {
    req.method() == Method::OPTIONS
        && req.headers().contains_key(header::ORIGIN)
        && req
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD)
}

// RFC 7235: the auth-scheme is case-insensitive and separated from
// its credentials by one or more spaces. Only the scheme is matched
// loosely; the token comparison itself stays exact.
fn strip_bearer_scheme(header: &str) -> Option<&str> {
    let (scheme, rest) = header.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("Bearer")
        .then(|| rest.trim_start_matches(' '))
}

// RFC 6750: a 401 to a bearer-protected resource must advertise the
// expected authentication scheme via WWW-Authenticate.
fn unauthorized_response() -> Response {
    let mut response = StatusCode::UNAUTHORIZED.into_response();
    response
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

fn build_health_router(draining: Arc<AtomicBool>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get(|State(flag): State<Arc<AtomicBool>>| async move {
                if flag.load(Ordering::Relaxed) {
                    (StatusCode::SERVICE_UNAVAILABLE, "draining")
                } else {
                    (StatusCode::OK, "ok")
                }
            }),
        )
        .with_state(draining)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::options;
    use tower::ServiceExt;

    fn authenticated_test_router(cors_enabled: bool) -> Router {
        Router::new()
            .route("/files", options(|| async { StatusCode::NO_CONTENT }))
            .layer(axum::middleware::from_fn_with_state(
                Arc::new(BearerAuthConfig {
                    tokens: vec!["secret".to_string()],
                    cors_enabled,
                }),
                bearer_auth,
            ))
    }

    fn preflight_request() -> Request<Body> {
        Request::builder()
            .method(Method::OPTIONS)
            .uri("/files")
            .header(header::ORIGIN, "https://example.com")
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "PATCH")
            .header(
                header::ACCESS_CONTROL_REQUEST_HEADERS,
                "authorization, tus-resumable, upload-offset",
            )
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn readyz_reports_draining_state() {
        let draining = Arc::new(AtomicBool::new(true));
        let app = build_health_router(draining);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn plain_options_requires_bearer_when_auth_is_enabled() {
        let response = authenticated_test_router(true)
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/files")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn unauthorized_response_advertises_bearer_scheme() {
        let response = authenticated_test_router(true)
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/files")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response
                .headers()
                .get(header::WWW_AUTHENTICATE)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer")
        );
    }

    #[tokio::test]
    async fn cors_preflight_bypasses_bearer_when_cors_is_enabled() {
        let response = authenticated_test_router(true)
            .oneshot(preflight_request())
            .await
            .unwrap();

        // The preflight must reach the OPTIONS handler (NO_CONTENT), not merely
        // fail somewhere other than the auth layer.
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn forged_cors_preflight_requires_bearer_when_cors_is_disabled() {
        let response = authenticated_test_router(false)
            .oneshot(preflight_request())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bearer_scheme_is_matched_case_insensitively() {
        for scheme in ["Bearer", "bearer", "BEARER", "BeArEr"] {
            let response = authenticated_test_router(false)
                .oneshot(
                    Request::builder()
                        .method(Method::OPTIONS)
                        .uri("/files")
                        .header(header::AUTHORIZATION, format!("{scheme} secret"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(
                response.status(),
                StatusCode::NO_CONTENT,
                "scheme `{scheme}` must authenticate"
            );
        }
    }

    #[tokio::test]
    async fn bearer_token_comparison_stays_exact() {
        for value in [
            "Bearer SECRET",
            "Bearer secre",
            "Bearersecret",
            "Basic secret",
        ] {
            let response = authenticated_test_router(false)
                .oneshot(
                    Request::builder()
                        .method(Method::OPTIONS)
                        .uri("/files")
                        .header(header::AUTHORIZATION, value)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "authorization `{value}` must be rejected"
            );
        }
    }

    #[test]
    fn strip_bearer_scheme_allows_multiple_separating_spaces() {
        assert_eq!(strip_bearer_scheme("Bearer  token"), Some("token"));
        assert_eq!(strip_bearer_scheme("bearer token"), Some("token"));
        assert_eq!(strip_bearer_scheme("Basic token"), None);
        assert_eq!(strip_bearer_scheme("Bearertoken"), None);
    }
}
