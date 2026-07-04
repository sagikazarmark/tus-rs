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
    let mut tus_router = tus_axum::create_router_with_download_and_options(state, &options)?;
    if !settings.auth_token.is_empty() {
        tracing::info!(
            tokens = settings.auth_token.len(),
            "enabling bearer-token auth on TUS routes"
        );
        tus_router = tus_router.layer(axum::middleware::from_fn_with_state(
            Arc::new(settings.auth_token.clone()),
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

// Constant-time byte-slice equality. Prevents the tiny amount of
// timing signal an attacker could use to learn a token's length or
// leading bytes via a naive `==` comparison.
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

async fn bearer_auth(State(tokens): State<Arc<Vec<String>>>, req: Request, next: Next) -> Response {
    let is_cors_preflight = req.method() == Method::OPTIONS
        && req.headers().contains_key(header::ORIGIN)
        && req
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD);

    // Browser CORS preflights are sent without credentials, but plain
    // OPTIONS requests should still authenticate like other TUS routes.
    if is_cors_preflight {
        return next.run(req).await;
    }

    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");

    if !presented.is_empty()
        && tokens
            .iter()
            .any(|t| ct_eq(t.as_bytes(), presented.as_bytes()))
    {
        next.run(req).await
    } else {
        unauthorized_response()
    }
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

    fn authenticated_test_router() -> Router {
        Router::new()
            .route("/files", options(|| async { StatusCode::NO_CONTENT }))
            .layer(axum::middleware::from_fn_with_state(
                Arc::new(vec!["secret".to_string()]),
                bearer_auth,
            ))
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
        let response = authenticated_test_router()
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
        let response = authenticated_test_router()
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
    async fn cors_preflight_bypasses_bearer_when_auth_is_enabled() {
        let response = authenticated_test_router()
            .oneshot(
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
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
