use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::{
    Router,
    extract::{Request, State},
    http::{Method, StatusCode, header},
    middleware::Next,
    response::Response,
    routing::get,
};
use tus_axum::TusState;
use tus_protocol::{HookExecutor, Locker, ProtocolHandle, StateStore, Storage};

#[derive(Clone, Debug)]
pub(crate) struct AppSettings {
    pub(crate) auth_token: Vec<String>,
    pub(crate) max_request_body_bytes: usize,
    pub(crate) request_body_read_timeout: u64,
}

pub(crate) fn build_app<S, I, L, H>(
    protocol: ProtocolHandle<S, I, L, H>,
    settings: &AppSettings,
    draining: Arc<AtomicBool>,
) -> Router
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let state = TusState::new(protocol);
    let mut tus_router = tus_axum::create_router(state);
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
        app = app.layer(tower_http::timeout::RequestBodyTimeoutLayer::new(timeout));
    }

    app
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

async fn bearer_auth(
    State(tokens): State<Arc<Vec<String>>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let is_cors_preflight = req.method() == Method::OPTIONS
        && req.headers().contains_key(header::ORIGIN)
        && req
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD);

    // Browser CORS preflights are sent without credentials, but plain
    // OPTIONS requests should still authenticate like other TUS routes.
    if is_cors_preflight {
        return Ok(next.run(req).await);
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
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
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
