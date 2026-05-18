//! Axum adapter for the `X-HTTP-Method-Override` fallback.
//!
//! The tus spec allows clients that can only speak POST (e.g. browsers in
//! constrained environments, or proxies that block PATCH/DELETE) to send a
//! POST carrying `X-HTTP-Method-Override: PATCH|DELETE` and have the server
//! treat it as the advertised method.
//!
//! Implemented as a POST handler on the item-level route rather than routing
//! middleware: axum's `Router::layer` wraps individual endpoint handlers
//! (including the 405 default), so rewriting `req.method()` from middleware
//! does not cause the router to re-dispatch.

use axum::{
    extract::{FromRequest, Path, Request, State},
    http::Method,
};

use tus_protocol::{HookExecutor, Locker, StateStore, Storage};

use crate::error::Error;
use crate::extractors::{Headers, TusBody, UploadId};
use crate::handlers::{handle_delete, handle_patch};
use crate::response::TusResponse;
use crate::state::TusProtocol;

/// POST handler at `/<base>/:upload_id` that honors `X-HTTP-Method-Override`.
///
/// Behavior:
/// - `X-HTTP-Method-Override: PATCH` → dispatch to the PATCH handler.
/// - `X-HTTP-Method-Override: DELETE` → dispatch to the DELETE handler.
/// - Missing or unrecognized value → 405 Method Not Allowed.
pub async fn handle_post_with_override<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    Path(upload_id): Path<String>,
    req: Request,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let override_method = req
        .headers()
        .get("x-http-method-override")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<Method>().ok());

    match override_method {
        Some(m) if m == Method::PATCH => {
            let headers = parse_headers(req.headers())?;
            let body = TusBody::from_request(req, &()).await?;
            let upload_id = tus_protocol::UploadId::try_from(upload_id)
                .map(UploadId)
                .map_err(Error)?;

            handle_patch(State(protocol), headers, upload_id, body).await
        }
        Some(m) if m == Method::DELETE => {
            let headers = parse_headers(req.headers())?;
            let upload_id = tus_protocol::UploadId::try_from(upload_id)
                .map(UploadId)
                .map_err(Error)?;

            handle_delete(State(protocol), headers, upload_id).await
        }
        _ => Err(Error(tus_protocol::Error::MethodNotAllowed(
            "POST is not allowed on upload resources without X-HTTP-Method-Override".to_string(),
        ))),
    }
}

fn parse_headers(headers: &axum::http::HeaderMap) -> Result<Headers, Error> {
    tus_protocol::Headers::from_headers(headers)
        .map(Headers)
        .map_err(Error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        extract::{Path, State},
        http::StatusCode,
        response::IntoResponse,
    };
    use std::sync::Arc;
    use tus_protocol::locking::memory::MemoryLocker;
    use tus_protocol::state::memory::MemoryStateStore;
    use tus_protocol::storage::memory::MemoryStorage;
    use tus_protocol::{Config, NoopHookExecutor, ProtocolHandle, StateStore, UploadState};

    #[tokio::test]
    async fn missing_override_returns_405_without_validating_tus_headers() {
        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::default(),
            MemoryStorage::new(),
            MemoryStateStore::new(),
            MemoryLocker::new(),
            NoopHookExecutor::new(),
        ));

        let response = handle_post_with_override(
            State(protocol),
            Path("upload-1".to_string()),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn unsupported_override_returns_405_without_validating_tus_headers() {
        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::default(),
            MemoryStorage::new(),
            MemoryStateStore::new(),
            MemoryLocker::new(),
            NoopHookExecutor::new(),
        ));

        let response = handle_post_with_override(
            State(protocol),
            Path("upload-1".to_string()),
            Request::builder()
                .header("x-http-method-override", "GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn delete_override_dispatches_to_delete_handler() {
        let state_store = Arc::new(MemoryStateStore::new());
        let upload = UploadState::new("upload-1").with_length(100);
        state_store.set(&upload, true).await.unwrap();
        let protocol = TusProtocol::new(ProtocolHandle::from_arcs(
            Arc::new(Config::with_all_extensions()),
            Arc::new(MemoryStorage::new()),
            state_store.clone(),
            Arc::new(MemoryLocker::new()),
            Arc::new(NoopHookExecutor::new()),
        ));

        let response = handle_post_with_override(
            State(protocol),
            Path("upload-1".to_string()),
            Request::builder()
                .method("POST")
                .header("x-http-method-override", "DELETE")
                .header("tus-resumable", "1.0.0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(state_store.get("upload-1").await.unwrap().is_none());
    }
}
