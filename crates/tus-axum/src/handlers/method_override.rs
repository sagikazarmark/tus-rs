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
