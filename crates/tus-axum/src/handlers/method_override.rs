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
    extract::{FromRequest, Request, State},
    http::{HeaderValue, Method, header},
    response::{IntoResponse, Response},
};

use tus_protocol::{HookExecutor, Locker, StateStore, Storage};

use crate::error::Error;
use crate::extractors::{TusBody, TusHeaders, TusUploadId};
use crate::handlers::{handle_delete, handle_patch};
use crate::router::UPLOAD_ALLOW;
use crate::state::TusProtocol;

/// POST handler at `/<base>/:upload_id` that honors `X-HTTP-Method-Override`.
///
/// Behavior:
/// - `X-HTTP-Method-Override: PATCH` → dispatch to the PATCH handler.
/// - `X-HTTP-Method-Override: DELETE` → dispatch to the DELETE handler.
/// - Missing or unrecognized value → 405 Method Not Allowed with an `Allow`
///   header listing the methods the upload resource accepts.
///
/// The override value is matched case-insensitively (`patch` and `PATCH` are
/// equivalent), as HTTP method names from constrained clients are not
/// reliably uppercased.
pub(crate) async fn handle_post_with_override<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    // Validate the upload id through the shared `TusUploadId` extractor rather
    // than a raw `Path<String>`. A malformed id (e.g. an un-decodable
    // percent-escape like `%FF`) then yields the same tus-compliant 400 +
    // `Tus-Resumable` response as the direct PATCH/DELETE/HEAD routes, instead
    // of axum's default plain-text 400 with no `Tus-Resumable` header.
    upload_id: TusUploadId,
    req: Request,
) -> Result<Response, Error>
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
        // Method parsing is case-sensitive ("patch" parses as an extension
        // method distinct from `Method::PATCH`), so normalize first.
        .and_then(|s| s.to_ascii_uppercase().parse::<Method>().ok());

    match override_method {
        Some(m) if m == Method::PATCH => {
            let headers = TusHeaders::from_header_map(req.headers())?;
            let body = TusBody::from_request(req, &()).await?;

            handle_patch(State(protocol), headers, upload_id, body)
                .await
                .map(IntoResponse::into_response)
        }
        Some(m) if m == Method::DELETE => {
            let headers = TusHeaders::from_header_map(req.headers())?;

            handle_delete(State(protocol), headers, upload_id)
                .await
                .map(IntoResponse::into_response)
        }
        _ => {
            // RFC 9110 requires 405 responses to carry an `Allow` header.
            // The protocol error mapping does not know the route table, so
            // the header is attached here on the axum side.
            let mut response = Error::from(tus_protocol::Error::MethodNotAllowed(
                "POST is not allowed on upload resources without X-HTTP-Method-Override"
                    .to_string(),
            ))
            .into_response();
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static(UPLOAD_ALLOW));
            Ok(response)
        }
    }
}
