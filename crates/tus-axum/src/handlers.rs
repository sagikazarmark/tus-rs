//! Axum handlers for TUS protocol endpoints.
//!
//! Each handler is a thin wrapper over the framework-neutral [`tus_protocol::Protocol`].
//! The adapter extracts axum-typed inputs (state, path, headers, body) and converts
//! protocol [`tus_protocol::Response`] values back into axum responses.

mod get;
mod method_override;

use axum::extract::State;

use tus_protocol::{HookExecutor, Locker, StateStore, Storage};

use crate::error::Error;
use crate::extractors::{TusBody, TusHeaders, TusUploadId};
use crate::response::TusResponse;
use crate::state::TusProtocol;

pub(crate) use get::handle_get;
pub(crate) use method_override::handle_post_with_override;

/// Handles OPTIONS requests.
pub(crate) async fn handle_options<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
) -> TusResponse
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    TusResponse(protocol.handle().options())
}

/// Handles POST requests to create new uploads.
pub(crate) async fn handle_post<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    TusHeaders(headers): TusHeaders,
    body: TusBody,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    Ok(protocol
        .handle()
        .post(headers, body.into_body())
        .await?
        .into())
}

/// Handles HEAD requests. The `TusHeaders` extractor validates the
/// `Tus-Resumable` header; its value is otherwise unused here.
pub(crate) async fn handle_head<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    TusHeaders(_): TusHeaders,
    TusUploadId(upload_id): TusUploadId,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    Ok(protocol.handle().head(&upload_id).await?.into())
}

/// Handles PATCH requests to upload data.
pub(crate) async fn handle_patch<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    TusHeaders(headers): TusHeaders,
    TusUploadId(upload_id): TusUploadId,
    body: TusBody,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let response = protocol
        .handle()
        .patch(headers, &upload_id, body.into_body())
        .await?;

    Ok(response.into())
}

/// Handles DELETE requests to terminate uploads.
pub(crate) async fn handle_delete<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    TusHeaders(headers): TusHeaders,
    TusUploadId(upload_id): TusUploadId,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    Ok(protocol.handle().delete(headers, &upload_id).await?.into())
}
