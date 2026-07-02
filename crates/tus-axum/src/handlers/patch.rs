//! Axum adapter for the PATCH handler.

use axum::extract::State;

use tus_protocol::{HookExecutor, Locker, StateStore, Storage};

use crate::error::Error;
use crate::extractors::{Headers, TusBody, UploadId};
use crate::response::TusResponse;
use crate::state::TusProtocol;

/// Handles PATCH requests to upload data.
pub async fn handle_patch<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    Headers(headers): Headers,
    UploadId(upload_id): UploadId,
    body: TusBody,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let response = protocol
        .patch(headers, &upload_id, body.into_body())
        .await?;

    Ok(response.into())
}
