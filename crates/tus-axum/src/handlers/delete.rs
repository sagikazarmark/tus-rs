//! Axum adapter for the DELETE handler.

use axum::extract::State;

use tus_protocol::{HookExecutor, Locker, StateStore, Storage};

use crate::error::Error;
use crate::extractors::{Headers, UploadId};
use crate::response::TusResponse;
use crate::state::TusProtocol;

/// Handles DELETE requests to terminate uploads.
pub async fn handle_delete<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    Headers(headers): Headers,
    UploadId(upload_id): UploadId,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    Ok(protocol.delete(&headers, &upload_id).await?.into())
}
