//! Axum adapter for the HEAD handler.

use axum::extract::State;

use tus_protocol::{HookExecutor, Locker, StateStore, Storage};

use crate::error::Error;
use crate::extractors::{Headers, UploadId};
use crate::response::TusResponse;
use crate::state::TusProtocol;

/// Handles HEAD requests. The `Headers` extractor validates the
/// `Tus-Resumable` header; its value is otherwise unused here.
pub async fn handle_head<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    Headers(_): Headers,
    UploadId(upload_id): UploadId,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    Ok(protocol.head(&upload_id).await?.into())
}
