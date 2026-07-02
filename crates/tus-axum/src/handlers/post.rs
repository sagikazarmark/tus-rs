//! Axum adapter for the POST handler.

use axum::extract::State;

use tus_protocol::{HookExecutor, Locker, StateStore, Storage};

use crate::error::Error;
use crate::extractors::{Headers, TusBody};
use crate::response::TusResponse;
use crate::state::TusProtocol;

/// Handles POST requests to create new uploads.
pub async fn handle_post<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    Headers(headers): Headers,
    body: TusBody,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    Ok(protocol.post(headers, body.into_body()).await?.into())
}
