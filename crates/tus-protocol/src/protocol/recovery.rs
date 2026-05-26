//! Cross-restart reconciliation between the state store and the storage backend.
//!
//! [`Protocol::head`](super::Protocol::head) and
//! [`Protocol::patch`](super::Protocol::patch) call reconciliation before
//! exposing or validating upload offsets.

use crate::error::Error;
use crate::hooks::{HookExecutor, HookRequestInfo};
use crate::lifecycle::{load_final_upload_status, run_pre_finish};
use crate::state::{StateStore, UploadState};
use crate::storage::Storage;

/// Reconciles `state.offset()` with the actual bytes persisted in storage.
///
/// Called before serving `HEAD` (and similar offset-revealing operations) so
/// the client always sees the authoritative offset even after a server crash
/// that left the recorded state behind the storage tail. For final
/// concatenated uploads, also reconciles `state.length()` and triggers the
/// concat operation when all parts are complete.
///
/// Persists the corrected state via `state_store.set(state, false)` only when
/// it actually changed.
///
pub(super) async fn reconcile_state_offset<S, I, H>(
    storage: &S,
    state_store: &I,
    hooks: &H,
    request_info: &HookRequestInfo,
    state: &mut UploadState,
) -> Result<(), Error>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    H: HookExecutor + ?Sized,
{
    if state.is_final() {
        return reconcile_final_offset(storage, state_store, hooks, request_info, state).await;
    }

    reconcile_storage_offset(storage, state_store, state).await
}

async fn reconcile_final_offset<S, I, H>(
    storage: &S,
    state_store: &I,
    hooks: &H,
    request_info: &HookRequestInfo,
    state: &mut UploadState,
) -> Result<(), Error>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    H: HookExecutor + ?Sized,
{
    if state.is_complete()
        && let Some(length) = state.length()
    {
        let actual_size = storage
            .size(state)
            .await
            .map_err(|err| Error::Internal(err.to_string()))?
            .unwrap_or(0);
        if actual_size == length {
            return Ok(());
        }
    }

    let Some(final_plan) = load_final_upload_status(state_store, state).await? else {
        return reconcile_storage_offset(storage, state_store, state).await;
    };

    let mut changed = false;

    if state.length() != final_plan.status.total_length
        && let Some(total_length) = final_plan.status.total_length
    {
        state.set_length(total_length);
        changed = true;
    }

    let actual_size = if final_plan.status.ready_to_materialize() {
        Some(
            storage
                .size(state)
                .await
                .map_err(|err| Error::Internal(err.to_string()))?
                .unwrap_or(0),
        )
    } else {
        None
    };
    let needs_materialization = actual_size
        .zip(final_plan.status.total_length)
        .is_some_and(|(actual_size, total_length)| actual_size != total_length);

    if final_plan.status.ready_to_materialize() && (!state.is_complete() || needs_materialization) {
        let mut completed_state = state.clone();
        let total_length = final_plan
            .status
            .total_length
            .expect("ready final upload has total length");
        completed_state.set_length(total_length);
        completed_state.set_offset(total_length);
        run_pre_finish(hooks, request_info, completed_state).await?;
    }

    if needs_materialization {
        storage.concat(state, final_plan.parts).await?;
        changed = true;
    }

    let expected_offset = final_plan.status.expected_offset();
    if state.offset() != expected_offset {
        state.set_offset(expected_offset);
        changed = true;
    }

    if changed {
        state_store
            .set(state, false)
            .await
            .map_err(|err| Error::Internal(err.to_string()))?;
    }

    Ok(())
}

async fn reconcile_storage_offset<S, I>(
    storage: &S,
    state_store: &I,
    state: &mut UploadState,
) -> Result<(), Error>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
{
    let actual_offset = storage
        .size(state)
        .await
        .map_err(|err| Error::Internal(err.to_string()))?
        .unwrap_or(0);

    if actual_offset == state.offset() {
        return Ok(());
    }

    if let Some(length) = state.length()
        && actual_offset > length
    {
        return Err(Error::Internal(format!(
            "storage size {actual_offset} exceeds declared length {length} for upload {}",
            state.id()
        )));
    }

    tracing::warn!(
        upload_id = %state.id(),
        recorded_offset = state.offset(),
        actual_offset,
        "reconciling upload offset against stored bytes"
    );

    state.set_offset(actual_offset);
    state_store
        .set(state, false)
        .await
        .map_err(|err| Error::Internal(err.to_string()))?;
    Ok(())
}
