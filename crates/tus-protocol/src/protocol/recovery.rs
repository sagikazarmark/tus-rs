//! Cross-restart reconciliation between the state store and the storage backend.
//!
//! [`Protocol::head`](super::Protocol::head) and
//! [`Protocol::patch`](super::Protocol::patch) call reconciliation before
//! exposing or validating upload offsets.

use crate::error::Error;
use crate::hooks::{HookContext, HookEvent, HookExecutor, HookRequestInfo};
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

    let Some(part_ids) = state.parts().map(|parts| parts.to_vec()) else {
        return reconcile_storage_offset(storage, state_store, state).await;
    };

    let mut parts = Vec::with_capacity(part_ids.len());
    let mut total_length = 0_u64;
    let mut current_offset = 0_u64;
    let mut all_complete = true;
    let mut length_known = true;

    for part_id in part_ids {
        let part_state = state_store
            .get(&part_id)
            .await
            .map_err(|err| Error::Internal(err.to_string()))?
            .ok_or_else(|| {
                Error::Internal(format!(
                    "final upload {} references missing partial {}",
                    state.id(),
                    part_id
                ))
            })?;

        if part_state.is_expired() {
            return Err(Error::Expired(part_id));
        }

        current_offset = current_offset.saturating_add(part_state.offset());

        match part_state.length() {
            Some(length) => total_length = total_length.saturating_add(length),
            None => {
                length_known = false;
                all_complete = false;
            }
        }

        if !part_state.is_complete() {
            all_complete = false;
        }

        parts.push(part_state);
    }

    let mut changed = false;

    if length_known && state.length() != Some(total_length) {
        state.set_length(total_length);
        changed = true;
    }

    let actual_size = if all_complete && length_known {
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
    let needs_materialization = actual_size.is_some_and(|actual_size| actual_size != total_length);

    if all_complete && length_known && (!state.is_complete() || needs_materialization) {
        let mut completed_state = state.clone();
        completed_state.set_length(total_length);
        completed_state.set_offset(total_length);
        execute_pre_finish(hooks, request_info, completed_state).await?;
    }

    if needs_materialization {
        storage.concat(state, parts).await?;
        changed = true;
    }

    let expected_offset = if all_complete && length_known {
        total_length
    } else {
        current_offset
    };

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

async fn execute_pre_finish<H>(
    hooks: &H,
    request_info: &HookRequestInfo,
    state: UploadState,
) -> Result<(), Error>
where
    H: HookExecutor + ?Sized,
{
    let pre_finish_ctx = HookContext::new(HookEvent::PreFinish, state, request_info.clone());
    let pre_finish_result = hooks.execute_pre(&pre_finish_ctx).await?;

    if !pre_finish_result.proceed {
        return Err(Error::HookRejected {
            status_code: pre_finish_result.reject_status.unwrap_or(400),
            message: pre_finish_result.reject_message.unwrap_or_default(),
        });
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
