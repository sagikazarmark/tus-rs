use crate::error::{Error, Result};
use crate::hooks::{HookExecutor, HookRequestInfo};
use crate::state::{StateStore, UploadState, WriteMode};
use crate::storage::Storage;

use super::UploadCompletion;

pub(crate) async fn reconcile_state_offset<S, St>(
    storage: &S,
    state_store: &St,
    state: &mut UploadState,
) -> Result<()>
where
    S: Storage + ?Sized,
    St: StateStore + ?Sized,
{
    reconcile_storage_offset(storage, state_store, state).await
}

/// Detects whether stored bytes have reached the declared length for an upload
/// whose recorded state does not yet mark it complete.
///
/// Returns `Some(length)` when the backing object's size equals the declared
/// `Upload-Length`, a crash-recovery completion is pending, and `None` when
/// there is nothing to recover. This is the single decision point for "are the
/// stored bytes a completed upload"; callers layer persistence and finish hooks
/// on top of it.
async fn detect_stored_completion<S>(storage: &S, state: &UploadState) -> Result<Option<u64>>
where
    S: Storage + ?Sized,
{
    if state.is_complete() || state.is_partial() {
        return Ok(None);
    }

    let Some(length) = state.length() else {
        return Ok(None);
    };
    let Some(handle) = state.storage_handle() else {
        return Ok(None);
    };

    let actual_offset = storage.size(&handle).await?.unwrap_or(0);
    if actual_offset > length {
        return Err(Error::Internal(format!(
            "storage size {actual_offset} exceeds declared length {length} for upload {}",
            state.id()
        )));
    }
    if actual_offset != length {
        return Ok(None);
    }

    Ok(Some(length))
}

/// Reports whether stored bytes have already completed an upload, without
/// persisting the completion or firing finish hooks.
///
/// Used by the hookless reclamation scan, which only needs to know whether an
/// expired candidate is in fact completed content (and therefore not
/// reclaimable). Persisting completion here would mark the upload finished
/// without ever running its `PreFinish`/`PostFinish` hooks; leaving it
/// unpersisted defers completion to the next client request, which runs the
/// finish gate. See [`reconcile_stored_completion`].
pub(crate) async fn stored_bytes_complete_upload<S>(
    storage: &S,
    state: &UploadState,
) -> Result<bool>
where
    S: Storage + ?Sized,
{
    Ok(detect_stored_completion(storage, state).await?.is_some())
}

/// Completes an upload whose bytes reached the declared length before the state
/// write landed, a crash between the durable storage append and the state
/// update, running the same finish gate as the normal completion path.
///
/// When stored bytes have reached the length, this runs the `PreFinish` gate
/// against the would-be-completed state (a rejection fails the request and
/// leaves the upload untouched), then persists the completed offset and runs
/// the best-effort `PostFinish` hook. Returns whether a completion was
/// recovered.
pub(crate) async fn reconcile_stored_completion<S, St, H>(
    storage: &S,
    state_store: &St,
    hooks: &H,
    request_info: &HookRequestInfo,
    state: &mut UploadState,
) -> Result<bool>
where
    S: Storage + ?Sized,
    St: StateStore + ?Sized,
    H: HookExecutor + ?Sized,
{
    let Some(length) = detect_stored_completion(storage, state).await? else {
        return Ok(false);
    };

    // Run the finish gate before completion is persisted so a `PreFinish`
    // rejection leaves the upload exactly as it was, mirroring the normal
    // receive completion path (`receive.rs`).
    let completion = UploadCompletion::new(hooks, request_info);
    let mut completed = state.clone();
    completed.set_offset(length);
    completion.finish_gate(completed).await?;

    tracing::warn!(
        upload_id = %state.id(),
        recorded_offset = state.offset(),
        actual_offset = length,
        "recovering completed upload offset against stored bytes"
    );

    state.set_offset(length);
    state_store.set(state, WriteMode::Update).await?;

    completion.after_commit(state).await;

    Ok(true)
}

async fn reconcile_storage_offset<S, St>(
    storage: &S,
    state_store: &St,
    state: &mut UploadState,
) -> Result<()>
where
    S: Storage + ?Sized,
    St: StateStore + ?Sized,
{
    let actual_offset = match state.storage_handle() {
        Some(handle) => storage.size(&handle).await?.unwrap_or(0),
        None => 0,
    };

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
    state_store.set(state, WriteMode::Update).await?;
    Ok(())
}
