use crate::error::{Error, Result};
use crate::hooks::{HookExecutor, HookRequestInfo};
use crate::state::{StateStore, UploadState, WriteMode};
use crate::storage::Storage;

use super::{UploadCompletion, ensure_active};

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
/// whose recorded state has not committed successful finish validation.
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
    if state.is_complete() && !state.is_finish_pending() {
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
    // Partials remain temporary concatenation resources even after all bytes
    // arrive, and must still be reclaimable under their expiration policy.
    if state.is_partial() {
        return Ok(false);
    }
    Ok(detect_stored_completion(storage, state).await?.is_some())
}

/// Completes an upload after a crash between append and state persistence, or
/// retries finish validation left pending by rejection or cancellation.
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
    // Unlike regular completed content, partials must honor expiration before
    // recovering a finish gate, including a crash before the offset write.
    if state.is_partial() {
        ensure_active(state)?;
    }
    let Some(length) = detect_stored_completion(storage, state).await? else {
        return Ok(false);
    };

    // Preserve the persisted state until finish validation is committed. For
    // old incomplete records, storage remains sufficient evidence to retry;
    // new completing receives have already persisted the pending flag.
    let completion = UploadCompletion::new(hooks, request_info);
    let mut completed = state.clone();
    completed.set_offset(length);
    completed.set_finish_pending(true);
    completion
        .commit_finish(state_store, &mut completed)
        .await?;

    tracing::warn!(
        upload_id = %state.id(),
        recorded_offset = state.offset(),
        actual_offset = length,
        "recovering completed upload offset against stored bytes"
    );

    *state = completed;

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
    if !state.is_complete() {
        state.set_finish_pending(false);
    }
    state_store.set(state, WriteMode::Update).await?;
    Ok(())
}
