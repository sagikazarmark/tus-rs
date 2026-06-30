use crate::error::{Error, Result};
use crate::hooks::{HookExecutor, HookRequestInfo};
use crate::state::{StateStore, UploadState};
use crate::storage::Storage;

use super::repair_final_upload;

pub(crate) async fn reconcile_state_offset<S, I, H>(
    storage: &S,
    state_store: &I,
    hooks: &H,
    request_info: &HookRequestInfo,
    state: &mut UploadState,
) -> Result<()>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    H: HookExecutor + ?Sized,
{
    if state.is_final()
        && repair_final_upload(storage, state_store, hooks, request_info, state).await?
    {
        return Ok(());
    }

    reconcile_storage_offset(storage, state_store, state).await
}

async fn reconcile_storage_offset<S, I>(
    storage: &S,
    state_store: &I,
    state: &mut UploadState,
) -> Result<()>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
{
    let actual_offset = match state.storage_handle() {
        Some(handle) => storage
            .size(&handle)
            .await
            .map_err(|err| Error::Internal(err.to_string()))?
            .unwrap_or(0),
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
    state_store
        .set(state, false)
        .await
        .map_err(|err| Error::Internal(err.to_string()))?;
    Ok(())
}
