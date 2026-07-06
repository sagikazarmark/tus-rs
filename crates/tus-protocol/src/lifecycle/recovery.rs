use crate::error::{Error, Result};
use crate::state::{StateStore, UploadState, WriteMode};
use crate::storage::Storage;

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

pub(crate) async fn reconcile_stored_completion<S, St>(
    storage: &S,
    state_store: &St,
    state: &mut UploadState,
) -> Result<bool>
where
    S: Storage + ?Sized,
    St: StateStore + ?Sized,
{
    if state.is_complete() || state.is_partial() {
        return Ok(false);
    }

    let Some(length) = state.length() else {
        return Ok(false);
    };
    let Some(handle) = state.storage_handle() else {
        return Ok(false);
    };

    let actual_offset = storage.size(&handle).await?.unwrap_or(0);
    if actual_offset > length {
        return Err(Error::Internal(format!(
            "storage size {actual_offset} exceeds declared length {length} for upload {}",
            state.id()
        )));
    }
    if actual_offset != length {
        return Ok(false);
    }

    tracing::warn!(
        upload_id = %state.id(),
        recorded_offset = state.offset(),
        actual_offset,
        "recovering completed upload offset against stored bytes"
    );

    state.set_offset(actual_offset);
    state_store.set(state, WriteMode::Update).await?;
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
