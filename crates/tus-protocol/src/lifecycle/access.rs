use crate::config::Config;
use crate::error::{Error, Result};
use crate::hooks::{HookExecutor, HookRequestInfo};
use crate::state::{StateStore, UploadState};
use crate::storage::Storage;

use super::{
    FinalUploadMaterializer, ensure_active, reconcile_state_offset, reconcile_stored_completion,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PreparedUploadAccess {
    pub(crate) facts: UploadAccessFacts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UploadAccessFacts {
    pub(crate) offset: Option<u64>,
    pub(crate) length: Option<u64>,
    pub(crate) defer_length: bool,
}

pub(crate) async fn prepare_upload_mutation_access<S, I>(
    storage: &S,
    state_store: &I,
    state: &mut UploadState,
) -> Result<()>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
{
    if state.is_final() {
        ensure_active(state)?;
        return Err(Error::FinalUploadModificationForbidden(
            state.id().to_string(),
        ));
    }

    reconcile_stored_completion(storage, state_store, state).await?;
    ensure_active(state)?;
    reconcile_state_offset(storage, state_store, state).await?;

    Ok(())
}

pub(crate) async fn prepare_upload_access<S, I, H>(
    storage: &S,
    state_store: &I,
    hooks: &H,
    config: &Config,
    request_info: &HookRequestInfo,
    state: &mut UploadState,
) -> Result<PreparedUploadAccess>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    H: HookExecutor + ?Sized,
{
    if state.is_final() {
        let materializer =
            FinalUploadMaterializer::new(storage, state_store, hooks, config, request_info);
        let prepared = materializer
            .prepare_read(state)
            .await?
            .expect("final upload preparation should return final upload facts");
        ensure_active(state)?;

        return Ok(PreparedUploadAccess {
            facts: UploadAccessFacts {
                offset: prepared.response_facts.offset,
                length: prepared.response_facts.length,
                defer_length: false,
            },
        });
    }

    reconcile_stored_completion(storage, state_store, state).await?;
    ensure_active(state)?;
    reconcile_state_offset(storage, state_store, state).await?;

    Ok(PreparedUploadAccess {
        facts: UploadAccessFacts {
            offset: Some(state.offset()),
            length: state.length(),
            defer_length: state.length().is_none(),
        },
    })
}
