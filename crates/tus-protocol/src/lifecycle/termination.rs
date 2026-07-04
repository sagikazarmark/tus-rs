use std::collections::HashMap;

use crate::error::Result;
use crate::hooks::{
    HookContext, HookEvent, HookExecutor, HookRequestInfo, execute_post_best_effort,
};
use crate::state::{StateStore, UploadState};
use crate::storage::Storage;

use super::PreHookGate;

pub(crate) struct UploadTerminator<'a, S, I, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    H: HookExecutor + ?Sized,
{
    storage: &'a S,
    state_store: &'a I,
    hooks: &'a H,
    request_info: &'a HookRequestInfo,
}

impl<'a, S, I, H> UploadTerminator<'a, S, I, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    H: HookExecutor + ?Sized,
{
    pub(crate) fn new(
        storage: &'a S,
        state_store: &'a I,
        hooks: &'a H,
        request_info: &'a HookRequestInfo,
    ) -> Self {
        Self {
            storage,
            state_store,
            hooks,
            request_info,
        }
    }

    pub(crate) async fn terminate(&self, state: UploadState) -> Result<TerminationOutcome> {
        let response_headers = self.run_pre_terminate(&state).await?;

        // Storage deletion failures must propagate: state is only deleted
        // after the bytes are gone, so the client can retry the DELETE.
        // Backends treat missing objects as success, keeping the retry
        // idempotent.
        if let Some(handle) = state.storage_handle() {
            self.storage.delete(&handle).await?;
        }

        self.state_store.delete(state.id()).await?;
        self.run_post_terminate(state).await;

        Ok(TerminationOutcome { response_headers })
    }

    async fn run_pre_terminate(&self, state: &UploadState) -> Result<HashMap<String, String>> {
        let pre_terminate = PreHookGate::Terminate
            .run(self.hooks, self.request_info, state.clone())
            .await?;

        Ok(pre_terminate.response_headers)
    }

    async fn run_post_terminate(&self, state: UploadState) {
        let post_ctx = HookContext::new(HookEvent::PostTerminate, state, self.request_info.clone());
        execute_post_best_effort(self.hooks, &post_ctx).await;
    }
}

pub(crate) struct TerminationOutcome {
    pub(crate) response_headers: HashMap<String, String>,
}
