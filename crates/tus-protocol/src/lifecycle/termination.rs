use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::hooks::{
    HookContext, HookEvent, HookExecutor, HookRequestInfo, execute_post_best_effort,
};
use crate::state::{StateStore, UploadState};
use crate::storage::Storage;

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

        if let Some(handle) = state.storage_handle()
            && let Err(err) = self.storage.delete(&handle).await
        {
            tracing::warn!(
                upload_id = %state.id(),
                error = %err,
                "failed to delete upload data from storage"
            );
        }

        self.state_store.delete(state.id()).await?;
        self.run_post_terminate(state).await;

        Ok(TerminationOutcome { response_headers })
    }

    async fn run_pre_terminate(&self, state: &UploadState) -> Result<HashMap<String, String>> {
        let pre_ctx = HookContext::new(
            HookEvent::PreTerminate,
            state.clone(),
            self.request_info.clone(),
        );
        let pre_result = self.hooks.execute_pre(&pre_ctx).await?;

        if !pre_result.proceed {
            return Err(Error::HookRejected {
                status_code: pre_result.reject_status.unwrap_or(400),
                message: pre_result.reject_message.unwrap_or_default(),
            });
        }

        Ok(pre_result.response_headers)
    }

    async fn run_post_terminate(&self, state: UploadState) {
        let post_ctx = HookContext::new(HookEvent::PostTerminate, state, self.request_info.clone());
        execute_post_best_effort(self.hooks, &post_ctx).await;
    }
}

pub(crate) struct TerminationOutcome {
    pub(crate) response_headers: HashMap<String, String>,
}
