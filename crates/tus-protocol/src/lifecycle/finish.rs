use crate::error::{Error, Result};
use crate::hooks::{
    HookContext, HookEvent, HookExecutor, HookRequestInfo, execute_post_best_effort,
};
use crate::state::UploadState;

/// Owns upload completion hook timing around durable completion commits.
pub(crate) struct UploadCompletion<'a, H>
where
    H: HookExecutor + ?Sized,
{
    hooks: &'a H,
    request_info: &'a HookRequestInfo,
}

impl<'a, H> UploadCompletion<'a, H>
where
    H: HookExecutor + ?Sized,
{
    pub(crate) fn new(hooks: &'a H, request_info: &'a HookRequestInfo) -> Self {
        Self {
            hooks,
            request_info,
        }
    }

    /// Runs the PreFinish hook gate before completion is durable.
    pub(crate) async fn before_commit(&self, state: UploadState) -> Result<()> {
        let pre_finish_ctx =
            HookContext::new(HookEvent::PreFinish, state, self.request_info.clone());
        let pre_finish_result = self.hooks.execute_pre(&pre_finish_ctx).await?;

        if !pre_finish_result.proceed {
            return Err(Error::HookRejected {
                status_code: pre_finish_result.reject_status.unwrap_or(400),
                message: pre_finish_result.reject_message.unwrap_or_default(),
            });
        }

        Ok(())
    }

    /// Runs the PostFinish hook after completion is observable.
    pub(crate) async fn after_commit(&self, state: &UploadState) {
        let post_finish_ctx = HookContext::new(
            HookEvent::PostFinish,
            state.clone(),
            self.request_info.clone(),
        );
        execute_post_best_effort(self.hooks, &post_finish_ctx).await;
    }

    /// Runs the PostFinish hook after commit when the upload is complete.
    pub(crate) async fn after_commit_if_complete(&self, state: &UploadState) {
        if state.is_complete() {
            self.after_commit(state).await;
        }
    }
}

#[cfg(all(test, not(feature = "local-futures")))]
mod tests {
    use super::*;
    use crate::hooks::{HookChain, PreHookResult};

    #[tokio::test]
    async fn completion_before_commit_returns_hook_rejection() {
        let hooks = HookChain::new()
            .on_pre_finish(|_| async { Ok(PreHookResult::reject(403, "finish blocked")) });
        let request_info = HookRequestInfo::default();
        let state = UploadState::new("upload-1").with_length(5);

        let err = UploadCompletion::new(&hooks, &request_info)
            .before_commit(state)
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::HookRejected {
                status_code: 403,
                message
            } if message == "finish blocked"
        ));
    }

    #[tokio::test]
    async fn completion_before_commit_defaults_rejection_response() {
        let hooks = HookChain::new().on_pre_finish(|_| async {
            Ok(PreHookResult {
                proceed: false,
                ..Default::default()
            })
        });
        let request_info = HookRequestInfo::default();
        let state = UploadState::new("upload-1").with_length(5);

        let err = UploadCompletion::new(&hooks, &request_info)
            .before_commit(state)
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::HookRejected {
                status_code: 400,
                message
            } if message.is_empty()
        ));
    }
}
