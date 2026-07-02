use crate::error::{Error, Result};
use crate::hooks::{
    HookContext, HookEvent, HookExecutor, HookRequestInfo, execute_post_best_effort,
};
use crate::state::UploadState;

/// Runs the PreFinish hook gate for an upload that is about to become complete.
pub async fn run_pre_finish<H>(
    hooks: &H,
    request_info: &HookRequestInfo,
    state: UploadState,
) -> Result<()>
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

/// Runs the PostFinish hook after an upload completion commit is observable.
pub(crate) async fn run_post_finish_best_effort<H>(
    hooks: &H,
    request_info: &HookRequestInfo,
    state: &UploadState,
) where
    H: HookExecutor + ?Sized,
{
    let post_finish_ctx =
        HookContext::new(HookEvent::PostFinish, state.clone(), request_info.clone());
    execute_post_best_effort(hooks, &post_finish_ctx).await;
}

#[cfg(all(test, not(feature = "local-futures")))]
mod tests {
    use super::*;
    use crate::hooks::{HookChain, PreHookResult};

    #[tokio::test]
    async fn run_pre_finish_returns_hook_rejection() {
        let hooks = HookChain::new()
            .on_pre_finish(|_| async { Ok(PreHookResult::reject(403, "finish blocked")) });
        let request_info = HookRequestInfo::default();
        let state = UploadState::new("upload-1").with_length(5);

        let err = run_pre_finish(&hooks, &request_info, state)
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
    async fn run_pre_finish_defaults_rejection_response() {
        let hooks = HookChain::new().on_pre_finish(|_| async {
            Ok(PreHookResult {
                proceed: false,
                ..Default::default()
            })
        });
        let request_info = HookRequestInfo::default();
        let state = UploadState::new("upload-1").with_length(5);

        let err = run_pre_finish(&hooks, &request_info, state)
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
