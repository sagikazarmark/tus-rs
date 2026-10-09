use crate::error::Result;
use crate::hooks::{
    HookContext, HookEvent, HookExecutor, HookRequestInfo, execute_post_best_effort,
};
use crate::state::{StateStore, UploadState, WriteMode};

use super::PreHookGate;

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

    /// Runs the PreFinish hook gate before completion is reported.
    ///
    /// For receive paths the completing bytes are already durable when this
    /// runs; a rejection fails the response and skips `PostFinish`. For
    /// concatenation paths this runs before the final record is materialized,
    /// so a rejection prevents the final upload from being created.
    pub(crate) async fn finish_gate(&self, state: UploadState) -> Result<()> {
        PreHookGate::Finish
            .run(self.hooks, self.request_info, state)
            .await?;
        Ok(())
    }

    /// Commits successful finish validation separately from accepted bytes.
    ///
    /// Keep the persisted pending flag until both the gate and state write
    /// succeed, so cancellation or rejection cannot bypass validation later.
    pub(crate) async fn commit_finish<St>(
        &self,
        state_store: &St,
        state: &mut UploadState,
    ) -> Result<()>
    where
        St: StateStore + ?Sized,
    {
        self.finish_gate(state.clone()).await?;
        let mut finished = state.clone();
        finished.set_finish_pending(false);
        state_store.set(&finished, WriteMode::Update).await?;
        *state = finished;
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::hooks::{HookChain, PreHookResult};

    #[tokio::test]
    async fn completion_finish_gate_returns_hook_rejection() {
        let hooks = HookChain::new()
            .on_pre_finish(|_| async { Ok(PreHookResult::reject(403, "finish blocked")) });
        let request_info = HookRequestInfo::default();
        let state = UploadState::new("upload-1").with_length(5);

        let err = UploadCompletion::new(&hooks, &request_info)
            .finish_gate(state)
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
    async fn completion_finish_gate_defaults_rejection_response() {
        // A bare default is a rejection (`proceed` is false) that specifies no
        // status or message, exercising the default 400/empty response.
        let hooks = HookChain::new().on_pre_finish(|_| async { Ok(PreHookResult::default()) });
        let request_info = HookRequestInfo::default();
        let state = UploadState::new("upload-1").with_length(5);

        let err = UploadCompletion::new(&hooks, &request_info)
            .finish_gate(state)
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

    #[cfg(feature = "state-memory")]
    #[tokio::test]
    async fn failed_finish_commit_preserves_pending_state_for_retry() {
        use crate::state::memory::MemoryStateStore;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        struct FailingStore {
            inner: MemoryStateStore,
            fail: AtomicBool,
        }

        #[async_trait::async_trait]
        impl StateStore for FailingStore {
            fn name(&self) -> &'static str {
                "failing"
            }
            async fn set(&self, state: &UploadState, mode: WriteMode) -> Result<()> {
                if !state.is_finish_pending() && self.fail.load(Ordering::SeqCst) {
                    return Err(Error::Internal("finish commit failed".into()));
                }
                self.inner.set(state, mode).await
            }
            async fn get(&self, id: &str) -> Result<Option<UploadState>> {
                self.inner.get(id).await
            }
            async fn delete(&self, id: &str) -> Result<()> {
                self.inner.delete(id).await
            }
            async fn list_expired(
                &self,
                before: chrono::DateTime<chrono::Utc>,
            ) -> Result<Vec<String>> {
                self.inner.list_expired(before).await
            }
        }

        let store = FailingStore {
            inner: MemoryStateStore::new(),
            fail: AtomicBool::new(true),
        };
        let mut state = UploadState::new("upload-1").with_length(5);
        state.set_offset(5);
        state.set_finish_pending(true);
        store.set(&state, WriteMode::CreateNew).await.unwrap();
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let hooks = HookChain::new().on_pre_finish({
            let calls = calls.clone();
            move |_| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(PreHookResult::proceed())
                }
            }
        });
        let request_info = HookRequestInfo::default();
        let completion = UploadCompletion::new(&hooks, &request_info);
        assert!(completion.commit_finish(&store, &mut state).await.is_err());
        assert!(state.is_finish_pending());
        assert!(
            store
                .get(state.id())
                .await
                .unwrap()
                .unwrap()
                .is_finish_pending()
        );

        store.fail.store(false, Ordering::SeqCst);
        completion.commit_finish(&store, &mut state).await.unwrap();
        assert!(!state.is_finish_pending());
        assert!(
            !store
                .get(state.id())
                .await
                .unwrap()
                .unwrap()
                .is_finish_pending()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
