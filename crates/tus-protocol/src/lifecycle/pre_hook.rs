use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::hooks::{HookContext, HookEvent, HookExecutor, HookRequestInfo, PreHookResult};
use crate::state::UploadState;

/// Internal lifecycle gate for event-specific pre-hook semantics.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PreHookGate {
    Create,
    Receive,
    Finish,
    Terminate,
}

impl PreHookGate {
    pub(crate) async fn run<H>(
        self,
        hooks: &H,
        request_info: &HookRequestInfo,
        mut state: UploadState,
    ) -> Result<PreHookOutcome>
    where
        H: HookExecutor + ?Sized,
    {
        let ctx = HookContext::new(self.event(), state.clone(), request_info.clone());
        let result = hooks.execute_pre(&ctx).await?;

        if !result.proceeds() {
            return Err(rejection_error(&result));
        }

        let effects = self.effects();
        if effects.replace_metadata
            && let Some(metadata) = result.metadata()
        {
            state.set_metadata(metadata.clone());
        }

        let response_headers = if effects.propagate_response_headers {
            result.response_headers().clone()
        } else {
            HashMap::new()
        };

        Ok(PreHookOutcome {
            state,
            response_headers,
        })
    }

    fn event(self) -> HookEvent {
        match self {
            PreHookGate::Create => HookEvent::PreCreate,
            PreHookGate::Receive => HookEvent::PreReceive,
            PreHookGate::Finish => HookEvent::PreFinish,
            PreHookGate::Terminate => HookEvent::PreTerminate,
        }
    }

    fn effects(self) -> PreHookEffects {
        match self {
            PreHookGate::Create | PreHookGate::Receive => PreHookEffects {
                replace_metadata: true,
                propagate_response_headers: true,
            },
            PreHookGate::Finish => PreHookEffects {
                replace_metadata: false,
                propagate_response_headers: false,
            },
            PreHookGate::Terminate => PreHookEffects {
                replace_metadata: false,
                propagate_response_headers: true,
            },
        }
    }
}

pub(crate) struct PreHookOutcome {
    pub(crate) state: UploadState,
    pub(crate) response_headers: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy)]
struct PreHookEffects {
    replace_metadata: bool,
    propagate_response_headers: bool,
}

fn rejection_error(result: &PreHookResult) -> Error {
    Error::HookRejected {
        status_code: result.reject_status().unwrap_or(400),
        message: result.reject_message().unwrap_or_default().to_string(),
    }
}
