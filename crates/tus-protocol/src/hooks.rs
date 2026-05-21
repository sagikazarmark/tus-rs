//! Hook system for extending TUS server behavior.
//!
//! Two ways to register hooks:
//!
//! ## Closures on [`HookChain`]
//!
//! Shortest path: good for small, event-specific callbacks:
//!
//! ```no_run
//! # #[cfg(all(feature = "storage-memory", feature = "state-memory"))] async fn _demo() {
//! use tus_protocol::{HookChain, PreHookResult};
//!
//! let hooks = HookChain::new()
//!     .on_pre_create(|ctx| async move {
//!         if ctx.upload.metadata().get("filename").is_none() {
//!             Ok(PreHookResult::reject(400, "filename required"))
//!         } else {
//!             Ok(PreHookResult::proceed())
//!         }
//!     })
//!     .on_post_finish(|ctx| async move {
//!         tracing::info!(upload = %ctx.upload.id(), "upload complete");
//!         Ok(())
//!     });
//! # let _ = hooks;
//! # }
//! ```
//!
//! One method per [`HookEvent`] variant
//! (`on_pre_create` / `on_post_create` / ... / `on_post_terminate`). Multiple
//! closures for the same event run in registration order.
//!
//! ## [`Hook`] trait for stateful hooks
//!
//! When a hook needs to own state or subscribe to multiple events, implement
//! the [`Hook`] trait directly and register with
//! [`HookChain::with_hook`]. The closure shortcuts above are just thin
//! wrappers over an internal [`FnHook`] that implements the same trait.
//!
//! ## Delivery semantics
//!
//! - **Pre-hooks** (`PreCreate`, `PreReceive`, `PreFinish`, `PreTerminate`)
//!   are gates: the protocol awaits the result and acts on it. A pre-hook
//!   that returns [`PreHookResult::reject`] aborts the operation; a pre-hook
//!   error fails the request. Run-to-completion is part of the contract.
//!
//! - **Post-hooks** (`PostCreate`, `PostReceive`, `PostFinish`, `PostTerminate`)
//!   are notifications, and they are **best-effort**. The protocol awaits
//!   them inline today, which means an HTTP adapter cancellation (for example,
//!   a client disconnect mid-request) can drop the handler future before the
//!   post-hook fires. The committed bytes and state are unaffected;
//!   per-PATCH atomicity plus reconcile-on-HEAD keep the upload consistent,
//!   but the post-hook callback may simply not run.
//!
//!   Implications for hook authors:
//!
//!   - Treat post-hooks as **at-most-once** notifications. Do not rely on
//!     them as the source of truth for whether a side effect needs to
//!     happen.
//!   - Make hook handlers idempotent so retries (or operator-driven
//!     reconciliation sweeps) are safe.
//!   - For audit logs, antivirus scans, or anything that *must* fire for
//!     every committed upload, run a periodic reconciliation job that
//!     compares your sink against the server's state store and
//!     re-fires the missed events. The protocol does not provide
//!     durable hook delivery; that's an operator concern.

use async_trait::async_trait;
#[cfg(not(feature = "local-futures"))]
use futures::future::BoxFuture;
#[cfg(feature = "local-futures")]
use futures::future::LocalBoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::runtime::{MaybeSend, MaybeSendSync};
use crate::state::UploadState;

/// Trait for implementing hooks.
///
/// Hooks can subscribe to specific events and will be called with
/// context about the operation being performed.
#[cfg_attr(not(feature = "local-futures"), async_trait)]
#[cfg_attr(feature = "local-futures", async_trait(?Send))]
pub trait Hook: MaybeSendSync {
    /// Returns the hook name for logging/debugging.
    fn name(&self) -> &str;

    /// Returns the events this hook subscribes to.
    fn events(&self) -> &[HookEvent];

    /// Executes a pre-hook before an operation.
    ///
    /// Pre-hooks can:
    /// - Reject the operation by returning `PreHookResult::reject()`
    /// - Modify the upload state by returning `PreHookResult::proceed_with()`
    /// - Allow the operation to proceed by returning `PreHookResult::proceed()`
    async fn pre_hook(&self, _ctx: &HookContext) -> Result<PreHookResult> {
        Ok(PreHookResult::proceed())
    }

    /// Executes a post-hook after an operation.
    ///
    /// Post-hooks are informational - they cannot affect the operation
    /// since it has already completed. Errors from post-hooks are logged
    /// but don't fail the operation.
    async fn post_hook(&self, _ctx: &HookContext) -> Result<()> {
        Ok(())
    }
}

/// Hook events that can be subscribed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HookEvent {
    /// Before creating a new upload (POST).
    /// Pre-hook can reject the creation or modify the upload state.
    PreCreate,

    /// After an upload is created.
    PostCreate,

    /// Before receiving upload data (PATCH).
    /// Pre-hook can reject the data or modify how it's handled.
    PreReceive,

    /// After upload data is received.
    PostReceive,

    /// Before an upload is completed (final PATCH that completes the upload).
    PreFinish,

    /// After an upload is completed.
    PostFinish,

    /// Before an upload is terminated (DELETE).
    PreTerminate,

    /// After an upload is terminated.
    PostTerminate,
}

impl HookEvent {
    /// Returns all hook events.
    pub fn all() -> &'static [HookEvent] {
        &[
            HookEvent::PreCreate,
            HookEvent::PostCreate,
            HookEvent::PreReceive,
            HookEvent::PostReceive,
            HookEvent::PreFinish,
            HookEvent::PostFinish,
            HookEvent::PreTerminate,
            HookEvent::PostTerminate,
        ]
    }

    /// Returns whether this is a pre-hook.
    pub fn is_pre(&self) -> bool {
        matches!(
            self,
            HookEvent::PreCreate
                | HookEvent::PreReceive
                | HookEvent::PreFinish
                | HookEvent::PreTerminate
        )
    }

    /// Returns the event name as a string.
    pub fn as_str(&self) -> &'static str {
        match self {
            HookEvent::PreCreate => "pre-create",
            HookEvent::PostCreate => "post-create",
            HookEvent::PreReceive => "pre-receive",
            HookEvent::PostReceive => "post-receive",
            HookEvent::PreFinish => "pre-finish",
            HookEvent::PostFinish => "post-finish",
            HookEvent::PreTerminate => "pre-terminate",
            HookEvent::PostTerminate => "post-terminate",
        }
    }
}

/// Context provided to hooks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct HookContext {
    /// The hook event type.
    pub event: HookEvent,

    /// The upload state at the time of the hook.
    pub upload: UploadState,

    /// HTTP request metadata.
    pub request: HookRequestInfo,
}

impl HookContext {
    /// Creates a new hook context.
    pub fn new(event: HookEvent, upload: UploadState, request: HookRequestInfo) -> Self {
        Self {
            event,
            upload,
            request,
        }
    }
}

/// HTTP request information for hooks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct HookRequestInfo {
    /// HTTP method.
    pub method: String,

    /// Request path.
    pub path: String,

    /// Remote address of the client.
    pub remote_addr: Option<String>,

    /// Selected request headers (subset for security).
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

/// Result from a pre-hook execution.
///
/// Construct with the associated helpers: [`PreHookResult::proceed`],
/// [`PreHookResult::proceed_with`], or [`PreHookResult::reject`], rather than
/// with a struct literal. The type is `#[non_exhaustive]` so new decision
/// knobs (for example, per-request rate-limit overrides) can be added without
/// a major version bump.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PreHookResult {
    /// Whether to proceed with the operation.
    pub proceed: bool,

    /// Modified upload state (if any).
    /// Pre-hooks can modify metadata, storage path, etc.
    pub upload: Option<UploadState>,

    /// HTTP status code for rejection.
    pub reject_status: Option<u16>,

    /// Rejection message for the client.
    pub reject_message: Option<String>,

    /// Additional response headers to include.
    pub response_headers: HashMap<String, String>,
}

impl PreHookResult {
    /// Creates a result that allows the operation to proceed.
    #[must_use]
    pub fn proceed() -> Self {
        Self {
            proceed: true,
            ..Default::default()
        }
    }

    /// Creates a result that proceeds with a modified upload state.
    #[must_use]
    pub fn proceed_with(upload: UploadState) -> Self {
        Self {
            proceed: true,
            upload: Some(upload),
            ..Default::default()
        }
    }

    /// Creates a result that rejects the operation.
    #[must_use]
    pub fn reject(status: u16, message: impl Into<String>) -> Self {
        Self {
            proceed: false,
            reject_status: Some(status),
            reject_message: Some(message.into()),
            ..Default::default()
        }
    }

    /// Adds a response header.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.response_headers.insert(name.into(), value.into());
        self
    }
}

/// Trait for executing a chain of hooks.
#[cfg_attr(not(feature = "local-futures"), async_trait)]
#[cfg_attr(feature = "local-futures", async_trait(?Send))]
pub trait HookExecutor: MaybeSendSync {
    /// Executes pre-hooks for an event.
    ///
    /// Hooks are executed in order. If any hook rejects, execution stops
    /// and the rejection is returned.
    async fn execute_pre(&self, ctx: &HookContext) -> Result<PreHookResult>;

    /// Executes post-hooks for an event.
    ///
    /// All hooks are executed even if some fail. Errors are logged but
    /// don't affect the result.
    async fn execute_post(&self, ctx: &HookContext) -> Result<()>;
}

/// A chain of hooks that are executed in order.
pub struct HookChain {
    hooks: Vec<Arc<dyn Hook>>,
}

impl HookChain {
    /// Creates an empty hook chain.
    pub fn new() -> Self {
        Self { hooks: Vec::new() }
    }

    /// Adds a hook to the chain.
    #[must_use]
    pub fn with_hook<H: Hook + 'static>(mut self, hook: H) -> Self {
        self.hooks.push(Arc::new(hook));
        self
    }

    /// Adds a shared hook to the chain.
    #[must_use]
    pub fn add_shared(mut self, hook: Arc<dyn Hook>) -> Self {
        self.hooks.push(hook);
        self
    }
}

impl Default for HookChain {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Closure-based registration
// ============================================================================
//
// Convenience methods on HookChain that let users register a closure for a
// specific event without defining a dedicated Hook impl. Each method wraps
// the closure in an internal FnHook that subscribes to exactly one event.
//
// For stateful hooks, or hooks spanning multiple events, users should still
// implement the `Hook` trait and register via `with_hook`.

/// Type alias for a boxed pre-hook future.
#[cfg(not(feature = "local-futures"))]
type PreHookFut = BoxFuture<'static, Result<PreHookResult>>;
#[cfg(feature = "local-futures")]
type PreHookFut = LocalBoxFuture<'static, Result<PreHookResult>>;

/// Type alias for a boxed post-hook future.
#[cfg(not(feature = "local-futures"))]
type PostHookFut = BoxFuture<'static, Result<()>>;
#[cfg(feature = "local-futures")]
type PostHookFut = LocalBoxFuture<'static, Result<()>>;

/// Storage type for a pre-hook closure.
#[cfg(not(feature = "local-futures"))]
type PreHookFn = Arc<dyn Fn(HookContext) -> PreHookFut + Send + Sync>;
#[cfg(feature = "local-futures")]
type PreHookFn = Arc<dyn Fn(HookContext) -> PreHookFut>;

/// Storage type for a post-hook closure.
#[cfg(not(feature = "local-futures"))]
type PostHookFn = Arc<dyn Fn(HookContext) -> PostHookFut + Send + Sync>;
#[cfg(feature = "local-futures")]
type PostHookFn = Arc<dyn Fn(HookContext) -> PostHookFut>;

/// Internal adapter that implements [`Hook`] from a pair of optional
/// closures. Produced by [`HookChain`]'s `on_*` methods; also constructible
/// directly for users who want finer control.
pub struct FnHook {
    name: String,
    events: Vec<HookEvent>,
    pre: Option<PreHookFn>,
    post: Option<PostHookFn>,
}

impl FnHook {
    /// Creates a new empty FnHook bound to no events. Chain `.for_event(...)`
    /// or `.for_events(...)` to subscribe.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            events: Vec::new(),
            pre: None,
            post: None,
        }
    }

    /// Subscribes this hook to a single event.
    #[must_use]
    pub fn for_event(mut self, event: HookEvent) -> Self {
        if !self.events.contains(&event) {
            self.events.push(event);
        }
        self
    }

    /// Subscribes this hook to multiple events.
    #[must_use]
    pub fn for_events(mut self, events: impl IntoIterator<Item = HookEvent>) -> Self {
        for event in events {
            if !self.events.contains(&event) {
                self.events.push(event);
            }
        }
        self
    }

    /// Sets the pre-hook closure. Only fires on pre-events.
    #[must_use]
    pub fn with_pre<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(HookContext) -> Fut + MaybeSendSync + 'static,
        Fut: std::future::Future<Output = Result<PreHookResult>> + MaybeSend + 'static,
    {
        self.pre = Some(Arc::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    /// Sets the post-hook closure. Only fires on post-events.
    #[must_use]
    pub fn with_post<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(HookContext) -> Fut + MaybeSendSync + 'static,
        Fut: std::future::Future<Output = Result<()>> + MaybeSend + 'static,
    {
        self.post = Some(Arc::new(move |ctx| Box::pin(f(ctx))));
        self
    }
}

#[cfg_attr(not(feature = "local-futures"), async_trait)]
#[cfg_attr(feature = "local-futures", async_trait(?Send))]
impl Hook for FnHook {
    fn name(&self) -> &str {
        &self.name
    }

    fn events(&self) -> &[HookEvent] {
        &self.events
    }

    async fn pre_hook(&self, ctx: &HookContext) -> Result<PreHookResult> {
        match &self.pre {
            Some(f) => f(ctx.clone()).await,
            None => Ok(PreHookResult::proceed()),
        }
    }

    async fn post_hook(&self, ctx: &HookContext) -> Result<()> {
        match &self.post {
            Some(f) => f(ctx.clone()).await,
            None => Ok(()),
        }
    }
}

/// Macro implementation helper: declares `on_<event>` convenience methods on
/// [`HookChain`], one per event variant. Each registers an [`FnHook`]
/// subscribed to exactly the matching event.
macro_rules! hook_chain_on_methods {
    (
        $( pre  $pre_name:ident  => $pre_event:ident ),* $(,)?
        ;
        $( post $post_name:ident => $post_event:ident ),* $(,)?
    ) => {
        impl HookChain {
            $(
                #[doc = concat!("Registers a closure to run on [`HookEvent::", stringify!($pre_event), "`].")]
                #[must_use]
                pub fn $pre_name<F, Fut>(self, f: F) -> Self
                where
                    F: Fn(HookContext) -> Fut + MaybeSendSync + 'static,
                    Fut: std::future::Future<Output = Result<PreHookResult>> + MaybeSend + 'static,
                {
                    self.with_hook(
                        FnHook::new(stringify!($pre_name))
                            .for_event(HookEvent::$pre_event)
                            .with_pre(f),
                    )
                }
            )*

            $(
                #[doc = concat!("Registers a closure to run on [`HookEvent::", stringify!($post_event), "`].")]
                #[must_use]
                pub fn $post_name<F, Fut>(self, f: F) -> Self
                where
                    F: Fn(HookContext) -> Fut + MaybeSendSync + 'static,
                    Fut: std::future::Future<Output = Result<()>> + MaybeSend + 'static,
                {
                    self.with_hook(
                        FnHook::new(stringify!($post_name))
                            .for_event(HookEvent::$post_event)
                            .with_post(f),
                    )
                }
            )*
        }
    };
}

hook_chain_on_methods! {
    pre on_pre_create    => PreCreate,
    pre on_pre_receive   => PreReceive,
    pre on_pre_finish    => PreFinish,
    pre on_pre_terminate => PreTerminate
    ;
    post on_post_create    => PostCreate,
    post on_post_receive   => PostReceive,
    post on_post_finish    => PostFinish,
    post on_post_terminate => PostTerminate,
}

#[cfg_attr(not(feature = "local-futures"), async_trait)]
#[cfg_attr(feature = "local-futures", async_trait(?Send))]
impl HookExecutor for HookChain {
    async fn execute_pre(&self, ctx: &HookContext) -> Result<PreHookResult> {
        let mut result = PreHookResult::proceed();
        let mut current_upload = ctx.upload.clone();

        for hook in &self.hooks {
            if !hook.events().contains(&ctx.event) {
                continue;
            }

            if !ctx.event.is_pre() {
                continue;
            }

            // Create context with potentially modified upload
            let hook_ctx = HookContext {
                upload: current_upload.clone(),
                ..ctx.clone()
            };

            let hook_result = hook.pre_hook(&hook_ctx).await?;

            // Merge response headers
            for (k, v) in hook_result.response_headers {
                result.response_headers.insert(k, v);
            }

            if !hook_result.proceed {
                // Hook rejected - return immediately
                return Ok(PreHookResult {
                    proceed: false,
                    upload: hook_result.upload,
                    reject_status: hook_result.reject_status,
                    reject_message: hook_result.reject_message,
                    response_headers: result.response_headers,
                });
            }

            // Update upload state if modified
            if let Some(upload) = hook_result.upload {
                current_upload = upload;
            }
        }

        // All hooks passed
        result.upload = Some(current_upload);
        Ok(result)
    }

    async fn execute_post(&self, ctx: &HookContext) -> Result<()> {
        for hook in &self.hooks {
            if !hook.events().contains(&ctx.event) {
                continue;
            }

            if ctx.event.is_pre() {
                continue;
            }

            // Execute post-hook, log errors but continue
            if let Err(e) = hook.post_hook(ctx).await {
                tracing::warn!(
                    hook = hook.name(),
                    event = ctx.event.as_str(),
                    error = %e,
                    "post-hook failed"
                );
            }
        }
        Ok(())
    }
}

/// A no-op hook executor that does nothing.
///
/// Useful when hooks are not needed.
#[derive(Debug, Clone, Default)]
pub struct NoopHookExecutor;

impl NoopHookExecutor {
    /// Creates a new no-op executor.
    pub fn new() -> Self {
        Self
    }
}

#[cfg_attr(not(feature = "local-futures"), async_trait)]
#[cfg_attr(feature = "local-futures", async_trait(?Send))]
impl HookExecutor for NoopHookExecutor {
    async fn execute_pre(&self, ctx: &HookContext) -> Result<PreHookResult> {
        Ok(PreHookResult::proceed_with(ctx.upload.clone()))
    }

    async fn execute_post(&self, _ctx: &HookContext) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestHook {
        name: String,
        events: Vec<HookEvent>,
        reject: bool,
    }

    impl TestHook {
        fn new(name: &str, events: Vec<HookEvent>) -> Self {
            Self {
                name: name.to_string(),
                events,
                reject: false,
            }
        }

        fn rejecting(name: &str, events: Vec<HookEvent>) -> Self {
            Self {
                name: name.to_string(),
                events,
                reject: true,
            }
        }
    }

    #[cfg_attr(not(feature = "local-futures"), async_trait)]
    #[cfg_attr(feature = "local-futures", async_trait(?Send))]
    impl Hook for TestHook {
        fn name(&self) -> &str {
            &self.name
        }

        fn events(&self) -> &[HookEvent] {
            &self.events
        }

        async fn pre_hook(&self, _ctx: &HookContext) -> Result<PreHookResult> {
            if self.reject {
                Ok(PreHookResult::reject(403, "Rejected by test hook"))
            } else {
                Ok(PreHookResult::proceed())
            }
        }

        async fn post_hook(&self, _ctx: &HookContext) -> Result<()> {
            Ok(())
        }
    }

    fn make_context(event: HookEvent) -> HookContext {
        HookContext::new(
            event,
            UploadState::new("test-id"),
            HookRequestInfo::default(),
        )
    }

    #[tokio::test]
    async fn test_hook_chain_empty() {
        let chain = HookChain::new();
        let ctx = make_context(HookEvent::PreCreate);

        let result = chain.execute_pre(&ctx).await.unwrap();
        assert!(result.proceed);
    }

    #[tokio::test]
    async fn test_hook_chain_proceed() {
        let chain = HookChain::new()
            .with_hook(TestHook::new("hook1", vec![HookEvent::PreCreate]))
            .with_hook(TestHook::new("hook2", vec![HookEvent::PreCreate]));

        let ctx = make_context(HookEvent::PreCreate);
        let result = chain.execute_pre(&ctx).await.unwrap();
        assert!(result.proceed);
    }

    #[tokio::test]
    async fn test_hook_chain_reject() {
        let chain = HookChain::new()
            .with_hook(TestHook::new("hook1", vec![HookEvent::PreCreate]))
            .with_hook(TestHook::rejecting("hook2", vec![HookEvent::PreCreate]));

        let ctx = make_context(HookEvent::PreCreate);
        let result = chain.execute_pre(&ctx).await.unwrap();
        assert!(!result.proceed);
        assert_eq!(result.reject_status, Some(403));
    }

    #[tokio::test]
    async fn test_hook_event_filtering() {
        let chain =
            HookChain::new().with_hook(TestHook::rejecting("hook1", vec![HookEvent::PreReceive])); // Wrong event

        let ctx = make_context(HookEvent::PreCreate);
        let result = chain.execute_pre(&ctx).await.unwrap();
        assert!(result.proceed); // Should proceed because hook doesn't match event
    }

    #[tokio::test]
    async fn test_noop_executor() {
        let executor = NoopHookExecutor::new();
        let ctx = make_context(HookEvent::PreCreate);

        let result = executor.execute_pre(&ctx).await.unwrap();
        assert!(result.proceed);
        assert!(result.upload.is_some());

        // Post should also succeed
        assert!(executor.execute_post(&ctx).await.is_ok());
    }

    #[test]
    fn test_hook_event_is_pre() {
        assert!(HookEvent::PreCreate.is_pre());
        assert!(HookEvent::PreReceive.is_pre());
        assert!(HookEvent::PreFinish.is_pre());
        assert!(HookEvent::PreTerminate.is_pre());

        assert!(!HookEvent::PostCreate.is_pre());
        assert!(!HookEvent::PostReceive.is_pre());
        assert!(!HookEvent::PostFinish.is_pre());
        assert!(!HookEvent::PostTerminate.is_pre());
    }

    #[test]
    fn test_pre_hook_result_builders() {
        let proceed = PreHookResult::proceed();
        assert!(proceed.proceed);
        assert!(proceed.upload.is_none());

        let state = UploadState::new("test");
        let proceed_with = PreHookResult::proceed_with(state);
        assert!(proceed_with.proceed);
        assert!(proceed_with.upload.is_some());

        let reject = PreHookResult::reject(400, "Bad request");
        assert!(!reject.proceed);
        assert_eq!(reject.reject_status, Some(400));
        assert_eq!(reject.reject_message, Some("Bad request".to_string()));
    }

    // ------------------------------------------------------------------
    // Closure-based API tests
    // ------------------------------------------------------------------

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn on_pre_create_runs_closure() {
        let chain = HookChain::new()
            .on_pre_create(|_ctx| async { Ok(PreHookResult::reject(418, "teapot")) });
        let ctx = make_context(HookEvent::PreCreate);
        let result = chain.execute_pre(&ctx).await.unwrap();
        assert!(!result.proceed);
        assert_eq!(result.reject_status, Some(418));
    }

    #[cfg(feature = "local-futures")]
    #[tokio::test]
    async fn hook_chain_accepts_non_send_closure_in_local_mode() {
        use std::cell::Cell;
        use std::rc::Rc;

        let calls = Rc::new(Cell::new(0));
        let calls_for_hook = calls.clone();
        let chain = HookChain::new().on_pre_create(move |_| {
            let calls_for_hook = calls_for_hook.clone();
            async move {
                calls_for_hook.set(calls_for_hook.get() + 1);
                Ok(PreHookResult::proceed())
            }
        });

        let ctx = make_context(HookEvent::PreCreate);
        let result = chain.execute_pre(&ctx).await.unwrap();

        assert!(result.proceed);
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test]
    async fn on_pre_create_ignores_other_events() {
        let chain = HookChain::new()
            .on_pre_create(|_ctx| async { Ok(PreHookResult::reject(500, "should not fire")) });
        // Use PreReceive; the closure was registered for PreCreate only.
        let ctx = make_context(HookEvent::PreReceive);
        let result = chain.execute_pre(&ctx).await.unwrap();
        assert!(result.proceed);
    }

    #[tokio::test]
    async fn on_post_finish_runs_closure() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let chain = HookChain::new().on_post_finish(move |_ctx| {
            let c = counter_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        let ctx = make_context(HookEvent::PostFinish);
        chain.execute_post(&ctx).await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn chain_mixes_closure_and_trait_hooks() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let chain = HookChain::new()
            .with_hook(TestHook::new("trait-hook", vec![HookEvent::PreCreate]))
            .on_pre_create(move |_ctx| {
                let c = counter_clone.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(PreHookResult::proceed())
                }
            });

        let ctx = make_context(HookEvent::PreCreate);
        let result = chain.execute_pre(&ctx).await.unwrap();
        assert!(result.proceed);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pre_hook_closure_can_mutate_state() {
        let chain = HookChain::new().on_pre_create(|ctx| async move {
            let mut upload = ctx.upload.clone();
            upload.metadata_mut().insert("injected", "yes");
            Ok(PreHookResult::proceed_with(upload))
        });

        let ctx = make_context(HookEvent::PreCreate);
        let result = chain.execute_pre(&ctx).await.unwrap();
        assert!(result.proceed);
        let updated = result.upload.expect("upload should be carried through");
        assert_eq!(
            updated.metadata().get("injected").and_then(|v| v.as_str()),
            Some("yes")
        );
    }

    #[tokio::test]
    async fn multiple_closures_for_same_event_run_in_order() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c1 = counter.clone();
        let c2 = counter.clone();

        let chain = HookChain::new()
            .on_post_create(move |_ctx| {
                let c = c1.clone();
                async move {
                    // First hook sees 0, writes 1.
                    assert_eq!(c.load(Ordering::SeqCst), 0);
                    c.store(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .on_post_create(move |_ctx| {
                let c = c2.clone();
                async move {
                    // Second hook sees 1, writes 2.
                    assert_eq!(c.load(Ordering::SeqCst), 1);
                    c.store(2, Ordering::SeqCst);
                    Ok(())
                }
            });

        let ctx = make_context(HookEvent::PostCreate);
        chain.execute_post(&ctx).await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn fn_hook_for_events_subscribes_multiple() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let hook = FnHook::new("multi")
            .for_events([HookEvent::PostCreate, HookEvent::PostFinish])
            .with_post(move |_ctx| {
                let c = counter_clone.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            });

        let chain = HookChain::new().with_hook(hook);

        chain
            .execute_post(&make_context(HookEvent::PostCreate))
            .await
            .unwrap();
        chain
            .execute_post(&make_context(HookEvent::PostFinish))
            .await
            .unwrap();
        // Not subscribed; should not fire.
        chain
            .execute_post(&make_context(HookEvent::PostReceive))
            .await
            .unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
