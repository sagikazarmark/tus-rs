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
//!   `PreCreate`, `PreReceive`, and `PreTerminate` may add response headers.
//!   `PreCreate` and `PreReceive` may replace user metadata through
//!   [`PreHookResult::proceed_with_metadata`]. `PreFinish` is gate-only: it may
//!   reject completion, but metadata and response headers are ignored. Hooks never
//!   receive or return storage locator or backend-internal storage facts.
//!
//! - **Post-hooks** (`PostCreate`, `PostReceive`, `PostFinish`, `PostTerminate`)
//!   are notifications, and they are **best-effort**. The protocol awaits
//!   them inline today, which means an HTTP adapter cancellation (for example,
//!   a client disconnect mid-request) can drop the handler future before the
//!   post-hook fires. Once storage or state has changed, post-hook errors are
//!   logged and swallowed. The committed bytes and state are unaffected;
//!   per-PATCH atomicity plus reconcile-on-HEAD keep the upload consistent,
//!   but the post-hook callback may simply not run.
//!
//!   Implications for hook authors:
//!
//!   - Treat post-hooks as non-durable notifications. Do not rely on them as
//!     the source of truth for whether a side effect needs to happen.
//!   - Make hook handlers idempotent so adapter retries (or operator-driven
//!     reconciliation sweeps) are safe.
//!   - For audit logs, antivirus scans, or anything that *must* fire for
//!     every committed upload, run a periodic reconciliation job that
//!     compares your sink against the server's state store and
//!     re-fires the missed events. The protocol does not provide
//!     durable hook delivery; that's an operator concern.
//!
//! ## Event Matrix
//!
//! | Request path | Hook events | Notes |
//! | --- | --- | --- |
//! | `POST` regular or partial upload | `PreCreate`, `PostCreate` | `PreCreate` may reject, add response headers, or replace user metadata before storage/state creation. `PostCreate` runs after storage and state are committed. |
//! | `POST` with Creation-With-Upload body | `PreCreate`, `PreReceive`, `PostCreate`, `PostReceive`, plus `PreFinish`/`PostFinish` when the initial body completes the upload | `PreReceive` runs before the body is collected and may reject, add response headers, or replace user metadata after `PreCreate`. `PreFinish` runs after the initial body is committed; because the upload resource is new, a rejection rolls the whole creation back and no post-hooks fire. It is gate-only. |
//! | `POST` final concatenation upload | `PreCreate`, `PostCreate`, plus `PreFinish`/`PostFinish` when every referenced partial is complete | Final upload state is derived from referenced partials before `PreCreate`; `PreCreate` may reject, add response headers, or replace user metadata. `PreFinish` gates the final record before it is materialized; it is gate-only. |
//! | `PATCH` | `PreReceive`, `PostReceive`, plus `PreFinish`/`PostFinish` when the patch completes the upload | `PreReceive` may reject, add response headers, or replace user metadata before bytes are committed. `PreFinish` runs after the completing bytes and state are durably committed; a rejection fails the PATCH response and skips `PostFinish`, but the upload remains stored and complete. It is gate-only. |
//! | `DELETE` | `PreTerminate`, `PostTerminate` | Requires the Termination extension. `PreTerminate` may reject or add response headers. `PostTerminate` runs after state deletion and best-effort storage deletion. |
//! | `HEAD` or `GET` | none normally; `PreFinish`/`PostFinish` may run for lazy final-upload materialization | Read paths reconcile final concatenation uploads. If complete referenced parts can materialize or repair the final upload, `PreFinish` gates that commit and `PostFinish` follows it. `PreFinish` is gate-only. |

use async_trait::async_trait;
use chrono::{DateTime, Utc};
#[cfg(not(target_arch = "wasm32"))]
use futures_util::future::BoxFuture;
#[cfg(target_arch = "wasm32")]
use futures_util::future::LocalBoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::runtime::{MaybeSend, MaybeSendSync};
use crate::state::{UploadMetadata, UploadState};

/// Trait for implementing hooks.
///
/// Hooks can subscribe to specific events and will be called with
/// context about the operation being performed.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Hook: MaybeSendSync {
    /// Returns the hook name for logging/debugging.
    fn name(&self) -> &str;

    /// Returns the events this hook subscribes to.
    fn events(&self) -> &[HookEvent];

    /// Executes a pre-hook before an operation.
    ///
    /// Pre-hooks can:
    /// - Reject the operation by returning `PreHookResult::reject()`
    /// - Replace user metadata by returning `PreHookResult::proceed_with_metadata()`
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
#[non_exhaustive]
pub enum HookEvent {
    /// Before creating a new upload (POST).
    /// Pre-hook can reject the creation or replace user metadata.
    PreCreate,

    /// After an upload is created.
    PostCreate,

    /// Before receiving upload data (PATCH).
    /// Pre-hook can reject the data or replace user metadata.
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

impl std::fmt::Display for HookEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Context provided to hooks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct HookContext {
    /// The hook event type.
    pub event: HookEvent,

    /// Hook-visible upload facts at the time of the hook.
    pub upload: HookUpload,

    /// HTTP request metadata.
    pub request: HookRequestInfo,
}

impl HookContext {
    /// Creates a new hook context.
    pub fn new(event: HookEvent, upload: impl Into<HookUpload>, request: HookRequestInfo) -> Self {
        Self {
            event,
            upload: upload.into(),
            request,
        }
    }
}

/// Hook-visible upload facts.
///
/// This snapshot intentionally excludes storage locator facts such as the
/// storage key and backend-internal storage metadata. Hooks can inspect protocol
/// state and user metadata, but storage adapters remain the only code that sees
/// or mutates storage-local bookkeeping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookUpload {
    /// Unique upload identifier.
    id: String,

    /// Bytes successfully uploaded (current offset).
    offset: u64,

    /// Total size in bytes. None if deferred (Upload-Defer-Length).
    length: Option<u64>,

    /// When the upload was created.
    created_at: DateTime<Utc>,

    /// Advertised protocol expiration deadline. None if expiration is disabled.
    expires_at: Option<DateTime<Utc>>,

    /// Whether this is a partial upload (for concatenation).
    is_partial: bool,

    /// Whether this is a final concatenated upload.
    is_final: bool,

    /// Part IDs for final uploads (Concatenation extension).
    parts: Option<Vec<String>>,

    /// User-provided metadata from the Upload-Metadata header.
    metadata: UploadMetadata,
}

impl HookUpload {
    /// Creates a new hook upload snapshot with the given ID.
    pub fn new(id: impl Into<String>) -> Self {
        UploadState::new(id).into()
    }

    /// Creates a hook upload snapshot from persisted upload state.
    pub fn from_state(state: &UploadState) -> Self {
        Self {
            id: state.id().to_string(),
            offset: state.offset(),
            length: state.length(),
            created_at: *state.created_at(),
            expires_at: state.expires_at().copied(),
            is_partial: state.is_partial(),
            is_final: state.is_final(),
            parts: state.parts().map(|parts| parts.to_vec()),
            metadata: state.metadata().clone(),
        }
    }

    /// Returns the upload identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the current offset.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the declared upload length, if any.
    pub fn length(&self) -> Option<u64> {
        self.length
    }

    /// Returns when the upload was created.
    pub fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    /// Returns the advertised protocol expiration deadline, if expiration is enabled.
    ///
    /// This timestamp is not, by itself, a completed-upload retention deadline;
    /// use [`HookUpload::is_expired`] for protocol expiration semantics.
    pub fn expires_at(&self) -> Option<&DateTime<Utc>> {
        self.expires_at.as_ref()
    }

    /// Returns whether the upload is marked as partial.
    pub fn is_partial(&self) -> bool {
        self.is_partial
    }

    /// Returns whether the upload is marked as final.
    pub fn is_final(&self) -> bool {
        self.is_final
    }

    /// Returns the concatenated part IDs for final uploads.
    pub fn parts(&self) -> Option<&[String]> {
        self.parts.as_deref()
    }

    /// Returns the user metadata map.
    pub fn metadata(&self) -> &UploadMetadata {
        &self.metadata
    }

    /// Returns whether the upload is complete.
    pub fn is_complete(&self) -> bool {
        match self.length {
            Some(length) => self.offset >= length,
            None => false,
        }
    }

    /// Returns whether the upload is protocol-expired.
    ///
    /// TUS expiration applies to unfinished upload resources. Completed
    /// non-partial uploads are deliverable content and do not expire through
    /// this policy.
    pub fn is_expired(&self) -> bool {
        crate::expiration::ProtocolExpiration::from_parts(
            self.expires_at,
            self.is_complete(),
            self.is_partial,
        )
        .is_expired()
    }

    fn set_metadata(&mut self, metadata: UploadMetadata) {
        self.metadata = metadata;
    }
}

impl From<UploadState> for HookUpload {
    fn from(state: UploadState) -> Self {
        Self::from_state(&state)
    }
}

impl From<&UploadState> for HookUpload {
    fn from(state: &UploadState) -> Self {
        Self::from_state(state)
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
/// Construct with the associated helpers — [`PreHookResult::proceed`],
/// [`PreHookResult::proceed_with_metadata`], or [`PreHookResult::reject`] —
/// then refine with the `with_*` builders. Fields are private so a "proceed"
/// result can never also carry a rejection status (invalid states are
/// unrepresentable), and read them back through the accessors
/// ([`proceeds`](Self::proceeds), [`reject_status`](Self::reject_status),
/// etc.). The type is `#[non_exhaustive]` so new decision knobs (for example,
/// per-request rate-limit overrides) can be added without a major version
/// bump.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PreHookResult {
    /// Whether to proceed with the operation.
    proceed: bool,

    /// Replacement user metadata, if any.
    ///
    /// The protocol applies this only at documented mutation points such as
    /// `PreCreate` and `PreReceive`; hooks cannot mutate storage locator or
    /// backend-internal storage facts.
    metadata: Option<UploadMetadata>,

    /// HTTP status code for rejection.
    reject_status: Option<u16>,

    /// Rejection message for the client.
    reject_message: Option<String>,

    /// Additional response headers to include.
    response_headers: HashMap<String, String>,
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

    /// Creates a result that proceeds with replacement user metadata.
    #[must_use]
    pub fn proceed_with_metadata(metadata: impl Into<UploadMetadata>) -> Self {
        Self::proceed().with_metadata(metadata)
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

    /// Replaces user metadata if the current hook event allows metadata changes.
    #[must_use]
    pub fn with_metadata(mut self, metadata: impl Into<UploadMetadata>) -> Self {
        self.metadata = Some(metadata.into());
        self
    }

    /// Returns whether the operation should proceed.
    #[must_use]
    pub fn proceeds(&self) -> bool {
        self.proceed
    }

    /// Returns the replacement user metadata, if the hook supplied any.
    #[must_use]
    pub fn metadata(&self) -> Option<&UploadMetadata> {
        self.metadata.as_ref()
    }

    /// Returns the rejection status code, if the operation was rejected.
    #[must_use]
    pub fn reject_status(&self) -> Option<u16> {
        self.reject_status
    }

    /// Returns the rejection message, if the operation was rejected.
    #[must_use]
    pub fn reject_message(&self) -> Option<&str> {
        self.reject_message.as_deref()
    }

    /// Returns the additional response headers the hook supplied.
    #[must_use]
    pub fn response_headers(&self) -> &HashMap<String, String> {
        &self.response_headers
    }
}

/// Trait for executing a chain of hooks.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait HookExecutor: MaybeSendSync {
    /// Executes pre-hooks for an event.
    ///
    /// Hooks are executed in order. If any hook rejects, execution stops
    /// and the rejection is returned.
    async fn execute_pre(&self, ctx: &HookContext) -> Result<PreHookResult>;

    /// Executes post-hooks for an event.
    ///
    /// Protocol handlers call post-hooks after committing storage or state
    /// changes. At those call sites, executor errors are logged and swallowed
    /// so an already-committed request is not reported as failed to the client.
    async fn execute_post(&self, ctx: &HookContext) -> Result<()>;
}

/// Executes a post-hook notification after protocol state or storage has changed.
///
/// Post-hooks are best-effort: executor failures are logged and swallowed so an
/// already-committed request is not reported as failed to the client.
pub(crate) async fn execute_post_best_effort<H>(hooks: &H, ctx: &HookContext)
where
    H: HookExecutor + ?Sized,
{
    if let Err(error) = hooks.execute_post(ctx).await {
        tracing::warn!(
            event = ctx.event.as_str(),
            upload_id = %ctx.upload.id(),
            error = %error,
            "post-hook executor failed after commit"
        );
    }
}

/// Compile-time proof that hook closures need not be `Send` on `wasm32`.
///
/// Not a runtime test: `cargo test` never runs for wasm32 in CI, but
/// `cargo check --target wasm32-unknown-unknown` type-checks this function,
/// which is the property being pinned (a `!Send` `Rc` captured across the
/// closure and its future).
#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
fn assert_hook_chain_accepts_non_send_closure() {
    use std::cell::Cell;
    use std::rc::Rc;

    let calls = Rc::new(Cell::new(0));
    let _chain = HookChain::new().on_pre_create(move |_| {
        let calls = Rc::clone(&calls);
        async move {
            calls.set(calls.get() + 1);
            Ok(PreHookResult::proceed())
        }
    });
}

/// A chain of hooks that are executed in order.
pub struct HookChain {
    hooks: Vec<Arc<dyn Hook>>,
}

impl std::fmt::Debug for HookChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.hooks.iter().map(|hook| hook.name()).collect();
        f.debug_struct("HookChain").field("hooks", &names).finish()
    }
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
#[cfg(not(target_arch = "wasm32"))]
type PreHookFut = BoxFuture<'static, Result<PreHookResult>>;
#[cfg(target_arch = "wasm32")]
type PreHookFut = LocalBoxFuture<'static, Result<PreHookResult>>;

/// Type alias for a boxed post-hook future.
#[cfg(not(target_arch = "wasm32"))]
type PostHookFut = BoxFuture<'static, Result<()>>;
#[cfg(target_arch = "wasm32")]
type PostHookFut = LocalBoxFuture<'static, Result<()>>;

/// Storage type for a pre-hook closure.
#[cfg(not(target_arch = "wasm32"))]
type PreHookFn = Arc<dyn Fn(HookContext) -> PreHookFut + Send + Sync>;
#[cfg(target_arch = "wasm32")]
type PreHookFn = Arc<dyn Fn(HookContext) -> PreHookFut>;

/// Storage type for a post-hook closure.
#[cfg(not(target_arch = "wasm32"))]
type PostHookFn = Arc<dyn Fn(HookContext) -> PostHookFut + Send + Sync>;
#[cfg(target_arch = "wasm32")]
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

impl std::fmt::Debug for FnHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnHook")
            .field("name", &self.name)
            .field("events", &self.events)
            .field("has_pre", &self.pre.is_some())
            .field("has_post", &self.post.is_some())
            .finish()
    }
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl HookExecutor for HookChain {
    async fn execute_pre(&self, ctx: &HookContext) -> Result<PreHookResult> {
        let mut result = PreHookResult::proceed();
        let mut current_upload = ctx.upload.clone();
        let mut metadata_changed = false;

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
                    metadata: None,
                    reject_status: hook_result.reject_status,
                    reject_message: hook_result.reject_message,
                    response_headers: result.response_headers,
                });
            }

            // Update user metadata if modified so later hooks see the current snapshot.
            if ctx.event.allows_metadata_replacement()
                && let Some(metadata) = hook_result.metadata
            {
                current_upload.set_metadata(metadata);
                metadata_changed = true;
            }
        }

        // All hooks passed
        if metadata_changed {
            result.metadata = Some(current_upload.metadata().clone());
        }
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

impl HookEvent {
    fn allows_metadata_replacement(self) -> bool {
        matches!(self, HookEvent::PreCreate | HookEvent::PreReceive)
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl HookExecutor for NoopHookExecutor {
    async fn execute_pre(&self, _ctx: &HookContext) -> Result<PreHookResult> {
        Ok(PreHookResult::proceed())
    }

    async fn execute_post(&self, _ctx: &HookContext) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageHandle;

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

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

    #[test]
    fn hook_upload_serialization_hides_storage_facts() {
        let mut state = UploadState::new("test-id").with_length(5);
        let mut handle = StorageHandle::new("storage-secret");
        handle.set_internal("backend-upload-id", "internal-secret");
        state.set_storage_handle(handle);
        let ctx = HookContext::new(HookEvent::PreCreate, state, HookRequestInfo::default());

        let json = serde_json::to_value(&ctx).unwrap();
        let serialized = json.to_string();

        assert_eq!(json["upload"]["id"], "test-id");
        assert!(json["upload"].get("storage_key").is_none());
        assert!(json["upload"].get("internal").is_none());
        assert!(!serialized.contains("storage-secret"));
        assert!(!serialized.contains("internal-secret"));
    }

    #[test]
    fn hook_upload_expiration_matches_completed_upload_policy() {
        let expired_at = Utc::now() - chrono::Duration::minutes(1);
        let mut completed = UploadState::new("completed")
            .with_length(5)
            .with_expiration(expired_at);
        completed.set_offset(5);
        let mut completed_partial = UploadState::new("completed-partial")
            .with_length(5)
            .with_expiration(expired_at)
            .as_partial();
        completed_partial.set_offset(5);

        assert!(!HookUpload::from_state(&completed).is_expired());
        assert!(HookUpload::from_state(&completed_partial).is_expired());
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
        assert!(result.metadata.is_none());

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
        assert!(proceed.metadata.is_none());

        let mut metadata = UploadMetadata::new();
        metadata.insert("filename", "test.txt");
        let proceed_with_metadata = PreHookResult::proceed_with_metadata(metadata);
        assert!(proceed_with_metadata.proceed);
        assert!(proceed_with_metadata.metadata.is_some());

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
    async fn pre_hook_closure_can_replace_metadata() {
        let chain = HookChain::new().on_pre_create(|ctx| async move {
            let mut metadata = ctx.upload.metadata().clone();
            metadata.insert("injected", "yes");
            Ok(PreHookResult::proceed_with_metadata(metadata))
        });

        let ctx = make_context(HookEvent::PreCreate);
        let result = chain.execute_pre(&ctx).await.unwrap();
        assert!(result.proceed);
        let updated = result.metadata.expect("metadata should be carried through");
        assert_eq!(
            updated.get("injected").and_then(|v| v.as_str()),
            Some("yes")
        );
    }

    #[tokio::test]
    async fn metadata_replacement_is_visible_to_later_pre_hooks() {
        let chain = HookChain::new()
            .on_pre_create(|ctx| async move {
                assert!(ctx.upload.metadata().get("injected").is_none());
                let mut metadata = ctx.upload.metadata().clone();
                metadata.insert("injected", "yes");
                Ok(PreHookResult::proceed_with_metadata(metadata))
            })
            .on_pre_create(|ctx| async move {
                assert_eq!(
                    ctx.upload
                        .metadata()
                        .get("injected")
                        .and_then(|v| v.as_str()),
                    Some("yes")
                );
                Ok(PreHookResult::proceed())
            });

        let ctx = make_context(HookEvent::PreCreate);
        let result = chain.execute_pre(&ctx).await.unwrap();

        assert_eq!(
            result
                .metadata
                .unwrap()
                .get("injected")
                .and_then(|v| v.as_str()),
            Some("yes")
        );
    }

    #[tokio::test]
    async fn metadata_replacement_is_ignored_for_gate_only_pre_hooks() {
        let chain = HookChain::new()
            .on_pre_finish(|ctx| async move {
                assert!(ctx.upload.metadata().get("uncommitted").is_none());
                let mut metadata = ctx.upload.metadata().clone();
                metadata.insert("uncommitted", "yes");
                Ok(PreHookResult::proceed_with_metadata(metadata))
            })
            .on_pre_finish(|ctx| async move {
                assert!(ctx.upload.metadata().get("uncommitted").is_none());
                Ok(PreHookResult::proceed())
            });

        let ctx = make_context(HookEvent::PreFinish);
        let result = chain.execute_pre(&ctx).await.unwrap();

        assert!(result.proceed);
        assert!(result.metadata.is_none());
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
