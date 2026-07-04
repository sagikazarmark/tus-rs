//! Async-runtime seams for the TUS client.
//!
//! Everything runtime-specific the client core needs lives here: the
//! `MaybeSend`/`MaybeSync` bounds that relax `Send + Sync` on `wasm32`, and
//! the timer used between retries. On native targets the timer is backed by
//! tokio — this module (plus the tokio `JoinSet` used by
//! `Client::upload_parallel`) is why client operations must run inside a
//! tokio runtime on native targets. On `wasm32` the browser event loop is
//! used instead and no runtime is required.

/// Sleeps for the given duration.
///
/// Native targets use `tokio::time::sleep`, so callers must be running on a
/// tokio runtime; `wasm32` uses `gloo-timers` (the browser event loop).
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn sleep(duration: std::time::Duration) {
    tokio::time::sleep(duration).await;
}

/// Sleeps for the given duration.
///
/// Native targets use `tokio::time::sleep`, so callers must be running on a
/// tokio runtime; `wasm32` uses `gloo-timers` (the browser event loop).
#[cfg(target_arch = "wasm32")]
pub(crate) async fn sleep(duration: std::time::Duration) {
    gloo_timers::future::sleep(duration).await;
}

#[doc(hidden)]
pub trait MaybeSendSync: MaybeSend + MaybeSync {}

impl<T: MaybeSend + MaybeSync + ?Sized> MaybeSendSync for T {}

#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub trait MaybeSend: Send {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}

#[cfg(target_arch = "wasm32")]
#[doc(hidden)]
pub trait MaybeSend {}

#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub trait MaybeSync: Sync {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync + ?Sized> MaybeSync for T {}

#[cfg(target_arch = "wasm32")]
#[doc(hidden)]
pub trait MaybeSync {}

#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSync for T {}
