//! Platform-conditional `Send`/`Sync` bounds used by this crate's traits.
//!
//! On native targets, `MaybeSend`/`MaybeSync` alias `Send`/`Sync`. On
//! `wasm32` targets they are empty traits, so trait objects and futures need
//! not be thread-safe in single-threaded runtimes such as Cloudflare
//! Workers. Blanket implementations cover every eligible type; these traits
//! are bounds vocabulary, never something to implement by hand.

/// Combined [`MaybeSend`] + [`MaybeSync`] bound.
pub trait MaybeSendSync: MaybeSend + MaybeSync {}

impl<T: MaybeSend + MaybeSync + ?Sized> MaybeSendSync for T {}

/// `Send` on native targets; no requirement on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}

/// `Send` on native targets; no requirement on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}

#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

/// `Sync` on native targets; no requirement on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync + ?Sized> MaybeSync for T {}

/// `Sync` on native targets; no requirement on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}

#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSync for T {}
