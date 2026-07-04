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
