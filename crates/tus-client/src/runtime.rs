#[doc(hidden)]
pub trait MaybeSendSync: MaybeSend + MaybeSync {}

impl<T: MaybeSend + MaybeSync + ?Sized> MaybeSendSync for T {}

#[cfg(all(not(feature = "local-futures"), not(target_arch = "wasm32")))]
#[doc(hidden)]
pub trait MaybeSend: Send {}

#[cfg(all(not(feature = "local-futures"), not(target_arch = "wasm32")))]
impl<T: Send + ?Sized> MaybeSend for T {}

#[cfg(any(feature = "local-futures", target_arch = "wasm32"))]
#[doc(hidden)]
pub trait MaybeSend {}

#[cfg(any(feature = "local-futures", target_arch = "wasm32"))]
impl<T: ?Sized> MaybeSend for T {}

#[cfg(all(not(feature = "local-futures"), not(target_arch = "wasm32")))]
#[doc(hidden)]
pub trait MaybeSync: Sync {}

#[cfg(all(not(feature = "local-futures"), not(target_arch = "wasm32")))]
impl<T: Sync + ?Sized> MaybeSync for T {}

#[cfg(any(feature = "local-futures", target_arch = "wasm32"))]
#[doc(hidden)]
pub trait MaybeSync {}

#[cfg(any(feature = "local-futures", target_arch = "wasm32"))]
impl<T: ?Sized> MaybeSync for T {}
