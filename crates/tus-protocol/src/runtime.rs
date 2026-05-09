pub trait MaybeSendSync: MaybeSend + MaybeSync {}

impl<T: MaybeSend + MaybeSync + ?Sized> MaybeSendSync for T {}

#[cfg(not(feature = "local-futures"))]
pub trait MaybeSend: Send {}

#[cfg(not(feature = "local-futures"))]
impl<T: Send + ?Sized> MaybeSend for T {}

#[cfg(feature = "local-futures")]
pub trait MaybeSend {}

#[cfg(feature = "local-futures")]
impl<T: ?Sized> MaybeSend for T {}

#[cfg(not(feature = "local-futures"))]
pub trait MaybeSync: Sync {}

#[cfg(not(feature = "local-futures"))]
impl<T: Sync + ?Sized> MaybeSync for T {}

#[cfg(feature = "local-futures")]
pub trait MaybeSync {}

#[cfg(feature = "local-futures")]
impl<T: ?Sized> MaybeSync for T {}
