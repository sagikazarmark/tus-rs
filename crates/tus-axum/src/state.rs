//! TUS server state for axum integration.
//!
//! This module provides the [`TusState`] struct that holds axum application
//! state for TUS protocol handling.

use axum::extract::FromRef;

use tus_protocol::{Config, HookExecutor, Locker, ProtocolHandle, StateStore, Storage};

/// Server state containing TUS application components.
///
/// This struct is designed to be used with axum's `State` extractor.
///
/// # Example
///
/// ```rust,no_run
/// # use tus_axum::{create_router, RouterOptions, TusState};
/// # use tus_protocol::{
/// #     Config, NoopHookExecutor, ProtocolHandle,
/// #     locking::memory::MemoryLocker,
/// #     state::memory::MemoryStateStore,
/// #     storage::memory::MemoryStorage,
/// # };
/// # fn run() -> Result<(), tus_axum::RouterError> {
/// let protocol = ProtocolHandle::new(
///     Config::default(),
///     MemoryStorage::new(),
///     MemoryStateStore::new(),
///     MemoryLocker::new(),
///     NoopHookExecutor::new(),
/// );
/// let state = TusState::new(protocol);
/// let router = create_router(state, RouterOptions::default())?;
/// # Ok(())
/// # }
/// ```
pub struct TusState<S, I, L, H>
where
    S: Storage,
    I: StateStore,
    L: Locker,
    H: HookExecutor,
{
    protocol: TusProtocol<S, I, L, H>,
}

impl<S, I, L, H> TusState<S, I, L, H>
where
    S: Storage,
    I: StateStore,
    L: Locker,
    H: HookExecutor,
{
    /// Creates a new TusState with the given protocol handle.
    #[must_use]
    pub fn new(protocol: ProtocolHandle<S, I, L, H>) -> Self {
        Self {
            protocol: TusProtocol::new(protocol),
        }
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &Config {
        self.protocol.handle().config()
    }
}

// Manual Debug implementation - the backend type parameters need not be Debug.
impl<S, I, L, H> std::fmt::Debug for TusState<S, I, L, H>
where
    S: Storage,
    I: StateStore,
    L: Locker,
    H: HookExecutor,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TusState").finish_non_exhaustive()
    }
}

// Manual Clone implementation - Arc<T> is Clone regardless of whether T is Clone
impl<S, I, L, H> Clone for TusState<S, I, L, H>
where
    S: Storage,
    I: StateStore,
    L: Locker,
    H: HookExecutor,
{
    fn clone(&self) -> Self {
        Self {
            protocol: self.protocol.clone(),
        }
    }
}

/// Axum-extractable TUS protocol substate.
///
/// This wrapper lets handlers request only the protocol handle from a larger
/// [`TusState`] via axum's `State` extractor.
pub struct TusProtocol<S, I, L, H>
where
    S: Storage,
    I: StateStore,
    L: Locker,
    H: HookExecutor,
{
    handle: ProtocolHandle<S, I, L, H>,
}

impl<S, I, L, H> TusProtocol<S, I, L, H>
where
    S: Storage,
    I: StateStore,
    L: Locker,
    H: HookExecutor,
{
    /// Creates a new protocol substate from a protocol handle.
    #[must_use]
    pub fn new(handle: ProtocolHandle<S, I, L, H>) -> Self {
        Self { handle }
    }

    /// Returns the wrapped [`ProtocolHandle`].
    ///
    /// This is an explicit accessor rather than a `Deref` impl so that
    /// `TusProtocol` does not masquerade as a `ProtocolHandle`; call protocol
    /// operations as `protocol.handle().patch(...)`.
    #[must_use]
    pub fn handle(&self) -> &ProtocolHandle<S, I, L, H> {
        &self.handle
    }
}

// Manual Debug implementation - the backend type parameters need not be Debug.
impl<S, I, L, H> std::fmt::Debug for TusProtocol<S, I, L, H>
where
    S: Storage,
    I: StateStore,
    L: Locker,
    H: HookExecutor,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TusProtocol").finish_non_exhaustive()
    }
}

impl<S, I, L, H> Clone for TusProtocol<S, I, L, H>
where
    S: Storage,
    I: StateStore,
    L: Locker,
    H: HookExecutor,
{
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
        }
    }
}

impl<S, I, L, H> FromRef<TusState<S, I, L, H>> for TusProtocol<S, I, L, H>
where
    S: Storage,
    I: StateStore,
    L: Locker,
    H: HookExecutor,
{
    fn from_ref(state: &TusState<S, I, L, H>) -> Self {
        state.protocol.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tus_protocol::locking::memory::MemoryLocker;
    use tus_protocol::state::memory::MemoryStateStore;
    use tus_protocol::storage::memory::MemoryStorage;
    use tus_protocol::{Config, NoopHookExecutor, ProtocolHandle};

    type Backends = TusState<MemoryStorage, MemoryStateStore, MemoryLocker, NoopHookExecutor>;

    fn build() -> Backends {
        TusState::new(ProtocolHandle::new(
            Config::default().with_base_path("/uploads"),
            MemoryStorage::new(),
            MemoryStateStore::new(),
            MemoryLocker::new(),
            NoopHookExecutor::new(),
        ))
    }

    #[test]
    fn new_starts_each_arc_at_strong_count_one() {
        let config = Arc::new(Config::default().with_base_path("/uploads"));
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        let locker = Arc::new(MemoryLocker::new());
        let hooks = Arc::new(NoopHookExecutor::new());

        let _state = TusState::new(ProtocolHandle::from_arcs(
            config.clone(),
            storage.clone(),
            state_store.clone(),
            locker.clone(),
            hooks.clone(),
        ));

        assert_eq!(Arc::strong_count(&config), 2);
        assert_eq!(Arc::strong_count(&storage), 2);
        assert_eq!(Arc::strong_count(&state_store), 2);
        assert_eq!(Arc::strong_count(&locker), 2);
        assert_eq!(Arc::strong_count(&hooks), 2);
    }

    #[test]
    fn config_exposes_base_path() {
        let state = build();
        assert_eq!(state.config().base_path(), "/uploads");
    }

    #[test]
    fn clone_shares_arcs_rather_than_deep_copying() {
        let state = build();
        let clone = state.clone();
        let protocol = TusProtocol::from_ref(&state);
        let clone_protocol = TusProtocol::from_ref(&clone);

        assert!(Arc::ptr_eq(
            &protocol.handle().config_arc(),
            &clone_protocol.handle().config_arc()
        ));
        assert!(Arc::ptr_eq(
            &protocol.handle().storage_arc(),
            &clone_protocol.handle().storage_arc()
        ));
        assert!(Arc::ptr_eq(
            &protocol.handle().state_store_arc(),
            &clone_protocol.handle().state_store_arc()
        ));
        assert!(Arc::ptr_eq(
            &protocol.handle().locker_arc(),
            &clone_protocol.handle().locker_arc()
        ));
        assert!(Arc::ptr_eq(
            &protocol.handle().hooks_arc(),
            &clone_protocol.handle().hooks_arc()
        ));
    }

    #[test]
    fn from_ref_clones_protocol_substate() {
        use axum::extract::FromRef;
        let state = build();
        let protocol = TusProtocol::from_ref(&state);
        let clone = TusProtocol::from_ref(&state);

        assert!(Arc::ptr_eq(
            &protocol.handle().config_arc(),
            &clone.handle().config_arc()
        ));
        assert!(Arc::ptr_eq(
            &protocol.handle().storage_arc(),
            &clone.handle().storage_arc()
        ));
        assert!(Arc::ptr_eq(
            &protocol.handle().state_store_arc(),
            &clone.handle().state_store_arc()
        ));
        assert!(Arc::ptr_eq(
            &protocol.handle().locker_arc(),
            &clone.handle().locker_arc()
        ));
        assert!(Arc::ptr_eq(
            &protocol.handle().hooks_arc(),
            &clone.handle().hooks_arc()
        ));
    }

    #[test]
    fn new_keeps_handle_supplied_arcs() {
        let config = Arc::new(Config::default());
        let outside = config.clone();
        let handle = ProtocolHandle::from_arcs(
            config,
            Arc::new(MemoryStorage::new()),
            Arc::new(MemoryStateStore::new()),
            Arc::new(MemoryLocker::new()),
            Arc::new(NoopHookExecutor::new()),
        );
        let state = TusState::new(handle);
        let state_config = TusProtocol::from_ref(&state).handle().config_arc();
        assert!(Arc::ptr_eq(&state_config, &outside));
    }
}
