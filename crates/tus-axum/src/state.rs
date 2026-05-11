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
/// # use tus_axum::{create_router, TusState};
/// # use tus_protocol::{
/// #     Config, ProtocolHandle,
/// #     hooks::NoopHookExecutor,
/// #     locking::memory::MemoryLocker,
/// #     state::memory::MemoryStateStore,
/// #     storage::memory::MemoryStorage,
/// # };
/// let protocol = ProtocolHandle::new(
///     Config::default(),
///     MemoryStorage::new(),
///     MemoryStateStore::new(),
///     MemoryLocker::new(),
///     NoopHookExecutor::new(),
/// );
/// let state = TusState::new(protocol);
/// let router = create_router(state);
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
    pub fn new(protocol: ProtocolHandle<S, I, L, H>) -> Self {
        Self {
            protocol: TusProtocol::new(protocol),
        }
    }

    /// Returns the configuration.
    pub fn config(&self) -> &Config {
        self.protocol.config()
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
    pub fn new(handle: ProtocolHandle<S, I, L, H>) -> Self {
        Self { handle }
    }
}

impl<S, I, L, H> std::ops::Deref for TusProtocol<S, I, L, H>
where
    S: Storage,
    I: StateStore,
    L: Locker,
    H: HookExecutor,
{
    type Target = ProtocolHandle<S, I, L, H>;

    fn deref(&self) -> &Self::Target {
        &self.handle
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
    use tus_protocol::hooks::NoopHookExecutor;
    use tus_protocol::locking::memory::MemoryLocker;
    use tus_protocol::state::memory::MemoryStateStore;
    use tus_protocol::storage::memory::MemoryStorage;
    use tus_protocol::{Config, ProtocolHandle};

    type Backends = TusState<MemoryStorage, MemoryStateStore, MemoryLocker, NoopHookExecutor>;

    fn build() -> Backends {
        TusState::new(ProtocolHandle::new(
            Config::default().base_path("/uploads"),
            MemoryStorage::new(),
            MemoryStateStore::new(),
            MemoryLocker::new(),
            NoopHookExecutor::new(),
        ))
    }

    #[test]
    fn new_starts_each_arc_at_strong_count_one() {
        let config = Arc::new(Config::default().base_path("/uploads"));
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
        assert_eq!(state.config().base_path_str(), "/uploads");
    }

    #[test]
    fn clone_shares_arcs_rather_than_deep_copying() {
        let state = build();
        let clone = state.clone();
        let protocol = TusProtocol::from_ref(&state);
        let clone_protocol = TusProtocol::from_ref(&clone);

        assert!(Arc::ptr_eq(
            &protocol.config_arc(),
            &clone_protocol.config_arc()
        ));
        assert!(Arc::ptr_eq(
            &protocol.storage_arc(),
            &clone_protocol.storage_arc()
        ));
        assert!(Arc::ptr_eq(
            &protocol.state_store_arc(),
            &clone_protocol.state_store_arc()
        ));
        assert!(Arc::ptr_eq(
            &protocol.locker_arc(),
            &clone_protocol.locker_arc()
        ));
        assert!(Arc::ptr_eq(
            &protocol.hooks_arc(),
            &clone_protocol.hooks_arc()
        ));
    }

    #[test]
    fn from_ref_clones_protocol_substate() {
        use axum::extract::FromRef;
        let state = build();
        let protocol = TusProtocol::from_ref(&state);
        let clone = TusProtocol::from_ref(&state);

        assert!(Arc::ptr_eq(&protocol.config_arc(), &clone.config_arc()));
        assert!(Arc::ptr_eq(&protocol.storage_arc(), &clone.storage_arc()));
        assert!(Arc::ptr_eq(
            &protocol.state_store_arc(),
            &clone.state_store_arc()
        ));
        assert!(Arc::ptr_eq(&protocol.locker_arc(), &clone.locker_arc()));
        assert!(Arc::ptr_eq(&protocol.hooks_arc(), &clone.hooks_arc()));
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
        let state_config = TusProtocol::from_ref(&state).config_arc();
        assert!(Arc::ptr_eq(&state_config, &outside));
    }
}
