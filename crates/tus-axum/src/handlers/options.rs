//! Axum adapter for the OPTIONS handler.
//!
//! Extracts state from axum and delegates to [`tus_protocol::Protocol::options`].

use axum::extract::State;

use tus_protocol::{HookExecutor, Locker, StateStore, Storage};

use crate::response::TusResponse;
use crate::state::TusProtocol;

/// Handles OPTIONS requests.
pub async fn handle_options<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
) -> TusResponse
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    TusResponse(protocol.options())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::response::IntoResponse;
    use chrono::Utc;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tus_protocol::ProtocolHandle;
    use tus_protocol::config::{Config, Extension, TUS_RESUMABLE};
    use tus_protocol::hooks::NoopHookExecutor;
    use tus_protocol::locking::NoopLocker;
    use tus_protocol::state::UploadState;
    use tus_protocol::storage::ChunkStream;

    struct MockStorage;

    #[async_trait]
    impl Storage for MockStorage {
        fn name(&self) -> &'static str {
            "mock"
        }
        async fn create(&self, state: &mut UploadState) -> tus_protocol::error::Result<String> {
            let key = format!("uploads/{}", state.id());
            state.set_storage_key(key.clone());
            Ok(key)
        }
        async fn append(
            &self,
            _state: &mut UploadState,
            _data: ChunkStream,
        ) -> tus_protocol::error::Result<u64> {
            Ok(0)
        }
        async fn get_stream(
            &self,
            _state: &UploadState,
        ) -> tus_protocol::error::Result<tus_protocol::storage::ByteStream> {
            unimplemented!()
        }
        async fn concat(
            &self,
            _target: &mut UploadState,
            _parts: Vec<UploadState>,
        ) -> tus_protocol::error::Result<()> {
            Ok(())
        }
        async fn delete(&self, _state: &UploadState) -> tus_protocol::error::Result<()> {
            Ok(())
        }
        async fn size(&self, _state: &UploadState) -> tus_protocol::error::Result<Option<u64>> {
            Ok(None)
        }
    }

    struct MockStateStore {
        states: Mutex<HashMap<String, UploadState>>,
    }

    impl MockStateStore {
        fn new() -> Self {
            Self {
                states: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl StateStore for MockStateStore {
        fn name(&self) -> &'static str {
            "mock"
        }
        async fn set(&self, state: &UploadState, _create: bool) -> tus_protocol::error::Result<()> {
            self.states
                .lock()
                .unwrap()
                .insert(state.id().to_string(), state.clone());
            Ok(())
        }
        async fn get(&self, id: &str) -> tus_protocol::error::Result<Option<UploadState>> {
            Ok(self.states.lock().unwrap().get(id).cloned())
        }
        async fn delete(&self, id: &str) -> tus_protocol::error::Result<()> {
            self.states.lock().unwrap().remove(id);
            Ok(())
        }
        async fn list_expired(
            &self,
            _before: chrono::DateTime<Utc>,
        ) -> tus_protocol::error::Result<Vec<String>> {
            Ok(vec![])
        }
        async fn list(
            &self,
            _limit: usize,
            _offset: usize,
        ) -> tus_protocol::error::Result<Vec<String>> {
            Ok(self.states.lock().unwrap().keys().cloned().collect())
        }
    }

    fn create_state(
        config: Config,
    ) -> TusProtocol<MockStorage, MockStateStore, NoopLocker, NoopHookExecutor> {
        TusProtocol::new(ProtocolHandle::new(
            config,
            MockStorage,
            MockStateStore::new(),
            NoopLocker::new(),
            NoopHookExecutor::new(),
        ))
    }

    #[tokio::test]
    async fn axum_adapter_returns_tus_resumable() {
        // Smoke test: the adapter must wire the protocol response through.
        // Detailed header/status assertions live in protocol/options.rs tests.
        let state = create_state(Config::default().with_extension(Extension::Creation));
        let response = handle_options(State(state)).await.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response.headers().get("tus-resumable").unwrap(),
            TUS_RESUMABLE
        );
    }
}
