//! Axum adapter for the HEAD handler.

use axum::extract::State;

use tus_protocol::{HookExecutor, Locker, StateStore, Storage};

use crate::error::Error;
use crate::extractors::{Headers, UploadId};
use crate::response::TusResponse;
use crate::state::TusProtocol;

/// Handles HEAD requests. The `Headers` extractor validates the
/// `Tus-Resumable` header; its value is otherwise unused here.
pub async fn handle_head<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    Headers(_): Headers,
    UploadId(upload_id): UploadId,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    Ok(protocol.head(&upload_id).await?.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use tus_protocol::ProtocolHandle;
    use tus_protocol::config::{Config, TUS_RESUMABLE};
    use tus_protocol::hooks::NoopHookExecutor;
    use tus_protocol::locking::NoopLocker;
    use tus_protocol::state::UploadState;
    use tus_protocol::state::memory::MemoryStateStore;
    use tus_protocol::storage::memory::MemoryStorage;

    #[tokio::test]
    async fn axum_adapter_wires_response() {
        let store = MemoryStateStore::new();
        let state = UploadState::new("test-id").with_length(1000);
        store.set(&state, true).await.unwrap();

        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::default(),
            MemoryStorage::new(),
            store,
            NoopLocker::new(),
            NoopHookExecutor::new(),
        ));

        let response = handle_head(
            State(protocol),
            Headers(Default::default()),
            UploadId("test-id".parse().unwrap()),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response.headers().get("tus-resumable").unwrap(),
            TUS_RESUMABLE
        );
    }
}
