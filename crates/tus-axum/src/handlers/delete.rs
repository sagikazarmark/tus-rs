//! Axum adapter for the DELETE handler.

use axum::extract::State;

use tus_protocol::{HookExecutor, Locker, StateStore, Storage};

use crate::error::Error;
use crate::extractors::{Headers, UploadId};
use crate::response::TusResponse;
use crate::state::TusProtocol;

/// Handles DELETE requests to terminate uploads.
pub async fn handle_delete<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    Headers(headers): Headers,
    UploadId(upload_id): UploadId,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    Ok(protocol.delete(&headers, &upload_id).await?.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use tus_protocol::ProtocolHandle;
    use tus_protocol::config::{Config, Extension, TUS_RESUMABLE};
    use tus_protocol::hooks::NoopHookExecutor;
    use tus_protocol::locking::NoopLocker;
    use tus_protocol::state::UploadState;
    use tus_protocol::state::memory::MemoryStateStore;
    use tus_protocol::storage::memory::MemoryStorage;

    #[tokio::test]
    async fn axum_adapter_returns_no_content() {
        let storage = MemoryStorage::new();
        let state_store = MemoryStateStore::new();
        let mut upload = UploadState::new("test-id").with_length(1000);
        upload.set_storage_key("uploads/test-id");
        state_store.set(&upload, true).await.unwrap();

        let config = Config::default().with_extension(Extension::Termination);
        let protocol = TusProtocol::new(ProtocolHandle::new(
            config,
            storage,
            state_store,
            NoopLocker::new(),
            NoopHookExecutor::new(),
        ));

        let response = handle_delete(
            State(protocol),
            Headers(Default::default()),
            UploadId("test-id".parse().unwrap()),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers().get("tus-resumable").unwrap(),
            TUS_RESUMABLE
        );
    }
}
