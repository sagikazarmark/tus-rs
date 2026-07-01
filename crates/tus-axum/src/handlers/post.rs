//! Axum adapter for the POST handler.

use axum::extract::State;

use tus_protocol::{HookExecutor, Locker, StateStore, Storage};

use crate::error::Error;
use crate::extractors::{Headers, TusBody};
use crate::response::TusResponse;
use crate::state::TusProtocol;

/// Handles POST requests to create new uploads.
pub async fn handle_post<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    Headers(headers): Headers,
    body: TusBody,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    Ok(protocol.post(headers, body.into_body()).await?.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use tus_protocol::state::memory::MemoryStateStore;
    use tus_protocol::storage::memory::MemoryStorage;
    use tus_protocol::{Config, NoopHookExecutor, NoopLocker, ProtocolHandle, TUS_RESUMABLE};

    #[tokio::test]
    async fn axum_adapter_creates_upload() {
        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::default(),
            MemoryStorage::new(),
            MemoryStateStore::new(),
            NoopLocker::new(),
            NoopHookExecutor::new(),
        ));

        let mut inner = tus_protocol::Headers::default();
        inner.upload_length = Some(1000);
        let headers = Headers(inner);
        let response = handle_post(State(protocol), headers, TusBody::absent())
            .await
            .unwrap()
            .into_response();

        assert_eq!(response.status(), axum::http::StatusCode::CREATED);
        assert_eq!(
            response.headers().get("tus-resumable").unwrap(),
            TUS_RESUMABLE
        );
        assert!(response.headers().get("location").is_some());
    }
}
