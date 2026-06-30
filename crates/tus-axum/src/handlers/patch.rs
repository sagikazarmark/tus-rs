//! Axum adapter for the PATCH handler.

use axum::extract::State;

use tus_protocol::{HookExecutor, Locker, StateStore, Storage};

use crate::error::Error;
use crate::extractors::{Headers, TusBody, UploadId};
use crate::response::TusResponse;
use crate::state::TusProtocol;

/// Handles PATCH requests to upload data.
pub async fn handle_patch<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    Headers(headers): Headers,
    UploadId(upload_id): UploadId,
    body: TusBody,
) -> Result<TusResponse, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let response = protocol
        .patch(headers, &upload_id, body.into_body())
        .await?;

    Ok(response.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use bytes::Bytes;
    use tus_protocol::state::memory::MemoryStateStore;
    use tus_protocol::storage::memory::MemoryStorage;
    use tus_protocol::{
        Config, Extension, NoopHookExecutor, NoopLocker, ProtocolHandle, TUS_RESUMABLE, UploadState,
    };

    #[tokio::test]
    async fn axum_adapter_writes_bytes() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(100);
        let handle = storage.create(state.id()).await.unwrap();
        state.set_storage_handle(handle);
        store.set(&state, true).await.unwrap();

        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::default(),
            storage,
            store,
            NoopLocker::new(),
            NoopHookExecutor::new(),
        ));

        let mut inner = tus_protocol::Headers::default();
        inner.upload_offset = Some(0);
        inner.content_type = Some("application/offset+octet-stream".to_string());
        let headers = Headers(inner);
        let body = TusBody::buffered(Bytes::from_static(b"Hello World"), None);

        let response = handle_patch(
            State(protocol),
            headers,
            UploadId("test-id".parse().unwrap()),
            body,
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::NO_CONTENT);
        assert_eq!(response.headers().get("upload-offset").unwrap(), "11");
        assert_eq!(
            response.headers().get("tus-resumable").unwrap(),
            TUS_RESUMABLE
        );
    }

    #[tokio::test]
    async fn checksum_trailer_reaches_protocol_and_is_verified() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(100);
        let handle = storage.create(state.id()).await.unwrap();
        state.set_storage_handle(handle);
        store.set(&state, true).await.unwrap();

        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::default().with_extension(Extension::ChecksumTrailer),
            storage,
            store,
            NoopLocker::new(),
            NoopHookExecutor::new(),
        ));

        let mut inner = tus_protocol::Headers::default();
        inner.upload_offset = Some(0);
        inner.content_type = Some("application/offset+octet-stream".to_string());
        let headers = Headers(inner);
        let mut trailers = axum::http::HeaderMap::new();
        trailers.insert(
            "upload-checksum",
            "sha1 qvTGHdzF6KLavt4PO0gs2a6pQ00=".parse().unwrap(),
        );
        let body = TusBody::buffered(Bytes::from_static(b"hello"), Some(trailers));

        let response = handle_patch(
            State(protocol),
            headers,
            UploadId("test-id".parse().unwrap()),
            body,
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), axum::http::StatusCode::NO_CONTENT);
        assert_eq!(response.headers().get("upload-offset").unwrap(), "5");
    }
}
