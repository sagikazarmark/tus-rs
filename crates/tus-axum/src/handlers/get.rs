//! Axum adapter for download GET requests.

use axum::{body::Body, extract::State, response::Response};
use http::{HeaderMap, header};

use tus_protocol::{DownloadRequest, HookExecutor, Locker, StateStore, Storage};

use crate::error::Error;
use crate::extractors::UploadId;
use crate::state::TusProtocol;

/// Handles GET requests that download an uploaded file.
///
/// This is a native-server convenience endpoint, not part of the core tus
/// upload protocol. It is available unless disabled in [`tus_protocol::Config`].
pub async fn handle_get<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    headers: HeaderMap,
    UploadId(upload_id): UploadId,
) -> Result<Response, Error>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let range = headers
        .get(header::RANGE)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| tus_protocol::Error::InvalidHeader {
                    header: "Range",
                    message: "header must be valid ASCII".to_string(),
                })
        })
        .transpose()?;

    let response = protocol
        .download(DownloadRequest {
            upload_id: &upload_id,
            range,
        })
        .await?;

    let tus_protocol::DownloadResponse {
        status,
        headers,
        body,
    } = response;

    let mut builder = Response::builder().status(status);
    if let Some(out_headers) = builder.headers_mut() {
        *out_headers = headers;
    }

    let response = builder.body(Body::from_stream(body)).map_err(|err| {
        tus_protocol::Error::Internal(format!("failed to build download response: {err}"))
    })?;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::HeaderValue;
    use http::StatusCode;
    use http_body_util::BodyExt;
    use tus_protocol::state::memory::MemoryStateStore;
    use tus_protocol::storage::memory::MemoryStorage;
    use tus_protocol::{
        ChunkStream, Config, Error as ProtocolError, NoopHookExecutor, NoopLocker, ProtocolHandle,
        Storage, UploadMetadata, UploadState,
    };

    #[tokio::test]
    async fn axum_adapter_streams_completed_upload() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(5);
        let mut metadata = UploadMetadata::new();
        metadata.insert("mimetype".to_string(), "text/plain");
        state.set_metadata(metadata);
        storage.create(&mut state).await.unwrap();
        storage
            .append(
                &mut state,
                ChunkStream::from_bytes(Bytes::from_static(b"hello")),
            )
            .await
            .unwrap();
        store.set(&state, true).await.unwrap();

        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::default(),
            storage,
            store,
            NoopLocker::new(),
            NoopHookExecutor::new(),
        ));

        let response = handle_get(
            State(protocol),
            HeaderMap::new(),
            UploadId("test-id".parse().unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain"
        );
        assert_eq!(response.headers().get("content-length").unwrap(), "5");

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn axum_adapter_rejects_disabled_downloads() {
        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::default().disable_download(),
            MemoryStorage::new(),
            MemoryStateStore::new(),
            NoopLocker::new(),
            NoopHookExecutor::new(),
        ));

        let error = handle_get(
            State(protocol),
            HeaderMap::new(),
            UploadId("test-id".parse().unwrap()),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error(ProtocolError::MethodNotAllowed(_))));
    }

    #[tokio::test]
    async fn axum_adapter_serves_single_byte_range() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(11);
        storage.create(&mut state).await.unwrap();
        storage
            .append(
                &mut state,
                ChunkStream::from_bytes(Bytes::from_static(b"hello world")),
            )
            .await
            .unwrap();
        store.set(&state, true).await.unwrap();

        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::default(),
            storage,
            store,
            NoopLocker::new(),
            NoopHookExecutor::new(),
        ));

        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=6-10"));

        let response = handle_get(
            State(protocol),
            headers,
            UploadId("test-id".parse().unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get("content-range").unwrap(),
            "bytes 6-10/11"
        );
        assert_eq!(response.headers().get("content-length").unwrap(), "5");

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"world");
    }

    #[tokio::test]
    async fn axum_adapter_reconciles_stale_state_before_download() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(5);
        storage.create(&mut state).await.unwrap();
        storage
            .append(
                &mut state,
                ChunkStream::from_bytes(Bytes::from_static(b"hello")),
            )
            .await
            .unwrap();

        let stale_state = state.clone();
        store.set(&stale_state, true).await.unwrap();

        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::default(),
            storage,
            store,
            NoopLocker::new(),
            NoopHookExecutor::new(),
        ));
        let store = protocol.state_store_arc();

        let response = handle_get(
            State(protocol),
            HeaderMap::new(),
            UploadId("test-id".parse().unwrap()),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"hello");
        assert_eq!(store.get("test-id").await.unwrap().unwrap().offset(), 5);
    }

    #[tokio::test]
    async fn axum_adapter_rejects_incomplete_upload_after_reconciliation() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(5);
        storage.create(&mut state).await.unwrap();
        storage
            .append(
                &mut state,
                ChunkStream::from_bytes(Bytes::from_static(b"hel")),
            )
            .await
            .unwrap();

        let stale_state = state.clone();
        store.set(&stale_state, true).await.unwrap();

        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::default(),
            storage,
            store,
            NoopLocker::new(),
            NoopHookExecutor::new(),
        ));

        let error = handle_get(
            State(protocol),
            HeaderMap::new(),
            UploadId("test-id".parse().unwrap()),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, Error(ProtocolError::IncompleteUpload(_))));
    }
}
