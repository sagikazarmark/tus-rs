//! Axum adapter for the PATCH handler.

use axum::{extract::State, http::HeaderMap};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use http_body_util::BodyExt;

use tus_protocol::{
    ByteStream, ChunkStream, Error as ProtocolError, Extension, HookExecutor, Locker, PatchBody,
    PatchBodyData, StateStore, Storage,
};

use crate::error::Error;
use crate::extractors::{BodyData, Headers, TusBody, UploadId};
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
    // Resolve the effective checksum (header wins over trailer). Trailer bodies
    // are collected by the protocol callback after PATCH preflight validation.
    let has_header_checksum = headers.upload_checksum.is_some();
    let expects_trailer = !has_header_checksum
        && protocol.config().has_extension(Extension::ChecksumTrailer)
        && body.has_trailers();

    let response = if expects_trailer {
        protocol
            .patch(
                headers,
                &upload_id,
                PatchBody::collector(|body_limit| async move {
                    let (data, trailers) = collect_body_and_trailers(body, body_limit).await?;
                    let checksum = TusBody::buffered(data.clone(), trailers).trailer_checksum()?;
                    Ok(PatchBodyData {
                        bytes: data,
                        checksum,
                    })
                }),
            )
            .await?
    } else {
        let effective_checksum = headers.upload_checksum.clone();
        let stream = match body.data {
            BodyData::Buffered(b) => ChunkStream::Buffered(b),
            BodyData::Stream(body) => {
                let byte_stream: ByteStream = Box::pin(
                    body.into_data_stream()
                        .map(|r| r.map_err(std::io::Error::other)),
                );
                ChunkStream::Stream(byte_stream)
            }
        };
        protocol
            .patch(
                headers,
                &upload_id,
                PatchBody::stream(stream, effective_checksum),
            )
            .await?
    };

    Ok(response.into())
}

fn enforce_body_limit(
    current_len: usize,
    next_len: usize,
    body_limit: Option<u64>,
) -> Result<(), ProtocolError> {
    let Some(limit) = body_limit else {
        return Ok(());
    };
    let next_total = (current_len as u64).saturating_add(next_len as u64);
    if next_total > limit {
        return Err(ProtocolError::SizeExceeded {
            size: next_total,
            max: limit,
        });
    }

    Ok(())
}

async fn collect_body_and_trailers(
    body: TusBody,
    body_limit: Option<u64>,
) -> Result<(Bytes, Option<HeaderMap>), ProtocolError> {
    let TusBody { data, trailers, .. } = body;

    match data {
        BodyData::Buffered(bytes) => {
            enforce_body_limit(0, bytes.len(), body_limit)?;
            Ok((bytes, trailers))
        }
        BodyData::Stream(mut body) => {
            let mut buffer = BytesMut::new();
            let mut trailers = trailers;

            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|e| ProtocolError::Internal(e.to_string()))?;
                if let Some(bytes) = frame.data_ref() {
                    enforce_body_limit(buffer.len(), bytes.len(), body_limit)?;
                    buffer.extend_from_slice(bytes);
                }
                if let Some(frame_trailers) = frame.trailers_ref() {
                    trailers = Some(frame_trailers.clone());
                }
            }

            Ok((buffer.freeze(), trailers))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use bytes::Bytes;
    use futures::stream;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tus_protocol::ProtocolHandle;
    use tus_protocol::config::{Config, TUS_RESUMABLE};
    use tus_protocol::hooks::NoopHookExecutor;
    use tus_protocol::locking::NoopLocker;
    use tus_protocol::state::UploadState;
    use tus_protocol::state::memory::MemoryStateStore;
    use tus_protocol::storage::memory::MemoryStorage;

    #[tokio::test]
    async fn axum_adapter_writes_bytes() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(100);
        storage.create(&mut state).await.unwrap();
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
    async fn checksum_trailer_body_exceeding_chunk_limit_returns_size_exceeded_while_collecting() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(100);
        storage.create(&mut state).await.unwrap();
        store.set(&state, true).await.unwrap();

        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::with_all_extensions().max_chunk_size(4),
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
        let stream = stream::iter([
            Ok::<_, std::io::Error>(Bytes::from_static(b"hello")),
            Err(std::io::Error::other(
                "body should not be polled after limit is exceeded",
            )),
        ]);
        let body = TusBody {
            data: BodyData::Stream(axum::body::Body::from_stream(stream)),
            trailers: Some(trailers),
            checksum_trailer_declared: true,
        };

        let err = handle_patch(
            State(protocol),
            headers,
            UploadId("test-id".parse().unwrap()),
            body,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            Error(ProtocolError::SizeExceeded { size: 5, max: 4 })
        ));
    }

    #[tokio::test]
    async fn checksum_trailer_wrong_offset_does_not_consume_body() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let mut state = UploadState::new("test-id").with_length(100);
        storage.create(&mut state).await.unwrap();
        store.set(&state, true).await.unwrap();

        let protocol = TusProtocol::new(ProtocolHandle::new(
            Config::with_all_extensions(),
            storage,
            store,
            NoopLocker::new(),
            NoopHookExecutor::new(),
        ));

        let mut inner = tus_protocol::Headers::default();
        inner.upload_offset = Some(5);
        inner.content_type = Some("application/offset+octet-stream".to_string());
        let headers = Headers(inner);
        let body_polled = Arc::new(AtomicBool::new(false));
        let body_polled_for_stream = body_polled.clone();
        let stream = stream::once(async move {
            body_polled_for_stream.store(true, Ordering::SeqCst);
            Ok::<_, std::io::Error>(Bytes::from_static(b"should-not-read"))
        });
        let body = TusBody {
            data: BodyData::Stream(axum::body::Body::from_stream(stream)),
            trailers: None,
            checksum_trailer_declared: true,
        };

        let err = handle_patch(
            State(protocol),
            headers,
            UploadId("test-id".parse().unwrap()),
            body,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            Error(ProtocolError::OffsetMismatch {
                expected: 0,
                actual: 5
            })
        ));
        assert!(!body_polled.load(Ordering::SeqCst));
    }
}
