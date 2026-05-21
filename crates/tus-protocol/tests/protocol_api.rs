#![cfg(all(
    feature = "storage-memory",
    feature = "state-memory",
    not(feature = "local-futures")
))]

use bytes::Bytes;
use http::StatusCode;
use tus_protocol::state::memory::MemoryStateStore;
use tus_protocol::storage::memory::MemoryStorage;
use tus_protocol::{
    ChunkStream, Config, Error, Headers, NoopHookExecutor, NoopLocker, Protocol, Response,
    StateStore, Storage, UploadId, UploadState,
};

#[tokio::test]
async fn protocol_head_uses_bundled_dependencies() {
    let config = Config::default();
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();

    let mut state = UploadState::new("test-id").with_length(42);
    storage.create(&mut state).await.unwrap();
    storage
        .append(
            &mut state,
            ChunkStream::from_bytes(Bytes::from_static(b"hello")),
        )
        .await
        .unwrap();

    state.set_offset(0);
    state_store.set(&state, true).await.unwrap();

    let _headers = Headers::default();
    let _response = Response::new(StatusCode::NO_CONTENT);
    let _error = Error::NotFound("missing".to_string());

    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);
    let upload_id: UploadId = "test-id".parse().unwrap();
    let response = protocol.head(&upload_id).await.unwrap();

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.headers.get("upload-length").unwrap(), "42");
    assert_eq!(response.headers.get("upload-offset").unwrap(), "5");

    let stored = state_store.get("test-id").await.unwrap().unwrap();
    assert_eq!(stored.offset(), 5);
}
