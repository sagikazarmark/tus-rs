#![cfg(all(
    feature = "storage-memory",
    feature = "state-memory",
    not(feature = "local-futures")
))]

use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use http::StatusCode;
use std::time::Duration as StdDuration;
use tus_protocol::state::memory::MemoryStateStore;
use tus_protocol::storage::memory::MemoryStorage;
use tus_protocol::{
    AppendRequest, ChunkStream, Config, Error, Extension, Headers, HookRequestInfo,
    NoopHookExecutor, NoopLocker, Protocol, RequestBody, Response, StateStore, Storage,
    UploadConcat, UploadId, UploadState, reclaim_expired_uploads,
};

fn post_headers_with_length(length: u64) -> Headers {
    let mut headers = Headers::default();
    headers.upload_length = Some(length);
    headers
}

fn patch_headers(offset: u64) -> Headers {
    let mut headers = Headers::default();
    headers.upload_offset = Some(offset);
    headers.content_type = Some("application/offset+octet-stream".to_string());
    headers
}

fn partial_post_headers(length: u64) -> Headers {
    let mut headers = post_headers_with_length(length);
    headers.upload_concat = Some(UploadConcat::Partial);
    headers
}

fn final_post_headers(part_ids: &[&UploadId]) -> Headers {
    let mut headers = Headers::default();
    headers.upload_concat = Some(UploadConcat::Final(
        part_ids
            .iter()
            .map(|id| format!("/files/{}", id.as_str()))
            .collect(),
    ));
    headers
}

fn upload_id_from_location(response: &Response) -> UploadId {
    response
        .headers
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn protocol_head_uses_bundled_dependencies() {
    let config = Config::default();
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();

    let mut state = UploadState::new("test-id").with_length(42);
    let handle = storage.create(state.id()).await.unwrap();
    let handle = storage
        .append(AppendRequest {
            handle,
            expected_offset: 0,
            data: ChunkStream::from_bytes(Bytes::from_static(b"hello")),
            completes_upload: false,
        })
        .await
        .unwrap();
    state.set_storage_handle(handle);

    state_store.set(&state, true).await.unwrap();

    let _headers = Headers::default();
    let _request_info = HookRequestInfo::default();
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

#[tokio::test]
async fn protocol_head_accepts_expired_completed_regular_upload() {
    let config = Config::default()
        .with_extension(Extension::Expiration)
        .expiration(StdDuration::from_secs(60));
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);

    let post_response = protocol
        .post(post_headers_with_length(5), RequestBody::absent())
        .await
        .unwrap();
    let upload_id = upload_id_from_location(&post_response);

    protocol
        .patch(
            patch_headers(0),
            &upload_id,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"hello"))),
        )
        .await
        .unwrap();

    let state = state_store
        .get(upload_id.as_str())
        .await
        .unwrap()
        .unwrap()
        .with_expiration(Utc::now() - ChronoDuration::minutes(1));
    state_store.set(&state, false).await.unwrap();

    let response = protocol.head(&upload_id).await.unwrap();

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
    assert_eq!(response.headers.get("upload-length").unwrap(), "5");
    assert!(response.headers.get("upload-expires").is_none());
}

#[tokio::test]
async fn protocol_head_recovers_expired_regular_upload_completed_in_storage() {
    let config = Config::default().with_extension(Extension::Expiration);
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let mut state = UploadState::new("test-id")
        .with_length(5)
        .with_expiration(Utc::now() - ChronoDuration::minutes(1));
    let handle = storage.create(state.id()).await.unwrap();
    let handle = storage
        .append(AppendRequest {
            handle,
            expected_offset: 0,
            data: ChunkStream::from_bytes(Bytes::from_static(b"hello")),
            completes_upload: true,
        })
        .await
        .unwrap();
    state.set_storage_handle(handle);
    state_store.set(&state, true).await.unwrap();

    let upload_id = "test-id".parse().unwrap();
    let response = Protocol::new(&config, &storage, &state_store, &locker, &hooks)
        .head(&upload_id)
        .await
        .unwrap();
    let recovered = state_store.get("test-id").await.unwrap().unwrap();

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
    assert!(response.headers.get("upload-expires").is_none());
    assert_eq!(recovered.offset(), 5);
}

#[tokio::test]
async fn protocol_patch_completion_omits_upload_expires() {
    let config = Config::default()
        .with_extension(Extension::Expiration)
        .expiration(StdDuration::from_secs(60));
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);

    let post_response = protocol
        .post(post_headers_with_length(5), RequestBody::absent())
        .await
        .unwrap();
    let upload_id = upload_id_from_location(&post_response);

    let response = protocol
        .patch(
            patch_headers(0),
            &upload_id,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"hello"))),
        )
        .await
        .unwrap();

    assert_eq!(response.status, StatusCode::NO_CONTENT);
    assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
    assert!(response.headers.get("upload-expires").is_none());
}

#[tokio::test]
async fn protocol_head_accepts_expired_completed_final_upload() {
    let config = Config::default()
        .with_extension(Extension::Concatenation)
        .with_extension(Extension::Expiration)
        .expiration(StdDuration::from_secs(60));
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);

    let part_response = protocol
        .post(partial_post_headers(5), RequestBody::absent())
        .await
        .unwrap();
    let part_id = upload_id_from_location(&part_response);
    protocol
        .patch(
            patch_headers(0),
            &part_id,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"hello"))),
        )
        .await
        .unwrap();
    let final_response = protocol
        .post(final_post_headers(&[&part_id]), RequestBody::absent())
        .await
        .unwrap();
    let final_id = upload_id_from_location(&final_response);

    let final_state = state_store
        .get(final_id.as_str())
        .await
        .unwrap()
        .unwrap()
        .with_expiration(Utc::now() - ChronoDuration::minutes(1));
    state_store.set(&final_state, false).await.unwrap();

    let response = protocol.head(&final_id).await.unwrap();
    let expired = state_store.list_expired(Utc::now()).await.unwrap();

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
    assert_eq!(response.headers.get("upload-length").unwrap(), "5");
    assert!(response.headers.get("upload-expires").is_none());
    assert!(!expired.contains(&final_id.as_str().to_string()));
}

#[tokio::test]
async fn protocol_head_rejects_expired_completed_partial_upload() {
    let config = Config::default()
        .with_extension(Extension::Concatenation)
        .with_extension(Extension::Expiration)
        .expiration(StdDuration::from_secs(60));
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);

    let part_response = protocol
        .post(partial_post_headers(5), RequestBody::absent())
        .await
        .unwrap();
    let part_id = upload_id_from_location(&part_response);
    protocol
        .patch(
            patch_headers(0),
            &part_id,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"hello"))),
        )
        .await
        .unwrap();

    let part_state = state_store
        .get(part_id.as_str())
        .await
        .unwrap()
        .unwrap()
        .with_expiration(Utc::now() - ChronoDuration::minutes(1));
    state_store.set(&part_state, false).await.unwrap();

    let err = protocol.head(&part_id).await.unwrap_err();
    let expired = state_store.list_expired(Utc::now()).await.unwrap();

    assert!(matches!(err, Error::Expired(id) if id == part_id.as_str()));
    assert!(expired.contains(&part_id.as_str().to_string()));
}

#[tokio::test]
async fn protocol_head_rejects_expired_unfinished_final_upload() {
    let config = Config::default()
        .with_extension(Extension::Concatenation)
        .with_extension(Extension::ConcatenationUnfinished)
        .with_extension(Extension::Expiration)
        .expiration(StdDuration::from_secs(60));
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);

    let part_response = protocol
        .post(partial_post_headers(10), RequestBody::absent())
        .await
        .unwrap();
    let part_id = upload_id_from_location(&part_response);
    protocol
        .patch(
            patch_headers(0),
            &part_id,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"hello"))),
        )
        .await
        .unwrap();
    let final_response = protocol
        .post(final_post_headers(&[&part_id]), RequestBody::absent())
        .await
        .unwrap();
    let final_id = upload_id_from_location(&final_response);

    let final_state = state_store
        .get(final_id.as_str())
        .await
        .unwrap()
        .unwrap()
        .with_expiration(Utc::now() - ChronoDuration::minutes(1));
    state_store.set(&final_state, false).await.unwrap();

    let err = protocol.head(&final_id).await.unwrap_err();
    let expired = state_store.list_expired(Utc::now()).await.unwrap();

    assert!(matches!(err, Error::Expired(id) if id == final_id.as_str()));
    assert!(expired.contains(&final_id.as_str().to_string()));
}

#[tokio::test]
async fn protocol_head_rejects_planned_final_upload_with_expired_partial() {
    let config = Config::default()
        .with_extension(Extension::Concatenation)
        .with_extension(Extension::ConcatenationUnfinished)
        .with_extension(Extension::Expiration)
        .expiration(StdDuration::from_secs(60));
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);

    let part_response = protocol
        .post(partial_post_headers(10), RequestBody::absent())
        .await
        .unwrap();
    let part_id = upload_id_from_location(&part_response);
    protocol
        .patch(
            patch_headers(0),
            &part_id,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"hello"))),
        )
        .await
        .unwrap();
    let final_response = protocol
        .post(final_post_headers(&[&part_id]), RequestBody::absent())
        .await
        .unwrap();
    let final_id = upload_id_from_location(&final_response);

    let part_state = state_store
        .get(part_id.as_str())
        .await
        .unwrap()
        .unwrap()
        .with_expiration(Utc::now() - ChronoDuration::minutes(1));
    state_store.set(&part_state, false).await.unwrap();

    let err = protocol.head(&final_id).await.unwrap_err();

    assert!(matches!(err, Error::Expired(id) if id == final_id.as_str()));
}

#[tokio::test]
async fn protocol_head_rejects_planned_final_upload_with_reclaimed_partial() {
    let config = Config::default()
        .with_extension(Extension::Concatenation)
        .with_extension(Extension::ConcatenationUnfinished)
        .with_extension(Extension::Expiration)
        .expiration(StdDuration::from_secs(60));
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);

    let part_response = protocol
        .post(partial_post_headers(10), RequestBody::absent())
        .await
        .unwrap();
    let part_id = upload_id_from_location(&part_response);
    protocol
        .patch(
            patch_headers(0),
            &part_id,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"hello"))),
        )
        .await
        .unwrap();
    let final_response = protocol
        .post(final_post_headers(&[&part_id]), RequestBody::absent())
        .await
        .unwrap();
    let final_id = upload_id_from_location(&final_response);

    let part_state = state_store
        .get(part_id.as_str())
        .await
        .unwrap()
        .unwrap()
        .with_expiration(Utc::now() - ChronoDuration::minutes(1));
    state_store.set(&part_state, false).await.unwrap();
    let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
        .await
        .unwrap();
    assert_eq!(report.removed(), 1);

    let err = protocol.head(&final_id).await.unwrap_err();

    assert!(matches!(err, Error::Expired(id) if id == final_id.as_str()));
}

#[tokio::test]
async fn protocol_planned_final_upload_expires_with_earliest_partial() {
    let config = Config::default()
        .with_extension(Extension::Concatenation)
        .with_extension(Extension::ConcatenationUnfinished)
        .with_extension(Extension::Expiration)
        .expiration(StdDuration::from_secs(24 * 60 * 60));
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);

    let part_response = protocol
        .post(partial_post_headers(10), RequestBody::absent())
        .await
        .unwrap();
    let part_id = upload_id_from_location(&part_response);
    protocol
        .patch(
            patch_headers(0),
            &part_id,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"hello"))),
        )
        .await
        .unwrap();

    let part_expires = Utc::now() + ChronoDuration::hours(1);
    let part_state = state_store
        .get(part_id.as_str())
        .await
        .unwrap()
        .unwrap()
        .with_expiration(part_expires);
    state_store.set(&part_state, false).await.unwrap();

    let final_response = protocol
        .post(final_post_headers(&[&part_id]), RequestBody::absent())
        .await
        .unwrap();
    let final_id = upload_id_from_location(&final_response);
    let expected_expires = part_expires.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

    assert_eq!(
        final_response.headers.get("upload-expires").unwrap(),
        expected_expires.as_str()
    );

    let response = protocol.head(&final_id).await.unwrap();
    assert_eq!(
        response.headers.get("upload-expires").unwrap(),
        expected_expires.as_str()
    );

    let expired = state_store
        .list_expired(part_expires + ChronoDuration::seconds(1))
        .await
        .unwrap();
    assert!(expired.contains(&final_id.as_str().to_string()));
}

#[tokio::test]
async fn protocol_head_accepts_materialized_final_upload_after_partial_reclamation() {
    let config = Config::default()
        .with_extension(Extension::Concatenation)
        .with_extension(Extension::Expiration)
        .expiration(StdDuration::from_secs(60));
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);

    let part_response = protocol
        .post(partial_post_headers(5), RequestBody::absent())
        .await
        .unwrap();
    let part_id = upload_id_from_location(&part_response);
    protocol
        .patch(
            patch_headers(0),
            &part_id,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"hello"))),
        )
        .await
        .unwrap();
    let final_response = protocol
        .post(final_post_headers(&[&part_id]), RequestBody::absent())
        .await
        .unwrap();
    let final_id = upload_id_from_location(&final_response);

    let part_state = state_store
        .get(part_id.as_str())
        .await
        .unwrap()
        .unwrap()
        .with_expiration(Utc::now() - ChronoDuration::minutes(1));
    state_store.set(&part_state, false).await.unwrap();
    let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
        .await
        .unwrap();
    assert_eq!(report.removed(), 1);

    let response = protocol.head(&final_id).await.unwrap();

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
    assert_eq!(response.headers.get("upload-length").unwrap(), "5");
    assert_eq!(
        response.headers.get("upload-concat").unwrap(),
        format!("final;/files/{}", part_id.as_str()).as_str()
    );
}
