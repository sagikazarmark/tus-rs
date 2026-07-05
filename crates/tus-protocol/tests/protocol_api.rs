#![cfg(all(
    feature = "storage-memory",
    feature = "state-memory",
    not(target_arch = "wasm32")
))]

use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use http::{HeaderMap, HeaderValue, StatusCode};
use std::time::Duration as StdDuration;
use tus_protocol::state::memory::MemoryStateStore;
use tus_protocol::storage::memory::MemoryStorage;
use tus_protocol::{
    AppendRequest, ChunkStream, Config, Error, Extension, Headers, HookChain, HookContext,
    HookEvent, HookExecutor, HookRequestInfo, NoopHookExecutor, NoopLocker, PreHookResult,
    Protocol, RequestBody, Response, StateStore, Storage, TUS_SUCCESS_RESPONSE_HEADERS,
    UploadConcat, UploadId, UploadMetadata, UploadState, WriteMode, reclaim_expired_uploads,
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

fn assert_default_hook_rejection(err: Error) {
    match err {
        Error::HookRejected {
            status_code: 400,
            message,
        } if message.is_empty() => {}
        err => panic!("expected default hook rejection, got {err:?}"),
    }
}

fn assert_response_headers_covered_by_protocol_facts(response: &Response) {
    for name in response.headers.keys() {
        let name = name.as_str();
        if is_cors_safelisted_response_header(name) {
            continue;
        }

        assert!(
            TUS_SUCCESS_RESPONSE_HEADERS
                .iter()
                .any(|expected| expected.eq_ignore_ascii_case(name)),
            "{name} missing from TUS_SUCCESS_RESPONSE_HEADERS",
        );
    }
}

fn is_cors_safelisted_response_header(name: &str) -> bool {
    matches!(
        name,
        "cache-control"
            | "content-language"
            | "content-length"
            | "content-type"
            | "expires"
            | "last-modified"
            | "pragma"
    )
}

struct FinishSideEffectHook;

#[async_trait::async_trait]
impl HookExecutor for FinishSideEffectHook {
    async fn execute_pre(&self, ctx: &HookContext) -> tus_protocol::Result<PreHookResult> {
        if ctx.event != HookEvent::PreFinish {
            return Ok(PreHookResult::proceed());
        }

        let mut metadata = UploadMetadata::new();
        metadata.insert("finish", "ignored");

        Ok(PreHookResult::proceed_with_metadata(metadata).with_header("x-finish", "ignored"))
    }

    async fn execute_post(&self, _ctx: &HookContext) {}
}

#[test]
fn upload_defer_length_header_must_be_one() {
    let mut raw = HeaderMap::new();
    raw.insert("tus-resumable", HeaderValue::from_static("1.0.0"));
    raw.insert("upload-length", HeaderValue::from_static("5"));
    raw.insert("upload-defer-length", HeaderValue::from_static("0"));

    let err = Headers::from_headers(&raw).unwrap_err();

    assert!(matches!(
        err,
        Error::InvalidHeader {
            header: "Upload-Defer-Length",
            ..
        }
    ));
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
        .append(AppendRequest::new(
            handle,
            0,
            ChunkStream::from_bytes(Bytes::from_static(b"hello")),
            false,
        ))
        .await
        .unwrap();
    state.set_storage_handle(handle);

    state_store.set(&state, WriteMode::CreateNew).await.unwrap();

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
async fn protocol_success_response_headers_are_covered_by_header_facts() {
    let config = Config::default()
        .with_extension(Extension::CreationWithUpload)
        .with_extension(Extension::Concatenation)
        .with_expiration(StdDuration::from_secs(60))
        .with_max_size(1024);
    #[cfg(feature = "checksum")]
    let config = config.with_extension(Extension::Checksum);
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);

    assert_response_headers_covered_by_protocol_facts(&protocol.options());

    let mut creation_with_upload = post_headers_with_length(10);
    creation_with_upload.content_type = Some("application/offset+octet-stream".to_string());
    creation_with_upload.content_length = Some(5);
    let post_response = protocol
        .post(
            creation_with_upload,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"hello"))),
        )
        .await
        .unwrap();
    assert_response_headers_covered_by_protocol_facts(&post_response);
    let upload_id = upload_id_from_location(&post_response);

    let head_response = protocol.head(&upload_id).await.unwrap();
    assert_response_headers_covered_by_protocol_facts(&head_response);

    let patch_response = protocol
        .patch(
            patch_headers(5),
            &upload_id,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"world"))),
        )
        .await
        .unwrap();
    assert_response_headers_covered_by_protocol_facts(&patch_response);

    let mut deferred_headers = Headers::default();
    deferred_headers.upload_defer_length = true;
    let deferred_response = protocol
        .post(deferred_headers, RequestBody::absent())
        .await
        .unwrap();
    let deferred_id = upload_id_from_location(&deferred_response);
    let deferred_head = protocol.head(&deferred_id).await.unwrap();
    assert_response_headers_covered_by_protocol_facts(&deferred_head);

    let partial_response = protocol
        .post(partial_post_headers(1), RequestBody::absent())
        .await
        .unwrap();
    let partial_id = upload_id_from_location(&partial_response);
    let partial_head = protocol.head(&partial_id).await.unwrap();
    assert_response_headers_covered_by_protocol_facts(&partial_head);

    let mut metadata = UploadMetadata::new();
    metadata.insert("filename".to_string(), "test.txt");
    let mut metadata_headers = Headers::default();
    metadata_headers.upload_length = Some(5);
    metadata_headers.upload_metadata = Some(metadata);
    let metadata_response = protocol
        .post(metadata_headers, RequestBody::absent())
        .await
        .unwrap();
    let metadata_id = upload_id_from_location(&metadata_response);
    let metadata_head = protocol.head(&metadata_id).await.unwrap();
    assert_response_headers_covered_by_protocol_facts(&metadata_head);
}

#[tokio::test]
async fn protocol_can_opt_into_rejecting_standard_empty_creation_requests() {
    let config = Config::default().without_empty_creation();
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);

    let err = protocol
        .post(post_headers_with_length(5), RequestBody::absent())
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        Error::InvalidHeader {
            header: "Upload-Length",
            ..
        }
    ));
}

#[tokio::test]
async fn protocol_pre_hook_rejections_use_default_status_and_message() {
    let create_config = Config::default();
    let create_storage = MemoryStorage::new();
    let create_state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let create_hooks = HookChain::new().on_pre_create(|_| async { Ok(PreHookResult::default()) });
    let protocol = Protocol::new(
        &create_config,
        &create_storage,
        &create_state_store,
        &locker,
        &create_hooks,
    );

    let err = protocol
        .post(post_headers_with_length(5), RequestBody::absent())
        .await
        .unwrap_err();

    assert_default_hook_rejection(err);

    let config = Config::default();
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let noop_hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &noop_hooks);
    let post_response = protocol
        .post(post_headers_with_length(5), RequestBody::absent())
        .await
        .unwrap();
    let upload_id = upload_id_from_location(&post_response);

    let receive_hooks = HookChain::new().on_pre_receive(|_| async { Ok(PreHookResult::default()) });
    let err = Protocol::new(&config, &storage, &state_store, &locker, &receive_hooks)
        .patch(
            patch_headers(0),
            &upload_id,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"h"))),
        )
        .await
        .unwrap_err();

    assert_default_hook_rejection(err);

    let finish_hooks = HookChain::new().on_pre_finish(|_| async { Ok(PreHookResult::default()) });
    let err = Protocol::new(&config, &storage, &state_store, &locker, &finish_hooks)
        .patch(
            patch_headers(0),
            &upload_id,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"hello"))),
        )
        .await
        .unwrap_err();

    assert_default_hook_rejection(err);
    // PreFinish runs after the completing bytes are durably committed; the
    // rejection fails the response but the stored upload stays complete.
    let stored = state_store.get(upload_id.as_str()).await.unwrap().unwrap();
    assert_eq!(stored.offset(), 5);

    let terminate_config = Config::default().with_extension(Extension::Termination);
    let terminate_hooks =
        HookChain::new().on_pre_terminate(|_| async { Ok(PreHookResult::default()) });
    let err = Protocol::new(
        &terminate_config,
        &storage,
        &state_store,
        &locker,
        &terminate_hooks,
    )
    .delete(Headers::default(), &upload_id)
    .await
    .unwrap_err();

    assert_default_hook_rejection(err);
    assert!(state_store.get(upload_id.as_str()).await.unwrap().is_some());
}

#[tokio::test]
async fn protocol_pre_finish_does_not_apply_metadata_or_response_headers() {
    let config = Config::default();
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let noop_hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &noop_hooks);
    let post_response = protocol
        .post(post_headers_with_length(5), RequestBody::absent())
        .await
        .unwrap();
    let upload_id = upload_id_from_location(&post_response);

    let response = Protocol::new(
        &config,
        &storage,
        &state_store,
        &locker,
        &FinishSideEffectHook,
    )
    .patch(
        patch_headers(0),
        &upload_id,
        RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"hello"))),
    )
    .await
    .unwrap();

    assert_eq!(response.status, StatusCode::NO_CONTENT);
    assert!(response.headers.get("x-finish").is_none());
    let stored = state_store.get(upload_id.as_str()).await.unwrap().unwrap();
    assert!(stored.is_complete());
    assert!(stored.metadata().get("finish").is_none());
}

#[tokio::test]
async fn protocol_pre_terminate_response_headers_are_returned() {
    let config = Config::default().with_extension(Extension::Termination);
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let noop_hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &noop_hooks);
    let post_response = protocol
        .post(post_headers_with_length(5), RequestBody::absent())
        .await
        .unwrap();
    let upload_id = upload_id_from_location(&post_response);
    let hooks = HookChain::new()
        .on_pre_terminate(|_| async { Ok(PreHookResult::proceed().with_header("x-delete", "ok")) });

    let response = Protocol::new(&config, &storage, &state_store, &locker, &hooks)
        .delete(Headers::default(), &upload_id)
        .await
        .unwrap();

    assert_eq!(response.status, StatusCode::NO_CONTENT);
    assert_eq!(response.headers.get("x-delete").unwrap(), "ok");
    assert!(state_store.get(upload_id.as_str()).await.unwrap().is_none());
}

#[tokio::test]
async fn protocol_head_accepts_expired_completed_regular_upload() {
    let config = Config::default()
        .with_extension(Extension::Expiration)
        .with_expiration(StdDuration::from_secs(60));
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
    state_store.set(&state, WriteMode::Update).await.unwrap();

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
        .append(AppendRequest::new(
            handle,
            0,
            ChunkStream::from_bytes(Bytes::from_static(b"hello")),
            true,
        ))
        .await
        .unwrap();
    state.set_storage_handle(handle);
    state_store.set(&state, WriteMode::CreateNew).await.unwrap();

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
        .with_expiration(StdDuration::from_secs(60));
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
        .with_expiration(StdDuration::from_secs(60));
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
    state_store
        .set(&final_state, WriteMode::Update)
        .await
        .unwrap();

    let response = protocol.head(&final_id).await.unwrap();
    let expired = state_store.list_expired(Utc::now()).await.unwrap();

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
    assert_eq!(response.headers.get("upload-length").unwrap(), "5");
    assert!(response.headers.get("upload-expires").is_none());
    assert!(!expired.contains(&final_id.as_str().to_string()));
}

#[tokio::test]
async fn protocol_head_reports_exact_final_upload_concat_parts_in_order() {
    let config = Config::default().with_extension(Extension::Concatenation);
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = NoopLocker::new();
    let hooks = NoopHookExecutor::new();
    let protocol = Protocol::new(&config, &storage, &state_store, &locker, &hooks);

    let first_part = upload_id_from_location(
        &protocol
            .post(partial_post_headers(2), RequestBody::absent())
            .await
            .unwrap(),
    );
    protocol
        .patch(
            patch_headers(0),
            &first_part,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"ab"))),
        )
        .await
        .unwrap();

    let second_part = upload_id_from_location(
        &protocol
            .post(partial_post_headers(3), RequestBody::absent())
            .await
            .unwrap(),
    );
    protocol
        .patch(
            patch_headers(0),
            &second_part,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"cde"))),
        )
        .await
        .unwrap();

    let final_response = protocol
        .post(
            final_post_headers(&[&first_part, &second_part]),
            RequestBody::absent(),
        )
        .await
        .unwrap();
    let final_id = upload_id_from_location(&final_response);

    let response = protocol.head(&final_id).await.unwrap();

    assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
    assert_eq!(response.headers.get("upload-length").unwrap(), "5");
    assert_eq!(
        response.headers.get("upload-concat").unwrap(),
        format!(
            "final;/files/{} /files/{}",
            first_part.as_str(),
            second_part.as_str()
        )
        .as_str()
    );
}

#[tokio::test]
async fn protocol_head_rejects_expired_completed_partial_upload() {
    let config = Config::default()
        .with_extension(Extension::Concatenation)
        .with_extension(Extension::Expiration)
        .with_expiration(StdDuration::from_secs(60));
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
    state_store
        .set(&part_state, WriteMode::Update)
        .await
        .unwrap();

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
        .with_expiration(StdDuration::from_secs(60));
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
    state_store
        .set(&final_state, WriteMode::Update)
        .await
        .unwrap();

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
        .with_expiration(StdDuration::from_secs(60));
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
    state_store
        .set(&part_state, WriteMode::Update)
        .await
        .unwrap();

    let err = protocol.head(&final_id).await.unwrap_err();

    assert!(matches!(err, Error::Expired(id) if id == final_id.as_str()));
}

#[tokio::test]
async fn protocol_head_rejects_planned_final_upload_with_reclaimed_partial() {
    let config = Config::default()
        .with_extension(Extension::Concatenation)
        .with_extension(Extension::ConcatenationUnfinished)
        .with_extension(Extension::Expiration)
        .with_expiration(StdDuration::from_secs(60));
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
    state_store
        .set(&part_state, WriteMode::Update)
        .await
        .unwrap();
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
        .with_expiration(StdDuration::from_secs(24 * 60 * 60));
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
    state_store
        .set(&part_state, WriteMode::Update)
        .await
        .unwrap();

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
        .with_expiration(StdDuration::from_secs(60));
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
    state_store
        .set(&part_state, WriteMode::Update)
        .await
        .unwrap();
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
