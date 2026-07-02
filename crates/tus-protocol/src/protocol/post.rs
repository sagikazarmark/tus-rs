//! Core POST handler (TUS Creation + related extensions).

use http::StatusCode;

use crate::config::Extension;
use crate::error::Error;
use crate::hooks::{HookEvent, HookExecutor, execute_post_best_effort};
use crate::lifecycle::{
    CreationRequest, CreationTransition, FinalUploadMaterializer, ReceiveBodyKind,
    commit_receive_body, prepare_creation, prepare_receive_body, run_post_finish_best_effort,
};
use crate::locking::Locker;
use crate::state::{StateStore, UploadState};
use crate::storage::Storage;

use super::body::RequestBody;
use super::hook_context::{HookContextBuilder, HookRequestFacts};
use super::{Headers, Protocol, Response};

/// Creates a new upload.
///
/// Covers the Creation, Creation-With-Upload, Creation-Defer-Length, and
/// Concatenation extensions depending on which are enabled in `config`.
impl<'a, S, I, L, H> Protocol<'a, S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    /// Creates a new upload.
    ///
    /// Covers the Creation, Creation-With-Upload, Creation-Defer-Length, and
    /// Concatenation extensions depending on which are enabled in the config.
    ///
    /// # Errors
    ///
    /// Returns an error if required extensions are disabled, mandatory headers
    /// are missing or invalid, the declared size exceeds configuration limits,
    /// hooks reject the request, or storage/state persistence fails.
    pub async fn post(&self, headers: Headers, body: RequestBody) -> Result<Response, Error> {
        let hook_contexts = HookContextBuilder::new(self.config, HookRequestFacts::post(&headers));
        let has_body = body.is_supplied();
        if has_body {
            headers.validate_patch_content_type()?;
        }

        let transition = prepare_creation(
            self.config,
            CreationRequest::from_headers(&headers, has_body),
        )?;

        let mut state = match transition {
            CreationTransition::Upload(state) => state,
            CreationTransition::Final { state, part_urls } => {
                return self
                    .create_final_upload(&headers, &hook_contexts, state, part_urls)
                    .await;
            }
        };

        let hook_ctx = hook_contexts.context(HookEvent::PreCreate, state.clone());
        let pre_result = self.hooks.execute_pre(&hook_ctx).await?;

        if !pre_result.proceed {
            return Err(Error::HookRejected {
                status_code: pre_result.reject_status.unwrap_or(400),
                message: pre_result.reject_message.unwrap_or_default(),
            });
        }

        let mut response_headers = pre_result.response_headers;

        if let Some(metadata) = pre_result.metadata {
            state.set_metadata(metadata);
        }

        // Creation-With-Upload path
        let is_creation_with_upload = has_body
            && self.config.has_extension(Extension::CreationWithUpload)
            && headers
                .content_type
                .as_deref()
                .map(|ct| ct.starts_with("application/offset+octet-stream"))
                .unwrap_or(false);
        let creation_body = if is_creation_with_upload {
            let prepared = prepare_receive_body(
                self.config,
                self.hooks,
                hook_contexts.request_info(),
                &headers,
                &mut state,
                body,
                ReceiveBodyKind::CreationWithUpload,
            )
            .await?;
            response_headers.extend(prepared.response_headers.clone());
            Some(prepared)
        } else {
            None
        };

        let handle = self.storage.create(state.id()).await?;
        state.set_storage_handle(handle);
        self.state_store.set(&state, true).await?;

        if let Some(prepared_body) = creation_body {
            if let Err(e) =
                commit_receive_body(self.storage, self.state_store, &mut state, prepared_body).await
            {
                // Something in the body-write phase (storage append or state
                // persistence) failed. Roll back the just-created state record and
                // storage object so the upload ID does not survive as a zombie
                // record.
                if let Some(handle) = state.storage_handle() {
                    let _ = self.storage.delete(&handle).await;
                }
                let _ = self.state_store.delete(state.id()).await;
                return Err(e);
            }

            let post_ctx = hook_contexts.context(HookEvent::PostCreate, state.clone());
            execute_post_best_effort(self.hooks, &post_ctx).await;

            let post_receive_ctx = hook_contexts.context(HookEvent::PostReceive, state.clone());
            execute_post_best_effort(self.hooks, &post_receive_ctx).await;

            if state.is_complete() {
                run_post_finish_best_effort(self.hooks, hook_contexts.request_info(), &state).await;
            }
        } else {
            let post_ctx = hook_contexts.context(HookEvent::PostCreate, state.clone());
            execute_post_best_effort(self.hooks, &post_ctx).await;
        }

        let location = self
            .config
            .upload_url(state.id(), headers.base_url(self.config).as_deref());
        let mut response = Response::new(StatusCode::CREATED).with_header("location", &location);

        if is_creation_with_upload {
            response = response.with_header("upload-offset", state.offset().to_string());
        }

        if self.config.has_extension(Extension::Expiration)
            && let Some(expires) = state.expires_header()
        {
            response = response.with_header("upload-expires", &expires);
        }

        for (name, value) in response_headers {
            response = response.with_header_owned(name, value);
        }

        Ok(response)
    }
}

/// Handles final-upload creation and response mapping.
impl<'a, S, I, L, H> Protocol<'a, S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    async fn create_final_upload(
        &self,
        headers: &Headers,
        hook_contexts: &HookContextBuilder,
        state: UploadState,
        part_urls: Vec<String>,
    ) -> Result<Response, Error> {
        let materializer = FinalUploadMaterializer::new(
            self.storage,
            self.state_store,
            self.hooks,
            self.config,
            hook_contexts.request_info(),
        );
        let created = materializer.create(state, part_urls).await?;
        let facts = created.response_facts();

        let location = self
            .config
            .upload_url(created.state.id(), headers.base_url(self.config).as_deref());
        let mut response = Response::new(StatusCode::CREATED).with_header("location", &location);

        if let Some(offset) = facts.offset {
            response = response.with_header("upload-offset", offset.to_string());
        }

        if let Some(length) = facts.length {
            response = response.with_header("upload-length", length.to_string());
        }

        if self.config.has_extension(Extension::Expiration)
            && let Some(expires) = created.state.expires_header()
        {
            response = response.with_header("upload-expires", &expires);
        }

        for (name, value) in created.response_headers {
            response = response.with_header_owned(name, value);
        }

        Ok(response)
    }
}

#[cfg(all(
    test,
    feature = "storage-memory",
    feature = "state-memory",
    not(feature = "local-futures")
))]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::extensions::UploadConcat;
    use crate::hooks::{HookChain, HookContext, HookExecutor, NoopHookExecutor, PreHookResult};
    use crate::locking::NoopLocker;
    use crate::state::UploadMetadata;
    use crate::state::memory::MemoryStateStore;
    use crate::storage::{AppendRequest, ChunkStream, StorageHandle, memory::MemoryStorage};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    struct FailingPostHookExecutor;

    #[derive(Clone, Default)]
    struct RecordingHookExecutor {
        events: Arc<Mutex<Vec<HookEvent>>>,
        offsets: Arc<Mutex<Vec<(HookEvent, u64)>>>,
    }

    impl RecordingHookExecutor {
        fn events(&self) -> Vec<HookEvent> {
            self.events.lock().unwrap().clone()
        }

        fn offsets(&self) -> Vec<(HookEvent, u64)> {
            self.offsets.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl HookExecutor for RecordingHookExecutor {
        async fn execute_pre(&self, ctx: &HookContext) -> crate::Result<PreHookResult> {
            self.events.lock().unwrap().push(ctx.event);
            self.offsets
                .lock()
                .unwrap()
                .push((ctx.event, ctx.upload.offset()));
            Ok(PreHookResult::proceed())
        }

        async fn execute_post(&self, ctx: &HookContext) -> crate::Result<()> {
            self.events.lock().unwrap().push(ctx.event);
            self.offsets
                .lock()
                .unwrap()
                .push((ctx.event, ctx.upload.offset()));
            Ok(())
        }
    }

    struct FailingAppendStorage {
        deleted: Arc<AtomicBool>,
    }

    impl FailingAppendStorage {
        fn new() -> Self {
            Self {
                deleted: Arc::new(AtomicBool::new(false)),
            }
        }

        fn deleted(&self) -> bool {
            self.deleted.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Storage for FailingAppendStorage {
        fn name(&self) -> &'static str {
            "failing-append"
        }

        async fn create(&self, upload_id: &str) -> crate::Result<crate::storage::StorageHandle> {
            Ok(crate::storage::StorageHandle::new(upload_id))
        }

        async fn append(
            &self,
            _request: AppendRequest,
        ) -> crate::Result<crate::storage::StorageHandle> {
            Err(Error::Storage(Box::new(std::io::Error::other(
                "append failed",
            ))))
        }

        async fn concat(
            &self,
            _request: crate::storage::ConcatRequest,
        ) -> crate::Result<crate::storage::StorageHandle> {
            Err(Error::Storage(Box::new(std::io::Error::other(
                "concat failed",
            ))))
        }

        async fn delete(&self, _handle: &crate::storage::StorageHandle) -> crate::Result<()> {
            self.deleted.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn size(
            &self,
            _handle: &crate::storage::StorageHandle,
        ) -> crate::Result<Option<u64>> {
            Ok(None)
        }
    }

    #[async_trait::async_trait]
    impl HookExecutor for FailingPostHookExecutor {
        async fn execute_pre(&self, _ctx: &HookContext) -> crate::Result<PreHookResult> {
            Ok(PreHookResult::proceed())
        }

        async fn execute_post(&self, _ctx: &HookContext) -> crate::Result<()> {
            Err(Error::hook(std::io::Error::other("post hook failed")))
        }
    }

    fn headers_with_length(length: u64) -> Headers {
        Headers {
            upload_length: Some(length),
            ..Default::default()
        }
    }

    async fn call(
        config: &Config,
        storage: &MemoryStorage,
        state_store: &MemoryStateStore,
        headers: Headers,
        body: RequestBody,
    ) -> Result<Response, Error> {
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        Protocol::new(config, storage, state_store, &locker, &hooks)
            .post(headers, body)
            .await
    }

    async fn create_storage(storage: &MemoryStorage, state: &mut UploadState) {
        let handle = storage.create(state.id()).await.unwrap();
        state.set_storage_handle(handle);
    }

    async fn append_storage(storage: &MemoryStorage, state: &mut UploadState, data: Bytes) {
        let projected_offset = state.offset().saturating_add(data.len() as u64);
        let completes_upload = state
            .length()
            .is_some_and(|length| projected_offset == length);
        let handle = storage
            .append(AppendRequest {
                handle: state.require_storage_handle().unwrap(),
                expected_offset: state.offset(),
                data: ChunkStream::from_bytes(data),
                completes_upload,
            })
            .await
            .unwrap();
        state.set_storage_handle(handle);
        state.set_offset(projected_offset);
    }

    #[tokio::test]
    async fn basic_create() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();

        let response = call(
            &Config::default(),
            &storage,
            &store,
            headers_with_length(1000),
            RequestBody::absent(),
        )
        .await
        .unwrap();

        assert_eq!(response.status, StatusCode::CREATED);
        assert!(response.headers.get("location").is_some());

        let location = response.headers.get("location").unwrap().to_str().unwrap();
        let id = location.rsplit('/').next().unwrap();
        let stored = store.get(id).await.unwrap().unwrap();
        assert_eq!(stored.length(), Some(1000));
    }

    #[tokio::test]
    async fn creation_extension_required() {
        let config = Config::default().without_extension(Extension::Creation);
        let err = call(
            &config,
            &MemoryStorage::new(),
            &MemoryStateStore::new(),
            headers_with_length(1000),
            RequestBody::absent(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::ExtensionNotSupported(_)));
    }

    #[tokio::test]
    async fn size_exceeded() {
        let config = Config::default().max_size(500);
        let err = call(
            &config,
            &MemoryStorage::new(),
            &MemoryStateStore::new(),
            headers_with_length(1000),
            RequestBody::absent(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::SizeExceeded { .. }));
    }

    #[tokio::test]
    async fn defer_length() {
        let headers = Headers {
            upload_defer_length: true,
            ..Default::default()
        };
        let response = call(
            &Config::default(),
            &MemoryStorage::new(),
            &MemoryStateStore::new(),
            headers,
            RequestBody::absent(),
        )
        .await
        .unwrap();
        assert_eq!(response.status, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn deferred_length_respects_allow_empty_creation() {
        let headers = Headers {
            upload_defer_length: true,
            ..Default::default()
        };

        let err = call(
            &Config::default().allow_empty_creation(false),
            &MemoryStorage::new(),
            &MemoryStateStore::new(),
            headers,
            RequestBody::absent(),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::ExtensionNotSupported(ext) if ext == "creation-defer-length"));
    }

    #[tokio::test]
    async fn fixed_length_creation_respects_allow_empty_creation() {
        let err = call(
            &Config::default().allow_empty_creation(false),
            &MemoryStorage::new(),
            &MemoryStateStore::new(),
            headers_with_length(100),
            RequestBody::absent(),
        )
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
    async fn creation_with_upload_is_allowed_when_empty_creation_disabled() {
        let config = Config::default()
            .allow_empty_creation(false)
            .with_extension(Extension::CreationWithUpload);
        let body_data = Bytes::from_static(b"Hello");
        let headers = Headers {
            upload_length: Some(5),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };

        let response = call(
            &config,
            &MemoryStorage::new(),
            &MemoryStateStore::new(),
            headers,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(body_data)),
        )
        .await
        .unwrap();

        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
    }

    #[tokio::test]
    async fn deferred_creation_with_upload_is_allowed_when_empty_creation_disabled() {
        let config = Config::default()
            .allow_empty_creation(false)
            .with_extension(Extension::CreationDeferLength)
            .with_extension(Extension::CreationWithUpload);
        let body_data = Bytes::from_static(b"Hello");
        let headers = Headers {
            upload_defer_length: true,
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };
        let store = MemoryStateStore::new();

        let response = call(
            &config,
            &MemoryStorage::new(),
            &store,
            headers,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(body_data)),
        )
        .await
        .unwrap();

        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
        let location = response.headers.get("location").unwrap().to_str().unwrap();
        let id = location.rsplit('/').next().unwrap();
        let stored = store.get(id).await.unwrap().unwrap();
        assert_eq!(stored.offset(), 5);
        assert_eq!(stored.length(), None);
    }

    #[tokio::test]
    async fn missing_length_rejected() {
        let err = call(
            &Config::default(),
            &MemoryStorage::new(),
            &MemoryStateStore::new(),
            Headers::default(),
            RequestBody::absent(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::MissingHeader(_)));
    }

    #[tokio::test]
    async fn length_and_defer_length_mutually_exclusive() {
        let headers = Headers {
            upload_length: Some(1000),
            upload_defer_length: true,
            ..Default::default()
        };
        let err = call(
            &Config::default(),
            &MemoryStorage::new(),
            &MemoryStateStore::new(),
            headers,
            RequestBody::absent(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::InvalidHeader { .. }));
    }

    #[tokio::test]
    async fn metadata_is_persisted() {
        let store = MemoryStateStore::new();
        let mut metadata = UploadMetadata::new();
        metadata.insert("filename".to_string(), "test.txt");

        let headers = Headers {
            upload_length: Some(1000),
            upload_metadata: Some(metadata),
            ..Default::default()
        };

        let response = call(
            &Config::default(),
            &MemoryStorage::new(),
            &store,
            headers,
            RequestBody::absent(),
        )
        .await
        .unwrap();

        let location = response.headers.get("location").unwrap().to_str().unwrap();
        let id = location.rsplit('/').next().unwrap();
        let stored = store.get(id).await.unwrap().unwrap();
        assert_eq!(
            stored.metadata().get("filename").and_then(|v| v.as_str()),
            Some("test.txt")
        );
    }

    #[tokio::test]
    async fn pre_create_can_replace_user_metadata() {
        let store = MemoryStateStore::new();
        let storage = MemoryStorage::new();
        let locker = NoopLocker::new();
        let hooks = HookChain::new().on_pre_create(|_| async {
            let mut metadata = UploadMetadata::new();
            metadata.insert("filename", "hook.txt");
            Ok(PreHookResult::proceed_with_metadata(metadata))
        });

        let response = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .post(headers_with_length(1000), RequestBody::absent())
            .await
            .unwrap();

        let location = response.headers.get("location").unwrap().to_str().unwrap();
        let id = location.rsplit('/').next().unwrap();
        let stored = store.get(id).await.unwrap().unwrap();
        assert_eq!(
            stored.metadata().get("filename").and_then(|v| v.as_str()),
            Some("hook.txt")
        );
    }

    #[tokio::test]
    async fn pre_create_response_headers_are_returned() {
        let store = MemoryStateStore::new();
        let storage = MemoryStorage::new();
        let locker = NoopLocker::new();
        let hooks = HookChain::new().on_pre_create(|_| async {
            Ok(PreHookResult::proceed().with_header("x-hook", "created"))
        });

        let response = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .post(headers_with_length(1000), RequestBody::absent())
            .await
            .unwrap();

        assert_eq!(response.headers.get("x-hook").unwrap(), "created");
    }

    #[tokio::test]
    async fn post_create_hook_failure_after_commit_is_swallowed() {
        let store = MemoryStateStore::new();
        let storage = MemoryStorage::new();
        let locker = NoopLocker::new();
        let hooks = FailingPostHookExecutor;

        let response = Protocol::new(&Config::default(), &storage, &store, &locker, &hooks)
            .post(headers_with_length(1000), RequestBody::absent())
            .await
            .unwrap();

        let location = response.headers.get("location").unwrap().to_str().unwrap();
        let id = location.rsplit('/').next().unwrap();

        assert_eq!(response.status, StatusCode::CREATED);
        assert!(store.get(id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn partial_upload() {
        let config = Config::default().with_extension(Extension::Concatenation);
        let store = MemoryStateStore::new();
        let headers = Headers {
            upload_length: Some(100),
            upload_concat: Some(UploadConcat::Partial),
            ..Default::default()
        };
        let response = call(
            &config,
            &MemoryStorage::new(),
            &store,
            headers,
            RequestBody::absent(),
        )
        .await
        .unwrap();
        let location = response.headers.get("location").unwrap().to_str().unwrap();
        let id = location.rsplit('/').next().unwrap();
        let stored = store.get(id).await.unwrap().unwrap();
        assert!(stored.is_partial());
    }

    #[tokio::test]
    async fn concat_extension_disabled() {
        let headers = Headers {
            upload_length: Some(100),
            upload_concat: Some(UploadConcat::Partial),
            ..Default::default()
        };
        let err = call(
            &Config::default(),
            &MemoryStorage::new(),
            &MemoryStateStore::new(),
            headers,
            RequestBody::absent(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::ExtensionNotSupported(_)));
    }

    #[tokio::test]
    async fn final_upload_without_upload_length() {
        let config = Config::default().with_extension(Extension::Concatenation);
        let store = MemoryStateStore::new();
        let storage = MemoryStorage::new();

        // Seed two complete partial uploads (in both state store and storage).
        let mut part1 = UploadState::new("part1").with_length(50).as_partial();
        create_storage(&storage, &mut part1).await;
        append_storage(&storage, &mut part1, Bytes::copy_from_slice(&[0u8; 50])).await;
        store.set(&part1, true).await.unwrap();

        let mut part2 = UploadState::new("part2").with_length(50).as_partial();
        create_storage(&storage, &mut part2).await;
        append_storage(&storage, &mut part2, Bytes::copy_from_slice(&[0u8; 50])).await;
        store.set(&part2, true).await.unwrap();

        let headers = Headers {
            upload_concat: Some(UploadConcat::Final(vec![
                "/files/part1".to_string(),
                "/files/part2".to_string(),
            ])),
            ..Default::default()
        };

        let response = call(&config, &storage, &store, headers, RequestBody::absent())
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(response.headers.get("upload-length").unwrap(), "100");
        assert_eq!(response.headers.get("upload-offset").unwrap(), "100");
    }

    #[tokio::test]
    async fn pre_finish_rejection_blocks_completed_final_upload() {
        let config = Config::default().with_extension(Extension::Concatenation);
        let store = MemoryStateStore::new();
        let storage = MemoryStorage::new();

        let mut part = UploadState::new("part1").with_length(5).as_partial();
        create_storage(&storage, &mut part).await;
        append_storage(&storage, &mut part, Bytes::from_static(b"Hello")).await;
        store.set(&part, true).await.unwrap();

        let headers = Headers {
            upload_concat: Some(UploadConcat::Final(vec!["/files/part1".to_string()])),
            ..Default::default()
        };
        let locker = NoopLocker::new();
        let hooks = HookChain::new()
            .on_pre_finish(|_| async { Ok(PreHookResult::reject(403, "finish blocked")) });

        let err = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .post(headers, RequestBody::absent())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::HookRejected {
                status_code: 403,
                ..
            }
        ));
        assert!(store.get("part1").await.unwrap().is_some());
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn final_upload_rejects_incomplete_parts() {
        let config = Config::default().with_extension(Extension::Concatenation);
        let store = MemoryStateStore::new();

        let mut part1 = UploadState::new("part1").with_length(50).as_partial();
        part1.set_offset(25);
        part1.set_storage_handle(StorageHandle::new("uploads/part1"));
        store.set(&part1, true).await.unwrap();

        let headers = Headers {
            upload_concat: Some(UploadConcat::Final(vec!["/files/part1".to_string()])),
            ..Default::default()
        };

        let err = call(
            &config,
            &MemoryStorage::new(),
            &store,
            headers,
            RequestBody::absent(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::IncompleteUpload(_)));
    }

    #[tokio::test]
    async fn unfinished_final_with_deferred_part_keeps_length_unknown() {
        let config = Config::default()
            .with_extension(Extension::Concatenation)
            .with_extension(Extension::ConcatenationUnfinished);
        let store = MemoryStateStore::new();
        let storage = MemoryStorage::new();

        let mut part = UploadState::new("part1").as_partial();
        part.set_offset(5);
        store.set(&part, true).await.unwrap();

        let headers = Headers {
            upload_concat: Some(UploadConcat::Final(vec!["/files/part1".to_string()])),
            ..Default::default()
        };

        let response = call(&config, &storage, &store, headers, RequestBody::absent())
            .await
            .unwrap();

        assert!(response.headers.get("upload-length").is_none());
        assert!(response.headers.get("upload-offset").is_none());

        let final_id = response
            .headers
            .get("location")
            .unwrap()
            .to_str()
            .unwrap()
            .rsplit('/')
            .next()
            .unwrap();
        let stored = store.get(final_id).await.unwrap().unwrap();
        assert_eq!(stored.length(), None);
        assert_eq!(stored.offset(), 5);
        assert!(!stored.is_complete());
    }

    #[tokio::test]
    async fn creation_with_upload_writes_bytes() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let store = MemoryStateStore::new();
        let body_data: &[u8] = b"Hello World";
        let body = RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::copy_from_slice(
            body_data,
        )));
        let headers = Headers {
            upload_length: Some(body_data.len() as u64),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(body_data.len() as u64),
            ..Default::default()
        };

        let response = call(&config, &MemoryStorage::new(), &store, headers, body)
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(
            response.headers.get("upload-offset").unwrap(),
            body_data.len().to_string().as_str()
        );

        let location = response.headers.get("location").unwrap().to_str().unwrap();
        let id = location.rsplit('/').next().unwrap();
        let stored = store.get(id).await.unwrap().unwrap();
        assert!(stored.is_complete());
    }

    #[tokio::test]
    async fn pre_finish_rejection_blocks_completed_creation_with_upload() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let locker = NoopLocker::new();
        let hooks = HookChain::new()
            .on_pre_finish(|_| async { Ok(PreHookResult::reject(403, "finish blocked")) });
        let headers = Headers {
            upload_length: Some(5),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };

        let err = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .post(
                headers,
                RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(
                    b"Hello",
                ))),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::HookRejected {
                status_code: 403,
                ..
            }
        ));
        assert!(store.is_empty());
        assert!(storage.is_empty());
    }

    #[tokio::test]
    async fn creation_with_upload_partial_bytes() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let store = MemoryStateStore::new();
        let body_data: &[u8] = b"Hello";
        let body = RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::copy_from_slice(
            body_data,
        )));
        let headers = Headers {
            upload_length: Some(100),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(body_data.len() as u64),
            ..Default::default()
        };

        let response = call(&config, &MemoryStorage::new(), &store, headers, body)
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(
            response.headers.get("upload-offset").unwrap(),
            body_data.len().to_string().as_str()
        );

        let location = response.headers.get("location").unwrap().to_str().unwrap();
        let id = location.rsplit('/').next().unwrap();
        let stored = store.get(id).await.unwrap().unwrap();
        assert!(!stored.is_complete());
    }

    #[tokio::test]
    async fn creation_with_upload_fires_receive_hooks_for_initial_body() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let locker = NoopLocker::new();
        let hooks = RecordingHookExecutor::default();
        let headers = Headers {
            upload_length: Some(100),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };

        let response = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .post(
                headers,
                RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(
                    b"Hello",
                ))),
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(
            hooks.events(),
            vec![
                HookEvent::PreCreate,
                HookEvent::PreReceive,
                HookEvent::PostCreate,
                HookEvent::PostReceive,
            ]
        );
    }

    #[tokio::test]
    async fn creation_with_upload_post_create_observes_committed_initial_body() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let locker = NoopLocker::new();
        let hooks = RecordingHookExecutor::default();
        let headers = Headers {
            upload_length: Some(100),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };

        Protocol::new(&config, &storage, &store, &locker, &hooks)
            .post(
                headers,
                RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(
                    b"Hello",
                ))),
            )
            .await
            .unwrap();

        assert!(
            hooks.offsets().contains(&(HookEvent::PostCreate, 5)),
            "PostCreate should observe the committed initial body offset"
        );
    }

    #[tokio::test]
    async fn creation_with_upload_pre_receive_metadata_wins_after_pre_create() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let locker = NoopLocker::new();
        let hooks = HookChain::new()
            .on_pre_create(|_| async {
                let mut metadata = UploadMetadata::new();
                metadata.insert("stage", "created");
                Ok(PreHookResult::proceed_with_metadata(metadata))
            })
            .on_pre_receive(|ctx| async move {
                if ctx.upload.metadata().get("stage").and_then(|v| v.as_str()) != Some("created") {
                    return Ok(PreHookResult::reject(409, "pre-create metadata missing"));
                }

                let mut metadata = UploadMetadata::new();
                metadata.insert("stage", "received");
                Ok(PreHookResult::proceed_with_metadata(metadata))
            });
        let headers = Headers {
            upload_length: Some(100),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };

        let response = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .post(
                headers,
                RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(
                    b"Hello",
                ))),
            )
            .await
            .unwrap();

        let location = response.headers.get("location").unwrap().to_str().unwrap();
        let id = location.rsplit('/').next().unwrap();
        let stored = store.get(id).await.unwrap().unwrap();
        assert_eq!(
            stored.metadata().get("stage").and_then(|v| v.as_str()),
            Some("received")
        );
    }

    #[tokio::test]
    async fn completing_creation_with_upload_fires_finish_hooks_around_commit() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let locker = NoopLocker::new();
        let hooks = RecordingHookExecutor::default();
        let headers = Headers {
            upload_length: Some(5),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };

        Protocol::new(&config, &storage, &store, &locker, &hooks)
            .post(
                headers,
                RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(
                    b"Hello",
                ))),
            )
            .await
            .unwrap();

        assert_eq!(
            hooks.events(),
            vec![
                HookEvent::PreCreate,
                HookEvent::PreReceive,
                HookEvent::PreFinish,
                HookEvent::PostCreate,
                HookEvent::PostReceive,
                HookEvent::PostFinish,
            ]
        );
        assert!(
            hooks.offsets().contains(&(HookEvent::PostFinish, 5)),
            "PostFinish should observe the completed offset"
        );
    }

    #[tokio::test]
    async fn creation_with_upload_pre_receive_rejection_does_not_poll_body() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let locker = NoopLocker::new();
        let hooks = HookChain::new()
            .on_pre_receive(|_| async { Ok(PreHookResult::reject(403, "receive blocked")) });
        let polled = Arc::new(AtomicBool::new(false));
        let polled_for_stream = Arc::clone(&polled);
        let stream: crate::BodyStream = Box::pin(futures::stream::once(async move {
            polled_for_stream.store(true, Ordering::SeqCst);
            Ok(crate::BodyFrame::Data(Bytes::from_static(b"Hello")))
        }));
        let headers = Headers {
            upload_length: Some(100),
            content_type: Some("application/offset+octet-stream".to_string()),
            ..Default::default()
        };

        let err = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .post(headers, RequestBody::from_stream(stream))
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            Error::HookRejected {
                status_code: 403,
                ..
            }
        ));
        assert!(!polled.load(Ordering::SeqCst));
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn creation_with_upload_pre_receive_response_headers_are_returned() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let locker = NoopLocker::new();
        let hooks = HookChain::new().on_pre_receive(|_| async {
            Ok(PreHookResult::proceed().with_header("x-receive", "accepted"))
        });
        let headers = Headers {
            upload_length: Some(100),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };

        let response = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .post(
                headers,
                RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(
                    b"Hello",
                ))),
            )
            .await
            .unwrap();

        assert_eq!(response.headers.get("x-receive").unwrap(), "accepted");
    }

    #[tokio::test]
    async fn creation_with_upload_rolls_back_append_failure_without_post_hooks() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let storage = FailingAppendStorage::new();
        let store = MemoryStateStore::new();
        let locker = NoopLocker::new();
        let hooks = RecordingHookExecutor::default();
        let headers = Headers {
            upload_length: Some(100),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };

        let err = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .post(
                headers,
                RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(
                    b"Hello",
                ))),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Storage(_)));
        assert_eq!(
            hooks.events(),
            vec![HookEvent::PreCreate, HookEvent::PreReceive]
        );
        assert!(storage.deleted());
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn creation_with_upload_rejects_actual_body_beyond_upload_length() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let headers = Headers {
            upload_length: Some(5),
            content_type: Some("application/offset+octet-stream".to_string()),
            transfer_encoding: Some("chunked".to_string()),
            ..Default::default()
        };

        let err = call(
            &config,
            &storage,
            &store,
            headers,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"123456"))),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::SizeExceeded { .. }));
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn creation_with_upload_rejects_invalid_body_before_post_create() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let post_create_calls = Arc::new(AtomicUsize::new(0));
        let hooks = HookChain::new().on_post_create({
            let post_create_calls = Arc::clone(&post_create_calls);
            move |_| {
                let post_create_calls = Arc::clone(&post_create_calls);
                async move {
                    post_create_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        });
        let locker = NoopLocker::new();
        let headers = Headers {
            upload_length: Some(5),
            content_type: Some("application/offset+octet-stream".to_string()),
            transfer_encoding: Some("chunked".to_string()),
            ..Default::default()
        };

        let err = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .post(
                headers,
                RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(
                    b"123456",
                ))),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, Error::SizeExceeded { .. }));
        assert_eq!(post_create_calls.load(Ordering::SeqCst), 0);
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn creation_with_upload_rejects_actual_body_beyond_max_size() {
        let config = Config::default()
            .with_extension(Extension::CreationWithUpload)
            .max_size(5);
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let headers = Headers {
            upload_defer_length: true,
            content_type: Some("application/offset+octet-stream".to_string()),
            transfer_encoding: Some("chunked".to_string()),
            ..Default::default()
        };

        let err = call(
            &config,
            &storage,
            &store,
            headers,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(b"123456"))),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::SizeExceeded { .. }));
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn creation_with_upload_empty_body() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let headers = Headers {
            upload_length: Some(100),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(0),
            ..Default::default()
        };

        let response = call(
            &config,
            &MemoryStorage::new(),
            &MemoryStateStore::new(),
            headers,
            RequestBody::from_bytes(Bytes::new()),
        )
        .await
        .unwrap();
        assert_eq!(response.headers.get("upload-offset").unwrap(), "0");
    }

    #[tokio::test]
    async fn supplied_post_body_without_framing_headers_requires_creation_with_upload_extension() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let body_data: &[u8] = b"body that must not be dropped";
        let headers = Headers {
            upload_length: Some(body_data.len() as u64),
            content_type: Some("application/offset+octet-stream".to_string()),
            ..Default::default()
        };

        let err = call(
            &Config::default(),
            &storage,
            &store,
            headers,
            RequestBody::from_bytes(Bytes::copy_from_slice(body_data)),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::ExtensionNotSupported(ext) if ext == "creation-with-upload"));
        assert!(
            store.is_empty(),
            "POST body rejection must happen before allocating state",
        );
    }

    #[tokio::test]
    async fn creation_with_upload_writes_body_without_framing_headers() {
        let config = Config::default().with_extension(Extension::CreationWithUpload);
        let store = MemoryStateStore::new();
        let body_data: &[u8] = b"Hello";
        let headers = Headers {
            upload_length: Some(body_data.len() as u64),
            content_type: Some("application/offset+octet-stream".to_string()),
            ..Default::default()
        };

        let response = call(
            &config,
            &MemoryStorage::new(),
            &store,
            headers,
            RequestBody::from_bytes(Bytes::copy_from_slice(body_data)),
        )
        .await
        .unwrap();

        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
        let location = response.headers.get("location").unwrap().to_str().unwrap();
        let id = location.rsplit('/').next().unwrap();
        let stored = store.get(id).await.unwrap().unwrap();
        assert_eq!(stored.offset(), 5);
        assert!(stored.is_complete());
    }

    #[tokio::test]
    async fn post_body_requires_creation_with_upload_extension() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let body_data: &[u8] = b"body that must not be dropped";
        let headers = Headers {
            upload_length: Some(body_data.len() as u64),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(body_data.len() as u64),
            ..Default::default()
        };

        let err = call(
            &Config::default(),
            &storage,
            &store,
            headers,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::copy_from_slice(
                body_data,
            ))),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::ExtensionNotSupported(ext) if ext == "creation-with-upload"));
        assert!(
            store.is_empty(),
            "POST body rejection must happen before allocating state",
        );
    }

    #[tokio::test]
    async fn chunked_post_body_requires_creation_with_upload_extension() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let body_data: &[u8] = b"chunked body that must not be dropped";
        let headers = Headers {
            upload_length: Some(body_data.len() as u64),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: None,
            transfer_encoding: Some("chunked".to_string()),
            ..Default::default()
        };

        let err = call(
            &Config::default(),
            &storage,
            &store,
            headers,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::copy_from_slice(
                body_data,
            ))),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::ExtensionNotSupported(ext) if ext == "creation-with-upload"));
        assert!(
            store.is_empty(),
            "chunked POST body rejection must happen before allocating state",
        );
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn creation_with_upload_accepts_checksum_trailer() {
        use crate::{BodyFrame, BodyStream};
        use base64::Engine;

        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let config = Config::default()
            .with_extension(Extension::CreationWithUpload)
            .with_extension(Extension::ChecksumTrailer);
        let checksum = base64::engine::general_purpose::STANDARD.encode(crate::calculate_checksum(
            crate::config::ChecksumAlgorithm::Sha1,
            b"hello",
        ));
        let mut trailers = http::HeaderMap::new();
        trailers.insert(
            "upload-checksum",
            format!("sha1 {checksum}").parse().unwrap(),
        );
        let stream: BodyStream = Box::pin(futures::stream::iter([
            Ok(BodyFrame::Data(Bytes::from_static(b"hello"))),
            Ok(BodyFrame::Trailers(trailers)),
        ]));
        let headers = Headers {
            upload_length: Some(5),
            content_type: Some("application/offset+octet-stream".to_string()),
            ..Default::default()
        };

        let response = call(
            &config,
            &storage,
            &store,
            headers,
            RequestBody::from_stream(stream),
        )
        .await
        .unwrap();

        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn creation_with_upload_rolls_back_on_checksum_mismatch() {
        use crate::config::ChecksumAlgorithm;
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let config = Config::with_all_extensions();
        let data = Bytes::from_static(b"hello");
        // Deliberately wrong expected checksum (3 zero bytes).
        let h = Headers {
            upload_length: Some(5),
            content_length: Some(5),
            content_type: Some("application/offset+octet-stream".to_string()),
            upload_checksum: Some((ChecksumAlgorithm::Sha1, vec![0u8; 20])),
            ..Default::default()
        };
        let err = call(
            &config,
            &storage,
            &store,
            h,
            RequestBody::from_chunk_stream(ChunkStream::from_bytes(data)),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::ChecksumMismatch { .. }));

        // No zombie state record should remain.
        assert!(store.is_empty(), "expected empty state store");
    }
}
