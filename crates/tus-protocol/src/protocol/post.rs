//! Core POST handler (TUS Creation + related extensions).

use http::StatusCode;

use crate::config::{Config, Extension};
use crate::error::Error;
use crate::hooks::{HookContext, HookEvent, HookExecutor, HookRequestInfo};
use crate::lifecycle::{
    CreationRequest, CreationTransition, create_final_upload as create_lifecycle_final_upload,
    ensure_committed_offset, prepare_creation, run_pre_finish,
};
use crate::locking::Locker;
use crate::state::{StateStore, UploadState};
use crate::storage::{ChunkStream, Storage};

use super::body::RequestBody;
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
        let has_body = post_has_body(&headers);
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
                return self.create_final_upload(&headers, state, part_urls).await;
            }
        };

        let hook_ctx = HookContext::new(
            HookEvent::PreCreate,
            state.clone(),
            make_hook_request_info(&headers),
        );
        let pre_result = self.hooks.execute_pre(&hook_ctx).await?;

        if !pre_result.proceed {
            return Err(Error::HookRejected {
                status_code: pre_result.reject_status.unwrap_or(400),
                message: pre_result.reject_message.unwrap_or_default(),
            });
        }

        if let Some(modified_state) = pre_result.upload {
            state = modified_state;
        }

        // Creation-With-Upload path
        let is_creation_with_upload = self.config.has_extension(Extension::CreationWithUpload)
            && headers
                .content_type
                .as_deref()
                .map(|ct| ct.starts_with("application/offset+octet-stream"))
                .unwrap_or(false);
        let creation_body = if is_creation_with_upload {
            Some(self.prepare_creation_body(&headers, &state, body).await?)
        } else {
            None
        };

        self.storage.create(&mut state).await?;
        self.state_store.set(&state, true).await?;

        let post_ctx = HookContext::new(
            HookEvent::PostCreate,
            state.clone(),
            make_hook_request_info(&headers),
        );
        self.hooks.execute_post(&post_ctx).await?;

        if let Some(data) = creation_body
            && let Err(e) = self.commit_creation_body(&headers, &mut state, data).await
        {
            // Something in the body-write phase (storage append or state
            // persistence) failed. Roll back the just-created state record and
            // storage object so the upload ID does not survive as a zombie
            // record.
            let _ = self.storage.delete(&state).await;
            let _ = self.state_store.delete(state.id()).await;
            return Err(e);
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

        Ok(response)
    }
}

/// Handles the Creation-With-Upload body in two phases: validate the complete
/// body before create side effects, then commit it after the upload exists.
impl<'a, S, I, L, H> Protocol<'a, S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    async fn prepare_creation_body(
        &self,
        headers: &Headers,
        state: &UploadState,
        body: RequestBody,
    ) -> Result<bytes::Bytes, Error> {
        let data = super::body::collect(
            self.config,
            headers,
            creation_body_size_limit(self.config, state),
            body,
        )
        .await?
        .bytes;
        let body_len = data.len() as u64;
        validate_creation_body_size(self.config, state, body_len)?;

        let projected_offset = state.offset().saturating_add(body_len);
        if state
            .length()
            .is_some_and(|length| projected_offset == length)
        {
            let mut completed_state = state.clone();
            completed_state.set_offset(projected_offset);
            self.execute_pre_finish(headers, completed_state).await?;
        }

        Ok(data)
    }

    async fn commit_creation_body(
        &self,
        headers: &Headers,
        state: &mut UploadState,
        data: bytes::Bytes,
    ) -> Result<(), Error> {
        let projected_offset = state.offset().saturating_add(data.len() as u64);
        let new_offset = self
            .storage
            .append(state, ChunkStream::Buffered(data))
            .await?;
        ensure_committed_offset(new_offset, projected_offset)?;
        state.set_offset(new_offset);
        self.state_store.set(state, false).await?;

        if state.is_complete() {
            let post_finish_ctx = HookContext::new(
                HookEvent::PostFinish,
                state.clone(),
                make_hook_request_info(headers),
            );
            self.hooks.execute_post(&post_finish_ctx).await?;
        }

        Ok(())
    }

    async fn create_final_upload(
        &self,
        headers: &Headers,
        state: UploadState,
        part_urls: Vec<String>,
    ) -> Result<Response, Error> {
        let created = create_lifecycle_final_upload(
            self.storage,
            self.state_store,
            self.hooks,
            self.config,
            &make_hook_request_info(headers),
            state,
            part_urls,
        )
        .await?;
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

    async fn execute_pre_finish(&self, headers: &Headers, state: UploadState) -> Result<(), Error> {
        run_pre_finish(self.hooks, &make_hook_request_info(headers), state).await
    }
}

fn post_has_body(headers: &Headers) -> bool {
    headers.content_length.unwrap_or(0) > 0
        || headers
            .transfer_encoding
            .as_deref()
            .map(|value| {
                value
                    .split(',')
                    .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
            })
            .unwrap_or(false)
}

fn creation_body_size_limit(config: &Config, state: &UploadState) -> Option<u64> {
    [
        config
            .max_size_limit()
            .map(|max_size| max_size.saturating_sub(state.offset())),
        state
            .length()
            .map(|length| length.saturating_sub(state.offset())),
    ]
    .into_iter()
    .flatten()
    .min()
}

fn validate_creation_body_size(
    config: &Config,
    state: &UploadState,
    body_len: u64,
) -> Result<(), Error> {
    if let Some(max_size) = config.max_size_limit() {
        let projected = state.offset().saturating_add(body_len);
        if projected > max_size {
            return Err(Error::SizeExceeded {
                size: projected,
                max: max_size,
            });
        }
    }

    if let Some(length) = state.length() {
        let projected = state.offset().saturating_add(body_len);
        if projected > length {
            return Err(Error::SizeExceeded {
                size: projected,
                max: length,
            });
        }
    }

    Ok(())
}

fn make_hook_request_info(headers: &Headers) -> HookRequestInfo {
    let mut hook_headers = std::collections::HashMap::new();

    if let Some(offset) = headers.upload_offset {
        hook_headers.insert("upload-offset".to_string(), offset.to_string());
    }
    if let Some(length) = headers.upload_length {
        hook_headers.insert("upload-length".to_string(), length.to_string());
    }
    if headers.upload_defer_length {
        hook_headers.insert("upload-defer-length".to_string(), "1".to_string());
    }
    if let Some(ct) = &headers.content_type {
        hook_headers.insert("content-type".to_string(), ct.clone());
    }
    if let Some(cl) = headers.content_length {
        hook_headers.insert("content-length".to_string(), cl.to_string());
    }

    HookRequestInfo {
        method: "POST".to_string(),
        path: String::new(),
        remote_addr: None,
        headers: hook_headers,
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
    use crate::hooks::{HookChain, NoopHookExecutor, PreHookResult};
    use crate::locking::NoopLocker;
    use crate::state::UploadMetadata;
    use crate::state::memory::MemoryStateStore;
    use crate::storage::ByteStream;
    use crate::storage::memory::MemoryStorage;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct WrongOffsetStorage {
        inner: MemoryStorage,
        returned_offset: u64,
    }

    impl WrongOffsetStorage {
        fn new(returned_offset: u64) -> Self {
            Self {
                inner: MemoryStorage::new(),
                returned_offset,
            }
        }
    }

    #[async_trait::async_trait]
    impl Storage for WrongOffsetStorage {
        fn name(&self) -> &'static str {
            "wrong-offset"
        }

        async fn create(&self, state: &mut UploadState) -> crate::error::Result<String> {
            self.inner.create(state).await
        }

        async fn append(
            &self,
            state: &mut UploadState,
            data: ChunkStream,
        ) -> crate::error::Result<u64> {
            self.inner.append(state, data).await?;
            Ok(self.returned_offset)
        }

        async fn get_stream(&self, state: &UploadState) -> crate::error::Result<ByteStream> {
            self.inner.get_stream(state).await
        }

        async fn concat(
            &self,
            target: &mut UploadState,
            parts: Vec<UploadState>,
        ) -> crate::error::Result<()> {
            self.inner.concat(target, parts).await
        }

        async fn delete(&self, state: &UploadState) -> crate::error::Result<()> {
            self.inner.delete(state).await
        }

        async fn size(&self, state: &UploadState) -> crate::error::Result<Option<u64>> {
            self.inner.size(state).await
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

    #[tokio::test]
    async fn basic_create() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();

        let response = call(
            &Config::default(),
            &storage,
            &store,
            headers_with_length(1000),
            RequestBody::from_chunk_stream(ChunkStream::empty()),
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
            RequestBody::from_chunk_stream(ChunkStream::empty()),
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
            RequestBody::from_chunk_stream(ChunkStream::empty()),
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
            RequestBody::from_chunk_stream(ChunkStream::empty()),
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
            RequestBody::from_chunk_stream(ChunkStream::empty()),
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
            RequestBody::from_chunk_stream(ChunkStream::empty()),
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
    async fn creation_with_upload_rejects_storage_offset_that_does_not_match_projected_offset() {
        let storage = WrongOffsetStorage::new(0);
        let store = MemoryStateStore::new();
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let headers = Headers {
            upload_length: Some(10),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };

        let config = Config::default().with_extension(Extension::CreationWithUpload);

        let err = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .post(
                headers,
                RequestBody::from_chunk_stream(ChunkStream::from_bytes(Bytes::from_static(
                    b"hello",
                ))),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, Error::Internal(message) if message.contains("storage returned offset 0") && message.contains("projected offset 5"))
        );
        assert!(store.list(10, 0).await.unwrap().is_empty());
        assert!(storage.inner.is_empty());
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
            RequestBody::from_chunk_stream(ChunkStream::empty()),
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
            RequestBody::from_chunk_stream(ChunkStream::empty()),
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
            RequestBody::from_chunk_stream(ChunkStream::empty()),
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
            RequestBody::from_chunk_stream(ChunkStream::empty()),
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
            RequestBody::from_chunk_stream(ChunkStream::empty()),
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
        storage.create(&mut part1).await.unwrap();
        storage
            .append(
                &mut part1,
                ChunkStream::from_bytes(Bytes::copy_from_slice(&[0u8; 50])),
            )
            .await
            .unwrap();
        part1.set_offset(50);
        store.set(&part1, true).await.unwrap();

        let mut part2 = UploadState::new("part2").with_length(50).as_partial();
        storage.create(&mut part2).await.unwrap();
        storage
            .append(
                &mut part2,
                ChunkStream::from_bytes(Bytes::copy_from_slice(&[0u8; 50])),
            )
            .await
            .unwrap();
        part2.set_offset(50);
        store.set(&part2, true).await.unwrap();

        let headers = Headers {
            upload_concat: Some(UploadConcat::Final(vec![
                "/files/part1".to_string(),
                "/files/part2".to_string(),
            ])),
            ..Default::default()
        };

        let response = call(
            &config,
            &storage,
            &store,
            headers,
            RequestBody::from_chunk_stream(ChunkStream::empty()),
        )
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
        storage.create(&mut part).await.unwrap();
        storage
            .append(
                &mut part,
                ChunkStream::from_bytes(Bytes::from_static(b"Hello")),
            )
            .await
            .unwrap();
        part.set_offset(5);
        store.set(&part, true).await.unwrap();

        let headers = Headers {
            upload_concat: Some(UploadConcat::Final(vec!["/files/part1".to_string()])),
            ..Default::default()
        };
        let locker = NoopLocker::new();
        let hooks = HookChain::new()
            .on_pre_finish(|_| async { Ok(PreHookResult::reject(403, "finish blocked")) });

        let err = Protocol::new(&config, &storage, &store, &locker, &hooks)
            .post(
                headers,
                RequestBody::from_chunk_stream(ChunkStream::empty()),
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
        assert_eq!(store.list(100, 0).await.unwrap(), vec!["part1".to_string()]);
    }

    #[tokio::test]
    async fn final_upload_rejects_incomplete_parts() {
        let config = Config::default().with_extension(Extension::Concatenation);
        let store = MemoryStateStore::new();

        let mut part1 = UploadState::new("part1").with_length(50).as_partial();
        part1.set_offset(25);
        part1.set_storage_key("uploads/part1");
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
            RequestBody::from_chunk_stream(ChunkStream::empty()),
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

        let response = call(
            &config,
            &storage,
            &store,
            headers,
            RequestBody::from_chunk_stream(ChunkStream::empty()),
        )
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
        assert!(store.list(100, 0).await.unwrap().is_empty());
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
        assert!(store.list(100, 0).await.unwrap().is_empty());
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
        assert!(store.list(100, 0).await.unwrap().is_empty());
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
        assert!(store.list(100, 0).await.unwrap().is_empty());
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
            RequestBody::from_chunk_stream(ChunkStream::empty()),
        )
        .await
        .unwrap();
        assert_eq!(response.headers.get("upload-offset").unwrap(), "0");
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
            store.list(100, 0).await.unwrap().is_empty(),
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
            store.list(100, 0).await.unwrap().is_empty(),
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
        let list = store.list(100, 0).await.unwrap();
        assert!(
            list.is_empty(),
            "expected empty state store, got {:?}",
            list
        );
    }
}
