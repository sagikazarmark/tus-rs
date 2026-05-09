//! Core POST handler (TUS Creation + related extensions).

use chrono::{Duration, Utc};
use futures::StreamExt;
use http::StatusCode;

use crate::config::{Config, Extension};
use crate::error::Error;
use crate::extensions::UploadConcat;
use crate::hooks::{HookContext, HookEvent, HookExecutor, HookRequestInfo};
use crate::locking::Locker;
use crate::state::{StateStore, UploadState};
use crate::storage::{ChunkStream, Storage};

use super::{Headers, Protocol, Response, UploadId};

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
    pub async fn post(&self, headers: Headers, body: ChunkStream) -> Result<Response, Error> {
        if !self.config.has_extension(Extension::Creation) {
            return Err(Error::ExtensionNotSupported("creation".to_string()));
        }

        if headers.upload_concat.is_some() && !self.config.has_extension(Extension::Concatenation) {
            return Err(Error::ExtensionNotSupported("concatenation".to_string()));
        }

        let is_final_upload = matches!(headers.upload_concat, Some(UploadConcat::Final(_)));
        if is_final_upload {
            // Spec: "The Client MUST NOT include the Upload-Length header in the
            // final upload creation."
            if headers.upload_length.is_some() || headers.upload_defer_length {
                return Err(Error::InvalidHeader {
                    header: "Upload-Length",
                    message: "Upload-Length and Upload-Defer-Length must not be set on a final concatenation upload".to_string(),
                });
            }
        } else {
            if headers.upload_length.is_none() && !headers.upload_defer_length {
                return Err(Error::MissingHeader("Upload-Length or Upload-Defer-Length"));
            }

            if headers.upload_length.is_some() && headers.upload_defer_length {
                return Err(Error::InvalidHeader {
                    header: "Upload-Defer-Length",
                    message: "Upload-Length and Upload-Defer-Length are mutually exclusive"
                        .to_string(),
                });
            }

            if headers.upload_defer_length
                && !self.config.has_extension(Extension::CreationDeferLength)
            {
                return Err(Error::ExtensionNotSupported(
                    "creation-defer-length".to_string(),
                ));
            }

            if headers.upload_defer_length && !self.config.allows_empty_creation() {
                return Err(Error::ExtensionNotSupported(
                    "creation-defer-length".to_string(),
                ));
            }
        }

        // A POST with a request body uses the Creation-With-Upload flow and MUST
        // declare Content-Type: application/offset+octet-stream. Anything else
        // with a non-zero body is rejected; we're not going to silently drop
        // the bytes the client sent.
        if post_has_body(&headers) {
            headers.validate_patch_content_type()?;
            if !self.config.has_extension(Extension::CreationWithUpload) {
                return Err(Error::ExtensionNotSupported(
                    "creation-with-upload".to_string(),
                ));
            }
        } else if !is_final_upload && !self.config.allows_empty_creation() {
            return Err(Error::InvalidHeader {
                header: "Upload-Length",
                message: "empty creation requests are disabled".to_string(),
            });
        }

        if let (Some(length), Some(max_size)) =
            (headers.upload_length, self.config.max_size_limit())
            && length > max_size
        {
            return Err(Error::SizeExceeded {
                size: length,
                max: max_size,
            });
        }

        let mut state = UploadState::with_uuid();
        if let Some(len) = headers.upload_length {
            state.set_length(len);
        }
        if let Some(metadata) = headers.upload_metadata.clone() {
            state.set_metadata(metadata);
        }

        if let Some(expiration) = self.config.expiration_duration() {
            state.set_expiration(Utc::now() + Duration::from_std(expiration).unwrap());
        }

        match &headers.upload_concat {
            Some(UploadConcat::Partial) => {
                state.mark_partial();
            }
            Some(UploadConcat::Final(parts)) => {
                return self
                    .create_final_upload(&headers, state, parts.clone())
                    .await;
            }
            None => {}
        }

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
        body: ChunkStream,
    ) -> Result<bytes::Bytes, Error> {
        let checksum_info = headers.upload_checksum.clone();

        #[cfg(feature = "checksum")]
        if let Some((algorithm, _)) = &checksum_info
            && self.config.has_extension(Extension::Checksum)
            && !self.config.supports_checksum_algorithm(*algorithm)
        {
            return Err(Error::UnsupportedChecksum(algorithm.as_str().to_string()));
        }

        // Buffer the body so we can validate the checksum before writing to
        // storage. Streaming-aware checksum validation is a separate follow-up.
        let data = collect_chunk_stream(body, creation_body_size_limit(self.config, state)).await?;
        let body_len = data.len() as u64;
        validate_content_length(headers, body_len)?;
        validate_creation_body_size(self.config, state, body_len)?;

        #[cfg(feature = "checksum")]
        if let Some((algorithm, expected)) = checksum_info {
            let calculated = crate::checksum::calculate(algorithm, &data);
            if calculated != expected {
                use base64::Engine;
                return Err(Error::ChecksumMismatch {
                    expected: base64::engine::general_purpose::STANDARD.encode(&expected),
                    actual: base64::engine::general_purpose::STANDARD.encode(&calculated),
                });
            }
        }
        #[cfg(not(feature = "checksum"))]
        let _ = checksum_info;

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
        let new_offset = self
            .storage
            .append(state, ChunkStream::Buffered(data))
            .await?;
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
        mut state: UploadState,
        part_urls: Vec<String>,
    ) -> Result<Response, Error> {
        let allow_unfinished = self
            .config
            .has_extension(Extension::ConcatenationUnfinished);

        let mut part_ids = Vec::new();
        let mut parts = Vec::new();
        let mut total_length: u64 = 0;
        let mut current_offset: u64 = 0;
        let mut all_complete = true;
        let mut length_known = true;

        for url in &part_urls {
            let id = extract_partial_id(url, self.config.base_path_str()).ok_or_else(|| {
                Error::InvalidHeader {
                    header: "Upload-Concat",
                    message: format!(
                        "partial URL not under base path {:?}: {}",
                        self.config.base_path_str(),
                        url
                    ),
                }
            })?;

            let part_state =
                self.state_store
                    .get(id.as_str())
                    .await?
                    .ok_or_else(|| Error::InvalidHeader {
                        header: "Upload-Concat",
                        message: format!("partial upload not found: {}", id),
                    })?;

            if !part_state.is_partial() {
                return Err(Error::NotPartialUpload(id.into_string()));
            }

            if !part_state.is_complete() {
                if !allow_unfinished {
                    return Err(Error::IncompleteUpload(id.into_string()));
                }
                all_complete = false;
            }

            if part_state.is_expired() {
                return Err(Error::Expired(id.into_string()));
            }

            match part_state.length() {
                Some(len) => total_length += len,
                None => {
                    length_known = false;
                    all_complete = false;
                }
            }
            current_offset += part_state.offset();

            part_ids.push(id.into_string());
            parts.push(part_state);
        }

        state.mark_final(part_ids);
        if length_known {
            state.set_length(total_length);
        }
        state.set_offset(if all_complete {
            total_length
        } else {
            current_offset
        });

        let hook_ctx = HookContext::new(
            HookEvent::PreCreate,
            state.clone(),
            make_hook_request_info(headers),
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

        if all_complete {
            self.execute_pre_finish(headers, state.clone()).await?;
        }

        self.storage.create(&mut state).await?;

        if all_complete {
            self.storage.concat(&mut state, parts).await?;
        }

        self.state_store.set(&state, true).await?;

        let post_create_ctx = HookContext::new(
            HookEvent::PostCreate,
            state.clone(),
            make_hook_request_info(headers),
        );
        self.hooks.execute_post(&post_create_ctx).await?;

        if all_complete {
            let post_finish_ctx = HookContext::new(
                HookEvent::PostFinish,
                state.clone(),
                make_hook_request_info(headers),
            );
            self.hooks.execute_post(&post_finish_ctx).await?;
        }

        let location = self
            .config
            .upload_url(state.id(), headers.base_url(self.config).as_deref());
        let mut response = Response::new(StatusCode::CREATED).with_header("location", &location);

        if !state.is_final() || state.is_complete() {
            response = response.with_header("upload-offset", state.offset().to_string());
        }

        if let Some(length) = state.length() {
            response = response.with_header("upload-length", length.to_string());
        }

        if self.config.has_extension(Extension::Expiration)
            && let Some(expires) = state.expires_header()
        {
            response = response.with_header("upload-expires", &expires);
        }

        for (name, value) in pre_result.response_headers {
            response = response.with_header_owned(name, value);
        }

        Ok(response)
    }

    async fn execute_pre_finish(&self, headers: &Headers, state: UploadState) -> Result<(), Error> {
        let pre_finish_ctx =
            HookContext::new(HookEvent::PreFinish, state, make_hook_request_info(headers));
        let pre_finish_result = self.hooks.execute_pre(&pre_finish_ctx).await?;

        if !pre_finish_result.proceed {
            return Err(Error::HookRejected {
                status_code: pre_finish_result.reject_status.unwrap_or(400),
                message: pre_finish_result.reject_message.unwrap_or_default(),
            });
        }

        Ok(())
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

async fn collect_chunk_stream(
    stream: ChunkStream,
    body_limit: Option<u64>,
) -> Result<bytes::Bytes, Error> {
    match stream {
        ChunkStream::Buffered(b) => {
            enforce_body_limit(0, b.len(), body_limit)?;
            Ok(b)
        }
        ChunkStream::Stream(mut s) => {
            let mut buffer = bytes::BytesMut::new();
            while let Some(chunk) = s.next().await {
                let bytes = chunk.map_err(|e| Error::Internal(e.to_string()))?;
                enforce_body_limit(buffer.len(), bytes.len(), body_limit)?;
                buffer.extend_from_slice(&bytes);
            }
            Ok(buffer.freeze())
        }
    }
}

fn enforce_body_limit(
    current_len: usize,
    next_len: usize,
    body_limit: Option<u64>,
) -> Result<(), Error> {
    let Some(limit) = body_limit else {
        return Ok(());
    };
    let next_total = (current_len as u64).saturating_add(next_len as u64);
    if next_total > limit {
        return Err(Error::SizeExceeded {
            size: next_total,
            max: limit,
        });
    }

    Ok(())
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

fn validate_content_length(headers: &Headers, actual_len: u64) -> Result<(), Error> {
    if let Some(content_length) = headers.content_length
        && content_length != actual_len
    {
        return Err(Error::InvalidHeader {
            header: "Content-Length",
            message: format!(
                "declared content length {content_length} does not match body size {actual_len}"
            ),
        });
    }

    Ok(())
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

/// Extracts an upload ID from a URL present in `Upload-Concat: final;...`,
/// validating that the URL points under the configured base path.
///
/// Accepts:
/// - A relative path: `/files/abc123`
/// - An absolute URL: `https://host.example/files/abc123`
///
/// Rejects anything whose path is not exactly `{base_path}/{id}` with a
/// non-empty id that does not itself contain a `/`. Returns `None` for
/// rejected inputs.
fn extract_partial_id(url: &str, base_path: &str) -> Option<UploadId> {
    // Take the path portion. For absolute URLs, strip scheme and authority;
    // for relative paths, use as-is. We do not perform full URL parsing here;
    // tus URLs are always produced by this server's `upload_url`, so their
    // structure is well-known.
    let path = if let Some(rest) = url.split_once("://") {
        // Scheme present; skip past the authority to the first "/".
        match rest.1.find('/') {
            Some(idx) => &rest.1[idx..],
            None => return None,
        }
    } else {
        url
    };

    // Strip optional query/fragment.
    let path = path.split(['?', '#']).next().unwrap_or(path);

    let expected_prefix = if base_path.ends_with('/') {
        base_path.to_string()
    } else {
        format!("{}/", base_path)
    };

    let id = path.strip_prefix(&expected_prefix)?;
    if id.is_empty() || id.contains('/') {
        return None;
    }
    id.parse().ok()
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
    use crate::hooks::{HookChain, NoopHookExecutor, PreHookResult};
    use crate::locking::NoopLocker;
    use crate::state::UploadMetadata;
    use crate::state::memory::MemoryStateStore;
    use crate::storage::memory::MemoryStorage;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        body: ChunkStream,
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
            ChunkStream::empty(),
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
            ChunkStream::empty(),
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
            ChunkStream::empty(),
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
            ChunkStream::empty(),
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
            ChunkStream::empty(),
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
            ChunkStream::empty(),
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
            ChunkStream::from_bytes(body_data),
        )
        .await
        .unwrap();

        assert_eq!(response.status, StatusCode::CREATED);
        assert_eq!(response.headers.get("upload-offset").unwrap(), "5");
    }

    #[tokio::test]
    async fn missing_length_rejected() {
        let err = call(
            &Config::default(),
            &MemoryStorage::new(),
            &MemoryStateStore::new(),
            Headers::default(),
            ChunkStream::empty(),
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
            ChunkStream::empty(),
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
            ChunkStream::empty(),
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
            ChunkStream::empty(),
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
            ChunkStream::empty(),
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

        let response = call(&config, &storage, &store, headers, ChunkStream::empty())
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
            .post(headers, ChunkStream::empty())
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
            ChunkStream::empty(),
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

        let response = call(&config, &storage, &store, headers, ChunkStream::empty())
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
        let body = ChunkStream::from_bytes(Bytes::copy_from_slice(body_data));
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
                ChunkStream::from_bytes(Bytes::from_static(b"Hello")),
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
        let body = ChunkStream::from_bytes(Bytes::copy_from_slice(body_data));
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
            ChunkStream::from_bytes(Bytes::from_static(b"123456")),
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
                ChunkStream::from_bytes(Bytes::from_static(b"123456")),
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
            ChunkStream::from_bytes(Bytes::from_static(b"123456")),
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
            ChunkStream::empty(),
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
            ChunkStream::from_bytes(Bytes::copy_from_slice(body_data)),
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
            ChunkStream::from_bytes(Bytes::copy_from_slice(body_data)),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::ExtensionNotSupported(ext) if ext == "creation-with-upload"));
        assert!(
            store.list(100, 0).await.unwrap().is_empty(),
            "chunked POST body rejection must happen before allocating state",
        );
    }

    #[test]
    fn extract_partial_id_accepts_relative_and_absolute() {
        assert_eq!(
            extract_partial_id("/files/abc123", "/files").map(UploadId::into_string),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_partial_id("https://host.example/files/abc123", "/files")
                .map(UploadId::into_string),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_partial_id("http://host/files/abc?x=1", "/files").map(UploadId::into_string),
            Some("abc".to_string())
        );
        // Base path with trailing slash is handled.
        assert_eq!(
            extract_partial_id("/files/abc", "/files/").map(UploadId::into_string),
            Some("abc".to_string())
        );
    }

    #[test]
    fn extract_partial_id_rejects_mismatched_base() {
        assert_eq!(extract_partial_id("/other/abc", "/files"), None);
        assert_eq!(extract_partial_id("abc", "/files"), None);
        assert_eq!(extract_partial_id("/files", "/files"), None);
        assert_eq!(extract_partial_id("/files/", "/files"), None);
        // Nested path: would require id to not contain a slash.
        assert_eq!(extract_partial_id("/files/a/b", "/files"), None);
        // Authority-less absolute URL is malformed.
        assert_eq!(extract_partial_id("https://", "/files"), None);
    }

    #[test]
    fn extract_partial_id_rejects_invalid_upload_ids() {
        assert_eq!(extract_partial_id("/files/foo\\bar", "/files"), None);
        assert_eq!(extract_partial_id("/files/foo\nbar", "/files"), None);
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
        let err = call(&config, &storage, &store, h, ChunkStream::from_bytes(data))
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
