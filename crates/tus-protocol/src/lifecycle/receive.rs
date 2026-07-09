use std::collections::HashMap;

mod body;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::hooks::{
    HookContext, HookEvent, HookExecutor, HookRequestInfo, execute_post_best_effort,
};
use crate::protocol::{Headers, RequestBody};
use crate::state::UploadState;
use crate::state::{StateStore, WriteMode};
use crate::storage::{AppendRequest, ChunkStream, Storage, StorageHandle};

use super::{PreHookGate, UploadCompletion, ensure_active};

/// Request fields needed before a PATCH body is accepted.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReceiveRequest {
    /// Client-supplied Upload-Offset.
    pub client_offset: u64,
    /// Optional Upload-Length used to resolve deferred length uploads.
    pub upload_length: Option<u64>,
}

/// Result of projecting a receive body against the current upload state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReceiveProjection {
    /// Offset expected after the body is committed.
    pub projected_offset: u64,
    /// Whether the receive operation completes the upload.
    pub completes_upload: bool,
}

/// Receive body path selected by the protocol request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceiveBodyKind {
    /// Body bytes supplied by a PATCH request.
    Patch,
    /// Initial body bytes supplied by Creation-With-Upload.
    CreationWithUpload,
}

/// Body bytes and lifecycle projection that have passed receive gates.
#[derive(Debug)]
struct PreparedReceiveBody {
    pub(crate) data: ChunkStream,
    pub(crate) projection: ReceiveProjection,
    pub(crate) response_headers: HashMap<String, String>,
    pub(crate) deferred_error: body::DeferredBodyError,
}

/// Result of accepting bytes for an existing upload.
#[derive(Debug)]
pub(crate) struct ReceiveOutcome {
    pub(crate) response_headers: HashMap<String, String>,
}

/// Result of accepting initial bytes during Creation-With-Upload.
#[derive(Debug)]
pub(crate) struct CreationWithUploadOutcome {
    pub(crate) state: UploadState,
    pub(crate) response_headers: HashMap<String, String>,
}

/// Internal Byte receive module for PATCH and Creation-With-Upload bytes.
pub(crate) struct ByteReceiver<'a, S, St, H>
where
    S: Storage + ?Sized,
    St: StateStore + ?Sized,
    H: HookExecutor + ?Sized,
{
    storage: &'a S,
    state_store: &'a St,
    hooks: &'a H,
    config: &'a Config,
    request_info: &'a HookRequestInfo,
}

impl<'a, S, St, H> ByteReceiver<'a, S, St, H>
where
    S: Storage + ?Sized,
    St: StateStore + ?Sized,
    H: HookExecutor + ?Sized,
{
    pub(crate) fn new(
        storage: &'a S,
        state_store: &'a St,
        hooks: &'a H,
        config: &'a Config,
        request_info: &'a HookRequestInfo,
    ) -> Self {
        Self {
            storage,
            state_store,
            hooks,
            config,
            request_info,
        }
    }

    pub(crate) async fn receive_patch(
        &self,
        headers: &Headers,
        state: &mut UploadState,
        request: ReceiveRequest,
        request_body: RequestBody,
    ) -> Result<ReceiveOutcome> {
        prepare_receive(self.config, state, request)?;
        let prepared = prepare_receive_body(
            self.config,
            self.hooks,
            self.request_info,
            headers,
            state,
            request_body,
            ReceiveBodyKind::Patch,
        )
        .await?;
        let response_headers = prepared.response_headers.clone();
        let completes_upload = prepared.projection.completes_upload;

        commit_receive_body(self.storage, self.state_store, state, prepared).await?;
        self.run_post_event(HookEvent::PostReceive, state).await;
        if completes_upload {
            // The completing bytes are already durable; a PreFinish rejection
            // fails this response and skips PostFinish, but cannot undo the
            // stored upload.
            self.completion().finish_gate(state.clone()).await?;
            self.completion().after_commit(state).await;
        }

        Ok(ReceiveOutcome { response_headers })
    }

    pub(crate) async fn receive_creation_with_upload(
        &self,
        headers: &Headers,
        state: UploadState,
        request_body: RequestBody,
    ) -> Result<CreationWithUploadOutcome> {
        let pre_create = PreHookGate::Create
            .run(self.hooks, self.request_info, state)
            .await?;
        let mut state = pre_create.state;
        let mut response_headers = pre_create.response_headers;
        let prepared = prepare_receive_body(
            self.config,
            self.hooks,
            self.request_info,
            headers,
            &mut state,
            request_body,
            ReceiveBodyKind::CreationWithUpload,
        )
        .await?;
        response_headers.extend(prepared.response_headers.clone());

        let handle = self.storage.create(state.id()).await?;
        state.set_storage_handle(handle);
        if let Err(err) = self.state_store.set(&state, WriteMode::CreateNew).await {
            // Best-effort rollback of the storage object only: the `CreateNew`
            // set never wrote our state record, so we must not delete the state
            // for this ID. If the failure was `AlreadyExists`, that record
            // belongs to a pre-existing upload and deleting it would corrupt a
            // different upload. Mirrors the non-CwU creation path in `post.rs`.
            self.rollback_storage(&state).await;
            return Err(err);
        }

        if let Err(err) =
            commit_receive_body(self.storage, self.state_store, &mut state, prepared).await
        {
            self.rollback_creation(&state).await;
            return Err(err);
        }

        // The upload resource is new, so a PreFinish rejection can still roll
        // the whole creation back instead of leaving a completed upload.
        if state.is_complete()
            && let Err(err) = self.completion().finish_gate(state.clone()).await
        {
            self.rollback_creation(&state).await;
            return Err(err);
        }

        self.run_post_event(HookEvent::PostCreate, &state).await;
        self.run_post_event(HookEvent::PostReceive, &state).await;
        self.completion().after_commit_if_complete(&state).await;

        Ok(CreationWithUploadOutcome {
            state,
            response_headers,
        })
    }

    /// Rolls back only the storage object created for a new upload.
    ///
    /// Used when the `CreateNew` state write itself failed: we own the storage
    /// object we just created, but no state record of ours was persisted, so
    /// the state record for this ID (if any) belongs to a different upload and
    /// must be left untouched.
    async fn rollback_storage(&self, state: &UploadState) {
        if let Some(handle) = state.storage_handle()
            && let Err(error) = self.storage.delete(&handle).await
        {
            tracing::warn!(
                upload_id = %state.id(),
                %error,
                "failed to roll back storage object while undoing upload creation"
            );
        }
    }

    /// Rolls back both the storage object and the state record for a new upload.
    ///
    /// Used after the `CreateNew` state write has succeeded, so this upload owns
    /// its state record and may delete it.
    async fn rollback_creation(&self, state: &UploadState) {
        self.rollback_storage(state).await;
        if let Err(error) = self.state_store.delete(state.id()).await {
            tracing::warn!(
                upload_id = %state.id(),
                %error,
                "failed to roll back state record while undoing upload creation"
            );
        }
    }

    fn completion(&self) -> UploadCompletion<'_, H> {
        UploadCompletion::new(self.hooks, self.request_info)
    }

    async fn run_post_event(&self, event: HookEvent, state: &UploadState) {
        let ctx = HookContext::new(event, state.clone(), self.request_info.clone());
        execute_post_best_effort(self.hooks, &ctx).await;
    }
}

/// Validates PATCH preflight state and applies deferred Upload-Length.
fn prepare_receive(
    config: &Config,
    state: &mut UploadState,
    request: ReceiveRequest,
) -> Result<()> {
    ensure_active(state)?;

    if state.is_final() {
        return Err(Error::FinalUploadModificationForbidden(
            state.id().to_string(),
        ));
    }

    // Completion is checked before the offset so a client retrying a finished
    // upload at any offset (typically `0`) gets the accurate
    // `CompletedUploadModificationForbidden` rather than an `OffsetMismatch`
    // against the full length.
    if state.is_complete() {
        return Err(Error::CompletedUploadModificationForbidden(
            state.id().to_string(),
        ));
    }

    if request.client_offset != state.offset() {
        return Err(Error::OffsetMismatch {
            expected: state.offset(),
            actual: request.client_offset,
        });
    }

    if let Some(length) = request.upload_length {
        if let Some(existing) = state.length() {
            if length != existing {
                return Err(Error::InvalidHeader {
                    header: "Upload-Length",
                    message: format!(
                        "cannot change Upload-Length after it is set (existing: {existing}, provided: {length})"
                    ),
                });
            }
        } else {
            if let Some(max_size) = config.max_size()
                && length > max_size
            {
                return Err(Error::SizeExceeded {
                    size: length,
                    max: max_size,
                });
            }
            state.set_length(length);
        }
    }

    Ok(())
}

/// Computes the maximum body bytes this receive may accept before buffering.
#[must_use]
fn receive_body_size_limit(config: &Config, state: &UploadState) -> Option<u64> {
    [
        config.max_chunk_size(),
        config
            .max_size()
            .map(|max_size| max_size.saturating_sub(state.offset())),
        state
            .length()
            .map(|length| length.saturating_sub(state.offset())),
    ]
    .into_iter()
    .flatten()
    .min()
}

/// Validates body length and computes the resulting offset.
fn validate_receive_body(
    config: &Config,
    state: &UploadState,
    body_len: u64,
) -> Result<ReceiveProjection> {
    if let Some(max_chunk) = config.max_chunk_size()
        && body_len > max_chunk
    {
        return Err(Error::SizeExceeded {
            size: body_len,
            max: max_chunk,
        });
    }

    let projected_offset = state.offset().saturating_add(body_len);

    if let Some(max_size) = config.max_size()
        && projected_offset > max_size
    {
        return Err(Error::SizeExceeded {
            size: projected_offset,
            max: max_size,
        });
    }

    if let Some(length) = state.length()
        && projected_offset > length
    {
        return Err(Error::SizeExceeded {
            size: projected_offset,
            max: length,
        });
    }

    Ok(ReceiveProjection {
        projected_offset,
        completes_upload: state
            .length()
            .is_some_and(|length| projected_offset == length),
    })
}

/// Runs receive hook gates, Body intake, and completion projection for accepted bytes.
async fn prepare_receive_body<H>(
    config: &Config,
    hooks: &H,
    request_info: &HookRequestInfo,
    headers: &Headers,
    state: &mut UploadState,
    request_body: RequestBody,
    kind: ReceiveBodyKind,
) -> Result<PreparedReceiveBody>
where
    H: HookExecutor + ?Sized,
{
    let pre_receive = PreHookGate::Receive
        .run(hooks, request_info, state.clone())
        .await?;
    let response_headers = pre_receive.response_headers;
    *state = pre_receive.state;

    let collected = collect_receive_body(config, headers, state, request_body, kind).await?;
    if matches!(kind, ReceiveBodyKind::CreationWithUpload) {
        debug_assert!(
            collected.supplied,
            "creation body collection should only run for supplied bodies"
        );
    }

    let projection = validate_body_for_receive(config, state, collected.size, kind)?;

    Ok(PreparedReceiveBody {
        data: collected.data,
        projection,
        response_headers,
        deferred_error: collected.deferred_error,
    })
}

/// Commits accepted receive bytes to storage and persists the resulting upload state.
async fn commit_receive_body<S, St>(
    storage: &S,
    state_store: &St,
    state: &mut UploadState,
    prepared: PreparedReceiveBody,
) -> Result<()>
where
    S: Storage + ?Sized,
    St: StateStore + ?Sized,
{
    let deferred_error = prepared.deferred_error.clone();
    let handle = match storage
        .append(AppendRequest::new(
            state.require_storage_handle()?,
            state.offset(),
            prepared.data,
            prepared.projection.completes_upload,
        ))
        .await
    {
        Ok(handle) => handle,
        Err(error) => return Err(deferred_error.take().unwrap_or(error)),
    };
    if let Some(error) = deferred_error.take() {
        return Err(error);
    }

    apply_receive_commit(state, prepared.projection, handle);
    state_store.set(state, WriteMode::Update).await?;

    Ok(())
}

fn apply_receive_commit(
    state: &mut UploadState,
    projection: ReceiveProjection,
    handle: StorageHandle,
) {
    state.set_storage_handle(handle);
    state.set_offset(projection.projected_offset);
}

fn receive_body_limit(config: &Config, state: &UploadState, kind: ReceiveBodyKind) -> Option<u64> {
    match kind {
        ReceiveBodyKind::Patch => receive_body_size_limit(config, state),
        ReceiveBodyKind::CreationWithUpload => creation_with_upload_body_size_limit(config, state),
    }
}

async fn collect_receive_body(
    config: &Config,
    headers: &Headers,
    state: &UploadState,
    request_body: RequestBody,
    kind: ReceiveBodyKind,
) -> Result<body::IntakeBody> {
    body::prepare(
        config,
        headers,
        receive_body_limit(config, state, kind),
        request_body,
    )
    .await
}

fn validate_body_for_receive(
    config: &Config,
    state: &UploadState,
    body_len: u64,
    kind: ReceiveBodyKind,
) -> Result<ReceiveProjection> {
    match kind {
        ReceiveBodyKind::Patch => validate_receive_body(config, state, body_len),
        ReceiveBodyKind::CreationWithUpload => {
            validate_creation_with_upload_body(config, state, body_len)
        }
    }
}

fn creation_with_upload_body_size_limit(config: &Config, state: &UploadState) -> Option<u64> {
    // `max_chunk_size` is a per-PATCH cap and is deliberately not applied here:
    // the initial data of a Creation-With-Upload request travels on the POST,
    // not a PATCH. The body is still bounded by `max_size` and the declared
    // `Upload-Length` below.
    [
        config
            .max_size()
            .map(|max_size| max_size.saturating_sub(state.offset())),
        state
            .length()
            .map(|length| length.saturating_sub(state.offset())),
    ]
    .into_iter()
    .flatten()
    .min()
}

fn validate_creation_with_upload_body(
    config: &Config,
    state: &UploadState,
    body_len: u64,
) -> Result<ReceiveProjection> {
    let projected_offset = state.offset().saturating_add(body_len);

    if let Some(max_size) = config.max_size()
        && projected_offset > max_size
    {
        return Err(Error::SizeExceeded {
            size: projected_offset,
            max: max_size,
        });
    }

    if let Some(length) = state.length()
        && projected_offset > length
    {
        return Err(Error::SizeExceeded {
            size: projected_offset,
            max: length,
        });
    }

    Ok(ReceiveProjection {
        projected_offset,
        completes_upload: state
            .length()
            .is_some_and(|length| projected_offset == length),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_receive_rejects_offset_mismatch() {
        let mut state = UploadState::new("upload-1").with_length(10);
        state.set_offset(4);

        let err = prepare_receive(
            &Config::default(),
            &mut state,
            ReceiveRequest {
                client_offset: 3,
                upload_length: None,
            },
        )
        .unwrap_err();

        assert!(matches!(
            err,
            Error::OffsetMismatch {
                expected: 4,
                actual: 3
            }
        ));
    }

    #[test]
    fn prepare_receive_reports_completion_before_offset_for_retried_upload() {
        // A client retrying an already-completed upload — typically at offset 0
        // — must get the accurate completion error, not an `OffsetMismatch`
        // against the full length.
        let mut state = UploadState::new("upload-1").with_length(10);
        state.set_offset(10);
        assert!(state.is_complete());

        let err = prepare_receive(
            &Config::default(),
            &mut state,
            ReceiveRequest {
                client_offset: 0,
                upload_length: None,
            },
        )
        .unwrap_err();

        assert!(matches!(
            err,
            Error::CompletedUploadModificationForbidden(id) if id == "upload-1"
        ));
    }

    #[test]
    fn prepare_receive_sets_deferred_length() {
        let mut state = UploadState::new("upload-1");

        prepare_receive(
            &Config::default(),
            &mut state,
            ReceiveRequest {
                client_offset: 0,
                upload_length: Some(10),
            },
        )
        .unwrap();

        assert_eq!(state.length(), Some(10));
    }

    #[test]
    fn validate_receive_body_detects_completion() {
        let state = UploadState::new("upload-1").with_length(5);

        let projection = validate_receive_body(&Config::default(), &state, 5).unwrap();

        assert_eq!(projection.projected_offset, 5);
        assert!(projection.completes_upload);
    }

    #[test]
    fn validate_receive_body_rejects_body_beyond_declared_length() {
        let state = UploadState::new("upload-1").with_length(5);

        let err = validate_receive_body(&Config::default(), &state, 6).unwrap_err();

        assert!(matches!(err, Error::SizeExceeded { size: 6, max: 5 }));
    }

    #[cfg(all(feature = "storage-memory", feature = "state-memory"))]
    #[tokio::test]
    async fn creation_with_upload_id_collision_preserves_existing_state() {
        use crate::hooks::NoopHookExecutor;
        use crate::protocol::RequestBody;
        use crate::state::memory::MemoryStateStore;
        use crate::storage::memory::MemoryStorage;

        // A pre-existing upload already owns this ID.
        let store = MemoryStateStore::new();
        let existing = UploadState::new("dup").with_length(999);
        store.set(&existing, WriteMode::CreateNew).await.unwrap();

        // A Creation-With-Upload request tries to create a new upload with the
        // same ID. The `CreateNew` write fails with `AlreadyExists`, and the
        // rollback must not touch the pre-existing upload's state record.
        let storage = MemoryStorage::new();
        let hooks = NoopHookExecutor::new();
        let config = Config::default();
        let request_info = HookRequestInfo::default();
        let receiver = ByteReceiver::new(&storage, &store, &hooks, &config, &request_info);

        let headers = Headers {
            upload_length: Some(5),
            content_type: Some("application/offset+octet-stream".to_string()),
            content_length: Some(5),
            ..Default::default()
        };
        let err = receiver
            .receive_creation_with_upload(
                &headers,
                UploadState::new("dup").with_length(5),
                RequestBody::from_chunk_stream(ChunkStream::from_bytes(bytes::Bytes::from_static(
                    b"Hello",
                ))),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, Error::AlreadyExists(_)));
        let survivor = store
            .get("dup")
            .await
            .unwrap()
            .expect("pre-existing upload state must survive a colliding creation");
        assert_eq!(
            survivor.length(),
            Some(999),
            "the surviving record must be the original upload, not the rejected one"
        );
    }
}
