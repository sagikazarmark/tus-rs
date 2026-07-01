use std::collections::HashMap;

use crate::config::{Config, Extension};
use crate::error::{Error, Result};
use crate::hooks::{
    HookContext, HookEvent, HookExecutor, HookRequestInfo, execute_post_best_effort,
};
use crate::protocol::UploadId;
use crate::state::{StateStore, UploadState};
use crate::storage::{ConcatRequest, Storage};

use super::run_pre_finish;

/// Loaded and validated final-upload parts.
#[derive(Debug, Clone)]
pub struct FinalUploadPlan {
    /// Part IDs in concatenation order.
    pub part_ids: Vec<String>,
    /// Part states in concatenation order.
    pub parts: Vec<UploadState>,
    /// Summarized final-upload state derived from the parts.
    pub status: FinalUploadStatus,
}

/// Final-upload state derived from its partial uploads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalUploadStatus {
    /// Known total length when all parts have known length.
    pub total_length: Option<u64>,
    /// Sum of current offsets for all parts.
    pub current_offset: u64,
    /// Whether every part is complete and total length is known.
    pub all_complete: bool,
}

impl FinalUploadStatus {
    /// Returns the offset the final upload state should expose internally.
    #[must_use]
    pub fn expected_offset(&self) -> u64 {
        if self.all_complete {
            self.total_length.unwrap_or(self.current_offset)
        } else {
            self.current_offset
        }
    }

    /// Returns whether storage materialization may run now.
    #[must_use]
    pub fn ready_to_materialize(&self) -> bool {
        self.all_complete && self.total_length.is_some()
    }
}

#[derive(Debug)]
pub(crate) struct CreatedFinalUpload {
    pub(crate) state: UploadState,
    pub(crate) status: FinalUploadStatus,
    pub(crate) response_headers: HashMap<String, String>,
}

impl CreatedFinalUpload {
    pub(crate) fn response_facts(&self) -> FinalUploadResponseFacts {
        FinalUploadResponseFacts {
            offset: self.status.all_complete.then_some(self.state.offset()),
            length: self.state.length(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FinalUploadResponseFacts {
    pub(crate) offset: Option<u64>,
    pub(crate) length: Option<u64>,
}

pub(crate) async fn create_final_upload<S, I, H>(
    storage: &S,
    state_store: &I,
    hooks: &H,
    config: &Config,
    request_info: &HookRequestInfo,
    mut state: UploadState,
    part_urls: Vec<String>,
) -> Result<CreatedFinalUpload>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    H: HookExecutor + ?Sized,
{
    let FinalUploadPlan {
        part_ids,
        parts,
        status,
    } = load_final_upload_plan(state_store, config, &part_urls).await?;

    apply_final_upload_plan(&mut state, part_ids, &status);
    let pre_create = run_pre_create(hooks, request_info, state).await?;
    state = pre_create.state;

    if status.all_complete {
        run_pre_finish(hooks, request_info, state.clone()).await?;
    }

    let handle = storage.create(state.id()).await?;
    state.set_storage_handle(handle);

    if status.ready_to_materialize() {
        let handle = storage
            .concat(ConcatRequest {
                target: state.require_storage_handle()?,
                parts: storage_handles(&parts)?,
            })
            .await?;
        state.set_storage_handle(handle);
    }

    state_store.set(&state, true).await?;

    run_post_event(hooks, HookEvent::PostCreate, request_info, &state).await;

    if status.all_complete {
        run_post_event(hooks, HookEvent::PostFinish, request_info, &state).await;
    }

    Ok(CreatedFinalUpload {
        state,
        status,
        response_headers: pre_create.response_headers,
    })
}

pub(crate) async fn repair_final_upload<S, I, H>(
    storage: &S,
    state_store: &I,
    hooks: &H,
    request_info: &HookRequestInfo,
    state: &mut UploadState,
) -> Result<bool>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    H: HookExecutor + ?Sized,
{
    if state.is_complete()
        && let Some(length) = state.length()
    {
        let actual_size = storage_size(storage, state).await?;
        if actual_size == length {
            return Ok(true);
        }
    }

    let Some(FinalUploadPlan { parts, status, .. }) =
        load_final_upload_status(state_store, state).await?
    else {
        return Ok(false);
    };

    let mut changed = false;

    if state.length() != status.total_length
        && let Some(total_length) = status.total_length
    {
        state.set_length(total_length);
        changed = true;
    }

    let actual_size = if status.ready_to_materialize() {
        Some(storage_size(storage, state).await?)
    } else {
        None
    };
    let needs_materialization = actual_size
        .zip(status.total_length)
        .is_some_and(|(actual_size, total_length)| actual_size != total_length);

    let will_complete =
        status.ready_to_materialize() && (!state.is_complete() || needs_materialization);

    if will_complete {
        let mut completed_state = state.clone();
        let total_length = status
            .total_length
            .expect("ready final upload has total length");
        completed_state.set_length(total_length);
        completed_state.set_offset(total_length);
        run_pre_finish(hooks, request_info, completed_state).await?;
    }

    if needs_materialization {
        let handle = storage
            .concat(ConcatRequest {
                target: state.require_storage_handle()?,
                parts: storage_handles(&parts)?,
            })
            .await?;
        state.set_storage_handle(handle);
        changed = true;
    }

    let expected_offset = status.expected_offset();
    if state.offset() != expected_offset {
        state.set_offset(expected_offset);
        changed = true;
    }

    if changed {
        state_store
            .set(state, false)
            .await
            .map_err(|err| Error::Internal(err.to_string()))?;
    }

    if will_complete {
        run_post_event(hooks, HookEvent::PostFinish, request_info, state).await;
    }

    Ok(true)
}

pub(crate) fn final_upload_response_facts(state: &UploadState) -> Option<FinalUploadResponseFacts> {
    state.is_final().then_some(FinalUploadResponseFacts {
        offset: state.is_complete().then_some(state.offset()),
        length: state.length(),
    })
}

/// Loads and validates parts referenced by a final Upload-Concat header.
pub async fn load_final_upload_plan<I>(
    state_store: &I,
    config: &Config,
    part_urls: &[String],
) -> Result<FinalUploadPlan>
where
    I: StateStore + ?Sized,
{
    let allow_unfinished = config.has_extension(Extension::ConcatenationUnfinished);
    let mut part_ids = Vec::with_capacity(part_urls.len());
    let mut parts = Vec::with_capacity(part_urls.len());

    for url in part_urls {
        let id = extract_partial_id(url, config.base_path_str()).ok_or_else(|| {
            Error::InvalidHeader {
                header: "Upload-Concat",
                message: format!(
                    "partial URL not under base path {:?}: {}",
                    config.base_path_str(),
                    url
                ),
            }
        })?;

        let part_state =
            state_store
                .get(id.as_str())
                .await?
                .ok_or_else(|| Error::InvalidHeader {
                    header: "Upload-Concat",
                    message: format!("partial upload not found: {}", id.as_str()),
                })?;

        if !part_state.is_partial() {
            return Err(Error::NotPartialUpload(id.clone().into_string()));
        }

        if !part_state.is_complete() && !allow_unfinished {
            return Err(Error::IncompleteUpload(id.clone().into_string()));
        }

        if part_state.is_expired() {
            return Err(Error::Expired(id.clone().into_string()));
        }

        part_ids.push(id.into_string());
        parts.push(part_state);
    }

    let status = summarize_final_parts(&parts)?;

    Ok(FinalUploadPlan {
        part_ids,
        parts,
        status,
    })
}

/// Loads final-upload parts referenced by persisted final upload state.
pub async fn load_final_upload_status<I>(
    state_store: &I,
    state: &UploadState,
) -> Result<Option<FinalUploadPlan>>
where
    I: StateStore + ?Sized,
{
    let Some(part_ids) = state.parts().map(|parts| parts.to_vec()) else {
        return Ok(None);
    };

    let mut parts = Vec::with_capacity(part_ids.len());
    for part_id in &part_ids {
        let part_state = state_store
            .get(part_id)
            .await
            .map_err(|err| Error::Internal(err.to_string()))?
            .ok_or_else(|| {
                Error::Internal(format!(
                    "final upload {} references missing partial {}",
                    state.id(),
                    part_id
                ))
            })?;

        if part_state.is_expired() {
            return Err(Error::Expired(part_id.clone()));
        }

        parts.push(part_state);
    }

    let status = summarize_final_parts(&parts)?;
    Ok(Some(FinalUploadPlan {
        part_ids,
        parts,
        status,
    }))
}

/// Summarizes final-upload state derived from partial uploads.
pub fn summarize_final_parts(parts: &[UploadState]) -> Result<FinalUploadStatus> {
    let mut total_length = 0_u64;
    let mut current_offset = 0_u64;
    let mut all_complete = true;
    let mut length_known = true;

    for part_state in parts {
        if part_state.is_expired() {
            return Err(Error::Expired(part_state.id().to_string()));
        }

        current_offset = current_offset.saturating_add(part_state.offset());

        match part_state.length() {
            Some(length) => total_length = total_length.saturating_add(length),
            None => {
                length_known = false;
                all_complete = false;
            }
        }

        if !part_state.is_complete() {
            all_complete = false;
        }
    }

    Ok(FinalUploadStatus {
        total_length: length_known.then_some(total_length),
        current_offset,
        all_complete: all_complete && length_known,
    })
}

fn apply_final_upload_plan(
    state: &mut UploadState,
    part_ids: Vec<String>,
    status: &FinalUploadStatus,
) {
    state.mark_final(part_ids);
    if let Some(total_length) = status.total_length {
        state.set_length(total_length);
    }
    state.set_offset(status.expected_offset());
}

struct PreCreateDecision {
    state: UploadState,
    response_headers: HashMap<String, String>,
}

async fn run_pre_create<H>(
    hooks: &H,
    request_info: &HookRequestInfo,
    state: UploadState,
) -> Result<PreCreateDecision>
where
    H: HookExecutor + ?Sized,
{
    let hook_ctx = HookContext::new(HookEvent::PreCreate, state.clone(), request_info.clone());
    let pre_result = hooks.execute_pre(&hook_ctx).await?;

    if !pre_result.proceed {
        return Err(Error::HookRejected {
            status_code: pre_result.reject_status.unwrap_or(400),
            message: pre_result.reject_message.unwrap_or_default(),
        });
    }

    Ok(PreCreateDecision {
        state: {
            let mut state = state;
            if let Some(metadata) = pre_result.metadata {
                state.set_metadata(metadata);
            }
            state
        },
        response_headers: pre_result.response_headers,
    })
}

async fn run_post_event<H>(
    hooks: &H,
    event: HookEvent,
    request_info: &HookRequestInfo,
    state: &UploadState,
) where
    H: HookExecutor + ?Sized,
{
    let ctx = HookContext::new(event, state.clone(), request_info.clone());
    execute_post_best_effort(hooks, &ctx).await;
}

async fn storage_size<S>(storage: &S, state: &UploadState) -> Result<u64>
where
    S: Storage + ?Sized,
{
    let Some(handle) = state.storage_handle() else {
        return Ok(0);
    };

    Ok(storage
        .size(&handle)
        .await
        .map_err(|err| Error::Internal(err.to_string()))?
        .unwrap_or(0))
}

fn storage_handles(parts: &[UploadState]) -> Result<Vec<crate::storage::StorageHandle>> {
    parts
        .iter()
        .map(UploadState::require_storage_handle)
        .collect()
}

/// Extracts an upload ID from a URL present in `Upload-Concat: final;<parts>`.
fn extract_partial_id(url: &str, base_path: &str) -> Option<UploadId> {
    let path = if let Some(rest) = url.split_once("://") {
        match rest.1.find('/') {
            Some(idx) => &rest.1[idx..],
            None => return None,
        }
    } else {
        url
    };

    let path = path.split(['?', '#']).next().unwrap_or(path);
    let expected_prefix = if base_path.ends_with('/') {
        base_path.to_string()
    } else {
        format!("{base_path}/")
    };

    let id = path.strip_prefix(&expected_prefix)?;
    if id.is_empty() || id.contains('/') {
        return None;
    }
    id.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::UploadId;
    use crate::state::UploadState;
    use chrono::{Duration, Utc};

    #[test]
    fn summarize_final_parts_reports_complete_known_length() {
        let mut part1 = UploadState::new("part-1").with_length(4).as_partial();
        part1.set_offset(4);
        let mut part2 = UploadState::new("part-2").with_length(5).as_partial();
        part2.set_offset(5);

        let status = summarize_final_parts(&[part1, part2]).unwrap();

        assert_eq!(status.total_length, Some(9));
        assert_eq!(status.current_offset, 9);
        assert!(status.all_complete);
        assert_eq!(status.expected_offset(), 9);
        assert!(status.ready_to_materialize());
    }

    #[test]
    fn summarize_final_parts_reports_incomplete_deferred_part() {
        let mut part = UploadState::new("part-1").as_partial();
        part.set_offset(5);

        let status = summarize_final_parts(&[part]).unwrap();

        assert_eq!(status.total_length, None);
        assert_eq!(status.current_offset, 5);
        assert!(!status.all_complete);
        assert_eq!(status.expected_offset(), 5);
        assert!(!status.ready_to_materialize());
    }

    #[test]
    fn summarize_final_parts_rejects_expired_part() {
        let part = UploadState::new("part-1")
            .with_length(5)
            .with_expiration(Utc::now() - Duration::seconds(1))
            .as_partial();

        let err = summarize_final_parts(&[part]).unwrap_err();

        assert!(matches!(err, crate::error::Error::Expired(id) if id == "part-1"));
    }

    #[test]
    fn final_upload_response_facts_expose_offset_only_when_complete() {
        let mut incomplete = UploadState::new("final-1").with_length(10);
        incomplete.mark_final(vec!["part-1".to_string()]);
        incomplete.set_offset(5);

        let facts = final_upload_response_facts(&incomplete).unwrap();

        assert_eq!(facts.offset, None);
        assert_eq!(facts.length, Some(10));

        let mut complete = incomplete;
        complete.set_offset(10);

        let facts = final_upload_response_facts(&complete).unwrap();

        assert_eq!(facts.offset, Some(10));
        assert_eq!(facts.length, Some(10));
    }

    #[test]
    fn extract_partial_id_accepts_relative_and_absolute_urls() {
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
    }

    #[test]
    fn extract_partial_id_rejects_urls_outside_base_path() {
        assert_eq!(extract_partial_id("/other/abc", "/files"), None);
        assert_eq!(extract_partial_id("abc", "/files"), None);
        assert_eq!(extract_partial_id("/files", "/files"), None);
        assert_eq!(extract_partial_id("/files/a/b", "/files"), None);
        assert_eq!(extract_partial_id("https://", "/files"), None);
    }
}

#[cfg(all(
    test,
    feature = "storage-memory",
    feature = "state-memory",
    not(feature = "local-futures")
))]
mod materialization_tests {
    use super::*;
    use crate::hooks::{HookChain, HookEvent, PreHookResult};
    use crate::state::memory::MemoryStateStore;
    use crate::storage::{AppendRequest, ChunkStream, StorageReader, memory::MemoryStorage};
    use bytes::{Bytes, BytesMut};
    use futures::StreamExt;
    use std::sync::{Arc, Mutex};

    async fn store_partial(
        storage: &MemoryStorage,
        store: &MemoryStateStore,
        id: &str,
        bytes: &'static [u8],
    ) {
        let mut part = UploadState::new(id)
            .with_length(bytes.len() as u64)
            .as_partial();
        let handle = storage.create(part.id()).await.unwrap();
        part.set_storage_handle(handle);
        let handle = storage
            .append(AppendRequest {
                handle: part.require_storage_handle().unwrap(),
                expected_offset: part.offset(),
                data: ChunkStream::from_bytes(Bytes::from_static(bytes)),
                completes_upload: true,
            })
            .await
            .unwrap();
        part.set_storage_handle(handle);
        part.set_offset(bytes.len() as u64);
        store.set(&part, true).await.unwrap();
    }

    async fn stored_bytes(storage: &MemoryStorage, state: &UploadState) -> Bytes {
        let body = storage
            .get_stream(&state.require_storage_handle().unwrap())
            .await
            .unwrap();
        body.collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|chunk| chunk.unwrap())
            .fold(BytesMut::new(), |mut acc, chunk| {
                acc.extend_from_slice(&chunk);
                acc
            })
            .freeze()
    }

    #[tokio::test]
    async fn create_final_upload_runs_hooks_materializes_and_persists_state() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        store_partial(&storage, &store, "part-1", b"ABCD").await;
        store_partial(&storage, &store, "part-2", b"EF").await;

        let events = Arc::new(Mutex::new(Vec::new()));
        let hooks = HookChain::new()
            .on_pre_create({
                let events = Arc::clone(&events);
                move |_| {
                    let events = Arc::clone(&events);
                    async move {
                        events.lock().unwrap().push(HookEvent::PreCreate);
                        Ok(PreHookResult::proceed().with_header("x-final", "created"))
                    }
                }
            })
            .on_pre_finish({
                let events = Arc::clone(&events);
                move |_| {
                    let events = Arc::clone(&events);
                    async move {
                        events.lock().unwrap().push(HookEvent::PreFinish);
                        Ok(PreHookResult::proceed())
                    }
                }
            })
            .on_post_create({
                let events = Arc::clone(&events);
                move |_| {
                    let events = Arc::clone(&events);
                    async move {
                        events.lock().unwrap().push(HookEvent::PostCreate);
                        Ok(())
                    }
                }
            })
            .on_post_finish({
                let events = Arc::clone(&events);
                move |_| {
                    let events = Arc::clone(&events);
                    async move {
                        events.lock().unwrap().push(HookEvent::PostFinish);
                        Ok(())
                    }
                }
            });

        let created = create_final_upload(
            &storage,
            &store,
            &hooks,
            &Config::default().with_extension(Extension::Concatenation),
            &HookRequestInfo::default(),
            UploadState::new("final-1"),
            vec!["/files/part-1".to_string(), "/files/part-2".to_string()],
        )
        .await
        .unwrap();

        assert_eq!(created.status.total_length, Some(6));
        assert_eq!(created.state.offset(), 6);
        assert_eq!(created.state.length(), Some(6));
        assert_eq!(created.response_headers.get("x-final").unwrap(), "created");
        assert_eq!(
            stored_bytes(&storage, &created.state).await.as_ref(),
            b"ABCDEF"
        );

        let stored = store.get("final-1").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 6);
        assert_eq!(stored.length(), Some(6));
        assert!(stored.is_final());

        assert_eq!(
            *events.lock().unwrap(),
            vec![
                HookEvent::PreCreate,
                HookEvent::PreFinish,
                HookEvent::PostCreate,
                HookEvent::PostFinish,
            ]
        );
    }

    #[tokio::test]
    async fn pre_finish_rejection_blocks_final_upload_creation_materialization() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        store_partial(&storage, &store, "part-1", b"ABCD").await;
        let hooks = HookChain::new()
            .on_pre_finish(|_| async { Ok(PreHookResult::reject(403, "finish blocked")) });

        let err = create_final_upload(
            &storage,
            &store,
            &hooks,
            &Config::default().with_extension(Extension::Concatenation),
            &HookRequestInfo::default(),
            UploadState::new("final-1"),
            vec!["/files/part-1".to_string()],
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
        assert!(store.get("final-1").await.unwrap().is_none());
        assert_eq!(storage.len(), 1);
    }

    #[tokio::test]
    async fn repair_final_upload_materializes_complete_parts_and_persists_state() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        store_partial(&storage, &store, "part-1", b"ABCD").await;
        store_partial(&storage, &store, "part-2", b"EFGH").await;

        let mut final_upload = UploadState::new("final-1");
        let handle = storage.create(final_upload.id()).await.unwrap();
        final_upload.set_storage_handle(handle);
        final_upload.mark_final(vec!["part-1".to_string(), "part-2".to_string()]);
        final_upload.set_length(8);
        final_upload.set_offset(4);
        store.set(&final_upload, true).await.unwrap();

        let events = Arc::new(Mutex::new(Vec::new()));
        let hooks = HookChain::new()
            .on_pre_finish({
                let events = Arc::clone(&events);
                move |_| {
                    let events = Arc::clone(&events);
                    async move {
                        events.lock().unwrap().push(HookEvent::PreFinish);
                        Ok(PreHookResult::proceed())
                    }
                }
            })
            .on_post_finish({
                let events = Arc::clone(&events);
                move |_| {
                    let events = Arc::clone(&events);
                    async move {
                        events.lock().unwrap().push(HookEvent::PostFinish);
                        Ok(())
                    }
                }
            });

        let handled = repair_final_upload(
            &storage,
            &store,
            &hooks,
            &HookRequestInfo::default(),
            &mut final_upload,
        )
        .await
        .unwrap();

        assert!(handled);
        assert_eq!(final_upload.offset(), 8);
        assert_eq!(
            stored_bytes(&storage, &final_upload).await.as_ref(),
            b"ABCDEFGH"
        );

        let stored = store.get("final-1").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 8);
        assert_eq!(stored.length(), Some(8));
        assert_eq!(
            *events.lock().unwrap(),
            vec![HookEvent::PreFinish, HookEvent::PostFinish]
        );
    }

    #[tokio::test]
    async fn pre_finish_rejection_blocks_repairing_complete_final_storage() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        store_partial(&storage, &store, "part-1", b"ABCD").await;

        let mut final_upload = UploadState::new("final-1");
        let handle = storage.create(final_upload.id()).await.unwrap();
        final_upload.set_storage_handle(handle);
        final_upload.mark_final(vec!["part-1".to_string()]);
        final_upload.set_length(4);
        final_upload.set_offset(4);
        store.set(&final_upload, true).await.unwrap();

        let hooks = HookChain::new()
            .on_pre_finish(|_| async { Ok(PreHookResult::reject(403, "finish blocked")) });
        let err = repair_final_upload(
            &storage,
            &store,
            &hooks,
            &HookRequestInfo::default(),
            &mut final_upload,
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
        let stored = store.get("final-1").await.unwrap().unwrap();
        assert_eq!(stored.offset(), 4);
        assert_eq!(
            storage
                .size(&stored.require_storage_handle().unwrap())
                .await
                .unwrap(),
            Some(0)
        );
    }
}
