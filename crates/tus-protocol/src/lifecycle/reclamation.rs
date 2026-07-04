use chrono::{DateTime, Utc};

use crate::error::{Error, Result};
use crate::locking::Locker;
use crate::state::StateStore;
use crate::storage::Storage;

use super::prepare_upload_reclamation_access;

/// Outcomes produced by an expired upload reclamation scan.
#[derive(Debug, Default)]
pub struct ExpiredUploadReclamationReport {
    outcomes: Vec<ExpiredUploadReclamationOutcome>,
}

impl ExpiredUploadReclamationReport {
    /// Returns the per-upload reclamation outcomes in candidate order.
    #[must_use]
    pub fn outcomes(&self) -> &[ExpiredUploadReclamationOutcome] {
        &self.outcomes
    }

    /// Returns the number of uploads whose data and state were removed.
    #[must_use]
    pub fn removed(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ExpiredUploadReclamationOutcome::Removed { .. }))
            .count()
    }

    /// Returns whether any candidate failed during reclamation.
    #[must_use]
    pub fn has_failures(&self) -> bool {
        self.outcomes
            .iter()
            .any(ExpiredUploadReclamationOutcome::is_failure)
    }

    fn push(&mut self, outcome: ExpiredUploadReclamationOutcome) {
        self.outcomes.push(outcome);
    }
}

/// Reclamation outcome for a single expired upload candidate.
#[derive(Debug)]
#[non_exhaustive]
pub enum ExpiredUploadReclamationOutcome {
    /// The upload data and state were removed.
    #[non_exhaustive]
    Removed {
        /// Candidate upload ID.
        upload_id: String,
    },
    /// The upload was locked by another operation and was not reclaimed.
    #[non_exhaustive]
    Locked {
        /// Candidate upload ID.
        upload_id: String,
    },
    /// The upload state disappeared after the candidate was listed.
    #[non_exhaustive]
    MissingState {
        /// Candidate upload ID.
        upload_id: String,
    },
    /// The upload state was no longer expired after locking and reloading it.
    #[non_exhaustive]
    NoLongerExpired {
        /// Candidate upload ID.
        upload_id: String,
    },
    /// Upload data deletion failed, so state deletion was not attempted.
    #[non_exhaustive]
    StorageDeleteFailed {
        /// Candidate upload ID.
        upload_id: String,
        /// Storage deletion error.
        error: Error,
    },
    /// Upload state deletion failed after upload data was deleted.
    #[non_exhaustive]
    StateDeleteFailed {
        /// Candidate upload ID.
        upload_id: String,
        /// State deletion error.
        error: Error,
    },
    /// Reclamation could not be attempted or completed because a preparatory
    /// step failed (locking, loading, or completion reconciliation). The scan
    /// records the failure and moves on to the next candidate.
    #[non_exhaustive]
    Failed {
        /// Candidate upload ID.
        upload_id: String,
        /// The error that prevented reclamation.
        error: Error,
    },
}

impl ExpiredUploadReclamationOutcome {
    /// Returns the candidate upload ID associated with this outcome.
    #[must_use]
    pub fn upload_id(&self) -> &str {
        match self {
            Self::Removed { upload_id }
            | Self::Locked { upload_id }
            | Self::MissingState { upload_id }
            | Self::NoLongerExpired { upload_id }
            | Self::StorageDeleteFailed { upload_id, .. }
            | Self::StateDeleteFailed { upload_id, .. }
            | Self::Failed { upload_id, .. } => upload_id,
        }
    }

    /// Returns whether the outcome represents a failed reclamation.
    #[must_use]
    pub fn is_failure(&self) -> bool {
        matches!(
            self,
            Self::StorageDeleteFailed { .. }
                | Self::StateDeleteFailed { .. }
                | Self::Failed { .. }
        )
    }
}

/// Reclaims protocol-expired uploads by deleting upload data before upload state.
///
/// Candidates are loaded from [`StateStore::list_expired`]. Each candidate is
/// locked with [`Locker::try_lock`], reloaded, checked for current expiration,
/// and then reclaimed. Per-candidate failures — whether a preparatory step
/// (locking, loading, completion reconciliation) or a deletion — are reported
/// as outcomes so the scan continues to the remaining candidates instead of
/// aborting and discarding the report. Only a failure to list candidates in the
/// first place propagates as an error.
///
/// Reclamation does not retain an expired partial upload because a planned final
/// upload references it, and does not cascade deletion to referencing final
/// uploads. A final upload that is itself expired is reclaimed as its own
/// candidate; otherwise protocol reads treat expired or missing referenced parts
/// as making the planned final upload expired until it has been materialized.
pub async fn reclaim_expired_uploads<S, I, L>(
    storage: &S,
    state_store: &I,
    locker: &L,
    before: DateTime<Utc>,
) -> Result<ExpiredUploadReclamationReport>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
{
    let mut report = ExpiredUploadReclamationReport::default();
    let upload_ids = state_store.list_expired(before).await?;

    for upload_id in upload_ids {
        report.push(reclaim_expired_upload(storage, state_store, locker, upload_id).await);
    }

    Ok(report)
}

async fn reclaim_expired_upload<S, I, L>(
    storage: &S,
    state_store: &I,
    locker: &L,
    upload_id: String,
) -> ExpiredUploadReclamationOutcome
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
{
    let _guard = match locker.try_lock(&upload_id).await {
        Ok(Some(guard)) => guard,
        Ok(None) => return ExpiredUploadReclamationOutcome::Locked { upload_id },
        Err(error) => return ExpiredUploadReclamationOutcome::Failed { upload_id, error },
    };

    let mut state = match state_store.get(&upload_id).await {
        Ok(Some(state)) => state,
        Ok(None) => return ExpiredUploadReclamationOutcome::MissingState { upload_id },
        Err(error) => return ExpiredUploadReclamationOutcome::Failed { upload_id, error },
    };

    match prepare_upload_reclamation_access(storage, state_store, &mut state).await {
        Ok(true) => {}
        Ok(false) => return ExpiredUploadReclamationOutcome::NoLongerExpired { upload_id },
        Err(error) => return ExpiredUploadReclamationOutcome::Failed { upload_id, error },
    }

    if let Some(handle) = state.storage_handle()
        && let Err(error) = storage.delete(&handle).await
    {
        return ExpiredUploadReclamationOutcome::StorageDeleteFailed { upload_id, error };
    }

    if let Err(error) = state_store.delete(state.id()).await {
        return ExpiredUploadReclamationOutcome::StateDeleteFailed { upload_id, error };
    }

    ExpiredUploadReclamationOutcome::Removed { upload_id }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};
    use std::time::Duration as StdDuration;

    use async_trait::async_trait;
    use chrono::Duration;

    use super::*;
    use crate::error::Error;
    use crate::locking::LockGuard;
    use crate::state::UploadState;
    use crate::storage::{AppendRequest, ConcatRequest, StorageHandle};

    type OperationLog = Arc<Mutex<Vec<String>>>;

    #[tokio::test]
    async fn reclaim_expired_uploads_removes_storage_and_state() {
        let storage = TestStorage::default();
        let state_store = TestStateStore::default();
        let locker = TestLocker::default();
        let state = expired_state("upload-1", "data-1");
        state_store.insert_candidate("upload-1");
        state_store.insert_state(state);

        let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
            .await
            .unwrap();

        assert_eq!(report.removed(), 1);
        assert!(!report.has_failures());
        assert!(matches!(
            report.outcomes(),
            [ExpiredUploadReclamationOutcome::Removed { upload_id }]
                if upload_id == "upload-1"
        ));
        assert_eq!(storage.deleted(), vec!["data-1"]);
        assert_eq!(state_store.deleted(), vec!["upload-1"]);
        assert!(!state_store.contains("upload-1"));
    }

    #[tokio::test]
    async fn reclaim_expired_uploads_locks_reloads_and_deletes_data_before_state() {
        let operations = Arc::new(Mutex::new(Vec::new()));
        let storage = TestStorage::with_operations(Arc::clone(&operations));
        let state_store = TestStateStore::with_operations(Arc::clone(&operations));
        let locker = TestLocker::with_operations(Arc::clone(&operations));
        state_store.insert_candidate("upload-1");
        state_store.insert_state(expired_state("upload-1", "data-1"));

        let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
            .await
            .unwrap();

        assert_eq!(report.removed(), 1);
        assert_eq!(
            *operations.lock().unwrap(),
            vec![
                "list_expired".to_string(),
                "try_lock upload-1".to_string(),
                "get upload-1".to_string(),
                "delete_storage data-1".to_string(),
                "delete_state upload-1".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn reclaim_expired_uploads_reports_locked_uploads() {
        let storage = TestStorage::default();
        let state_store = TestStateStore::default();
        let locker = TestLocker::default();
        state_store.insert_candidate("upload-1");
        state_store.insert_state(expired_state("upload-1", "data-1"));
        locker.mark_locked("upload-1");

        let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
            .await
            .unwrap();

        assert_eq!(report.removed(), 0);
        assert!(matches!(
            report.outcomes(),
            [ExpiredUploadReclamationOutcome::Locked { upload_id }]
                if upload_id == "upload-1"
        ));
        assert!(storage.deleted().is_empty());
        assert!(state_store.deleted().is_empty());
        assert!(state_store.contains("upload-1"));
    }

    #[tokio::test]
    async fn reclaim_expired_uploads_reports_missing_state_after_listing() {
        let storage = TestStorage::default();
        let state_store = TestStateStore::default();
        let locker = TestLocker::default();
        state_store.insert_candidate("upload-1");

        let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
            .await
            .unwrap();

        assert_eq!(report.removed(), 0);
        assert!(matches!(
            report.outcomes(),
            [ExpiredUploadReclamationOutcome::MissingState { upload_id }]
                if upload_id == "upload-1"
        ));
        assert!(storage.deleted().is_empty());
        assert!(state_store.deleted().is_empty());
    }

    #[tokio::test]
    async fn reclaim_expired_uploads_reports_no_longer_expired_after_locking() {
        let operations = Arc::new(Mutex::new(Vec::new()));
        let storage = TestStorage::with_operations(Arc::clone(&operations));
        let state_store = TestStateStore::with_operations(Arc::clone(&operations));
        let locker = TestLocker::with_operations(Arc::clone(&operations));
        state_store.insert_candidate("upload-1");
        state_store.insert_state(active_state("upload-1", "data-1"));

        let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
            .await
            .unwrap();

        assert_eq!(report.removed(), 0);
        assert!(matches!(
            report.outcomes(),
            [ExpiredUploadReclamationOutcome::NoLongerExpired { upload_id }]
                if upload_id == "upload-1"
        ));
        assert!(storage.deleted().is_empty());
        assert!(state_store.deleted().is_empty());
        assert!(state_store.contains("upload-1"));
        assert_eq!(
            *operations.lock().unwrap(),
            vec![
                "list_expired".to_string(),
                "try_lock upload-1".to_string(),
                "get upload-1".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn reclaim_expired_uploads_passes_storage_owned_handle_facts_to_delete() {
        let storage = TestStorage::default();
        let state_store = TestStateStore::default();
        let locker = TestLocker::default();
        let mut handle = StorageHandle::new("data-1");
        handle.set_internal("multipart-upload-id", "session-1");
        let mut state =
            UploadState::new("upload-1").with_expiration(Utc::now() - Duration::hours(1));
        state.set_storage_handle(handle.clone());
        state_store.insert_candidate("upload-1");
        state_store.insert_state(state);

        let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
            .await
            .unwrap();

        assert_eq!(report.removed(), 1);
        assert_eq!(storage.deleted_handles(), vec![handle]);
    }

    #[tokio::test]
    async fn reclaim_expired_uploads_does_not_retain_or_cascade_referenced_partials() {
        let storage = TestStorage::default();
        let state_store = TestStateStore::default();
        let locker = TestLocker::default();
        let partial = expired_state("part-1", "part-data").as_partial();
        let mut final_upload = active_state("final-1", "final-data")
            .with_length(10)
            .as_final(vec!["part-1".to_string()]);
        final_upload.set_offset(5);
        state_store.insert_candidate("part-1");
        state_store.insert_state(partial);
        state_store.insert_state(final_upload);

        let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
            .await
            .unwrap();

        assert_eq!(report.removed(), 1);
        assert_eq!(storage.deleted(), vec!["part-data"]);
        assert!(!state_store.contains("part-1"));
        assert!(state_store.contains("final-1"));
    }

    #[tokio::test]
    async fn reclaim_expired_uploads_recovers_storage_completed_upload_before_delete() {
        let storage = TestStorage::default();
        let state_store = TestStateStore::default();
        let locker = TestLocker::default();
        let mut state = expired_state("upload-1", "data-1").with_length(5);
        state.set_offset(0);
        state_store.insert_candidate("upload-1");
        state_store.insert_state(state);
        storage.set_size("data-1", 5);

        let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
            .await
            .unwrap();
        let recovered = state_store.get("upload-1").await.unwrap().unwrap();

        assert_eq!(report.removed(), 0);
        assert!(matches!(
            report.outcomes(),
            [ExpiredUploadReclamationOutcome::NoLongerExpired { upload_id }]
                if upload_id == "upload-1"
        ));
        assert!(storage.deleted().is_empty());
        assert!(state_store.deleted().is_empty());
        assert_eq!(recovered.offset(), 5);
    }

    #[tokio::test]
    async fn reclaim_expired_uploads_reports_delete_failures() {
        let storage = TestStorage::default();
        let state_store = TestStateStore::default();
        let locker = TestLocker::default();
        state_store.insert_candidate("storage-fails");
        state_store.insert_candidate("state-fails");
        state_store.insert_state(expired_state("storage-fails", "data-storage-fails"));
        state_store.insert_state(expired_state("state-fails", "data-state-fails"));
        storage.fail_delete("data-storage-fails");
        state_store.fail_delete("state-fails");

        let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
            .await
            .unwrap();

        assert_eq!(report.removed(), 0);
        assert!(report.has_failures());
        assert!(matches!(
            &report.outcomes()[0],
            ExpiredUploadReclamationOutcome::StorageDeleteFailed { upload_id, error }
                if upload_id == "storage-fails"
                    && error.to_string().contains("storage delete failed")
        ));
        assert!(matches!(
            &report.outcomes()[1],
            ExpiredUploadReclamationOutcome::StateDeleteFailed { upload_id, error }
                if upload_id == "state-fails"
                    && error.to_string().contains("state delete failed")
        ));
        assert_eq!(storage.deleted(), vec!["data-state-fails"]);
        assert!(state_store.deleted().is_empty());
        assert!(state_store.contains("storage-fails"));
        assert!(state_store.contains("state-fails"));
    }

    #[tokio::test]
    async fn reclaim_expired_uploads_reports_preparatory_failures_and_continues() {
        let storage = TestStorage::default();
        let state_store = TestStateStore::default();
        let locker = TestLocker::default();
        // First candidate cannot be locked (transient locker error); the scan
        // must record the failure and still reclaim the second candidate.
        state_store.insert_candidate("lock-fails");
        state_store.insert_candidate("upload-2");
        state_store.insert_state(expired_state("lock-fails", "data-1"));
        state_store.insert_state(expired_state("upload-2", "data-2"));
        locker.fail_lock("lock-fails");

        let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
            .await
            .unwrap();

        assert_eq!(report.removed(), 1);
        assert!(report.has_failures());
        assert!(matches!(
            &report.outcomes()[0],
            ExpiredUploadReclamationOutcome::Failed { upload_id, .. }
                if upload_id == "lock-fails"
        ));
        assert!(matches!(
            &report.outcomes()[1],
            ExpiredUploadReclamationOutcome::Removed { upload_id }
                if upload_id == "upload-2"
        ));
        assert_eq!(storage.deleted(), vec!["data-2"]);
    }

    #[tokio::test]
    async fn reclaim_expired_uploads_reports_state_load_failures() {
        let storage = TestStorage::default();
        let state_store = TestStateStore::default();
        let locker = TestLocker::default();
        state_store.insert_candidate("get-fails");
        state_store.insert_state(expired_state("get-fails", "data-1"));
        state_store.fail_get("get-fails");

        let report = reclaim_expired_uploads(&storage, &state_store, &locker, Utc::now())
            .await
            .unwrap();

        assert_eq!(report.removed(), 0);
        assert!(report.has_failures());
        assert!(matches!(
            &report.outcomes()[0],
            ExpiredUploadReclamationOutcome::Failed { upload_id, .. }
                if upload_id == "get-fails"
        ));
    }

    fn expired_state(id: &str, storage_key: &str) -> UploadState {
        let mut state = UploadState::new(id).with_expiration(Utc::now() - Duration::hours(1));
        state.set_storage_handle(StorageHandle::new(storage_key));
        state
    }

    fn active_state(id: &str, storage_key: &str) -> UploadState {
        let mut state = UploadState::new(id).with_expiration(Utc::now() + Duration::hours(1));
        state.set_storage_handle(StorageHandle::new(storage_key));
        state
    }

    #[derive(Default)]
    struct TestStorage {
        deleted: Mutex<Vec<StorageHandle>>,
        fail_delete: Mutex<HashSet<String>>,
        sizes: Mutex<HashMap<String, u64>>,
        operations: Option<OperationLog>,
    }

    impl TestStorage {
        fn with_operations(operations: OperationLog) -> Self {
            Self {
                operations: Some(operations),
                ..Self::default()
            }
        }

        fn set_size(&self, key: &str, size: u64) {
            self.sizes.lock().unwrap().insert(key.to_string(), size);
        }

        fn fail_delete(&self, key: &str) {
            self.fail_delete.lock().unwrap().insert(key.to_string());
        }

        fn deleted(&self) -> Vec<String> {
            self.deleted
                .lock()
                .unwrap()
                .iter()
                .map(|handle| handle.key().to_string())
                .collect()
        }

        fn deleted_handles(&self) -> Vec<StorageHandle> {
            self.deleted.lock().unwrap().clone()
        }

        fn record(&self, operation: impl Into<String>) {
            if let Some(operations) = &self.operations {
                operations.lock().unwrap().push(operation.into());
            }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl Storage for TestStorage {
        fn name(&self) -> &'static str {
            "test"
        }

        async fn create(&self, upload_id: &str) -> Result<StorageHandle> {
            Ok(StorageHandle::new(upload_id))
        }

        async fn append(&self, request: AppendRequest) -> Result<StorageHandle> {
            Ok(request.handle)
        }

        async fn concat(&self, request: ConcatRequest) -> Result<StorageHandle> {
            Ok(request.target)
        }

        async fn delete(&self, handle: &StorageHandle) -> Result<()> {
            if self.fail_delete.lock().unwrap().contains(handle.key()) {
                return Err(Error::Internal(format!(
                    "storage delete failed for {}",
                    handle.key()
                )));
            }

            self.record(format!("delete_storage {}", handle.key()));
            self.deleted.lock().unwrap().push(handle.clone());
            Ok(())
        }

        async fn size(&self, handle: &StorageHandle) -> Result<Option<u64>> {
            Ok(self.sizes.lock().unwrap().get(handle.key()).copied())
        }
    }

    #[derive(Default)]
    struct TestStateStore {
        states: Mutex<HashMap<String, UploadState>>,
        expired: Mutex<Vec<String>>,
        deleted: Mutex<Vec<String>>,
        fail_delete: Mutex<HashSet<String>>,
        fail_get: Mutex<HashSet<String>>,
        operations: Option<OperationLog>,
    }

    impl TestStateStore {
        fn with_operations(operations: OperationLog) -> Self {
            Self {
                operations: Some(operations),
                ..Self::default()
            }
        }

        fn insert_candidate(&self, upload_id: &str) {
            self.expired.lock().unwrap().push(upload_id.to_string());
        }

        fn insert_state(&self, state: UploadState) {
            self.states
                .lock()
                .unwrap()
                .insert(state.id().to_string(), state);
        }

        fn fail_delete(&self, upload_id: &str) {
            self.fail_delete
                .lock()
                .unwrap()
                .insert(upload_id.to_string());
        }

        fn fail_get(&self, upload_id: &str) {
            self.fail_get.lock().unwrap().insert(upload_id.to_string());
        }

        fn deleted(&self) -> Vec<String> {
            self.deleted.lock().unwrap().clone()
        }

        fn contains(&self, upload_id: &str) -> bool {
            self.states.lock().unwrap().contains_key(upload_id)
        }

        fn record(&self, operation: impl Into<String>) {
            if let Some(operations) = &self.operations {
                operations.lock().unwrap().push(operation.into());
            }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl StateStore for TestStateStore {
        fn name(&self) -> &'static str {
            "test"
        }

        async fn set(&self, state: &UploadState, create: bool) -> Result<()> {
            let mut states = self.states.lock().unwrap();
            if create && states.contains_key(state.id()) {
                return Err(Error::AlreadyExists(state.id().to_string()));
            }
            states.insert(state.id().to_string(), state.clone());
            Ok(())
        }

        async fn get(&self, id: &str) -> Result<Option<UploadState>> {
            self.record(format!("get {id}"));
            if self.fail_get.lock().unwrap().contains(id) {
                return Err(Error::Internal(format!("state get failed for {id}")));
            }
            Ok(self.states.lock().unwrap().get(id).cloned())
        }

        async fn delete(&self, id: &str) -> Result<()> {
            if self.fail_delete.lock().unwrap().contains(id) {
                return Err(Error::Internal(format!("state delete failed for {id}")));
            }

            self.record(format!("delete_state {id}"));
            self.deleted.lock().unwrap().push(id.to_string());
            self.states.lock().unwrap().remove(id);
            Ok(())
        }

        async fn list_expired(&self, _before: DateTime<Utc>) -> Result<Vec<String>> {
            self.record("list_expired");
            Ok(self.expired.lock().unwrap().clone())
        }
    }

    #[derive(Default)]
    struct TestLocker {
        locked: Mutex<HashSet<String>>,
        fail_lock: Mutex<HashSet<String>>,
        operations: Option<OperationLog>,
    }

    impl TestLocker {
        fn with_operations(operations: OperationLog) -> Self {
            Self {
                operations: Some(operations),
                ..Self::default()
            }
        }

        fn mark_locked(&self, upload_id: &str) {
            self.locked.lock().unwrap().insert(upload_id.to_string());
        }

        fn fail_lock(&self, upload_id: &str) {
            self.fail_lock.lock().unwrap().insert(upload_id.to_string());
        }

        fn record(&self, operation: impl Into<String>) {
            if let Some(operations) = &self.operations {
                operations.lock().unwrap().push(operation.into());
            }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl Locker for TestLocker {
        fn name(&self) -> &'static str {
            "test"
        }

        async fn lock(&self, upload_id: &str, _timeout: StdDuration) -> Result<LockGuard> {
            Ok(LockGuard::new(upload_id))
        }

        async fn try_lock(&self, upload_id: &str) -> Result<Option<LockGuard>> {
            self.record(format!("try_lock {upload_id}"));
            if self.fail_lock.lock().unwrap().contains(upload_id) {
                return Err(Error::Internal(format!("try_lock failed for {upload_id}")));
            }
            if self.locked.lock().unwrap().contains(upload_id) {
                return Ok(None);
            }

            Ok(Some(LockGuard::new(upload_id)))
        }
    }
}
