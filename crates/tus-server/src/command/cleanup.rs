use anyhow::Context;
use tus_protocol::{ExpiredUploadReclamationReport, Locker, StateStore, Storage};

use crate::{
    config::{self, CleanupCli, CleanupSettings},
    expiration::{self, ExpirationTarget},
};

pub(super) async fn run(command: CleanupCli) -> anyhow::Result<()> {
    let (settings, config_path) = config::load_cleanup_settings(&command)?;

    super::init_tracing(settings.log_format)?;
    super::log_config_file(config_path.as_deref());

    // Cleanup builds its own process-local memory locker, so its
    // try_lock always succeeds: it cannot see locks held by a running
    // serve process and could delete data mid-upload. Require an
    // explicit acknowledgement before touching anything.
    if !settings.force {
        anyhow::bail!(
            "refusing to run cleanup without --force: cleanup uses a process-local memory \
             locker and cannot see locks held by a running serve process, so running it \
             against a live server can delete upload data mid-transfer. Stop the server \
             first, then re-run with --force (or TUS_CLEANUP_FORCE=true)."
        );
    }

    run_with_settings(&settings).await?;
    Ok(())
}

async fn run_with_settings(
    settings: &CleanupSettings,
) -> anyhow::Result<ExpiredUploadReclamationReport> {
    let metadata = tokio::fs::metadata(&settings.state_dir)
        .await
        .with_context(|| {
            format!(
                "cleanup state directory not found: {}",
                settings.state_dir.display()
            )
        })?;
    if !metadata.is_dir() {
        anyhow::bail!(
            "cleanup state path is not a directory: {}",
            settings.state_dir.display()
        );
    }

    let runtime =
        super::runtime::build_command_runtime(&settings.storage, &settings.state_dir).await?;
    run_once(&runtime.cleanup_target).await
}

async fn run_once<S, I, L>(
    target: &ExpirationTarget<S, I, L>,
) -> anyhow::Result<ExpiredUploadReclamationReport>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
{
    expiration::run_cleanup_once(target).await
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use chrono::{Duration as ChronoDuration, Utc};
    use tus_protocol::{
        AppendRequest, ConcatRequest, LockGuard, Locker, StateStore, Storage, StorageHandle,
        UploadState,
    };

    use crate::expiration::ExpirationTarget;

    #[tokio::test]
    async fn run_once_fails_when_reclamation_reports_deletion_failure() {
        let storage = Arc::new(TestStorage::default());
        let state_store = Arc::new(TestStateStore::default());
        let locker = Arc::new(TestLocker);
        state_store.insert_expired(expired_state("upload-1", "data-1"));
        storage.fail_delete("data-1");
        let target = ExpirationTarget::new("test", storage, state_store, locker);

        let err = super::run_once(&target).await.unwrap_err();

        assert!(
            err.to_string()
                .contains("failed to clean up one or more expired uploads"),
            "unexpected error: {err}"
        );
    }

    fn expired_state(id: &str, storage_key: &str) -> UploadState {
        let mut state = UploadState::new(id).with_expiration(Utc::now() - ChronoDuration::hours(1));
        state.set_storage_handle(StorageHandle::new(storage_key));
        state
    }

    #[derive(Default)]
    struct TestStorage {
        fail_delete: Mutex<HashSet<String>>,
    }

    impl TestStorage {
        fn fail_delete(&self, key: &str) {
            self.fail_delete.lock().unwrap().insert(key.to_string());
        }
    }

    #[async_trait]
    impl Storage for TestStorage {
        fn name(&self) -> &'static str {
            "test"
        }

        async fn create(&self, upload_id: &str) -> tus_protocol::Result<StorageHandle> {
            Ok(StorageHandle::new(upload_id))
        }

        async fn append(&self, request: AppendRequest) -> tus_protocol::Result<StorageHandle> {
            Ok(request.handle)
        }

        async fn concat(&self, request: ConcatRequest) -> tus_protocol::Result<StorageHandle> {
            Ok(request.target)
        }

        async fn delete(&self, handle: &StorageHandle) -> tus_protocol::Result<()> {
            if self.fail_delete.lock().unwrap().contains(handle.key()) {
                return Err(tus_protocol::Error::Internal(format!(
                    "storage delete failed for {}",
                    handle.key()
                )));
            }

            Ok(())
        }

        async fn size(&self, _handle: &StorageHandle) -> tus_protocol::Result<Option<u64>> {
            Ok(None)
        }
    }

    #[derive(Default)]
    struct TestStateStore {
        states: Mutex<HashMap<String, UploadState>>,
        expired: Mutex<Vec<String>>,
    }

    impl TestStateStore {
        fn insert_expired(&self, state: UploadState) {
            self.expired.lock().unwrap().push(state.id().to_string());
            self.states
                .lock()
                .unwrap()
                .insert(state.id().to_string(), state);
        }
    }

    #[async_trait]
    impl StateStore for TestStateStore {
        fn name(&self) -> &'static str {
            "test"
        }

        async fn set(&self, state: &UploadState, _create: bool) -> tus_protocol::Result<()> {
            self.states
                .lock()
                .unwrap()
                .insert(state.id().to_string(), state.clone());
            Ok(())
        }

        async fn get(&self, id: &str) -> tus_protocol::Result<Option<UploadState>> {
            Ok(self.states.lock().unwrap().get(id).cloned())
        }

        async fn delete(&self, id: &str) -> tus_protocol::Result<()> {
            self.states.lock().unwrap().remove(id);
            Ok(())
        }

        async fn list_expired(
            &self,
            _before: chrono::DateTime<Utc>,
        ) -> tus_protocol::Result<Vec<String>> {
            Ok(self.expired.lock().unwrap().clone())
        }
    }

    struct TestLocker;

    #[async_trait]
    impl Locker for TestLocker {
        fn name(&self) -> &'static str {
            "test"
        }

        async fn lock(
            &self,
            upload_id: &str,
            _timeout: Duration,
        ) -> tus_protocol::Result<LockGuard> {
            Ok(LockGuard::new(upload_id))
        }

        async fn try_lock(&self, upload_id: &str) -> tus_protocol::Result<Option<LockGuard>> {
            Ok(Some(LockGuard::new(upload_id)))
        }
    }
}
