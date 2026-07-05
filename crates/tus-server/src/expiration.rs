use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use tus_protocol::{
    ExpiredUploadReclamationOutcome, ExpiredUploadReclamationReport, Locker, StateStore, Storage,
    reclaim_expired_uploads,
};

use crate::lifecycle::ShutdownSignal;

#[derive(Clone)]
pub(crate) struct ExpirationTarget<S, I, L> {
    scope: String,
    storage: Arc<S>,
    state_store: Arc<I>,
    locker: Arc<L>,
}

impl<S, I, L> ExpirationTarget<S, I, L> {
    pub(crate) fn new(
        scope: impl Into<String>,
        storage: Arc<S>,
        state_store: Arc<I>,
        locker: Arc<L>,
    ) -> Self {
        Self {
            scope: scope.into(),
            storage,
            state_store,
            locker,
        }
    }

    pub(crate) fn scope(&self) -> &str {
        &self.scope
    }
}

pub(crate) async fn sweep_expired_uploads<S, I, L>(
    target: &ExpirationTarget<S, I, L>,
) -> anyhow::Result<ExpiredUploadReclamationReport>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
{
    reclaim_expired_uploads(
        target.storage.as_ref(),
        target.state_store.as_ref(),
        target.locker.as_ref(),
        Utc::now(),
    )
    .await
    .with_context(|| format!("failed to sweep expired uploads for scope {}", target.scope))
}

pub(crate) async fn run_cleanup_once<S, I, L>(
    target: &ExpirationTarget<S, I, L>,
) -> anyhow::Result<ExpiredUploadReclamationReport>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
{
    tracing::warn!(
        "cleanup is not online-safe with a live serve process when using the process-local memory locker"
    );

    let report = sweep_expired_uploads(target).await?;
    report_reclamation_outcomes(target.scope(), &report);
    ensure_cleanup_succeeded(&report)?;

    Ok(report)
}

fn ensure_cleanup_succeeded(report: &ExpiredUploadReclamationReport) -> anyhow::Result<()> {
    if report.has_failures() {
        anyhow::bail!("failed to clean up one or more expired uploads");
    }

    Ok(())
}

pub(crate) fn report_reclamation_outcomes(scope: &str, report: &ExpiredUploadReclamationReport) {
    log_reclamation_outcomes(scope, report);
    let removed = report.removed();
    if removed > 0 {
        tracing::info!(scope = %scope, removed, "cleaned up expired uploads");
    }
}

fn log_reclamation_outcomes(scope: &str, report: &ExpiredUploadReclamationReport) {
    for outcome in report.outcomes() {
        match outcome {
            ExpiredUploadReclamationOutcome::Removed { .. } => {}
            ExpiredUploadReclamationOutcome::Locked { upload_id, .. } => {
                tracing::debug!(scope = %scope, upload_id = %upload_id, "skipping cleanup for locked expired upload");
            }
            ExpiredUploadReclamationOutcome::MissingState { upload_id, .. } => {
                tracing::debug!(scope = %scope, upload_id = %upload_id, "skipping cleanup for missing expired upload state");
            }
            ExpiredUploadReclamationOutcome::NoLongerExpired { upload_id, .. } => {
                tracing::debug!(scope = %scope, upload_id = %upload_id, "skipping cleanup for upload that is no longer expired");
            }
            ExpiredUploadReclamationOutcome::StorageDeleteFailed {
                upload_id, error, ..
            } => {
                tracing::warn!(scope = %scope, upload_id = %upload_id, error = %error, "failed to delete expired upload data");
            }
            ExpiredUploadReclamationOutcome::StateDeleteFailed {
                upload_id, error, ..
            } => {
                tracing::warn!(scope = %scope, upload_id = %upload_id, error = %error, "failed to delete expired upload state");
            }
            ExpiredUploadReclamationOutcome::Failed {
                upload_id, error, ..
            } => {
                tracing::warn!(scope = %scope, upload_id = %upload_id, error = %error, "failed to reclaim expired upload");
            }
            // ExpiredUploadReclamationOutcome is #[non_exhaustive]:
            // future outcome kinds are surfaced generically until a
            // dedicated log line exists for them.
            outcome => {
                tracing::debug!(scope = %scope, ?outcome, "unrecognized expired upload reclamation outcome");
            }
        }
    }
}

pub(crate) fn spawn_expiration_sweeper<S, I, L>(
    shutdown_signal: ShutdownSignal,
    scan_interval: Duration,
    targets: Vec<ExpirationTarget<S, I, L>>,
) where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(scan_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut shutdown = shutdown_signal.clone();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    for target in &targets {
                        match sweep_expired_uploads(target).await {
                            Ok(report) => {
                                report_reclamation_outcomes(target.scope(), &report);
                            }
                            Err(error) => {
                                tracing::warn!(scope = %target.scope(), error = %error, "failed to sweep expired uploads");
                            }
                        }
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{Duration as ChronoDuration, Utc};
    use tus_protocol::locking::memory::MemoryLocker;
    use tus_protocol::state::file::FileStateStore;
    use tus_protocol::{AppendRequest, ChunkStream, StateStore, Storage, UploadState, WriteMode};
    use tus_storage_opendal::OpendalStorage;

    use super::*;
    use crate::config::{StorageConfig, build_storage_operator};

    #[tokio::test]
    async fn sweep_expired_uploads_removes_storage_and_state() {
        let root = tempfile::tempdir().unwrap();
        let state_dir = root.path().join("state");
        let mut config = StorageConfig::default();
        config.settings.insert(
            "root".to_string(),
            root.path().join("uploads").display().to_string(),
        );
        let (operator, _) = build_storage_operator(&config).unwrap();
        let storage = Arc::new(OpendalStorage::new(operator));
        let state_store = Arc::new(FileStateStore::new(&state_dir).await.unwrap());

        let mut state = UploadState::new("expired-test")
            .with_length(10)
            .with_expiration(Utc::now() - ChronoDuration::seconds(1));
        let handle = storage.create(state.id()).await.unwrap();
        state.set_storage_handle(handle);
        let handle = storage
            .append(AppendRequest::new(
                state.storage_handle().unwrap(),
                state.offset(),
                ChunkStream::from_bytes(b"hello".to_vec().into()),
                false,
            ))
            .await
            .unwrap();
        state.set_storage_handle(handle);
        state_store.set(&state, WriteMode::CreateNew).await.unwrap();

        assert_eq!(
            storage
                .size(&state.storage_handle().unwrap())
                .await
                .unwrap(),
            Some(5)
        );

        let target = ExpirationTarget::new(
            "test",
            storage.clone(),
            state_store.clone(),
            Arc::new(MemoryLocker::new()),
        );

        let report = sweep_expired_uploads(&target).await.unwrap();
        assert_eq!(report.removed(), 1);
        assert_eq!(
            storage
                .size(&state.storage_handle().unwrap())
                .await
                .unwrap(),
            None
        );
        assert!(state_store.get("expired-test").await.unwrap().is_none());
    }
}
