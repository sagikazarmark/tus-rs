use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use tus_protocol::{Locker, StateStore, Storage};

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
) -> anyhow::Result<u64>
where
    S: Storage + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
{
    let mut removed = 0u64;
    let expired_ids = target
        .state_store
        .list_expired(Utc::now())
        .await
        .with_context(|| format!("failed to list expired uploads for scope {}", target.scope))?;

    for upload_id in expired_ids {
        let Some(_guard) = target.locker.try_lock(&upload_id).await.with_context(|| {
            format!(
                "failed to lock expired upload {} for scope {}",
                upload_id, target.scope
            )
        })?
        else {
            tracing::debug!(scope = %target.scope, upload_id = %upload_id, "skipping cleanup for locked expired upload");
            continue;
        };

        let Some(state) = target.state_store.get(&upload_id).await.with_context(|| {
            format!(
                "failed to load expired upload state {} for scope {}",
                upload_id, target.scope
            )
        })?
        else {
            continue;
        };

        if !state.is_expired() {
            continue;
        }

        target.storage.delete(&state).await.with_context(|| {
            format!(
                "failed to delete expired upload data {} for scope {}",
                upload_id, target.scope
            )
        })?;
        target
            .state_store
            .delete(state.id())
            .await
            .with_context(|| {
                format!(
                    "failed to delete expired upload state {} for scope {}",
                    upload_id, target.scope
                )
            })?;
        removed += 1;
    }

    Ok(removed)
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
                            Ok(removed) if removed > 0 => {
                                tracing::info!(scope = %target.scope(), removed, "cleaned up expired uploads");
                            }
                            Ok(_) => {}
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
    use tus_protocol::{ChunkStream, StateStore, Storage, UploadState};
    use tus_storage_opendal::Storage as ServerStorage;

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
        let storage = Arc::new(ServerStorage::new(operator, ""));
        let state_store = Arc::new(FileStateStore::new(&state_dir).await.unwrap());

        let mut state = UploadState::new("expired-test")
            .with_length(5)
            .with_expiration(Utc::now() - ChronoDuration::seconds(1));
        storage.create(&mut state).await.unwrap();
        storage
            .append(
                &mut state,
                ChunkStream::from_bytes(b"hello".to_vec().into()),
            )
            .await
            .unwrap();
        state_store.set(&state, true).await.unwrap();

        assert_eq!(storage.size(&state).await.unwrap(), Some(5));

        let target = ExpirationTarget::new(
            "test",
            storage.clone(),
            state_store.clone(),
            Arc::new(MemoryLocker::new()),
        );

        let removed = sweep_expired_uploads(&target).await.unwrap();
        assert_eq!(removed, 1);
        assert_eq!(storage.size(&state).await.unwrap(), None);
        assert!(state_store.get("expired-test").await.unwrap().is_none());
    }
}
