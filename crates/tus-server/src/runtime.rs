use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use tus_hook_http::{HttpHookConfig, HttpHookExecutor};
use tus_protocol::{
    HookContext, HookExecutor, Locker, NoopHookExecutor, PreHookResult, StateStore, Storage,
    locking::memory::MemoryLocker, state::file::FileStateStore,
};
use tus_storage_opendal::Storage as ServerStorage;

use crate::config::{HookConfig, StorageConfig, build_storage_operator};

// Wraps the two hook executors so TusState's concrete type stays
// monomorphic regardless of whether the operator configured webhooks.
pub(crate) enum ServerHooks {
    Noop(NoopHookExecutor),
    Http(HttpHookExecutor),
}

#[async_trait]
impl HookExecutor for ServerHooks {
    async fn execute_pre(&self, ctx: &HookContext) -> tus_protocol::Result<PreHookResult> {
        match self {
            ServerHooks::Noop(h) => h.execute_pre(ctx).await,
            ServerHooks::Http(h) => h.execute_pre(ctx).await,
        }
    }

    async fn execute_post(&self, ctx: &HookContext) -> tus_protocol::Result<()> {
        match self {
            ServerHooks::Noop(h) => h.execute_post(ctx).await,
            ServerHooks::Http(h) => h.execute_post(ctx).await,
        }
    }
}

pub(crate) struct RuntimeBackends {
    pub(crate) storage: Arc<ServerStorage>,
    pub(crate) state_store: Arc<FileStateStore>,
    pub(crate) locker: Arc<MemoryLocker>,
}

pub(crate) fn build_hooks(config: &HookConfig) -> anyhow::Result<ServerHooks> {
    let Some(url) = config.url.as_deref() else {
        tracing::info!("Hooks: disabled");
        return Ok(ServerHooks::Noop(NoopHookExecutor::new()));
    };

    let mut cfg = HttpHookConfig::new(url)
        .with_timeout(Duration::from_secs(config.timeout))
        .with_retry(config.retry)
        .with_max_retries(config.max_retries);

    if let Some(secret) = config.signing_secret.as_deref() {
        cfg = cfg.with_signing_secret(secret);
    }

    for header in &config.header {
        let (name, value) = header.split_once(':').ok_or_else(|| {
            anyhow::anyhow!("--hook-header must be 'Name: Value', got `{}`", header)
        })?;
        cfg = cfg.with_header(name.trim(), value.trim());
    }

    tracing::info!(
        url = %url,
        timeout_secs = config.timeout,
        retry = config.retry,
        signed = config.signing_secret.is_some(),
        "Hooks: http webhook"
    );

    Ok(ServerHooks::Http(HttpHookExecutor::new(cfg)))
}

pub(crate) async fn build_backends(
    storage_config: &StorageConfig,
    state_dir: &std::path::Path,
) -> anyhow::Result<RuntimeBackends> {
    tokio::fs::create_dir_all(state_dir)
        .await
        .with_context(|| format!("failed to create state directory {}", state_dir.display()))?;

    let (storage_operator, storage_scheme) = build_storage_operator(storage_config)?;
    let storage = Arc::new(ServerStorage::new(storage_operator, ""));
    let state_store = Arc::new(FileStateStore::new(state_dir).await.with_context(|| {
        format!(
            "failed to initialize file state store at {}",
            state_dir.display()
        )
    })?);
    let locker = Arc::new(MemoryLocker::new());

    tracing::info!(scheme = %storage_scheme, "Storage scheme configured");
    tracing::info!("Storage backend: {}", storage.name());
    tracing::info!("State store: {}", state_store.name());
    tracing::info!("Locker: {}", locker.name());

    Ok(RuntimeBackends {
        storage,
        state_store,
        locker,
    })
}
