use std::path::Path;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod cleanup;
mod runtime;
mod serve;

pub(crate) async fn run_serve(command: crate::config::ServeCli) -> anyhow::Result<()> {
    serve::run(command).await
}

pub(crate) async fn run_cleanup(command: crate::config::CleanupCli) -> anyhow::Result<()> {
    cleanup::run(command).await
}

fn init_tracing(log_format: crate::config::LogFormat) -> anyhow::Result<()> {
    match log_format {
        crate::config::LogFormat::Text => tracing_subscriber::registry()
            .with(default_env_filter())
            .with(tracing_subscriber::fmt::layer())
            .init(),
        crate::config::LogFormat::Json => tracing_subscriber::registry()
            .with(default_env_filter())
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
    }

    Ok(())
}

fn log_config_file(path: Option<&Path>) {
    if let Some(path) = path {
        tracing::info!(path = %path.display(), "loaded config file");
    }
}

fn default_env_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "tus_server=info,tus=debug,tower_http=debug".into())
}
