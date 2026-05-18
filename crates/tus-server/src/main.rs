//! TUS Resumable Upload Server
//!
//! A standalone server implementing the TUS protocol for resumable file uploads.

mod app;
mod config;
mod expiration;
mod lifecycle;
mod runtime;

use clap::Parser;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::Context;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use tus_protocol::ProtocolHandle;

use crate::config::{Cli, Command};

fn default_env_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "tus_server=info,tus=debug,tower_http=debug".into())
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

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        match cli.command {
            Command::Serve(command) => run_serve(*command).await,
            Command::Cleanup(command) => run_cleanup(command).await,
        }
    })
}

async fn run_serve(command: config::ServeCli) -> anyhow::Result<()> {
    let (settings, config_path) = config::load_serve_settings(&command)?;

    init_tracing(settings.log_format)?;

    // Install the signal listener before backend construction and serving.
    let shutdown_notify = lifecycle::spawn_signal_listener()?;

    if let Some(path) = config_path.as_ref() {
        tracing::info!(path = %path.display(), "loaded config file");
    }

    tracing::info!("Starting TUS server");
    tracing::info!("State directory: {:?}", settings.state_dir);

    let config = config::build_tus_config(&settings);
    log_tus_config(&config);

    let backends = runtime::build_backends(&settings.storage, &settings.state_dir).await?;
    let hooks = Arc::new(runtime::build_hooks(&settings.hook)?);

    let protocol = ProtocolHandle::from_arcs(
        Arc::new(config),
        backends.storage.clone(),
        backends.state_store.clone(),
        backends.locker.clone(),
        hooks,
    );

    // Shared flag flipped when shutdown begins so /readyz reports
    // NOT READY and load balancers stop routing new traffic to this
    // instance while in-flight requests drain.
    let draining = Arc::new(AtomicBool::new(false));
    let app = app::build_app(
        protocol,
        &app::AppSettings {
            auth_token: settings.auth_token.clone(),
            max_request_body_bytes: settings.max_request_body_bytes,
            request_body_read_timeout: settings.request_body_read_timeout,
        },
        draining.clone(),
    );

    if settings.cleanup {
        let scan_interval = settings
            .expiration_scan_interval
            .as_duration()
            .max(Duration::from_secs(1));
        tracing::info!(
            interval_secs = scan_interval.as_secs(),
            "enabling in-process expired upload cleanup"
        );
        expiration::spawn_expiration_sweeper(
            shutdown_notify.clone(),
            scan_interval,
            vec![expiration::ExpirationTarget::new(
                "default",
                backends.storage.clone(),
                backends.state_store.clone(),
                backends.locker.clone(),
            )],
        );
    }

    lifecycle::serve_app(
        app,
        lifecycle::ServeOptions {
            addr: settings.addr.clone(),
            shutdown_grace: Duration::from_secs(settings.shutdown_grace),
            drain_delay: Duration::from_secs(settings.drain_delay),
        },
        shutdown_notify,
        draining,
    )
    .await?;

    tracing::info!("server stopped");
    Ok(())
}

async fn run_cleanup(command: config::CleanupCli) -> anyhow::Result<()> {
    let (settings, config_path) = config::load_cleanup_settings(&command)?;

    init_tracing(settings.log_format)?;

    if let Some(path) = config_path.as_ref() {
        tracing::info!(path = %path.display(), "loaded config file");
    }

    tracing::warn!(
        "cleanup is not online-safe with a live serve process when using the process-local memory locker"
    );

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

    let backends = runtime::build_backends(&settings.storage, &settings.state_dir).await?;
    let removed = expiration::sweep_expired_uploads(&expiration::ExpirationTarget::new(
        "default",
        backends.storage.clone(),
        backends.state_store.clone(),
        backends.locker.clone(),
    ))
    .await?;

    tracing::info!(removed, "cleaned up expired uploads");
    Ok(())
}

fn log_tus_config(config: &tus_protocol::Config) {
    tracing::info!("TUS configuration:");
    tracing::info!("  Base path: {}", config.base_path_str());
    if let Some(url) = config.base_url_str() {
        tracing::info!("  Base URL: {}", url);
    }
    tracing::info!("  Extensions: {}", config.extensions_string());
    if let Some(max) = config.max_size_limit() {
        tracing::info!("  Max size: {} bytes", max);
    }
    if !config.cors_allowed_origins().is_empty() {
        tracing::info!(
            "  CORS origins: {}",
            config.cors_allowed_origins().join(", ")
        );
    }
}
