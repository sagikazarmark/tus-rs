use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use axum::Router;
use tus_protocol::{ProtocolHandle, locking::memory::MemoryLocker, state::file::FileStateStore};
use tus_storage_opendal::OpendalStorage;

use crate::{
    app,
    config::{self, ServeCli, Settings},
    expiration::ExpirationTarget,
    lifecycle,
};

struct ServeCommandParts {
    app: Router,
    cleanup_targets: Vec<ExpirationTarget<OpendalStorage, FileStateStore, MemoryLocker>>,
    draining: Arc<AtomicBool>,
}

pub(super) async fn run(command: ServeCli) -> anyhow::Result<()> {
    let (settings, config_path) = config::load_serve_settings(&command)?;

    super::init_tracing(settings.log_format)?;

    // Install the signal listener before backend construction and serving.
    let shutdown_notify = lifecycle::spawn_signal_listener()?;

    super::log_config_file(config_path.as_deref());
    config::warn_unknown_tus_env_keys();

    tracing::info!("Starting TUS server");
    tracing::info!("State directory: {:?}", settings.state_dir);

    let config = config::build_tus_config(&settings);
    log_tus_config(&config);

    let parts = build_serve_command_parts(&settings, config).await?;
    let ServeCommandParts {
        app,
        cleanup_targets,
        draining,
    } = parts;

    if !cleanup_targets.is_empty() {
        let scan_interval = settings
            .expiration_scan_interval
            .as_duration()
            .max(Duration::from_secs(1));
        tracing::info!(
            interval_secs = scan_interval.as_secs(),
            "reclaiming expired upload data and state in-process; disable with --disable-expiration-reclamation"
        );
        crate::expiration::spawn_expiration_sweeper(
            shutdown_notify.clone(),
            scan_interval,
            cleanup_targets,
        );
    }

    lifecycle::serve_app(
        app,
        lifecycle::ServeOptions {
            addr: settings.addr.clone(),
            shutdown_grace: Duration::from_secs(settings.shutdown_grace),
            drain_delay: Duration::from_secs(settings.drain_delay),
            header_read_timeout: Duration::from_secs(settings.request_header_read_timeout),
        },
        shutdown_notify,
        draining,
    )
    .await?;

    tracing::info!("server stopped");
    Ok(())
}

async fn build_serve_command_parts(
    settings: &Settings,
    config: tus_protocol::Config,
) -> anyhow::Result<ServeCommandParts> {
    let runtime =
        super::runtime::build_command_runtime(&settings.storage, &settings.state_dir).await?;
    let hooks = Arc::new(super::runtime::build_hooks(&settings.hook)?);

    let protocol = ProtocolHandle::from_arcs(
        Arc::new(config),
        runtime.backends.storage.clone(),
        runtime.backends.state_store.clone(),
        runtime.backends.locker.clone(),
        hooks,
    );

    let draining = Arc::new(AtomicBool::new(false));
    let cors_origins = if !settings.cors_origins.is_empty() {
        settings.cors_origins.clone()
    } else if settings.cors {
        vec!["*".to_string()]
    } else {
        Vec::new()
    };
    if !cors_origins.is_empty() {
        tracing::info!("  CORS origins: {}", cors_origins.join(", "));
    }
    let app = app::build_app(
        protocol,
        &app::AppSettings {
            auth_token: settings.auth_token.clone(),
            max_request_body_bytes: settings.max_request_body_bytes,
            request_body_read_timeout: settings.request_body_read_timeout,
            cors_origins,
        },
        draining.clone(),
    )?;
    // The in-process sweeper reclaims expired upload data and state.
    // It shares the live protocol's process-local locker, so it is
    // online-safe and follows --expiration by default; only skip it
    // when there is nothing to expire or the operator opted out.
    let expiration_configured = !settings.expiration.as_duration().is_zero();
    let cleanup_targets = if expiration_configured && !settings.disable_expiration_reclamation {
        vec![runtime.cleanup_target]
    } else {
        if expiration_configured && settings.disable_expiration_reclamation {
            tracing::warn!(
                "expiration is configured but in-process reclamation is disabled; expired upload data and state will accumulate on disk until reclaimed out-of-band (for example with `tus-server cleanup`)"
            );
        }
        Vec::new()
    };

    Ok(ServeCommandParts {
        app,
        cleanup_targets,
        draining,
    })
}

fn log_tus_config(config: &tus_protocol::Config) {
    tracing::info!("TUS configuration:");
    tracing::info!("  Base path: {}", config.base_path());
    if let Some(url) = config.base_url() {
        tracing::info!("  Base URL: {}", url);
    }
    tracing::info!("  Extensions: {}", config.extensions_string());
    if let Some(max) = config.max_size() {
        tracing::info!("  Max size: {} bytes", max);
    }
    match config.max_chunk_size() {
        Some(max) => tracing::info!("  Max chunk size: {} bytes", max),
        None => tracing::info!("  Max chunk size: unlimited"),
    }
    if config.respects_forwarded_headers() {
        tracing::info!("  Respecting Forwarded/X-Forwarded-* headers");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::config::{Settings, StorageConfig};

    fn settings_with_storage(root: &std::path::Path) -> Settings {
        Settings {
            storage: StorageConfig {
                uri: "fs://".to_string(),
                settings: BTreeMap::from([(
                    "root".to_string(),
                    root.join("uploads").display().to_string(),
                )]),
            },
            state_dir: root.join("state"),
            ..Settings::default()
        }
    }

    #[tokio::test]
    async fn build_serve_command_parts_builds_app_and_cleanup_targets_without_listening() {
        let root = tempfile::tempdir().unwrap();
        // Expiration configured and reclamation left at its default (on)
        // must wire up the in-process sweeper without any opt-in flag.
        let settings = Settings {
            expiration: "1h".parse().unwrap(),
            ..settings_with_storage(root.path())
        };

        let config = crate::config::build_tus_config(&settings);
        let parts = super::build_serve_command_parts(&settings, config)
            .await
            .unwrap();
        let response = parts
            .app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(parts.cleanup_targets.len(), 1);
    }

    #[tokio::test]
    async fn reclamation_can_be_disabled_and_is_absent_without_expiration() {
        let root = tempfile::tempdir().unwrap();

        // No expiration configured: nothing to reclaim, no sweeper.
        let settings = settings_with_storage(root.path());
        let config = crate::config::build_tus_config(&settings);
        let parts = super::build_serve_command_parts(&settings, config)
            .await
            .unwrap();
        assert!(parts.cleanup_targets.is_empty());

        // Expiration configured but reclamation explicitly disabled.
        let settings = Settings {
            expiration: "1h".parse().unwrap(),
            disable_expiration_reclamation: true,
            ..settings_with_storage(root.path())
        };
        let config = crate::config::build_tus_config(&settings);
        let parts = super::build_serve_command_parts(&settings, config)
            .await
            .unwrap();
        assert!(parts.cleanup_targets.is_empty());
    }
}
