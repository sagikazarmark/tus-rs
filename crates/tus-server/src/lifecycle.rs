use std::future::IntoFuture;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as HyperBuilder,
    service::TowerToHyperService,
};
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::config::BindTarget;

pub(crate) struct ServeOptions {
    pub(crate) addr: BindTarget,
    pub(crate) shutdown_grace: Duration,
    pub(crate) drain_delay: Duration,
}

#[derive(Clone)]
pub(crate) struct ShutdownSignal {
    receiver: tokio::sync::watch::Receiver<bool>,
}

impl ShutdownSignal {
    pub(crate) async fn cancelled(&mut self) {
        if *self.receiver.borrow() {
            return;
        }

        while self.receiver.changed().await.is_ok() {
            if *self.receiver.borrow() {
                return;
            }
        }
    }
}

#[cfg(unix)]
struct UnixSocketGuard {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

#[cfg(unix)]
impl UnixSocketGuard {
    fn new(path: PathBuf, metadata: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;

        Self {
            path,
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    }
}

#[cfg(unix)]
impl Drop for UnixSocketGuard {
    fn drop(&mut self) {
        use std::os::unix::fs::{FileTypeExt, MetadataExt};

        let Ok(metadata) = std::fs::metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.dev
            && metadata.ino() == self.ino
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

// Installs signal handlers on a spawned task so the tokio signal driver
// is active from the first scheduling tick, avoiding the race where a
// signal arrives before the `with_graceful_shutdown` future has been
// polled for the first time.
pub(crate) fn spawn_signal_listener() -> anyhow::Result<ShutdownSignal> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    #[cfg(unix)]
    let (mut sigint, mut sigterm, mut sighup) = {
        use tokio::signal::unix::{SignalKind, signal};

        (
            signal(SignalKind::interrupt()).context("failed to install SIGINT handler")?,
            signal(SignalKind::terminate()).context("failed to install SIGTERM handler")?,
            signal(SignalKind::hangup()).context("failed to install SIGHUP handler")?,
        )
    };

    tokio::spawn(async move {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = sigint.recv() => {
                    tracing::info!("received SIGINT");
                }
                _ = sigterm.recv() => {
                    tracing::info!("received SIGTERM");
                }
                _ = sighup.recv() => {
                    tracing::info!("received SIGHUP");
                }
            }
        }

        #[cfg(not(unix))]
        {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::warn!(error = %e, "failed to install Ctrl-C handler");
                return;
            }
            tracing::info!("received Ctrl-C");
        }

        let _ = shutdown_tx.send(true);
    });

    Ok(ShutdownSignal {
        receiver: shutdown_rx,
    })
}

pub(crate) async fn serve_app(
    app: Router,
    options: ServeOptions,
    shutdown_signal: ShutdownSignal,
    draining: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let grace = options.shutdown_grace;
    let drain_delay = options.drain_delay;
    let drain_started = Arc::new(Notify::new());
    let shutdown = wait_for_shutdown_signal(
        shutdown_signal.clone(),
        drain_started.clone(),
        draining.clone(),
        drain_delay,
        grace,
    );

    match &options.addr {
        BindTarget::Tcp(bind) => {
            let listener = TcpListener::bind(bind)
                .await
                .with_context(|| format!("failed to bind TCP listener at {bind}"))?;
            tracing::info!("Listening on http://{}", bind);

            let serve = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown)
                .into_future();

            if grace.is_zero() {
                tokio::pin!(serve);
                tokio::select! {
                    result = &mut serve => result?,
                    _ = drain_started.notified() => {}
                }
            } else {
                // The grace timer must only bound the drain phase,
                // after any lame-duck delay has elapsed.
                tokio::pin!(serve);
                tokio::select! {
                    result = &mut serve => result?,
                    _ = drain_started.notified() => {
                        match tokio::time::timeout(grace, &mut serve).await {
                            Ok(result) => result?,
                            Err(_) => {
                                tracing::warn!(
                                    grace_secs = grace.as_secs(),
                                    "shutdown grace period elapsed, forcing exit"
                                );
                            }
                        }
                    }
                }
            }
        }
        #[cfg(unix)]
        BindTarget::Unix(path) => {
            let (listener, _guard) = bind_unix_listener(path).await?;
            tracing::info!("Listening on unix:{}", path.display());

            serve_unix(
                listener,
                app,
                shutdown_signal.clone(),
                draining.clone(),
                drain_delay,
                grace,
            )
            .await?;
        }
    }

    Ok(())
}

#[cfg(unix)]
async fn bind_unix_listener(
    path: &Path,
) -> anyhow::Result<(tokio::net::UnixListener, UnixSocketGuard)> {
    use std::os::unix::fs::FileTypeExt;

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed to create Unix socket directory {}",
                parent.display()
            )
        })?;
    }

    match tokio::fs::metadata(path).await {
        Ok(metadata) if metadata.file_type().is_socket() => {
            tokio::fs::remove_file(path).await.with_context(|| {
                format!("failed to remove stale Unix socket {}", path.display())
            })?;
        }
        Ok(_) => anyhow::bail!(
            "refusing to overwrite non-socket path at {}",
            path.display()
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }

    let listener = tokio::net::UnixListener::bind(path)
        .with_context(|| format!("failed to bind Unix listener at {}", path.display()))?;
    let metadata = std::fs::metadata(path).with_context(|| {
        format!(
            "failed to read metadata for bound Unix socket {}",
            path.display()
        )
    })?;
    Ok((
        listener,
        UnixSocketGuard::new(path.to_path_buf(), &metadata),
    ))
}

#[cfg(unix)]
async fn serve_unix(
    listener: tokio::net::UnixListener,
    app: Router,
    shutdown_signal: ShutdownSignal,
    draining: Arc<AtomicBool>,
    drain_delay: Duration,
    grace: Duration,
) -> anyhow::Result<()> {
    let mut tasks = tokio::task::JoinSet::new();
    let mut shutdown = shutdown_signal.clone();
    let mut drain_sleep = None::<Pin<Box<tokio::time::Sleep>>>;

    loop {
        tokio::select! {
            _ = shutdown.cancelled(), if drain_sleep.is_none() => {
                draining.store(true, Ordering::Relaxed);
                tracing::info!(
                    delay_secs = drain_delay.as_secs(),
                    "shutdown signal received, entering lame-duck state"
                );
                if drain_delay.is_zero() {
                    tracing::info!(grace_secs = grace.as_secs(), "draining in-flight requests");
                    break;
                }
                drain_sleep = Some(Box::pin(tokio::time::sleep(drain_delay)));
            }
            _ = async {
                drain_sleep
                    .as_mut()
                    .expect("drain sleep exists when select branch is enabled")
                    .as_mut()
                    .await;
            }, if drain_sleep.is_some() => {
                tracing::info!(grace_secs = grace.as_secs(), "draining in-flight requests");
                break;
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("failed to accept unix socket connection")?;
                let service = TowerToHyperService::new(app.clone());
                tasks.spawn(async move {
                    let io = TokioIo::new(stream);
                    if let Err(error) = HyperBuilder::new(TokioExecutor::new())
                        .serve_connection_with_upgrades(io, service)
                        .await
                    {
                        tracing::warn!(error = %error, "unix socket connection error");
                    }
                });
            }
        }
    }

    if grace.is_zero() {
        tasks.abort_all();
        return Ok(());
    }

    match tokio::time::timeout(grace, async { while tasks.join_next().await.is_some() {} }).await {
        Ok(_) => Ok(()),
        Err(_) => {
            tracing::warn!(
                grace_secs = grace.as_secs(),
                "shutdown grace period elapsed, forcing exit"
            );
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            Ok(())
        }
    }
}

async fn wait_for_shutdown_signal(
    mut shutdown_signal: ShutdownSignal,
    drain_started: Arc<Notify>,
    draining: Arc<AtomicBool>,
    drain_delay: Duration,
    grace: Duration,
) {
    shutdown_signal.cancelled().await;
    draining.store(true, Ordering::Relaxed);
    if !drain_delay.is_zero() {
        tracing::info!(
            delay_secs = drain_delay.as_secs(),
            "shutdown signal received, entering lame-duck state"
        );
        tokio::time::sleep(drain_delay).await;
    }
    drain_started.notify_one();
    tracing::info!(grace_secs = grace.as_secs(), "draining in-flight requests");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_signal_is_observed_by_late_subscribers() {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        shutdown_tx.send(true).unwrap();

        let mut shutdown = ShutdownSignal {
            receiver: shutdown_rx,
        };

        tokio::time::timeout(Duration::from_millis(100), shutdown.cancelled())
            .await
            .expect("late subscriber should observe sticky shutdown");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_socket_guard_preserves_replaced_path() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        let guard = UnixSocketGuard::new(path.clone(), &metadata);

        drop(listener);
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, "replacement").unwrap();

        drop(guard);

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "replacement");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_serving_accepts_connections_during_drain_delay() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let shutdown_signal = ShutdownSignal {
            receiver: shutdown_rx,
        };
        let draining = Arc::new(AtomicBool::new(false));
        let app = Router::new().route("/", axum::routing::get(|| async { "ok" }));
        let server = tokio::spawn(serve_unix(
            listener,
            app,
            shutdown_signal,
            draining.clone(),
            Duration::from_millis(500),
            Duration::from_secs(1),
        ));

        shutdown_tx.send(true).unwrap();
        while !draining.load(Ordering::Relaxed) {
            tokio::task::yield_now().await;
        }

        let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();

        let mut response = Vec::new();
        tokio::time::timeout(
            Duration::from_millis(150),
            stream.read_to_end(&mut response),
        )
        .await
        .expect("connection accepted during drain delay")
        .unwrap();

        assert!(
            String::from_utf8_lossy(&response).contains("ok"),
            "unexpected response: {}",
            String::from_utf8_lossy(&response)
        );

        server.await.unwrap().unwrap();
    }
}
