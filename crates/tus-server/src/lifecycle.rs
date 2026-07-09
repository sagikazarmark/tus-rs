use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::{conn::auto::Builder as HyperBuilder, graceful::GracefulShutdown},
    service::TowerToHyperService,
};
use tokio::net::TcpListener;

use crate::config::BindTarget;

pub(crate) struct ServeOptions {
    pub(crate) addr: BindTarget,
    pub(crate) shutdown_grace: Duration,
    pub(crate) drain_delay: Duration,
    /// Maximum time a connection may spend sending its request
    /// headers before it is torn down; zero disables the limit.
    pub(crate) header_read_timeout: Duration,
}

impl From<crate::config::RuntimeConfig> for ServeOptions {
    fn from(runtime: crate::config::RuntimeConfig) -> Self {
        Self {
            addr: runtime.addr,
            shutdown_grace: runtime.shutdown_grace,
            drain_delay: runtime.drain_delay,
            header_read_timeout: runtime.header_read_timeout,
        }
    }
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
// signal arrives before the serve loop's shutdown branch has been
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
    let loop_options = ServeLoopOptions {
        drain_delay: options.drain_delay,
        grace: options.shutdown_grace,
        header_read_timeout: options.header_read_timeout,
    };

    match &options.addr {
        BindTarget::Tcp(bind) => {
            let listener = TcpListener::bind(bind)
                .await
                .with_context(|| format!("failed to bind TCP listener at {bind}"))?;
            tracing::info!("Listening on http://{}", bind);

            serve_connections(listener, app, shutdown_signal, draining, loop_options).await
        }
        #[cfg(unix)]
        BindTarget::Unix(path) => {
            let (listener, _guard) = bind_unix_listener(path).await?;
            tracing::info!("Listening on unix:{}", path.display());

            serve_connections(listener, app, shutdown_signal, draining, loop_options).await
        }
    }
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

#[derive(Clone, Copy)]
struct ServeLoopOptions {
    drain_delay: Duration,
    grace: Duration,
    header_read_timeout: Duration,
}

/// Accept source abstraction so TCP and Unix sockets share one
/// connection-serving loop (and therefore the same header timeout,
/// accept-error, and graceful-shutdown behavior).
trait Listener {
    type Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static;

    async fn accept(&self) -> io::Result<Self::Io>;

    /// Transport label for log messages.
    fn transport(&self) -> &'static str;
}

impl Listener for TcpListener {
    type Io = tokio::net::TcpStream;

    async fn accept(&self) -> io::Result<Self::Io> {
        TcpListener::accept(self).await.map(|(stream, _)| stream)
    }

    fn transport(&self) -> &'static str {
        "tcp"
    }
}

#[cfg(unix)]
impl Listener for tokio::net::UnixListener {
    type Io = tokio::net::UnixStream;

    async fn accept(&self) -> io::Result<Self::Io> {
        tokio::net::UnixListener::accept(self)
            .await
            .map(|(stream, _)| stream)
    }

    fn transport(&self) -> &'static str {
        "unix"
    }
}

async fn serve_connections<L>(
    listener: L,
    app: Router,
    shutdown_signal: ShutdownSignal,
    draining: Arc<AtomicBool>,
    options: ServeLoopOptions,
) -> anyhow::Result<()>
where
    L: Listener,
{
    let ServeLoopOptions {
        drain_delay,
        grace,
        header_read_timeout,
    } = options;

    // hyper 1.x only enforces its header-read timeout when a timer is
    // installed; without one the advertised 30 s default is silently
    // disabled, leaving the header phase open to slowloris clients.
    // (`axum::serve` never installs a timer, which is why this loop
    // exists for TCP as well.)
    let mut builder = HyperBuilder::new(TokioExecutor::new());
    builder.http1().timer(TokioTimer::new());
    builder.http2().timer(TokioTimer::new());
    if header_read_timeout.is_zero() {
        // 0 is the documented opt-out; hyper would otherwise apply
        // its own 30 s default now that a timer is present.
        builder.http1().header_read_timeout(None);
    } else {
        builder.http1().header_read_timeout(header_read_timeout);
    }

    let graceful = GracefulShutdown::new();
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
                break;
            }
            // Reap finished connection tasks as they complete; JoinSet buffers
            // results until joined, so skipping this would grow memory with
            // every connection served.
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
            accepted = listener.accept() => {
                let stream = match accepted {
                    Ok(stream) => stream,
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            transport = listener.transport(),
                            "accept error, retrying"
                        );
                        let backoff = accept_retry_backoff(&error);
                        if !backoff.is_zero() {
                            tokio::time::sleep(backoff).await;
                        }
                        continue;
                    }
                };

                let service = TowerToHyperService::new(app.clone());
                let connection = graceful.watch(
                    builder
                        .serve_connection_with_upgrades(TokioIo::new(stream), service)
                        .into_owned(),
                );
                let transport = listener.transport();
                tasks.spawn(async move {
                    if let Err(error) = connection.await {
                        tracing::warn!(error = %error, transport = transport, "connection error");
                    }
                });
            }
        }
    }

    // Close the listener before draining so the kernel stops completing
    // handshakes into the backlog; mid-drain clients get an immediate
    // refusal instead of an established-but-never-served connection.
    drop(listener);

    tracing::info!(grace_secs = grace.as_secs(), "draining in-flight requests");

    if grace.is_zero() {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        return Ok(());
    }

    // Tell every watched connection to shut down gracefully (close
    // after the in-flight response on HTTP/1, GOAWAY on HTTP/2) so
    // idle keep-alive connections drain promptly instead of burning
    // the whole grace window.
    tokio::select! {
        _ = graceful.shutdown() => Ok(()),
        _ = tokio::time::sleep(grace) => {
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

// Backoff before retrying `accept()` after resource exhaustion, so an
// EMFILE/ENFILE storm cannot spin the accept loop hot.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(500);

/// Returns how long to wait before retrying a failed `accept()`.
///
/// Accept errors never stop the server: accept(2) documents that
/// already-pending network errors (ENETDOWN, ENOBUFS, EHOSTUNREACH, ...)
/// surface through `accept` and should be treated like EAGAIN, and axum's
/// own serve loop likewise retries everything. Connection-level errors
/// (the peer vanished between arrival and accept — attacker-inducible)
/// are retried immediately; anything else, including fd or memory
/// exhaustion, backs off briefly so an error storm cannot spin the loop
/// hot.
fn accept_retry_backoff(error: &io::Error) -> Duration {
    match error.kind() {
        io::ErrorKind::ConnectionRefused
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::Interrupted => Duration::ZERO,
        _ => ACCEPT_ERROR_BACKOFF,
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    fn test_loop_options(drain_delay: Duration, grace: Duration) -> ServeLoopOptions {
        ServeLoopOptions {
            drain_delay,
            grace,
            header_read_timeout: Duration::from_secs(30),
        }
    }

    fn test_shutdown_channel() -> (tokio::sync::watch::Sender<bool>, ShutdownSignal) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        (
            shutdown_tx,
            ShutdownSignal {
                receiver: shutdown_rx,
            },
        )
    }

    fn ok_router() -> Router {
        Router::new().route("/", axum::routing::get(|| async { "ok" }))
    }

    #[tokio::test]
    async fn shutdown_signal_is_observed_by_late_subscribers() {
        let (shutdown_tx, mut shutdown) = test_shutdown_channel();
        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_millis(100), shutdown.cancelled())
            .await
            .expect("late subscriber should observe sticky shutdown");
    }

    #[test]
    fn accept_errors_are_classified_for_retry() {
        // Connection-level errors: retry immediately.
        assert_eq!(
            accept_retry_backoff(&io::Error::from(io::ErrorKind::ConnectionAborted)),
            Duration::ZERO
        );
        assert_eq!(
            accept_retry_backoff(&io::Error::from(io::ErrorKind::Interrupted)),
            Duration::ZERO
        );

        // Everything else — resource exhaustion, pending network errors,
        // unknown kinds — retries after a backoff; accept errors never
        // stop the server.
        #[cfg(unix)]
        {
            let emfile = io::Error::from_raw_os_error(24);
            let enfile = io::Error::from_raw_os_error(23);
            assert_eq!(accept_retry_backoff(&emfile), ACCEPT_ERROR_BACKOFF);
            assert_eq!(accept_retry_backoff(&enfile), ACCEPT_ERROR_BACKOFF);
        }
        assert_eq!(
            accept_retry_backoff(&io::Error::from(io::ErrorKind::InvalidInput)),
            ACCEPT_ERROR_BACKOFF
        );
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
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let (shutdown_tx, shutdown_signal) = test_shutdown_channel();
        let draining = Arc::new(AtomicBool::new(false));
        let server = tokio::spawn(serve_connections(
            listener,
            ok_router(),
            shutdown_signal,
            draining.clone(),
            test_loop_options(Duration::from_millis(500), Duration::from_secs(1)),
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

    #[tokio::test]
    async fn graceful_shutdown_promptly_closes_idle_keepalive_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_signal) = test_shutdown_channel();
        let draining = Arc::new(AtomicBool::new(false));
        // A grace window far longer than the test budget: if drain
        // waited out idle keep-alive connections, the timeout below
        // would trip.
        let server = tokio::spawn(serve_connections(
            listener,
            ok_router(),
            shutdown_signal,
            draining,
            test_loop_options(Duration::ZERO, Duration::from_secs(600)),
        ));

        // Complete one request on a keep-alive connection, then leave
        // the connection idle.
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        let mut buf = [0u8; 256];
        loop {
            let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
                .await
                .expect("first response must arrive")
                .unwrap();
            assert_ne!(n, 0, "connection closed before first response completed");
            response.extend_from_slice(&buf[..n]);
            if String::from_utf8_lossy(&response).ends_with("ok") {
                break;
            }
        }

        shutdown_tx.send(true).unwrap();

        // Drain must complete promptly because the idle connection is
        // told to shut down instead of being waited out.
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("drain must not wait out idle keep-alive connections")
            .unwrap()
            .unwrap();

        // The idle connection observes the server-initiated close.
        let n = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf))
            .await
            .expect("idle connection must be closed by the server")
            .unwrap();
        assert_eq!(n, 0, "expected EOF on the idle keep-alive connection");
    }

    #[tokio::test]
    async fn in_flight_requests_complete_during_graceful_drain() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_signal) = test_shutdown_channel();
        let draining = Arc::new(AtomicBool::new(false));
        let app = Router::new().route(
            "/slow",
            axum::routing::get(|| async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                "slow-ok"
            }),
        );
        let server = tokio::spawn(serve_connections(
            listener,
            app,
            shutdown_signal,
            draining,
            test_loop_options(Duration::ZERO, Duration::from_secs(600)),
        ));

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /slow HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        // Give the server a moment to start handling the request,
        // then signal shutdown mid-response.
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown_tx.send(true).unwrap();

        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
            .await
            .expect("in-flight response must complete during drain")
            .unwrap();
        assert!(
            String::from_utf8_lossy(&response).contains("slow-ok"),
            "unexpected response: {}",
            String::from_utf8_lossy(&response)
        );

        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("server must stop after the in-flight request completes")
            .unwrap()
            .unwrap();
    }
}
