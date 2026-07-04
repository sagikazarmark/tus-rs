//! Slowloris-style request-body timeout test.
//!
//! A client that opens a connection, sends headers + a fat
//! `Content-Length`, then dribbles the body slowly (or stops
//! sending entirely) holds the server's connection AND the
//! upload's lock open. Without a body-read timeout, a handful
//! of malicious clients can DoS the server with negligible
//! bandwidth.
//!
//! The server exposes `--request-body-read-timeout <SECS>` which
//! wraps request bodies with an idle-timeout middleware (built on
//! `tower_http::timeout::TimeoutBody`, applied only to bodies that
//! are not already at end of stream). The default is 60 seconds, so
//! a stock server tears down stalled bodies on its own; `0` is the
//! explicit opt-out that disables the timeout. This
//! test pins down two properties:
//!
//!   1. With the timeout set, a stalled body causes the server to
//!      tear the connection within bounded time.
//!   2. With the explicit `0` opt-out, a stalled body holds the
//!      connection open. Operators who disable the timeout take on
//!      the slowloris exposure themselves.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn server_bin() -> &'static str {
    env!("CARGO_BIN_EXE_tus-server")
}

static RESERVED_PORTS: Mutex<Vec<u16>> = Mutex::new(Vec::new());

struct PortToken {
    port: u16,
}

struct ReservedPort {
    listener: Option<std::net::TcpListener>,
    token: PortToken,
}

impl Drop for PortToken {
    fn drop(&mut self) {
        let mut ports = RESERVED_PORTS
            .lock()
            .expect("reserved port registry must not be poisoned");
        if let Some(index) = ports.iter().position(|port| *port == self.port) {
            ports.swap_remove(index);
        }
    }
}

impl ReservedPort {
    fn into_token(self) -> PortToken {
        let Self { listener, token } = self;
        drop(listener);
        token
    }
}

impl std::fmt::Display for ReservedPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.token.port.fmt(f)
    }
}

fn reserve_port() -> ReservedPort {
    loop {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral port must be reserved");
        let port = listener
            .local_addr()
            .expect("reserved listener must have a local address")
            .port();
        let mut ports = RESERVED_PORTS
            .lock()
            .expect("reserved port registry must not be poisoned");
        if ports.contains(&port) {
            continue;
        }
        ports.push(port);
        drop(ports);

        return ReservedPort {
            listener: Some(listener),
            token: PortToken { port },
        };
    }
}

struct ServerProcess {
    child: Child,
    addr: String,
    _port_token: PortToken,
    _root: tempfile::TempDir,
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `timeout_secs` is passed through as an explicit
/// `--request-body-read-timeout` value; `0` is the documented
/// opt-out that disables the timeout entirely.
fn spawn_server(timeout_secs: u64) -> ServerProcess {
    spawn_server_with(timeout_secs, None)
}

/// Like [`spawn_server`], additionally passing
/// `--request-header-read-timeout` when `header_timeout_secs` is set.
fn spawn_server_with(timeout_secs: u64, header_timeout_secs: Option<u64>) -> ServerProcess {
    let root = tempfile::tempdir().unwrap();
    let state_dir: PathBuf = root.path().join("state");

    for _ in 0..10 {
        let reserved_port = reserve_port();
        let addr = format!("127.0.0.1:{reserved_port}");
        let mut args = vec![
            "serve".to_string(),
            "--addr".to_string(),
            addr.clone(),
            "--storage-uri".into(),
            "fs://".into(),
            "--state-dir".into(),
            state_dir.display().to_string(),
            "--max-size".into(),
            "104857600".into(),
            "--request-body-read-timeout".into(),
            timeout_secs.to_string(),
        ];
        if let Some(header_timeout_secs) = header_timeout_secs {
            args.push("--request-header-read-timeout".into());
            args.push(header_timeout_secs.to_string());
        }

        let port_token = reserved_port.into_token();
        let mut child = Command::new(server_bin())
            .args(&args)
            .current_dir(root.path())
            .env_clear()
            .env("TUS_STORAGE_ROOT", "uploads")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("tus-server must spawn");

        if child_exited_with_address_in_use(&mut child) {
            continue;
        }

        return ServerProcess {
            child,
            addr,
            _port_token: port_token,
            _root: root,
        };
    }

    panic!("tus-server could not bind an ephemeral test port after retries");
}

#[cfg(unix)]
#[test]
fn address_in_use_detection_waits_for_delayed_bind_failure() {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg("sleep 0.1; echo 'Error: Address already in use' >&2; exit 1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("delayed failure child must start");

    assert!(child_exited_with_address_in_use(&mut child));
}

fn child_exited_with_address_in_use(child: &mut Child) -> bool {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("child status must be readable") {
            let stderr = read_child_stderr(child);
            if stderr.contains("Address already in use") {
                return true;
            }

            panic!("server exited early with {status}: {stderr}");
        }

        std::thread::sleep(Duration::from_millis(25));
    }

    false
}

fn read_child_stderr(child: &mut Child) -> String {
    child
        .stderr
        .take()
        .map(|mut stderr| {
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut stderr, &mut bytes).expect("stderr must be readable");
            String::from_utf8_lossy(&bytes).into_owned()
        })
        .unwrap_or_default()
}

async fn wait_for_ready(server: &ServerProcess) {
    for _ in 0..100 {
        if let Ok(mut stream) = TcpStream::connect(&server.addr).await {
            let req = format!(
                "OPTIONS /files HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
                server.addr
            );
            if stream.write_all(req.as_bytes()).await.is_ok() {
                let mut buf = [0u8; 64];
                if tokio::time::timeout(Duration::from_millis(300), stream.read(&mut buf))
                    .await
                    .is_ok()
                {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server didn't become ready");
}

async fn create_upload(server: &ServerProcess, length: usize) -> String {
    let mut stream = TcpStream::connect(&server.addr).await.unwrap();
    let req = format!(
        "POST /files HTTP/1.1\r\n\
         Host: {}\r\n\
         Tus-Resumable: 1.0.0\r\n\
         Upload-Length: {length}\r\n\
         Content-Length: 0\r\n\
         Connection: close\r\n\
         \r\n",
        server.addr,
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let location_line = response
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("location:"))
        .expect("Location header in POST response");
    let location = location_line[9..].trim();
    location
        .split_once("/files/")
        .map(|(_, id)| format!("/files/{id}"))
        .unwrap_or_else(|| location.to_string())
}

/// Open a PATCH stream, send the headers, then write the body one
/// byte at a time with `byte_interval` between writes. Concurrently
/// reads the response so we can detect a server-initiated close.
///
/// Returns: (elapsed, closed_by_server).
async fn dribble_patch(
    server: &ServerProcess,
    item_path: &str,
    body_len: usize,
    byte_interval: Duration,
    wall_timeout: Duration,
) -> (Duration, bool) {
    let stream = TcpStream::connect(&server.addr).await.unwrap();
    let (mut read_half, mut write_half) = stream.into_split();

    let req = format!(
        "PATCH {item_path} HTTP/1.1\r\n\
         Host: {}\r\n\
         Tus-Resumable: 1.0.0\r\n\
         Upload-Offset: 0\r\n\
         Content-Type: application/offset+octet-stream\r\n\
         Content-Length: {body_len}\r\n\
         \r\n",
        server.addr,
    );
    write_half.write_all(req.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();

    let start = Instant::now();

    let read_done = tokio::spawn(async move {
        let mut buf = [0u8; 256];
        loop {
            match read_half.read(&mut buf).await {
                Ok(0) => return true,
                Ok(_) => continue,
                Err(_) => return true,
            }
        }
    });

    let dribble = async move {
        for _ in 0..body_len {
            if write_half.write_all(b"x").await.is_err() {
                return;
            }
            if write_half.flush().await.is_err() {
                return;
            }
            tokio::time::sleep(byte_interval).await;
        }
    };

    tokio::select! {
        _ = dribble => (start.elapsed(), false),
        closed = read_done => (start.elapsed(), closed.unwrap_or(false)),
        _ = tokio::time::sleep(wall_timeout) => (start.elapsed(), false),
    }
}

#[tokio::test]
async fn slow_body_times_out_when_request_body_read_timeout_is_set() {
    let server = spawn_server(2);
    wait_for_ready(&server).await;

    let item = create_upload(&server, 4096).await;
    let (elapsed, closed) = dribble_patch(
        &server,
        &item,
        4096,
        Duration::from_secs(5),
        Duration::from_secs(15),
    )
    .await;

    assert!(
        closed,
        "server did not close the slow connection within 15 s — \
         --request-body-read-timeout=2 failed to enforce"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "server eventually closed the connection but took {elapsed:?} — \
         expected close within ~5 s of the timeout firing"
    );
}

/// Header-phase slowloris: a client that connects and then trickles
/// (or never finishes) the request *headers* must be torn down by the
/// server's header-read timeout. This is enforced by hyper's
/// `header_read_timeout`, which only works because the serve loop
/// installs a `TokioTimer` — without a timer hyper silently disables
/// the timeout.
#[tokio::test]
async fn slow_headers_time_out_when_header_read_timeout_is_set() {
    let server = spawn_server_with(60, Some(1));
    wait_for_ready(&server).await;

    let stream = TcpStream::connect(&server.addr).await.unwrap();
    let (mut read_half, mut write_half) = stream.into_split();

    // Send an incomplete header section, then dribble one byte at a
    // time slower than the 1 s header timeout.
    write_half
        .write_all(b"PATCH /files/whatever HTTP/1.1\r\nHost: localhost\r\nX-Slow: ")
        .await
        .unwrap();
    write_half.flush().await.unwrap();

    let start = Instant::now();
    let closed = tokio::time::timeout(Duration::from_secs(10), async move {
        let mut buf = [0u8; 256];
        loop {
            // Keep the header section forever unterminated.
            tokio::select! {
                read = read_half.read(&mut buf) => {
                    match read {
                        Ok(0) | Err(_) => return true,
                        Ok(_) => continue,
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(2)) => {
                    if write_half.write_all(b"x").await.is_err()
                        || write_half.flush().await.is_err()
                    {
                        return true;
                    }
                }
            }
        }
    })
    .await
    .unwrap_or(false);
    let elapsed = start.elapsed();

    assert!(
        closed,
        "server did not close the header-trickling connection within 10 s — \
         --request-header-read-timeout=1 failed to enforce"
    );
    assert!(
        elapsed < Duration::from_secs(6),
        "server eventually closed the connection but took {elapsed:?} — \
         expected close within a few seconds of the 1 s header timeout firing"
    );
}

#[tokio::test]
async fn slow_body_is_not_timed_out_when_timeout_is_disabled_explicitly() {
    // Documents the explicit opt-out: --request-body-read-timeout=0
    // disables the timeout, so a slow body holds the connection.
    // The DEFAULT (no flag) is 60 s, which is too slow to exercise
    // in CI; the opt-out path is what remains observable here.
    // Wall budget kept small to keep CI fast.
    let server = spawn_server(0);
    wait_for_ready(&server).await;

    let item = create_upload(&server, 4096).await;
    let (_elapsed, closed) = dribble_patch(
        &server,
        &item,
        4096,
        Duration::from_secs(5),
        Duration::from_secs(8),
    )
    .await;

    assert!(
        !closed,
        "server closed a slow connection despite --request-body-read-timeout=0 — \
         the explicit opt-out no longer disables the timeout; update operator docs"
    );
}
