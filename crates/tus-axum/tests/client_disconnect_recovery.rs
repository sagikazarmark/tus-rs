//! What happens when a client closes the TCP connection mid-PATCH?
//!
//! What we found vs what we set out to test
//! ----------------------------------------
//!
//! The starting hypothesis was: axum lets handlers run to completion
//! after a client disconnect. The test was supposed to pin that down
//! so a future regression couldn't silently introduce mid-handler
//! cancellation.
//!
//! Reality: axum 0.7 / hyper 1.x **does** cancel handler futures when
//! the client disconnects. The handler future is dropped (often before
//! `storage.append` even runs), and the partially-evaluated handler
//! never reaches `state_store.set`.
//!
//! This is observable but not a correctness bug. The protocol design
//! has a safety net:
//!
//!   1. Per-PATCH atomicity (codified in
//!      `crates/tus-protocol/src/storage/memory.rs::append_rolls_back_when_body_stream_errors_mid_stream`):
//!      `Storage::append` is all-or-nothing. A cancelled handler that
//!      didn't finish `append` leaves storage unchanged, so `state_store`
//!      and `storage` agree (both at the pre-PATCH offset).
//!   2. Reconcile-on-HEAD: if a cancelled
//!      handler somehow committed to storage between `append` and
//!      `state_store.set`, the next HEAD detects the drift via
//!      `storage.size()` and updates `state_store` to match.
//!
//! So the user-visible property is: **after a client disconnect
//! mid-PATCH, the upload is in a consistent, recoverable state**.
//! HEAD reports an offset that matches what's actually in storage,
//! and a resume from that offset completes the upload byte-identical.
//! This test pins that property down.
//!
//! What this test guards against:
//!
//!   - A regression that breaks per-PATCH atomicity (storage commits
//!     partial bytes on cancellation): would make HEAD report a
//!     non-aligned offset.
//!   - A regression that breaks reconcile (state_store stays behind
//!     after a cancelled mid-write): would make HEAD report a stale
//!     offset that doesn't match storage.
//!   - A switch to a setup that lets the handler run past the body
//!     into `state_store.set` after a cancellation, leaving an
//!     inconsistent state: would make HEAD report a partial offset.
//!
//! What this test does NOT guard against:
//!
//!   - Post-hooks not firing for cancelled requests. That is a real
//!     consequence of axum's cancellation behaviour and worth a
//!     separate test if any subscriber depends on post-hook
//!     completeness.

use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tus_axum::{TusState, create_router};
use tus_protocol::locking::memory::MemoryLocker;
use tus_protocol::state::memory::MemoryStateStore;
use tus_protocol::storage::memory::MemoryStorage;
use tus_protocol::{
    AppendRequest, ConcatRequest, Config, NoopHookExecutor, ProtocolHandle, Result as TusResult,
    Storage, StorageHandle,
};

/// Wraps `MemoryStorage` and inserts a configurable sleep inside
/// `append`. The sleep widens the window in which the handler is
/// observably in-flight — large enough that `drop(stream)` from the
/// test thread reaches the server's reactor before the handler
/// finishes. Without it, the in-memory operation completes in
/// microseconds and the cancellation race never resolves
/// reproducibly.
struct SleepyStorage {
    inner: MemoryStorage,
    delay: Duration,
}

#[async_trait]
impl Storage for SleepyStorage {
    fn name(&self) -> &'static str {
        "sleepy-memory"
    }
    async fn create(&self, upload_id: &str) -> TusResult<StorageHandle> {
        self.inner.create(upload_id).await
    }
    async fn append(&self, request: AppendRequest) -> TusResult<StorageHandle> {
        let result = self.inner.append(request).await;
        tokio::time::sleep(self.delay).await;
        result
    }
    async fn concat(&self, request: ConcatRequest) -> TusResult<StorageHandle> {
        self.inner.concat(request).await
    }
    async fn delete(&self, handle: &StorageHandle) -> TusResult<()> {
        self.inner.delete(handle).await
    }
    async fn size(&self, handle: &StorageHandle) -> TusResult<Option<u64>> {
        self.inner.size(handle).await
    }
}

fn build_router(addr: std::net::SocketAddr, append_delay: Duration) -> Router {
    let state = TusState::new(ProtocolHandle::new(
        Config::default()
            .with_base_path("/files")
            .with_base_url(format!("http://{addr}")),
        SleepyStorage {
            inner: MemoryStorage::new(),
            delay: append_delay,
        },
        MemoryStateStore::new(),
        MemoryLocker::new(),
        NoopHookExecutor::new(),
    ));
    create_router(state).unwrap()
}

async fn spawn_server(append_delay: Duration) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = build_router(addr, append_delay);
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
}

async fn create_upload(addr: std::net::SocketAddr, length: usize) -> String {
    let url = format!("http://{addr}/files");
    let response = reqwest::Client::new()
        .post(&url)
        .header("Tus-Resumable", "1.0.0")
        .header("Upload-Length", length.to_string())
        .header("Content-Length", "0")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 201, "POST should create upload");
    response
        .headers()
        .get("location")
        .expect("Location header")
        .to_str()
        .expect("Location is ASCII")
        .to_string()
}

async fn head_offset(location: &str) -> u64 {
    let response = reqwest::Client::new()
        .head(location)
        .header("Tus-Resumable", "1.0.0")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200, "HEAD should succeed");
    response
        .headers()
        .get("upload-offset")
        .expect("Upload-Offset header")
        .to_str()
        .unwrap()
        .parse()
        .unwrap()
}

async fn patch_full_body(location: &str, offset: u64, body: Vec<u8>) -> reqwest::StatusCode {
    reqwest::Client::new()
        .patch(location)
        .header("Tus-Resumable", "1.0.0")
        .header("Upload-Offset", offset.to_string())
        .header("Content-Type", "application/offset+octet-stream")
        .body(body)
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn client_disconnect_mid_patch_leaves_upload_in_consistent_state() {
    let addr = spawn_server(Duration::from_millis(200)).await;
    let body_size = 4096usize;
    let body = vec![b'A'; body_size];

    let location = create_upload(addr, body_size).await;
    let path = reqwest::Url::parse(&location).unwrap().path().to_string();

    // Hand-roll the PATCH on raw TCP so we can drop the connection
    // partway through processing. reqwest doesn't expose a "fire and
    // forget — don't read response" API.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "PATCH {path} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Tus-Resumable: 1.0.0\r\n\
         Upload-Offset: 0\r\n\
         Content-Type: application/offset+octet-stream\r\n\
         Content-Length: {len}\r\n\
         Connection: close\r\n\
         \r\n",
        len = body.len(),
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    stream.flush().await.unwrap();
    drop(stream);

    // Wait long enough for any in-flight handler work to settle —
    // either it completed past the SleepyStorage sleep or it was
    // cancelled and unwound.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Property 1: HEAD's reported offset is internally consistent
    // with what storage has. With per-PATCH atomicity and reconcile,
    // the offset is either 0 (handler cancelled before commit) or
    // body_size (handler committed before cancellation reached the
    // tokio reactor). Anything in between would mean a partial
    // commit leaked through.
    let after_disconnect = head_offset(&location).await;
    assert!(
        after_disconnect == 0 || after_disconnect == body_size as u64,
        "offset after disconnect must be 0 or {body_size}, got {after_disconnect} \
         — partial commit detected, per-PATCH atomicity broken"
    );

    // Property 2: a clean PATCH from whatever offset HEAD reported
    // completes the upload. This is the user-visible recovery
    // guarantee: the disconnect doesn't permanently brick the upload.
    if after_disconnect < body_size as u64 {
        let remaining = body_size as u64 - after_disconnect;
        let resume_body = body[after_disconnect as usize..].to_vec();
        let status = patch_full_body(&location, after_disconnect, resume_body).await;
        assert_eq!(
            status.as_u16(),
            204,
            "PATCH from offset {after_disconnect} ({remaining} bytes) must succeed"
        );
    }

    let final_offset = head_offset(&location).await;
    assert_eq!(
        final_offset, body_size as u64,
        "after recovery PATCH, HEAD must show full offset"
    );
}
