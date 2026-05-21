//! Property: concurrent PATCHes to the same upload are serialised.
//!
//! When multiple clients fire a PATCH at the same `upload_id` at the
//! same offset, the protocol's lock-then-CAS-on-offset machinery
//! must let exactly one win. The others see either:
//!
//!   - `409 Conflict` (Upload-Offset mismatch — the winner advanced
//!     the offset before this PATCH's own offset check), or
//!   - `423 Locked` (the lock guard rejected the second acquirer).
//!
//! Anything else — two PATCHes both winning, the offset advancing
//! by 2x the PATCH body size, a 5xx — is a race that would corrupt
//! the upload.
//!
//! This test fills the in-process, single-replica race surface.

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode},
};
use tower::ServiceExt;
use tus_axum::{TusState, create_router};
use tus_protocol::locking::memory::MemoryLocker;
use tus_protocol::state::memory::MemoryStateStore;
use tus_protocol::storage::memory::MemoryStorage;
use tus_protocol::{Config, NoopHookExecutor, ProtocolHandle};

const TUS_RESUMABLE: &str = "1.0.0";

fn build_router() -> Router {
    let state = TusState::new(ProtocolHandle::new(
        Config::default()
            .base_path("/files")
            .max_size(10 * 1024 * 1024),
        MemoryStorage::new(),
        MemoryStateStore::new(),
        MemoryLocker::new(),
        NoopHookExecutor::new(),
    ));
    create_router(state)
}

async fn create_upload(router: &Router, length: usize) -> String {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/files")
                .header("tus-resumable", TUS_RESUMABLE)
                .header("upload-length", length.to_string())
                .header("content-length", "0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let location = response
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    // Strip http://host if present; routing matches on path only.
    location
        .split_once("/files/")
        .map(|(_, id)| format!("/files/{id}"))
        .unwrap_or(location)
}

async fn patch_one_byte(router: Arc<Router>, item_path: String, offset: u64) -> StatusCode {
    let response = router
        .as_ref()
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(&item_path)
                .header("tus-resumable", TUS_RESUMABLE)
                .header("upload-offset", offset.to_string())
                .header("content-type", "application/offset+octet-stream")
                .header("content-length", "1")
                .body(Body::from("x"))
                .unwrap(),
        )
        .await
        .unwrap();
    response.status()
}

async fn head_offset(router: &Router, item_path: &str) -> u64 {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::HEAD)
                .uri(item_path)
                .header("tus-resumable", TUS_RESUMABLE)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response
        .headers()
        .get("upload-offset")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_patches_at_same_offset_serialize_to_one_winner() {
    let router = Arc::new(build_router());
    let item_path = create_upload(&router, 10 * 1024).await;

    // 8 concurrent PATCHes, every one at offset=0, every one with a
    // 1-byte body. Exactly one should reach the storage write.
    const N: usize = 8;
    let handles: Vec<_> = (0..N)
        .map(|_| {
            let r = router.clone();
            let p = item_path.clone();
            tokio::spawn(async move { patch_one_byte(r, p, 0).await })
        })
        .collect();

    let mut statuses = Vec::with_capacity(N);
    for h in handles {
        statuses.push(h.await.unwrap());
    }

    let winners = statuses
        .iter()
        .filter(|s| **s == StatusCode::NO_CONTENT)
        .count();
    let mismatches = statuses
        .iter()
        .filter(|s| **s == StatusCode::CONFLICT)
        .count();
    let lock_busy = statuses.iter().filter(|s| s.as_u16() == 423).count();
    let unexpected: Vec<_> = statuses
        .iter()
        .filter(|s| {
            **s != StatusCode::NO_CONTENT && **s != StatusCode::CONFLICT && s.as_u16() != 423
        })
        .collect();

    assert!(
        unexpected.is_empty(),
        "concurrent PATCH produced unexpected statuses: {unexpected:?} \
         (full set: {statuses:?})"
    );
    assert_eq!(
        winners, 1,
        "exactly one PATCH must win at the same offset; got {winners} \
         (statuses: {statuses:?})"
    );
    assert_eq!(
        mismatches + lock_busy,
        N - 1,
        "the other {} PATCHes must be 409 OffsetMismatch or 423 Locked; \
         got {mismatches} mismatches + {lock_busy} locked (statuses: {statuses:?})",
        N - 1,
    );

    // The single winner advanced the offset by exactly 1 byte.
    let final_offset = head_offset(&router, &item_path).await;
    assert_eq!(
        final_offset, 1,
        "a single 1-byte PATCH won; offset must be 1, got {final_offset}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_sequential_patches_advance_offset_monotonically() {
    // Sanity: not strictly a concurrency test, but pins down the
    // companion property — sequential PATCHes accumulate, so the
    // "concurrent only one wins" outcome above is the difference
    // a lock makes, not a side effect of the storage layer.
    let router = Arc::new(build_router());
    let item_path = create_upload(&router, 100).await;
    for i in 0..32 {
        let status = patch_one_byte(router.clone(), item_path.clone(), i).await;
        assert_eq!(
            status,
            StatusCode::NO_CONTENT,
            "sequential PATCH {i} failed"
        );
    }
    let offset = head_offset(&router, &item_path).await;
    assert_eq!(offset, 32);
}
