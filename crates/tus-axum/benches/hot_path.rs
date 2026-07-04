//! Microbenchmarks for the hot paths of the TUS server.
//!
//! Two axes we care about:
//!
//! - **Chunk size** — the cost of a PATCH should scale linearly with the
//!   body, and shouldn't degrade sharply at larger sizes.
//! - **Adapter overhead** — `axum_patch` runs a PATCH through
//!   `axum::Router` while `protocol_patch` calls the framework-neutral
//!   [`Protocol::patch`] facade directly. The difference is the cost of the
//!   axum adapter (extractors, router dispatch, response assembly).
//!
//! All benches use the in-memory storage/state backends to eliminate I/O.
//! Each sample is wrapped in `iter_batched` so state is freshly allocated
//! per iteration — otherwise `MemoryStorage` would accumulate gigabytes of
//! written bytes across the thousands of iterations criterion runs.

use std::hint::black_box;
use std::time::Duration;

use axum::{
    Router,
    body::Body,
    http::{Method, Request},
};
use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use tower::ServiceExt;

use tus_axum::{RouterOptions, TusState, create_router};
use tus_protocol::locking::memory::MemoryLocker;
use tus_protocol::state::memory::MemoryStateStore;
use tus_protocol::storage::memory::MemoryStorage;
use tus_protocol::{
    ChunkStream, Config, Headers, NoopHookExecutor, Protocol, ProtocolHandle, RequestBody,
    StateStore, Storage, TUS_RESUMABLE, UploadState,
};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn config() -> Config {
    Config::with_all_extensions().with_base_path("/files")
}

fn patch_headers(offset: u64, size: u64) -> Headers {
    let mut headers = Headers::default();
    headers.upload_offset = Some(offset);
    headers.content_type = Some("application/offset+octet-stream".to_string());
    headers.content_length = Some(size);
    headers
}

/// Freshly-initialized protocol-level state for a single-iteration bench.
struct DirectState {
    config: Config,
    storage: MemoryStorage,
    state_store: MemoryStateStore,
    locker: MemoryLocker,
    hooks: NoopHookExecutor,
    upload_id: String,
}

async fn direct_setup() -> DirectState {
    let config = config();
    let storage = MemoryStorage::new();
    let state_store = MemoryStateStore::new();
    let locker = MemoryLocker::new();
    let hooks = NoopHookExecutor::new();

    let mut state = UploadState::new("bench-upload").with_length(u64::MAX);
    let handle = storage.create(state.id()).await.unwrap();
    state.set_storage_handle(handle);
    state_store.set(&state, true).await.unwrap();

    DirectState {
        config,
        storage,
        state_store,
        locker,
        hooks,
        upload_id: "bench-upload".to_string(),
    }
}

async fn axum_setup() -> (Router, String) {
    let state = TusState::new(ProtocolHandle::new(
        config(),
        MemoryStorage::new(),
        MemoryStateStore::new(),
        MemoryLocker::new(),
        NoopHookExecutor::new(),
    ));
    let router = create_router(state, &RouterOptions::default()).unwrap();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/files")
                .header("tus-resumable", TUS_RESUMABLE)
                .header("upload-length", u64::MAX.to_string())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let upload_id = response
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_string();
    (router, upload_id)
}

// ---------------------------------------------------------------------------
// Bench: Protocol::patch directly
// ---------------------------------------------------------------------------

fn bench_protocol_patch(c: &mut Criterion) {
    let rt = runtime();

    let mut group = c.benchmark_group("protocol_patch");
    group.measurement_time(Duration::from_secs(5));

    for size in [
        4 * 1024,    // 4 KiB
        64 * 1024,   // 64 KiB
        1024 * 1024, // 1 MiB
    ] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{} B", size)),
            &size,
            |b, &size| {
                let payload_template = Bytes::from(vec![0u8; size]);
                b.to_async(&rt).iter_batched(
                    || {
                        // `futures::executor::block_on` sidesteps the tokio
                        // "nested runtime" panic — the memory-backed
                        // Storage/StateStore APIs are async in signature
                        // but do purely synchronous work.
                        let s = futures::executor::block_on(direct_setup());
                        (s, payload_template.clone())
                    },
                    |(s, payload)| async move {
                        let upload_id = s.upload_id.parse().unwrap();
                        let response = Protocol::new(
                            &s.config,
                            &s.storage,
                            &s.state_store,
                            &s.locker,
                            &s.hooks,
                        )
                        .patch(
                            patch_headers(0, size as u64),
                            &upload_id,
                            RequestBody::from_chunk_stream(ChunkStream::from_bytes(payload)),
                        )
                        .await
                        .unwrap();
                        black_box(response);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Bench: the same PATCH through the axum router
// ---------------------------------------------------------------------------

fn bench_axum_patch(c: &mut Criterion) {
    let rt = runtime();

    let mut group = c.benchmark_group("axum_patch");
    group.measurement_time(Duration::from_secs(5));

    for size in [
        4 * 1024,    // 4 KiB
        64 * 1024,   // 64 KiB
        1024 * 1024, // 1 MiB
    ] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{} B", size)),
            &size,
            |b, &size| {
                let payload_template = Bytes::from(vec![0u8; size]);
                b.to_async(&rt).iter_batched(
                    || {
                        let (router, upload_id) = futures::executor::block_on(axum_setup());
                        (router, upload_id, payload_template.clone())
                    },
                    |(router, upload_id, payload)| async move {
                        let response = router
                            .oneshot(
                                Request::builder()
                                    .method(Method::PATCH)
                                    .uri(format!("/files/{}", upload_id))
                                    .header("tus-resumable", TUS_RESUMABLE)
                                    .header("content-type", "application/offset+octet-stream")
                                    .header("upload-offset", "0")
                                    .header("content-length", size.to_string())
                                    .body(Body::from(payload))
                                    .unwrap(),
                            )
                            .await
                            .unwrap();
                        black_box(response);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Bench: POST → PATCH → HEAD → DELETE at a fixed 64 KiB body
// ---------------------------------------------------------------------------

fn bench_lifecycle(c: &mut Criterion) {
    let rt = runtime();
    let payload_size: usize = 64 * 1024;

    let mut group = c.benchmark_group("lifecycle");
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(1));

    group.bench_function("post_patch_head_delete_64kb", |b| {
        let payload_template = Bytes::from(vec![0u8; payload_size]);
        b.to_async(&rt).iter_batched(
            || {
                let state = TusState::new(ProtocolHandle::new(
                    config(),
                    MemoryStorage::new(),
                    MemoryStateStore::new(),
                    MemoryLocker::new(),
                    NoopHookExecutor::new(),
                ));
                (create_router(state, &RouterOptions::default()).unwrap(), payload_template.clone())
            },
            |(router, payload)| async move {
                // POST
                let post = router
                    .clone()
                    .oneshot(
                        Request::builder()
                            .method(Method::POST)
                            .uri("/files")
                            .header("tus-resumable", TUS_RESUMABLE)
                            .header("upload-length", payload_size.to_string())
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let upload_id = post
                    .headers()
                    .get("location")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .rsplit('/')
                    .next()
                    .unwrap()
                    .to_string();
                let item = format!("/files/{}", upload_id);

                // PATCH
                router
                    .clone()
                    .oneshot(
                        Request::builder()
                            .method(Method::PATCH)
                            .uri(&item)
                            .header("tus-resumable", TUS_RESUMABLE)
                            .header("content-type", "application/offset+octet-stream")
                            .header("upload-offset", "0")
                            .header("content-length", payload_size.to_string())
                            .body(Body::from(payload))
                            .unwrap(),
                    )
                    .await
                    .unwrap();

                // HEAD
                router
                    .clone()
                    .oneshot(
                        Request::builder()
                            .method(Method::HEAD)
                            .uri(&item)
                            .header("tus-resumable", TUS_RESUMABLE)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();

                // DELETE
                let delete = router
                    .oneshot(
                        Request::builder()
                            .method(Method::DELETE)
                            .uri(&item)
                            .header("tus-resumable", TUS_RESUMABLE)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                black_box(delete);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_protocol_patch,
    bench_axum_patch,
    bench_lifecycle
);
criterion_main!(benches);
