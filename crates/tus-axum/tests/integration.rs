//! End-to-end integration tests.
//!
//! Drives a real `axum::Router` (from `tus_axum::create_router`) through
//! `tower::ServiceExt::oneshot`, exercising the full stack:
//! `axum adapter` -> `tus_protocol` -> `Storage` / `StateStore` /
//! `Locker` / `HookExecutor`. Backends are the in-memory implementations so
//! tests are fast and require no filesystem or network.
//!
//! These tests complement the per-module unit tests: they catch integration
//! bugs (routing, extractor wiring, response assembly) that slip past
//! isolated tests.

use axum::{
    Router,
    body::Body,
    http::{Method, Request, Response, StatusCode},
};
use base64::Engine;
use bytes::Bytes;
use http_body_util::BodyExt;
use tower::ServiceExt;

use async_trait::async_trait;
use tus_axum::{RouterOptions, TusState, create_router};
use tus_protocol::{
    Config, Extension, HookContext, HookExecutor, NoopHookExecutor, PreHookResult, ProtocolHandle,
    Result as TusResult, TUS_RESUMABLE, locking::memory::MemoryLocker,
    state::memory::MemoryStateStore, storage::memory::MemoryStorage,
};

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

fn default_config() -> Config {
    Config::all_extensions()
        .with_base_path("/files")
        .with_max_size(10 * 1024 * 1024)
}

fn build_router_with(config: Config) -> Router {
    let state = TusState::new(ProtocolHandle::new(
        config,
        MemoryStorage::new(),
        MemoryStateStore::new(),
        MemoryLocker::new(),
        NoopHookExecutor::new(),
    ));
    create_router(state, &RouterOptions::default()).unwrap()
}

fn build_router() -> Router {
    build_router_with(default_config())
}

fn tus_request(method: Method, uri: &str) -> axum::http::request::Builder {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("tus-resumable", TUS_RESUMABLE)
}

async fn send(router: Router, req: Request<Body>) -> Response<Body> {
    router.oneshot(req).await.expect("router must not fail")
}

async fn read_body(response: Response<Body>) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .expect("body collectable")
        .to_bytes()
        .to_vec()
}

fn upload_id_from_location(location: &str) -> &str {
    location.rsplit('/').next().expect("location has an id")
}

// ---------------------------------------------------------------------------
// Happy path: POST → HEAD → PATCH → PATCH → HEAD → DELETE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn happy_path_lifecycle() {
    let router = build_router();

    // POST — create a 10-byte upload.
    let response = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", "10")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let location = response
        .headers()
        .get("location")
        .expect("location header present")
        .to_str()
        .unwrap()
        .to_string();
    let id = upload_id_from_location(&location).to_string();
    let item = format!("/files/{}", id);

    // HEAD — fresh upload has offset 0.
    let response = send(
        router.clone(),
        tus_request(Method::HEAD, &item)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("upload-offset").unwrap(), "0");
    assert_eq!(response.headers().get("upload-length").unwrap(), "10");

    // PATCH — first 4 bytes.
    let response = send(
        router.clone(),
        tus_request(Method::PATCH, &item)
            .header("content-type", "application/offset+octet-stream")
            .header("upload-offset", "0")
            .header("content-length", "4")
            .body(Body::from("Hell"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers().get("upload-offset").unwrap(), "4");

    // PATCH — remaining 6 bytes. Completes the upload.
    let response = send(
        router.clone(),
        tus_request(Method::PATCH, &item)
            .header("content-type", "application/offset+octet-stream")
            .header("upload-offset", "4")
            .header("content-length", "6")
            .body(Body::from("o worl"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers().get("upload-offset").unwrap(), "10");

    // HEAD — upload is complete.
    let response = send(
        router.clone(),
        tus_request(Method::HEAD, &item)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.headers().get("upload-offset").unwrap(), "10");

    // DELETE — terminate.
    let response = send(
        router.clone(),
        tus_request(Method::DELETE, &item)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // HEAD — upload is gone.
    let response = send(
        router,
        tus_request(Method::HEAD, &item)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// OPTIONS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn options_advertises_extensions_and_max_size() {
    let router = build_router();
    let response = send(
        router,
        Request::builder()
            .method(Method::OPTIONS)
            .uri("/files")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    // OPTIONS does not require Tus-Resumable and should return 200.
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("tus-resumable").unwrap(),
        TUS_RESUMABLE
    );
    assert_eq!(
        response.headers().get("tus-version").unwrap(),
        TUS_RESUMABLE
    );
    let extensions = response
        .headers()
        .get("tus-extension")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(extensions.contains("creation"));
    assert!(extensions.contains("termination"));
    assert!(extensions.contains("expiration"));
    assert!(extensions.contains("concatenation"));
    assert_eq!(
        response.headers().get("tus-max-size").unwrap(),
        &(10 * 1024 * 1024).to_string()
    );
}

// ---------------------------------------------------------------------------
// Protocol version enforcement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn missing_tus_resumable_returns_412() {
    let router = build_router();
    let response = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/files")
            .header("upload-length", "10")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    // Tus-Version must be advertised on the error response per the TUS spec.
    assert!(response.headers().get("tus-version").is_some());
}

#[tokio::test]
async fn unsupported_tus_version_returns_412() {
    let router = build_router();
    let response = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/files")
            .header("tus-resumable", "0.9.0")
            .header("upload-length", "10")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
}

// ---------------------------------------------------------------------------
// Creation-With-Upload
// ---------------------------------------------------------------------------

#[tokio::test]
async fn creation_with_upload_in_single_request() {
    let router = build_router();
    let payload = b"hello world";

    let response = send(
        router,
        tus_request(Method::POST, "/files")
            .header("upload-length", payload.len())
            .header("content-type", "application/offset+octet-stream")
            .header("content-length", payload.len())
            .body(Body::from(payload.as_ref()))
            .unwrap(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.headers().get("upload-offset").unwrap(),
        &payload.len().to_string()
    );
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn head_unknown_upload_returns_404() {
    let router = build_router();
    let response = send(
        router,
        tus_request(Method::HEAD, "/files/does-not-exist")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn patch_with_wrong_offset_returns_409() {
    let router = build_router();
    // Create a 100-byte upload.
    let post = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", "100")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let item = format!(
        "/files/{}",
        upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
    );

    // Upload starts at offset 0; client sends offset 10.
    let response = send(
        router,
        tus_request(Method::PATCH, &item)
            .header("content-type", "application/offset+octet-stream")
            .header("upload-offset", "10")
            .header("content-length", "5")
            .body(Body::from("xxxxx"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    // Best-practice: 409 responses carry the server's authoritative offset
    // so clients can recover without an extra HEAD round-trip.
    assert_eq!(response.headers().get("upload-offset").unwrap(), "0");
}

#[tokio::test]
async fn post_with_body_and_wrong_content_type_returns_415() {
    let router = build_router();
    let response = send(
        router,
        tus_request(Method::POST, "/files")
            .header("upload-length", "12")
            .header("content-type", "text/plain")
            .header("content-length", "12")
            .body(Body::from("Hello, tus!\n"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn post_final_concat_with_upload_length_is_rejected() {
    let router = build_router();

    // Seed a partial upload so we have a URL to reference.
    let partial = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", "5")
            .header("upload-concat", "partial")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let partial_id =
        upload_id_from_location(partial.headers().get("location").unwrap().to_str().unwrap())
            .to_string();

    // Attempt final POST with Upload-Length — spec says it MUST NOT be set.
    let response = send(
        router,
        tus_request(Method::POST, "/files")
            .header("upload-concat", format!("final;/files/{}", partial_id))
            .header("upload-length", "5")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert!(
        response.status().is_client_error(),
        "expected 4xx, got {}",
        response.status()
    );
}

#[tokio::test]
async fn patch_without_content_type_returns_415() {
    let router = build_router();
    let post = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", "100")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let item = format!(
        "/files/{}",
        upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
    );

    let response = send(
        router,
        tus_request(Method::PATCH, &item)
            .header("upload-offset", "0")
            .header("content-length", "5")
            .body(Body::from("xxxxx"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn post_exceeding_max_size_returns_413() {
    let router = build_router_with(
        Config::all_extensions()
            .with_base_path("/files")
            .with_max_size(100),
    );
    let response = send(
        router,
        tus_request(Method::POST, "/files")
            .header("upload-length", "1000")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn post_both_length_and_defer_length_is_rejected() {
    let router = build_router();
    let response = send(
        router,
        tus_request(Method::POST, "/files")
            .header("upload-length", "100")
            .header("upload-defer-length", "1")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Deferred length
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deferred_length_set_on_first_patch() {
    let router = build_router();

    // Create with Upload-Defer-Length: 1 — no Upload-Length yet.
    let post = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-defer-length", "1")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(post.status(), StatusCode::CREATED);
    let item = format!(
        "/files/{}",
        upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
    );

    // HEAD — Upload-Length absent, Upload-Defer-Length: 1.
    let head = send(
        router.clone(),
        tus_request(Method::HEAD, &item)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert!(head.headers().get("upload-length").is_none());
    assert_eq!(head.headers().get("upload-defer-length").unwrap(), "1");

    // PATCH with Upload-Length to finalize the size, then write the full body.
    let response = send(
        router.clone(),
        tus_request(Method::PATCH, &item)
            .header("content-type", "application/offset+octet-stream")
            .header("upload-offset", "0")
            .header("upload-length", "5")
            .header("content-length", "5")
            .body(Body::from("hello"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // HEAD — now reports Upload-Length and is complete.
    let head = send(
        router,
        tus_request(Method::HEAD, &item)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(head.headers().get("upload-length").unwrap(), "5");
    assert_eq!(head.headers().get("upload-offset").unwrap(), "5");
}

// ---------------------------------------------------------------------------
// Concatenation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concatenation_creates_final_from_partials() {
    let router = build_router();

    // Create two partial uploads, each 4 bytes.
    async fn make_partial(router: &Router, body: &'static [u8]) -> String {
        let post = router
            .clone()
            .oneshot(
                tus_request(Method::POST, "/files")
                    .header("upload-length", body.len())
                    .header("upload-concat", "partial")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(post.status(), StatusCode::CREATED);
        let location = post
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let id = upload_id_from_location(&location).to_string();
        let item = format!("/files/{}", id);

        // Fill it.
        let patch = router
            .clone()
            .oneshot(
                tus_request(Method::PATCH, &item)
                    .header("content-type", "application/offset+octet-stream")
                    .header("upload-offset", "0")
                    .header("content-length", body.len())
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(patch.status(), StatusCode::NO_CONTENT);
        id
    }

    let part1_id = make_partial(&router, b"ABCD").await;
    let part2_id = make_partial(&router, b"EFGH").await;

    // Create a final upload that concatenates both.
    let concat_value = format!("final;/files/{} /files/{}", part1_id, part2_id);
    let response = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-concat", &concat_value)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers().get("upload-length").unwrap(), "8");
    assert_eq!(response.headers().get("upload-offset").unwrap(), "8");

    let final_id = upload_id_from_location(
        response
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap(),
    )
    .to_string();
    let final_item = format!("/files/{}", final_id);

    // HEAD on the final upload reports Upload-Concat: final.
    let head = send(
        router,
        tus_request(Method::HEAD, &final_item)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let concat = head
        .headers()
        .get("upload-concat")
        .unwrap()
        .to_str()
        .unwrap();
    // Per the TUS spec the response MUST advertise the participating parts:
    // `Upload-Concat: final;<url> <url>`.
    assert!(
        concat.starts_with("final;"),
        "expected 'final;<urls>', got {:?}",
        concat
    );
    assert!(concat.contains("/files/"), "got {:?}", concat);
    assert_eq!(head.headers().get("upload-length").unwrap(), "8");
}

#[tokio::test]
async fn patch_on_final_upload_is_forbidden() {
    let router = build_router();

    // Seed a partial + a final that references it.
    async fn partial(router: &Router) -> String {
        let post = router
            .clone()
            .oneshot(
                tus_request(Method::POST, "/files")
                    .header("upload-length", "4")
                    .header("upload-concat", "partial")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let id = upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
            .to_string();
        let item = format!("/files/{}", id);
        router
            .clone()
            .oneshot(
                tus_request(Method::PATCH, &item)
                    .header("content-type", "application/offset+octet-stream")
                    .header("upload-offset", "0")
                    .header("content-length", "4")
                    .body(Body::from("ABCD"))
                    .unwrap(),
            )
            .await
            .unwrap();
        id
    }

    let part = partial(&router).await;

    let concat = format!("final;/files/{}", part);
    let final_response = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-concat", &concat)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let final_id = upload_id_from_location(
        final_response
            .headers()
            .get("location")
            .unwrap()
            .to_str()
            .unwrap(),
    )
    .to_string();

    // PATCH on the final upload is forbidden.
    let response = send(
        router,
        tus_request(Method::PATCH, &format!("/files/{}", final_id))
            .header("content-type", "application/offset+octet-stream")
            .header("upload-offset", "0")
            .header("content-length", "4")
            .body(Body::from("xxxx"))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

// ---------------------------------------------------------------------------
// Metadata round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn upload_metadata_round_trips_through_head() {
    let router = build_router();
    // "filename" -> "test.txt"
    let filename_b64 = base64::engine::general_purpose::STANDARD.encode("test.txt");
    let metadata = format!("filename {}", filename_b64);

    let post = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", "10")
            .header("upload-metadata", &metadata)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let item = format!(
        "/files/{}",
        upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
    );

    let head = send(
        router,
        tus_request(Method::HEAD, &item)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let returned = head
        .headers()
        .get("upload-metadata")
        .expect("metadata header present")
        .to_str()
        .unwrap();
    assert!(returned.contains("filename"));
    assert!(returned.contains(&filename_b64));
}

// ---------------------------------------------------------------------------
// Termination extension gating
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_rejected_when_termination_extension_is_disabled() {
    let config = Config::all_extensions()
        .with_base_path("/files")
        .without_extension(Extension::Termination);
    let router = build_router_with(config);

    // Create something to delete.
    let post = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", "10")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let item = format!(
        "/files/{}",
        upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
    );

    let response = send(
        router,
        tus_request(Method::DELETE, &item)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Error body should mention the missing upload id (for body-bearing methods)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn not_found_body_mentions_upload_id() {
    // Use DELETE (not HEAD) so the response is permitted to carry a body per
    // HTTP semantics — HEAD responses may legitimately be stripped upstream.
    let router = build_router();
    let response = send(
        router,
        tus_request(Method::DELETE, "/files/does-not-exist")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(read_body(response).await).unwrap();
    assert!(
        body.contains("does-not-exist"),
        "error body should mention the missing upload id, got {:?}",
        body
    );
}

// ---------------------------------------------------------------------------
// Checksum extension: 460 on mismatch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn patch_checksum_mismatch_returns_460() {
    let router = build_router();

    let post = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", "5")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(post.status(), StatusCode::CREATED);
    let item = format!(
        "/files/{}",
        upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
    );

    // Send a body "hello" with a deliberately wrong sha1 checksum.
    let wrong_sha1 = base64::engine::general_purpose::STANDARD.encode([0u8; 20]);
    let checksum_header = format!("sha1 {}", wrong_sha1);

    let response = send(
        router,
        tus_request(Method::PATCH, &item)
            .header("content-type", "application/offset+octet-stream")
            .header("upload-offset", "0")
            .header("content-length", "5")
            .header("upload-checksum", checksum_header)
            .body(Body::from(&b"hello"[..]))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status().as_u16(), 460);
}

#[tokio::test]
async fn patch_accepts_checksum_trailer_through_axum_body_frames() {
    use http_body_util::Full;
    use tus_protocol::ChecksumAlgorithm;

    let router = build_router();
    let post = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", "5")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(post.status(), StatusCode::CREATED);
    let item = format!(
        "/files/{}",
        upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
    );
    let checksum = tus_protocol::calculate_checksum(ChecksumAlgorithm::Sha1, b"hello");
    let checksum = base64::engine::general_purpose::STANDARD.encode(checksum);
    let mut trailers = axum::http::HeaderMap::new();
    trailers.insert(
        "upload-checksum",
        format!("sha1 {checksum}").parse().unwrap(),
    );
    let body =
        Full::new(Bytes::from_static(b"hello")).with_trailers(async move { Some(Ok(trailers)) });

    let response = send(
        router,
        tus_request(Method::PATCH, &item)
            .header("content-type", "application/offset+octet-stream")
            .header("upload-offset", "0")
            .header("content-length", "5")
            .body(Body::new(body))
            .unwrap(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers().get("upload-offset").unwrap(), "5");
}

// ---------------------------------------------------------------------------
// Upload-Expires is set on POST when the Expiration extension is configured
// ---------------------------------------------------------------------------

#[tokio::test]
async fn post_emits_upload_expires_header_when_configured() {
    let config = default_config().with_expiration(std::time::Duration::from_secs(3600));
    let router = build_router_with(config);

    let response = send(
        router,
        tus_request(Method::POST, "/files")
            .header("upload-length", "10")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let expires = response
        .headers()
        .get("upload-expires")
        .expect("upload-expires must be advertised when expiration is configured")
        .to_str()
        .unwrap();
    // RFC 7231 IMF-fixdate ends in " GMT".
    assert!(
        expires.ends_with(" GMT"),
        "upload-expires should be RFC 7231 format, got {:?}",
        expires
    );
}

// ---------------------------------------------------------------------------
// OPTIONS advertises Tus-Checksum-Algorithm when the Checksum extension is on
// ---------------------------------------------------------------------------

#[tokio::test]
async fn options_advertises_tus_checksum_algorithm() {
    // default_config() uses all_extensions(), which enables Checksum
    // and populates sha1/sha256/md5.
    let router = build_router();
    let response = send(
        router,
        Request::builder()
            .method(Method::OPTIONS)
            .uri("/files")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let algs = response
        .headers()
        .get("tus-checksum-algorithm")
        .expect("tus-checksum-algorithm must be advertised when checksum is enabled")
        .to_str()
        .unwrap();
    assert!(algs.contains("sha1"), "got {:?}", algs);
    assert!(algs.contains("sha256"), "got {:?}", algs);
}

// ---------------------------------------------------------------------------
// HEAD responses set Cache-Control: no-store
// ---------------------------------------------------------------------------

#[tokio::test]
async fn head_sets_cache_control_no_store() {
    let router = build_router();

    let post = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", "10")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let item = format!(
        "/files/{}",
        upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
    );

    let head = send(
        router,
        tus_request(Method::HEAD, &item)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()
            .get("cache-control")
            .expect("cache-control must be set on HEAD")
            .to_str()
            .unwrap(),
        "no-store"
    );
}

// ---------------------------------------------------------------------------
// X-HTTP-Method-Override: POST → PATCH for clients behind restrictive proxies
// ---------------------------------------------------------------------------

#[tokio::test]
async fn x_http_method_override_rewrites_post_to_patch() {
    let router = build_router();

    let post = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", "5")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let item = format!(
        "/files/{}",
        upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
    );

    // POST with X-HTTP-Method-Override: PATCH should be treated as a PATCH.
    let response = send(
        router,
        tus_request(Method::POST, &item)
            .header("x-http-method-override", "PATCH")
            .header("content-type", "application/offset+octet-stream")
            .header("upload-offset", "0")
            .header("content-length", "5")
            .body(Body::from(&b"hello"[..]))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers().get("upload-offset").unwrap(), "5");
}

#[tokio::test]
async fn x_http_method_override_rewrites_post_to_delete() {
    let router = build_router();

    let post = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", "5")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let item = format!(
        "/files/{}",
        upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
    );

    let response = send(
        router.clone(),
        tus_request(Method::POST, &item)
            .header("x-http-method-override", "DELETE")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = send(
        router,
        tus_request(Method::HEAD, &item)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Metadata edge cases: empty-value pair and invalid-key rejection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn post_accepts_metadata_pair_with_empty_value() {
    // Per the TUS spec, `key` (with no value) is a valid pair meaning
    // "the value is empty."
    let router = build_router();

    let filename_b64 = base64::engine::general_purpose::STANDARD.encode("test.txt");
    // Two pairs: "filename <b64>" and "is_public" (no value → empty).
    let metadata = format!("filename {},is_public", filename_b64);

    let post = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", "10")
            .header("upload-metadata", &metadata)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(post.status(), StatusCode::CREATED);

    let item = format!(
        "/files/{}",
        upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
    );

    let head = send(
        router,
        tus_request(Method::HEAD, &item)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let returned = head
        .headers()
        .get("upload-metadata")
        .expect("metadata header present")
        .to_str()
        .unwrap();
    assert!(returned.contains("filename"), "got {:?}", returned);
    assert!(returned.contains("is_public"), "got {:?}", returned);
}

#[tokio::test]
async fn post_rejects_metadata_with_invalid_base64_value() {
    let router = build_router();
    let response = send(
        router,
        tus_request(Method::POST, "/files")
            .header("upload-length", "10")
            // "filename" with non-base64 junk as the value.
            .header("upload-metadata", "filename !!!not-base64!!!")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Hook rejection
// ---------------------------------------------------------------------------

/// HookExecutor whose pre-hook always rejects, with caller-supplied status
/// and message. Used to prove the protocol surfaces hook rejections as
/// `Error::HookRejected` and that the rejection's status code reaches
/// the client untouched.
struct RejectingHookExecutor {
    status: u16,
    message: String,
}

#[async_trait]
impl HookExecutor for RejectingHookExecutor {
    async fn execute_pre(&self, _ctx: &HookContext) -> TusResult<PreHookResult> {
        Ok(PreHookResult::reject(self.status, self.message.clone()))
    }
    async fn execute_post(&self, _ctx: &HookContext) {}
}

fn build_router_with_rejecting_hooks(status: u16, message: &str) -> Router {
    let state = TusState::new(ProtocolHandle::new(
        default_config(),
        MemoryStorage::new(),
        MemoryStateStore::new(),
        MemoryLocker::new(),
        RejectingHookExecutor {
            status,
            message: message.to_string(),
        },
    ));
    create_router(state, &RouterOptions::default()).unwrap()
}

#[tokio::test]
async fn pre_hook_rejection_blocks_post_with_supplied_status_and_message() {
    let router = build_router_with_rejecting_hooks(403, "denied by policy");

    let response = send(
        router,
        tus_request(Method::POST, "/files")
            .header("upload-length", "10")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        response.headers().get("tus-resumable").unwrap(),
        TUS_RESUMABLE,
    );
    let body = String::from_utf8(read_body(response).await).unwrap();
    assert!(
        body.contains("denied by policy"),
        "rejection message must reach the client (got: {body:?})",
    );
}

#[tokio::test]
async fn pre_hook_rejection_uses_non_standard_status_codes() {
    // 451 Unavailable For Legal Reasons -- exotic but valid. Proves the
    // status code passes through as-is rather than getting clamped.
    let router = build_router_with_rejecting_hooks(451, "takedown notice");

    let response = send(
        router,
        tus_request(Method::POST, "/files")
            .header("upload-length", "10")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(response.status().as_u16(), 451);
}
