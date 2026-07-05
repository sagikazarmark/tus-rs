//! Path safety: a malicious `upload_id` from the URL must never
//! cause the server to read or write files outside the configured
//! state and storage directories.
//!
//! The `upload_id` becomes part of:
//!   - the state file path (`FileStateStore::state_path` joins it
//!     into the configured state directory),
//!   - the storage key (the file storage backend treats it as a path
//!     under the upload directory).
//!
//! If a client can craft an id that escapes either, the server is a
//! traversal vulnerability. The router or the path-extractor should
//! reject such ids before they ever reach the storage layer.
//!
//! All tests use tempdirs and assert that whatever the response, no
//! file appears OUTSIDE those tempdirs after the request.

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode},
};
use tempfile::TempDir;
use tower::ServiceExt;
use tus_axum::{RouterOptions, TusState, create_router};
use tus_protocol::locking::file::FileLocker;
use tus_protocol::state::file::FileStateStore;
use tus_protocol::storage::file::FileStorage;
use tus_protocol::{Config, NoopHookExecutor, ProtocolHandle};

const TUS_RESUMABLE: &str = "1.0.0";

struct Sandbox {
    router: Router,
    /// Outer per-test tempdir. The state and upload subdirs and the
    /// sentinels all live inside it, so concurrent tests never share
    /// a path. Held to keep the dir alive for the test's duration.
    _root: TempDir,
    sentinel_outside_state: std::path::PathBuf,
    sentinel_outside_upload: std::path::PathBuf,
}

async fn build_router(root: &std::path::Path) -> axum::Router {
    let storage = FileStorage::new(root.join("uploads")).await.unwrap();
    let state_store = FileStateStore::new(root.join("state")).await.unwrap();
    let locker = FileLocker::new(root.join("locks")).await.unwrap();
    let state = TusState::new(ProtocolHandle::new(
        Config::default().with_base_path("/files"),
        storage,
        state_store,
        locker,
        NoopHookExecutor::new(),
    ));
    create_router(state, &RouterOptions::default()).unwrap()
}

async fn build_sandbox() -> Sandbox {
    // Single per-test root so each test's sentinel paths are unique
    // even with `cargo test`'s default thread-parallel runner.
    let root = TempDir::new().unwrap();

    // Sentinels live in the root, ABOVE state/ and upload/. A
    // traversal from `state/<id>.json` of the form `../sentinel-state.txt`
    // lands on the sentinel — proof that the traversal escaped the
    // configured state dir.
    let sentinel_outside_state = root.path().join("sentinel-state.txt");
    let sentinel_outside_upload = root.path().join("sentinel-upload.bin");
    std::fs::write(&sentinel_outside_state, b"do not touch (state)").unwrap();
    std::fs::write(&sentinel_outside_upload, b"do not touch (upload)").unwrap();

    Sandbox {
        router: build_router(root.path()).await,
        _root: root,
        sentinel_outside_state,
        sentinel_outside_upload,
    }
}

impl Sandbox {
    async fn dispatch(&self, req: Request<Body>) -> (StatusCode, Vec<u8>) {
        let response = self.router.clone().oneshot(req).await.unwrap();
        let status = response.status();
        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        (status, body)
    }

    /// Verify the sandbox is intact: sentinels still readable with
    /// original contents. Sentinels live just outside the configured
    /// state and upload directories, so any `..`-style traversal
    /// would land on or near them. If the sentinels survive, no
    /// traversal write reached outside the sandbox.
    ///
    /// (We don't scan the parent dir for unexpected files — that
    /// turns into a fight with concurrent tests' tempdirs and
    /// system-tempdir noise. The sentinel check is the actual
    /// property: nothing wrote where we said "don't write here".)
    fn assert_no_traversal_writes(&self) {
        let state_sentinel =
            std::fs::read(&self.sentinel_outside_state).expect("state sentinel must still exist");
        assert_eq!(
            state_sentinel, b"do not touch (state)",
            "state sentinel was overwritten — traversal write succeeded"
        );
        let upload_sentinel =
            std::fs::read(&self.sentinel_outside_upload).expect("upload sentinel must still exist");
        assert_eq!(
            upload_sentinel, b"do not touch (upload)",
            "upload sentinel was overwritten — traversal write succeeded"
        );
    }
}

fn req(method: Method, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("tus-resumable", TUS_RESUMABLE)
        .body(Body::empty())
        .unwrap()
}

// ---------------------------------------------------------------------------
// HEAD with malicious ids
// ---------------------------------------------------------------------------

#[tokio::test]
async fn head_with_dotdot_id_does_not_escape() {
    let sb = build_sandbox().await;
    let (status, _) = sb.dispatch(req(Method::HEAD, "/files/..")).await;
    // Either 404 (id doesn't exist) or 400 (invalid id) is acceptable;
    // anything 2xx would mean a literal `..` matched something real.
    assert!(
        status == StatusCode::NOT_FOUND || status.is_client_error(),
        "HEAD /files/.. returned {status}, expected 404 or 4xx"
    );
    sb.assert_no_traversal_writes();
}

#[tokio::test]
async fn head_with_encoded_slash_id_does_not_escape() {
    let sb = build_sandbox().await;
    // %2F is `/`. With %2F decoded inside the segment, the id becomes
    // `../etc/passwd` and must be rejected before it reaches storage.
    let (status, _) = sb
        .dispatch(req(Method::HEAD, "/files/..%2Fetc%2Fpasswd"))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    sb.assert_no_traversal_writes();
}

#[tokio::test]
async fn head_with_backslash_id_does_not_escape() {
    let sb = build_sandbox().await;
    // Windows-flavoured path separator. On Unix this is just a
    // funny character in the id; on Windows the FileStateStore
    // would treat it as a separator. Either way the response
    // should not 5xx.
    let (status, _) = sb
        .dispatch(req(Method::HEAD, "/files/..%5Cetc%5Cpasswd"))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    sb.assert_no_traversal_writes();
}

#[tokio::test]
async fn head_with_nul_byte_id_returns_400() {
    let sb = build_sandbox().await;
    // %00 is NUL. This is rejected at the protocol layer with
    // `Error::InvalidUploadId` -> 400,
    // not propagated to the filesystem layer where it used to
    // become 500. The SECURITY property (no escape) still holds.
    let (status, _) = sb.dispatch(req(Method::HEAD, "/files/foo%00bar")).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "HEAD with NUL-byte id should return 400, got {status}"
    );
    sb.assert_no_traversal_writes();
}

#[tokio::test]
async fn head_with_very_long_id_returns_400() {
    let sb = build_sandbox().await;
    // 10 KB id is past the 256-byte upload-id limit; rejected by
    // upload-id parsing -> 400, not by the filesystem layer's
    // ENAMETOOLONG which used to bubble out as 500.
    let huge_id: String = "A".repeat(10_000);
    let (status, _) = sb
        .dispatch(req(Method::HEAD, &format!("/files/{huge_id}")))
        .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "HEAD with 10KB id should return 400, got {status}"
    );
    sb.assert_no_traversal_writes();
}

#[tokio::test]
async fn head_with_dotjson_id_does_not_collide_with_state_file_format() {
    // The FileStateStore stores state as `<id>.json`. A client supplying
    // an id that ends in `.json` could in principle make the on-disk
    // filename `foo.json.json`, which is harmless. But an id LIKE another
    // upload's filename (e.g., `bar` while there's a real upload `bar`)
    // would collide. This test pins down: an id with `.json` suffix is
    // accepted as a literal id, not interpreted as a filename.
    let sb = build_sandbox().await;
    let (status, _) = sb.dispatch(req(Method::HEAD, "/files/foo.json")).await;
    // Just NotFound (no such upload) is the expected outcome.
    assert_eq!(status, StatusCode::NOT_FOUND);
    sb.assert_no_traversal_writes();
}

// ---------------------------------------------------------------------------
// DELETE with malicious ids — same surface, different verb. DELETE is the
// scariest because a successful traversal would actually unlink a file.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_with_dotdot_id_does_not_unlink_anything() {
    let sb = build_sandbox().await;
    let (status, _) = sb.dispatch(req(Method::DELETE, "/files/..")).await;
    assert!(
        status.is_client_error(),
        "DELETE /files/.. returned {status}"
    );
    sb.assert_no_traversal_writes();
}

#[tokio::test]
async fn delete_with_encoded_slash_id_does_not_unlink_anything() {
    let sb = build_sandbox().await;
    let (status, _) = sb
        .dispatch(req(Method::DELETE, "/files/..%2Fetc%2Fpasswd"))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    sb.assert_no_traversal_writes();
}

// ---------------------------------------------------------------------------
// PATCH (the worst case — a successful traversal here would WRITE)
// ---------------------------------------------------------------------------

fn patch(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::PATCH)
        .uri(uri)
        .header("tus-resumable", TUS_RESUMABLE)
        .header("upload-offset", "0")
        .header("content-type", "application/offset+octet-stream")
        .header("content-length", "5")
        .body(Body::from("hello"))
        .unwrap()
}

#[tokio::test]
async fn patch_with_dotdot_id_does_not_write_anywhere() {
    let sb = build_sandbox().await;
    let (status, _) = sb.dispatch(patch("/files/..")).await;
    assert!(
        status.is_client_error(),
        "PATCH /files/.. returned {status}"
    );
    sb.assert_no_traversal_writes();
}

#[tokio::test]
async fn patch_with_encoded_slash_id_does_not_write_anywhere() {
    let sb = build_sandbox().await;
    let (status, _) = sb.dispatch(patch("/files/..%2Fetc%2Fpasswd")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    sb.assert_no_traversal_writes();
}

// ---------------------------------------------------------------------------
// X-HTTP-Method-Override POST path: a malformed upload id must be rejected the
// same way as the equivalent direct PATCH/DELETE — a tus-compliant 400 that
// still carries the `Tus-Resumable` header — rather than axum's default
// plain-text 400 with no such header.
// ---------------------------------------------------------------------------

/// `%FF` is not a valid UTF-8 percent-escape, so axum's raw `Path<String>`
/// extractor fails to decode the segment. The `TusUploadId` extractor maps
/// that failure to a `tus_protocol::Error`, which the response layer renders
/// with the `Tus-Resumable` header.
#[tokio::test]
async fn method_override_post_with_malformed_id_returns_400_with_tus_resumable() {
    let sb = build_sandbox().await;

    for override_method in ["PATCH", "DELETE"] {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/files/%FF")
            .header("tus-resumable", TUS_RESUMABLE)
            .header("x-http-method-override", override_method)
            .body(Body::empty())
            .unwrap();

        let response = sb.router.clone().oneshot(request).await.unwrap();

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "method-override POST->{override_method} with malformed id should be 400"
        );
        assert_eq!(
            response
                .headers()
                .get("tus-resumable")
                .expect("Tus-Resumable header must be present, matching direct PATCH/DELETE")
                .to_str()
                .unwrap(),
            TUS_RESUMABLE,
            "method-override POST->{override_method} 400 must carry Tus-Resumable"
        );
    }

    sb.assert_no_traversal_writes();
}

/// The id is validated before the `X-HTTP-Method-Override` header is inspected,
/// so a malformed id on the override POST path is rejected with the same
/// tus-compliant 400 + `Tus-Resumable` even when the override header is absent
/// or unrecognized (the cases that would otherwise fall through to the 405
/// fallback for a *valid* id). This keeps a malformed id a uniform 400 across
/// every route rather than depending on the override value.
#[tokio::test]
async fn method_override_post_with_malformed_id_rejects_before_405_fallback() {
    let sb = build_sandbox().await;

    // No override header, and an unrecognized override value: both would drive
    // the 405 fallback for a well-formed id, but the malformed id short-circuits
    // to 400 first.
    let cases: [&[(&str, &str)]; 2] = [
        &[("tus-resumable", TUS_RESUMABLE)],
        &[
            ("tus-resumable", TUS_RESUMABLE),
            ("x-http-method-override", "BOGUS"),
        ],
    ];

    for headers in cases {
        let mut builder = Request::builder().method(Method::POST).uri("/files/%FF");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let request = builder.body(Body::empty()).unwrap();

        let response = sb.router.clone().oneshot(request).await.unwrap();

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "malformed id must be 400 regardless of the override header ({headers:?})"
        );
        assert_eq!(
            response
                .headers()
                .get("tus-resumable")
                .expect("Tus-Resumable header must be present")
                .to_str()
                .unwrap(),
            TUS_RESUMABLE,
            "malformed-id 400 must carry Tus-Resumable ({headers:?})"
        );
    }

    sb.assert_no_traversal_writes();
}

/// A direct PATCH with the same malformed id is the reference behavior: 400
/// with `Tus-Resumable`. Pinning it here makes the parity with the
/// method-override path above explicit.
#[tokio::test]
async fn direct_patch_with_malformed_id_returns_400_with_tus_resumable() {
    let sb = build_sandbox().await;

    let response = sb
        .router
        .clone()
        .oneshot(patch("/files/%FF"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response
            .headers()
            .get("tus-resumable")
            .expect("Tus-Resumable header must be present")
            .to_str()
            .unwrap(),
        TUS_RESUMABLE,
    );

    sb.assert_no_traversal_writes();
}
