use dioxus_tus::state::{TusUploadState, UploadStatus};

#[test]
fn default_state_is_idle() {
    let s = TusUploadState::default();
    assert!(s.is_idle());
    assert!(!s.is_uploading());
    assert!(s.progress_fraction().is_none());
}

#[test]
fn progress_fraction_zero_when_no_bytes_uploaded() {
    let s = TusUploadState {
        status: UploadStatus::Uploading,
        bytes_uploaded: 0,
        bytes_total: Some(100),
        ..Default::default()
    };
    assert_eq!(s.progress_fraction(), Some(0.0));
}

#[test]
fn progress_fraction_half() {
    let s = TusUploadState {
        status: UploadStatus::Uploading,
        bytes_uploaded: 50,
        bytes_total: Some(100),
        ..Default::default()
    };
    assert_eq!(s.progress_fraction(), Some(0.5));
}

#[test]
fn progress_fraction_complete() {
    let s = TusUploadState {
        status: UploadStatus::Complete,
        bytes_uploaded: 100,
        bytes_total: Some(100),
        ..Default::default()
    };
    assert_eq!(s.progress_fraction(), Some(1.0));
}

#[test]
fn progress_fraction_none_when_total_unknown() {
    let s = TusUploadState {
        status: UploadStatus::Uploading,
        bytes_uploaded: 50,
        bytes_total: None,
        ..Default::default()
    };
    assert!(s.progress_fraction().is_none());
}

#[test]
fn progress_fraction_one_for_zero_size_file() {
    let s = TusUploadState {
        status: UploadStatus::Complete,
        bytes_uploaded: 0,
        bytes_total: Some(0),
        ..Default::default()
    };
    assert_eq!(s.progress_fraction(), Some(1.0));
}

use dioxus_tus::config::{TusConfig, TusStartOptions};

#[test]
fn start_options_token_overrides_config_token() {
    let config = TusConfig::new("http://example.test/files").with_bearer_token("config-token");
    let mut options = TusStartOptions::default();
    options.bearer_token_override = Some("upload-token".into());
    let resolved = options
        .bearer_token_override
        .as_deref()
        .or(config.bearer_token.as_deref());
    assert_eq!(resolved, Some("upload-token"));
}

#[test]
fn config_token_used_when_start_options_token_is_none() {
    let config = TusConfig::new("http://example.test/files").with_bearer_token("config-token");
    let options = TusStartOptions::default();
    let resolved = options
        .bearer_token_override
        .as_deref()
        .or(config.bearer_token.as_deref());
    assert_eq!(resolved, Some("config-token"));
}

#[test]
fn no_token_when_both_are_none() {
    let config = TusConfig::new("http://example.test/files");
    let options = TusStartOptions::default();
    let resolved = options
        .bearer_token_override
        .as_deref()
        .or(config.bearer_token.as_deref());
    assert!(resolved.is_none());
}

#[test]
fn start_options_identifies_request_specific_headers() {
    let mut options = TusStartOptions::default();
    options.bearer_token_override = Some("upload-token".into());
    assert!(options.has_request_specific_headers());

    options.bearer_token_override = None;
    options
        .extra_headers
        .push(("X-Tenant-Id".into(), "tenant-a".into()));
    assert!(options.has_request_specific_headers());
}

#[test]
fn start_options_without_per_upload_auth_is_options_cacheable() {
    let options = TusStartOptions::default();
    assert!(!options.has_request_specific_headers());
}

#[test]
fn metadata_auto_populates_filename_and_filetype() {
    let opts = TusStartOptions::default();
    let meta = opts.build_metadata("photo.jpg", "image/jpeg");
    assert_eq!(meta.get("filename").map(String::as_str), Some("photo.jpg"));
    assert_eq!(meta.get("filetype").map(String::as_str), Some("image/jpeg"));
}

#[test]
fn filename_override_replaces_auto_populated() {
    let mut opts = TusStartOptions::default();
    opts.filename_override = Some("renamed.jpg".into());
    let meta = opts.build_metadata("original.jpg", "image/jpeg");
    assert_eq!(
        meta.get("filename").map(String::as_str),
        Some("renamed.jpg")
    );
}

#[test]
fn extra_metadata_is_merged() {
    let mut opts = TusStartOptions::default();
    opts.extra_metadata.insert("user_id".into(), "u123".into());
    let meta = opts.build_metadata("file.bin", "application/octet-stream");
    assert_eq!(meta.get("user_id").map(String::as_str), Some("u123"));
    assert!(meta.contains_key("filename"));
}

#[test]
fn extra_metadata_key_wins_over_auto_populated() {
    let mut opts = TusStartOptions::default();
    opts.extra_metadata
        .insert("filename".into(), "from-extra.txt".into());
    let meta = opts.build_metadata("auto.txt", "text/plain");
    assert_eq!(
        meta.get("filename").map(String::as_str),
        Some("from-extra.txt")
    );
}

// Mock transport, helpers, and the client-driven integration/error-mapping
// tests are native-only — `tus_client::Client::with_transport` and
// `tokio::test` don't make sense under wasm32. Gating the helpers too keeps
// wasm clippy from flagging them as dead code when the dependent tests are
// gated out.
//
// The new `tus_client::Error` variants are all `#[non_exhaustive]` and cannot
// be constructed from outside the crate, so the error-mapping and
// retry-classification tests that used to build `ClientError` variants
// directly now drive a `MockTransport` through the client to produce genuine
// `tus_client::Error`s and then assert on the mapped `TusError` /
// `is_retryable_error` result.
#[cfg(not(target_arch = "wasm32"))]
mod native_client {
    use async_trait::async_trait;
    use dioxus_tus::TusError;
    use dioxus_tus::retry::is_retryable_error;
    use http::{HeaderMap, HeaderName, HeaderValue};
    use std::collections::VecDeque;
    use tus_client::url::Url;
    use tus_client::{Client, Error, Transport, TransportRequest, TransportResponse};

    #[derive(Clone, Default)]
    struct MockTransport {
        requests: std::sync::Arc<std::sync::Mutex<Vec<TransportRequest>>>,
        responses:
            std::sync::Arc<std::sync::Mutex<VecDeque<tus_client::Result<TransportResponse>>>>,
    }

    impl MockTransport {
        fn push(&self, r: TransportResponse) {
            self.responses.lock().unwrap().push_back(Ok(r));
        }
        fn requests(&self) -> Vec<TransportRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Transport for MockTransport {
        async fn send(&self, req: TransportRequest) -> tus_client::Result<TransportResponse> {
            self.requests.lock().unwrap().push(req);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err(Error::transport("no mock response")))
        }
    }

    fn header_map_with(key: &'static str, val: &str) -> HeaderMap {
        let mut m = HeaderMap::new();
        m.insert(
            HeaderName::from_static(key),
            HeaderValue::from_str(val).unwrap(),
        );
        m
    }

    fn resp(status: u16, headers: http::HeaderMap, body: Vec<u8>) -> TransportResponse {
        let mut r = http::Response::new(body);
        *r.status_mut() = http::StatusCode::from_u16(status).unwrap();
        *r.headers_mut() = headers;
        r
    }

    fn mock_client(transport: MockTransport) -> Client<MockTransport> {
        Client::with_transport(Url::parse("http://test.local/files").unwrap(), transport)
    }

    // =================================================================
    // upload_chunk (formerly patch_chunk) — single-request primitive
    // with no internal retry.
    // =================================================================

    #[tokio::test]
    async fn patch_chunk_updates_offset_and_sends_correct_headers() {
        let transport = MockTransport::default();
        transport.push(resp(204, header_map_with("upload-offset", "1024"), vec![]));

        let client = mock_client(transport.clone());

        let new_offset = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .upload_chunk(0, vec![0u8; 1024])
            .await
            .unwrap();

        assert_eq!(new_offset, 1024);

        let reqs = transport.requests();
        assert_eq!(reqs.len(), 1, "expected exactly one request to be sent");
        let req = &reqs[0];
        assert_eq!(
            req.headers()
                .get("upload-offset")
                .and_then(|v| v.to_str().ok()),
            Some("0")
        );
        assert_eq!(
            req.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/offset+octet-stream")
        );
        assert_eq!(
            req.headers()
                .get("tus-resumable")
                .and_then(|v| v.to_str().ok()),
            Some("1.0.0")
        );
    }

    #[tokio::test]
    async fn patch_chunk_returns_error_on_server_4xx() {
        let transport = MockTransport::default();
        transport.push(resp(403, HeaderMap::new(), b"forbidden".to_vec()));

        let client = mock_client(transport.clone());

        let result = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .upload_chunk(0, vec![1u8; 4])
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn patch_chunk_returns_error_on_5xx() {
        // upload_chunk does not retry internally; a 500 response is an immediate error.
        let transport = MockTransport::default();
        transport.push(resp(500, HeaderMap::new(), b"internal error".to_vec()));

        let client = mock_client(transport.clone());

        let result = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .upload_chunk(0, vec![0u8; 512])
            .await;

        assert!(result.is_err());
        assert_eq!(transport.requests().len(), 1);
    }

    #[tokio::test]
    async fn patch_chunk_returns_error_on_repeated_5xx() {
        // upload_chunk does not retry; even with max_retries set it sends exactly one request.
        let transport = MockTransport::default();
        for _ in 0..3 {
            transport.responses.lock().unwrap().push_back(Ok(resp(
                500,
                HeaderMap::new(),
                b"down".to_vec(),
            )));
        }

        let client = mock_client(transport.clone()).with_max_retries(2);

        let result = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .upload_chunk(0, vec![1u8; 4])
            .await;

        assert!(result.is_err());
        // Only one request is sent; upload_chunk does not retry.
        assert_eq!(transport.requests().len(), 1);
    }

    // =================================================================
    // From<tus_client::Error> for TusError mapping. Pin the error
    // contract so downstream consumers can rely on which variant they
    // get. Errors are produced by driving the mock through the client.
    // =================================================================

    #[tokio::test]
    async fn client_4xx_maps_to_server() {
        let transport = MockTransport::default();
        transport.push(resp(403, HeaderMap::new(), b"forbidden".to_vec()));
        let client = mock_client(transport);

        let err = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .upload_chunk(0, vec![0u8; 4])
            .await
            .unwrap_err();

        match TusError::from(err) {
            TusError::Server { status, body } => {
                assert_eq!(status, 403);
                assert_eq!(body, "forbidden");
            }
            other => panic!("expected Server, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn client_5xx_maps_to_server() {
        let transport = MockTransport::default();
        transport.push(resp(503, HeaderMap::new(), b"down".to_vec()));
        let client = mock_client(transport);

        let err = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .upload_chunk(0, vec![0u8; 4])
            .await
            .unwrap_err();

        matches!(TusError::from(err), TusError::Server { status: 503, .. })
            .then_some(())
            .expect("expected Server { 503, .. }");
    }

    #[tokio::test]
    async fn client_missing_header_maps_to_missing_header() {
        // A 200 HEAD response missing Upload-Offset yields MissingHeader.
        let transport = MockTransport::default();
        transport.push(resp(200, HeaderMap::new(), vec![]));
        let client = mock_client(transport);

        let err = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .info()
            .await
            .unwrap_err();

        match TusError::from(err) {
            TusError::MissingHeader(name) => assert_eq!(name, "Upload-Offset"),
            other => panic!("expected MissingHeader, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn client_invalid_header_maps_to_typed_invalid_header() {
        // Distinct from MissingHeader: header was *present* but malformed.
        // Consumers that branch on the error variant must be able to
        // distinguish "server didn't send it" from "server sent garbage".
        let transport = MockTransport::default();
        transport.push(resp(
            200,
            header_map_with("upload-offset", "not-a-number"),
            vec![],
        ));
        let client = mock_client(transport);

        let err = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .info()
            .await
            .unwrap_err();

        match TusError::from(err) {
            TusError::InvalidHeader { header, value } => {
                assert_eq!(header, "Upload-Offset");
                assert_eq!(value, "not-a-number");
            }
            other => panic!("expected InvalidHeader, got {other:?}"),
        }
    }

    // NOTE: The `invalid_default_header_redacts_*` /
    // `invalid_default_header_keeps_other_values` tests (4) and
    // `client_offset_beyond_source_maps_to_transport` (1) were removed.
    // They constructed `tus_client::Error` variants (`InvalidDefaultHeader`,
    // `OffsetBeyondSource`) directly, which is no longer possible now that
    // every `tus_client::Error` variant is `#[non_exhaustive]` and
    // unconstructable from outside the crate. In addition, the
    // auth-header-redaction path is no longer emitted by the client: it now
    // takes a pre-validated `HeaderMap` and the engine validates bearer
    // tokens itself without ever echoing them back through an error, so there
    // is no `InvalidDefaultHeader` variant for the client to produce.

    // =================================================================
    // Retry classification — pins the predicate the engine consults on
    // every PATCH failure. `dioxus_tus::retry::is_retryable_error` is the
    // same function the chunk loop in src/hook.rs calls.
    // =================================================================

    #[tokio::test]
    async fn retry_classification_5xx_is_retryable() {
        let transport = MockTransport::default();
        transport.push(resp(503, HeaderMap::new(), vec![]));
        let client = mock_client(transport);
        let err = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .upload_chunk(0, vec![0u8; 4])
            .await
            .unwrap_err();
        assert!(is_retryable_error(&err));
    }

    #[tokio::test]
    async fn retry_classification_409_is_retryable() {
        let transport = MockTransport::default();
        transport.push(resp(409, HeaderMap::new(), vec![]));
        let client = mock_client(transport);
        let err = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .upload_chunk(0, vec![0u8; 4])
            .await
            .unwrap_err();
        assert!(is_retryable_error(&err));
    }

    #[tokio::test]
    async fn retry_classification_403_is_not_retryable() {
        let transport = MockTransport::default();
        transport.push(resp(403, HeaderMap::new(), vec![]));
        let client = mock_client(transport);
        let err = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .upload_chunk(0, vec![0u8; 4])
            .await
            .unwrap_err();
        assert!(!is_retryable_error(&err));
    }

    #[tokio::test]
    async fn retry_classification_408_is_retryable() {
        // Request Timeout: proxy / server-side timeout. PATCH is idempotent
        // under TUS, so a single retry is the textbook recovery.
        let transport = MockTransport::default();
        transport.push(resp(408, HeaderMap::new(), vec![]));
        let client = mock_client(transport);
        let err = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .upload_chunk(0, vec![0u8; 4])
            .await
            .unwrap_err();
        assert!(is_retryable_error(&err));
    }

    #[tokio::test]
    async fn retry_classification_429_is_retryable() {
        // Too Many Requests: server-applied rate limiting. Without retry the
        // upload fails permanently on a transient throttle.
        let transport = MockTransport::default();
        transport.push(resp(429, HeaderMap::new(), vec![]));
        let client = mock_client(transport);
        let err = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .upload_chunk(0, vec![0u8; 4])
            .await
            .unwrap_err();
        assert!(is_retryable_error(&err));
    }

    #[tokio::test]
    async fn retry_classification_missing_header_is_not_retryable() {
        let transport = MockTransport::default();
        transport.push(resp(200, HeaderMap::new(), vec![]));
        let client = mock_client(transport);
        let err = client
            .upload_at("http://test.local/files/abc")
            .unwrap()
            .info()
            .await
            .unwrap_err();
        assert!(!is_retryable_error(&err));
    }
} // end mod native_client

// Mirrors the WASM blob_slice_to_bytes logic using plain Vec<u8>.
// The actual WASM implementation is tested in the Layer 2 suite.
fn slice_bytes(data: &[u8], start: u64, end: u64) -> Vec<u8> {
    let start = (start as usize).min(data.len());
    let end = (end as usize).min(data.len());
    data[start..end].to_vec()
}

#[test]
fn blob_slice_returns_correct_chunk() {
    let data = b"0123456789";
    assert_eq!(slice_bytes(data, 0, 4), b"0123");
    assert_eq!(slice_bytes(data, 4, 8), b"4567");
    assert_eq!(slice_bytes(data, 8, 12), b"89"); // clamps to end
}

#[test]
fn blob_slice_empty_at_end() {
    let data = b"hello";
    assert_eq!(slice_bytes(data, 5, 10), b"");
}

#[test]
fn blob_slice_start_past_end_returns_empty() {
    let data = b"hello";
    assert_eq!(slice_bytes(data, 10, 20), b"");
}

// =====================================================================
// From<tus_client::Error> for TusError — transport CORS heuristic. These
// use the externally-constructible `Error::transport(msg)` constructor;
// `TusError::from` inspects the transport error's `source.to_string()`,
// which equals the string passed here, so the per-browser heuristic fires.
// =====================================================================

use dioxus_tus::TusError;

#[test]
fn client_transport_failed_to_fetch_maps_to_cors() {
    let e = tus_client::Error::transport("TypeError: Failed to fetch");
    let mapped: TusError = e.into();
    matches!(mapped, TusError::Cors)
        .then_some(())
        .expect("Failed to fetch should map to Cors");
}

#[test]
fn client_transport_network_error_maps_to_cors() {
    let e = tus_client::Error::transport("NetworkError when attempting to fetch resource");
    let mapped: TusError = e.into();
    matches!(mapped, TusError::Cors)
        .then_some(())
        .expect("NetworkError should map to Cors");
}

/// Safari/WebKit emits `TypeError: Load failed` for the same class of
/// fetch failures Chromium calls "Failed to fetch" and Firefox calls
/// "NetworkError". Without this string in the heuristic, Safari users
/// hitting CORS preflight failures would surface as a generic
/// `TusError::Transport`, breaking any consumer that branches on
/// `TusError::Cors` to show the CORS-help UI.
#[test]
fn client_transport_load_failed_maps_to_cors() {
    let e = tus_client::Error::transport("TypeError: Load failed");
    let mapped: TusError = e.into();
    matches!(mapped, TusError::Cors)
        .then_some(())
        .expect("Safari 'Load failed' should map to Cors");
}

#[test]
fn client_transport_other_string_maps_to_transport() {
    let e = tus_client::Error::transport("connection reset by peer");
    let mapped: TusError = e.into();
    match mapped {
        TusError::Transport(s) => assert!(s.contains("connection reset")),
        other => panic!("expected Transport, got {other:?}"),
    }
}

// =====================================================================
// TusConfig builder ergonomics + clamp behaviour
// =====================================================================

#[test]
fn config_builder_chains_correctly() {
    let c = TusConfig::new("https://tus.example.com/files")
        .with_bearer_token("abc")
        .with_chunk_size(2 * 1024 * 1024)
        .with_max_retries(5)
        .with_retry_delay_ms(500)
        .with_creation_with_upload_threshold(64 * 1024);
    assert_eq!(c.endpoint, "https://tus.example.com/files");
    assert_eq!(c.bearer_token.as_deref(), Some("abc"));
    assert_eq!(c.chunk_size, 2 * 1024 * 1024);
    assert_eq!(c.max_retries, 5);
    assert_eq!(c.retry_delay_ms, 500);
    assert_eq!(c.creation_with_upload_threshold, 64 * 1024);
}

#[test]
fn config_chunk_size_clamps_to_at_least_one() {
    let c = TusConfig::new("https://x.test/files").with_chunk_size(0);
    assert_eq!(c.chunk_size, 1, "chunk_size=0 must be clamped");
}

#[test]
fn config_default_chunk_size_is_one_mib() {
    let c = TusConfig::new("https://x.test/files");
    assert_eq!(c.chunk_size, 1024 * 1024);
}

// =====================================================================
// creation-with-upload predicate — regression tests for the wasm32 32-bit
// `usize` truncation bug. With the fix the predicate compares in `u64`
// space, so files larger than `u32::MAX` no longer alias into the
// "small enough for cwu" range.
// =====================================================================

#[test]
fn cwu_predicate_includes_files_under_threshold() {
    let c = TusConfig::new("https://x.test/files").with_creation_with_upload_threshold(256 * 1024);
    assert!(c.use_creation_with_upload(1));
    assert!(c.use_creation_with_upload(100 * 1024));
    assert!(c.use_creation_with_upload(256 * 1024));
}

#[test]
fn cwu_predicate_excludes_files_over_threshold() {
    let c = TusConfig::new("https://x.test/files").with_creation_with_upload_threshold(256 * 1024);
    assert!(!c.use_creation_with_upload(256 * 1024 + 1));
    assert!(!c.use_creation_with_upload(10 * 1024 * 1024));
}

#[test]
fn cwu_predicate_excludes_zero_size_files() {
    // Empty files don't take the cwu fast path — the create_upload branch
    // handles them and short-circuits the chunk loop.
    let c = TusConfig::new("https://x.test/files").with_creation_with_upload_threshold(256 * 1024);
    assert!(!c.use_creation_with_upload(0));
}

#[test]
fn cwu_predicate_does_not_truncate_huge_files_on_wasm32() {
    // Pre-fix this used `(file_size as usize) <= threshold`. On wasm32 (32-bit
    // usize) a 4 GiB + 100 KiB file truncates to 100 KiB, falsely matches a
    // 256 KiB threshold, and routes the entire >4 GiB payload through the
    // load-whole-body POST — OOMing wasm linear memory. The predicate now
    // compares in u64 space so the truncation can't happen.
    let c = TusConfig::new("https://x.test/files").with_creation_with_upload_threshold(256 * 1024);
    let four_gib_plus_100kib: u64 = (1u64 << 32) + 100 * 1024;
    assert!(
        !c.use_creation_with_upload(four_gib_plus_100kib),
        "huge file whose low 32 bits land below the threshold must NOT \
         take the cwu path",
    );

    // Boundary at exactly 2^32: low 32 bits are 0, which would have aliased
    // to "zero-size" and ALSO failed the `> 0` guard. Make sure the u64
    // comparison gets it right.
    let exactly_four_gib: u64 = 1u64 << 32;
    assert!(!c.use_creation_with_upload(exactly_four_gib));
}

// =====================================================================
// Retry classification — transport errors. Uses the
// externally-constructible `Error::transport` constructor, which yields a
// retryable transport error.
// =====================================================================

use dioxus_tus::retry::is_retryable_error;

#[test]
fn retry_classification_transport_is_retryable() {
    let e = tus_client::Error::transport("connection reset");
    assert!(is_retryable_error(&e));
}
