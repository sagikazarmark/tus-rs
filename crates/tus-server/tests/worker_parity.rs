//! Native-vs-Worker response parity harness.
//!
//! The native `tus-server` and `tus-worker` are held to the same
//! `tus-compliance-tests` suite, but that is a pass/fail gate; it
//! does not tell us whether the two implementations produce
//! byte-identical responses for the protocol-relevant headers. This
//! test drives a fixture of success and error requests at both and
//! diffs the response bits that clients actually consume.
//!
//! It is opt-in: the test skips unless both
//! `TUS_PARITY_NATIVE_URL` and `TUS_PARITY_WORKER_URL` are set in
//! the environment. Each variable must point at the base URL that
//! already includes the TUS base path, e.g.
//! `http://127.0.0.1:8080/files` for native and
//! `http://127.0.0.1:8787/files` for Worker. Running `cargo test`
//! without those variables is a no-op, so the test is safe to keep
//! in the default run. Set `TUS_PARITY_REQUIRED=1` in CI/Dagger so a
//! missing URL fails closed instead of silently skipping the gate.
//!
//! The headers diffed are the ones the TUS protocol defines as
//! response-affecting: `Tus-Resumable`, `Upload-Offset`,
//! `Upload-Length`, `Upload-Metadata`, `Upload-Concat`,
//! `Upload-Defer-Length`, `Upload-Expires`, and `Location` (normalized
//! to the presence of an upload URL). Implementation-specific headers
//! like `Server`, `Date`, or `x-amz-*` are ignored.

use std::collections::BTreeMap;
use std::env;

use reqwest::{Client, Method, Response, StatusCode, Url};

const HEADERS_TO_DIFF: &[&str] = &[
    "tus-resumable",
    "upload-offset",
    "upload-length",
    "upload-metadata",
    "upload-concat",
    "upload-defer-length",
    "upload-expires",
    "tus-version",
    "tus-extension",
    "tus-checksum-algorithm",
    "tus-max-size",
    "content-range",
];

#[derive(Debug, Clone)]
struct Recorded {
    status: StatusCode,
    headers: BTreeMap<String, String>,
    upload_id: Option<String>,
}

fn extract_upload_id(location: Option<&str>) -> Option<String> {
    location
        .and_then(|value| value.rsplit('/').next())
        .map(str::to_string)
        .filter(|id| !id.is_empty())
}

async fn record(response: Response) -> Recorded {
    let status = response.status();
    let mut headers = BTreeMap::new();
    let location = response
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok());
    for header in HEADERS_TO_DIFF {
        if let Some(value) = response.headers().get(*header) {
            headers.insert(
                header.to_string(),
                value.to_str().unwrap_or_default().to_string(),
            );
        }
    }
    let upload_id = extract_upload_id(location);
    if location.is_some() {
        headers.insert(
            "location".to_string(),
            if upload_id.is_some() {
                "<upload-url>"
            } else {
                "<invalid-location>"
            }
            .to_string(),
        );
    }
    Recorded {
        status,
        headers,
        upload_id,
    }
}

async fn run_fixture(client: &Client, base: &str) -> Vec<(String, Recorded)> {
    let mut records = Vec::new();

    // 1. OPTIONS must reflect the implementation's advertised
    //    protocol surface (Tus-Version / Tus-Extension / Tus-Max-Size).
    let response = client
        .request(Method::OPTIONS, base)
        .send()
        .await
        .expect("OPTIONS must complete");
    let options = record(response).await;
    records.push(("options".to_string(), options));

    // 2. Error responses are part of client-observable protocol parity too.
    let missing_tus = client
        .post(base)
        .header("upload-length", "1")
        .send()
        .await
        .expect("POST without Tus-Resumable must complete");
    records.push((
        "missing_tus_resumable".to_string(),
        record(missing_tus).await,
    ));

    let unsupported_tus = client
        .post(base)
        .header("tus-resumable", "0.2.2")
        .header("upload-length", "1")
        .send()
        .await
        .expect("POST with unsupported Tus-Resumable must complete");
    records.push((
        "unsupported_tus_resumable".to_string(),
        record(unsupported_tus).await,
    ));

    // 3. Deferred length + metadata should echo consistently via HEAD.
    let deferred_body = b"deferred-parity";
    let create_deferred = client
        .post(base)
        .header("tus-resumable", "1.0.0")
        .header("upload-defer-length", "1")
        .header("upload-metadata", "filename ZGVmZXJyZWQudHh0")
        .send()
        .await
        .expect("deferred POST must complete");
    let create_deferred = record(create_deferred).await;
    let deferred_id = create_deferred
        .upload_id
        .clone()
        .expect("deferred POST must return a location with an id");
    records.push(("create_deferred".to_string(), create_deferred));

    let deferred_url = format!("{base}/{deferred_id}");
    let head_deferred = client
        .request(Method::HEAD, &deferred_url)
        .header("tus-resumable", "1.0.0")
        .send()
        .await
        .expect("HEAD on deferred upload must complete");
    records.push(("head_deferred".to_string(), record(head_deferred).await));

    let patch_deferred = client
        .patch(&deferred_url)
        .header("tus-resumable", "1.0.0")
        .header("upload-offset", "0")
        .header("upload-length", deferred_body.len())
        .header("content-type", "application/offset+octet-stream")
        .body(deferred_body.to_vec())
        .send()
        .await
        .expect("PATCH with deferred Upload-Length must complete");
    records.push(("patch_deferred".to_string(), record(patch_deferred).await));

    let head_deferred_full = client
        .request(Method::HEAD, &deferred_url)
        .header("tus-resumable", "1.0.0")
        .send()
        .await
        .expect("HEAD on full deferred upload must complete");
    records.push((
        "head_deferred_full".to_string(),
        record(head_deferred_full).await,
    ));

    // 4. POST to create a fixed-length upload used by normal and error paths.
    let body = b"parity-probe";
    let create = client
        .post(base)
        .header("tus-resumable", "1.0.0")
        .header("upload-length", body.len())
        .send()
        .await
        .expect("POST must complete");
    let create = record(create).await;
    let upload_id = create
        .upload_id
        .clone()
        .expect("POST must return a location with an id");
    records.push(("create".to_string(), create));

    let upload_url = format!("{base}/{upload_id}");

    let offset_mismatch = client
        .patch(&upload_url)
        .header("tus-resumable", "1.0.0")
        .header("upload-offset", "1")
        .header("content-type", "application/offset+octet-stream")
        .send()
        .await
        .expect("offset mismatch PATCH must complete");
    records.push(("offset_mismatch".to_string(), record(offset_mismatch).await));

    let unsupported_checksum = client
        .patch(&upload_url)
        .header("tus-resumable", "1.0.0")
        .header("upload-offset", "0")
        .header("upload-checksum", "crc32 AAAAAA==")
        .header("content-type", "application/offset+octet-stream")
        .body(body.to_vec())
        .send()
        .await
        .expect("unsupported checksum PATCH must complete");
    records.push((
        "unsupported_checksum".to_string(),
        record(unsupported_checksum).await,
    ));

    // 5. HEAD on the empty upload.
    let head_empty = client
        .request(Method::HEAD, &upload_url)
        .header("tus-resumable", "1.0.0")
        .send()
        .await
        .expect("HEAD must complete");
    records.push(("head_empty".to_string(), record(head_empty).await));

    // 6. PATCH writes the full body.
    let patch = client
        .patch(&upload_url)
        .header("tus-resumable", "1.0.0")
        .header("upload-offset", "0")
        .header("content-type", "application/offset+octet-stream")
        .body(body.to_vec())
        .send()
        .await
        .expect("PATCH must complete");
    records.push(("patch".to_string(), record(patch).await));

    // 7. HEAD on the full upload.
    let head_full = client
        .request(Method::HEAD, &upload_url)
        .header("tus-resumable", "1.0.0")
        .send()
        .await
        .expect("HEAD must complete");
    records.push(("head_full".to_string(), record(head_full).await));

    let completed_mutation = client
        .patch(&upload_url)
        .header("tus-resumable", "1.0.0")
        .header("upload-offset", body.len())
        .header("content-type", "application/offset+octet-stream")
        .body(b"x".to_vec())
        .send()
        .await
        .expect("completed upload mutation PATCH must complete");
    records.push((
        "completed_mutation".to_string(),
        record(completed_mutation).await,
    ));

    // 8. DELETE cleans up; HEAD after should 404.
    let delete = client
        .delete(&upload_url)
        .header("tus-resumable", "1.0.0")
        .send()
        .await
        .expect("DELETE must complete");
    records.push(("delete".to_string(), record(delete).await));

    let head_gone = client
        .request(Method::HEAD, &upload_url)
        .header("tus-resumable", "1.0.0")
        .send()
        .await
        .expect("HEAD must complete");
    records.push(("head_gone".to_string(), record(head_gone).await));

    records
}

fn truthy_env(name: &str) -> bool {
    matches!(
        env::var(name)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes"
    )
}

fn parity_required() -> bool {
    truthy_env("TUS_PARITY_REQUIRED")
}

fn worker_smoke_required() -> bool {
    truthy_env("TUS_WORKER_SMOKE_REQUIRED")
}

fn parity_url(name: &str) -> Option<String> {
    match env::var(name) {
        Ok(value) => Some(value),
        Err(_) if parity_required() => panic!("{name} must be set when TUS_PARITY_REQUIRED=1"),
        Err(_) => None,
    }
}

fn worker_smoke_url() -> Option<String> {
    match env::var("TUS_WORKER_SMOKE_URL") {
        Ok(value) => Some(value),
        Err(_) if worker_smoke_required() => {
            panic!("TUS_WORKER_SMOKE_URL must be set when TUS_WORKER_SMOKE_REQUIRED=1")
        }
        Err(_) => None,
    }
}

fn worker_smoke_iterations() -> usize {
    env::var("TUS_WORKER_SMOKE_ITERATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(10)
}

fn header_value(response: &Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn resolve_location(base_url: &str, location: &str) -> String {
    if Url::parse(location).is_ok() {
        return location.to_string();
    }

    let base = format!("{}/", base_url.trim_end_matches('/'));
    Url::parse(&base)
        .expect("TUS_WORKER_SMOKE_URL must be an absolute URL")
        .join(location)
        .expect("Location header must resolve against TUS_WORKER_SMOKE_URL")
        .to_string()
}

fn diff(label: &str, native: &Recorded, worker: &Recorded) -> Vec<String> {
    let mut diffs = Vec::new();

    if native.status != worker.status {
        diffs.push(format!(
            "[{label}] status: native={} worker={}",
            native.status, worker.status
        ));
    }

    let mut keys: Vec<&String> = native.headers.keys().chain(worker.headers.keys()).collect();
    keys.sort();
    keys.dedup();

    for key in keys {
        let nv = native.headers.get(key);
        let wv = worker.headers.get(key);
        if nv != wv {
            diffs.push(format!(
                "[{label}] header {key}: native={:?} worker={:?}",
                nv, wv
            ));
        }
    }

    diffs
}

#[tokio::test]
async fn native_and_worker_agree_on_protocol_responses() {
    let native_url = match parity_url("TUS_PARITY_NATIVE_URL") {
        Some(value) => value,
        None => {
            eprintln!(
                "skipping parity test: set TUS_PARITY_NATIVE_URL and TUS_PARITY_WORKER_URL to run"
            );
            return;
        }
    };
    let worker_url = match parity_url("TUS_PARITY_WORKER_URL") {
        Some(value) => value,
        None => {
            eprintln!("skipping parity test: TUS_PARITY_WORKER_URL not set");
            return;
        }
    };

    let client = Client::new();

    let native = run_fixture(&client, native_url.trim_end_matches('/')).await;
    let worker = run_fixture(&client, worker_url.trim_end_matches('/')).await;

    assert_eq!(
        native.len(),
        worker.len(),
        "fixture length must match between implementations"
    );

    let mut diffs = Vec::new();
    for (native_entry, worker_entry) in native.iter().zip(worker.iter()) {
        assert_eq!(
            native_entry.0, worker_entry.0,
            "fixture step labels must align"
        );
        diffs.extend(diff(&native_entry.0, &native_entry.1, &worker_entry.1));
    }

    assert!(
        diffs.is_empty(),
        "native/worker parity diverged on {} point(s):\n  - {}",
        diffs.len(),
        diffs.join("\n  - "),
    );
}

#[tokio::test]
async fn worker_state_is_immediately_consistent_after_create_and_patch() {
    let base_url = match worker_smoke_url() {
        Some(value) => value.trim_end_matches('/').to_string(),
        None => {
            eprintln!(
                "skipping Worker state smoke: set TUS_WORKER_SMOKE_URL to a deployed Worker /files URL"
            );
            return;
        }
    };

    let client = Client::new();
    let body = b"worker-state-smoke";
    let expected_len = body.len().to_string();

    for iteration in 1..=worker_smoke_iterations() {
        let create = client
            .post(&base_url)
            .header("tus-resumable", "1.0.0")
            .header("upload-length", body.len())
            .send()
            .await
            .expect("Worker smoke POST must complete");

        assert_eq!(
            create.status(),
            StatusCode::CREATED,
            "iteration {iteration}: POST should create an upload"
        );

        let location = header_value(&create, "location")
            .expect("iteration {iteration}: POST must return Location");
        let upload_url = resolve_location(&base_url, &location);

        let head_empty = client
            .request(Method::HEAD, &upload_url)
            .header("tus-resumable", "1.0.0")
            .send()
            .await
            .expect("Worker smoke immediate HEAD after POST must complete");

        assert_eq!(
            head_empty.status(),
            StatusCode::OK,
            "iteration {iteration}: immediate HEAD after POST should see upload state"
        );
        assert_eq!(
            header_value(&head_empty, "upload-offset").as_deref(),
            Some("0"),
            "iteration {iteration}: new upload offset should be immediately visible"
        );

        let patch = client
            .patch(&upload_url)
            .header("tus-resumable", "1.0.0")
            .header("upload-offset", "0")
            .header("content-type", "application/offset+octet-stream")
            .body(body.to_vec())
            .send()
            .await
            .expect("Worker smoke immediate PATCH must complete");

        assert_eq!(
            patch.status(),
            StatusCode::NO_CONTENT,
            "iteration {iteration}: immediate PATCH after POST should append data"
        );
        assert_eq!(
            header_value(&patch, "upload-offset").as_deref(),
            Some(expected_len.as_str()),
            "iteration {iteration}: PATCH response should expose the new offset"
        );

        let head_full = client
            .request(Method::HEAD, &upload_url)
            .header("tus-resumable", "1.0.0")
            .send()
            .await
            .expect("Worker smoke immediate HEAD after PATCH must complete");

        assert_eq!(
            head_full.status(),
            StatusCode::OK,
            "iteration {iteration}: immediate HEAD after PATCH should see upload state"
        );
        assert_eq!(
            header_value(&head_full, "upload-offset").as_deref(),
            Some(expected_len.as_str()),
            "iteration {iteration}: PATCH offset should be immediately visible"
        );

        let delete = client
            .delete(&upload_url)
            .header("tus-resumable", "1.0.0")
            .send()
            .await
            .expect("Worker smoke DELETE cleanup must complete");

        assert_eq!(
            delete.status(),
            StatusCode::NO_CONTENT,
            "iteration {iteration}: DELETE cleanup should succeed"
        );
    }
}
