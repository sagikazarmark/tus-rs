use std::{
    collections::HashMap,
    path::PathBuf,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    middleware::{Next, from_fn_with_state},
    response::Response,
};
use tokio::net::TcpListener;
use tus_axum::TusState;
use tus_protocol::{
    Config, NoopHookExecutor, ProtocolHandle, UploadMetadata, locking::memory::MemoryLocker,
    state::memory::MemoryStateStore, storage::memory::MemoryStorage,
};
use tus_uploader::{Client, NewUpload};

fn tus_bin() -> &'static str {
    env!("CARGO_BIN_EXE_tus")
}

async fn spawn_server(config: Config) -> (String, tokio::task::JoinHandle<()>) {
    spawn_server_with_bearer(config, None).await
}

#[derive(Clone, Debug, Default)]
struct PatchRequestLog {
    requests: Arc<Mutex<Vec<PatchRequest>>>,
}

impl PatchRequestLog {
    fn push(&self, request: PatchRequest) {
        self.requests.lock().unwrap().push(request);
    }

    fn requests(&self) -> Vec<PatchRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PatchRequest {
    offset: u64,
    length: u64,
}

#[derive(Clone, Debug)]
struct HeadRewrite {
    path: PathBuf,
    contents: Vec<u8>,
    rewritten: Arc<AtomicBool>,
}

impl PatchRequest {
    fn from_headers(headers: &HeaderMap) -> Result<Self, StatusCode> {
        let offset = headers
            .get("upload-offset")
            .ok_or(StatusCode::BAD_REQUEST)?
            .to_str()
            .map_err(|_| StatusCode::BAD_REQUEST)?
            .parse()
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        let length = headers
            .get(header::CONTENT_LENGTH)
            .ok_or(StatusCode::BAD_REQUEST)?
            .to_str()
            .map_err(|_| StatusCode::BAD_REQUEST)?
            .parse()
            .map_err(|_| StatusCode::BAD_REQUEST)?;

        Ok(Self { offset, length })
    }
}

fn four_byte_patch_requests() -> Vec<PatchRequest> {
    vec![
        PatchRequest {
            offset: 0,
            length: 4,
        },
        PatchRequest {
            offset: 4,
            length: 4,
        },
        PatchRequest {
            offset: 8,
            length: 2,
        },
    ]
}

fn endpoint_url(endpoint: &str) -> reqwest::Url {
    reqwest::Url::parse(endpoint).unwrap()
}

fn parse_upload_url(upload_url: &str) -> reqwest::Url {
    reqwest::Url::parse(upload_url).unwrap()
}

fn upload_id(upload_url: &str) -> String {
    parse_upload_url(upload_url)
        .path_segments()
        .unwrap()
        .next_back()
        .unwrap()
        .to_string()
}

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8(output.stderr.clone()).unwrap()
}

fn completed_upload_url(output: &std::process::Output) -> String {
    stderr(output)
        .lines()
        .find_map(|line| line.strip_prefix("Upload complete: "))
        .expect("stderr should include completed upload URL")
        .to_string()
}

fn created_upload_url(output: &std::process::Output) -> String {
    let stdout = stdout(output);
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1, "{stdout}");
    lines[0].to_string()
}

fn assert_upload_human_output(output: &std::process::Output, upload_url: &str) {
    assert_eq!(stdout(output), "");
    assert_eq!(
        stderr(output),
        format!("Upload created: {upload_url}\nUpload complete: {upload_url}\n")
    );
}

fn assert_existing_upload_human_output(output: &std::process::Output, upload_url: &str) {
    assert_eq!(stdout(output), "");
    assert_eq!(
        stderr(output),
        format!("Uploading to {upload_url}\nUpload complete: {upload_url}\n")
    );
}

async fn bearer_auth(
    State(token): State<String>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    if presented == Some(token.as_str()) {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

async fn record_patch_request(
    State(log): State<PatchRequestLog>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if req.method() == Method::PATCH {
        log.push(PatchRequest::from_headers(req.headers())?);
    }

    Ok(next.run(req).await)
}

async fn rewrite_file_on_head(
    State(rewrite): State<HeadRewrite>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if req.method() == Method::HEAD && !rewrite.rewritten.swap(true, Ordering::SeqCst) {
        tokio::fs::write(&rewrite.path, &rewrite.contents)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    Ok(next.run(req).await)
}

async fn spawn_recording_server(
    config: Config,
) -> (String, tokio::task::JoinHandle<()>, PatchRequestLog) {
    let state = TusState::new(ProtocolHandle::new(
        config,
        MemoryStorage::new(),
        MemoryStateStore::new(),
        MemoryLocker::new(),
        NoopHookExecutor::new(),
    ));
    let patch_requests = PatchRequestLog::default();
    let app: Router = tus_axum::create_router(state, tus_axum::RouterOptions::default())
        .unwrap()
        .layer(from_fn_with_state(
            patch_requests.clone(),
            record_patch_request,
        ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/files"), handle, patch_requests)
}

async fn spawn_head_rewrite_server(
    config: Config,
    rewrite: HeadRewrite,
) -> (String, tokio::task::JoinHandle<()>) {
    let state = TusState::new(ProtocolHandle::new(
        config,
        MemoryStorage::new(),
        MemoryStateStore::new(),
        MemoryLocker::new(),
        NoopHookExecutor::new(),
    ));
    let app: Router =
        tus_axum::create_router_with_download(state, tus_axum::RouterOptions::default())
            .unwrap()
            .layer(from_fn_with_state(rewrite, rewrite_file_on_head));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/files"), handle)
}

async fn spawn_server_with_bearer(
    config: Config,
    bearer_token: Option<&str>,
) -> (String, tokio::task::JoinHandle<()>) {
    let state = TusState::new(ProtocolHandle::new(
        config,
        MemoryStorage::new(),
        MemoryStateStore::new(),
        MemoryLocker::new(),
        NoopHookExecutor::new(),
    ));
    let app: Router = tus_axum::create_router(state, tus_axum::RouterOptions::default()).unwrap();
    let app = match bearer_token {
        Some(token) => app.layer(from_fn_with_state(token.to_string(), bearer_auth)),
        None => app,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/files"), handle)
}

async fn run_cli(args: &[&str]) -> std::process::Output {
    run_cli_with_env(args, &[]).await
}

async fn run_cli_with_env(args: &[&str], env: &[(&str, &str)]) -> std::process::Output {
    let args = args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>();
    let env = env
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect::<Vec<_>>();
    tokio::task::spawn_blocking(move || {
        let mut command = Command::new(tus_bin());
        command.args(&args);
        for key in [
            "TUS_CONFIG",
            "TUS_ENDPOINT",
            "TUS_BEARER_TOKEN",
            "TUS_CHUNK_SIZE",
        ] {
            command.env_remove(key);
        }
        for (key, value) in env {
            command.env(key, value);
        }
        command.output().unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn upload_prints_created_upload_url_and_metadata() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("upload.txt");
    tokio::fs::write(&path, b"hello").await.unwrap();

    let output = run_cli(&[
        "--endpoint",
        &endpoint,
        "upload",
        path.to_str().unwrap(),
        "--metadata",
        "filename=upload.txt",
    ])
    .await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let upload_url = completed_upload_url(&output);
    assert_upload_human_output(&output, &upload_url);
    assert!(upload_url.starts_with(&endpoint));

    let client = Client::new(endpoint_url(&endpoint));
    let info = client.upload_at(&upload_url).unwrap().info().await.unwrap();
    assert_eq!(info.offset(), 5);
    assert_eq!(info.length(), Some(5));
    assert_eq!(
        info.metadata().get("filename").unwrap().to_string_lossy(),
        "upload.txt"
    );

    handle.abort();
}

#[tokio::test]
async fn upload_url_output_prints_only_created_upload_url() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("url-output-upload.txt");
    tokio::fs::write(&path, b"hello").await.unwrap();

    let output = run_cli(&[
        "--endpoint",
        &endpoint,
        "upload",
        "-o",
        "url",
        path.to_str().unwrap(),
    ])
    .await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(stderr(&output), "");
    let stdout = stdout(&output);
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1, "{stdout}");
    let upload_url = lines[0];
    assert!(upload_url.starts_with(&endpoint));
    assert_eq!(stdout, format!("{upload_url}\n"));

    handle.abort();
}

/// The whole point of printing the upload URL at creation time: a
/// mid-upload failure must still leave the user with the URL so the upload
/// can be resumed with `tus upload FILE URL`.
#[tokio::test]
async fn upload_failure_still_prints_created_upload_url() {
    async fn reject_patch(req: Request, next: Next) -> Result<Response, StatusCode> {
        if req.method() == Method::PATCH {
            return Err(StatusCode::BAD_REQUEST);
        }
        Ok(next.run(req).await)
    }

    let state = TusState::new(ProtocolHandle::new(
        Config::default(),
        MemoryStorage::new(),
        MemoryStateStore::new(),
        MemoryLocker::new(),
        NoopHookExecutor::new(),
    ));
    let app: Router = tus_axum::create_router(state, tus_axum::RouterOptions::default())
        .unwrap()
        .layer(axum::middleware::from_fn(reject_patch));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let endpoint = format!("http://{addr}/files");

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("doomed-upload.txt");
    tokio::fs::write(&path, b"hello").await.unwrap();

    // URL output mode: the URL must be on stdout even though PATCH failed.
    let output = run_cli(&[
        "--endpoint",
        &endpoint,
        "upload",
        "-o",
        "url",
        path.to_str().unwrap(),
    ])
    .await;

    assert!(!output.status.success());
    let upload_url = created_upload_url(&output);
    assert!(upload_url.starts_with(&endpoint), "{upload_url}");

    // Human output mode: the URL must be on stderr even though PATCH failed.
    let output = run_cli(&["--endpoint", &endpoint, "upload", path.to_str().unwrap()]).await;

    assert!(!output.status.success());
    let stderr = stderr(&output);
    let created_line = stderr
        .lines()
        .find_map(|line| line.strip_prefix("Upload created: "))
        .expect("stderr should include the created upload URL");
    assert!(created_line.starts_with(&endpoint), "{stderr}");
    assert!(!stderr.contains("Upload complete:"), "{stderr}");

    handle.abort();
}

#[tokio::test]
async fn upload_uses_config_file_for_endpoint_and_bearer_token() {
    let (endpoint, handle) =
        spawn_server_with_bearer(Config::default(), Some("secret-token")).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config-upload.txt");
    let config_path = dir.path().join("tus-uploader.toml");
    tokio::fs::write(&path, b"hello").await.unwrap();
    tokio::fs::write(
        &config_path,
        format!("endpoint = \"{endpoint}\"\nbearer_token = \"secret-token\"\n"),
    )
    .await
    .unwrap();

    let output = run_cli(&[
        "--config",
        config_path.to_str().unwrap(),
        "upload",
        path.to_str().unwrap(),
    ])
    .await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let upload_url = completed_upload_url(&output);
    assert_upload_human_output(&output, &upload_url);
    assert!(upload_url.starts_with(&endpoint));

    handle.abort();
}

#[tokio::test]
async fn create_then_upload_uses_created_upload_url() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("created-upload.txt");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();

    let create = run_cli(&["--endpoint", &endpoint, "create", "--length", "10"]).await;

    assert!(
        create.status.success(),
        "{}",
        String::from_utf8_lossy(&create.stderr)
    );
    assert_eq!(stderr(&create), "");
    let upload_url = created_upload_url(&create);
    assert!(upload_url.starts_with(&endpoint));

    let upload = run_cli(&[
        "upload",
        "--output",
        "url",
        path.to_str().unwrap(),
        &upload_url,
    ])
    .await;

    assert!(
        upload.status.success(),
        "{}",
        String::from_utf8_lossy(&upload.stderr)
    );
    assert_eq!(stdout(&upload).trim(), upload_url);
    assert_eq!(stderr(&upload), "");
    let client = Client::new(endpoint_url(&endpoint));
    let info = client.upload_at(&upload_url).unwrap().info().await.unwrap();
    assert_eq!(info.offset(), 10);
    assert_eq!(info.length(), Some(10));

    handle.abort();
}

#[tokio::test]
async fn create_then_terminate_removes_created_upload() {
    let (endpoint, handle) = spawn_server(Config::default()).await;

    let create = run_cli(&["--endpoint", &endpoint, "create", "--length", "3"]).await;

    assert!(
        create.status.success(),
        "{}",
        String::from_utf8_lossy(&create.stderr)
    );
    let upload_url = created_upload_url(&create);

    let terminate = run_cli(&["terminate", &upload_url]).await;

    assert!(
        terminate.status.success(),
        "{}",
        String::from_utf8_lossy(&terminate.stderr)
    );
    assert_eq!(stdout(&terminate), "");
    assert_eq!(stderr(&terminate), "Upload terminated\n");
    let client = Client::new(endpoint_url(&endpoint));
    let err = client
        .upload_at(&upload_url)
        .unwrap()
        .info()
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        tus_uploader::Error::UnexpectedResponse { status, .. } if status == tus_uploader::http::StatusCode::NOT_FOUND
    ));

    handle.abort();
}

#[tokio::test]
async fn create_then_info_reports_created_upload() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("info-created-upload.txt");
    tokio::fs::write(&path, b"hello world!").await.unwrap();

    let create = run_cli(&[
        "--endpoint",
        &endpoint,
        "create",
        path.to_str().unwrap(),
        "--metadata",
        "filename=info-created-upload.txt",
    ])
    .await;

    assert!(
        create.status.success(),
        "{}",
        String::from_utf8_lossy(&create.stderr)
    );
    let upload_url = created_upload_url(&create);

    let info = run_cli(&["info", &upload_url]).await;

    assert!(
        info.status.success(),
        "{}",
        String::from_utf8_lossy(&info.stderr)
    );
    assert_eq!(stderr(&info), "");
    assert_eq!(
        stdout(&info),
        format!(
            "url: {}\noffset: 0\nlength: 12\nmetadata:\nfilename=info-created-upload.txt\n",
            upload_url
        )
    );

    handle.abort();
}

#[tokio::test]
async fn upload_chunk_size_flag_splits_patch_requests() {
    let (endpoint, handle, patch_requests) = spawn_recording_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chunked-upload.txt");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();

    let output = run_cli(&[
        "--endpoint",
        &endpoint,
        "upload",
        "--chunk-size",
        "4",
        "--output",
        "url",
        path.to_str().unwrap(),
    ])
    .await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(patch_requests.requests(), four_byte_patch_requests());

    handle.abort();
}

#[tokio::test]
async fn upload_uses_config_file_chunk_size_for_patch_requests() {
    let (endpoint, handle, patch_requests) = spawn_recording_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config-chunked-upload.txt");
    let config_path = dir.path().join("tus-uploader.toml");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();
    tokio::fs::write(
        &config_path,
        format!("endpoint = \"{endpoint}\"\nchunk_size = 4\n"),
    )
    .await
    .unwrap();

    let output = run_cli(&[
        "--config",
        config_path.to_str().unwrap(),
        "upload",
        "--output",
        "url",
        path.to_str().unwrap(),
    ])
    .await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(patch_requests.requests(), four_byte_patch_requests());

    handle.abort();
}

#[tokio::test]
async fn upload_with_url_uploads_to_existing_upload() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing-upload.txt");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();
    let client = Client::new(endpoint_url(&endpoint));
    let (upload, _info) = client
        .create_upload(NewUpload::new(10, UploadMetadata::new()))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();

    let output = run_cli(&["upload", path.to_str().unwrap(), &upload_url]).await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_existing_upload_human_output(&output, &upload_url);
    let info = client.upload_at(&upload_url).unwrap().info().await.unwrap();
    assert_eq!(info.offset(), 10);

    handle.abort();
}

#[tokio::test]
async fn upload_chunk_size_flag_splits_existing_upload_patch_requests() {
    let (endpoint, handle, patch_requests) = spawn_recording_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing-chunked-upload.txt");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();
    let client = Client::new(endpoint_url(&endpoint));
    let (upload, _info) = client
        .create_upload(NewUpload::new(10, UploadMetadata::new()))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();

    let output = run_cli(&[
        "upload",
        "--chunk-size",
        "4",
        "--output",
        "url",
        path.to_str().unwrap(),
        &upload_url,
    ])
    .await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(stdout(&output).trim(), upload_url);
    assert_eq!(patch_requests.requests(), four_byte_patch_requests());
    let info = client.upload_at(&upload_url).unwrap().info().await.unwrap();
    assert_eq!(info.offset(), 10);

    handle.abort();
}

#[tokio::test]
async fn upload_url_output_prints_only_existing_upload_url() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing-url-output.txt");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();
    let client = Client::new(endpoint_url(&endpoint));
    let (upload, _info) = client
        .create_upload(NewUpload::new(10, UploadMetadata::new()))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();

    let output = run_cli(&["upload", "-o", "url", path.to_str().unwrap(), &upload_url]).await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(stdout(&output).trim(), upload_url);
    assert_eq!(stderr(&output), "");
    let info = client.upload_at(&upload_url).unwrap().info().await.unwrap();
    assert_eq!(info.offset(), 10);

    handle.abort();
}

#[tokio::test]
async fn upload_accepts_relative_existing_upload_url() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("relative-existing-upload.txt");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();
    let client = Client::new(endpoint_url(&endpoint));
    let (upload, _info) = client
        .create_upload(NewUpload::new(10, UploadMetadata::new()))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();
    let id = upload_id(&upload_url);

    let output = run_cli(&[
        "--endpoint",
        &endpoint,
        "upload",
        path.to_str().unwrap(),
        &id,
    ])
    .await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_existing_upload_human_output(&output, &upload_url);
    let info = client.upload_at(&upload_url).unwrap().info().await.unwrap();
    assert_eq!(info.offset(), 10);

    handle.abort();
}

#[tokio::test]
async fn upload_rejects_metadata_with_existing_upload_url() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metadata-existing-upload.txt");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();
    let client = Client::new(endpoint_url(&endpoint));
    let (upload, _info) = client
        .create_upload(NewUpload::new(10, UploadMetadata::new()))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();

    let output = run_cli(&[
        "upload",
        path.to_str().unwrap(),
        &upload_url,
        "--metadata",
        "filename=ignored.txt",
    ])
    .await;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("cannot be used with"), "{stderr}");
    assert!(stderr.contains("--metadata"), "{stderr}");
    assert!(stderr.contains("<UPLOAD_URL>"), "{stderr}");

    handle.abort();
}

#[tokio::test]
async fn upload_existing_relative_url_resumes_from_current_offset() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("resume-relative.bin");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();

    let client = Client::new(endpoint_url(&endpoint));
    let (upload, _info) = client
        .create_upload(NewUpload::new(10, UploadMetadata::new()))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();
    let id = upload_id(&upload_url);
    let response = reqwest::Client::new()
        .patch(&upload_url)
        .header("tus-resumable", tus_protocol::TUS_RESUMABLE)
        .header("upload-offset", "0")
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/offset+octet-stream",
        )
        .body("abcde")
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    let info = client.upload_at(&upload_url).unwrap().info().await.unwrap();
    assert_eq!(info.offset(), 5);

    let output = run_cli(&[
        "--endpoint",
        &endpoint,
        "upload",
        path.to_str().unwrap(),
        &id,
    ])
    .await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_existing_upload_human_output(&output, &upload_url);
    let info = client.upload_at(&upload_url).unwrap().info().await.unwrap();
    assert_eq!(info.offset(), 10);

    handle.abort();
}

#[tokio::test]
async fn upload_existing_url_resumes_from_current_offset() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("resume.bin");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();

    let client = Client::new(endpoint_url(&endpoint));
    let (upload, _info) = client
        .create_upload(NewUpload::new(10, UploadMetadata::new()))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();
    let response = reqwest::Client::new()
        .patch(&upload_url)
        .header("tus-resumable", tus_protocol::TUS_RESUMABLE)
        .header("upload-offset", "0")
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/offset+octet-stream",
        )
        .body("abcde")
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    let info = client.upload_at(&upload_url).unwrap().info().await.unwrap();
    assert_eq!(info.offset(), 5);

    let output = run_cli(&["upload", path.to_str().unwrap(), &upload_url]).await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_existing_upload_human_output(&output, &upload_url);
    let info = client.upload_at(&upload_url).unwrap().info().await.unwrap();
    assert_eq!(info.offset(), 10);

    handle.abort();
}

#[tokio::test]
async fn upload_existing_url_reads_file_chunks_after_resume_offset_check() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("file-backed-resume.bin");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();
    let rewritten = Arc::new(AtomicBool::new(false));
    let (endpoint, handle) = spawn_head_rewrite_server(
        Config::default(),
        HeadRewrite {
            path: path.clone(),
            contents: b"abcdeVWXYZ".to_vec(),
            rewritten: rewritten.clone(),
        },
    )
    .await;
    let client = Client::new(endpoint_url(&endpoint));
    let (upload, _info) = client
        .create_upload(NewUpload::new(10, UploadMetadata::new()))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();
    let response = reqwest::Client::new()
        .patch(&upload_url)
        .header("tus-resumable", tus_protocol::TUS_RESUMABLE)
        .header("upload-offset", "0")
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/offset+octet-stream",
        )
        .body("abcde")
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "{}", response.status());

    let output = run_cli(&["upload", "-o", "url", path.to_str().unwrap(), &upload_url]).await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(stdout(&output).trim(), upload_url);
    assert_eq!(stderr(&output), "");
    assert!(rewritten.load(Ordering::SeqCst));
    let download = reqwest::Client::new()
        .get(&upload_url)
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), StatusCode::OK);
    let body = download.bytes().await.unwrap();
    assert_eq!(body.as_ref(), b"abcdeVWXYZ");

    handle.abort();
}

#[tokio::test]
async fn upload_resume_option_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("removed-resume-option.bin");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();

    let output = run_cli(&[
        "upload",
        "--resume",
        path.to_str().unwrap(),
        "http://example.test/files/upload-1",
    ])
    .await;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("unexpected argument"), "{stderr}");
    assert!(stderr.contains("--resume"), "{stderr}");
}

#[tokio::test]
async fn resume_command_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("removed-resume.bin");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();

    let output = run_cli(&[
        "resume",
        "http://example.test/files/upload-1",
        path.to_str().unwrap(),
    ])
    .await;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("unrecognized subcommand"), "{stderr}");
    assert!(stderr.contains("resume"), "{stderr}");
}

#[tokio::test]
async fn info_accepts_relative_upload_url() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let client = Client::new(endpoint_url(&endpoint));
    let (upload, _info) = client
        .create_upload(NewUpload::new(12, UploadMetadata::new()))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();
    let id = upload_id(&upload_url);

    let output = run_cli(&["--endpoint", &endpoint, "info", &id]).await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.starts_with(&format!("url: {upload_url}\n")),
        "{stdout}"
    );

    handle.abort();
}

#[tokio::test]
async fn info_prints_human_offset_length_and_metadata() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let client = Client::new(endpoint_url(&endpoint));
    let mut metadata = HashMap::new();
    metadata.insert("z-last".to_string(), "tail".to_string());
    metadata.insert("a-first".to_string(), "info.txt".to_string());
    let (upload, _info) = client
        .create_upload(NewUpload::new(12, &metadata))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();

    let output = run_cli(&["info", &upload_url]).await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        stdout,
        format!(
            "url: {}\noffset: 0\nlength: 12\nmetadata:\na-first=info.txt\nz-last=tail\n",
            upload_url
        )
    );

    handle.abort();
}

#[tokio::test]
async fn info_reports_missing_upload_as_an_error() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let missing_upload = format!("{endpoint}/missing-upload");

    let output = run_cli(&["info", &missing_upload]).await;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("unexpected upload info response: status 404"),
        "{stderr}"
    );

    handle.abort();
}

#[tokio::test]
async fn info_json_prints_upload_info() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let client = Client::new(endpoint_url(&endpoint));
    let mut metadata = HashMap::new();
    metadata.insert("z-last".to_string(), "tail".to_string());
    metadata.insert("a-first".to_string(), "info.txt".to_string());
    let (upload, _info) = client
        .create_upload(NewUpload::new(12, &metadata))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();

    let output = run_cli(&["info", "-o", "json", &upload_url]).await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        stdout,
        format!(
            "{{\n  \"url\": \"{}\",\n  \"offset\": 0,\n  \"length\": 12,\n  \"metadata\": {{\n    \"a-first\": \"info.txt\",\n    \"z-last\": \"tail\"\n  }}\n}}\n",
            upload_url
        )
    );

    handle.abort();
}

#[tokio::test]
async fn info_json_prints_deferred_length_and_empty_metadata() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let response = reqwest::Client::new()
        .post(&endpoint)
        .header("tus-resumable", tus_protocol::TUS_RESUMABLE)
        .header("upload-defer-length", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let location = response
        .headers()
        .get(reqwest::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let upload_url = endpoint_url(&endpoint).join(location).unwrap().to_string();

    let output = run_cli(&["info", "-o", "json", &upload_url]).await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        stdout,
        format!(
            "{{\n  \"url\": \"{}\",\n  \"offset\": 0,\n  \"length\": null,\n  \"metadata\": {{}}\n}}\n",
            upload_url
        )
    );

    handle.abort();
}

#[tokio::test]
async fn info_rejects_invalid_output_format() {
    let output = run_cli(&["info", "-o", "xml", "http://example.test/files/upload-1"]).await;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid value"), "{stderr}");
    assert!(stderr.contains("xml"), "{stderr}");
}

#[tokio::test]
async fn head_command_is_removed() {
    let output = run_cli(&["head", "http://example.test/files/upload-1"]).await;

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("unrecognized subcommand"), "{stderr}");
    assert!(stderr.contains("head"), "{stderr}");
}

#[tokio::test]
async fn terminate_accepts_absolute_path_upload_url() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let client = Client::new(endpoint_url(&endpoint));
    let (upload, _info) = client
        .create_upload(NewUpload::new(3, UploadMetadata::new()))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();
    let upload_path = parse_upload_url(&upload_url).path().to_string();

    let output = run_cli(&["--endpoint", &endpoint, "terminate", &upload_path]).await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(stdout(&output), "");
    assert_eq!(stderr(&output), "Upload terminated\n");
    let err = client
        .upload_at(parse_upload_url(&upload_url))
        .unwrap()
        .info()
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        tus_uploader::Error::UnexpectedResponse { status, .. } if status == tus_uploader::http::StatusCode::NOT_FOUND
    ));

    handle.abort();
}

#[tokio::test]
async fn terminate_terminates_the_upload() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let client = Client::new(endpoint_url(&endpoint));
    let (upload, _info) = client
        .create_upload(NewUpload::new(3, UploadMetadata::new()))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();

    let output = run_cli(&["terminate", &upload_url]).await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(stdout(&output), "");
    assert_eq!(stderr(&output), "Upload terminated\n");
    let err = client
        .upload_at(parse_upload_url(&upload_url))
        .unwrap()
        .info()
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        tus_uploader::Error::UnexpectedResponse { status, .. } if status == tus_uploader::http::StatusCode::NOT_FOUND
    ));

    handle.abort();
}
