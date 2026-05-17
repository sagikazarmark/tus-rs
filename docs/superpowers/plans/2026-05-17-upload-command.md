# Upload Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Revise `tus upload` so it creates a new upload when no upload URL is supplied and uploads/resumes to an existing upload resource when a URL is supplied.

**Architecture:** Keep `tus put` compatible: its optional URL remains a collection endpoint override. Split the CLI implementation into a create path using `Client::upload_from_with_progress` / `Client::upload_from` and an existing-resource path using `Client::upload(...)? .upload_with_progress(...)` / `.upload(...)`.

**Tech Stack:** Rust workspace, `clap` subcommands, `tus-client`, Tokio integration tests, Cargo formatting/tests.

**Workspace Policy:** Do not create git commits during execution unless the user explicitly requests them.

---

## File Structure

- Modify `crates/tus-cli/tests/cli.rs`: adjust existing `upload` tests for no-URL creation and add existing-upload URL tests.
- Modify `crates/tus-cli/src/main.rs`: change `Command::Upload` URL from `Option<Url>` to `Option<String>`, split `upload` and `put` paths, reject metadata with existing upload URL, and add an existing-resource upload helper.
- Modify `crates/tus-cli/src/settings.rs`: update endpoint help text to cover configured endpoints for create and relative upload URLs.
- Modify `tmp/README.md`: document `upload` create mode with `--endpoint`, existing-upload mode with upload URL, and `put` compatibility.

### Task 1: Write Failing Tests For Revised `upload` Semantics

**Files:**
- Modify: `crates/tus-cli/tests/cli.rs`

- [ ] **Step 1: Update create-mode test to omit positional URL**

Replace the body of `upload_prints_created_upload_url_and_metadata` in `crates/tus-cli/tests/cli.rs` with:

```rust
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
    let upload_url = String::from_utf8(output.stdout).unwrap();
    assert!(upload_url.trim().starts_with(&endpoint));

    let client = Client::new(endpoint_url(&endpoint));
    let info = client.upload(upload_url.trim()).unwrap().info().await.unwrap();
    assert_eq!(info.offset, 5);
    assert_eq!(info.length, Some(5));
    assert_eq!(
        info.metadata
            .get("filename")
            .unwrap()
            .to_string_lossy(),
        "upload.txt"
    );

    handle.abort();
}
```

- [ ] **Step 2: Add existing upload URL tests**

Insert these tests after `upload_uses_config_file_for_endpoint_and_bearer_token`:

```rust
#[tokio::test]
async fn upload_with_url_uploads_to_existing_upload() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing-upload.txt");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();
    let client = Client::new(endpoint_url(&endpoint));
    let upload = client
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
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), upload_url);
    let info = client.upload(&upload_url).unwrap().info().await.unwrap();
    assert_eq!(info.offset, 10);

    handle.abort();
}

#[tokio::test]
async fn upload_accepts_relative_existing_upload_url() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("relative-existing-upload.txt");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();
    let client = Client::new(endpoint_url(&endpoint));
    let upload = client
        .create_upload(NewUpload::new(10, UploadMetadata::new()))
        .await
        .unwrap();
    let upload_url = upload.url().to_string();
    let id = upload_id(&upload_url);

    let output = run_cli(&["--endpoint", &endpoint, "upload", path.to_str().unwrap(), &id]).await;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), upload_url);
    let info = client.upload(&upload_url).unwrap().info().await.unwrap();
    assert_eq!(info.offset, 10);

    handle.abort();
}

#[tokio::test]
async fn upload_rejects_metadata_with_existing_upload_url() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metadata-existing-upload.txt");
    tokio::fs::write(&path, b"abcdefghij").await.unwrap();
    let client = Client::new(endpoint_url(&endpoint));
    let upload = client
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
    assert!(
        stderr.contains("--metadata cannot be used with an existing upload URL"),
        "{stderr}"
    );

    handle.abort();
}
```

- [ ] **Step 3: Run tests to verify failure**

Run: `cargo test -p tus-cli --test cli upload_`

Expected: FAIL because current `upload` still treats the positional URL as a collection endpoint and shares the `put` create path.

### Task 2: Implement Revised Upload Semantics

**Files:**
- Modify: `crates/tus-cli/src/main.rs`

- [ ] **Step 1: Change `Command::Upload` URL type**

In `crates/tus-cli/src/main.rs`, replace the `Upload` variant with:

```rust
    /// Upload a file and print the resulting upload URL.
    Upload {
        file: PathBuf,
        #[arg(value_name = "UPLOAD_URL")]
        url: Option<String>,
        #[arg(long = "metadata", value_parser = parse_metadata)]
        metadata: Vec<(String, String)>,
    },
```

- [ ] **Step 2: Split `put` and `upload` match arms**

Replace the shared `Command::Put | Command::Upload` arm with:

```rust
        Command::Put {
            file,
            url,
            metadata,
        } => {
            let upload = create_upload_file(file, url, metadata, &settings).await?;
            println!("{}", upload.url);
        }
        Command::Upload {
            file,
            url,
            metadata,
        } => {
            let upload = upload_file(file, url, metadata, &settings).await?;
            println!("{}", upload.url);
        }
```

- [ ] **Step 3: Replace upload helper with create/resume helpers**

Replace the current `upload_file` helper with:

```rust
async fn upload_file(
    file: PathBuf,
    upload_url: Option<String>,
    metadata: Vec<(String, String)>,
    settings: &Settings,
) -> Result<tus_client::UploadInfo> {
    match upload_url {
        Some(upload_url) => {
            if !metadata.is_empty() {
                anyhow::bail!("--metadata cannot be used with an existing upload URL");
            }
            upload_existing_file(file, &upload_url, settings).await
        }
        None => create_upload_file(file, None, metadata, settings).await,
    }
}

async fn create_upload_file(
    file: PathBuf,
    url: Option<Url>,
    metadata: Vec<(String, String)>,
    settings: &Settings,
) -> Result<tus_client::UploadInfo> {
    let endpoint = resolve_collection_endpoint(url, settings)?;
    let client = build_collection_client(endpoint, settings)?;
    let metadata = to_metadata_map(metadata);
    let upload = if settings.progress {
        let contents = read_upload_file(&file).await?;
        let total = contents.len() as u64;
        let mut progress = Progress::new(total);
        let upload = client
            .upload_from_with_progress(contents, &metadata, &mut progress)
            .await?;
        progress.finish(upload.offset);
        upload
    } else {
        client
            .upload_from(read_upload_file(&file).await?, &metadata)
            .await?
    };

    Ok(upload)
}

async fn upload_existing_file(
    file: PathBuf,
    upload_url: &str,
    settings: &Settings,
) -> Result<tus_client::UploadInfo> {
    let client = build_upload_client(upload_url, settings)?;
    let upload = client.upload(upload_url)?;
    let info = if settings.progress {
        let contents = read_upload_file(&file).await?;
        let total = contents.len() as u64;
        let mut progress = Progress::new(total);
        let info = upload.upload_with_progress(contents, &mut progress).await?;
        progress.finish(info.offset);
        info
    } else {
        upload.upload(read_upload_file(&file).await?).await?
    };

    Ok(info)
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test -p tus-cli --test cli upload_`

Expected: PASS for all `upload_` tests.

- [ ] **Step 5: Verify `put` compatibility**

Run: `cargo test -p tus-cli --test cli put_`

Expected: PASS for all existing `put_` tests.

### Task 3: Update CLI Docs And Final Verification

**Files:**
- Modify: `crates/tus-cli/src/settings.rs`
- Modify: `tmp/README.md`

- [ ] **Step 1: Update endpoint help text**

In `crates/tus-cli/src/settings.rs`, set the endpoint comment to:

```rust
    /// Default TUS collection endpoint used for new uploads and relative upload URLs.
```

- [ ] **Step 2: Update README examples**

In `tmp/README.md`, update the CLI examples to show `upload` create mode via `--endpoint`, existing-upload mode with URL, and `put` compatibility:

```markdown
# Create a new upload from the configured endpoint and print the upload URL.
cargo run -p tus-cli -- --endpoint http://127.0.0.1:8080/files upload ./video.mp4

# Upload/resume to an existing upload URL.
cargo run -p tus-cli -- upload ./video.mp4 http://127.0.0.1:8080/files/<upload-id>

# `put` remains supported with the legacy collection-URL positional argument.
cargo run -p tus-cli -- put ./video.mp4 http://127.0.0.1:8080/files
```

Change the config example to:

```markdown
cargo run -p tus-cli -- --config ./tus-client.toml upload ./video.mp4
```

- [ ] **Step 3: Run formatting**

Run: `cargo fmt --all`

Expected: command exits successfully.

- [ ] **Step 4: Run targeted CLI tests**

Run: `cargo test -p tus-cli --test cli upload_ && cargo test -p tus-cli --test cli put_`

Expected: both commands pass.

- [ ] **Step 5: Run broader CLI tests**

Run: `cargo test -p tus-cli`

Expected: all `tus-cli` tests pass.

## Self-Review

- Spec coverage: `upload` create mode, `upload` existing-resource mode, metadata rejection, `put` compatibility, docs, and no parallel changes are covered by Tasks 1-3.
- Placeholder scan: the plan contains concrete file paths, code snippets, commands, and expected outcomes.
- Type consistency: `Command::Upload` uses `Option<String>`, `Command::Put` keeps `Option<Url>`, and helper signatures match the call sites.
