# Info Command Output Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `tus head` with `tus info` and add `-o, --output <human|json>` formatting for upload information.

**Architecture:** Keep upload information retrieval on the existing `build_upload_client(...).upload(...).info()` path so absolute, absolute-path, and relative upload references behave as they do today. Move output rendering into small CLI-local helpers: one for the existing human-readable format and one for deterministic JSON output.

**Tech Stack:** Rust workspace, `clap` subcommands and `ValueEnum`, `serde`/`serde_json`, `tus-client`, Tokio integration tests, Cargo formatting/tests.

**Workspace Policy:** Do not create git commits during execution unless the user explicitly requests them.

---

## File Structure

- Modify `crates/tus-cli/tests/cli.rs`: replace `head` tests with `info` tests, add JSON output tests, add invalid output format test, and add `head` removal test.
- Modify `crates/tus-cli/src/main.rs`: rename `Command::Head` to `Command::Info`, add `OutputFormat`, and add output rendering helpers.
- Modify `crates/tus-cli/src/settings.rs`: update the unit test fixture to construct `Command::Info`.
- Modify `crates/tus-cli/Cargo.toml`: add `serde_json` dependency for JSON rendering.
- Modify `tmp/README.md`: document `info` and `info -o json` instead of `head`.

### Task 1: Write Failing Tests For `info` And JSON Output

**Files:**
- Modify: `crates/tus-cli/tests/cli.rs`

- [ ] **Step 1: Replace the existing `head` tests with `info` tests**

In `crates/tus-cli/tests/cli.rs`, replace the three tests `head_accepts_relative_upload_url`, `head_prints_offset_length_and_metadata`, and `head_reports_missing_upload_as_an_error` with this block:

```rust
#[tokio::test]
async fn info_accepts_relative_upload_url() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let client = Client::new(endpoint_url(&endpoint));
    let upload = client
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
    let upload = client
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
```

- [ ] **Step 2: Add JSON output and parser validation tests**

Insert these tests immediately after `info_reports_missing_upload_as_an_error`:

```rust
#[tokio::test]
async fn info_json_prints_upload_info() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let client = Client::new(endpoint_url(&endpoint));
    let mut metadata = HashMap::new();
    metadata.insert("z-last".to_string(), "tail".to_string());
    metadata.insert("a-first".to_string(), "info.txt".to_string());
    let upload = client
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
```

- [ ] **Step 3: Run the RED tests**

Run: `cargo test -p tus-cli --test cli info`

Expected: FAIL because `info` is not yet a recognized subcommand.

Run: `cargo test -p tus-cli --test cli head_command_is_removed`

Expected: FAIL because `head` is still recognized at this point.

### Task 2: Implement `info` Command And Output Formatting

**Files:**
- Modify: `crates/tus-cli/Cargo.toml`
- Modify: `crates/tus-cli/src/main.rs`
- Modify: `crates/tus-cli/src/settings.rs`

- [ ] **Step 1: Add the JSON dependency**

In `crates/tus-cli/Cargo.toml`, add `serde_json` after `serde` in `[dependencies]`:

```toml
serde = { workspace = true }
serde_json = "1"
tokio = { workspace = true, features = ["full"] }
```

- [ ] **Step 2: Update imports in `main.rs`**

At the top of `crates/tus-cli/src/main.rs`, replace the current imports with:

```rust
use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
};
use tus_client::Client;
use url::Url;
```

- [ ] **Step 3: Rename the subcommand and add output format parsing**

In `crates/tus-cli/src/main.rs`, replace the `Head` variant with `Info`, and insert `OutputFormat` after the `Command` enum:

```rust
    /// Print the current offset, length, and metadata for an upload.
    Info {
        upload_url: String,
        #[arg(short = 'o', long = "output", value_enum, default_value = "human")]
        output: OutputFormat,
    },
```

```rust
#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputFormat {
    Human,
    Json,
}
```

- [ ] **Step 4: Route `info` through the formatter**

In the `match cli.command` block in `crates/tus-cli/src/main.rs`, replace the current `Command::Head` arm with:

```rust
        Command::Info { upload_url, output } => {
            let client = build_upload_client(&upload_url, &settings)?;
            let upload = client.upload(&upload_url)?.info().await?;
            print_upload_info(upload, output)?;
        }
```

- [ ] **Step 5: Add output formatting helpers**

Insert these helpers after `apply_client_settings` in `crates/tus-cli/src/main.rs`:

```rust
fn print_upload_info(upload: tus_client::UploadInfo, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Human => {
            print_upload_info_human(upload);
            Ok(())
        }
        OutputFormat::Json => print_upload_info_json(upload),
    }
}

fn print_upload_info_human(upload: tus_client::UploadInfo) {
    println!("url: {}", upload.url);
    println!("offset: {}", upload.offset);
    match upload.length {
        Some(length) => println!("length: {}", length),
        None => println!("length: deferred"),
    }
    println!("metadata:");
    for (key, value) in metadata_to_sorted_strings(upload.metadata) {
        println!("{}={}", key, value);
    }
}

fn print_upload_info_json(upload: tus_client::UploadInfo) -> Result<()> {
    let output = UploadInfoJson {
        url: upload.url.to_string(),
        offset: upload.offset,
        length: upload.length,
        metadata: metadata_to_sorted_strings(upload.metadata),
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn metadata_to_sorted_strings(metadata: tus_client::UploadMetadata) -> BTreeMap<String, String> {
    metadata
        .into_iter()
        .map(|(key, value)| (key, value.to_string_lossy().into_owned()))
        .collect()
}

#[derive(Serialize)]
struct UploadInfoJson {
    url: String,
    offset: u64,
    length: Option<u64>,
    metadata: BTreeMap<String, String>,
}
```

- [ ] **Step 6: Update unit-test command names in `main.rs`**

In `crates/tus-cli/src/main.rs`, update the command name in parser tests from `head` to `info`:

```rust
            "info",
```

There are two occurrences in the `tests` module.

- [ ] **Step 7: Update the settings test fixture**

In `crates/tus-cli/src/settings.rs`, replace the test import and command fixture with:

```rust
    use crate::{Command, OutputFormat};
```

```rust
            command: Command::Info {
                upload_url: "http://example.com/uploads/1".to_string(),
                output: OutputFormat::Human,
            },
```

- [ ] **Step 8: Run targeted tests to verify implementation**

Run: `cargo test -p tus-cli --test cli info && cargo test -p tus-cli --test cli head_command_is_removed`

Expected: PASS for all `info` filtered tests and for `head_command_is_removed`.

- [ ] **Step 9: Run broader CLI tests**

Run: `cargo test -p tus-cli`

Expected: PASS for all `tus-cli` tests.

### Task 3: Update CLI Documentation And Final Verification

**Files:**
- Modify: `tmp/README.md`

- [ ] **Step 1: Update CLI examples**

In `tmp/README.md`, replace the inspect/terminate block:

```markdown
# Inspect or terminate an upload.
cargo run -p tus-cli -- head http://127.0.0.1:8080/files/<upload-id>
cargo run -p tus-cli -- terminate http://127.0.0.1:8080/files/<upload-id>
```

with:

```markdown
# Inspect or terminate an upload.
cargo run -p tus-cli -- info http://127.0.0.1:8080/files/<upload-id>
cargo run -p tus-cli -- info -o json http://127.0.0.1:8080/files/<upload-id>
cargo run -p tus-cli -- terminate http://127.0.0.1:8080/files/<upload-id>
```

- [ ] **Step 2: Run formatting**

Run: `cargo fmt --all`

Expected: command exits successfully.

- [ ] **Step 3: Run targeted CLI tests**

Run: `cargo test -p tus-cli --test cli info && cargo test -p tus-cli --test cli head_command_is_removed && cargo test -p tus-cli --test cli terminate_`

Expected: all three commands pass.

- [ ] **Step 4: Run full verification**

Run: `cargo fmt --all --check && cargo test`

Expected: formatting check and the full workspace test suite pass.

## Self-Review

- Spec coverage: `head` to `info` rename, removal of `head`, command-local `-o/--output`, default human output, JSON output, deferred length as `null`, empty metadata as `{}`, deterministic metadata ordering, relative upload URL support, docs, and final verification are covered by Tasks 1-3.
- Placeholder scan: the plan contains concrete file paths, code snippets, commands, and expected outcomes; it has no placeholder implementation steps.
- Type consistency: `Command::Info` carries `OutputFormat`, `OutputFormat` is a `clap::ValueEnum`, `print_upload_info` accepts `tus_client::UploadInfo`, and JSON output uses a serializable `UploadInfoJson` with a `BTreeMap<String, String>` metadata field.
