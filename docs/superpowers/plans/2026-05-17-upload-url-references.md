# Upload URL References Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow `tus-client` and `tus-cli` to accept absolute upload URLs, endpoint-absolute paths, and endpoint-relative paths for existing upload resources and server `Location` headers.

**Architecture:** Add one resolver in `tus-client` that turns an upload URL reference into an absolute `Url` using collection semantics for the configured endpoint. Make public upload-handle construction validate and resolve once, so `Upload` continues storing a concrete `Url` and all operations reuse it.

**Tech Stack:** Rust workspace, `url::Url`, `anyhow` for CLI errors, existing mock transport tests and CLI integration tests.

---

### File Map

- Modify: `crates/tus-client/src/helpers.rs` for central upload URL reference resolution and tests.
- Modify: `crates/tus-client/src/client/handle.rs` for `Client::upload` returning `Result<Upload<T>>` and handle tests.
- Modify: `crates/tus-client/src/client/protocol.rs` for server `Location` handling expectations and concatenation URL resolution.
- Modify: `crates/tus-client/src/client/upload.rs` for any public `Client::upload` call sites affected by the new `Result`.
- Modify: `crates/tus-cli/src/main.rs` for raw upload reference handling in `resume`, `head`, `terminate`, and `cat`.
- Modify: `crates/tus-cli/tests/cli.rs` for CLI coverage using `--endpoint` plus absolute-path and relative upload references.

### Task 1: Central URL Reference Resolver

**Files:**
- Modify: `crates/tus-client/src/helpers.rs:116-118`
- Test: `crates/tus-client/src/helpers.rs:259-292`

- [ ] **Step 1: Write failing resolver tests**

Add tests covering collection semantics:

```rust
#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
fn resolve_upload_url_accepts_absolute_url_absolute_path_and_relative_path() {
    let endpoint = Url::parse("http://example.test/files").unwrap();
    let cases = [
        (
            "http://uploads.example.test/upload-1",
            "http://uploads.example.test/upload-1",
        ),
        ("/files/upload-1", "http://example.test/files/upload-1"),
        ("upload-1", "http://example.test/files/upload-1"),
        ("nested/upload-1", "http://example.test/files/nested/upload-1"),
    ];

    for (reference, expected) in cases {
        let url = resolve_upload_url(&endpoint, reference).unwrap();

        assert_eq!(url.as_str(), expected);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tus-client resolve_upload_url_accepts_absolute_url_absolute_path_and_relative_path`

Expected: FAIL because `resolve_upload_url` does not exist.

- [ ] **Step 3: Implement resolver with endpoint collection semantics**

Replace the existing `resolve_upload_location` helper with this resolver and keep `resolve_upload_location` as a named wrapper for protocol code:

```rust
pub(crate) fn resolve_upload_url(endpoint: &Url, reference: &str) -> Result<Url> {
    let mut base = endpoint.clone();
    if !base.path().ends_with('/') {
        let path = format!("{}/", base.path());
        base.set_path(&path);
    }

    Ok(base.join(reference)?)
}

pub(crate) fn resolve_upload_location(endpoint: &Url, location: &str) -> Result<Url> {
    resolve_upload_url(endpoint, location)
}
```

- [ ] **Step 4: Update existing Location resolver test expectations**

Rename `resolve_upload_location_delegates_to_standard_url_resolution` to reflect collection semantics and change the `"upload-1"` case for endpoint `http://example.test/files` from `http://example.test/upload-1` to `http://example.test/files/upload-1`.

- [ ] **Step 5: Run resolver tests**

Run: `cargo test -p tus-client resolve_upload_`

Expected: PASS.

### Task 2: Resolve `Client::upload` Inputs Immediately

**Files:**
- Modify: `crates/tus-client/src/client/handle.rs:63-80`
- Test: `crates/tus-client/src/client/handle.rs:99-304`

- [ ] **Step 1: Write failing handle tests**

Add tests proving public handle creation accepts all requested forms:

```rust
#[cfg_attr(not(target_arch = "wasm32"), test)]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
fn upload_accepts_absolute_path_and_relative_path() {
    let client = Client::with_transport(endpoint_url(), MockTransport::default());

    assert_eq!(
        client.upload("/files/upload-1").unwrap().url().as_str(),
        "http://example.test/files/upload-1"
    );
    assert_eq!(
        client.upload("upload-1").unwrap().url().as_str(),
        "http://example.test/files/upload-1"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tus-client upload_accepts_absolute_path_and_relative_path`

Expected: FAIL because `Client::upload` currently takes `Url` and returns `Upload<T>`.

- [ ] **Step 3: Change `Client::upload` signature and construction**

Use the resolver from Task 1:

```rust
use crate::helpers::resolve_upload_url;

pub fn upload(&self, upload_url: impl AsRef<str>) -> Result<Upload<T>> {
    Ok(Upload {
        client: (*self).clone(),
        url: resolve_upload_url(&self.endpoint, upload_url.as_ref())?,
    })
}
```

- [ ] **Step 4: Update direct call sites**

Change tests and code from:

```rust
client.upload(upload_url()).info().await.unwrap();
```

to:

```rust
client.upload(upload_url()).unwrap().info().await.unwrap();
```

Change `create_upload` to construct a handle without delaying errors:

```rust
Ok(self.upload(upload.url.as_str())?)
```

- [ ] **Step 5: Run handle tests**

Run: `cargo test -p tus-client client::handle`

Expected: PASS.

### Task 3: Apply Resolver to Protocol URL Inputs

**Files:**
- Modify: `crates/tus-client/src/client/protocol.rs:177-199`
- Modify: `crates/tus-client/src/client/protocol.rs:453-500`
- Test: `crates/tus-client/src/client/protocol.rs:721-741`

- [ ] **Step 1: Update server `Location` expectations**

Change tests that currently expect `Location: upload-1` against endpoint `/files` to resolve to `http://example.test/files/upload-1`.

- [ ] **Step 2: Write failing concatenation reference test**

Add a test that calls `concatenate_uploads(&["part-1"], ...)` and asserts the outgoing `Upload-Concat` header contains `http://example.test/files/part-1`.

- [ ] **Step 3: Make `concatenate_uploads` generic over string references**

Change the signature and resolve each partial URL before building the header:

```rust
pub async fn concatenate_uploads<S>(
    &self,
    part_urls: &[S],
    metadata: impl Into<UploadMetadata>,
) -> Result<UploadInfo>
where
    S: AsRef<str>,
{
    let metadata = metadata.into();
    let part_urls = part_urls
        .iter()
        .map(|url| resolve_upload_location(&self.endpoint, url.as_ref()).map(|url| url.to_string()))
        .collect::<Result<Vec<_>>>()?;
    let upload_concat = format!("final;{}", part_urls.join(" "));
```

- [ ] **Step 4: Run protocol tests**

Run: `cargo test -p tus-client client::protocol`

Expected: PASS.

### Task 4: Update CLI Existing-Upload Commands

**Files:**
- Modify: `crates/tus-cli/src/main.rs:88-151`
- Modify: `crates/tus-cli/src/main.rs:167-210`
- Test: `crates/tus-cli/tests/cli.rs:431-585`

- [ ] **Step 1: Write failing CLI tests for relative references**

Add tests for `head`, `resume`, and `terminate` using `--endpoint <collection>` plus either `/files/<id>` or `<id>`. Assert the commands succeed and print resolved absolute URLs where applicable.

- [ ] **Step 2: Run CLI tests to verify failure**

Run: `cargo test -p tus-cli head_accepts_relative_upload_url resume_accepts_relative_upload_url terminate_accepts_absolute_path_upload_url`

Expected: FAIL because `main.rs` still calls `Url::parse(&upload_url)`.

- [ ] **Step 3: Stop pre-parsing existing upload references**

Change command handlers to use the client resolver:

```rust
let handle = client.upload(&upload_url)?;
let upload = handle.info().await?;
```

For `resume`, create the handle once and call either `upload_with_progress` or `upload` on it. For `terminate`, print `handle.url()` after termination instead of the raw input string.

- [ ] **Step 4: Resolve CLI base endpoint for relative upload references**

Replace `build_upload_client` and `collection_endpoint` so absolute upload URLs preserve existing parent-endpoint derivation, while relative references require configured `endpoint`:

```rust
fn build_upload_client(upload_url: &str, settings: &Settings) -> Result<Client> {
    let endpoint = match Url::parse(upload_url) {
        Ok(url) => collection_endpoint(url)?,
        Err(url::ParseError::RelativeUrlWithoutBase) => settings.endpoint.clone().context(
            "endpoint required for relative upload URL; pass --endpoint or configure `endpoint`",
        )?,
        Err(err) => return Err(err).context("invalid upload URL"),
    };
    let client = Client::new(endpoint);
    apply_client_settings(client, settings)
}

fn collection_endpoint(mut url: Url) -> Result<Url> {
    let mut segments = url
        .path_segments()
        .context("upload URL must include a path")?
        .collect::<Vec<_>>();
    if segments.is_empty() {
        anyhow::bail!("upload URL must include an upload id path segment");
    }
    segments.pop();
    url.set_path(&segments.join("/"));
    Ok(url)
}
```

- [ ] **Step 5: Run CLI tests**

Run: `cargo test -p tus-cli`

Expected: PASS.

### Task 5: Documentation and Verification

**Files:**
- Modify: `crates/tus-client/README.md:48-60`
- Modify: `README.md` CLI examples if the current examples need relative-reference coverage.

- [ ] **Step 1: Update client API docs**

Document `Client::upload(upload_url)` as accepting absolute URLs, absolute paths, or endpoint-relative paths and returning `Result<Upload>`.

- [ ] **Step 2: Run formatting**

Run: `cargo fmt --all`

Expected: no errors.

- [ ] **Step 3: Run targeted tests**

Run: `cargo test -p tus-client -p tus-cli`

Expected: PASS.

- [ ] **Step 4: Check worktree**

Run: `git status --short`

Expected: only the files touched by this plan are modified. Do not commit unless the user explicitly asks for a commit.

### Self-Review

- Spec coverage: URL resolution covers absolute URLs, absolute paths, relative paths, server `Location` headers, `Client::upload`, CLI existing-upload commands, and concatenation partial URLs.
- Placeholder scan: no TBD/TODO placeholders remain.
- Type consistency: `Client::upload` returns `Result<Upload<T>>`; existing `Upload` methods continue returning `Result<UploadInfo>` or `Result<()>`; CLI continues using `anyhow::Result`.
