# Terminate Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the CLI upload termination command and public client termination API from `rm` / `delete` to `terminate`.

**Architecture:** Keep the existing HTTP behavior intact: termination still sends `DELETE` and expects `204 No Content`. Rename only the user-facing CLI command, public client handle method, and client-side helper/error label.

**Tech Stack:** Rust workspace, `clap` CLI parsing, async `tus-client`, Tokio tests, Cargo formatting/tests.

**Workspace Policy:** Do not create git commits during execution unless the user explicitly requests them.

---

## File Structure

- Modify `crates/tus-cli/tests/cli.rs`: rename CLI tests and switch invocations from `rm` to `terminate`.
- Modify `crates/tus-cli/src/main.rs`: rename `Command::Rm` to `Command::Terminate` and dispatch the new subcommand.
- Modify `crates/tus-client/src/client/handle.rs`: rename `Upload::delete()` to `Upload::terminate()` and update unit tests.
- Modify `crates/tus-client/src/client/protocol.rs`: rename `delete_upload_at` to `terminate_upload_at` and update the client-side unexpected-response label.
- Modify `crates/tus-client/src/transport/reqwest.rs`: update integration test naming/calls for `Upload::terminate()`.
- Modify `crates/tus-client/README.md`: update public API documentation from `Upload::delete()` to `Upload::terminate()`.

### Task 1: Rename CLI Command

**Files:**
- Modify: `crates/tus-cli/tests/cli.rs:600-663`
- Modify: `crates/tus-cli/src/main.rs:48-49, 123-128`

- [ ] **Step 1: Write the failing CLI tests**

Replace the two `rm` tests in `crates/tus-cli/tests/cli.rs` with:

```rust
#[tokio::test]
async fn terminate_accepts_absolute_path_upload_url() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let client = Client::new(endpoint_url(&endpoint));
    let upload = client
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
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), upload_url);
    let err = client
        .upload(parse_upload_url(&upload_url))
        .unwrap()
        .info()
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        tus_client::Error::UnexpectedResponse { status: 404, .. }
    ));

    handle.abort();
}

#[tokio::test]
async fn terminate_terminates_the_upload() {
    let (endpoint, handle) = spawn_server(Config::default()).await;
    let client = Client::new(endpoint_url(&endpoint));
    let upload = client
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
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), upload_url);
    let err = client
        .upload(parse_upload_url(&upload_url))
        .unwrap()
        .info()
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        tus_client::Error::UnexpectedResponse { status: 404, .. }
    ));

    handle.abort();
}
```

- [ ] **Step 2: Run the CLI tests to verify they fail**

Run: `cargo test -p tus-cli --test cli terminate_`

Expected: FAIL because `terminate` is not yet a recognized subcommand.

- [ ] **Step 3: Rename the CLI command implementation**

In `crates/tus-cli/src/main.rs`, change the enum variant and match arm to:

```rust
    /// Terminate an upload.
    Terminate { upload_url: String },
```

```rust
        Command::Terminate { upload_url } => {
            let client = build_upload_client(&upload_url, &settings)?;
            let upload = client.upload(&upload_url)?;
            upload.delete().await?;
            println!("{}", upload.url());
        }
```

- [ ] **Step 4: Run the CLI tests to verify they pass**

Run: `cargo test -p tus-cli --test cli terminate_`

Expected: PASS for both `terminate_` tests.

### Task 2: Rename Public Client Method

**Files:**
- Modify: `crates/tus-client/src/client/handle.rs:30-33, 185-200`
- Modify: `crates/tus-client/src/client/protocol.rs:305-313, 726`
- Modify: `crates/tus-client/src/transport/reqwest.rs:559-570`
- Modify: `crates/tus-cli/src/main.rs:123-128`

- [ ] **Step 1: Write the failing client tests**

In `crates/tus-client/src/client/handle.rs`, rename the test and call:

```rust
    #[async_test]
    async fn upload_terminate_uses_resource_url() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                204,
                http::HeaderMap::new(),
                Vec::new(),
            )));
        let client = Client::with_transport(endpoint_url(), transport.clone());

        client.upload(upload_url()).unwrap().terminate().await.unwrap();

        let requests = transport.requests.lock().unwrap();
        let request = requests.first().unwrap();
        assert_eq!(request.method(), Method::DELETE);
        assert_eq!(
            request.uri().to_string(),
            "http://example.test/files/upload-1"
        );
        assert_eq!(
            request
                .headers()
                .get("tus-resumable")
                .and_then(|value| value.to_str().ok()),
            Some(tus_protocol::TUS_RESUMABLE)
        );
        assert!(matches!(request.body(), TransportBody::Empty));
    }
```

In `crates/tus-client/src/transport/reqwest.rs`, rename the test and call:

```rust
    #[tokio::test]
    async fn terminate_upload_terminates_remote_upload() {
        let (endpoint, server_handle) = spawn_test_server().await;
        let client = Client::new(endpoint_url(&endpoint));
        let upload = client
            .create_upload(NewUpload::new(5, UploadMetadata::new()))
            .await
            .unwrap();

        let handle = client.upload(upload.url().clone()).unwrap();
        handle.terminate().await.unwrap();

        let err = handle.info().await.unwrap_err();
        assert!(matches!(err, Error::UnexpectedResponse { status: 404, .. }));

        server_handle.abort();
    }
```

- [ ] **Step 2: Run the client tests to verify they fail**

Run: `cargo test -p tus-client terminate`

Expected: FAIL to compile because `Upload::terminate()` is not yet defined.

- [ ] **Step 3: Rename the public method and helper**

In `crates/tus-client/src/client/handle.rs`, replace `delete` with:

```rust
    /// Terminates this upload resource.
    pub async fn terminate(&self) -> Result<()> {
        self.client.terminate_upload_at(&self.url).await
    }
```

In `crates/tus-client/src/client/protocol.rs`, replace `delete_upload_at` with:

```rust
    /// Terminates an existing upload.
    pub(super) async fn terminate_upload_at(&self, upload_url: &Url) -> Result<()> {
        let response = self
            .transport
            .send(self.request(Method::DELETE, upload_url.as_str())?)
            .await?;
        if response.status().as_u16() != 204 {
            Err(unexpected_response("terminate upload", response).await)
        } else {
            Ok(())
        }
    }
```

In `crates/tus-client/src/client/protocol.rs`, update the client test expectation from `"delete upload"` to `"terminate upload"` where it checks the unexpected-response operation label.

In `crates/tus-cli/src/main.rs`, update the command handler to call the renamed method:

```rust
        Command::Terminate { upload_url } => {
            let client = build_upload_client(&upload_url, &settings)?;
            let upload = client.upload(&upload_url)?;
            upload.terminate().await?;
            println!("{}", upload.url());
        }
```

- [ ] **Step 4: Run the client tests to verify they pass**

Run: `cargo test -p tus-client terminate`

Expected: PASS for the termination-related client tests.

- [ ] **Step 5: Run the CLI tests again**

Run: `cargo test -p tus-cli --test cli terminate_`

Expected: PASS for both CLI termination tests.

### Task 3: Update Documentation And Verify

**Files:**
- Modify: `crates/tus-client/README.md:59, 96`

- [ ] **Step 1: Update public API documentation**

In `crates/tus-client/README.md`, replace the public method references with:

```markdown
| `Upload::terminate()` | Terminate an upload when the server supports termination. |
```

```markdown
| Termination | Supported | Terminate upload resources with `Upload::terminate`. |
```

- [ ] **Step 2: Search for stale client-facing names**

Run: `rg "Upload::delete|\.delete\(\)|\brm\b|Command::Rm|delete_upload_at" crates/tus-cli crates/tus-client README.md crates/tus-client/README.md`

Expected: no matches for stale CLI/client-facing names. Matches in server protocol, storage/state traits, Axum handlers, or old superpowers plan files are outside the rename scope.

- [ ] **Step 3: Run formatting**

Run: `cargo fmt --all`

Expected: command exits successfully.

- [ ] **Step 4: Run targeted tests**

Run: `cargo test -p tus-client terminate && cargo test -p tus-cli --test cli terminate_`

Expected: both commands pass.

- [ ] **Step 5: Run broader tests if time permits**

Run: `cargo test -p tus-client && cargo test -p tus-cli`

Expected: both packages pass.

## Self-Review

- Spec coverage: CLI command, public client method, internal helper, tests, docs, and non-goals are covered by Tasks 1-3.
- Placeholder scan: the plan contains exact paths, code snippets, commands, and expected outcomes.
- Type consistency: `Command::Terminate`, `Upload::terminate()`, and `terminate_upload_at` are used consistently after their defining steps.
