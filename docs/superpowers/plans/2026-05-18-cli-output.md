# CLI Output Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make upload and terminate default to human stderr status while preserving URL-only upload output for scripts.

**Architecture:** Keep output formatting local to `crates/tus-cli/src/main.rs`. Add a command-local upload output enum for `url`, derive progress suppression from that mode, and leave `info`'s existing `OutputFormat` unchanged.

**Tech Stack:** Rust, clap value enums, Tokio CLI integration tests, Cargo test.

**Workspace Policy:** Do not create git commits during execution unless the user explicitly requests them.

---

## File Structure

- Modify `crates/tus-cli/src/main.rs`: add upload output parsing, print upload/terminate human messages to stderr, and make URL-only upload print to stdout.
- Modify `crates/tus-cli/tests/cli.rs`: update expected stdout/stderr behavior and add URL-only upload coverage.

### Task 1: Update CLI Tests For Output Modes

**Files:**
- Modify: `crates/tus-cli/tests/cli.rs`

- [ ] **Step 1: Update upload create-mode expectations**

Change `upload_prints_created_upload_url_and_metadata` so default upload asserts empty stdout and stderr containing `Upload complete: <url>`, then uses the URL parsed from stderr for the existing metadata assertions.

- [ ] **Step 2: Add URL-only create-mode test**

Add a test that runs `tus upload -o url <file>` with `--endpoint`, expects stdout to contain only the upload URL, and expects stderr to be empty.

- [ ] **Step 3: Update existing-upload expectations**

For existing-upload tests, default mode should assert empty stdout plus stderr containing `Uploading to <url>` and `Upload complete: <url>`. Tests that need the URL can use the pre-created upload URL.

- [ ] **Step 4: Add URL-only existing-upload test**

Add a test that runs `tus upload -o url <file> <upload-url>`, expects stdout to contain only the upload URL, and expects stderr to be empty.

- [ ] **Step 5: Update terminate expectations**

Change terminate tests to assert stdout is empty and stderr is exactly `Upload terminated\n`.

- [ ] **Step 6: Run targeted tests and verify RED**

Run: `cargo test -p tus-cli --test cli upload_ terminate_`

Expected: FAIL because the implementation still prints upload URLs to stdout and terminate currently prints the URL to stdout.

### Task 2: Implement CLI Output Modes

**Files:**
- Modify: `crates/tus-cli/src/main.rs`

- [ ] **Step 1: Add upload output enum and command option**

Add an `UploadOutputFormat` enum with `Human` and `Url`, and add `-o, --output <human|url>` to the `upload` subcommand with default `human`.

- [ ] **Step 2: Route upload output through helpers**

Pass the upload output format into upload execution. In human mode, emit status to stderr; in URL mode, emit only the URL to stdout.

- [ ] **Step 3: Suppress progress in URL-only mode**

Ensure `upload -o url` does not show progress even when stderr is interactive and `--no-progress` was not passed.

- [ ] **Step 4: Change terminate output**

Remove URL stdout printing from terminate and emit `Upload terminated` to stderr after a successful terminate.

- [ ] **Step 5: Run targeted tests and verify GREEN**

Run: `cargo test -p tus-cli --test cli upload_ terminate_`

Expected: PASS for upload and terminate CLI tests.

- [ ] **Step 6: Run full workspace tests**

Run: `cargo test`

Expected: PASS.

## Self-Review

- Spec coverage: The plan covers human stderr defaults, URL-only upload stdout, terminate stderr-only output, no shared output option, and unchanged info output.
- Placeholder scan: No placeholders or deferred implementation details remain.
- Type consistency: `OutputFormat` remains for info; `UploadOutputFormat` is upload-specific.
