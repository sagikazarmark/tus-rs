# Dagger E2E Test Design

## Goal

Add a simple Dagger e2e check for `tus-cli` that proves the built `tus` binary can upload a local file to a real `tusd` server.

## Scope

- Add an `e2e` check function to `dagger.dang`.
- Start the official `tusproject/tusd` container as a Dagger service.
- Configure `tusd` with file-backed upload storage under `/srv/tusd/data`.
- Mount the locally built `tus` binary into a runner container.
- Create a small test file in the runner and run `tus --endpoint http://tusd:1080/files/ --no-progress upload /tmp/input.txt`.
- Treat command success as the test assertion.

## Non-Goals

- Do not inspect tusd's storage layout or verify uploaded bytes in this first smoke test.
- Do not replace existing Rust unit or integration tests.
- Do not add CI workflow changes in this task.
- Do not run this repository's Axum example server; the e2e target is the official `tusd` service.

## Behavior

The Dagger check builds the `tus` CLI through the existing Rust module, starts a `tusd` service with file storage enabled, and runs one upload command against the service's `/files/` collection endpoint. The test passes when the CLI exits successfully and fails if the service cannot start, the endpoint is wrong, or the upload command returns a non-zero exit status.

## Implementation Notes

- Keep `dagger.dang` minimal by adding helper fields only if the Dagger service setup would otherwise be hard to read.
- Use a generic Linux runner container for the CLI invocation and mount the `tus` binary at a stable path such as `/usr/local/bin/tus`.
- Bind the Dagger service with alias `tusd` so the CLI can use `http://tusd:1080/files/`.
- Prefer `--no-progress` to keep check output deterministic.

## Verification

- Run the new Dagger e2e check.
- If Dagger syntax or runtime behavior fails, fix `dagger.dang` and rerun the check.
