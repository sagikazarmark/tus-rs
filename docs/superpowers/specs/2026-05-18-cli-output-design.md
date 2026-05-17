# CLI Output Design

## Goal

Make `tus upload` and `tus terminate` default to human-oriented status messages on stderr while preserving a URL-only upload mode for scripts.

## Scope

- Keep `tus info` output behavior unchanged: human and JSON data go to stdout.
- Change default `tus upload` output to write human status to stderr and no data to stdout.
- Add `tus upload -o url` / `tus upload --output url` to print only the final upload URL to stdout.
- Make `tus upload -o url` suppress progress and other stderr status output.
- Change `tus terminate` to write `Upload terminated` to stderr and no data to stdout.
- Do not add an output option to `terminate`.

## Behavior

`tus upload <file>` creates an upload and, after success, writes `Upload complete: <url>` to stderr. Existing progress behavior remains enabled only when stderr is interactive and `--no-progress` is not set. Stdout stays empty.

`tus upload <file> <upload-url>` resolves the upload URL, writes `Uploading to <resolved-url>` to stderr before uploading, and writes `Upload complete: <resolved-url>` to stderr after success. Stdout stays empty.

`tus upload -o url <file> [upload-url]` is the scripting mode. It disables progress and status messages and writes only `<resolved-url>\n` to stdout after success.

`tus terminate <upload-url>` resolves and terminates the upload, writes `Upload terminated` to stderr after success, and leaves stdout empty.

## Non-Goals

- Do not make `--output` a global or shared option.
- Do not add JSON output for `upload` or `terminate` in this change.
- Do not change `info` output behavior.
- Do not change protocol/client behavior.

## Verification

- Add/adjust CLI tests for default upload human stderr output and empty stdout.
- Add CLI tests for `upload -o url` stdout-only behavior.
- Adjust terminate tests to expect stderr status and empty stdout.
- Run targeted `tus-cli` tests and the full workspace test suite.
