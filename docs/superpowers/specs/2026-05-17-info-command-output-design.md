# Info Command Output Design

## Goal

Rename the CLI upload inspection command from `head` to `info` and add selectable output formatting for upload information.

## Scope

- Replace `tus head <upload-url>` with `tus info <upload-url>`.
- Remove the old `head` subcommand instead of keeping it as an alias.
- Add a command-local `-o, --output <human|json>` option to `info`.
- Keep human output as the default format.
- Preserve relative upload URL support through configured `endpoint`.
- Update CLI tests and user-facing CLI documentation.

## Non-Goals

- Do not add a global output format flag for all commands.
- Do not change output behavior for `upload`, `put`, `terminate`, or `cat`.
- Do not keep a compatibility alias for `head`.

## Behavior

`tus info <upload-url>` fetches upload information through the existing upload resource resolution path and prints human-readable output by default:

```text
url: http://127.0.0.1:8080/files/upload-id
offset: 0
length: 12
metadata:
filename=video.mp4
```

Metadata entries remain sorted by key in human output to keep output deterministic. Empty metadata prints the `metadata:` heading with no entries. Deferred length prints `length: deferred`.

`tus info -o json <upload-url>` prints a single JSON object to stdout:

```json
{
  "url": "http://127.0.0.1:8080/files/upload-id",
  "offset": 0,
  "length": 12,
  "metadata": {
    "filename": "video.mp4"
  }
}
```

JSON output uses `null` for deferred length and `{}` for empty metadata. Metadata values are rendered as strings using the same lossy UTF-8 conversion used by current human output. Metadata keys are sorted before JSON serialization so output is deterministic.

Unsupported output formats fail during CLI parsing with clap's normal invalid value error.

## Implementation Notes

- Rename `Command::Head` to `Command::Info` in `crates/tus-cli/src/main.rs`.
- Introduce a small output format enum for `human` and `json`.
- Keep formatting logic local to the CLI binary; the client API is unchanged.
- Add `serde_json` to `crates/tus-cli/Cargo.toml` because the workspace currently exposes `serde` but not `serde_json`.

## Verification

- Add tests proving `info` accepts relative upload URLs with `--endpoint`.
- Update human-output tests from `head` to `info` and keep their expected output unchanged.
- Add JSON-output tests for finite length, sorted/deterministic metadata object content, deferred length as `null`, and empty metadata as `{}`.
- Add a test proving `head` is no longer accepted.
- Add a test proving invalid `-o` values fail.
- Keep existing upload, put, terminate, and cat tests passing.
- Run formatting and the full workspace test suite.
