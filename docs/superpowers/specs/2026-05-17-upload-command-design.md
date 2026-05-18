# Upload Command Design

## Goal

Add a `tus upload` command that either creates a new upload from configured endpoint defaults or uploads to an existing upload resource when a URL is supplied. Remove the standalone `tus resume` command without replacing it with a separate resume flag.

## Scope

- Add a new `upload` CLI subcommand with file path, optional upload URL, and repeated `--metadata KEY=VALUE` options.
- When `upload` receives no URL, create a new upload using the configured collection `endpoint` and call `Client::upload_from_with_progress` or `Client::upload_from`.
- When `upload` receives a URL, treat it as an existing upload resource reference, resolve it through `Client::upload`, and call `Upload::upload_with_progress` or `Upload::upload`.
- Reject `--metadata` when `upload` receives an existing upload URL because metadata cannot be changed while PATCHing an existing upload.
- Remove the standalone `resume` CLI subcommand.
- Keep the existing `put` subcommand unchanged: its optional URL remains a collection endpoint override and it creates a new upload.
- Update CLI tests and user-facing CLI documentation to show `upload` while preserving existing `put` behavior.

## Non-Goals

- Do not add parallel upload flags in this change.
- Do not change the `tus-client` parallel upload API in this change.
- Do not remove, deprecate, or change the existing `put` command semantics.
- Do not add an explicit resume flag; `tus upload <file> <upload-url>` is the existing-upload resume path.

## Parallel Upload Decision

`Client::upload_parallel` currently has no progress callback. A parallel CLI flag without progress would be inconsistent with the normal upload command, while useful progress reporting would require client API changes. Parallel upload remains a future design topic.

## Behavior

`tus upload <file> [upload-url] [--metadata KEY=VALUE]` has two modes. Without `upload-url`, it requires configured `endpoint`, creates a new upload, sends the full file, prints the upload URL on stdout, and keeps progress output on stderr when enabled. With `upload-url`, it resolves an existing upload resource reference using the configured endpoint rules, rejects metadata options, uploads/resumes the file to that resource, prints the resolved upload URL, and keeps progress output on stderr when enabled. The old `tus resume <upload-url> <file>` command is removed rather than retained as an alias.

`tus put <file> [collection-url] [--metadata KEY=VALUE]` remains compatible with the existing behavior: explicit URL is a collection endpoint override, otherwise configured `endpoint` is used, and a new upload is created.

## Verification

- Add tests proving `upload` without URL creates a new upload from the configured endpoint and accepts `--metadata KEY=VALUE`; verify metadata by reading the created upload state.
- Add tests proving `upload` with URL resolves an existing upload resource and resumes/uploads to it.
- Add tests proving `upload` rejects `--metadata` when URL is present.
- Add tests proving `upload --resume <file> <upload-url>` fails because `--resume` is not a supported option.
- Remove tests for the standalone `resume` command or update them to use `upload <file> <upload-url>`.
- Keep existing `put` tests passing.
- Run formatting and CLI tests.
