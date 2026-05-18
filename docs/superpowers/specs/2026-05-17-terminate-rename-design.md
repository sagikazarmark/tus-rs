# Rename Upload Termination API

## Goal

Rename the user-facing upload termination command and client API from `rm` / `delete` to `terminate`.

## Scope

- Replace the CLI subcommand `rm` with `terminate`.
- Rename `Command::Rm` to `Command::Terminate`.
- Rename `Upload::delete()` to `Upload::terminate()`.
- Rename the internal client helper `delete_upload_at` to `terminate_upload_at`.
- Update tests and documentation snippets that reference the CLI command or public client method.

## Non-Goals

- Do not keep deprecated aliases for `rm` or `delete`.
- Do not rename server protocol modules, Axum handlers, storage/state traits, or HTTP method assertions that model HTTP `DELETE` or persistence deletion.

## Behavior

The operation continues to send HTTP `DELETE` and expects `204 No Content`. Only user-facing names and client-side operation labels change.

## Verification

- Run formatting.
- Run targeted CLI and client tests covering termination.
- Run broader workspace tests if feasible.
