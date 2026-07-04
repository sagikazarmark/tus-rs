# Architecture

This repository is organized around a small set of deep modules. The public
interfaces are the seams that server authors, framework adapter authors, and
backend adapter authors should depend on. Protocol lifecycle details stay behind
those interfaces unless a real adapter need justifies a new seam.

## Module Map

| Module | Interface | Implementation owns |
|--------|-----------|---------------------|
| `tus-protocol` | `Protocol`, `ProtocolHandle`, typed request/response values, backend traits, hooks, upload state, and expired upload reclamation | TUS lifecycle ordering, extension rules, body intake, byte receive, final upload materialization, recovery, hook timing, and protocol response facts. |
| `tus-axum` | `create_router`, `create_router_with_download`, `TusState`, extractors, `Error`, and CORS helper | Axum routing, extraction, body-frame conversion, error mapping, and response conversion. |
| `tus-server` | `tus-server serve` and `tus-server cleanup` | Standalone server assembly, config loading, OpenDAL storage wiring, file-backed state, in-process locking, optional HTTP hooks, health endpoints, auth, request limits, shutdown, and expired upload reclamation scheduling. |
| Backend adapters | `Storage`, `StorageReader`, `StateStore`, `UploadInventory`, `Locker`, `HookExecutor`, and `Hook` | Provider-specific persistence, object keys, leases, retries, and operational details. |
| Clients and CLI | `tus-client` and the `tus` binary | Client-side creation, resume, PATCH chunking, metadata, and termination workflows. |

`tus_protocol::Protocol` and `tus_protocol::ProtocolHandle` are the public
request-handling interface. Lower-level lifecycle helpers are internal
implementation. `tus_axum::create_router` and
`tus_axum::create_router_with_download` are the public Axum adapter interface;
handler functions are internal wiring.

## Protocol Core

`tus-protocol` is framework-neutral. It accepts typed inputs that an HTTP
adapter has already parsed:

- `Headers` for TUS request headers.
- `UploadId` for validated upload resource identifiers.
- `RequestBody` or `PatchBody` for body intake.

It returns framework-neutral `Response` values or `Error` values. Adapters map
those results back to their HTTP framework.

The protocol implementation owns the lifecycle rules:

- Creation creates storage, persists protocol upload state, and emits creation
  hooks.
- Creation-With-Upload runs body intake and byte receive in the initial `POST`.
- `HEAD` returns protocol upload state and reconciles storage size when needed.
- `PATCH` validates offset and body headers, accepts bytes, advances protocol
  upload state, and detects upload completion.
- `DELETE` terminates upload state and asks storage to delete upload bytes.
- Final upload materialization turns complete partial uploads into deliverable
  upload content.
- Expired upload reclamation removes protocol-expired unfinished or intermediate
  uploads through an explicit operational interface.

Protocol upload state means the facts owned by TUS lifecycle rules: upload ID,
offset, length, expiration, concatenation role and parts, creation time, and
user-provided upload metadata. Storage-owned facts remain behind `StorageHandle`.

## Backend Seams

### Storage

`Storage` is the required upload-byte seam. It creates upload storage, appends
bytes, concatenates parts, reports size for recovery, and deletes upload data.
`StorageHandle` carries storage-owned facts as an opaque snapshot persisted with
`UploadState`. Protocol code stores and passes the handle back to the same
storage adapter, but does not interpret handle internals.

`StorageReader` is optional. It exists for non-standard download or inspection
paths. Upload-only adapters do not need to implement it.

### State Store

`StateStore` is the required protocol upload-state seam. It persists
`UploadState` snapshots, looks them up by upload ID, deletes them, and lists
protocol-expired upload IDs for reclamation.

`UploadInventory` is optional. It lists all known upload IDs for operational
tooling and is not required by request handling.

### Locking

`Locker` coordinates access to a single upload ID. Protocol handlers use it so
concurrent requests do not accept conflicting byte ranges or lifecycle
transitions for the same upload.

The no-op locker is valid only when the host environment already serializes
requests for each upload ID or in tests. Native multi-request servers should use
a real locker.

### Hooks

`HookExecutor` runs lifecycle hooks. `HookChain` and `Hook` are the built-in
composition interface. Hooks receive `HookContext` values containing
protocol-level `HookUpload` snapshots. Hook contexts intentionally omit storage
keys and backend-internal storage metadata.

Pre-hooks are gates. `PreCreate`, `PreReceive`, and `PreTerminate` may add
response headers. `PreCreate` and `PreReceive` may replace user metadata before
commit. `PreFinish` is gate-only. Post-hooks are best-effort notifications after
commit; post-hook failures do not fail an already-committed request.

## Framework Adapters

Framework adapters sit between HTTP framework types and `tus-protocol` typed
inputs. They should keep their interface small:

- Parse headers into `tus_protocol::Headers`.
- Validate path parameters as `tus_protocol::UploadId`.
- Convert request bodies into `tus_protocol::RequestBody`.
- Call the matching `Protocol` or `ProtocolHandle` method.
- Convert `Response` and `Error` into framework responses.

`tus-axum` is the first-party example. It exposes router construction and a
small set of support types. Its handlers remain implementation details so route
assembly, extractor choices, and response conversion can change without forcing
callers to learn that implementation.

## Server Assembly

`tus-server serve` assembles the native standalone server:

1. Load settings from defaults, config file, environment, and CLI flags.
2. Build `tus_protocol::Config` for protocol extensions, upload limits,
   expiration, base path, base URL, CORS, and download behavior.
3. Build an OpenDAL storage adapter from `--storage-uri` / `TUS_STORAGE_URI`
   plus storage settings.
4. Build a file-backed `StateStore` under `--state-dir`.
5. Build an in-memory `Locker` for process-local upload coordination.
6. Build a `HookExecutor`, using the HTTP hook adapter when hook settings are
   configured.
7. Wrap the pieces in `ProtocolHandle` and `TusState`.
8. Build the Axum router with `create_router_with_download`; protocol config
   controls whether the non-standard `GET` download path is allowed.
9. Add server concerns around the TUS router: bearer-token auth, request-body
   limits, request-body idle timeout, `/healthz`, `/readyz`, tracing, and
   graceful shutdown.

`tus-server cleanup` assembles the same storage and state adapters, runs one
expired upload reclamation sweep, reports outcomes, and exits. It is an
operational cleanup path, not a request handler.

Object storage only owns uploaded bytes. In the current standalone server,
upload state remains file-backed under `--state-dir`, and locking remains
process-local through the in-memory locker.

## Lifecycle Workflows

### Create Upload

1. The framework adapter parses the `POST` request and calls `Protocol::post`.
2. Protocol validates creation headers, configured extensions, upload length,
   concatenation role, metadata, and limits.
3. Pre-create hooks can reject the operation or replace user metadata.
4. Storage creates upload bytes and returns a `StorageHandle`.
5. State store persists `UploadState` with protocol upload state and the opaque
   storage handle snapshot.
6. Post-create hooks run as best-effort notifications.
7. The adapter returns `201 Created` with the upload resource `Location`.

### Receive Bytes

1. The adapter parses `PATCH` or Creation-With-Upload body inputs and calls the
   protocol facade.
2. Protocol locks the upload ID and loads `UploadState`.
3. Protocol rejects missing, expired, final, or offset-conflicting uploads before
   accepting bytes.
4. Pre-receive hooks gate body intake.
5. Body intake validates body metadata such as content type, content length, and
   checksum headers or trailers.
6. Storage appends bytes at the expected offset and returns the complete current
   `StorageHandle`.
7. Protocol advances the offset, persists the updated `UploadState`, and runs
   post-receive hooks.
8. If accepted bytes reach the declared length, upload completion triggers
   pre-finish and post-finish hooks around the completing commit.

### Inspect Upload

1. The adapter parses `HEAD /{upload_id}` and calls `Protocol::head`.
2. Protocol loads state, checks protocol expiration, and reconciles storage size
   when state may be stale after a crash.
3. Planned final uploads may be materialized or repaired when all referenced
   partial uploads are complete.
4. The response advertises offset, length or deferred length, expiration,
   metadata, and concatenation facts.

Completed deliverable uploads do not expire through TUS protocol expiration.
Completed partial uploads and planned final uploads remain intermediate upload
resources and can expire.

### Terminate Upload

1. The adapter parses `DELETE /{upload_id}` and calls `Protocol::delete`.
2. Protocol checks the Termination extension, locks the upload ID, and loads
   state.
3. Pre-terminate hooks can reject the operation.
4. Protocol asks storage to delete upload bytes first. Storage deletion is
   expected to be idempotent; DELETE logs and ignores storage deletion errors so
   state deletion can still remove otherwise undeletable upload IDs.
5. Protocol deletes upload state.
6. Post-terminate hooks run as best-effort notifications.

### Final Upload Materialization

1. A final upload references ordered partial upload resources.
2. Protocol validates that referenced partial uploads exist, are not expired,
   and are complete before materialization.
3. Storage concatenates part handles in order into the final upload handle.
4. Protocol marks the final upload complete and persists the materialized final
   state.
5. After materialization, the final upload is deliverable upload content and no
   longer depends on continued availability of referenced partial uploads.

Planned final uploads from the non-standard `concatenation-unfinished` workflow
are not deliverable content. They become unavailable if any referenced partial
upload is missing or protocol-expired, and their advertised protocol expiration
is capped by the earliest referenced partial upload deadline.

### Expired Upload Reclamation

1. An operator calls `reclaim_expired_uploads`, starts `tus-server serve
   --cleanup`, or runs `tus-server cleanup`.
2. The state store lists protocol-expired upload IDs.
3. For each listed upload, protocol tries to lock the upload ID and skips locked
   candidates.
4. After locking, protocol reloads state, reconciles stored completion, and
   rechecks expiration because the listed candidate may have changed.
5. If the upload is still expired, protocol asks storage to delete upload bytes,
   then deletes state.
6. The report records removed uploads, skipped uploads, and failures.

Expired upload reclamation applies to unfinished or intermediate upload
resources. Completed-upload retention is a separate operational policy for
completed deliverable content.
