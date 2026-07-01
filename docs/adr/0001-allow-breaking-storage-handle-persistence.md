# Storage-owned locator and metadata persistence

Status: Accepted

## Context

Protocol lifecycle code needs durable upload state to answer HEAD requests, validate PATCH offsets, run hooks, concatenate partial uploads, delete uploads, recover after crashes, and reclaim expired uploads. Storage adapters also need durable facts that are not protocol facts: object keys, file names, multipart cursors, staging prefixes, temporary materialization state, or provider-specific upload IDs.

Those storage-owned facts need persistence, but exposing a separate public `StorageMetadataStore` seam would make the interface shallower unless multiple real adapters need the same independent adapter slot. The current adapters do not: their storage-owned facts are only meaningful to the adapter that produced them.

## Decision

`UploadState` owns protocol upload state:

- upload ID
- current protocol offset
- declared or deferred length
- creation and expiration timestamps
- concatenation role and referenced part IDs
- user-provided `Upload-Metadata`

Storage adapters own storage facts. Those facts travel as a `StorageHandle` returned by `Storage::create`, `Storage::append`, and `Storage::concat`. Lifecycle code persists the handle with `UploadState`, reloads it, and passes it back to the same storage adapter, but does not interpret storage-owned facts.

`StorageHandle` is the public interface for storage-owned locator and metadata persistence. It contains an opaque key plus adapter-owned internal string facts. The state store persists the complete handle snapshot as part of upload state; the storage adapter is responsible for deciding which internal facts are authoritative and which are cache or cursor hints.

Do not add a public `StorageMetadataStore` seam now. Add one only if at least two real adapters need the same separate metadata adapter slot, independent of their `Storage` implementation.

Hook snapshots and protocol response facts remain protocol-level. They must not expose storage keys or storage-owned metadata.

## Adapter Strategies

Memory storage uses `memory://{upload_id}` as the handle key. Upload bytes live in the in-process map under that key. It does not persist extra internal handle facts; process restart loses both bytes and handle usefulness.

File storage creates a generated single-component `{uuid}.upload` key under the configured root directory. The key is validated before path construction so persisted handles cannot escape the root. The file length is the authoritative size. It does not persist extra internal handle facts. Concatenation writes a temporary file and promotes it over the target only after all parts are copied.

OpenDAL storage uses `{prefix}/{upload_id}` as the main object key. PATCH bodies are staged under `{key}.parts/{part-number}` until the upload completes; completion materializes the main object through a temporary key and then cleans staging objects. The handle persists `opendal_next_part` as a cursor hint. Recovery does not trust the cursor as authoritative: `size` and the next append inspect staged objects and main-object metadata so stale persisted handles after crashes do not overwrite accepted parts.

## Operation Access

Create asks `Storage::create` for a handle and persists that handle with the new upload state.

Append reloads the persisted handle from `UploadState`, passes it to `Storage::append`, then persists the returned complete handle and advances the protocol offset only after storage accepts the body.

Size and recovery reload the persisted handle and ask `Storage::size`. Storage is responsible for reconstructing authoritative size from its own backend facts, including when a previously returned handle update was not persisted before a crash.

Concatenation reloads the final target handle and each partial upload handle from their states, passes them to `Storage::concat`, then persists the returned target handle with the final upload state.

Delete and expired upload reclamation reload the persisted handle and call `Storage::delete` before deleting state. Storage deletion is idempotent and should clean unfinished staging data, temporary materializations, and completed data for that handle.

Final upload repair reloads the final upload handle and part handles from state. If complete parts can be materialized and the target size is missing or stale, lifecycle calls `Storage::concat` again and persists the returned target handle.

## Consequences

The persisted upload state contains storage-owned facts, but only as an opaque handle snapshot. This preserves locality for adapter-specific recovery and cleanup logic while keeping protocol lifecycle code independent of object-store, filesystem, or memory-storage details.

Changing the serialized handle shape remains a breaking persisted-state change. We accept that compatibility break if a future representation better preserves the same seam: protocol facts stay in `UploadState`, storage facts stay behind `StorageHandle`, and no generic storage metadata store is introduced without real adapter pressure.
