# Optional storage read seam

Status: Accepted

## Context

The tus protocol lifecycle needs storage for create, append, concatenate, size,
delete, recovery, and expired upload reclamation. Downloading completed uploads
with `GET` is a server convenience path, not part of the core tus protocol.

Keeping full-stream and range-read methods on the required `Storage` interface
forces upload-only adapters to implement behavior they do not need for protocol
compliance.

## Decision

`Storage` remains the required upload lifecycle seam. It does not include read
or range-read methods.

`StorageReader` is the optional read seam for adapters that expose stored bytes
through non-standard download or inspection paths. First-party memory, file, and
OpenDAL storage implement both `Storage` and `StorageReader`.

The default Axum `create_router` mounts only standard upload routes. The
non-standard GET route is available through `create_router_with_download`, which
requires storage that implements `StorageReader`. The standalone `tus-server`
uses the download-enabled router to preserve its documented convenience download
behavior.

## Consequences

TUS protocol compliance is unchanged because the required upload lifecycle still
depends only on `Storage`, `StateStore`, `Locker`, and `HookExecutor`.

Upload-only adapters can implement `Storage` and use the default Axum router
without providing full-stream or range-read behavior.

Download-capable adapters keep the existing full download and single-range
download behavior by implementing `StorageReader`.
