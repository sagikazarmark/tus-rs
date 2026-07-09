# Context

## Glossary

### Body intake

The protocol responsibility of turning request body bytes plus body-related metadata into validated upload bytes before they are accepted for an upload.

### Byte receive

The protocol act of accepting request body bytes for an upload, regardless of whether the bytes arrive through `PATCH` or Creation-With-Upload.

### Completed-upload retention

An operational policy for deleting or hiding completed deliverable upload content after completion. This is distinct from TUS protocol expiration and is not implied by an upload expiration deadline.

### Creation-With-Upload

The TUS extension workflow where a creation request supplies initial upload bytes in the same `POST` request that creates the upload resource.

### Deliverable upload

An upload whose content is complete and ready to be exposed as final application content. Completed partial uploads are not necessarily deliverable because they may still be intermediate material for a final upload.

### Expired upload reclamation

The process of removing upload data and upload state after a protocol-expired upload has expired. This applies to unfinished or intermediate upload resources, not completed deliverable upload content; completed deliverable content requires completed-upload retention. It does not retain or cascade through planned final upload dependencies.

### Final upload materialization

The process of turning a final upload's ordered partial uploads into deliverable upload content. After materialization, the final upload no longer depends on the continued availability of its referenced partial uploads.

### Header provider

A client-side hook that supplies dynamic per-request headers — typically freshly refreshed credentials — recomputed before each request attempt. A header provider failure is a distinct error category from malformed header bytes, and unlike an upload source failure it may be retryable.
_Avoid_: auth hook, interceptor, middleware.

### Planned final upload

A final upload whose ordered partial uploads have been accepted as a dependency list, but whose content is not yet deliverable. It is an intermediate upload resource: it does not retain referenced partial uploads and is no longer available once any referenced partial upload becomes unavailable or protocol-expired. Its availability deadline is capped by the earliest referenced partial upload deadline.

### Protocol expiration

The TUS lifecycle rule that makes an unfinished or intermediate upload unavailable after its advertised deadline. It is a resumable-upload deadline, not a retention policy for completed deliverable content.

### Protocol upload state

The facts owned by TUS lifecycle rules: upload ID, offset, length, expiration, concatenation role and parts, creation time, and user-provided upload metadata.

### Retryable failure

A failure the client classifies as plausibly succeeding on a later attempt (a network hiccup, a transient `5xx`, a refreshable credential), as opposed to a permanent failure that is deterministic and never retried (a torn upload source, an offset desync, a bad request line). The classification is a typed property of the error the retry loop reads, not a per-call-site guess.
_Avoid_: transient error, recoverable error.

### Stored bytes

The byte count a Storage adapter reports through `Storage::size`. Crash recovery adopts this count as the accepted offset and completes an upload once it reaches the declared length, so a backend must report only accepted bytes — never bytes from an append that did not succeed. This is distinct from accepted bytes only when a backend misbehaves; the contract exists to keep them equal.

### Storage-owned facts

The locator and backend-specific bookkeeping needed by a Storage adapter to find, append, concatenate, size, delete, recover, or clean up upload bytes. These facts are persisted as an opaque `StorageHandle` with upload state, but protocol lifecycle code does not interpret them.

### Transport

The client-side seam that executes an already-prepared HTTP request and returns the response, with no knowledge of TUS protocol rules. The default transport is backed by `reqwest`; a middleware transport is a separate type so the default path is never reshaped by an unrelated crate enabling the middleware feature.
_Avoid_: HTTP client, backend, adapter.

### Upload access preparation

The protocol responsibility of reconciling an upload's stored bytes and lifecycle availability before a request observes, downloads, or attempts to modify it. It includes final upload materialization when applicable, but is distinct from Body intake and Byte receive.

### Upload completion

The lifecycle point where accepted upload bytes reach the declared upload length and the upload becomes complete.

### Upload inventory

The optional operational view that enumerates all known upload IDs for administration, debugging, inspection, or tooling, including uploads that protocol requests may reject until reclamation removes them. This is distinct from protocol upload state lookup and expired upload reclamation.

### Upload source

The client-side origin of the bytes being uploaded, read chunk-by-chunk by offset. It is a user-pluggable seam (an in-memory buffer, a file, or a custom implementation); a source that misbehaves — a short or oversized read, or content that changed underneath the client — is a permanent failure, never retried, because the local source is treated as authoritative.
_Avoid_: file, input, stream, data source.
