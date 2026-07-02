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

### Planned final upload

A final upload whose ordered partial uploads have been accepted as a dependency list, but whose content is not yet deliverable. It is an intermediate upload resource: it does not retain referenced partial uploads and is no longer available once any referenced partial upload becomes unavailable or protocol-expired. Its availability deadline is capped by the earliest referenced partial upload deadline.

### Protocol expiration

The TUS lifecycle rule that makes an unfinished or intermediate upload unavailable after its advertised deadline. It is a resumable-upload deadline, not a retention policy for completed deliverable content.

### Protocol upload state

The facts owned by TUS lifecycle rules: upload ID, offset, length, expiration, concatenation role and parts, creation time, and user-provided upload metadata.

### Storage-owned facts

The locator and backend-specific bookkeeping needed by a Storage adapter to find, append, concatenate, size, delete, recover, or clean up upload bytes. These facts are persisted as an opaque `StorageHandle` with upload state, but protocol lifecycle code does not interpret them.

### Upload completion

The lifecycle point where accepted upload bytes reach the declared upload length and the upload becomes complete.

### Upload inventory

The optional operational view that enumerates all known upload IDs for administration, debugging, inspection, or tooling, including uploads that protocol requests may reject until reclamation removes them. This is distinct from protocol upload state lookup and expired upload reclamation.
