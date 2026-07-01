# Optional upload inventory seam

Status: Accepted

## Context

The TUS protocol lifecycle needs upload state persistence for create, lookup,
update, delete, and expired upload reclamation. Full upload listing is useful for
administration, debugging, inspection, and tooling, but protocol workflows only
require listing expired uploads for reclamation.

Keeping full listing on the required `StateStore` trait forces upload-only
adapters to implement operational enumeration even when they only need protocol
lifecycle behavior.

## Decision

`StateStore` remains the required protocol upload-state seam. It includes
expiration listing for reclamation, but not full upload inventory.

`UploadInventory` is the optional operational seam for adapters that can
enumerate upload IDs. It mirrors `StorageReader` as a sibling optional trait:
callers that need both state lookup and inventory require both `StateStore` and
`UploadInventory`. First-party state stores implement both traits. Inventory
returns upload IDs only, includes every known persisted upload ID, and pages IDs
in deterministic upload-ID order. Pagination is not a multi-call snapshot:
concurrent creates or deletes may affect later pages.

Issue #58 does not introduce a new server/admin HTTP endpoint. Future admin or
debug APIs can require `UploadInventory` when they need operational upload
enumeration.

## Consequences

Protocol APIs and shared protocol workflow tests must not depend on full upload
inventory. Inventory behavior is covered separately from required `StateStore`
conformance.

Removing `StateStore::list` is an intentional breaking public trait change that
keeps the required adapter seam focused on protocol lifecycle behavior.
