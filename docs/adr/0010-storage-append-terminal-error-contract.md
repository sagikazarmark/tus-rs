# Storage append terminal-error contract

Status: Accepted

## Context

A streamed PATCH or Creation-With-Upload body with `Content-Length` is handed to
`Storage::append` as a `ChunkStream::Stream`. Late body validation — a checksum
mismatch or an exact-length shortfall discovered only after the whole body has
drained — is surfaced to the backend as an `Err` item at the end of that stream,
because a checksum cannot be verified until the last byte is seen and buffering
the whole body would defeat streaming.

On such a failure the protocol returns the error and leaves the recorded offset
unchanged. On the next request, crash recovery reconciles the recorded offset
against `Storage::size`: `reconcile_storage_offset` adopts `size()` as the new
accepted offset, and `reconcile_stored_completion` treats `size() == length` as a
completed upload and runs the finish gate. Neither re-validates content, because
the per-request checksum is ephemeral and never persisted.

This is only sound if `Storage::size` reports **accepted** bytes, never bytes
from an append that did not succeed. The hazard is specific: a checksum failure
on a *completing* chunk arrives at full byte count but wrong content, so a backend
that flushes frames as they arrive and then exposes those bytes through `size()`
would have corrupt, full-length content adopted as a completed upload. A partial
write (fewer bytes) is safe to expose — recovery adopts the smaller offset — but a
full-length wrong-content chunk is not.

## Decision

`Storage::size` must report only accepted bytes. Concretely:

- On a terminal stream `Err`, `append` must roll the write back to
  `request.expected_offset` before returning the error, so `size()` reports at
  most `expected_offset`. Leaving the failed bytes in place ("leave enough actual
  size for recovery to reconcile") is **not** permitted for a returned error.
- A completing append's bytes must not become `size()`-visible at the upload
  `length` until `append` has returned `Ok`. A backend that finalizes
  incrementally must stage the completing write and expose it only once it has
  fully succeeded.
- "Report actual size for recovery" applies only to a genuine crash of a
  *partial* write (a process that died mid-append and never returned), and even
  then only reports the smaller, partial byte count — never full-length content.

This invariant is already enforced by the storage conformance suite
(`storage::conformance`), which asserts that a failed body stream leaves the
pre-append size visible, and is satisfied by every first-party backend. This ADR
records the contract and aligns the `Storage` trait docs with it.

## Considered Options

**Buffer checksummed bodies before append (rejected).** Validating the whole
chunk in memory before handing pre-validated `Buffered` bytes to the backend
would close the hazard purely protocol-side, with no backend cooperation. It was
rejected because it regresses the constant-memory streaming of large checksummed
chunks — the capability the streaming path and the OpenDAL backend exist to
provide — penalizing a correct backend to defend against a hypothetical incorrect
one.

**Persist a protocol-side "append in flight" marker (rejected).** Recording
intent before `append` and refusing recovery-completion unless it cleared cannot
work: the legitimate recovery case (crash *after* `append` returned `Ok`, before
the offset persisted) and the dangerous case (crash *before* `Ok`) are
indistinguishable from any marker written before the call — both leave "intent
set, offset not advanced, `size() == length`." Only the backend knows whether it
accepted the bytes, so the guarantee must live in the backend.

## Consequences

The correctness of streamed checksummed uploads depends on backend cooperation,
which is why the contract is expressed as an enforceable conformance test rather
than trusted prose. OpenDAL is the reference for a zero-window backend: it
discards the staged part on any terminal error and promotes the completing part
atomically behind a durable completion marker, so `size()` never reflects an
un-accepted completing chunk even across a crash.

The `FileStorage` backend satisfies the terminal-error contract (it truncates
back to `expected_offset` on error) but retains a narrow residual crash window:
it appends the completing chunk directly to the main file, so a process crash
after the operating system flushes the final bytes but before `append` returns
could leave a full-length file that recovery adopts. Closing it would require a
per-completing-PATCH full-file copy or restructuring `FileStorage` into a
parts-staging model, which is disproportionate for a page-cache-timing-dependent
window on the local/development backend. It is documented as a known limitation;
deployments that need a zero-window guarantee for streamed checksummed uploads
should use a staging backend such as OpenDAL.
