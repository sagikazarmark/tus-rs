# Adapter Implementation Guide

Adapters should be tested through the interface they implement. Do not assert on
provider-internal tables, object names, lock rows, or helper calls when the same
behavior can be observed through the public seam.

## Storage Adapters

Implement `tus_protocol::Storage` when a backend stores upload bytes for the TUS
lifecycle.

Required behavior:

- `create` returns a non-empty opaque `StorageHandle` that can be passed back to
  the same adapter later.
- `append` accepts bytes only at the requested expected offset.
- `append` returns the complete current `StorageHandle`; it must preserve any
  still-valid storage-owned facts from the request handle.
- Failed appends do not expose partially accepted bytes at a PATCH boundary.
- `size` reports the bytes the backend has accepted, including after a crash
  where an updated handle or upload offset was not persisted.
- `concat` copies or links part handles into the target in the order supplied.
- Failed `concat` does not expose a partially concatenated target.
- `delete` is idempotent and removes unfinished staging data as well as completed
  data for the handle.

Only the storage adapter should interpret `StorageHandle::key` or internal handle
values. Protocol code, framework adapters, hooks, and application code should
not derive protocol behavior from those storage-owned facts.

Optional read behavior belongs behind `StorageReader`:

- `stream` returns all stored bytes for a readable completed upload.
- `stream_range` returns the requested byte range and may use native provider range
  reads when available.

Conformance checklist:

- Enable `tus-protocol` with `conformance-storage` in dev-dependencies.
- Run `tus_protocol::storage::conformance::assert_upload_write_semantics` for
  upload-only storage adapters.
- Run `tus_protocol::storage::conformance::assert_full_semantics` for adapters
  that also implement `StorageReader`.
- Isolate the backend namespace used by conformance tests so old objects cannot
  affect size, delete, or range assertions.
- Add provider-specific tests only for behavior not covered by the shared suite,
  such as credential configuration, path escaping, object-store capability
  errors, or recovery from provider-specific partial failures.

## State Store Adapters

Implement `tus_protocol::StateStore` when a backend persists protocol upload
state.

Required behavior:

- `set(state, true)` creates a new upload state and rejects duplicate upload IDs
  atomically when the backend supports conditional writes.
- `set(state, false)` updates an existing upload state snapshot.
- `get` returns a snapshot; mutating the returned `UploadState` must not mutate
  persisted state until `set` is called again.
- `delete` is idempotent.
- `list_expired` returns upload IDs whose protocol expiration deadline is before
  the cutoff and whose lifecycle role is eligible for TUS expiration.
- Unsafe upload IDs are rejected before they become backend keys or path
  components.
- Protocol upload state and opaque `StorageHandle` facts round-trip together.

Optional inventory behavior belongs behind `UploadInventory`:

- `list_upload_ids` returns a deterministic page of known persisted upload IDs
  for operational tooling.
- All known upload IDs are available across pages through the `limit` and
  `offset` parameters.
- IDs are returned in deterministic upload-ID order for a call.
- Pagination is not a multi-call snapshot unless the adapter explicitly provides
  that stronger guarantee.

Conformance checklist:

- Enable `tus-protocol` with `conformance-state` in dev-dependencies.
- Run `tus_protocol::state::conformance::assert_state_store_semantics` for every
  `StateStore` adapter.
- Run `tus_protocol::state::conformance::assert_upload_inventory_semantics` when
  the adapter implements `UploadInventory`.
- Use an isolated empty backend namespace for inventory tests because inventory
  intentionally lists all known upload IDs.
- Add provider-specific tests only for behavior outside the seam, such as file
  permissions, serialization migrations, conditional-write mapping, or database
  connection configuration.

## Locker Adapters

Implement `tus_protocol::Locker` when a backend coordinates concurrent access to
an upload ID.

Required behavior:

- Only one caller can hold a lock for an upload ID at a time.
- `try_lock` returns `None` when the upload ID is already locked.
- `lock` waits for release until the requested timeout and returns
  `Error::LockTimeout` on timeout.
- A dropped `LockGuard` either releases the lock promptly or the backend lease
  expires within a documented bounded duration.
- A lock stays held for as long as its `LockGuard` lives; a PATCH can hold its
  guard for the entire streamed request body, so backends must never expire a
  held lock on a fixed TTL. Lease-based backends must renew while the guard is
  alive.
- Independent upload IDs do not block each other.

Conformance checklist:

- Enable `tus-protocol` with `conformance-lock` in dev-dependencies.
- Run `tus_protocol::locking::conformance::assert_locker_semantics` for lockers
  where dropping the guard releases immediately.
- Run `assert_locker_semantics_with` and configure `ReleaseExpectation` for
  lease-backed lockers that release after a bounded lease timeout.
- Choose conformance timeouts that are stable in CI for the provider.
- Add provider-specific tests for lease renewal, fencing tokens, process death,
  or cleanup mechanics if the adapter exposes those operational guarantees.

## Hook Adapters

Implement `HookExecutor` when a backend executes lifecycle hooks. Implement
`Hook` when you need a stateful hook that can be registered in `HookChain`.

Required behavior for hook executors:

- Pre-hooks run in registration order and stop at the first rejection or error.
- Pre-hook rejections are surfaced as request failures before storage or state
  commits.
- Metadata replacement is honored only at documented mutation points,
  currently `PreCreate` and `PreReceive`.
- Post-hooks run after commit and are best-effort notifications.
- Post-hook errors are logged or reported according to the executor's policy but
  do not fail already-committed requests.
- Hook contexts expose protocol upload state and selected request facts, not
  storage-owned facts.

HTTP hook adapters should also define:

- Transport timeout and retry policy.
- Which responses count as hook execution errors.
- Request headers and signing behavior.
- Whether retries can duplicate hook delivery and how consumers should make
  side effects idempotent.

Conformance checklist:

- Exercise hooks through `Protocol` or a framework router when verifying request
  effects such as rejection status, metadata replacement, response headers, and
  post-commit behavior.
- Test `HookChain` or a custom `HookExecutor` directly for pure ordering and
  event-subscription behavior.
- Use literal expected hook payload fields from the documented hook contract;
  do not snapshot provider-internal request builders.
- Assert that hook-visible uploads omit storage keys and backend-internal handle
  facts.
- For HTTP hooks, use a local HTTP test server at the HTTP seam rather than
  mocking internal transport helpers.

## HTTP And Framework Adapters

Framework adapters translate between a framework's HTTP types and
`tus_protocol` inputs and outputs. They should not reimplement protocol
lifecycle rules.

Required behavior:

- Parse TUS headers into `Headers` and reject invalid wire values through the
  same error mapping a real request would use.
- Validate upload path segments through `UploadId`.
- Convert request bodies and trailers into `RequestBody` without losing checksum
  trailer information.
- Call the matching `Protocol` or `ProtocolHandle` method for `OPTIONS`, `POST`,
  `HEAD`, `PATCH`, and `DELETE`.
- Map `Response` headers, status, and body into the framework response type.
- Map `tus_protocol::Error` into HTTP responses with required TUS headers.
- Treat `X-HTTP-Method-Override` as standard tus core behavior. First-party
  adapters expose the proxy-friendly `POST` fallback for `PATCH` and `DELETE`.
- Keep non-standard download routes behind an explicit `StorageReader` seam.
- Keep framework-specific handler functions internal unless downstream users
  have a real need for that seam.

Axum adapter checklist:

- Use `tus_axum::create_router` for standard TUS routes.
- Use `tus_axum::create_router_with_download` only when storage implements
  `StorageReader` and the application wants non-standard `GET` downloads.
- Exercise router behavior through `tower::ServiceExt::oneshot` or an HTTP
  client against the router, not by calling internal handler functions.
- Cover routing, extractor, body-frame, trailer, CORS, method-override, and error
  conversion behavior at the router seam.

Generic framework adapter checklist:

- Run the same public protocol scenarios through the framework's router or test
  server: create, inspect, append, deferred length, Creation-With-Upload,
  termination, expiration, concatenation, checksum, and hook rejection.
- Include error cases that prove framework-level parsing maps to protocol-level
  failures: missing `Tus-Resumable`, unsupported version, wrong offset, bad
  content type, invalid metadata, unsafe upload ID, and unsupported extension.
- If the adapter supports request cancellation, verify that partially read body
  failures do not commit partially accepted bytes.
- If the adapter supports CORS or method override, test those through real HTTP
  requests at the framework seam.

## Server Adapters And Assemblies

A server assembly chooses concrete adapters and operational policy. It should be
tested through the process, CLI, config, or HTTP interface it exposes rather than
through its private builder helpers.

Standalone server checklist:

- CLI and environment settings resolve in the documented precedence order.
- `serve` builds storage, state, locking, hooks, protocol config, router,
  auth, limits, health endpoints, and shutdown behavior.
- `cleanup` uses the same storage/state configuration and runs one expired
  upload reclamation sweep.
- Expired upload reclamation is explicit: serving rejects protocol-expired
  unfinished/intermediate uploads, while deletion happens only through cleanup.
- Completed-upload retention is documented separately from TUS protocol
  expiration.
