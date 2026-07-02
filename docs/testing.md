# Testing Guide

Tests should verify behavior through public interfaces. In this repository,
those interfaces are also the architecture seams: helpers, `Protocol`, framework
routers, server commands, and adapter conformance suites.

## Choosing The Seam

| Behavior | Test seam | Why |
|----------|-----------|-----|
| Pure parsing or formatting with no lifecycle dependency | Helper or value type | Keeps examples small when the behavior is fully described by one public type. |
| TUS lifecycle rules | `tus_protocol::Protocol` or `ProtocolHandle` | Exercises storage, state, locking, hooks, recovery, and response facts without framework noise. |
| HTTP extraction, routing, body frames, CORS, method override, and error mapping | Framework router such as `tus_axum::create_router` | Proves the framework adapter translates real HTTP requests into protocol calls. |
| Standalone process behavior, CLI flags, config precedence, auth, health, cleanup, shutdown | `tus-server` command/config/server seam | Proves the assembled server behaves as operators use it. |
| Storage, state, and locking provider contracts | Adapter conformance helpers | Lets every adapter prove the same behavior through the trait it implements. |
| Provider-specific operational behavior | Provider adapter public interface | Covers details not shared by all adapters, without leaking them into protocol tests. |

Prefer the highest seam that still isolates the behavior. A lifecycle rule should
not need an Axum request unless the behavior depends on Axum extraction or HTTP
wire mapping. A storage adapter's crash-recovery size behavior should not need a
full TUS request unless the bug is in protocol recovery.

## Helper And Value Tests

Use helper-level tests for small public types and functions whose behavior is
complete without storage or HTTP context:

- `UploadId` validation accepts safe upload IDs and rejects path traversal,
  separators, empty strings, and control characters.
- `Headers` parsing turns known wire examples into typed values.
- Metadata values preserve bytes and serialize known-good examples.
- Expiration header formatting matches RFC 7231 examples.

Expected values should be independent literals from the spec or a worked
example. Do not recompute the expected value with the same code path under test.

Avoid helper tests for private lifecycle steps. If renaming or rearranging an
internal helper breaks a test while public behavior is unchanged, the test is at
the wrong seam.

## Protocol Tests

Use `Protocol` or `ProtocolHandle` tests for behavior owned by TUS lifecycle
rules:

- Creation, Creation-With-Upload, and Creation-Defer-Length.
- Byte receive through `PATCH`, including offset conflicts, content type,
  content length, checksum, body stream errors, and upload completion.
- `HEAD` state reporting, storage-size reconciliation, and cache-related
  response headers.
- Termination extension gating and deletion behavior.
- Protocol expiration, expired upload reclamation, and completed-upload
  retention boundaries.
- Concatenation, planned final upload availability, final upload materialization,
  and final upload repair.
- Hook timing and effects when the observable behavior is a protocol response or
  persisted protocol upload state.

Build protocol tests with real in-memory adapters unless the behavior is about a
specific adapter. This keeps the test focused on lifecycle behavior while still
crossing the same public backend seams that production code uses.

Do not assert that lifecycle helper functions were called. Assert the response,
state, storage size, hook-visible facts, or reclamation report visible through
public interfaces.

## Router Tests

Use router tests for behavior added by a framework adapter:

- Routes and methods are mounted at `Config::base_path`.
- Framework extractors reject malformed headers, upload IDs, bodies, or trailers.
- Body frames preserve checksum trailers.
- Protocol errors become HTTP responses with the required TUS headers.
- CORS middleware allows and exposes the expected headers when configured.
- `X-HTTP-Method-Override` reaches the intended protocol operation.
- Optional download routes exist only behind the explicit download router and
  `StorageReader` seam.

For `tus-axum`, drive `create_router` or `create_router_with_download` through
`tower::ServiceExt::oneshot`. Do not call internal handler functions in tests;
the public adapter seam is the router.

Router tests can use in-memory storage, state, and locking to keep setup cheap.
If a test needs file storage, object storage, or a real network server, that is a
signal it may belong in adapter conformance or server integration tests instead.

## Server Tests

Use server-level tests for behavior operators observe from `tus-server`:

- CLI parsing for `serve` and `cleanup`.
- Config file, environment, and CLI precedence.
- Storage URI and state directory wiring.
- Auth behavior at the HTTP seam.
- Request body size limits and idle timeouts.
- Health and readiness endpoints.
- Expired upload cleanup scheduling and one-shot cleanup outcomes.
- Graceful shutdown and readiness during drain.

Server tests should not re-test every TUS protocol scenario. The server assembly
should prove that it wires `ProtocolHandle`, `tus-axum`, storage, state, locking,
hooks, and operational middleware together. Detailed lifecycle behavior belongs
at the protocol seam.

## Adapter Conformance Tests

Adapter conformance tests are shared behavior tests for trait implementations.
They are the default test surface for backend adapters.

Use these helpers:

- `tus_protocol::storage::conformance::assert_upload_write_semantics` for
  required `Storage` behavior.
- `tus_protocol::storage::conformance::assert_full_semantics` for `Storage` plus
  `StorageReader` behavior.
- `tus_protocol::state::conformance::assert_state_store_semantics` for
  `StateStore` behavior.
- `tus_protocol::state::conformance::assert_upload_inventory_semantics` for
  optional `UploadInventory` behavior.
- `tus_protocol::locking::conformance::assert_locker_semantics` for immediate
  release lockers.
- `tus_protocol::locking::conformance::assert_locker_semantics_with` for
  lease-backed lockers.

Conformance helpers should use only the public trait interface. If an adapter
needs tests against provider internals, keep those provider-specific tests next
to the adapter and make clear which operational guarantee they cover.

## Hook Tests

Use `HookChain` tests for pure hook composition behavior:

- Event subscription filters hooks.
- Multiple hooks for the same event run in registration order.
- Pre-hook rejection stops later hooks.
- Metadata replacement is visible to later pre-hooks only for events that allow
  metadata replacement.

Use `Protocol` or router tests for hook behavior that affects requests:

- Rejection status and message reach the client.
- Hook-added response headers are included.
- Hook-visible upload snapshots hide storage-owned facts.
- Post-hook failures do not fail already-committed requests.

Use an HTTP test server for HTTP hook adapters. Mocking internal request builders
couples the tests to implementation rather than the hook adapter interface.

## Refactor Safety

A useful test should survive these refactors:

- Moving lifecycle helper functions between files.
- Replacing the internal body intake implementation.
- Changing how Axum handlers are split internally.
- Changing storage adapter internal object names while preserving
  `StorageHandle` behavior.
- Changing state store serialization format while preserving persisted
  `UploadState` facts.

If a test fails during one of those refactors but public behavior is unchanged,
move the test up to the public seam or delete it.

## Checklist

Before adding a test, verify:

- The test name describes observable behavior.
- The test uses the public interface for the module under test.
- The expected values come from the TUS spec, domain vocabulary, or a worked
  literal example.
- The test would still pass if private helper names or internal modules changed.
- The test setup uses the smallest real adapters needed to exercise the behavior.
- The test is not duplicating behavior already covered at a better seam.
