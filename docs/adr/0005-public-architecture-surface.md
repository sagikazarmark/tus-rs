# Public architecture surface

Status: Accepted

## Context

The protocol crate has two audiences: protocol users who want to run a tus
server, and framework adapter authors who need a framework-neutral seam to call
from their HTTP integration.

Exposing low-level lifecycle transition helpers makes the public interface
shallower: callers can bypass the protocol facade and must learn internal
ordering rules that should stay local to the implementation. Exposing axum
handler functions has the same effect in the adapter crate, because the stable
adapter interface is the router plus state/extractor/error types.

## Decision

`tus_protocol::Protocol` and `tus_protocol::ProtocolHandle` are the public
request-handling interface for framework adapters and protocol users. Backend
traits, typed request/response values, upload state, hooks, locking, and storage
types remain available from the crate root.

Lifecycle transition helpers are internal implementation behind `Protocol`.
The `tus_protocol::lifecycle` module is not public. The root-level
`reclaim_expired_uploads`, `ExpiredUploadReclamationReport`, and
`ExpiredUploadReclamationOutcome` remain public because expired upload
reclamation is an operational cleanup interface used outside request handling.

`tus_axum::create_router` and `tus_axum::create_router_with_download` are the
public axum adapter interface. `TusState`, extractor types, `Error`, and
`build_cors_layer` remain public support types. `tus_axum::handlers` is internal
wiring and is not a stable public module.

## Consequences

Removing `tus_protocol::lifecycle`, the root-level lifecycle transition helper
exports, and `tus_axum::handlers` is an intentional breaking public interface
change. Downstream framework adapters should call `Protocol` or
`ProtocolHandle` methods instead of lifecycle helpers. Axum applications should
build routes through `create_router` or `create_router_with_download` instead of
importing handler functions.

The public surface is deeper and smaller: protocol lifecycle ordering, recovery,
final-upload materialization, hook timing, and response mapping remain local to
the protocol implementation unless a future real adapter need justifies another
public seam.
