# Public tuple fields on axum extractors

Status: Accepted

## Context

The three `tus-axum` extractors — `TusUploadId(pub UploadId)`, `TusHeaders(pub Headers)`,
and `TusBody(pub RequestBody)` — expose their inner value as a `pub` tuple field,
while the response newtype `TusResponse(pub(crate) …)` keeps its field private.
Before 1.0 we had to decide whether this split is a mistake to fix (make the
extractors `pub(crate)` + `into_inner()`) or a deliberate commitment.

## Decision

Keep the `pub` tuple field on all three extractors as an explicit 1.0 commitment.

The reference class is *other axum extractors*, not `TusResponse`: `Path`, `Query`,
`Json`, `Form`, `State`, and `Extension` are all `pub` tuple newtypes destructured
in the handler signature (`TusHeaders(headers): TusHeaders`). Matching a tuple
struct at an external call site requires a visible field, so `pub(crate)` would
break signature-destructuring and force `.into_inner()` — the friction landing
exactly on the documented custom-handler seam (`state.rs`). A `pub(crate)`
extractor would be the unidiomatic outlier among axum's own extractors.

The `TusResponse` visibility split is principled, not an inconsistency to fix:
visibility follows data-flow direction. Extractors are crate-constructed and
user-destructured (field must be nameable at the use site); `TusResponse` is
crate-constructed and axum-consumed (the user never names the field).

## Considered Options

- **`pub(crate)` + `into_inner()`/`Deref`** — rejected. It buys wrapper-field
  evolvability the wrappers will never need: all three are pure orphan-rule
  vehicles (they exist only so `FromRequest`/`FromRequestParts` can be implemented
  locally), so anything the value carries belongs in the inner type, which stays
  fully evolvable. `Deref` is separately rejected for the same anti-masquerade
  reason the crate already gave for `TusProtocol` (`state.rs`).

## Consequences

- The extractors' representation is locked at 1.0; this is an accepted commitment,
  not an oversight.
- `TusBody`'s `pub RequestBody` re-exposes a deliberately exhaustive enum, so a new
  `RequestBody` variant is a breaking change for `tus-axum` consumers. This coupling
  is inherent to exposing the body at all — `into_inner()` would hand back the same
  exhaustive enum — so `pub(crate)` would not have decoupled it. It is accepted, not
  mitigated by field visibility.
- `into_inner()` may be added later as a free, non-breaking convenience for
  non-pattern call sites; it is not required, since `pub` already permits `.0` and
  `let TusHeaders(h) = x;`.
