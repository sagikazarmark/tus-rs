# Distinct reqwest-middleware transport type

Status: Accepted

## Context

`ReqwestTransport` was backed by a compile-time type alias that flipped between
`reqwest::Client` and `reqwest_middleware::ClientWithMiddleware` based on the
`transport-reqwest-middleware` feature. Because Cargo features are additive and
unified across the whole dependency graph, an **unrelated** crate enabling
`transport-reqwest-middleware` flipped the alias for everyone: the default
transport silently began wrapping every client in a no-op
`reqwest_middleware::ClientBuilder::new(client).build()` chain. The default
[transport]'s behavior must not depend on a transitively-enabled feature.

Separately, `impl From<reqwest_middleware::ClientWithMiddleware> for
ReqwestTransport` put a `reqwest_middleware` (0.x) type in the public API while
only `reqwest` was re-exported, leaving consumers to add their own
`reqwest-middleware` dependency with no re-exported version to pin against, the
version-skew footgun the `reqwest` re-export exists to avoid.

## Decision

Split the middleware path into its own type.

- `ReqwestTransport` is **unconditionally** backed by `reqwest::Client`. The
  alias swap, the middleware-wrapping helpers, and `From<ClientWithMiddleware>`
  are removed from it. No feature enabled anywhere in the graph can change what
  `ReqwestTransport::new()` or `From<reqwest::Client>` produce.
- A separate, feature-gated `ReqwestMiddlewareTransport` wraps
  `ClientWithMiddleware`. It is constructed **only** via
  `From<ClientWithMiddleware>`: no `Default` / no-arg `new()`, because a
  middleware transport with no middleware is just the plain transport, and the
  absence of a no-arg constructor enforces that invariant structurally.
- `reqwest_middleware` is re-exported under the feature (`pub use
  reqwest_middleware;`), mirroring the existing `reqwest` re-export, so consumers
  have a version to pin against.

Both transports' `send` bodies stay identical: request assembly, response body
capping, response assembly, and error mapping are written once. The two `reqwest`
builder types (plain and middleware) expose the same inherent methods but share
no common trait, so a small private adapter trait (`ReqwestRequestBuilder`)
bridges them; its two impls differ only in how a failed `send` is classified.
The split removes the feature-unification footgun without duplicating transport
logic.

## Considered Options

- **Keep the alias swap, only re-export `reqwest_middleware`.** Rejected: fixes
  the leaking type but leaves the feature-unification footgun fully in place.
- **One `ReqwestTransport` with an internal runtime enum** `{ Plain, Middleware }`.
  Rejected: it still bakes `ClientWithMiddleware` into the default type's
  definition, adds a per-`send` branch to the hot path, and keeps
  `From<ClientWithMiddleware>` on the default type. The distinct type deletes all
  middleware surface from the plain path.
- **Hide `reqwest_middleware` behind a constructor.** Rejected as a mirage: to add
  middleware at all, a user must name `reqwest_middleware::Middleware`, so the
  type is unavoidably in the middleware transport's public surface; hence the
  re-export is the honest fix, not concealment.

## Consequences

The public surface gains a second transport type (`ReqwestMiddlewareTransport`)
and a `reqwest_middleware` re-export, both under `transport-reqwest-middleware`.
This is a deliberate 1.0 commitment: middleware is a genuinely different transport
with a genuinely different dependency, and the API now says so explicitly instead
of mutating the default transport behind a feature flag.

[transport]: ../../CONTEXT.md
