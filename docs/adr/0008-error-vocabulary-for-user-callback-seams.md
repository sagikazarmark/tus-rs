# Error vocabulary for the user-callback seams

Status: Accepted

## Context

The client exposes two user-implemented callback seams that return
`crate::Result`: [`UploadSource::read_chunk`] and [`HeaderProvider::headers`].
The semantically-correct failure for a misbehaving source is
`Error::Source`, but `Error` is `#[non_exhaustive]` and its only public
constructors were `Error::transport` / `Error::transport_permanent`. So an
external implementor of either seam could not construct the right variant — they
could only *mislabel* a failure as `Io` or `Transport`. A `HeaderProvider`
implementor refreshing an OAuth token had no correct variant at all.

This had to be settled before 1.0 because the payload *shape* of these variants
is a breaking commitment: `Error::Source` carried a flat `message: String`, and
switching it to a boxed source error later would be a major bump.

## Decision

Model the two seams as **two distinct error categories**, each carrying a
`#[source] BoxError`, with public constructors:

- `Error::Source { source: BoxError }` — a misbehaving [upload source]. Always
  **permanent** (never retried): the local source is treated as authoritative,
  so a torn or changed source will not un-tear on retry; a flaky custom source
  is expected to retry internally before returning. Constructor
  `Error::source(impl Into<BoxError>)`.
- `Error::HeaderProvider { source: BoxError, retryable: bool }` — a failing
  [header provider]. Ships as a **retryable/permanent pair** mirroring
  `transport`: `Error::header_provider(...)` (retryable) and
  `Error::header_provider_permanent(...)`. The retry loop rebuilds the request —
  and re-invokes `headers()` — on every attempt, so a retryable header-provider
  failure genuinely gives a transient token refresh another chance after backoff.

Both constructors take `impl Into<BoxError>`, which still accepts a plain
`&str` / `String`, so message-only callers are not forced to define an error
type — exactly as `Error::transport_permanent("bad request line")` already works.

## Considered Options

- **Keep `String` payloads.** Rejected: it flattens the implementor's real error
  and discards the `std::error::Error::source()` chain at the boundary — the same
  information loss this issue exists to fix — and diverges from `Error::Transport`,
  which already models its cause as `#[source] BoxError`.
- **One shared category** for both seams. Rejected: the seams disagree on retry
  policy (source = never, header refresh = maybe), so collapsing them forces a
  wrong answer on one.
- **Route header-provider failures through `Error::transport`.** Rejected: it
  re-introduces the exact mislabeling complaint — an auth-hook failure would read
  as `"transport failed"` and send debuggers hunting the network layer.
- **Name the header variant `Error::Header`.** Rejected: it collides conceptually
  with the existing byte-level trio (`MissingHeader`, `InvalidHeader`,
  `InvalidRequestHeader`). `HeaderProvider` names the seam that produced the
  failure and stays honest for non-auth uses (tracing, signing, tenant routing)
  that a `Credentials`/`Auth` name would mislabel.

## Consequences

Adding the `HeaderProvider` variant is non-breaking under `#[non_exhaustive]`;
the `String` → `BoxError` change on `Error::Source` is the breaking part and is
done now, before 1.0. Internal source-failure call sites migrate from
`Error::Source { message }` to `Error::source(...)`, and the bundled custom-source
example demonstrates the constructor on a failure branch.

[`UploadSource::read_chunk`]: ../../crates/tus-client/src/client/upload.rs
[`HeaderProvider::headers`]: ../../crates/tus-client/src/client.rs
[upload source]: ../../CONTEXT.md
[header provider]: ../../CONTEXT.md
