# tus-cloudflare: Durable-Object-per-upload, reusing tus-axum on wasm

Status: Accepted

## Context

`tus-cloudflare` runs the TUS protocol inside a Cloudflare Worker (workerd,
`wasm32`, single-threaded). TUS requires per-upload write serialization, atomic
`CreateNew`, and strongly-consistent offset reads. On Cloudflare, the only
primitive giving strong consistency plus natural serialization is a Durable
Object; KV is eventually consistent with no compare-and-set and cannot host a
lock. The workspace already carries the machinery for this target: the
`MaybeSend`/`MaybeSync` conditional bounds, `async_trait(?Send)` on `wasm32`, and
`NoopLocker`, whose own docs name "per-upload Durable Object" as the motivating
case.

## Decision

**One Durable Object instance per upload id.** The Worker is a thin front that
routes `POST` (create) to a DO named by a freshly-minted id and every
`/{id}` request to `DO(id)`. Inside the DO:

- `Storage` is Cloudflare R2 (bytes), per ADR 0011.
- `StateStore` is an implementation over the DO's own SQLite-backed transactional
  storage. SQLite-backed (not the legacy KV backend) because the multipart
  part-to-etag list can approach 10,000 entries and would breach the legacy
  128 KiB-per-value limit.
- `Locker` is `NoopLocker`: the DO's single-threaded input/output gates already
  serialize concurrent requests to the same upload, so no distributed lock is
  needed or wanted.

**Expiration is alarm-native, not scan-native.** A per-upload DO cannot enumerate
other uploads, so `StateStore::list_expired` returns empty and the
`reclaim_expired_uploads` scan is unused. Instead each DO calls `setAlarm(expires_at)`
at creation; `alarm()` reclaims the upload (abort the R2 multipart upload, delete
R2 objects, `deleteAll()` its storage) only if it is still unfinished, honoring
the protocol-expiration-vs-completed-retention split already in the glossary. R2
bucket lifecycle rules are the backstop (auto-abort stale multipart uploads;
optional TTL purge of completed objects for cost bounding). This also reclaims
abandoned uploads that a native server would leak. Expiration should therefore
default ON in the Worker. `UploadInventory` is not implemented in v1.

**The HTTP layer reuses tus-axum, made wasm-capable behind a `worker` feature.**
Rather than duplicate TUS HTTP semantics (CORS, `X-HTTP-Method-Override`,
405+`Allow`+`Tus-Resumable`, bare-`OPTIONS` discovery, base-path parsing),
tus-cloudflare reuses tus-axum's router. axum's `Handler` blanket impl requires
`Fut: Send`, which the `?Send` protocol futures do not satisfy on wasm, so
tus-axum gains an opt-in `worker` feature that wraps each handler future in
`send_wrapper::SendWrapper` under `cfg(all(target_arch = "wasm32", feature = "worker"))`
(sound because workerd is single-threaded; the generic `send_wrapper` crate keeps
tus-axum free of any `worker`-crate coupling). This mirrors a pattern already
shipped in `dioxus-clerk`, generalized from a middleware `Service` future to
handler futures. tus-cloudflare supplies `Send + Sync` backends (the `!Send`
`JsValue`-backed R2/DO handles wrapped in `SendWrapper`) and drives
`create_router(...).call(req)` in the DO fetch handler via the `worker` crate's
`http`/`axum` request/body integration.

The axum dependency is split: `axum::serve` (the only consumer of the wasm-hostile
`http1`/`tokio` features) lives solely in tus-server, so those features move onto
tus-server's own axum line while the workspace and tus-axum keep axum minimal
(`default-features = false`). tus-axum's library never touches `serve`.

## Consequences

`tus-cloudflare` requires the Workers Paid plan (Durable Objects are not on the
free tier); the free tier was explicitly out of scope. tus-axum stays
native-by-default with wasm as an opt-in feature, and its handlers change shape
from `async fn` to `fn -> impl Future + Send` returning a `maybe_send(async {..})`
body. Reclamation correctness now depends on DO alarms firing (at-least-once with
retry) rather than on a central cleanup job, with R2 lifecycle rules as the
backstop.

## Considered Options

**Stateless Worker + KV/R2 (rejected).** KV's eventual consistency and lack of
CAS cannot provide atomic `CreateNew` or a lock, so concurrent PATCHes to one
upload can corrupt it: a TUS server that violates the protocol's core invariant.

**Own thin handlers in tus-cloudflare / extract a shared `tus-http` crate
(rejected for now).** Both avoid modifying tus-axum, but the first duplicates the
subtle CORS/discovery logic and the second is an upfront refactor of a shipped
crate. Reusing tus-axum behind a `worker` feature is less total change and keeps
one source of truth for HTTP semantics. A `tus-http` extraction remains a clean
future consolidation if a third HTTP adapter appears.
