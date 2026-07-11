# Config carries a pluggable upload-id source

Status: Accepted

## Context

`prepare_creation` mints an upload's id unconditionally with
`UploadState::new_random()` (a v4 UUID); no caller can supply the id. The
`tus-cloudflare` architecture (ADR 0012) needs the upload id to equal the
addressable id of the per-upload Durable Object, because a later `/{id}` request
routes to `DO(id)`. A DO's address must be chosen before the DO runs, so it can
never depend on a protocol-minted uuid: the id has to be decided outside the
protocol and adopted by it.

That injection cannot happen at the call site. tus-cloudflare reuses tus-axum's
router wholesale (ADR 0012), and tus-axum's create handler calls
`protocol.post(headers, body)` with no id argument. Forking that handler to pass
an id would sacrifice the reuse.

## Decision

Add a pluggable upload-id source to `Config`:

```rust
pub trait UploadIdSource: MaybeSendSync {
    fn name(&self) -> &'static str;
    fn generate(&self) -> String;
}
```

`Config` holds an `Arc<dyn UploadIdSource>`, defaulting to `RandomUploadIdSource`
(v4 UUID), so existing behavior is unchanged. `prepare_creation` calls
`config.id_source().generate()` instead of `new_random()` and validates the
result through `UploadId` parsing, erroring if a custom source yields an invalid
id. The `tus-cloudflare` DO configures a source that returns its own DO id
string, so the reused `post()` path mints exactly that id with no per-call
plumbing.

The source is synchronous and infallible (a DO id and a UUID are both available
that way); a future async or fallible variant can be added without breaking the
trait since it is the extension point, not the call site.

## Considered Options

**Closure seam (`Config::with_id_generator(Fn() -> String)`) (rejected).**
Lighter and ergonomic, but inconsistent with the crate's other pluggable seams
(`Storage`, `Locker`), less discoverable, carries no `name()` for logging, and
puts a `dyn Fn` in `Config`'s type.

**Targeted `post_with_id` entry point (rejected).** Supplying the id as a call
argument would force tus-cloudflare to bypass tus-axum's reused create handler,
defeating ADR 0012's router reuse. Config-level policy keeps `post()` the single
entry point.
