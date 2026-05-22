# tus-axum

[![crates.io](https://img.shields.io/crates/v/tus-axum?style=flat-square)](https://crates.io/crates/tus-axum)
[![docs.rs](https://img.shields.io/docsrs/tus-axum?style=flat-square)](https://docs.rs/tus-axum)

**axum integration for the [tus resumable upload protocol](https://tus.io/).**

`tus-axum` wires the framework-neutral [`tus-protocol`](https://crates.io/crates/tus-protocol)
core into an axum application. It provides the router, request extractors, CORS
setup, response conversion, and error conversion needed to expose a tus-compatible
upload endpoint from an axum server.

The protocol behavior, storage, state, locking, checksum validation, and hooks
come from `tus-protocol`. This crate is intentionally a thin adapter around
those framework-neutral pieces.

## Install

For a small axum server using the built-in in-memory backends:

```toml
[dependencies]
axum = "0.8"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
tus-axum = "0.0.1"
tus-protocol = { version = "0.0.1", features = ["storage-memory", "state-memory", "lock-memory"] }
```

For a native server using the bundled filesystem backends and checksum support,
enable the `tus-protocol` native feature set instead:

```toml
[dependencies]
tus-protocol = { version = "0.0.1", features = ["full-native"] }
```

## Quick Start

```rust,no_run
use tus_axum::{TusState, create_router};
use tus_protocol::{
    Config, NoopHookExecutor, ProtocolHandle,
    locking::memory::MemoryLocker,
    state::memory::MemoryStateStore,
    storage::memory::MemoryStorage,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::with_all_extensions().base_path("/files");

    let protocol = ProtocolHandle::new(
        config,
        MemoryStorage::new(),
        MemoryStateStore::new(),
        MemoryLocker::new(),
        NoopHookExecutor::new(),
    );

    let state = TusState::new(protocol);
    let router = create_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    axum::serve(listener, router).await?;

    Ok(())
}
```

The repository also includes a runnable example:

```bash
cargo run -p tus-axum --example server
```

Create an upload and send data to it with curl:

```bash
curl -i http://127.0.0.1:8080/files \
    -H "Tus-Resumable: 1.0.0" \
    -H "Upload-Length: 11" \
    -X POST

curl -i http://127.0.0.1:8080/files/<id> \
    -H "Tus-Resumable: 1.0.0" \
    -H "Upload-Offset: 0" \
    -H "Content-Type: application/offset+octet-stream" \
    -X PATCH \
    --data-binary "hello world"
```

## Router Behavior

`create_router` mounts tus routes at `Config::base_path`, which defaults to
`/files`.

| Route | Purpose |
|-------|---------|
| `OPTIONS /files` and `OPTIONS /files/{upload_id}` | Advertise tus protocol version, extensions, limits, and checksum algorithms. |
| `POST /files` | Create uploads. |
| `HEAD /files/{upload_id}` | Inspect upload offset, length, metadata, expiration, and concatenation state. |
| `PATCH /files/{upload_id}` | Append upload bytes. |
| `DELETE /files/{upload_id}` | Terminate uploads when the termination extension is enabled. |
| `GET /files/{upload_id}` | Download a completed upload unless downloads are disabled in `Config`. |
| `POST /files/{upload_id}` with `X-HTTP-Method-Override` | Proxy-friendly fallback for `PATCH` and `DELETE`. |

CORS middleware is applied only when allowed origins are configured through
`Config`. When enabled, the layer allows tus request headers, exposes tus
response headers, and supports the method-override header.

## Feature Flags

The default `tus-axum` feature set is empty. Backend and protocol capabilities
are selected through `tus-protocol` feature flags.

Common `tus-protocol` features for axum servers:

| Feature | Purpose |
|---------|---------|
| `storage-memory` | In-memory upload bytes for tests and local development. |
| `state-memory` | In-memory upload state for tests and local development. |
| `lock-memory` | In-process upload locking for native single-server deployments. |
| `storage-file` | Native filesystem-backed upload storage. |
| `state-file` | Native filesystem-backed upload state. |
| `lock-file` | Native filesystem-backed upload locks. |
| `checksum` | Checksum validation algorithms. |
| `full-native` | Convenience set for native servers: native runtime support, file storage/state, memory locks, and checksums. |

## Protocol Support

`tus-axum` exposes the native `tus-protocol` server behavior through axum.

| Capability | Status | Notes |
|------------|--------|-------|
| Core protocol | Supported | `POST`, `HEAD`, `PATCH`, `OPTIONS`, offsets, metadata, and version negotiation. |
| Creation | Supported | Create uploads with `POST`. |
| Creation-With-Upload | Supported | Accept upload bytes in the initial `POST` when enabled. |
| Creation-Defer-Length | Supported | Create uploads before the final size is known. |
| Termination | Supported | Delete uploads with `DELETE`. |
| Expiration | Supported | Expiration timestamps and rejection of expired uploads. |
| Concatenation | Supported | Server-side final uploads from partial uploads. |
| Checksum | Supported | Header and trailer checksum validation when `tus-protocol/checksum` is enabled. |
| Download | Supported | Non-standard convenience `GET` endpoint for completed uploads, configurable through `Config`. |

## Relationship To tus-protocol

The adapter translates axum requests into the framework-neutral request values
expected by `tus-protocol`, then translates protocol responses and errors back
into axum responses.

Useful entry points:

- `TusState` stores the `ProtocolHandle` as axum application state.
- `create_router` builds the complete tus route table for the configured base path.
- `build_cors_layer` builds the CORS layer used when CORS origins are configured.
- `Error` converts `tus_protocol::Error` into an axum response.
- `Headers`, `TusBody`, and `UploadId` are axum extractors used by the handlers.

Use `tus-protocol` directly to configure protocol extensions, upload limits,
expiration, storage, state, locking, hooks, and checksum support.

## Runtime Notes

`tus-axum` targets native axum servers. The storage, state, locker, and hook
implementations used with `create_router` must be `Send + Sync + 'static`.

Use the built-in memory backends for tests and local development. Use the file
backends or custom implementations for durable deployments. For deployments
behind proxies that block `PATCH` or `DELETE`, clients can send `POST` to an
upload resource with `X-HTTP-Method-Override: PATCH` or
`X-HTTP-Method-Override: DELETE`.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
