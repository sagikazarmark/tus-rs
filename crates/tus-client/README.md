# tus-client

[![crates.io](https://img.shields.io/crates/v/tus-client?style=flat-square)](https://crates.io/crates/tus-client)
[![docs.rs](https://img.shields.io/docsrs/tus-client?style=flat-square)](https://docs.rs/tus-client)

**Async Rust client for the [tus resumable upload protocol](https://tus.io/).**

`tus-client` drives uploads to tus-compatible servers from Rust applications. It
provides a resource-oriented API for creating upload resources, resuming existing
upload URLs, uploading offset-addressable sources, retrying transient PATCH
failures, and using custom transports.

The default transport is backed by [`reqwest`](https://crates.io/crates/reqwest).
The client core is transport-agnostic and can also be used with platform-specific
transports or single-threaded runtimes.

## Install

```toml
[dependencies]
tus-client = "0.0.1"
```

## Quick Start

```rust,no_run
use std::collections::HashMap;

use tus_client::Client;
use url::Url;

#[tokio::main]
async fn main() -> tus_client::Result<()> {
    let endpoint = Url::parse("http://127.0.0.1:8080/files")?;

    let mut metadata = HashMap::new();
    metadata.insert("filename".to_string(), "hello.txt".to_string());

    let upload = Client::new(endpoint)
        .upload_from(b"hello world".to_vec(), &metadata)
        .await?;

    println!("uploaded {} bytes to {}", upload.offset, upload.url);

    Ok(())
}
```

The repository also includes a native file upload example:

```bash
cargo run -p tus-client --example upload_file -- http://127.0.0.1:8080/files ./hello.txt
```

## Client API

`Client` exposes two styles of upload operation:

| API | Purpose |
|-----|---------|
| `Client::upload_from(source, metadata)` | Create a new upload resource and upload a source to it. |
| `Client::upload_from_with_progress(source, metadata, progress)` | Create and upload while reporting remote offset advances. |
| `Client::create_upload(NewUpload)` | Create a remote upload resource without sending the full source. Returns `(Upload, UploadInfo)`: the resource reference plus the state observed at creation. |
| `Client::upload_at(upload_url)` | Resolve an existing upload URL reference and return an `Upload`. |
| `Upload::info()` | Read the current remote offset, length, and metadata. |
| `Upload::terminate()` | Terminate an upload when the server supports termination. |

Server capabilities discovered via `Client::server_capabilities()` (the tus
`OPTIONS` probe) are cached per client, so repeated uploads through the same
client cost a single extra roundtrip.

Existing upload references may be absolute URLs, absolute paths on the endpoint
origin, or paths relative to the configured endpoint collection. For example,
with endpoint `http://127.0.0.1:8080/files`, `upload-1` resolves to
`http://127.0.0.1:8080/files/upload-1` and `/files/upload-1` resolves to
`http://127.0.0.1:8080/files/upload-1`.

`Vec<u8>` implements `UploadSource` out of the box. On native targets, enabling
`source-file` also exposes `FileSource`, a Tokio path-backed source that supports
resume and parallel uploads. Larger or platform-specific sources can implement
`UploadSource` to provide offset-addressable reads.

## Feature Flags

The default feature set enables the reqwest transport (with reqwest's default
features, including TLS) and checksum support.

| Feature | Purpose |
|---------|---------|
| `checksum` | Enable per-chunk checksum support through `tus-protocol/checksum`. |
| `reqwest-native-tls` | Re-enable reqwest's `native-tls` backend when default features are disabled. |
| `reqwest-rustls` | Re-enable reqwest's `rustls` backend when default features are disabled. |
| `source-file` | Expose native Tokio filesystem upload sources (pulls in `tokio/fs`). |
| `transport-reqwest` | Enable the default reqwest-backed transport and `Client::new`. |
| `transport-reqwest-middleware` | Accept `reqwest_middleware::ClientWithMiddleware` as a reqwest transport client. |

Disable default features when providing a custom transport or when building for a
runtime that should not pull in reqwest.

> **Note:** `--no-default-features` also drops reqwest's default features, which
> include TLS; plain `transport-reqwest` can then only speak `http://`. Add
> `reqwest-rustls` or `reqwest-native-tls` to restore `https://` support.

## Protocol Support

`tus-client` targets tus 1.0.0 upload workflows.

| Capability | Status | Notes |
|------------|--------|-------|
| Core protocol | Supported | `OPTIONS`, `POST`, `HEAD`, and `PATCH` requests with offsets, metadata, and version negotiation. |
| Creation | Supported | Create upload resources with `Client::create_upload` or as part of `Client::upload_from`. |
| Creation-With-Upload | Supported | Small sources can be sent in the initial `POST` when the server advertises support. |
| Termination | Supported | Terminate upload resources with `Upload::terminate`. |
| Concatenation | Supported on native | `Client::upload_parallel` creates partial uploads and concatenates them. |
| Checksum | Supported | Header checksums are supported everywhere; trailer checksums require the native reqwest transport and fail with a permanent error on `wasm32`. |

## Runtime Notes

On native targets, the default reqwest transport uses Tokio and supports regular
reqwest middleware when `transport-reqwest-middleware` is enabled. The
`upload_parallel` helper is native-only because it uses native task spawning and
the tus concatenation extension.

On `wasm32`, sequential upload and resume APIs work with `UploadSource`. The
reqwest transport uses the browser `fetch` backend, so request trailers and
manually setting `Content-Length` are not available. `ReqwestMiddlewareTransport`
compiles for `wasm32`, but `reqwest-middleware`'s own wasm support is unmaintained
upstream, so middleware on wasm is best-effort and untested here; use it only
when the middleware implementation itself is known to be wasm-compatible.

On `wasm32` targets, trait bounds relax to non-`Send` futures automatically.
Custom transports and upload sources should match the target runtime's
concurrency model.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
