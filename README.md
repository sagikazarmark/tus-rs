# tus-rs

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/sagikazarmark/tus-rs/dagger.yaml?style=flat-square)](https://github.com/sagikazarmark/tus-rs/actions/workflows/dagger.yaml)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/sagikazarmark/tus-rs/badge?style=flat-square)](https://securityscorecards.dev/viewer/?uri=github.com/sagikazarmark/tus-rs)
[![crates.io](https://img.shields.io/crates/v/tus-protocol?style=flat-square)](https://crates.io/crates/tus-protocol)
[![docs.rs](https://img.shields.io/docsrs/tus-protocol?style=flat-square)](https://docs.rs/tus-protocol)

**Rust implementation of the [TUS resumable upload protocol](https://tus.io/).**

## Features

- **Standard TUS 1.0.0 core and extension support on the native server path**
- **Extensible storage backends**
- **Pluggable state storage**
- **Distributed locking support**
- **Flexible hook system** for customization

## TUS Protocol Support

| Capability | Status | Notes |
|------------|--------|-------|
| [Core protocol](https://tus.io/protocols/resumable-upload#core-protocol) | Supported | `POST`, `HEAD`, `PATCH`, `OPTIONS`, offsets, metadata, and version negotiation. |
| [Creation](https://tus.io/protocols/resumable-upload#creation) | Supported | Create new uploads via `POST`. |
| [Creation-With-Upload](https://tus.io/protocols/resumable-upload#creation-with-upload) | Supported | Include data in the initial `POST` request. |
| [Creation-Defer-Length](https://tus.io/protocols/resumable-upload#creation) | Supported | Create uploads before the final size is known. |
| [Termination](https://tus.io/protocols/resumable-upload#termination) | Supported | Cancel/delete uploads via `DELETE`. |
| [Expiration](https://tus.io/protocols/resumable-upload#expiration) | Supported | Expiration timestamps, rejection of expired unfinished/intermediate uploads, and background cleanup. |
| [Concatenation](https://tus.io/protocols/resumable-upload#concatenation) | Supported | Standard final concatenation is supported. The non-standard `concatenation-unfinished` check is separate and outside the stable protocol contract. |
| [Checksum](https://tus.io/protocols/resumable-upload#checksum) | Supported | Bodied and trailer checksums are supported. |

Standard protocol behavior is advertised through `Tus-Extension` only when the
matching extension is enabled. Non-standard conveniences are explicit opt-ins:
GET download routes require the `StorageReader` seam and download-enabled router,
and `concatenation-unfinished` is separate from standard final concatenation.
`Config::allow_empty_creation(false)` is also an opt-in non-compliant mode: leave
the default enabled to accept standard empty Creation requests.

## Documentation

- API documentation: <https://docs.rs/tus-protocol>
- Protocol reference: <https://tus.io/protocols/resumable-upload>
- Architecture: [docs/architecture.md](docs/architecture.md)
- Adapter implementation guide: [docs/adapters.md](docs/adapters.md)
- Testing guide: [docs/testing.md](docs/testing.md)

## Standalone Server Storage

The `tus-server` binary stores uploaded bytes through OpenDAL. The default build enables the `opendal-fs` feature and uses local filesystem storage rooted at `./uploads`:

```bash
cargo run -p tus-server -- serve
```

Use `--storage-uri` / `TUS_STORAGE_URI` to select an OpenDAL service:

```bash
tus-server serve --storage-uri fs:///var/lib/tus/uploads
```

Set storage backend options with `TUS_STORAGE_<KEY>` environment variables or direct keys under `[storage]` in the server config file. For example, use `TUS_STORAGE_ROOT=uploads` with `fs://` for a relative filesystem root.

S3 support requires building the server with `opendal-s3`:

```bash
cargo build -p tus-server --features opendal-s3
cargo build -p tus-server --no-default-features --features opendal-s3
```

```bash
TUS_STORAGE_URI=s3:// \
TUS_STORAGE_BUCKET=my-bucket \
TUS_STORAGE_REGION=us-east-1 \
TUS_STORAGE_ROOT=/uploads \
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
cargo run -p tus-server --features opendal-s3 -- serve
```

Without a local Rust toolchain, Dagger builds the `tus-server` and `tus` CLI
binaries in a container:

```sh
dagger call server export --path ./tus-server   # tus-server binary
dagger call cli export --path ./tus             # tus client CLI binary
```

These produce the same default-feature (`opendal-fs`) binaries as
`cargo build -p tus-server` and `cargo build -p tus-cli`; run the exported files
exactly as the `cargo run` examples above. The Dagger build has no feature knobs,
so alternative feature sets such as `opendal-s3` still require the Cargo build.

Object storage only covers uploaded bytes. Upload state remains file-backed under `--state-dir`, and locking remains process-local through the in-memory locker.

Expired upload reclamation follows expiration by default. `tus-server serve` rejects protocol-expired unfinished or intermediate uploads according to protocol configuration, and whenever `--expiration` is set it also runs an in-process sweeper that deletes the expired uploads' data and state, so they do not accumulate on disk. That sweeper shares the live server's process-local locker, so it is safe to run alongside serving. Pass `--disable-expiration-reclamation` (or set `TUS_DISABLE_EXPIRATION_RECLAMATION=true`) to keep expiry enforced on access while leaving expired data in place. Completed deliverable uploads do not expire through TUS expiration; deleting them is a separate retention policy. To reclaim expired uploads out-of-band in a single sweep, use `tus-server cleanup` with the same storage and state configuration; that subcommand builds its own locker and is not safe to run concurrently with a live `serve` process until cross-process locking is available.

This repository provides `tus-protocol`, the framework-neutral core crate,
framework adapters such as `tus-axum`, a standalone `tus-server`, the `tus`
client CLI, and first-party backend/hook adapters. The core crate exposes typed
request headers, response values, upload state, storage, state-store, locking,
and hook traits. HTTP adapters parse their framework-specific request types,
call the matching `Protocol` handler, and map the returned `Response` or `Error`
back into the framework response type.

Useful entry points:

- `Config` configures enabled extensions, size limits, expiration, base paths,
  and CORS-related response behavior.
- `Protocol` contains the core `POST`, `HEAD`, `PATCH`, `DELETE`, and `OPTIONS`
  handlers.
- `Storage`, `StateStore`, and `Locker` define the required backend contracts.
- `StorageReader` is the optional read seam for non-standard download paths.
- `HookChain` and `Hook` provide lifecycle extension points with hook-safe
  upload snapshots that hide storage-local facts.
- `reclaim_expired_uploads` is the operational cleanup entry point for expired
  unfinished or intermediate uploads.

Feature flags enable optional built-in backends and checksum support:

- `storage-memory`, `state-memory`, `lock-memory` for in-process testing and
  development backends.
- `storage-file`, `state-file`, `lock-file` for native filesystem-backed
  backends.
- `checksum` for checksum validation algorithms.
- `native` for the async runtime support the file and lock backends build on.

On `wasm32` targets (such as Cloudflare Workers), trait bounds relax to
non-`Send` futures automatically; no feature flag is needed.

## Development

Minimum verification:

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features --workspace`
- `cargo test --all-targets --all-features --workspace`

Or run the same checks (fmt, clippy, test, doc, build) in a container with [Dagger](https://dagger.io), exactly as CI does:

- `dagger check`: from the repo root for the workspace (including the CLI end-to-end suites against `tusd` and `rustus`), or from `demo/` for the demo app

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
