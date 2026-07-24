# tus-protocol

[![crates.io](https://img.shields.io/crates/v/tus-protocol?style=flat-square)](https://crates.io/crates/tus-protocol)
[![docs.rs](https://img.shields.io/docsrs/tus-protocol?style=flat-square)](https://docs.rs/tus-protocol)

**Framework-neutral Rust implementation of the [tus resumable upload protocol](https://tus.io/).**

`tus-protocol` contains the protocol core for building tus-compatible upload
servers. It does not depend on a web framework. Instead, adapters parse incoming
HTTP requests, call the matching protocol handler, and convert the returned
framework-neutral response back into their own response type.

The public request-handling seam is `Protocol` or the owned `ProtocolHandle`.
Lower-level lifecycle transition helpers are internal implementation details.
Servers that run cleanup use the root-level `reclaim_expired_uploads` operation
and its report/outcome types.

## Install

Enable only the built-in backends and extensions your server needs:

```toml
[dependencies]
tus-protocol = { version = "0.0.1", features = ["storage-memory", "state-memory"] }
```

For a native server with the bundled filesystem backends and checksum support:

```toml
[dependencies]
tus-protocol = { version = "0.0.1", features = ["storage-file", "state-file", "lock-memory", "checksum"] }
```

The quick start below also uses `http` and `tokio` directly:

```toml
[dependencies]
http = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Feature Flags

The default feature set is empty.

| Feature          | Purpose                                                         |
| ---------------- | --------------------------------------------------------------- |
| `storage-memory` | In-memory upload bytes for tests and development.               |
| `state-memory`   | In-memory upload state for tests and development.               |
| `lock-memory`    | In-process upload locking for native single-server deployments. |
| `storage-file`   | Native filesystem-backed upload storage.                        |
| `state-file`     | Native filesystem-backed upload state.                          |
| `lock-file`      | Native filesystem-backed upload locks.                          |
| `checksum`       | Checksum validation algorithms.                                 |
| `native`         | Native async runtime support used by file and lock backends.    |

On `wasm32` targets (such as Cloudflare Workers), trait bounds automatically
relax to non-`Send` futures; no feature flag is needed. The built-in memory and
file backends are not exposed on `wasm32`. There, provide runtime-specific
implementations of `Storage`, `StateStore`, and `Locker`.

## Protocol Support

`tus-protocol` implements tus 1.0.0 server behavior for the native protocol path.

| Capability            | Status    | Notes                                                                           |
| --------------------- | --------- | ------------------------------------------------------------------------------- |
| Core protocol         | Supported | `POST`, `HEAD`, `PATCH`, `OPTIONS`, offsets, metadata, and version negotiation. |
| Creation              | Supported | Create uploads with `POST`.                                                     |
| Creation-With-Upload  | Supported | Accept upload bytes in the initial `POST` when enabled.                         |
| Creation-Defer-Length | Supported | Create uploads before the final size is known.                                  |
| Termination           | Supported | Delete uploads with `DELETE`.                                                   |
| Expiration            | Supported | Expiration timestamps and rejection of expired unfinished/intermediate uploads. |
| Concatenation         | Supported | Server-side final uploads from partial uploads.                                 |
| Checksum              | Supported | Header and trailer checksum validation when `checksum` is enabled.              |

Compliance notes:

- `X-HTTP-Method-Override` is part of the tus core protocol. Framework adapters
  can expose it as a proxy-friendly fallback for clients that cannot send
  `PATCH` or `DELETE` directly.
- `StorageReader` is not required for tus uploads. It supports non-standard
  download or inspection paths in framework adapters that opt into them.
- `concatenation-unfinished` is a non-standard advertised token. It allows final
  uploads to be planned before every partial upload is complete; strict standard
  concatenation should enable `Concatenation` without `ConcatenationUnfinished`.
- `Config::allow_empty_creation(false)` is an opt-in non-compliant mode. Leave
  the default enabled to accept standard empty Creation requests.
- Final upload `HEAD` responses report `Upload-Concat` as `final;<parts>` with
  part URLs normalized to the configured base path in the original order.

## Storage, State, And Locking

The protocol is built around three required backend traits plus one optional read seam:

| Trait           | Responsibility                                                                     |
| --------------- | ---------------------------------------------------------------------------------- |
| `Storage`       | Stores upload bytes for the upload lifecycle, and owns storage-local handle facts. |
| `StorageReader` | Optionally reads stored bytes for non-standard download or inspection paths.       |
| `StateStore`    | Persists protocol upload state plus opaque `StorageHandle` snapshots.              |
| `Locker`        | Coordinates concurrent access to a single upload ID.                               |

Use the built-in memory backends for tests and local development. Use the file
backends or custom implementations for durable deployments. Third-party or
first-party integration crates can implement these traits for object stores,
databases, distributed locks, or platform-specific runtimes.

## Hooks

`HookChain` and the `Hook` trait provide lifecycle extension points:

- `PreCreate` and `PostCreate`
- `PreReceive` and `PostReceive`
- `PreFinish` and `PostFinish`
- `PreTerminate` and `PostTerminate`

Hook contexts expose `HookUpload`, a protocol-level upload snapshot that omits
storage keys and backend-internal storage metadata. Pre-hooks can reject
requests. `PreCreate`, `PreReceive`, and `PreTerminate` may add response
headers. `PreCreate` and `PreReceive` may replace user metadata before an
operation is committed. `PreFinish` is gate-only. Post-hooks are best-effort
notifications; failures are logged and do not fail already-committed requests.

| Request path                          | Hook events                                                                                                            | Notes                                                                                                                                                                                                                                                                                  |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `POST` regular or partial upload      | `PreCreate`, `PostCreate`                                                                                              | `PreCreate` may add response headers or replace user metadata before storage/state creation.                                                                                                                                                                                           |
| `POST` with Creation-With-Upload body | `PreCreate`, `PreReceive`, `PostCreate`, `PostReceive`, plus `PreFinish`/`PostFinish` if the body completes the upload | `PreReceive` gates the initial body before it is collected and may add response headers or replace metadata after `PreCreate`. `PreFinish` gates a completing body after validation. Post-hooks run only after storage and state commit; commit failure rolls back without post-hooks. |
| `POST` final concatenation upload     | `PreCreate`, `PostCreate`, plus `PreFinish`/`PostFinish` if every referenced partial is complete                       | Final upload facts are derived from referenced partials before `PreCreate`; `PreCreate` may add response headers or replace user metadata. `PreFinish` is gate-only.                                                                                                                   |
| `PATCH`                               | `PreReceive`, `PostReceive`, plus `PreFinish`/`PostFinish` if the patch completes the upload                           | `PreReceive` may add response headers or replace user metadata before bytes are committed. `PreFinish` is gate-only.                                                                                                                                                                   |
| `DELETE`                              | `PreTerminate`, `PostTerminate`                                                                                        | Requires the Termination extension. `PreTerminate` may add response headers.                                                                                                                                                                                                           |
| `HEAD` or optional `GET`              | none normally; `PreFinish`/`PostFinish` may run for lazy final-upload materialization                                  | Read paths can materialize or repair final concatenation uploads from complete parts. `PreFinish` is gate-only.                                                                                                                                                                        |

## Runtime Notes

On native targets, protocol traits use `Send` futures suitable for
multi-threaded async runtimes. On `wasm32` targets the bounds relax to
non-`Send` futures automatically; provide backend implementations that match
the target runtime and concurrency model.

The `NoopLocker` is useful when the hosting environment already serializes
access per upload, or in tests. For multi-request native servers, use an actual
locker implementation.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
