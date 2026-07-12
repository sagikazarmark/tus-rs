# tus-server

[![crates.io](https://img.shields.io/crates/v/tus-server?style=flat-square)](https://crates.io/crates/tus-server)
[![docs.rs](https://img.shields.io/docsrs/tus-server?style=flat-square)](https://docs.rs/tus-server)

**Standalone server for the [tus resumable upload protocol](https://tus.io/).**

`tus-server` provides the `tus-server` binary: an HTTP server implementing the
TUS protocol on top of [`tus-protocol`](https://crates.io/crates/tus-protocol)
and [`tus-axum`](https://crates.io/crates/tus-axum), with OpenDAL-backed
storage, file-based upload state, optional webhooks, and expiration cleanup.

## Install

```bash
cargo install tus-server
```

## Quick Start

```bash
cargo run -p tus-server -- serve --addr 127.0.0.1:8080
```

Configuration resolves in precedence order: defaults < config file (`--config`,
TOML or YAML) < `TUS_*` environment variables < CLI flags. Run
`tus-server serve --help` for the full list.

### Safe defaults

A stock server bounds resource usage out of the box. Pass `0` to any of these
to explicitly opt out:

| Setting | Default | Meaning of `0` |
| --- | --- | --- |
| `--max-request-body-bytes` | 1 GiB | unlimited request bodies |
| `--request-body-read-timeout` | 60 s | stalled bodies are never timed out |
| `--max-chunk-size` | 256 MiB | unlimited per-PATCH chunk size |

### Running behind a proxy

Behind a TLS-terminating reverse proxy, either `--base-url` or
`--respect-forwarded-headers` is required for correct absolute `Location`
URLs. `--respect-forwarded-headers` trusts `Forwarded`/`X-Forwarded-*` headers
and is off by default; enable it only when a trusted proxy sets those headers.

## Deployment constraints

**Run a single server instance per storage bucket and state directory.**
Upload locking uses a process-local in-memory locker, so two replicas sharing
the same bucket or state directory cannot see each other's locks and will race
on concurrent PATCH requests, terminations, and expiration cleanup. File-based
locking would only help when every instance shares the same local filesystem;
it does not make object-storage backends safe for multiple replicas.

The `cleanup` subcommand is subject to the same constraint: it builds its own
memory locker and cannot see locks held by a running server. Stop the server
first, then run `tus-server cleanup --force` (or set `TUS_CLEANUP_FORCE=true`)
to acknowledge that. Without `--force` the command refuses to run.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
