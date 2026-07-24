# tus-hook-http

[![crates.io](https://img.shields.io/crates/v/tus-hook-http?style=flat-square)](https://crates.io/crates/tus-hook-http)
[![docs.rs](https://img.shields.io/docsrs/tus-hook-http?style=flat-square)](https://docs.rs/tus-hook-http)

**HTTP webhook hook executor for the [tus resumable upload protocol](https://tus.io/).**

`tus-hook-http` provides a [`tus-protocol`](https://crates.io/crates/tus-protocol)
`HookExecutor` implementation that POSTs hook events to an HTTP endpoint. It can
send custom headers, sign webhook bodies with HMAC-SHA256, and retry transport
failures as well as retryable HTTP responses (`429` and all `5xx` statuses).

Blocking pre-hooks treat non-2xx responses and invalid successful JSON responses
as hook execution errors. Post-hooks are best-effort notifications: failures are
logged and do not affect the already-completed upload operation.

## Install

```toml
[dependencies]
tus-hook-http = "0.0.1"
tus-protocol = "0.0.1"
```

## Quick Start

```rust,no_run
use std::time::Duration;

use tus_hook_http::{HttpHookConfig, HttpHookExecutor};

let config = HttpHookConfig::new("https://example.com/tus-hooks")
    .with_timeout(Duration::from_secs(10))
    .with_header("Authorization", "Bearer hook-token")
    .with_retry(true)
    .with_max_retries(3)
    .with_signing_secret("shared-secret");

let hooks = HttpHookExecutor::new(config).expect("failed to build HTTP client");
```

The executor sends JSON-serialized hook contexts with:

- `Content-Type: application/json`
- `X-Tus-Hook-Event: <event>`
- `X-Tus-Signature-256: sha256=<hex-hmac>` when a signing secret is configured

The payload's `upload` object contains protocol-level upload facts only. It does
not include storage keys or backend-internal storage metadata.

For pre-hooks, return a JSON response whose fields map to
`tus_protocol::PreHookResult`, for example:

```json
{
  "proceed": false,
  "metadata": { "filename": "example.bin" },
  "reject_status": 403,
  "reject_message": "upload rejected"
}
```

`metadata` replaces user metadata only for hook events that allow metadata
changes, such as `PreCreate` and `PreReceive`.

## TLS

The crate's default features enable the bundled `reqwest` client's default TLS
support. Disabling default features (`default-features = false`) drops TLS from
the bundled `reqwest`, so `https://` webhook URLs will fail unless you supply
your own TLS-capable client via `HttpHookExecutor::with_client`. The crate
re-exports `reqwest` (`tus_hook_http::reqwest`) so you can build such a client
against the exact version this crate uses.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
