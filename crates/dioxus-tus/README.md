# dioxus-tus

A headless TUS resumable-upload hook for [Dioxus](https://dioxuslabs.com)
0.7 web apps. Type-safe state via Dioxus `Signal`, chunked PATCH with retry,
pause / resume / abort controls, and resume-from-existing-URL for
server-orchestrated uploads.

```rust,ignore
use dioxus::prelude::*;
use dioxus_tus::{file_from_event, use_tus_upload, TusConfig, TusStartOptions};

#[component]
fn Uploader() -> Element {
    let (state, handle) = use_tus_upload(
        TusConfig::new("https://your-tus-server/files"),
    );

    rsx! {
        input {
            r#type: "file",
            onchange: move |evt| {
                if let Some(file) = file_from_event(&evt) {
                    handle.start(file, TusStartOptions::default());
                }
            }
        }
        if let Some(pct) = state.read().progress_fraction() {
            progress { value: "{(pct * 100.0) as u32}", max: "100" }
        }
    }
}
```

## What you get

- **Type-safe state.** `state.read().is_uploading()`, `progress_fraction()`,
  `bytes_uploaded`, `upload_url`, typed `TusError`. Compare with
  tus-js-client's stringly-typed event names.
- **Chunked PATCH with retry.** Configurable `chunk_size`, `max_retries`,
  exponential backoff. 5xx and transport errors retry; 4xx aborts.
- **Pause / Resume / Abort** at chunk boundaries. Mid-upload `start(other_file)`
  aborts the current upload and starts the new one.
- **Resume from existing URL.** `start_with_url(file, url, options)` for
  uploads where the URL was created server-side or persisted from a prior
  session.
- **Compile-time-validated assets.** Uses `web_sys::File` directly; no
  serialization hop.

## Stability

**Pre-1.0 and exploratory.** Expect breaking changes between releases until
the API stabilises. The `TusConfig` and `TusStartOptions`
types are `#[non_exhaustive]`; adding fields is not a breaking change.
Consumers must use the builder API (`TusConfig::new(endpoint).with_*`)
rather than struct literals.

## CORS requirements

Cross-origin upload requires the server to advertise:

```
Access-Control-Allow-Origin: *          (or your app's origin)
Access-Control-Allow-Headers: tus-resumable, upload-offset, upload-length,
                              upload-metadata, content-type, authorization
Access-Control-Expose-Headers: upload-offset, location, tus-resumable,
                               tus-version, tus-extension, tus-max-size,
                               tus-checksum-algorithm
Access-Control-Allow-Methods: POST, PATCH, HEAD, DELETE, OPTIONS
```

Missing `Access-Control-Expose-Headers: location` is the most common cause
of "the upload POST succeeds but my client never sees the upload URL." It
surfaces as `TusError::MissingHeader("location")` here. The OPTIONS discovery
headers must also be exposed so the hook can read server capabilities before
using `creation-with-upload`.

When the heuristic detects a CORS preflight failure (browser "Failed to
fetch" string), the error surfaces as `TusError::Cors` so consumers can
branch on it specifically, instead of an opaque "Transport(...)".

## Logging

```sh
RUST_LOG=dioxus_tus=debug
```

Hook emits `tracing::debug!`/`info!` at create_upload, each PATCH, retry,
and error mapping.

## Limitations

- **WASM only.** Hook is gated `#[cfg(target_arch = "wasm32")]`.
- **HTTP trailers not supported** by browser Fetch. `TusStartOptions` doesn't
  expose trailer-mode checksums; use header-mode on the underlying
  `tus_client::Client` or omit checksums.
- **Mid-upload bearer-token renewal not supported.** Abort and re-start
  with the new token.

## Example

A complete Dioxus app, a feature-by-feature gallery covering progress UI,
a concurrent upload queue, pause/resume/abort, resume-across-reload, and
per-upload tokens/metadata, with DaisyUI styling, lives in
[`demo/`](../../demo). Each page mounts a live uploader next to the exact
source that produced it.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
