# demo

A single Dioxus web app that showcases [`dioxus-tus`](../crates/dioxus-tus)
**feature by feature** and doubles as a docs-by-example gallery. Every page
mounts a real, working uploader with the exact source that produced it, so
the snippet you read is the code that runs. All pages share one router layout,
one endpoint switcher, and the shared system light/dark Tailwind/DaisyUI theme.

## Structure

| Path | Role |
| --- | --- |
| `src/examples{.rs,/...}` | Small, pure uploader components, one per feature. Mounted live *and* quoted as the on-page source. |
| `src/examples/presentation.rs` | Upload-specific display helpers such as byte counts, speed, and ETA formatting. |
| `src/pages{.rs,/...}` | Route components: prose plus the example's live render and its source. |
| `src/app.rs` | Router (`Route`), the shared shell (header, endpoint switcher, grouped sidebar). |
| `src/components{.rs,/...}` | Project-agnostic docs-gallery presentation grouped by responsibility. |
| `src/endpoint.rs` | Resolves the TUS endpoint, starts the browser-local worker, and shares the endpoint through context. |
| `service-worker/` | Private Rust/WASM TUS endpoint used by the static demo. Runs `tus-protocol`, persists offsets in IndexedDB, and discards uploaded bytes. |
| `public/service-worker.js` | Small JavaScript lifecycle shell that loads the Rust worker and synchronously intercepts fetch events. |
| `wrangler.toml` | Asset-only Cloudflare Workers deployment; it has no server script or storage bindings. |
| `src/style.css` | Tailwind/DaisyUI source compiled to the ignored `build/style.css` asset. |

## Pages

| Path | Section | What it shows |
| --- | --- | --- |
| `/` | Basics | Overview + quick start |
| `/minimal` | Basics | `use_tus_upload`: a file input and a progress bar |
| `/queue` | Uploading | `use_tus_upload_queue`: concurrent drag-and-drop queue with speed/ETA |
| `/controls` | Uploading | `pause()` / `resume()` / `abort()` at chunk boundaries |
| `/options` | Configuration | `TusConfig` (chunk size, retries, backoff, cwu threshold) + per-upload `TusStartOptions` (token, metadata) |
| `/headers` | Configuration | `with_header()`, `with_filename()`, `with_content_type()` |
| `/resume` | Resuming | `scan_resumable()` + `resume_entry()` + `resume_persisted()`: resume across a tab reload |
| `/existing-url` | Resuming | `start_with_url()` / `with_existing_url()` + `state.upload_url`: resume a server-issued URL |
| `/errors` | Advanced | Branching on the typed `TusError` surface (CORS, server status, oversize, …) |
| `/transport` | Advanced | `use_tus_upload_with_transport()`: a custom `tus_uploader::Transport` that logs requests |

Snippets are highlighted at compile time with [`dioxus-code`](https://crates.io/crates/dioxus-code)'s `code!` macro (its tree-sitter parser cross-compiles a C sysroot for wasm, so the build needs `clang`; the devenv shell and Dagger container both provide it). Styling uses the shared system light/dark [Tailwind CSS](https://tailwindcss.com) + [DaisyUI](https://daisyui.com) baseline from `src/style.css`.

## The TUS endpoint

Every example reads a single endpoint, resolved once at startup in this order:
`?endpoint=...` query string → the `TUS_ENDPOINT` env var baked in at build time
→ the build's default. Local builds default to `http://localhost:8081/files`;
Cloudflare builds default to the same-origin, app-relative `files` endpoint
provided by the demo's service worker. The header can toggle browser-local mode
or re-point the whole demo at another server. Either action reloads with a fresh
`?endpoint=`.

The browser-local endpoint runs entirely in the browser. It executes the real
`tus-protocol` state machine, stores upload metadata and accepted offsets in
IndexedDB so reload/resume works, and discards each upload chunk after it has
been processed. File contents never reach the static hosting server and are not
retained in browser storage. The local endpoint accepts uploads up to 256 MiB
with chunks up to 4 MiB.

## Prerequisites

- Rust with the `wasm32-unknown-unknown` target (`rustup target add wasm32-unknown-unknown`)
- The [Dioxus CLI](https://dioxuslabs.com/learn/0.7/getting_started) (`cargo install dioxus-cli --version 0.7.9 --locked`)
- Node and npm for the Tailwind + DaisyUI stylesheet build
- [Dagger](https://docs.dagger.io/install/) for the Rust service-worker build

The fully containerized path below needs only Dagger.

## Run locally

Install the Tailwind toolchain, build the stylesheet, export the Rust service
worker from Dagger, then serve the app:

```sh
cd demo
npm ci
npm run build
dagger call service-worker export --path ./public/service-worker
dx serve --platform web --port 8080
```

`build/style.css` is generated from `src/style.css` and is git-ignored, so run
`npm run build` after changing RSX utility classes. `npm run watch` rebuilds
it continuously during development. The worker's generated JavaScript and WASM
under `public/service-worker/` are also ignored; export them again with Dagger
after changing the worker or `tus-protocol`.

Open `http://localhost:8080`. The local build defaults to the native server at
`http://localhost:8081/files`; start that server as shown below, enable the
browser-local toggle, or bake in another endpoint with
`TUS_ENDPOINT=https://uploads.example.com/files dx serve`. `localhost` is
treated as a secure context, as required for service workers. Production
deployments must use HTTPS.

For non-Dagger local development, start the native server from the repository
root:

```sh
cargo run -p tus-server -- serve \
  --addr 0.0.0.0:8081 \
  --base-url http://localhost:8081 \
  --all-extensions \
  --cors \
  --storage-uri "fs:///tmp/tus-data" \
  --state-dir /tmp/tus-state
```

To produce a static bundle instead of serving, build the stylesheet and export
the service worker as above, then run `dx bundle --platform web`. The bundle is
written under `target/dx/` and contains the service-worker entry point,
generated glue, and WASM binary.

## Run with Dagger

Dagger builds and runs everything in containers, no local `dx`, `wasm-pack`,
Node, npm, Wrangler, or Rust toolchain required. Start the demo and this
repository's native `tus-server` together:

```sh
cd demo
dagger up
```

Then open `http://localhost:8080`. Uploads default to the real `tus-server` on
port 8081. Its data lives in the container and is discarded when Dagger stops.
Use the header toggle to switch to the browser-local Rust worker, or point to
another endpoint with the adjacent field. To run only one service, use
`dagger up service` or `dagger up server`.

The local service and generic bundle default to the native endpoint. Override
that build-time default with `dagger call service --browser-local up`. To
produce the static bundle in a container instead (the Dagger counterpart of
`dx bundle --platform web` above):

```sh
dagger call build export --path ./dist   # static SPA bundle to ./dist
dagger call service-worker export --path ./public/service-worker
```

The second command exports only the generated service-worker JavaScript and
WASM for use with a local Dioxus build.

## Cloudflare Workers

The Cloudflare target is the same browser-only static bundle. Cloudflare serves
the Dioxus app and browser Service Worker as assets; there is no Cloudflare
Worker script, R2 bucket, Durable Object, or other upload storage. Upload chunks
are intercepted and discarded in the browser as described above.

Build or run that deployment locally with Dagger:

```sh
dagger call worker build export --path ./dist-cloudflare
dagger call worker dev up
```

Deploy it with explicit Cloudflare credentials:

```sh
dagger call worker deploy \
  --account-id "$CLOUDFLARE_ACCOUNT_ID" \
  --api-token env://CLOUDFLARE_API_TOKEN
```

The deployment config uses `tus-demo.dioxus.cc` as its custom domain and also
enables `workers.dev` and preview URLs. CI deploys pushes to `main` to production
and uploads same-repository pull requests as preview versions. Both deployment
jobs need `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN` repository secrets.

## CORS

The browser-local endpoint is same-origin and needs no CORS configuration. If
an external TUS server runs on a different origin than the app, configure it to
allow browser cross-origin requests:

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Headers: tus-resumable, upload-offset, upload-length, upload-metadata, content-type, authorization
Access-Control-Expose-Headers: upload-offset, location, tus-resumable, tus-version, tus-extension, tus-max-size, tus-checksum-algorithm
Access-Control-Allow-Methods: POST, PATCH, HEAD, DELETE, OPTIONS
```

The built-in `tus-server` (this repo) handles CORS automatically with `--cors`.

## Verify

Build-only checks, without serving anything:

```sh
cargo check --target wasm32-unknown-unknown   # the wasm client
cargo check -p demo-service-worker --target wasm32-unknown-unknown
npm run build                                 # stylesheet
dagger call service-worker entries            # Rust service worker
dagger check                                  # web and Cloudflare release bundles, as CI does
```
