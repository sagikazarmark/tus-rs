# demo

A single Dioxus web app that showcases [`dioxus-tus`](../crates/dioxus-tus)
**feature by feature** and doubles as a docs-by-example gallery. Every page
mounts a real, working uploader with the exact source that produced it, so
the snippet you read is the code that runs. All pages share one router layout,
one endpoint switcher, and one Tailwind/DaisyUI theme (light by default).

## Structure

| Path | Role |
| --- | --- |
| `src/examples/` | Small, pure uploader components, one per feature. Mounted live *and* quoted as the on-page source. |
| `src/examples/presentation.rs` | Upload-specific display helpers such as byte counts, speed, and ETA formatting. |
| `src/pages/` | Route components: prose plus the example's live render and its source. |
| `src/app.rs` | Router (`Route`), the shared shell (header, endpoint switcher, grouped sidebar). |
| `src/components{.rs,/...}` | Project-agnostic docs-gallery presentation grouped by responsibility. |
| `src/endpoint.rs` | Resolves the TUS endpoint, starts the browser-local worker, and shares the endpoint through context. |
| `service-worker/` | Private Rust/WASM TUS endpoint used by the static demo. Runs `tus-protocol`, persists offsets in IndexedDB, and discards uploaded bytes. |
| `public/service-worker.js` | Small JavaScript lifecycle shell that loads the Rust worker and synchronously intercepts fetch events. |
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
| `/transport` | Advanced | `use_tus_upload_with_transport()`: a custom `tus_client::Transport` that logs requests |

Snippets are highlighted at compile time with [`dioxus-code`](https://crates.io/crates/dioxus-code)'s `code!` macro (its tree-sitter parser cross-compiles a C sysroot for wasm, so the build needs `clang`, the devenv shell and the Dagger container both provide it). Styled with [Tailwind CSS](https://tailwindcss.com) + [DaisyUI](https://daisyui.com) using a custom light-default theme (`src/style.css`).

## The TUS endpoint

Every example reads a single endpoint, resolved once at startup in this order:
`?endpoint=...` query string → the `TUS_ENDPOINT` env var baked in at build time
→ the same-origin, app-relative `files` endpoint provided by the demo's service worker. The
header's endpoint switcher re-points the whole demo at another server by
reloading with a fresh `?endpoint=`.

The default endpoint runs entirely in the browser. It executes the real
`tus-protocol` state machine, stores upload metadata and accepted offsets in
IndexedDB so reload/resume works, and discards each upload chunk after it has
been processed. File contents never reach the static hosting server and are not
retained in browser storage. The local endpoint accepts uploads up to 256 MiB
with chunks up to 4 MiB.

## Prerequisites

- Rust with the `wasm32-unknown-unknown` target (`rustup target add wasm32-unknown-unknown`)
- The [Dioxus CLI](https://dioxuslabs.com/learn/0.7/getting_started) (`cargo install dioxus-cli`)
- [`wasm-pack`](https://rustwasm.github.io/wasm-pack/installer/) (`cargo install wasm-pack`)
- [Bun](https://bun.sh) for the Tailwind + DaisyUI stylesheet build

The Dagger path below needs none of the local tooling above, just Dagger.

## Run locally

Install the Tailwind toolchain, build the stylesheet and Rust service worker,
then serve the app:

```sh
bun install
bun run build
dx serve --port 8080
```

`build/style.css` is generated from `src/style.css` and is git-ignored, so run
`bun run build:css` after changing RSX utility classes. `bun run watch` rebuilds
it continuously during development. The worker's generated JavaScript and WASM
under `public/service-worker/` are also ignored; run
`bun run build:service-worker` after changing the worker or `tus-protocol`.

Open `http://localhost:8080`. `localhost` is treated as a secure context, as
required for service workers. Production deployments must use HTTPS. Set an
external upload endpoint from the header switcher, or bake one in with
`TUS_ENDPOINT=https://uploads.example.com/files dx serve`.

To exercise a native server instead of the browser-local endpoint, start one
from the repository root and select `http://localhost:8081/files` in the header:

```sh
cargo run -p tus-server -- serve \
  --addr 0.0.0.0:8081 \
  --base-url http://localhost:8081 \
  --all-extensions \
  --cors \
  --storage-uri "fs:///tmp/tus-data" \
  --state-dir /tmp/tus-state
```

To produce a static bundle instead of serving, run `bun run build` followed by
`dx bundle --platform web`; the bundle is written under `target/dx/` and
contains the service-worker entry point, generated glue, and WASM binary.

## Run with Dagger

Dagger builds and runs everything in containers, no local `dx`, `wasm-pack`,
Bun, Node, or Rust toolchain required. The default demo needs no backend service:

```sh
dagger up
```

Then open `http://localhost:8080`. Uploads use the browser-local Rust worker and
never enter the Dagger container. Point elsewhere anytime with the header's
endpoint switcher.

Run `dagger up server` separately when you want the optional native server on
port 8081. Uploads that server handles live in the container and are discarded
when it stops. To produce the static bundle in a container instead (the Dagger
counterpart of `dx bundle --platform web` above):

```sh
dagger call build export --path ./dist   # static SPA bundle to ./dist
```

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
bun run build                                 # stylesheet + Rust service worker
dagger check                                  # release bundle in a container, as CI does
```
