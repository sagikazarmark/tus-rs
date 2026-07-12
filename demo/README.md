# demo

A single Dioxus web app that showcases [`dioxus-tus`](../crates/dioxus-tus)
**feature by feature** and doubles as a docs-by-example gallery. Every page
mounts a real, working uploader next to the exact source that produced it, so
the snippet you read is the code that runs. All pages share one router layout,
one endpoint switcher, and one Tailwind/DaisyUI theme (light by default).

## Structure

| Path | Role |
| --- | --- |
| `src/examples/` | Small, pure uploader components, one per feature. Mounted live *and* quoted as the on-page source. |
| `src/pages/` | Route components: prose plus the example's live render and its source. |
| `src/app.rs` | Router (`Route`), the shared shell (header, endpoint switcher, grouped sidebar). |
| `src/ui.rs` | Presentation-only helpers (`PageHeader`, `ExampleSection`, `InlineCode`, `SourcePanel`, formatters). |
| `src/endpoint.rs` | Resolves the TUS endpoint and shares it through context. |

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

Snippets are highlighted at compile time with [`dioxus-code`](https://crates.io/crates/dioxus-code)'s `code!` macro (its tree-sitter parser cross-compiles a C sysroot for wasm, so the build needs `clang`, the devenv shell and the Dagger container both provide it). Styled with [Tailwind CSS](https://tailwindcss.com) + [DaisyUI](https://daisyui.com) using a custom light-default theme (`style.css`).

## The TUS endpoint

Every example reads a single endpoint, resolved once at startup in this order:
`?endpoint=...` query string → the `TUS_ENDPOINT` env var baked in at build time
→ `http://localhost:8081/files`. The header's endpoint switcher re-points the
whole demo at another server by reloading with a fresh `?endpoint=`.

## Prerequisites

- Rust with the `wasm32-unknown-unknown` target (`rustup target add wasm32-unknown-unknown`)
- The [Dioxus CLI](https://dioxuslabs.com/learn/0.7/getting_started) (`cargo install dioxus-cli`)
- [Bun](https://bun.sh) (or Node) for the Tailwind + DaisyUI stylesheet build
- A running TUS server (e.g. `tus-server` from this repo, or [tusd](https://github.com/tus/tusd))

The Dagger path below needs none of the local tooling above, just Dagger.

## Run locally

Start a TUS server on port 8081:

```sh
cargo run -p tus-server -- serve \
  --addr 0.0.0.0:8081 \
  --base-url http://localhost:8081 \
  --all-extensions \
  --cors \
  --storage-uri "fs:///tmp/tus-data" \
  --state-dir /tmp/tus-state
```

Install the Tailwind toolchain, build the stylesheet, then serve the app:

```sh
bun install
bun run build           # or `bun run watch` in a second terminal
dx serve --port 8080
```

Open `http://localhost:8080`. Set the upload endpoint from the header switcher,
or bake a default in with `TUS_ENDPOINT=http://localhost:8081/files dx serve`.

To produce a static bundle instead of serving, run `dx bundle --platform web`;
the bundle is written under `target/dx/`.

## Run with Dagger

Dagger builds and runs everything in containers, no local `dx`, Bun, Node, or
even a Rust toolchain required. The demo is a **client**, so it needs a TUS
server to upload to; both are declared as `@up` services, so one command starts
both and tunnels their ports:

```sh
dagger up
```

Then open `http://localhost:8080`: the demo (`serve`) runs there and uploads to
this repo's `tus-server` (`server`) on `localhost:8081`. The two ports differ on
purpose: with the demo on the same port as its endpoint it would upload to its
own dev server and get a `405`. Point elsewhere anytime with the header's
endpoint switcher.

Start just one with `dagger up serve` or `dagger up server`; uploads the server
handles live in the container and are discarded when it stops. To produce that
same static bundle in a container instead (the Dagger counterpart of
`dx bundle --platform web` above):

```sh
dagger call build export --path ./dist   # static SPA bundle to ./dist
```

## CORS

If the TUS server runs on a different origin than the app, configure it to allow browser cross-origin requests:

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
dagger check                                  # release bundle in a container, as CI does
```
