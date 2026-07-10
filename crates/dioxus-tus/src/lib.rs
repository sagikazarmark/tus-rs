//! Headless TUS upload hook for Dioxus web.
//!
//! Type-safe state via Dioxus `Signal<TusUploadState>` (`is_uploading()`,
//! `progress_fraction()`, etc.) plus chunked PATCH with retry, pause/resume/abort
//! controls, and resume-from-existing-URL for server-orchestrated uploads.
//!
//! # Quick start
//!
//! ```rust,ignore
//! use dioxus::prelude::*;
//! use dioxus_tus::{file_from_event, use_tus_upload, TusConfig, TusStartOptions};
//!
//! #[component]
//! fn Uploader() -> Element {
//!     let (state, handle) = use_tus_upload(
//!         TusConfig::new("https://your-tus-server/files"),
//!     );
//!
//!     rsx! {
//!         input {
//!             r#type: "file",
//!             onchange: move |evt| {
//!                 if let Some(file) = file_from_event(&evt) {
//!                     handle.start(file, TusStartOptions::default());
//!                 }
//!             }
//!         }
//!         if let Some(pct) = state.read().progress_fraction() {
//!             progress { value: "{(pct * 100.0) as u32}", max: "100" }
//!         }
//!     }
//! }
//! ```
//!
//! # Bearer tokens
//! Default token in [`TusConfig::with_bearer_token`] applies to every upload;
//! override per-upload via [`TusStartOptions::bearer_token_override`].
//!
//! **Mid-upload token renewal is not supported yet.** The token is applied
//! once when the upload starts and cannot be changed while the chunk loop is
//! running. To use a refreshed token, call [`TusUploadHandle::abort`] and then
//! [`TusUploadHandle::start`] again with the new token.
//!
//! # CORS requirements
//! Your TUS server must allow browser cross-origin requests. At minimum:
//!
//! ```text
//! Access-Control-Allow-Origin: *          (or your app's origin)
//! Access-Control-Allow-Headers: tus-resumable, upload-offset, upload-length,
//!                               upload-metadata, content-type, authorization
//! Access-Control-Expose-Headers: upload-offset, location, tus-resumable,
//!                                tus-version, tus-extension, tus-max-size,
//!                                tus-checksum-algorithm
//! Access-Control-Allow-Methods: POST, PATCH, HEAD, DELETE, OPTIONS
//! ```
//!
//! Missing `Access-Control-Expose-Headers` is the most common cause of silent
//! failures where the upload POST succeeds but the `Location` header is not
//! readable by the browser, surfacing as [`TusError::MissingHeader`].
//!
//! When the heuristic detects a CORS preflight failure (browser "Failed to
//! fetch" string), the error surfaces as [`TusError::Cors`] specifically.
//!
//! # Limitations
//! - **WASM only.** This crate targets `wasm32-unknown-unknown` (browser).
//!   Native targets (Dioxus desktop / fullstack) are not supported yet.
//! - **HTTP trailers not supported.** Browser Fetch does not support HTTP
//!   trailers. The `TusStartOptions` surface here doesn't expose trailer
//!   checksum mode at all; use header-mode checksums on the
//!   underlying `tus_client::Client`, or omit checksums entirely.
//! - **Mid-upload bearer-token renewal not supported.** The token is applied
//!   once when the upload starts. Call [`TusUploadHandle::abort`] then
//!   [`TusUploadHandle::start`] again with the new token to renew.
//! - **Main-thread blob reads.** Each PATCH chunk is read from the browser
//!   `Blob` on the main thread. Files larger than ~100 MB at the default
//!   1 MiB chunk size may show UI jank; Web Worker offload is a future
//!   enhancement.
//!
//! # Troubleshooting / FAQ
//!
//! ### "TypeError: Failed to fetch" / [`TusError::Cors`] on every request
//! Almost always a CORS misconfiguration on the server. Check that the
//! preflight (OPTIONS) returns:
//! - `Access-Control-Allow-Origin` matching your app's origin (or `*`).
//! - `Access-Control-Allow-Headers` including the TUS request headers
//!   (`tus-resumable`, `upload-offset`, `upload-length`, `upload-metadata`,
//!   `content-type`, plus `authorization` if you use bearer tokens).
//! - `Access-Control-Allow-Methods` including `POST`, `PATCH`, `HEAD`,
//!   `DELETE`, `OPTIONS`.
//!
//! Common server-specific causes:
//! - **`tusd`**: pass `-cors=true` (or `-cors-allow-origin=https://app.example.com`).
//! - **nginx in front of TUS**: `proxy_pass_header Location;` is required;
//!   otherwise the `Location` header is stripped and you'll see
//!   [`TusError::MissingHeader`] for `location` instead of `Cors`.
//! - **CloudFront/etc.**: typically needs an explicit OPTIONS-method allow
//!   list and forwarded headers.
//!
//! ### Upload POST succeeds but the URL is missing
//! [`TusError::MissingHeader`] for `location`: the server sends the header
//! but the browser hides it because `Access-Control-Expose-Headers` is
//! missing. Add `location`, `tus-resumable`, `upload-offset`, and the
//! OPTIONS discovery headers (`tus-version`, `tus-extension`,
//! `tus-max-size`, `tus-checksum-algorithm`) to the `Expose-Headers` list.
//!
//! ### Where do I see what the hook is doing?
//! Set `RUST_LOG=dioxus_tus=debug` (in your dev environment) or hook the
//! `tracing` subscriber up the way your Dioxus app expects. The chunk
//! loop emits a span at `create_upload`, each PATCH, each retry, and on
//! error mapping.
//!
//! ### Why does my upload restart every time I refresh the tab?
//! Resume across reload requires the stored entry to match the same file
//! (by name + size + last-modified). If any of those changed (e.g. the
//! browser reports a different `lastModified`), the rebind misses and a
//! fresh upload starts. Use [`TusUploadHandle::scan_resumable`] on mount
//! to inspect what's persisted.
//!
//! ### How do I test the hook with a mock server?
//! Use [`use_tus_upload_with_transport`] and supply your own
//! [`tus_client::Transport`] implementation that returns recorded
//! responses.
//!
//! # Stability
//! Pre-1.0 and exploratory. Expect breaking changes between releases until
//! the API stabilises.

// The hook, handle, and transport APIs are `#[cfg(target_arch = "wasm32")]`, so
// intra-doc links to them only resolve for the wasm target. docs.rs and CI
// build this crate for wasm (see the `package.metadata.docs.rs` table), where
// these links are still validated under `-D warnings`. On other targets, the
// host doc builds used by some CI checks, those items don't exist, so tolerate
// the unresolvable links there rather than failing the build.
#![cfg_attr(not(target_arch = "wasm32"), allow(rustdoc::broken_intra_doc_links))]

pub mod config;
pub mod persistence;
pub mod retry;
pub mod state;

// WASM-only modules: transport, blob reader, the Dioxus hook, and the helper
// for extracting `web_sys::File`s from form events.
// Not compiled for native test builds (cargo test --tests).
#[cfg(target_arch = "wasm32")]
mod blob;
#[cfg(target_arch = "wasm32")]
mod event;
#[cfg(target_arch = "wasm32")]
mod hook;
#[cfg(target_arch = "wasm32")]
mod options_cache;
#[cfg(target_arch = "wasm32")]
mod queue;
#[cfg(target_arch = "wasm32")]
pub mod transport;

/// Re-export of the underlying [`tus_client`] crate.
///
/// The custom-transport use case ([`use_tus_upload_with_transport`], which is
/// generic over [`tus_client::Transport`]) requires naming trait and types
/// from this crate. Re-exporting it lets consumers do so without adding, and
/// version-matching, their own direct `tus_client` dependency.
pub use tus_client;

pub use config::{TusConfig, TusStartOptions};
pub use state::{TusError, TusUploadState, UploadStatus};

#[cfg(target_arch = "wasm32")]
pub use event::{file_from_event, files_from_drag_event, files_from_event};
#[cfg(target_arch = "wasm32")]
pub use hook::{TusUploadHandle, use_tus_upload, use_tus_upload_with_transport};
#[cfg(target_arch = "wasm32")]
pub use queue::{
    QueueItemStatus, TusQueueHandle, TusQueueItem, TusQueueState, use_tus_upload_queue,
};
