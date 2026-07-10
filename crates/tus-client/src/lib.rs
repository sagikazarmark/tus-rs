//! Async TUS client helpers.
//!
//! This crate provides a small native client for driving TUS uploads from Rust
//! applications. The client works with offset-addressable upload sources and
//! resumable PATCH recovery, which also makes it useful as an end-to-end
//! integration-test partner for `tus-server`.

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]
#![warn(unreachable_pub)]

mod client;
mod error;
mod helpers;
mod runtime;
mod transport;

#[cfg(feature = "checksum")]
pub use client::ChecksumMode;
#[cfg(all(feature = "source-file", not(target_arch = "wasm32")))]
pub use client::FileSource;
#[cfg(not(target_arch = "wasm32"))]
pub use client::ParallelUpload;
pub use client::{
    Client, HeaderProvider, NewUpload, RetryHook, ServerCapabilities, Upload, UploadInfo,
    UploadProgress, UploadSource,
};
pub use error::{BoxError, Error, Result};
pub use transport::{BoxTransport, Transport, TransportBody, TransportRequest, TransportResponse};
#[cfg(feature = "checksum")]
pub use tus_protocol::ChecksumAlgorithm;
// `Extension` is not checksum-specific: `ServerCapabilities::supports_extension`
// takes it regardless of the `checksum` feature, so the re-export must not be
// gated behind `checksum` or callers lose the type the method needs.
pub use tus_protocol::{Extension, MetadataValue, UploadMetadata};

// Re-exported dependency crates whose types appear in this crate's public
// API (`http::HeaderMap` in `Client::with_headers`, `http::Request` behind
// `TransportRequest`, `#[async_trait]` for implementing `Transport`,
// `UploadSource`, and `HeaderProvider`, protocol types like
// `UploadMetadata`, and `url::Url` throughout). Depend on these through the
// re-export so your version can never skew from the one `tus-client` was
// built against.
pub use {async_trait, http, tus_protocol, url};

#[cfg(feature = "transport-reqwest")]
pub use transport::ReqwestTransport;

#[cfg(feature = "transport-reqwest-middleware")]
pub use transport::ReqwestMiddlewareTransport;

/// Re-export of the `reqwest` crate backing the default transport.
#[cfg(feature = "transport-reqwest")]
pub use reqwest;

/// Re-export of the `reqwest-middleware` crate, so consumers can build a
/// [`ClientWithMiddleware`](reqwest_middleware::ClientWithMiddleware) against
/// the exact version [`ReqwestMiddlewareTransport`] wraps, never a skewed one.
#[cfg(feature = "transport-reqwest-middleware")]
pub use reqwest_middleware;
