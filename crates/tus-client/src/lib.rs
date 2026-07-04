//! Async TUS client helpers.
//!
//! This crate provides a small native client for driving TUS uploads from Rust
//! applications. The client works with offset-addressable upload sources and
//! resumable PATCH recovery, which also makes it useful as an end-to-end
//! integration-test partner for `tus-server`.

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![warn(missing_docs)]

mod client;
mod error;
mod helpers;
mod runtime;
mod transport;

#[cfg(feature = "checksum")]
pub use client::ChecksumMode;
#[cfg(all(feature = "source-file", not(target_arch = "wasm32")))]
pub use client::FileSource;
pub use client::{
    Client, HeaderProvider, NewUpload, ParallelUpload, RetryHook, ServerCapabilities, Upload,
    UploadInfo, UploadProgress, UploadSource,
};
pub use error::{BoxError, Error, Result};
pub use transport::{BoxTransport, Transport, TransportBody, TransportRequest, TransportResponse};
#[cfg(feature = "checksum")]
pub use tus_protocol::ChecksumAlgorithm;
pub use tus_protocol::{MetadataValue, UploadMetadata};

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

/// Re-export of the `reqwest` crate backing the default transport.
#[cfg(feature = "transport-reqwest")]
pub use reqwest;
