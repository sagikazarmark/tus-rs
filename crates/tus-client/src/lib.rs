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
pub use error::{Error, Result};
pub use transport::{Transport, TransportBody, TransportRequest, TransportResponse};
#[cfg(feature = "checksum")]
pub use tus_protocol::ChecksumAlgorithm;
pub use tus_protocol::{MetadataValue, UploadMetadata};

/// Re-export of the `url` crate used in the public API.
pub use url;

#[cfg(feature = "transport-reqwest")]
pub use transport::ReqwestTransport;

/// Re-export of the `reqwest` crate backing the default transport.
#[cfg(feature = "transport-reqwest")]
pub use reqwest;
