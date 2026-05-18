//! Async TUS client helpers.
//!
//! This crate provides a small native client for driving TUS uploads from Rust
//! applications. The client works with offset-addressable upload sources and
//! resumable PATCH recovery, which also makes it useful as an end-to-end
//! integration-test partner for `tus-server`.

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
pub use tus_protocol::{MetadataValue, UploadMetadata};

#[cfg(feature = "transport-reqwest")]
pub use transport::ReqwestTransport;
