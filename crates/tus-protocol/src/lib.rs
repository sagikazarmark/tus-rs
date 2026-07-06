//! TUS Resumable Upload Protocol implementation for Rust.
//!
//! This crate provides a complete implementation of the [TUS protocol](https://tus.io/)
//! for resumable file uploads. It's designed to work across different platforms
//! and storage backends through trait abstractions.
//!
//! # Features
//!
//! - TUS 1.0.0 core protocol and standard extension support
//! - Extensible storage backends
//! - Pluggable state storage
//! - Distributed locking support
//! - Flexible hook system for customization
//!
//!
//! # TUS Extensions
//!
//! This implementation supports the following TUS extensions:
//!
//! - **Creation**: Create new uploads via POST
//! - **Creation-With-Upload**: Include data in initial POST
//! - **Creation-Defer-Length**: Create without knowing size upfront
//! - **Termination**: Cancel/delete uploads via DELETE
//! - **Expiration**: Expiration timestamps and rejection of protocol-expired
//!   unfinished/intermediate uploads
//! - **Concatenation**: Parallel uploads that merge server-side
//! - **Checksum**: Verify chunk integrity
//!
//! Non-standard conveniences, such as download routes built on [`StorageReader`]
//! and the `concatenation-unfinished` token, are explicit opt-ins and should not
//! be treated as part of the stable tus protocol contract.
//!
//! # Architecture
//!
//! The crate is organized around four required backend traits plus optional
//! operational seams:
//!
//! - [`Storage`]: Stores upload file data for the upload lifecycle
//! - [`StorageReader`]: Optionally reads stored bytes for non-standard download paths
//! - [`StateStore`]: Stores upload metadata and progress
//! - [`UploadInventory`]: Optionally enumerates upload IDs for operational tooling
//! - [`Locker`]: Coordinates concurrent access to uploads
//! - [`HookExecutor`]: Executes lifecycle hooks
//!
//! Implement these traits with the crate's re-exported `#[async_trait]` macro.
//! tus-protocol deliberately keeps the `async_trait` shape as stable API rather
//! than native `async fn` in traits, so each method's returned future can keep a
//! `Send` bound on native targets and drop it on `wasm32` — a conditional bound
//! that native async-fn-in-trait cannot yet express.
//!
//! [`Protocol`] bundles those traits with [`Config`] and exposes the
//! framework-neutral protocol handlers adapters call from HTTP integrations.
//! Expired upload reclamation is the root-level operational cleanup interface
//! for servers that run background or one-shot cleanup.
//! Lower-level lifecycle transition helpers are internal implementation behind
//! that facade:
//!
//! ```compile_fail
//! use tus_protocol::lifecycle::prepare_creation;
//! ```
//!
//! Each trait has multiple implementations available through feature flags:
//!
//! - Storage: `storage::memory`, `storage::file`, and first-party integration crates
//! - State: `state::memory`, `state::file`
//! - Locking: `locking::memory`, `locking::file`
//! - Distributed state+locking via first-party integration crates

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]
#![warn(unreachable_pub)]
#![warn(clippy::all)]

// Core modules (always available)
pub mod config;
pub mod error;
mod expiration;
mod extensions;
pub mod hooks;
mod lifecycle;
pub mod locking;
pub mod protocol;
pub mod runtime;
pub mod state;
pub mod storage;

// Feature-gated modules
#[cfg(feature = "checksum")]
mod checksum;

// Re-exported dependency crates whose types appear in this crate's public
// API. Depend on these through the re-export so your version can never skew
// from the one `tus-protocol` was built against.
//
// Only `futures_core::Stream` appears in this crate's public signatures
// (`ByteStream`, `BodyStream`), so `futures_core` — not the full `futures`
// umbrella — is the re-exported streaming surface.
pub use {async_trait, bytes, chrono, futures_core, http, serde};

// Re-export main types at crate root
#[cfg(feature = "checksum")]
pub use checksum::{Hasher as ChecksumHasher, calculate as calculate_checksum};
pub use config::{ChecksumAlgorithm, Config, Extension, TUS_RESUMABLE, TUS_VERSION};
pub use error::{Error, ErrorResponse, Result};
pub use extensions::UploadConcat;
pub use hooks::{
    Hook, HookChain, HookContext, HookEvent, HookExecutor, HookRequestInfo, HookUpload,
    NoopHookExecutor, PreHookResult,
};
pub use lifecycle::{
    ExpiredUploadReclamationOutcome, ExpiredUploadReclamationReport, reclaim_expired_uploads,
};
pub use locking::{LockGuard, Locker, NoopLocker};
pub use protocol::{
    BodyFrame, BodyStream, DownloadRequest, DownloadResponse, Headers, Protocol, ProtocolHandle,
    RequestBody, Response, TUS_SUCCESS_RESPONSE_HEADERS, UploadChecksum, UploadId,
};
pub use state::{
    MetadataValue, StateStore, UPLOAD_STATE_SCHEMA_VERSION, UploadInventory, UploadMetadata,
    UploadState, WriteMode,
};
pub use storage::{
    AppendRequest, ByteStream, ChunkStream, ConcatRequest, Storage, StorageHandle, StorageReader,
};

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::config::{Config, Extension};
    pub use crate::error::{Error, Result};
    pub use crate::hooks::{
        HookChain, HookContext, HookEvent, HookExecutor, HookRequestInfo, HookUpload,
    };
    pub use crate::lifecycle::{
        ExpiredUploadReclamationOutcome, ExpiredUploadReclamationReport, reclaim_expired_uploads,
    };
    pub use crate::locking::Locker;
    pub use crate::protocol::{
        BodyFrame, BodyStream, DownloadRequest, DownloadResponse, Headers, Protocol,
        ProtocolHandle, RequestBody, Response, UploadChecksum, UploadId,
    };
    pub use crate::state::{StateStore, UploadInventory, UploadState, WriteMode};
    pub use crate::storage::{AppendRequest, ConcatRequest, Storage, StorageHandle, StorageReader};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_constant() {
        assert_eq!(TUS_VERSION, "1.0.0");
        assert_eq!(TUS_RESUMABLE, "1.0.0");
    }
}
