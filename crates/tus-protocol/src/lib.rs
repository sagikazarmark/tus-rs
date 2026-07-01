//! TUS Resumable Upload Protocol implementation for Rust.
//!
//! This crate provides a complete implementation of the [TUS protocol](https://tus.io/)
//! for resumable file uploads. It's designed to work across different platforms
//! and storage backends through trait abstractions.
//!
//! # Features
//!
//! - Full TUS 1.0.0 protocol support
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
//! - **Expiration**: Expiration timestamps and rejection of expired uploads
//! - **Concatenation**: Parallel uploads that merge server-side
//! - **Checksum**: Verify chunk integrity
//!
//! # Architecture
//!
//! The crate is organized around four required backend traits plus one optional
//! read seam:
//!
//! - [`Storage`]: Stores upload file data for the upload lifecycle
//! - [`StorageReader`]: Optionally reads stored bytes for non-standard download paths
//! - [`StateStore`]: Stores upload metadata and progress
//! - [`Locker`]: Coordinates concurrent access to uploads
//! - [`HookExecutor`]: Executes lifecycle hooks
//!
//! [`Protocol`] bundles those traits with [`Config`] and exposes the
//! framework-neutral protocol handlers adapters call from HTTP integrations.
//!
//! Each trait has multiple implementations available through feature flags:
//!
//! - Storage: `storage::memory`, `storage::file`, and first-party integration crates
//! - State: `state::memory`, `state::file`
//! - Locking: `locking::memory`, `locking::file`
//! - Distributed state+locking via first-party integration crates

#![warn(missing_docs)]
#![warn(clippy::all)]

// Core modules (always available)
pub mod config;
pub mod error;
mod extensions;
pub mod hooks;
pub mod lifecycle;
pub mod locking;
pub mod protocol;
#[doc(hidden)]
pub mod runtime;
pub mod state;
pub mod storage;

// Feature-gated modules
#[cfg(feature = "checksum")]
mod checksum;

// Re-export main types at crate root
#[cfg(feature = "checksum")]
pub use checksum::calculate as calculate_checksum;
pub use config::{ChecksumAlgorithm, Config, Extension, TUS_RESUMABLE, TUS_VERSION};
pub use error::{Error, Result};
pub use extensions::UploadConcat;
pub use hooks::{
    Hook, HookChain, HookContext, HookEvent, HookExecutor, HookRequestInfo, HookUpload,
    NoopHookExecutor, PreHookResult,
};
pub use lifecycle::{
    CreationRequest, CreationTransition, ExpiredUploadReclamationOutcome,
    ExpiredUploadReclamationReport, FinalUploadPlan, FinalUploadStatus, ReceiveProjection,
    ReceiveRequest, apply_receive_commit, ensure_active, load_final_upload_plan,
    load_final_upload_status, prepare_creation, prepare_receive, receive_body_size_limit,
    reclaim_expired_uploads, run_pre_finish, summarize_final_parts, validate_receive_body,
};
pub use locking::{LockGuard, Locker, NoopLocker};
pub use protocol::{
    BodyFrame, BodyStream, DownloadRequest, DownloadResponse, Headers, PatchBody, Protocol,
    ProtocolHandle, RequestBody, Response, UploadId,
};
pub use state::{MetadataValue, StateStore, UploadMetadata, UploadState};
pub use storage::ByteStream;
pub use storage::{
    AppendRequest, ChunkStream, ConcatRequest, Storage, StorageHandle, StorageReader,
};

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::config::{Config, Extension};
    pub use crate::error::{Error, Result};
    pub use crate::hooks::{
        HookChain, HookContext, HookEvent, HookExecutor, HookRequestInfo, HookUpload,
    };
    pub use crate::lifecycle::{
        CreationRequest, CreationTransition, ExpiredUploadReclamationOutcome,
        ExpiredUploadReclamationReport, FinalUploadPlan, FinalUploadStatus, ReceiveProjection,
        ReceiveRequest, prepare_creation, prepare_receive, reclaim_expired_uploads, run_pre_finish,
    };
    pub use crate::locking::Locker;
    pub use crate::protocol::{
        BodyFrame, BodyStream, DownloadRequest, DownloadResponse, Headers, PatchBody, Protocol,
        ProtocolHandle, RequestBody, Response, UploadId,
    };
    pub use crate::state::{StateStore, UploadState};
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
