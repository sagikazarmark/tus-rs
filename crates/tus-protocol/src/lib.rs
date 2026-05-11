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
//! The crate is organized around four key traits:
//!
//! - [`Storage`]: Stores upload file data
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
pub use config::{ChecksumAlgorithm, Config, Extension, TUS_RESUMABLE, TUS_VERSION};
pub use error::{Error, Result};
pub use extensions::UploadConcat;
pub use hooks::{
    Hook, HookChain, HookContext, HookEvent, HookExecutor, NoopHookExecutor, PreHookResult,
};
pub use locking::{LockGuard, Locker, NoopLocker};
pub use protocol::{
    DownloadRequest, DownloadResponse, Headers, PatchBody, PatchBodyCollector,
    PatchBodyCollectorFuture, PatchBodyData, PatchChecksum, Protocol, ProtocolHandle, Response,
    UploadId,
};
pub use state::{MetadataValue, StateStore, UploadMetadata, UploadState};
pub use storage::ByteStream;
pub use storage::{ChunkStream, Storage};

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::config::{Config, Extension};
    pub use crate::error::{Error, Result};
    pub use crate::hooks::{HookChain, HookContext, HookEvent, HookExecutor};
    pub use crate::locking::Locker;
    pub use crate::protocol::{
        DownloadRequest, DownloadResponse, Headers, PatchBody, PatchBodyCollector,
        PatchBodyCollectorFuture, PatchBodyData, PatchChecksum, Protocol, ProtocolHandle, Response,
        UploadId,
    };
    pub use crate::state::{StateStore, UploadState};
    pub use crate::storage::Storage;
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
