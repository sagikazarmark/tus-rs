//! Framework-neutral TUS protocol implementation.
//!
//! This module contains the TUS protocol logic without any dependency on a
//! specific HTTP framework. Adapters construct [`Protocol`] with their
//! storage, state, locking, hooks, and configuration, then call the method
//! matching the incoming HTTP request.
//!
//! # Shape
//!
//! Handler methods take already-parsed request inputs:
//!
//! - [`Headers`]: a typed view over TUS-specific request headers
//! - A validated [`UploadId`] path parameter
//! - A [`ChunkStream`](crate::storage::ChunkStream) for the request body
//!
//! They return `Result<Response, Error>`; adapters convert the
//! response into their framework's response type.
//!
//! Upload ID validation is exposed through [`UploadId`] parsing, not through
//! lower-level validation helpers.
//!
//! # Example
//!
//! Framework adapters typically parse the raw HTTP headers, validate path
//! parameters as [`UploadId`], then call the matching [`Protocol`] method.
//!
//! ```rust
//! # fn main() -> tus_protocol::Result<()> {
//! use http::{HeaderMap, HeaderValue};
//! use tus_protocol::protocol::{Headers, UploadId};
//!
//! let mut raw_headers = HeaderMap::new();
//! raw_headers.insert("tus-resumable", HeaderValue::from_static("1.0.0"));
//!
//! let headers = Headers::from_headers(&raw_headers)?;
//! let upload_id: UploadId = "01H8XGJWBWBAQ4SHN3JPHQM6JZ".parse()?;
//!
//! # let _ = (headers, upload_id);
//! # Ok(())
//! # }
//! ```

mod delete;
mod head;
mod headers;
mod options;
mod patch;
mod post;
mod recovery;
mod response;
mod upload_id;

pub use headers::Headers;
pub use response::Response;
pub use upload_id::UploadId;

#[cfg(feature = "fuzzing")]
pub use headers::{
    fuzz_parse_upload_checksum, fuzz_parse_upload_concat, fuzz_parse_upload_metadata,
};

use crate::config::Config;
use crate::hooks::HookExecutor;
use crate::locking::Locker;
use crate::state::StateStore;
use crate::storage::Storage;

/// Framework-neutral TUS protocol facade.
///
/// This type bundles the long-lived protocol dependencies so each handler
/// method only takes request-specific inputs.
pub struct Protocol<'a, S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    config: &'a Config,
    storage: &'a S,
    state_store: &'a I,
    locker: &'a L,
    hooks: &'a H,
}

impl<'a, S, I, L, H> Protocol<'a, S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    /// Creates a protocol facade over the provided dependencies.
    pub fn new(
        config: &'a Config,
        storage: &'a S,
        state_store: &'a I,
        locker: &'a L,
        hooks: &'a H,
    ) -> Self {
        Self {
            config,
            storage,
            state_store,
            locker,
            hooks,
        }
    }
}
