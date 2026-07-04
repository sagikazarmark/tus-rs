//! axum integration for [`tus_protocol`].
//!
//! This crate supports **axum 0.8.x**.
//!
//! This crate carries the axum-specific surface of a TUS server: error
//! conversion ([`Error`]), request extractors ([`Headers`], [`TusBody`],
//! [`UploadId`]), the route table ([`create_router`] and friends, configured
//! through [`RouterOptions`]), and internal [`tus_protocol`]-backed handlers
//! wired into axum signatures.
//!
//! The protocol logic itself stays in [`tus_protocol`]. This crate is a thin
//! adapter that translates axum requests into the framework-neutral inputs
//! [`tus_protocol`] expects, then translates the framework-neutral outputs
//! back into axum responses.
//!
//! HTTP-adapter concerns such as CORS are configured through
//! [`RouterOptions`] (see [`create_router_with_options`]), not through
//! [`tus_protocol::Config`].
//!
//! Public adapter types are re-exported from the crate root, which is the
//! only stable path for them. Implementation modules such as `state`,
//! `response`, `error`, `extractors`, and `router` are intentionally not
//! part of the public module surface:
//!
//! ```compile_fail
//! use tus_axum::state::TusState;
//! ```
//!
//! ```compile_fail
//! use tus_axum::response::TusResponse;
//! ```
//!
//! ```compile_fail
//! use tus_axum::error::Error;
//! ```
//!
//! ```compile_fail
//! use tus_axum::extractors::Headers;
//! ```
//!
//! ```compile_fail
//! use tus_axum::router::create_router;
//! ```
//!
//! `handlers` is an internal adapter wiring module. Use [`create_router`] or
//! [`create_router_with_download`] instead of importing handler functions:
//!
//! ```compile_fail
//! use tus_axum::handlers::handle_post;
//! ```
//!
//! # Example
//!
//! [`create_router`] builds the standard upload route table. Non-standard GET
//! downloads are opt-in through [`create_router_with_download`] and require a
//! storage adapter that implements [`tus_protocol::StorageReader`].
//!
//! See [`examples/server.rs`] for a complete runnable server.
//!
//! [`examples/server.rs`]: https://github.com/sagikazarmark/tus-rs/blob/main/crates/tus-axum/examples/server.rs
//!
//! ```rust,no_run
//! # use tus_axum::{create_router, TusState};
//! # use tus_protocol::{
//! #     Config, NoopHookExecutor, ProtocolHandle,
//! #     locking::memory::MemoryLocker,
//! #     state::memory::MemoryStateStore,
//! #     storage::memory::MemoryStorage,
//! # };
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let protocol = ProtocolHandle::new(
//!     Config::default(),
//!     MemoryStorage::new(),
//!     MemoryStateStore::new(),
//!     MemoryLocker::new(),
//!     NoopHookExecutor::new(),
//! );
//! let state = TusState::new(protocol);
//! let router = create_router(state)?;
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
//! axum::serve(listener, router).await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]
#![warn(unreachable_pub)]

mod error;
mod extractors;
mod handlers;
mod response;
mod router;
mod state;

// Re-exported dependency crates whose types appear in this crate's public
// API. Depend on these through the re-export so your version can never skew
// from the one `tus-axum` was built against.
pub use {axum, tus_protocol};

pub use error::Error;
pub use extractors::{Headers, TusBody, UploadId};
pub use response::TusResponse;
pub use router::{
    RouterError, RouterOptions, create_router, create_router_with_download,
    create_router_with_download_and_options, create_router_with_options,
};
pub use state::{TusProtocol, TusState};
