//! axum integration for [`tus_protocol`].
//!
//! This crate carries the axum-specific surface of a TUS server: error
//! conversion ([`Error`]), request extractors ([`extractors`]), the
//! route table ([`router`]), and internal [`tus_protocol`]-backed handlers wired
//! into axum signatures.
//!
//! The protocol logic itself stays in [`tus_protocol`]. This crate is a thin
//! adapter that translates axum requests into the framework-neutral inputs
//! [`tus_protocol`] expects, then translates the framework-neutral outputs
//! back into axum responses.
//!
//! Public adapter types are re-exported from the crate root. Implementation
//! modules such as `state` and `response` are intentionally not part of the
//! public module surface:
//!
//! ```compile_fail
//! use tus_axum::state::TusState;
//! ```
//!
//! ```compile_fail
//! use tus_axum::response::TusResponse;
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
//! let router = create_router(state);
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
//! axum::serve(listener, router).await?;
//! # Ok(())
//! # }
//! ```

pub mod error;
pub mod extractors;
mod handlers;
mod response;
pub mod router;
mod state;

pub use error::Error;
pub use extractors::{Headers, TusBody, UploadId};
pub use response::TusResponse;
pub use router::{build_cors_layer, create_router, create_router_with_download};
pub use state::{TusProtocol, TusState};
