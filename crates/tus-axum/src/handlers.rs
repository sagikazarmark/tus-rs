//! Axum handlers for TUS protocol endpoints.
//!
//! Each handler is a thin wrapper over the framework-neutral [`tus_protocol::Protocol`].
//! The adapter extracts axum-typed inputs (state, path, headers, body) and converts
//! protocol [`tus_protocol::Response`] values back into axum responses.

mod delete;
mod get;
mod head;
mod method_override;
mod options;
mod patch;
mod post;

pub use delete::handle_delete;
pub use get::handle_get;
pub use head::handle_head;
pub use method_override::handle_post_with_override;
pub use options::handle_options;
pub use patch::handle_patch;
pub use post::handle_post;
