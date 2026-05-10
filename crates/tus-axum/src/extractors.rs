//! Axum extractors for TUS protocol handling.
//!
//! - [`Headers`] — newtype around [`tus_protocol::Headers`]
//!   with the `Tus-Resumable` validation
//! - [`TusBody`] — body extractor with checksum-trailer support

mod body;
mod headers;
mod upload_id;

pub use body::{BodyData, TusBody};
pub use headers::Headers;
pub use upload_id::UploadId;
