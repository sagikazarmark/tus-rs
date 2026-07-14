//! The small, pure uploader components. Each page in [`crate::pages`] mounts
//! one of these live and quotes its source, so what you read is what runs.

pub mod controls;
pub mod errors;
pub mod existing_url;
pub mod headers;
pub mod minimal;
pub mod options;
mod presentation;
pub mod queue;
pub mod resume;
pub mod resume_persisted;
pub mod transport;

pub(crate) use presentation::{format_bytes_per_sec, format_eta, format_size};
