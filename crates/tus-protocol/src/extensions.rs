//! TUS protocol extension types.
//!
//! This module provides types related to TUS protocol extensions.

/// Upload concatenation type.
///
/// Used by the Concatenation extension to identify partial uploads
/// and final uploads that combine multiple partials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadConcat {
    /// This is a partial upload that will be concatenated later.
    Partial,
    /// This is a final upload that concatenates the given partial URLs.
    Final(Vec<String>),
}
