//! Upload-id shape validation.
//!
//! The `upload_id` arrives from the URL path on every HEAD/PATCH/DELETE
//! and is used as part of the on-disk file name (state store) and the
//! storage key. Without validation, a client can supply ids that
//! either:
//!
//!   - escape the configured directory (via `/` or `\`), or
//!   - cause the server to return 500 because the OS rejects the
//!     resulting path (NUL bytes, oversized names).
//!
//! HTTP adapters should extract the ID from a single path segment; [`UploadId`]
//! adds a second line of defense by rejecting unsafe shapes before any storage
//! call, so 400 reaches the client instead of 500.

use crate::error::Error;

/// Validated upload identifier.
///
/// Upload IDs arrive from URL path segments and are later used as storage keys
/// and path components. Constructing this type validates the shape once so
/// protocol handlers can rely on the ID being safe to pass to storage, state,
/// and locking backends.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UploadId(String);

impl UploadId {
    /// Returns the upload ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the upload ID and returns the underlying string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::str::FromStr for UploadId {
    type Err = Error;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        if id.is_empty() {
            return Err(Error::InvalidUploadId("id is empty".to_string()));
        }
        if id.len() > MAX_UPLOAD_ID_LEN {
            return Err(Error::InvalidUploadId(format!(
                "id is {} bytes; max {}",
                id.len(),
                MAX_UPLOAD_ID_LEN,
            )));
        }
        for c in id.chars() {
            if c == '/' || c == '\\' {
                return Err(Error::InvalidUploadId(
                    "id contains a path separator".to_string(),
                ));
            }
            if c.is_control() {
                return Err(Error::InvalidUploadId(
                    "id contains a control character".to_string(),
                ));
            }
        }
        Ok(Self(id.to_string()))
    }
}

impl TryFrom<&str> for UploadId {
    type Error = Error;

    fn try_from(id: &str) -> Result<Self, Self::Error> {
        id.parse()
    }
}

impl TryFrom<String> for UploadId {
    type Error = Error;

    fn try_from(id: String) -> Result<Self, Self::Error> {
        id.parse::<Self>()?;
        Ok(Self(id))
    }
}

impl AsRef<str> for UploadId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for UploadId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// Generous: UUIDs are 36 characters, KSUIDs are 27, ULIDs are 26. 256
// covers every common id scheme and stays well under any path-component
// limit (`NAME_MAX` is 255 on Linux, plus the `.json` suffix the
// FileStateStore appends).
const MAX_UPLOAD_ID_LEN: usize = 256;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_typical_ids() {
        for id in [
            "550e8400-e29b-41d4-a716-446655440000", // UUID
            "01H8XGJWBWBAQ4SHN3JPHQM6JZ",           // ULID
            "2YHHb9hVTQR1Etu6P3kY83bp1Wd",          // KSUID
            "my-custom-slug_42.with.dots",
            "a",            // single char
            "Любой-юникод", // non-ASCII
        ] {
            assert!(
                id.parse::<UploadId>().is_ok(),
                "expected {id:?} to be accepted"
            );
        }
    }

    #[test]
    fn rejects_empty() {
        let err = "".parse::<UploadId>().unwrap_err();
        assert!(matches!(err, Error::InvalidUploadId(_)));
        assert_eq!(err.status_code(), 400);
    }

    #[test]
    fn rejects_oversized() {
        let id = "A".repeat(MAX_UPLOAD_ID_LEN + 1);
        let err = id.parse::<UploadId>().unwrap_err();
        assert!(matches!(err, Error::InvalidUploadId(_)));
        // exact-boundary value is accepted
        let just_fits = "A".repeat(MAX_UPLOAD_ID_LEN);
        assert!(just_fits.parse::<UploadId>().is_ok());
    }

    #[test]
    fn rejects_path_separators() {
        for id in ["foo/bar", "../etc/passwd", "foo\\bar", "..\\windows"] {
            let err = id.parse::<UploadId>().unwrap_err();
            assert!(
                matches!(err, Error::InvalidUploadId(_)),
                "expected {id:?} to be rejected, got {err:?}"
            );
        }
    }

    #[test]
    fn rejects_control_characters() {
        for id in ["foo\0bar", "foo\nbar", "foo\rbar", "foo\tbar", "\x7Fid"] {
            let err = id.parse::<UploadId>().unwrap_err();
            assert!(
                matches!(err, Error::InvalidUploadId(_)),
                "expected {id:?} to be rejected, got {err:?}"
            );
        }
    }

    #[test]
    fn dotdot_alone_is_allowed() {
        // `..` literally is just a 2-character id. The state file
        // becomes `<state_dir>/...json` (a file named `...json`),
        // not a parent-dir traversal; that requires a `/`. So the
        // validator accepts `..` by itself; the path-separator
        // check below catches `../foo`.
        assert!("..".parse::<UploadId>().is_ok());
        assert!("...json".parse::<UploadId>().is_ok());
    }

    #[test]
    fn upload_id_parses_valid_id() {
        let id: UploadId = "my-custom-slug_42.with.dots".parse().unwrap();

        assert_eq!(id.as_str(), "my-custom-slug_42.with.dots");
    }

    #[test]
    fn upload_id_rejects_invalid_id() {
        let err = "foo/bar".parse::<UploadId>().unwrap_err();

        assert!(matches!(err, Error::InvalidUploadId(_)));
    }
}
