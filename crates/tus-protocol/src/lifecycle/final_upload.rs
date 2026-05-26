use crate::config::{Config, Extension};
use crate::error::{Error, Result};
use crate::protocol::UploadId;
use crate::state::{StateStore, UploadState};

/// Loaded and validated final-upload parts.
#[derive(Debug, Clone)]
pub struct FinalUploadPlan {
    /// Part IDs in concatenation order.
    pub part_ids: Vec<String>,
    /// Part states in concatenation order.
    pub parts: Vec<UploadState>,
    /// Summarized final-upload state derived from the parts.
    pub status: FinalUploadStatus,
}

/// Final-upload state derived from its partial uploads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalUploadStatus {
    /// Known total length when all parts have known length.
    pub total_length: Option<u64>,
    /// Sum of current offsets for all parts.
    pub current_offset: u64,
    /// Whether every part is complete and total length is known.
    pub all_complete: bool,
}

impl FinalUploadStatus {
    /// Returns the offset the final upload state should expose internally.
    #[must_use]
    pub fn expected_offset(&self) -> u64 {
        if self.all_complete {
            self.total_length.unwrap_or(self.current_offset)
        } else {
            self.current_offset
        }
    }

    /// Returns whether storage materialization may run now.
    #[must_use]
    pub fn ready_to_materialize(&self) -> bool {
        self.all_complete && self.total_length.is_some()
    }
}

/// Loads and validates parts referenced by a final Upload-Concat header.
pub async fn load_final_upload_plan<I>(
    state_store: &I,
    config: &Config,
    part_urls: &[String],
) -> Result<FinalUploadPlan>
where
    I: StateStore + ?Sized,
{
    let allow_unfinished = config.has_extension(Extension::ConcatenationUnfinished);
    let mut part_ids = Vec::with_capacity(part_urls.len());
    let mut parts = Vec::with_capacity(part_urls.len());

    for url in part_urls {
        let id = extract_partial_id(url, config.base_path_str()).ok_or_else(|| {
            Error::InvalidHeader {
                header: "Upload-Concat",
                message: format!(
                    "partial URL not under base path {:?}: {}",
                    config.base_path_str(),
                    url
                ),
            }
        })?;

        let part_state =
            state_store
                .get(id.as_str())
                .await?
                .ok_or_else(|| Error::InvalidHeader {
                    header: "Upload-Concat",
                    message: format!("partial upload not found: {}", id.as_str()),
                })?;

        if !part_state.is_partial() {
            return Err(Error::NotPartialUpload(id.clone().into_string()));
        }

        if !part_state.is_complete() && !allow_unfinished {
            return Err(Error::IncompleteUpload(id.clone().into_string()));
        }

        if part_state.is_expired() {
            return Err(Error::Expired(id.clone().into_string()));
        }

        part_ids.push(id.into_string());
        parts.push(part_state);
    }

    let status = summarize_final_parts(&parts)?;

    Ok(FinalUploadPlan {
        part_ids,
        parts,
        status,
    })
}

/// Loads final-upload parts referenced by persisted final upload state.
pub async fn load_final_upload_status<I>(
    state_store: &I,
    state: &UploadState,
) -> Result<Option<FinalUploadPlan>>
where
    I: StateStore + ?Sized,
{
    let Some(part_ids) = state.parts().map(|parts| parts.to_vec()) else {
        return Ok(None);
    };

    let mut parts = Vec::with_capacity(part_ids.len());
    for part_id in &part_ids {
        let part_state = state_store
            .get(part_id)
            .await
            .map_err(|err| Error::Internal(err.to_string()))?
            .ok_or_else(|| {
                Error::Internal(format!(
                    "final upload {} references missing partial {}",
                    state.id(),
                    part_id
                ))
            })?;

        if part_state.is_expired() {
            return Err(Error::Expired(part_id.clone()));
        }

        parts.push(part_state);
    }

    let status = summarize_final_parts(&parts)?;
    Ok(Some(FinalUploadPlan {
        part_ids,
        parts,
        status,
    }))
}

/// Summarizes final-upload state derived from partial uploads.
pub fn summarize_final_parts(parts: &[UploadState]) -> Result<FinalUploadStatus> {
    let mut total_length = 0_u64;
    let mut current_offset = 0_u64;
    let mut all_complete = true;
    let mut length_known = true;

    for part_state in parts {
        if part_state.is_expired() {
            return Err(Error::Expired(part_state.id().to_string()));
        }

        current_offset = current_offset.saturating_add(part_state.offset());

        match part_state.length() {
            Some(length) => total_length = total_length.saturating_add(length),
            None => {
                length_known = false;
                all_complete = false;
            }
        }

        if !part_state.is_complete() {
            all_complete = false;
        }
    }

    Ok(FinalUploadStatus {
        total_length: length_known.then_some(total_length),
        current_offset,
        all_complete: all_complete && length_known,
    })
}

/// Extracts an upload ID from a URL present in `Upload-Concat: final;<parts>`.
fn extract_partial_id(url: &str, base_path: &str) -> Option<UploadId> {
    let path = if let Some(rest) = url.split_once("://") {
        match rest.1.find('/') {
            Some(idx) => &rest.1[idx..],
            None => return None,
        }
    } else {
        url
    };

    let path = path.split(['?', '#']).next().unwrap_or(path);
    let expected_prefix = if base_path.ends_with('/') {
        base_path.to_string()
    } else {
        format!("{base_path}/")
    };

    let id = path.strip_prefix(&expected_prefix)?;
    if id.is_empty() || id.contains('/') {
        return None;
    }
    id.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::UploadId;
    use crate::state::UploadState;
    use chrono::{Duration, Utc};

    #[test]
    fn summarize_final_parts_reports_complete_known_length() {
        let mut part1 = UploadState::new("part-1").with_length(4).as_partial();
        part1.set_offset(4);
        let mut part2 = UploadState::new("part-2").with_length(5).as_partial();
        part2.set_offset(5);

        let status = summarize_final_parts(&[part1, part2]).unwrap();

        assert_eq!(status.total_length, Some(9));
        assert_eq!(status.current_offset, 9);
        assert!(status.all_complete);
        assert_eq!(status.expected_offset(), 9);
        assert!(status.ready_to_materialize());
    }

    #[test]
    fn summarize_final_parts_reports_incomplete_deferred_part() {
        let mut part = UploadState::new("part-1").as_partial();
        part.set_offset(5);

        let status = summarize_final_parts(&[part]).unwrap();

        assert_eq!(status.total_length, None);
        assert_eq!(status.current_offset, 5);
        assert!(!status.all_complete);
        assert_eq!(status.expected_offset(), 5);
        assert!(!status.ready_to_materialize());
    }

    #[test]
    fn summarize_final_parts_rejects_expired_part() {
        let part = UploadState::new("part-1")
            .with_length(5)
            .with_expiration(Utc::now() - Duration::seconds(1))
            .as_partial();

        let err = summarize_final_parts(&[part]).unwrap_err();

        assert!(matches!(err, crate::error::Error::Expired(id) if id == "part-1"));
    }

    #[test]
    fn extract_partial_id_accepts_relative_and_absolute_urls() {
        assert_eq!(
            extract_partial_id("/files/abc123", "/files").map(UploadId::into_string),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_partial_id("https://host.example/files/abc123", "/files")
                .map(UploadId::into_string),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_partial_id("http://host/files/abc?x=1", "/files").map(UploadId::into_string),
            Some("abc".to_string())
        );
    }

    #[test]
    fn extract_partial_id_rejects_urls_outside_base_path() {
        assert_eq!(extract_partial_id("/other/abc", "/files"), None);
        assert_eq!(extract_partial_id("abc", "/files"), None);
        assert_eq!(extract_partial_id("/files", "/files"), None);
        assert_eq!(extract_partial_id("/files/a/b", "/files"), None);
        assert_eq!(extract_partial_id("https://", "/files"), None);
    }
}
