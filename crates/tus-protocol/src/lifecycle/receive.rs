use crate::config::Config;
use crate::error::{Error, Result};
use crate::state::UploadState;
use crate::storage::StorageHandle;

use super::ensure_active;

/// Request fields needed before a PATCH body is accepted.
#[derive(Debug, Clone, Copy)]
pub struct ReceiveRequest {
    /// Client-supplied Upload-Offset.
    pub client_offset: u64,
    /// Optional Upload-Length used to resolve deferred length uploads.
    pub upload_length: Option<u64>,
}

/// Result of projecting a receive body against the current upload state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveProjection {
    /// Offset expected after the body is committed.
    pub projected_offset: u64,
    /// Whether the receive operation completes the upload.
    pub completes_upload: bool,
}

/// Validates PATCH preflight state and applies deferred Upload-Length.
pub fn prepare_receive(
    config: &Config,
    state: &mut UploadState,
    request: ReceiveRequest,
) -> Result<()> {
    ensure_active(state)?;

    if state.is_final() {
        return Err(Error::FinalUploadModificationForbidden(
            state.id().to_string(),
        ));
    }

    if request.client_offset != state.offset() {
        return Err(Error::OffsetMismatch {
            expected: state.offset(),
            actual: request.client_offset,
        });
    }

    if state.is_complete() {
        return Err(Error::CompletedUploadModificationForbidden(
            state.id().to_string(),
        ));
    }

    if let Some(length) = request.upload_length {
        if let Some(existing) = state.length() {
            if length != existing {
                return Err(Error::InvalidHeader {
                    header: "Upload-Length",
                    message: format!(
                        "cannot change Upload-Length after it is set (existing: {existing}, provided: {length})"
                    ),
                });
            }
        } else {
            if let Some(max_size) = config.max_size_limit()
                && length > max_size
            {
                return Err(Error::SizeExceeded {
                    size: length,
                    max: max_size,
                });
            }
            state.set_length(length);
        }
    }

    Ok(())
}

/// Computes the maximum body bytes this receive may accept before buffering.
#[must_use]
pub fn receive_body_size_limit(config: &Config, state: &UploadState) -> Option<u64> {
    [
        config.max_chunk_size_limit(),
        config
            .max_size_limit()
            .map(|max_size| max_size.saturating_sub(state.offset())),
        state
            .length()
            .map(|length| length.saturating_sub(state.offset())),
    ]
    .into_iter()
    .flatten()
    .min()
}

/// Validates body length and computes the resulting offset.
pub fn validate_receive_body(
    config: &Config,
    state: &UploadState,
    body_len: u64,
) -> Result<ReceiveProjection> {
    if let Some(max_chunk) = config.max_chunk_size_limit()
        && body_len > max_chunk
    {
        return Err(Error::SizeExceeded {
            size: body_len,
            max: max_chunk,
        });
    }

    let projected_offset = state.offset().saturating_add(body_len);

    if let Some(max_size) = config.max_size_limit()
        && projected_offset > max_size
    {
        return Err(Error::SizeExceeded {
            size: projected_offset,
            max: max_size,
        });
    }

    if let Some(length) = state.length()
        && projected_offset > length
    {
        return Err(Error::SizeExceeded {
            size: projected_offset,
            max: length,
        });
    }

    Ok(ReceiveProjection {
        projected_offset,
        completes_upload: state
            .length()
            .is_some_and(|length| projected_offset == length),
    })
}

/// Applies lifecycle-owned state changes after a successful storage commit.
pub fn apply_receive_commit(
    state: &mut UploadState,
    projection: ReceiveProjection,
    handle: StorageHandle,
) {
    state.set_storage_handle(handle);
    state.set_offset(projection.projected_offset);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_receive_rejects_offset_mismatch() {
        let mut state = UploadState::new("upload-1").with_length(10);
        state.set_offset(4);

        let err = prepare_receive(
            &Config::default(),
            &mut state,
            ReceiveRequest {
                client_offset: 3,
                upload_length: None,
            },
        )
        .unwrap_err();

        assert!(matches!(
            err,
            Error::OffsetMismatch {
                expected: 4,
                actual: 3
            }
        ));
    }

    #[test]
    fn prepare_receive_sets_deferred_length() {
        let mut state = UploadState::new("upload-1");

        prepare_receive(
            &Config::default(),
            &mut state,
            ReceiveRequest {
                client_offset: 0,
                upload_length: Some(10),
            },
        )
        .unwrap();

        assert_eq!(state.length(), Some(10));
    }

    #[test]
    fn validate_receive_body_detects_completion() {
        let state = UploadState::new("upload-1").with_length(5);

        let projection = validate_receive_body(&Config::default(), &state, 5).unwrap();

        assert_eq!(projection.projected_offset, 5);
        assert!(projection.completes_upload);
    }

    #[test]
    fn validate_receive_body_rejects_body_beyond_declared_length() {
        let state = UploadState::new("upload-1").with_length(5);

        let err = validate_receive_body(&Config::default(), &state, 6).unwrap_err();

        assert!(matches!(err, Error::SizeExceeded { size: 6, max: 5 }));
    }
}
