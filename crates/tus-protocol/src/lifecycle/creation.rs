use chrono::{Duration as ChronoDuration, Utc};

use crate::config::{Config, Extension};
use crate::error::{Error, Result};
use crate::extensions::UploadConcat;
use crate::protocol::Headers;
use crate::state::{UploadMetadata, UploadState};

/// Request fields needed to create an upload state.
#[derive(Debug, Clone)]
pub(crate) struct CreationRequest {
    /// Upload-Length, if supplied.
    pub(crate) upload_length: Option<u64>,
    /// Whether Upload-Defer-Length was set.
    pub(crate) upload_defer_length: bool,
    /// Upload-Metadata, if supplied.
    pub(crate) upload_metadata: Option<UploadMetadata>,
    /// Upload-Concat, if supplied.
    pub(crate) upload_concat: Option<UploadConcat>,
    /// Whether the POST request carries bytes.
    pub(crate) has_body: bool,
}

impl CreationRequest {
    /// Builds lifecycle creation input from parsed protocol headers.
    #[must_use]
    pub(crate) fn from_headers(headers: &Headers, has_body: bool) -> Self {
        Self {
            upload_length: headers.upload_length,
            upload_defer_length: headers.upload_defer_length,
            upload_metadata: headers.upload_metadata.clone(),
            upload_concat: headers.upload_concat.clone(),
            has_body,
        }
    }
}

/// Result of applying creation-time protocol rules.
#[derive(Debug)]
pub(crate) enum CreationTransition {
    /// A regular or partial upload ready for hooks and persistence.
    Upload(UploadState),
    /// A final upload that still needs its partial uploads loaded.
    Final {
        /// Initial final upload state.
        state: UploadState,
        /// Part URLs from Upload-Concat.
        part_urls: Vec<String>,
    },
}

/// Applies creation-time protocol rules and returns the initial upload state.
pub(crate) fn prepare_creation(
    config: &Config,
    request: CreationRequest,
) -> Result<CreationTransition> {
    if !config.has_extension(Extension::Creation) {
        return Err(Error::ExtensionNotSupported("creation".to_string()));
    }

    if request.upload_concat.is_some() && !config.has_extension(Extension::Concatenation) {
        return Err(Error::ExtensionNotSupported("concatenation".to_string()));
    }

    let is_final_upload = matches!(request.upload_concat, Some(UploadConcat::Final(_)));
    if is_final_upload {
        if request.upload_length.is_some() || request.upload_defer_length {
            return Err(Error::InvalidHeader {
                header: "Upload-Length",
                message: "Upload-Length and Upload-Defer-Length must not be set on a final concatenation upload".to_string(),
            });
        }

        if request.has_body {
            return Err(Error::InvalidHeader {
                header: "Upload-Concat",
                message: "a final concatenation upload must not include a request body".to_string(),
            });
        }
    } else {
        if request.upload_length.is_none() && !request.upload_defer_length {
            return Err(Error::MissingHeader("Upload-Length or Upload-Defer-Length"));
        }

        if request.upload_length.is_some() && request.upload_defer_length {
            return Err(Error::InvalidHeader {
                header: "Upload-Defer-Length",
                message: "Upload-Length and Upload-Defer-Length are mutually exclusive".to_string(),
            });
        }

        if request.upload_defer_length && !config.has_extension(Extension::CreationDeferLength) {
            return Err(Error::ExtensionNotSupported(
                "creation-defer-length".to_string(),
            ));
        }

        if request.upload_defer_length && !config.allows_empty_creation() && !request.has_body {
            return Err(Error::ExtensionNotSupported(
                "creation-defer-length".to_string(),
            ));
        }
    }

    if request.has_body && !config.has_extension(Extension::CreationWithUpload) {
        return Err(Error::ExtensionNotSupported(
            "creation-with-upload".to_string(),
        ));
    }

    if !request.has_body && !is_final_upload && !config.allows_empty_creation() {
        return Err(Error::InvalidHeader {
            header: "Upload-Length",
            message: "empty creation requests are disabled".to_string(),
        });
    }

    if let (Some(length), Some(max_size)) = (request.upload_length, config.max_size())
        && length > max_size
    {
        return Err(Error::SizeExceeded {
            size: length,
            max: max_size,
        });
    }

    let mut state = UploadState::new_random();
    if let Some(length) = request.upload_length {
        state.set_length(length);
    }
    if let Some(metadata) = request.upload_metadata {
        state.set_metadata(metadata);
    }
    if let Some(expiration) = config.expiration() {
        let expiration = ChronoDuration::from_std(expiration)
            .map_err(|error| Error::Internal(error.to_string()))?;
        state.set_expiration(Utc::now() + expiration);
    }

    match request.upload_concat {
        Some(UploadConcat::Partial) => {
            state.mark_partial();
            Ok(CreationTransition::Upload(state))
        }
        Some(UploadConcat::Final(part_urls)) => Ok(CreationTransition::Final { state, part_urls }),
        None => Ok(CreationTransition::Upload(state)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_creation_builds_regular_upload_state() {
        let mut metadata = UploadMetadata::new();
        metadata.insert("filename", "report.pdf");
        let request = CreationRequest {
            upload_length: Some(42),
            upload_defer_length: false,
            upload_metadata: Some(metadata),
            upload_concat: None,
            has_body: false,
        };

        let transition = prepare_creation(&Config::default(), request).unwrap();

        let CreationTransition::Upload(state) = transition else {
            panic!("expected regular upload transition");
        };
        assert_eq!(state.length(), Some(42));
        assert_eq!(
            state
                .metadata()
                .get("filename")
                .and_then(|value| value.as_str()),
            Some("report.pdf")
        );
        assert!(!state.is_partial());
        assert!(!state.is_final());
    }

    #[test]
    fn prepare_creation_rejects_missing_length_for_regular_upload() {
        let request = CreationRequest {
            upload_length: None,
            upload_defer_length: false,
            upload_metadata: None,
            upload_concat: None,
            has_body: false,
        };

        let err = prepare_creation(&Config::default(), request).unwrap_err();

        assert!(matches!(
            err,
            Error::MissingHeader("Upload-Length or Upload-Defer-Length")
        ));
    }

    #[test]
    fn prepare_creation_returns_final_transition_without_length() {
        let request = CreationRequest {
            upload_length: None,
            upload_defer_length: false,
            upload_metadata: None,
            upload_concat: Some(UploadConcat::Final(vec!["/files/part-1".to_string()])),
            has_body: false,
        };
        let config = Config::default().with_extension(Extension::Concatenation);

        let transition = prepare_creation(&config, request).unwrap();

        let CreationTransition::Final { state, part_urls } = transition else {
            panic!("expected final upload transition");
        };
        assert_eq!(state.length(), None);
        assert_eq!(part_urls, vec!["/files/part-1".to_string()]);
    }

    #[test]
    fn prepare_creation_rejects_body_without_creation_with_upload() {
        let request = CreationRequest {
            upload_length: Some(5),
            upload_defer_length: false,
            upload_metadata: None,
            upload_concat: None,
            has_body: true,
        };

        let err = prepare_creation(&Config::default(), request).unwrap_err();

        assert!(matches!(err, Error::ExtensionNotSupported(ext) if ext == "creation-with-upload"));
    }

    #[test]
    fn prepare_creation_allows_deferred_length_creation_with_upload_when_empty_creation_disabled() {
        let request = CreationRequest {
            upload_length: None,
            upload_defer_length: true,
            upload_metadata: None,
            upload_concat: None,
            has_body: true,
        };
        let config = Config::default()
            .with_extension(Extension::CreationDeferLength)
            .with_extension(Extension::CreationWithUpload)
            .without_empty_creation();

        let transition = prepare_creation(&config, request).unwrap();

        let CreationTransition::Upload(state) = transition else {
            panic!("expected regular upload transition");
        };
        assert_eq!(state.length(), None);
    }
}
