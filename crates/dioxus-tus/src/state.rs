/// Current status of an upload managed by [`crate::use_tus_upload`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum UploadStatus {
    #[default]
    Idle,
    Uploading,
    Paused,
    Complete,
    Error,
}

/// Error surfaced by the upload hook.
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TusError {
    /// Network request failed before getting a response (DNS, connection refused,
    /// TLS, abrupt disconnect). On the browser side this is the catch-all "Failed
    /// to fetch" — see also [`TusError::Cors`] for the cross-origin specialisation.
    #[error("network request failed: {0}")]
    Transport(String),

    /// HTTP 4xx/5xx response from the TUS server.
    #[error("server returned {status}: {body}")]
    Server { status: u16, body: String },

    /// The server did not send a required response header. Common cause: CORS
    /// `Access-Control-Expose-Headers` is missing this header in the preflight
    /// response. See <https://docs.rs/dioxus-tus> for CORS setup.
    #[error("missing required response header: {0}")]
    MissingHeader(String),

    /// The server sent a required header, but its value could not be parsed.
    /// Distinct from [`TusError::MissingHeader`] — the header was *present*,
    /// just malformed (e.g. `Upload-Offset: not-a-number`). Useful for
    /// distinguishing CORS / proxy header-stripping from a misbehaving server.
    #[error("invalid header `{header}` value `{value}`")]
    InvalidHeader { header: String, value: String },

    /// Cross-origin request blocked by the browser. Confirm the server is sending
    /// CORS preflight headers (Access-Control-Allow-Origin / -Headers / -Methods).
    /// See <https://docs.rs/dioxus-tus> for the required header set.
    #[error(
        "cross-origin request blocked by browser CORS policy. See https://docs.rs/dioxus-tus for CORS setup."
    )]
    Cors,

    /// Failed to read browser Blob/File contents.
    #[error("blob read failed: {0}")]
    BlobRead(String),

    /// The configured endpoint URL could not be parsed.
    #[error("invalid upload endpoint URL: {0}")]
    InvalidUrl(String),

    /// The TUS server doesn't advertise a required extension via its `Tus-Extension`
    /// response header. Configure the server to enable the named extension.
    #[error("server is missing required TUS extension: {0}")]
    ServerMissingExtension(String),

    /// The file size exceeds the server's advertised `Tus-Max-Size`. Caught
    /// before any network call so the user sees a clear error rather than a
    /// 413 surfacing after the server has already allocated the resource.
    #[error("file size {file_size} exceeds server's Tus-Max-Size of {max_size}")]
    FileTooLarge { file_size: u64, max_size: u64 },
}

impl From<tus_client::Error> for TusError {
    fn from(e: tus_client::Error) -> Self {
        use tus_client::Error;
        match e {
            Error::MissingHeader { header, .. } => TusError::MissingHeader(header.to_string()),
            Error::InvalidHeader { header, value, .. } => TusError::InvalidHeader {
                header: header.to_string(),
                value,
            },
            Error::InvalidRequestHeader { name, value, .. } => {
                // Redact credentials before they can leak through Display'd
                // errors into application logs.
                let display_value = if is_sensitive_header_name(&name) {
                    "<redacted>".to_string()
                } else {
                    value
                };
                TusError::Transport(format!("invalid request header `{name}`: {display_value}"))
            }
            Error::OffsetBeyondSource {
                offset, source_len, ..
            } => TusError::Transport(format!(
                "server offset {offset} exceeds local file size {source_len}"
            )),
            Error::LengthMismatch { remote, local, .. } => TusError::Transport(format!(
                "server length {remote} does not match local file size {local}"
            )),
            Error::OffsetDesync {
                expected, actual, ..
            } => TusError::Transport(format!(
                "server acknowledged offset {actual}, expected {expected}"
            )),
            Error::Source { source, .. } => {
                TusError::Transport(format!("upload source failed: {source}"))
            }
            Error::UnexpectedResponse { status, body, .. } => TusError::Server {
                status: status.as_u16(),
                body,
            },
            Error::UnsupportedExtension { extension, .. } => {
                TusError::ServerMissingExtension(extension.to_string())
            }
            Error::CrossOriginLocation {
                endpoint, location, ..
            } => TusError::Transport(format!(
                "server upload location `{location}` is not on endpoint origin `{endpoint}`"
            )),
            Error::Internal { message, .. } => {
                TusError::Transport(format!("internal client error: {message}"))
            }
            Error::Url(e) => TusError::InvalidUrl(e.to_string()),
            Error::Io(e) => TusError::Transport(format!("io: {e}")),
            Error::Transport { source, .. } => {
                // Heuristic: per-browser opaque fetch failure strings almost
                // always mean CORS preflight blocked the request. Map to the
                // typed Cors variant so the consumer can branch on it.
                // - Chromium / Edge: "TypeError: Failed to fetch"
                // - Firefox:        "TypeError: NetworkError when attempting to fetch resource."
                // - Safari (WebKit): "TypeError: Load failed"
                // Best-effort hint, not a guarantee — DNS/cert/server-down
                // can produce these strings too. Consumers should treat
                // Cors as a strong hint, not a definitive classification.
                let s = source.to_string();
                if s.contains("Failed to fetch")
                    || s.contains("NetworkError")
                    || s.contains("Load failed")
                {
                    TusError::Cors
                } else {
                    TusError::Transport(s)
                }
            }
            // `tus_client::Error` is `#[non_exhaustive]`; a variant added in a
            // future release degrades to a generic transport error carrying
            // its `Display` text rather than failing to compile.
            other => TusError::Transport(other.to_string()),
        }
    }
}

fn is_sensitive_header_name(name: &str) -> bool {
    let lower = name.trim().to_ascii_lowercase();
    lower == "authorization"
        || lower == "proxy-authorization"
        || lower == "cookie"
        || lower == "set-cookie"
        || lower == "x-api-key"
        || lower == "api-key"
        || lower.ends_with("-api-key")
        || lower.contains("token")
        || lower.contains("secret")
}

/// Reactive snapshot of upload state returned by [`crate::use_tus_upload`].
///
/// `#[non_exhaustive]`: consumers read this snapshot rather than constructing
/// it, and it is expected to grow (e.g. `speed`, `eta`, `started_at`), so new
/// fields must not be a breaking change.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct TusUploadState {
    pub status: UploadStatus,
    pub bytes_uploaded: u64,
    pub bytes_total: Option<u64>,
    /// The TUS resource URL, available once the upload has been created.
    pub upload_url: Option<String>,
    pub error: Option<TusError>,
}

impl TusUploadState {
    pub fn is_idle(&self) -> bool {
        self.status == UploadStatus::Idle
    }
    pub fn is_uploading(&self) -> bool {
        self.status == UploadStatus::Uploading
    }
    pub fn is_paused(&self) -> bool {
        self.status == UploadStatus::Paused
    }
    pub fn is_complete(&self) -> bool {
        self.status == UploadStatus::Complete
    }
    pub fn is_error(&self) -> bool {
        self.status == UploadStatus::Error
    }

    /// Upload progress as a fraction in `[0.0, 1.0]`.
    /// Returns `None` when the total size is unknown or the upload hasn't started.
    pub fn progress_fraction(&self) -> Option<f64> {
        let total = self.bytes_total? as f64;
        if total == 0.0 {
            return Some(1.0);
        }
        Some(self.bytes_uploaded as f64 / total)
    }
}

/// Abstract write-target for [`TusUploadState`] updates from the upload engine.
///
/// Production wraps a Dioxus `Signal<TusUploadState>` so writes drive
/// re-renders. Tests can substitute a captured-updates implementation that
/// records every transition for later assertion. Decoupling the engine from
/// `Signal` means the chunk loop is testable without a Dioxus runtime.
///
/// Method names avoid `read` to keep `state.read().is_uploading()` resolving
/// to Dioxus's `ReadableExt::read` at the few call sites that depend on it
/// (notably outside the engine).
///
/// Internal write seam (`pub(crate)`): not object-safe and not part of the
/// public surface. Consulted only by the wasm upload engine.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) trait StateSink {
    /// Mutate the state in place. Production-side this acquires a write
    /// guard on the underlying `Signal` and notifies subscribers.
    fn update<F: FnOnce(&mut TusUploadState)>(&mut self, f: F);

    /// Snapshot the current state. Equivalent to `signal.read().clone()` on
    /// the production side; tests return a clone of their captured value.
    fn snapshot(&self) -> TusUploadState;
}

#[cfg(test)]
mod tests {
    // `TusUploadState` is `#[non_exhaustive]`, so it can only be constructed
    // with a struct literal from inside this crate — hence these live here
    // rather than in the `tests/` integration suite.
    use super::*;

    #[test]
    fn default_state_is_idle() {
        let s = TusUploadState::default();
        assert!(s.is_idle());
        assert!(!s.is_uploading());
        assert!(s.progress_fraction().is_none());
    }

    #[test]
    fn progress_fraction_zero_when_no_bytes_uploaded() {
        let s = TusUploadState {
            status: UploadStatus::Uploading,
            bytes_uploaded: 0,
            bytes_total: Some(100),
            ..Default::default()
        };
        assert_eq!(s.progress_fraction(), Some(0.0));
    }

    #[test]
    fn progress_fraction_half() {
        let s = TusUploadState {
            status: UploadStatus::Uploading,
            bytes_uploaded: 50,
            bytes_total: Some(100),
            ..Default::default()
        };
        assert_eq!(s.progress_fraction(), Some(0.5));
    }

    #[test]
    fn progress_fraction_complete() {
        let s = TusUploadState {
            status: UploadStatus::Complete,
            bytes_uploaded: 100,
            bytes_total: Some(100),
            ..Default::default()
        };
        assert_eq!(s.progress_fraction(), Some(1.0));
    }

    #[test]
    fn progress_fraction_none_when_total_unknown() {
        let s = TusUploadState {
            status: UploadStatus::Uploading,
            bytes_uploaded: 50,
            bytes_total: None,
            ..Default::default()
        };
        assert!(s.progress_fraction().is_none());
    }

    #[test]
    fn progress_fraction_one_for_zero_size_file() {
        let s = TusUploadState {
            status: UploadStatus::Complete,
            bytes_uploaded: 0,
            bytes_total: Some(0),
            ..Default::default()
        };
        assert_eq!(s.progress_fraction(), Some(1.0));
    }
}
