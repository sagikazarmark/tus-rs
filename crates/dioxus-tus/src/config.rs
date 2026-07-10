use std::collections::HashMap;
use std::fmt;

/// Static configuration for a [`crate::use_tus_upload`] hook instance.
///
/// Construct via [`TusConfig::new`] (endpoint is non-optional) and chain
/// `with_*` setters for the rest. The struct is `#[non_exhaustive]` so adding
/// fields is not a breaking change for external consumers.
#[derive(Clone)]
#[non_exhaustive]
pub struct TusConfig {
    pub endpoint: String,
    /// Bearer token applied to all uploads; can be overridden per upload via
    /// [`TusStartOptions::bearer_token_override`].
    pub bearer_token: Option<String>,
    /// Maximum bytes per PATCH request. Defaults to 1 MiB.
    ///
    /// Sized for main-thread `web_sys::Blob` reads: larger chunks can stall
    /// the UI while the slice is read into wasm linear memory. Bump for
    /// throughput-bound LAN uploads if jank isn't observable.
    pub chunk_size: usize,
    /// Maximum PATCH retries on transient failure (default 3).
    pub max_retries: usize,
    /// Base retry delay in milliseconds, doubled on each attempt (default 200).
    pub retry_delay_ms: u64,
    /// Files at or below this size use TUS `creation-with-upload` (POST + body
    /// in one request) when supported by the server. Defaults to 256 KiB.
    ///
    /// The full body is read into wasm linear memory before POSTing, so don't
    /// raise this past a few MiB without measuring main-thread jank; the
    /// chunked path's amortised reads scale better. The chunked path's
    /// per-chunk size is controlled separately by [`Self::chunk_size`].
    pub creation_with_upload_threshold: usize,
}

/// Redacts `bearer_token` so credentials never reach logs via `{:?}`.
impl fmt::Debug for TusConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TusConfig")
            .field("endpoint", &self.endpoint)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[redacted]"),
            )
            .field("chunk_size", &self.chunk_size)
            .field("max_retries", &self.max_retries)
            .field("retry_delay_ms", &self.retry_delay_ms)
            .field(
                "creation_with_upload_threshold",
                &self.creation_with_upload_threshold,
            )
            .finish()
    }
}

impl TusConfig {
    /// Constructs a config for the given TUS endpoint URL.
    ///
    /// Endpoint typically ends in `/files`; the exact path is
    /// server-determined. The hook does not validate the URL until the first
    /// `start()`; invalid URLs surface as [`crate::TusError::InvalidUrl`] at
    /// upload time.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            bearer_token: None,
            chunk_size: 1024 * 1024,
            max_retries: 3,
            retry_delay_ms: 200,
            creation_with_upload_threshold: 256 * 1024,
        }
    }

    #[must_use]
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    /// Sets the per-PATCH chunk size, clamped to at least 1 byte.
    #[must_use]
    pub fn with_chunk_size(mut self, bytes: usize) -> Self {
        self.chunk_size = bytes.max(1);
        self
    }

    #[must_use]
    pub fn with_max_retries(mut self, n: usize) -> Self {
        self.max_retries = n;
        self
    }

    #[must_use]
    pub fn with_retry_delay_ms(mut self, ms: u64) -> Self {
        self.retry_delay_ms = ms;
        self
    }

    #[must_use]
    pub fn with_creation_with_upload_threshold(mut self, bytes: usize) -> Self {
        self.creation_with_upload_threshold = bytes;
        self
    }

    /// Returns whether a file of `file_size` bytes should be sent via the
    /// `creation-with-upload` fast path (POST + body in one request) given
    /// this config.
    ///
    /// Compares in `u64` space rather than truncating `file_size` to
    /// `usize`: on `wasm32-unknown-unknown` `usize` is 32-bit, so casting
    /// a `u64` file size to `usize` truncates to the low 32 bits. A file of
    /// e.g. `4 GiB + 100 KiB` would truncate to `100 KiB`, falsely match a
    /// 256 KiB threshold, and route the entire 4 GiB payload through the
    /// load-whole-body path, OOMing wasm linear memory.
    ///
    /// Consulted only by the wasm upload engine; `allow(dead_code)` off-wasm.
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub(crate) fn use_creation_with_upload(&self, file_size: u64) -> bool {
        file_size > 0 && file_size <= self.creation_with_upload_threshold as u64
    }
}

/// Per-upload options passed to [`crate::TusUploadHandle::start`].
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct TusStartOptions {
    /// Bearer token for this upload only; takes precedence over
    /// [`TusConfig::bearer_token`] when `Some`.
    pub bearer_token_override: Option<String>,
    /// Additional request headers sent with every TUS request for this upload.
    pub extra_headers: Vec<(String, String)>,
    /// Additional TUS metadata key-value pairs merged with auto-populated filename/filetype.
    /// Keys here override auto-populated values.
    pub extra_metadata: HashMap<String, String>,
    /// Overrides the `filename` metadata key (auto-populated from `web_sys::File::name()`).
    pub filename_override: Option<String>,
    /// Overrides the `filetype` metadata key (auto-populated from `web_sys::File::type_()`).
    pub content_type_override: Option<String>,
    /// Existing TUS upload URL to resume from. When present, `start` issues a
    /// HEAD against this URL to learn the server-side offset rather than
    /// creating a fresh upload via POST.
    ///
    /// This value is trusted as-is: any configured `bearer_token`/`Authorization`
    /// and [`extra_headers`](Self::extra_headers) are sent to it, even if it is
    /// cross-origin (a spec-compliant TUS `Location` may point to another origin).
    /// Set it only to URLs you trust. Untrusted, persisted resume URLs are a
    /// separate concern and are already filtered by origin before reaching here.
    pub existing_url: Option<String>,
}

/// Redacts `bearer_token_override` and header values (any of which may carry
/// credentials such as `Authorization`) so `{:?}` never leaks them. Header
/// names are kept to preserve debuggability.
impl fmt::Debug for TusStartOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TusStartOptions")
            .field(
                "bearer_token_override",
                &self.bearer_token_override.as_ref().map(|_| "[redacted]"),
            )
            .field(
                "extra_headers",
                &self
                    .extra_headers
                    .iter()
                    .map(|(name, _)| (name.as_str(), "[redacted]"))
                    .collect::<Vec<_>>(),
            )
            .field("extra_metadata", &self.extra_metadata)
            .field("filename_override", &self.filename_override)
            .field("content_type_override", &self.content_type_override)
            .field("existing_url", &self.existing_url)
            .finish()
    }
}

impl TusStartOptions {
    /// Sets a bearer token for this upload only, taking precedence over any
    /// [`TusConfig::bearer_token`].
    #[must_use]
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token_override = Some(token.into());
        self
    }

    /// Appends an extra request header sent with every TUS request for this
    /// upload. Call repeatedly to add more than one.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_headers.push((name.into(), value.into()));
        self
    }

    /// Adds (or overrides) a single `Upload-Metadata` key-value pair. Keys set
    /// here win over the auto-populated `filename` / `filetype`. Call
    /// repeatedly to add more than one.
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_metadata.insert(key.into(), value.into());
        self
    }

    /// Overrides the auto-populated `filename` metadata value.
    #[must_use]
    pub fn with_filename(mut self, filename: impl Into<String>) -> Self {
        self.filename_override = Some(filename.into());
        self
    }

    /// Overrides the auto-populated `filetype` metadata value.
    #[must_use]
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type_override = Some(content_type.into());
        self
    }

    /// Resumes from an existing TUS upload URL instead of creating a new one.
    /// Equivalent to [`crate::TusUploadHandle::start_with_url`].
    #[must_use]
    pub fn with_existing_url(mut self, url: impl Into<String>) -> Self {
        self.existing_url = Some(url.into());
        self
    }

    /// Returns true when these options can change request-specific server
    /// capabilities and therefore must not reuse endpoint-only OPTIONS cache.
    /// Static config-level bearer tokens are intentionally not represented here.
    ///
    /// Consulted only by the wasm upload engine; `allow(dead_code)` off-wasm.
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub(crate) fn has_request_specific_headers(&self) -> bool {
        self.bearer_token_override.is_some() || !self.extra_headers.is_empty()
    }

    /// Builds the TUS `Upload-Metadata` map for this upload.
    ///
    /// `file_name` and `file_type` come from `web_sys::File::name()` and
    /// `web_sys::File::type_()`. Overrides from `self` take precedence.
    /// `self.extra_metadata` is merged last (wins over auto-populated values).
    ///
    /// Consulted only by the wasm upload engine; `allow(dead_code)` off-wasm.
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub(crate) fn build_metadata(
        &self,
        file_name: &str,
        file_type: &str,
    ) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert(
            "filename".into(),
            self.filename_override
                .clone()
                .unwrap_or_else(|| file_name.to_string()),
        );
        m.insert(
            "filetype".into(),
            self.content_type_override
                .clone()
                .unwrap_or_else(|| file_type.to_string()),
        );
        for (k, v) in &self.extra_metadata {
            m.insert(k.clone(), v.clone());
        }
        m
    }
}

#[cfg(test)]
mod tests {
    // These exercise the `pub(crate)` engine helpers (`has_request_specific_headers`,
    // `build_metadata`, `use_creation_with_upload`), so they live here rather than in
    // the `tests/` integration suite, which can only reach the public surface.
    use super::*;

    #[test]
    fn start_options_identifies_request_specific_headers() {
        let options = TusStartOptions::default().with_bearer_token("upload-token");
        assert!(options.has_request_specific_headers());

        let options = TusStartOptions::default().with_header("X-Tenant-Id", "tenant-a");
        assert!(options.has_request_specific_headers());
    }

    #[test]
    fn start_options_without_per_upload_auth_is_options_cacheable() {
        let options = TusStartOptions::default();
        assert!(!options.has_request_specific_headers());
    }

    #[test]
    fn metadata_auto_populates_filename_and_filetype() {
        let opts = TusStartOptions::default();
        let meta = opts.build_metadata("photo.jpg", "image/jpeg");
        assert_eq!(meta.get("filename").map(String::as_str), Some("photo.jpg"));
        assert_eq!(meta.get("filetype").map(String::as_str), Some("image/jpeg"));
    }

    #[test]
    fn filename_override_replaces_auto_populated() {
        let opts = TusStartOptions::default().with_filename("renamed.jpg");
        let meta = opts.build_metadata("original.jpg", "image/jpeg");
        assert_eq!(
            meta.get("filename").map(String::as_str),
            Some("renamed.jpg")
        );
    }

    #[test]
    fn extra_metadata_is_merged() {
        let opts = TusStartOptions::default().with_metadata("user_id", "u123");
        let meta = opts.build_metadata("file.bin", "application/octet-stream");
        assert_eq!(meta.get("user_id").map(String::as_str), Some("u123"));
        assert!(meta.contains_key("filename"));
    }

    #[test]
    fn extra_metadata_key_wins_over_auto_populated() {
        let opts = TusStartOptions::default().with_metadata("filename", "from-extra.txt");
        let meta = opts.build_metadata("auto.txt", "text/plain");
        assert_eq!(
            meta.get("filename").map(String::as_str),
            Some("from-extra.txt")
        );
    }

    // =================================================================
    // creation-with-upload predicate: regression tests for the wasm32
    // 32-bit `usize` truncation bug. With the fix the predicate compares
    // in `u64` space, so files larger than `u32::MAX` no longer alias into
    // the "small enough for cwu" range.
    // =================================================================

    #[test]
    fn cwu_predicate_includes_files_under_threshold() {
        let c =
            TusConfig::new("https://x.test/files").with_creation_with_upload_threshold(256 * 1024);
        assert!(c.use_creation_with_upload(1));
        assert!(c.use_creation_with_upload(100 * 1024));
        assert!(c.use_creation_with_upload(256 * 1024));
    }

    #[test]
    fn cwu_predicate_excludes_files_over_threshold() {
        let c =
            TusConfig::new("https://x.test/files").with_creation_with_upload_threshold(256 * 1024);
        assert!(!c.use_creation_with_upload(256 * 1024 + 1));
        assert!(!c.use_creation_with_upload(10 * 1024 * 1024));
    }

    #[test]
    fn cwu_predicate_excludes_zero_size_files() {
        // Empty files don't take the cwu fast path; the create_upload branch
        // handles them and short-circuits the chunk loop.
        let c =
            TusConfig::new("https://x.test/files").with_creation_with_upload_threshold(256 * 1024);
        assert!(!c.use_creation_with_upload(0));
    }

    #[test]
    fn cwu_predicate_does_not_truncate_huge_files_on_wasm32() {
        // Pre-fix this used `(file_size as usize) <= threshold`. On wasm32 (32-bit
        // usize) a 4 GiB + 100 KiB file truncates to 100 KiB, falsely matches a
        // 256 KiB threshold, and routes the entire >4 GiB payload through the
        // load-whole-body POST, OOMing wasm linear memory. The predicate now
        // compares in u64 space so the truncation can't happen.
        let c =
            TusConfig::new("https://x.test/files").with_creation_with_upload_threshold(256 * 1024);
        let four_gib_plus_100kib: u64 = (1u64 << 32) + 100 * 1024;
        assert!(
            !c.use_creation_with_upload(four_gib_plus_100kib),
            "huge file whose low 32 bits land below the threshold must NOT \
             take the cwu path",
        );

        // Boundary at exactly 2^32: low 32 bits are 0, which would have aliased
        // to "zero-size" and ALSO failed the `> 0` guard. Make sure the u64
        // comparison gets it right.
        let exactly_four_gib: u64 = 1u64 << 32;
        assert!(!c.use_creation_with_upload(exactly_four_gib));
    }
}
