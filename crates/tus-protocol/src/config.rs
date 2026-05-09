//! TUS server configuration.
//!
//! This module defines the configuration options for a TUS server,
//! including supported extensions, size limits, and timeouts.

use std::collections::HashSet;
use std::time::Duration;

/// The TUS protocol version supported by this implementation.
pub const TUS_VERSION: &str = "1.0.0";

/// Resumable header value for the TUS protocol.
pub const TUS_RESUMABLE: &str = "1.0.0";

/// TUS server configuration.
///
/// # Defaults
///
/// [`Config::default`] enables Creation, Termination, and
/// Creation-Defer-Length; uses `/files` as the base path; has no maximum upload
/// size, no expiration, no CORS origins, and a 30-second lock timeout; and does
/// not trust forwarded proxy headers.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
/// use tus_protocol::{Config, Extension};
///
/// let config = Config::new()
///     .base_path("/uploads")
///     .max_size(100 * 1024 * 1024)
///     .expiration(Duration::from_secs(24 * 60 * 60))
///     .with_extension(Extension::Concatenation);
///
/// assert_eq!(config.base_path_str(), "/uploads");
/// assert!(config.has_extension(Extension::Concatenation));
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    /// Maximum upload size in bytes. None means unlimited.
    max_size: Option<u64>,

    /// Enabled TUS extensions.
    extensions: HashSet<Extension>,

    /// Supported checksum algorithms (only relevant if Checksum extension is enabled).
    checksum_algorithms: HashSet<ChecksumAlgorithm>,

    /// Default expiration duration for uploads. None means uploads don't expire.
    expiration: Option<Duration>,

    /// Lock acquisition timeout.
    lock_timeout: Duration,

    /// Base path for the TUS endpoint (e.g., "/files").
    base_path: String,

    /// Base URL for absolute Location headers (e.g., "http://localhost:8080").
    /// If set, Location headers will be absolute URLs.
    base_url: Option<String>,

    /// Whether to respect forwarded headers (X-Forwarded-Host, X-Forwarded-Proto).
    respect_forwarded_headers: bool,

    /// CORS allowed origins. Empty means CORS is disabled.
    cors_origins: Vec<String>,

    /// Maximum chunk size per PATCH request. None means unlimited.
    max_chunk_size: Option<u64>,

    /// Whether to disable download (GET) endpoint.
    disable_download: bool,

    /// Whether to allow creation with deferred length.
    allow_empty_creation: bool,
}

impl Default for Config {
    fn default() -> Self {
        let mut extensions = HashSet::new();
        extensions.insert(Extension::Creation);
        extensions.insert(Extension::Termination);
        extensions.insert(Extension::CreationDeferLength);

        Self {
            max_size: None,
            extensions,
            checksum_algorithms: HashSet::new(),
            expiration: None,
            lock_timeout: Duration::from_secs(30),
            base_path: "/files".to_string(),
            base_url: None,
            respect_forwarded_headers: false,
            cors_origins: vec![],
            max_chunk_size: None,
            disable_download: false,
            allow_empty_creation: true,
        }
    }
}

impl Config {
    /// Creates a new configuration with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a configuration with all extensions enabled, including the
    /// non-standard `ConcatenationUnfinished`.
    ///
    /// Note: advertising `concatenation-unfinished` means final uploads with
    /// incomplete partials are accepted (the extension's defining behavior),
    /// which supersedes the core concatenation requirement that partials must
    /// be complete. If you want strict core-concatenation semantics, use
    /// [`Config::default()`] plus explicit `.with_extension(...)` calls
    /// and don't enable `ConcatenationUnfinished`.
    pub fn with_all_extensions() -> Self {
        Self {
            extensions: Extension::supported().iter().copied().collect(),
            checksum_algorithms: [
                ChecksumAlgorithm::Sha1,
                ChecksumAlgorithm::Sha256,
                ChecksumAlgorithm::Md5,
            ]
            .into_iter()
            .collect(),
            ..Self::default()
        }
    }

    /// Sets the maximum upload size.
    pub fn max_size(mut self, size: u64) -> Self {
        self.max_size = Some(size);
        self
    }

    /// Adds an extension to the enabled set.
    ///
    /// When enabling the Checksum extension, default algorithms (sha1) are automatically
    /// added if no algorithms are already configured. Use `with_checksum()` to add
    /// additional algorithms.
    pub fn with_extension(mut self, ext: Extension) -> Self {
        #[cfg(not(feature = "checksum"))]
        if matches!(ext, Extension::Checksum | Extension::ChecksumTrailer) {
            return self;
        }

        self.extensions.insert(ext);
        // When enabling Checksum extension, ensure at least sha1 is available
        #[cfg(feature = "checksum")]
        if matches!(ext, Extension::Checksum | Extension::ChecksumTrailer) {
            self.extensions.insert(Extension::Checksum);
            if self.checksum_algorithms.is_empty() {
                self.checksum_algorithms.insert(ChecksumAlgorithm::Sha1);
            }
        }
        self
    }

    /// Removes an extension from the enabled set.
    pub fn without_extension(mut self, ext: Extension) -> Self {
        self.extensions.remove(&ext);
        self
    }

    /// Sets the expiration duration for uploads.
    pub fn expiration(mut self, duration: Duration) -> Self {
        self.expiration = Some(duration);
        self.extensions.insert(Extension::Expiration);
        self
    }

    /// Sets the lock acquisition timeout.
    pub fn lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = timeout;
        self
    }

    /// Sets the base path for the TUS endpoint.
    pub fn base_path(mut self, path: impl Into<String>) -> Self {
        self.base_path = path.into();
        self
    }

    /// Sets the base URL for absolute Location headers.
    ///
    /// When set, Location headers will include the full URL (e.g., "http://localhost:8080/files/abc").
    /// The base URL should not include a trailing slash.
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }

    /// Enables CORS for the specified origins.
    pub fn cors(mut self, origins: Vec<String>) -> Self {
        self.cors_origins = origins;
        self
    }

    /// Enables CORS for all origins.
    pub fn cors_all(mut self) -> Self {
        self.cors_origins = vec!["*".to_string()];
        self
    }

    /// Enables respect for forwarded headers.
    pub fn respect_forwarded_headers(mut self) -> Self {
        self.respect_forwarded_headers = true;
        self
    }

    /// Sets the maximum chunk size per PATCH request.
    pub fn max_chunk_size(mut self, size: u64) -> Self {
        self.max_chunk_size = Some(size);
        self
    }

    /// Disables the download endpoint.
    pub fn disable_download(mut self) -> Self {
        self.disable_download = true;
        self
    }

    /// Adds a checksum algorithm to the supported set.
    #[cfg(feature = "checksum")]
    pub fn with_checksum(mut self, algorithm: ChecksumAlgorithm) -> Self {
        self.checksum_algorithms.insert(algorithm);
        self.checksum_algorithms.insert(ChecksumAlgorithm::Sha1);
        self.extensions.insert(Extension::Checksum);
        self.extensions.insert(Extension::ChecksumTrailer);
        self
    }

    /// Controls whether empty creation requests are allowed.
    pub fn allow_empty_creation(mut self, allow: bool) -> Self {
        self.allow_empty_creation = allow;
        self
    }

    /// Checks if an extension is enabled.
    pub fn has_extension(&self, ext: Extension) -> bool {
        self.extensions.contains(&ext)
    }

    /// Returns the configured maximum upload size in bytes, if any.
    pub fn max_size_limit(&self) -> Option<u64> {
        self.max_size
    }

    /// Returns the supported checksum algorithms.
    pub fn checksum_algorithms(&self) -> &HashSet<ChecksumAlgorithm> {
        &self.checksum_algorithms
    }

    /// Returns whether a checksum algorithm is supported.
    pub fn supports_checksum_algorithm(&self, algorithm: ChecksumAlgorithm) -> bool {
        self.checksum_algorithms.contains(&algorithm)
    }

    /// Returns the configured expiration duration, if any.
    pub fn expiration_duration(&self) -> Option<Duration> {
        self.expiration
    }

    /// Returns the lock acquisition timeout.
    pub fn lock_timeout_duration(&self) -> Duration {
        self.lock_timeout
    }

    /// Returns the base path for TUS endpoints.
    pub fn base_path_str(&self) -> &str {
        &self.base_path
    }

    /// Returns the configured base URL for absolute Location headers.
    pub fn base_url_str(&self) -> Option<&str> {
        self.base_url.as_deref()
    }

    /// Returns whether forwarded proxy headers should be trusted.
    pub fn uses_forwarded_headers(&self) -> bool {
        self.respect_forwarded_headers
    }

    /// Returns the allowed CORS origins.
    pub fn cors_allowed_origins(&self) -> &[String] {
        &self.cors_origins
    }

    /// Returns the maximum PATCH chunk size, if configured.
    pub fn max_chunk_size_limit(&self) -> Option<u64> {
        self.max_chunk_size
    }

    /// Returns whether the download endpoint is disabled.
    pub fn is_download_disabled(&self) -> bool {
        self.disable_download
    }

    /// Returns whether empty creation requests are allowed.
    pub fn allows_empty_creation(&self) -> bool {
        self.allow_empty_creation
    }

    /// Returns the extensions as a comma-separated string for the Tus-Extension header.
    pub fn extensions_string(&self) -> String {
        let mut exts: Vec<_> = self.extensions.iter().map(|e| e.as_str()).collect();
        exts.sort();
        exts.join(",")
    }

    /// Returns the checksum algorithms as a comma-separated string.
    pub fn checksum_algorithms_string(&self) -> String {
        let mut algs: Vec<_> = self
            .checksum_algorithms
            .iter()
            .map(|a| a.as_str())
            .collect();
        algs.sort();
        algs.join(",")
    }

    /// Builds the full URL for an upload.
    ///
    /// Uses the following priority for base URL:
    /// 1. Config's base_url if set
    /// 2. Request's base URL (from scheme + host) if provided
    /// 3. Falls back to relative path
    pub fn upload_url(&self, upload_id: &str, request_base_url: Option<&str>) -> String {
        let base = self.base_url.as_deref().or(request_base_url);
        match base {
            Some(base) => format!("{}{}/{}", base, self.base_path, upload_id),
            None => format!("{}/{}", self.base_path, upload_id),
        }
    }
}

/// TUS protocol extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Extension {
    /// Creation extension - allows creating new uploads via POST.
    Creation,
    /// Creation with upload - allows including data in the initial POST request.
    CreationWithUpload,
    /// Creation with deferred length - allows uploads without knowing size upfront.
    CreationDeferLength,
    /// Termination extension - allows deleting uploads via DELETE.
    Termination,
    /// Expiration extension - uploads can expire and be cleaned up.
    Expiration,
    /// Concatenation extension - allows merging partial uploads.
    Concatenation,
    /// Concatenation-Unfinished extension - allows creating final uploads before all partials are complete.
    ConcatenationUnfinished,
    /// Checksum extension - allows verifying chunk integrity.
    Checksum,
    /// Checksum trailer extension - allows checksums to be sent as HTTP trailers.
    ChecksumTrailer,
}

impl Extension {
    /// Returns the extension name as used in the Tus-Extension header.
    pub fn as_str(&self) -> &'static str {
        match self {
            Extension::Creation => "creation",
            Extension::CreationWithUpload => "creation-with-upload",
            Extension::CreationDeferLength => "creation-defer-length",
            Extension::Termination => "termination",
            Extension::Expiration => "expiration",
            Extension::Concatenation => "concatenation",
            Extension::ConcatenationUnfinished => "concatenation-unfinished",
            Extension::Checksum => "checksum",
            Extension::ChecksumTrailer => "checksum-trailer",
        }
    }

    /// Parses an extension from its string name.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "creation" => Some(Extension::Creation),
            "creation-with-upload" => Some(Extension::CreationWithUpload),
            "creation-defer-length" => Some(Extension::CreationDeferLength),
            "termination" => Some(Extension::Termination),
            "expiration" => Some(Extension::Expiration),
            "concatenation" => Some(Extension::Concatenation),
            "concatenation-unfinished" => Some(Extension::ConcatenationUnfinished),
            "checksum" => Some(Extension::Checksum),
            "checksum-trailer" => Some(Extension::ChecksumTrailer),
            _ => None,
        }
    }

    /// Returns all extensions supported by this build.
    pub fn supported() -> &'static [Extension] {
        &[
            Extension::Creation,
            Extension::CreationWithUpload,
            Extension::CreationDeferLength,
            Extension::Termination,
            Extension::Expiration,
            Extension::Concatenation,
            Extension::ConcatenationUnfinished,
            #[cfg(feature = "checksum")]
            Extension::Checksum,
            #[cfg(feature = "checksum")]
            Extension::ChecksumTrailer,
        ]
    }
}

/// Checksum algorithms supported by the checksum extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChecksumAlgorithm {
    /// SHA-1 hash.
    Sha1,
    /// SHA-256 hash.
    Sha256,
    /// MD5 hash.
    Md5,
    /// CRC32 checksum.
    Crc32,
}

impl ChecksumAlgorithm {
    /// Returns the algorithm name as used in the Tus-Checksum-Algorithm header.
    pub fn as_str(&self) -> &'static str {
        match self {
            ChecksumAlgorithm::Sha1 => "sha1",
            ChecksumAlgorithm::Sha256 => "sha256",
            ChecksumAlgorithm::Md5 => "md5",
            ChecksumAlgorithm::Crc32 => "crc32",
        }
    }

    /// Parses an algorithm from its string name.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "sha1" => Some(ChecksumAlgorithm::Sha1),
            "sha256" => Some(ChecksumAlgorithm::Sha256),
            "md5" => Some(ChecksumAlgorithm::Md5),
            "crc32" => Some(ChecksumAlgorithm::Crc32),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_supported_extensions_match_build_features() {
        let extensions = Extension::supported();

        assert!(extensions.contains(&Extension::Creation));
        assert!(extensions.contains(&Extension::ConcatenationUnfinished));

        #[cfg(feature = "checksum")]
        {
            assert!(extensions.contains(&Extension::Checksum));
            assert!(extensions.contains(&Extension::ChecksumTrailer));
        }

        #[cfg(not(feature = "checksum"))]
        {
            assert!(!extensions.contains(&Extension::Checksum));
            assert!(!extensions.contains(&Extension::ChecksumTrailer));
        }
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.has_extension(Extension::Creation));
        assert!(config.has_extension(Extension::Termination));
        assert!(!config.has_extension(Extension::Checksum));
    }

    #[cfg(feature = "checksum")]
    #[test]
    fn test_builder_pattern() {
        let config = Config::new()
            .max_size(1024 * 1024 * 100) // 100 MB
            .with_extension(Extension::Checksum)
            .with_checksum(ChecksumAlgorithm::Sha256)
            .expiration(Duration::from_secs(3600))
            .base_path("/uploads");

        assert_eq!(config.max_size_limit(), Some(100 * 1024 * 1024));
        assert!(config.has_extension(Extension::Checksum));
        assert!(config.has_extension(Extension::Expiration));
        assert!(config.supports_checksum_algorithm(ChecksumAlgorithm::Sha256));
        assert_eq!(config.base_path_str(), "/uploads");
    }

    #[test]
    fn test_extensions_string() {
        let config = Config::new()
            .with_extension(Extension::Creation)
            .with_extension(Extension::Termination);

        let exts = config.extensions_string();
        assert!(exts.contains("creation"));
        assert!(exts.contains("termination"));
    }
}
