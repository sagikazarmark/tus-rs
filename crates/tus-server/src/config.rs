use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use tus_protocol::{Config as TusConfig, Extension};
// The `opendal` version and enabled services come from
// tus-storage-opendal's re-export and `services-*` passthrough
// features, so this crate needs no version-locked `opendal`
// dependency of its own.
use tus_storage_opendal::opendal;

pub(crate) const DEFAULT_STORAGE_URI: &str = "fs://";
pub(crate) const DEFAULT_FS_ROOT: &str = "./uploads";

/// Default cap on a single HTTP request body: 1 GiB. `0` is the
/// explicit opt-out meaning "unlimited".
pub(crate) const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 1024 * 1024 * 1024;
/// Default idle timeout between successive body frames: 60 seconds.
/// `0` is the explicit opt-out meaning "disabled".
pub(crate) const DEFAULT_REQUEST_BODY_READ_TIMEOUT_SECS: u64 = 60;
/// Default limit on how long a connection may spend sending its
/// request headers (slowloris defense for the header phase, matching
/// hyper's intended default): 30 seconds. `0` is the explicit opt-out
/// meaning "disabled".
pub(crate) const DEFAULT_REQUEST_HEADER_READ_TIMEOUT_SECS: u64 = 30;
/// Default cap on a single PATCH chunk: 256 MiB. Bounds per-request
/// memory/staging on a stock server. `0` is the explicit opt-out
/// meaning "unlimited".
pub(crate) const DEFAULT_MAX_CHUNK_SIZE: u64 = 256 * 1024 * 1024;

/// TUS Resumable Upload Server
#[derive(Parser, Clone, Debug)]
#[command(name = "tus-server")]
#[command(about = "A TUS protocol server for resumable file uploads")]
#[command(version)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Clone, Debug)]
pub(crate) enum Command {
    /// Run the TUS HTTP server.
    ///
    /// Deployment constraints: upload locking uses a process-local
    /// in-memory locker, so run only a single server instance per
    /// storage bucket and state directory. Two replicas sharing a
    /// bucket or state directory will race on concurrent PATCH,
    /// termination, and expiration cleanup. File-based locking would
    /// only help when every instance shares the same local
    /// filesystem; it does not make object-storage backends safe for
    /// multiple replicas.
    Serve(Box<ServeCli>),
    /// Remove expired upload data and state once, then exit.
    ///
    /// The cleanup command builds its own process-local memory
    /// locker, so it cannot see locks held by a running serve
    /// process. Only run it while the server is stopped, and pass
    /// --force to acknowledge that.
    Cleanup(CleanupCli),
}

#[derive(Parser, Clone, Debug)]
pub(crate) struct ServeCli {
    /// Address to listen on. Env: TUS_ADDR.
    #[arg(long = "addr")]
    pub(crate) addr: Option<BindTarget>,

    /// OpenDAL storage URI for uploaded bytes. Env: TUS_STORAGE_URI.
    #[arg(long = "storage-uri")]
    pub(crate) storage_uri: Option<String>,

    /// Directory to store upload state/metadata. Env: TUS_STATE_DIR.
    #[arg(short, long)]
    pub(crate) state_dir: Option<PathBuf>,

    /// Maximum upload size in bytes (0 = unlimited). Env: TUS_MAX_SIZE.
    #[arg(short, long)]
    pub(crate) max_size: Option<u64>,

    /// Maximum bytes accepted per PATCH chunk (0 = unlimited, default 256 MiB). Env: TUS_MAX_CHUNK_SIZE.
    #[arg(long)]
    pub(crate) max_chunk_size: Option<u64>,

    /// Base path for TUS endpoints. Env: TUS_BASE_PATH.
    #[arg(long)]
    pub(crate) base_path: Option<String>,

    /// Enable CORS for all origins. Env: TUS_CORS.
    #[arg(long, action = clap::ArgAction::Set, default_missing_value = "true", num_args = 0..=1, require_equals = true)]
    pub(crate) cors: Option<bool>,

    /// Allow CORS only from these exact origins. Env: TUS_CORS_ORIGIN.
    #[arg(long = "cors-origin", value_delimiter = ',')]
    pub(crate) cors_origins: Option<Vec<String>>,

    /// Upload expiration as seconds or a duration string such as 5s, 10m, or 1h. Env: TUS_EXPIRATION.
    #[arg(long)]
    pub(crate) expiration: Option<DurationValue>,

    /// Interval between background scans that delete expired uploads. Env: TUS_EXPIRATION_SCAN_INTERVAL.
    #[arg(long)]
    pub(crate) expiration_scan_interval: Option<DurationValue>,

    /// Disable GET downloads on upload resource URLs. Env: TUS_DISABLE_DOWNLOAD.
    #[arg(long, action = clap::ArgAction::Set, default_missing_value = "true", num_args = 0..=1, require_equals = true)]
    pub(crate) disable_download: Option<bool>,

    /// Base URL for absolute Location headers. Env: TUS_BASE_URL.
    ///
    /// Behind a TLS-terminating proxy either this option or
    /// --respect-forwarded-headers is required for correct absolute
    /// Location URLs.
    #[arg(long)]
    pub(crate) base_url: Option<String>,

    /// Trust Forwarded/X-Forwarded-* headers when building absolute URLs. Env: TUS_RESPECT_FORWARDED_HEADERS.
    ///
    /// Off by default. Behind a TLS-terminating proxy either this
    /// flag or --base-url is required for correct absolute Location
    /// URLs. Enable it only when a trusted proxy sets these headers.
    #[arg(long, action = clap::ArgAction::Set, default_missing_value = "true", num_args = 0..=1, require_equals = true)]
    pub(crate) respect_forwarded_headers: Option<bool>,

    /// Enable all TUS extensions. Env: TUS_ALL_EXTENSIONS.
    #[arg(long, action = clap::ArgAction::Set, default_missing_value = "true", num_args = 0..=1, require_equals = true)]
    pub(crate) all_extensions: Option<bool>,

    /// Disable the non-standard concatenation-unfinished extension. Env: TUS_DISABLE_CONCATENATION_UNFINISHED.
    #[arg(long, action = clap::ArgAction::Set, default_missing_value = "true", num_args = 0..=1, require_equals = true)]
    pub(crate) disable_concatenation_unfinished: Option<bool>,

    /// Disable checksum-trailer advertisement while keeping bodied checksums. Env: TUS_DISABLE_CHECKSUM_TRAILER.
    #[arg(long, action = clap::ArgAction::Set, default_missing_value = "true", num_args = 0..=1, require_equals = true)]
    pub(crate) disable_checksum_trailer: Option<bool>,

    /// Path to a TOML or YAML config file. Env: TUS_CONFIG.
    #[arg(long, env = "TUS_CONFIG")]
    pub(crate) config: Option<PathBuf>,

    /// Grace period the server waits for in-flight requests to finish. Env: TUS_SHUTDOWN_GRACE.
    #[arg(long)]
    pub(crate) shutdown_grace: Option<u64>,

    /// Lame-duck delay before draining in-flight requests. Env: TUS_DRAIN_DELAY.
    #[arg(long)]
    pub(crate) drain_delay: Option<u64>,

    /// Webhook endpoint URL. Env: TUS_HOOK_URL.
    #[arg(long = "hook-url")]
    pub(crate) hook_url: Option<String>,

    /// Webhook request timeout in seconds. Env: TUS_HOOK_TIMEOUT.
    #[arg(long = "hook-timeout")]
    pub(crate) hook_timeout: Option<u64>,

    /// Extra header to send with each webhook; repeat the flag for multiple headers. Env: TUS_HOOK_HEADER.
    ///
    /// Each occurrence is one `Name: Value` pair, so values may
    /// contain commas. The TUS_HOOK_HEADER environment variable
    /// holds newline-separated pairs for the same reason.
    #[arg(long = "hook-header")]
    pub(crate) hook_header: Option<Vec<String>>,

    /// Retry webhook requests with exponential backoff. Env: TUS_HOOK_RETRY.
    #[arg(long = "hook-retry", action = clap::ArgAction::Set, default_missing_value = "true", num_args = 0..=1, require_equals = true)]
    pub(crate) hook_retry: Option<bool>,

    /// Maximum retry attempts when webhook retry is enabled. Env: TUS_HOOK_MAX_RETRIES.
    #[arg(long = "hook-max-retries")]
    pub(crate) hook_max_retries: Option<u32>,

    /// Shared secret used to sign webhook bodies with HMAC-SHA256. Env: TUS_HOOK_SIGNING_SECRET.
    #[arg(long = "hook-signing-secret")]
    pub(crate) hook_signing_secret: Option<String>,

    /// Maximum request body size in bytes (0 = unlimited, default 1 GiB). Env: TUS_MAX_REQUEST_BODY_BYTES.
    #[arg(long)]
    pub(crate) max_request_body_bytes: Option<usize>,

    /// Idle timeout in seconds between successive body frames (0 = disabled, default 60). Env: TUS_REQUEST_BODY_READ_TIMEOUT.
    #[arg(long)]
    pub(crate) request_body_read_timeout: Option<u64>,

    /// Maximum seconds a connection may spend sending request headers (0 = disabled, default 30). Env: TUS_REQUEST_HEADER_READ_TIMEOUT.
    #[arg(long)]
    pub(crate) request_header_read_timeout: Option<u64>,

    /// Require an Authorization: Bearer token on every TUS request. Env: TUS_AUTH_TOKEN.
    ///
    /// Warning: tokens passed on the command line are visible in
    /// process listings (ps, /proc). Prefer the TUS_AUTH_TOKEN
    /// environment variable or the auth_token config-file key.
    #[arg(long = "auth-token", value_delimiter = ',')]
    pub(crate) auth_token: Option<Vec<String>>,

    /// Log output format. Env: TUS_LOG_FORMAT.
    #[arg(long, value_enum)]
    pub(crate) log_format: Option<LogFormat>,

    /// Disable the in-process sweeper that reclaims expired upload data and state. Env: TUS_DISABLE_EXPIRATION_RECLAMATION.
    ///
    /// Reclamation deletes the on-disk data and state of uploads that
    /// have passed their expiration deadline. It runs automatically
    /// whenever --expiration is set, so expired uploads do not
    /// accumulate. Pass this flag to keep expiry enforced on access
    /// while leaving expired data in place, for example to reclaim it
    /// out-of-band with the `cleanup` subcommand.
    #[arg(long = "disable-expiration-reclamation", action = clap::ArgAction::Set, default_missing_value = "true", num_args = 0..=1, require_equals = true)]
    pub(crate) disable_expiration_reclamation: Option<bool>,
}

#[derive(Parser, Clone, Debug)]
pub(crate) struct CleanupCli {
    #[arg(long = "storage-uri")]
    pub(crate) storage_uri: Option<String>,

    #[arg(short, long)]
    pub(crate) state_dir: Option<PathBuf>,

    #[arg(long, env = "TUS_CONFIG")]
    pub(crate) config: Option<PathBuf>,

    #[arg(long, value_enum)]
    pub(crate) log_format: Option<LogFormat>,

    /// Acknowledge that cleanup must not run against a live server. Env: TUS_CLEANUP_FORCE.
    ///
    /// Cleanup builds its own process-local memory locker, so it
    /// cannot see locks held by a running serve process and can
    /// delete data mid-upload. Stop the server first, then pass this
    /// flag.
    #[arg(long, action = clap::ArgAction::Set, default_missing_value = "true", num_args = 0..=1, require_equals = true)]
    pub(crate) force: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Settings {
    pub(crate) addr: BindTarget,
    pub(crate) storage: StorageConfig,
    pub(crate) state_dir: PathBuf,
    pub(crate) max_size: u64,
    pub(crate) max_chunk_size: u64,
    pub(crate) base_path: String,
    pub(crate) cors: bool,
    pub(crate) cors_origins: Vec<String>,
    pub(crate) expiration: DurationValue,
    pub(crate) expiration_scan_interval: DurationValue,
    pub(crate) disable_download: bool,
    pub(crate) base_url: Option<String>,
    pub(crate) respect_forwarded_headers: bool,
    pub(crate) all_extensions: bool,
    pub(crate) disable_concatenation_unfinished: bool,
    pub(crate) disable_checksum_trailer: bool,
    pub(crate) shutdown_grace: u64,
    pub(crate) drain_delay: u64,
    pub(crate) hook: HookConfig,
    pub(crate) max_request_body_bytes: usize,
    pub(crate) request_body_read_timeout: u64,
    pub(crate) request_header_read_timeout: u64,
    pub(crate) auth_token: Vec<String>,
    pub(crate) log_format: LogFormat,
    pub(crate) disable_expiration_reclamation: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            addr: BindTarget::Tcp("127.0.0.1:8080".parse().unwrap()),
            storage: StorageConfig::default(),
            state_dir: PathBuf::from("./state"),
            max_size: 0,
            max_chunk_size: DEFAULT_MAX_CHUNK_SIZE,
            base_path: "/files".to_string(),
            cors: false,
            cors_origins: Vec::new(),
            expiration: DurationValue::default(),
            expiration_scan_interval: DurationValue(Duration::from_secs(60)),
            disable_download: false,
            base_url: None,
            respect_forwarded_headers: false,
            all_extensions: false,
            disable_concatenation_unfinished: false,
            disable_checksum_trailer: false,
            shutdown_grace: 30,
            drain_delay: 0,
            hook: HookConfig::default(),
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            request_body_read_timeout: DEFAULT_REQUEST_BODY_READ_TIMEOUT_SECS,
            request_header_read_timeout: DEFAULT_REQUEST_HEADER_READ_TIMEOUT_SECS,
            auth_token: Vec::new(),
            log_format: LogFormat::Text,
            disable_expiration_reclamation: false,
        }
    }
}

/// Storage byte backend plus the on-disk upload-state location. Shared
/// by the serve runtime and the `cleanup` command so both build their
/// backends from exactly the same source fields.
#[derive(Clone, Debug)]
pub(crate) struct BackendConfig {
    pub(crate) storage: StorageConfig,
    pub(crate) state_dir: PathBuf,
}

/// HTTP runtime group consumed by the serving loop: the bind target and
/// the connection-lifecycle durations.
#[derive(Clone, Debug)]
pub(crate) struct RuntimeConfig {
    pub(crate) addr: BindTarget,
    pub(crate) shutdown_grace: Duration,
    pub(crate) drain_delay: Duration,
    pub(crate) header_read_timeout: Duration,
}

/// The subset of settings that maps into `tus_protocol::Config`: the
/// protocol-behavior group consumed by [`build_tus_config`].
#[derive(Clone, Debug)]
pub(crate) struct ProtocolConfig {
    pub(crate) base_path: String,
    pub(crate) base_url: Option<String>,
    pub(crate) max_size: u64,
    pub(crate) max_chunk_size: u64,
    pub(crate) respect_forwarded_headers: bool,
    pub(crate) expiration: Duration,
    pub(crate) disable_download: bool,
    pub(crate) all_extensions: bool,
    pub(crate) disable_concatenation_unfinished: bool,
    pub(crate) disable_checksum_trailer: bool,
}

/// Expiration-reclamation policy for the in-process sweeper.
#[derive(Clone, Debug)]
pub(crate) struct ReclamationConfig {
    /// Upload expiration window; zero means expiration is not configured.
    pub(crate) expiration: Duration,
    /// Interval between background scans that delete expired uploads.
    pub(crate) scan_interval: Duration,
    /// Operator opt-out of in-process reclamation.
    pub(crate) disabled: bool,
}

impl ReclamationConfig {
    /// Whether expiration is configured at all (a non-zero window).
    pub(crate) fn expiration_configured(&self) -> bool {
        !self.expiration.is_zero()
    }

    /// Whether the in-process sweeper should run: expiration is
    /// configured and reclamation has not been disabled.
    pub(crate) fn is_enabled(&self) -> bool {
        self.expiration_configured() && !self.disabled
    }
}

/// Auth and HTTP-body group consumed when building the axum application:
/// the bearer tokens, the request-body bounds, and the resolved list of
/// CORS-allowed origins.
#[derive(Clone, Debug, Default)]
pub(crate) struct AppConfig {
    pub(crate) auth_token: Vec<String>,
    pub(crate) max_request_body_bytes: usize,
    pub(crate) request_body_read_timeout: u64,
    pub(crate) cors_origins: Vec<String>,
}

impl Settings {
    /// Storage + state backend locations, shared with the cleanup path.
    pub(crate) fn backend(&self) -> BackendConfig {
        BackendConfig {
            storage: self.storage.clone(),
            state_dir: self.state_dir.clone(),
        }
    }

    /// HTTP runtime group: bind target and connection-lifecycle timeouts.
    pub(crate) fn runtime(&self) -> RuntimeConfig {
        RuntimeConfig {
            addr: self.addr.clone(),
            shutdown_grace: Duration::from_secs(self.shutdown_grace),
            drain_delay: Duration::from_secs(self.drain_delay),
            header_read_timeout: Duration::from_secs(self.request_header_read_timeout),
        }
    }

    /// Protocol-behavior group consumed by [`build_tus_config`].
    pub(crate) fn protocol(&self) -> ProtocolConfig {
        ProtocolConfig {
            base_path: self.base_path.clone(),
            base_url: self.base_url.clone(),
            max_size: self.max_size,
            max_chunk_size: self.max_chunk_size,
            respect_forwarded_headers: self.respect_forwarded_headers,
            expiration: self.expiration.as_duration(),
            disable_download: self.disable_download,
            all_extensions: self.all_extensions,
            disable_concatenation_unfinished: self.disable_concatenation_unfinished,
            disable_checksum_trailer: self.disable_checksum_trailer,
        }
    }

    /// Expiration-reclamation policy for the in-process sweeper.
    pub(crate) fn reclamation(&self) -> ReclamationConfig {
        ReclamationConfig {
            expiration: self.expiration.as_duration(),
            scan_interval: self.expiration_scan_interval.as_duration(),
            disabled: self.disable_expiration_reclamation,
        }
    }

    /// Auth + HTTP-body + resolved CORS group consumed by `build_app`.
    pub(crate) fn app(&self) -> AppConfig {
        AppConfig {
            auth_token: self.auth_token.clone(),
            max_request_body_bytes: self.max_request_body_bytes,
            request_body_read_timeout: self.request_body_read_timeout,
            cors_origins: self.resolved_cors_origins(),
        }
    }

    /// The effective CORS allow-list: explicit origins win; otherwise a
    /// bare `cors = true` allows any origin (`*`); otherwise CORS is off.
    fn resolved_cors_origins(&self) -> Vec<String> {
        if !self.cors_origins.is_empty() {
            self.cors_origins.clone()
        } else if self.cors {
            vec!["*".to_string()]
        } else {
            Vec::new()
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct StorageConfig {
    #[serde(default = "default_storage_uri")]
    pub(crate) uri: String,
    #[serde(flatten)]
    pub(crate) settings: BTreeMap<String, String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            uri: default_storage_uri(),
            settings: BTreeMap::new(),
        }
    }
}

pub(crate) fn default_storage_uri() -> String {
    DEFAULT_STORAGE_URI.to_string()
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct HookConfig {
    pub(crate) url: Option<String>,
    pub(crate) timeout: u64,
    pub(crate) header: Vec<String>,
    pub(crate) retry: bool,
    pub(crate) max_retries: u32,
    pub(crate) signing_secret: Option<String>,
}

impl Default for HookConfig {
    fn default() -> Self {
        Self {
            url: None,
            timeout: 30,
            header: Vec::new(),
            retry: false,
            max_retries: 3,
            signing_secret: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct SettingsPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) addr: Option<BindTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) storage: Option<StoragePatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) state_dir: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_chunk_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) base_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cors: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cors_origins: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expiration: Option<DurationValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expiration_scan_interval: Option<DurationValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) disable_download: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) base_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) respect_forwarded_headers: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) all_extensions: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) disable_concatenation_unfinished: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) disable_checksum_trailer: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) shutdown_grace: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) drain_delay: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) hook: Option<HookPatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_request_body_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) request_body_read_timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) request_header_read_timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) auth_token: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) log_format: Option<LogFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) disable_expiration_reclamation: Option<bool>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct StoragePatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) uri: Option<String>,
    #[serde(flatten, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) settings: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct HookPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) header: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) retry: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_retries: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) signing_secret: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct CleanupSettingsPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    storage: Option<StoragePatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state_dir: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    log_format: Option<LogFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    force: Option<bool>,
}

impl ServeCli {
    pub(crate) fn settings_patch(&self) -> SettingsPatch {
        SettingsPatch {
            addr: self.addr.clone(),
            storage: self.storage_uri.as_ref().map(|uri| StoragePatch {
                uri: Some(uri.clone()),
                settings: BTreeMap::new(),
            }),
            state_dir: self.state_dir.clone(),
            max_size: self.max_size,
            max_chunk_size: self.max_chunk_size,
            base_path: self.base_path.clone(),
            cors: self.cors,
            cors_origins: self.cors_origins.clone(),
            expiration: self.expiration,
            expiration_scan_interval: self.expiration_scan_interval,
            disable_download: self.disable_download,
            base_url: self.base_url.clone(),
            respect_forwarded_headers: self.respect_forwarded_headers,
            all_extensions: self.all_extensions,
            disable_concatenation_unfinished: self.disable_concatenation_unfinished,
            disable_checksum_trailer: self.disable_checksum_trailer,
            shutdown_grace: self.shutdown_grace,
            drain_delay: self.drain_delay,
            hook: hook_patch_from_serve_cli(self),
            max_request_body_bytes: self.max_request_body_bytes,
            request_body_read_timeout: self.request_body_read_timeout,
            request_header_read_timeout: self.request_header_read_timeout,
            auth_token: self.auth_token.clone(),
            log_format: self.log_format,
            disable_expiration_reclamation: self.disable_expiration_reclamation,
        }
    }
}

impl CleanupCli {
    fn settings_patch(&self) -> CleanupSettingsPatch {
        CleanupSettingsPatch {
            storage: self.storage_uri.as_ref().map(|uri| StoragePatch {
                uri: Some(uri.clone()),
                settings: BTreeMap::new(),
            }),
            state_dir: self.state_dir.clone(),
            log_format: self.log_format,
            force: self.force,
        }
    }
}

pub(crate) fn hook_patch_from_serve_cli(cli: &ServeCli) -> Option<HookPatch> {
    let patch = HookPatch {
        url: cli.hook_url.clone(),
        timeout: cli.hook_timeout,
        header: cli.hook_header.clone(),
        retry: cli.hook_retry,
        max_retries: cli.hook_max_retries,
        signing_secret: cli.hook_signing_secret.clone(),
    };

    (patch.url.is_some()
        || patch.timeout.is_some()
        || patch.header.is_some()
        || patch.retry.is_some()
        || patch.max_retries.is_some()
        || patch.signing_secret.is_some())
    .then_some(patch)
}

#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum LogFormat {
    Text,
    Json,
}

impl FromStr for LogFormat {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        <Self as ValueEnum>::from_str(value, true)
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) struct DurationValue(Duration);

#[allow(dead_code)]
impl DurationValue {
    pub(crate) fn as_duration(self) -> Duration {
        self.0
    }

    pub(crate) fn as_secs(self) -> u64 {
        self.0.as_secs()
    }
}

impl FromStr for DurationValue {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        if let Ok(seconds) = value.parse::<u64>() {
            return Ok(Self(Duration::from_secs(seconds)));
        }

        humantime::parse_duration(value)
            .map(Self)
            .map_err(|error| format!("invalid duration `{value}`: {error}"))
    }
}

impl Serialize for DurationValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&humantime::format_duration(self.as_duration()).to_string())
    }
}

impl<'de> Deserialize<'de> for DurationValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = DurationValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a duration as seconds or a string such as 5s, 10m, or 1h")
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(DurationValue(Duration::from_secs(value)))
            }

            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value < 0 {
                    return Err(E::custom("duration seconds must not be negative"));
                }
                Ok(DurationValue(Duration::from_secs(value as u64)))
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                value.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BindTarget {
    Tcp(SocketAddr),
    #[cfg(unix)]
    Unix(PathBuf),
}

impl fmt::Display for BindTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindTarget::Tcp(addr) => write!(f, "{addr}"),
            #[cfg(unix)]
            BindTarget::Unix(path) => write!(f, "unix:{}", path.display()),
        }
    }
}

impl FromStr for BindTarget {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if let Some(path) = s.strip_prefix("unix:") {
            #[cfg(unix)]
            {
                if path.is_empty() {
                    return Err("unix bind path must not be empty".to_string());
                }
                return Ok(Self::Unix(PathBuf::from(path)));
            }

            #[cfg(not(unix))]
            {
                return Err("unix socket binding is only supported on unix".to_string());
            }
        }

        s.parse::<SocketAddr>()
            .map(Self::Tcp)
            .map_err(|e| format!("invalid bind address `{s}`: {e}"))
    }
}

impl Serialize for BindTarget {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for BindTarget {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

pub(crate) fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()
}

/// Splits an env value on newlines. Used for TUS_HOOK_HEADER, whose
/// `Name: Value` entries may legitimately contain commas, so CSV
/// splitting would corrupt them.
pub(crate) fn split_newlines(value: &str) -> Vec<String> {
    value
        .lines()
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()
}

pub(crate) fn parse_env_value<T>(key: &str, value: &str) -> anyhow::Result<T>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    value
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid value for {key}: {error}"))
}

pub(crate) fn settings_patch_from_env_vars<I, K, V>(vars: I) -> anyhow::Result<SettingsPatch>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut patch = SettingsPatch::default();
    let mut storage = StoragePatch::default();
    let mut has_storage = false;
    let mut hook = HookPatch::default();
    let mut has_hook = false;

    for (key, value) in vars {
        let key = key.as_ref();
        let value = value.as_ref();
        // Empty env values are skipped rather than treated as
        // overrides, so an empty TUS_* variable cannot reset a
        // config-file value back to empty. Unset the variable and use
        // a config file or CLI flag to express an empty value.
        if value.is_empty() {
            continue;
        }

        if apply_storage_env_var(key, value, &mut storage) {
            has_storage = true;
            continue;
        }

        if let Some(name) = key.strip_prefix("TUS_HOOK_") {
            match name {
                "URL" => hook.url = Some(value.to_string()),
                "TIMEOUT" => hook.timeout = Some(parse_env_value(key, value)?),
                // Newline-separated: header values may contain commas.
                "HEADER" => hook.header = Some(split_newlines(value)),
                "RETRY" => hook.retry = Some(parse_env_value(key, value)?),
                "MAX_RETRIES" => hook.max_retries = Some(parse_env_value(key, value)?),
                "SIGNING_SECRET" => hook.signing_secret = Some(value.to_string()),
                // Unknown TUS_HOOK_* keys are reported by
                // unknown_tus_env_keys at startup.
                _ => continue,
            }
            has_hook = true;
            continue;
        }

        match key {
            "TUS_ADDR" => patch.addr = Some(parse_env_value(key, value)?),
            "TUS_STATE_DIR" => patch.state_dir = Some(PathBuf::from(value)),
            "TUS_MAX_SIZE" => patch.max_size = Some(parse_env_value(key, value)?),
            "TUS_MAX_CHUNK_SIZE" => patch.max_chunk_size = Some(parse_env_value(key, value)?),
            "TUS_BASE_PATH" => patch.base_path = Some(value.to_string()),
            "TUS_CORS" => patch.cors = Some(parse_env_value(key, value)?),
            "TUS_CORS_ORIGIN" => patch.cors_origins = Some(split_csv(value)),
            "TUS_EXPIRATION" => patch.expiration = Some(parse_env_value(key, value)?),
            "TUS_EXPIRATION_SCAN_INTERVAL" => {
                patch.expiration_scan_interval = Some(parse_env_value(key, value)?);
            }
            "TUS_DISABLE_DOWNLOAD" => patch.disable_download = Some(parse_env_value(key, value)?),
            "TUS_BASE_URL" => patch.base_url = Some(value.to_string()),
            "TUS_RESPECT_FORWARDED_HEADERS" => {
                patch.respect_forwarded_headers = Some(parse_env_value(key, value)?);
            }
            "TUS_ALL_EXTENSIONS" => patch.all_extensions = Some(parse_env_value(key, value)?),
            "TUS_DISABLE_CONCATENATION_UNFINISHED" => {
                patch.disable_concatenation_unfinished = Some(parse_env_value(key, value)?);
            }
            "TUS_DISABLE_CHECKSUM_TRAILER" => {
                patch.disable_checksum_trailer = Some(parse_env_value(key, value)?);
            }
            "TUS_SHUTDOWN_GRACE" => patch.shutdown_grace = Some(parse_env_value(key, value)?),
            "TUS_DRAIN_DELAY" => patch.drain_delay = Some(parse_env_value(key, value)?),
            "TUS_MAX_REQUEST_BODY_BYTES" => {
                patch.max_request_body_bytes = Some(parse_env_value(key, value)?);
            }
            "TUS_REQUEST_BODY_READ_TIMEOUT" => {
                patch.request_body_read_timeout = Some(parse_env_value(key, value)?);
            }
            "TUS_REQUEST_HEADER_READ_TIMEOUT" => {
                patch.request_header_read_timeout = Some(parse_env_value(key, value)?);
            }
            "TUS_AUTH_TOKEN" => patch.auth_token = Some(split_csv(value)),
            "TUS_LOG_FORMAT" => patch.log_format = Some(parse_env_value(key, value)?),
            "TUS_DISABLE_EXPIRATION_RECLAMATION" => {
                patch.disable_expiration_reclamation = Some(parse_env_value(key, value)?);
            }
            _ => {}
        }
    }

    if has_storage {
        patch.storage = Some(storage);
    }
    if has_hook {
        patch.hook = Some(hook);
    }

    Ok(patch)
}

pub(crate) fn env_settings_patch() -> anyhow::Result<SettingsPatch> {
    settings_patch_from_env_vars(std::env::vars())
}

/// Every exact TUS_* env key the server recognizes across commands.
/// TUS_STORAGE_* is accepted with any suffix (forwarded to the
/// storage backend), and TUS_HOOK_* only with the suffixes listed in
/// KNOWN_TUS_HOOK_ENV_SUFFIXES.
const KNOWN_TUS_ENV_KEYS: &[&str] = &[
    "TUS_ADDR",
    "TUS_ALL_EXTENSIONS",
    "TUS_AUTH_TOKEN",
    "TUS_BASE_PATH",
    "TUS_BASE_URL",
    "TUS_CLEANUP_FORCE",
    "TUS_CONFIG",
    "TUS_CORS",
    "TUS_CORS_ORIGIN",
    "TUS_DISABLE_CHECKSUM_TRAILER",
    "TUS_DISABLE_CONCATENATION_UNFINISHED",
    "TUS_DISABLE_DOWNLOAD",
    "TUS_DISABLE_EXPIRATION_RECLAMATION",
    "TUS_DRAIN_DELAY",
    "TUS_EXPIRATION",
    "TUS_EXPIRATION_SCAN_INTERVAL",
    "TUS_LOG_FORMAT",
    "TUS_MAX_CHUNK_SIZE",
    "TUS_MAX_REQUEST_BODY_BYTES",
    "TUS_MAX_SIZE",
    "TUS_REQUEST_BODY_READ_TIMEOUT",
    "TUS_REQUEST_HEADER_READ_TIMEOUT",
    "TUS_RESPECT_FORWARDED_HEADERS",
    "TUS_SHUTDOWN_GRACE",
    "TUS_STATE_DIR",
];

const KNOWN_TUS_HOOK_ENV_SUFFIXES: &[&str] = &[
    "HEADER",
    "MAX_RETRIES",
    "RETRY",
    "SIGNING_SECRET",
    "TIMEOUT",
    "URL",
];

/// Returns TUS_-prefixed env keys the server does not recognize, so
/// startup can warn about typos (for example TUS_HOOK_HEADERS)
/// instead of silently ignoring them.
pub(crate) fn unknown_tus_env_keys<I, K, V>(vars: I) -> Vec<String>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut unknown: Vec<String> = vars
        .into_iter()
        .filter_map(|(key, _)| {
            let key = key.as_ref();
            if !key.starts_with("TUS_") {
                return None;
            }
            if key.starts_with("TUS_STORAGE_") {
                return None;
            }
            if let Some(name) = key.strip_prefix("TUS_HOOK_") {
                return (!KNOWN_TUS_HOOK_ENV_SUFFIXES.contains(&name)).then(|| key.to_string());
            }
            (!KNOWN_TUS_ENV_KEYS.contains(&key)).then(|| key.to_string())
        })
        .collect();
    unknown.sort();
    unknown
}

/// Applies a `TUS_STORAGE_*` environment variable to a storage patch.
///
/// Returns `true` when the variable was a storage key and has been consumed.
fn apply_storage_env_var(key: &str, value: &str, storage: &mut StoragePatch) -> bool {
    let Some(name) = key.strip_prefix("TUS_STORAGE_") else {
        return false;
    };

    if name == "URI" {
        storage.uri = Some(value.to_string());
    } else {
        storage
            .settings
            .insert(name.to_ascii_lowercase(), value.to_string());
    }

    true
}

pub(crate) fn warn_unknown_tus_env_keys() {
    for key in unknown_tus_env_keys(std::env::vars()) {
        tracing::warn!(key = %key, "ignoring unrecognized TUS_-prefixed environment variable");
    }
}

fn cleanup_settings_patch_from_env_vars<I, K, V>(vars: I) -> anyhow::Result<CleanupSettingsPatch>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut patch = CleanupSettingsPatch::default();
    let mut storage = StoragePatch::default();
    let mut has_storage = false;

    for (key, value) in vars {
        let key = key.as_ref();
        let value = value.as_ref();
        // Same empty-value skip as settings_patch_from_env_vars: empty env
        // values cannot reset config-file values.
        if value.is_empty() {
            continue;
        }

        if apply_storage_env_var(key, value, &mut storage) {
            has_storage = true;
            continue;
        }

        match key {
            "TUS_STATE_DIR" => patch.state_dir = Some(PathBuf::from(value)),
            "TUS_LOG_FORMAT" => patch.log_format = Some(parse_env_value(key, value)?),
            "TUS_CLEANUP_FORCE" => patch.force = Some(parse_env_value(key, value)?),
            _ => {}
        }
    }

    if has_storage {
        patch.storage = Some(storage);
    }

    Ok(patch)
}

fn cleanup_env_settings_patch() -> anyhow::Result<CleanupSettingsPatch> {
    cleanup_settings_patch_from_env_vars(std::env::vars())
}

pub(crate) fn merge_config_file(
    mut figment: figment::Figment,
    path: &Path,
) -> anyhow::Result<figment::Figment> {
    use figment::providers::Format;

    if !path.exists() {
        anyhow::bail!("config file not found: {}", path.display());
    }

    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();
    figment = match ext.as_str() {
        "toml" => figment.merge(figment::providers::Toml::file(path)),
        "yaml" | "yml" => figment.merge(figment::providers::Yaml::file(path)),
        other => anyhow::bail!(
            "unsupported config extension '.{}': use .toml, .yaml, or .yml",
            other
        ),
    };

    Ok(figment)
}

/// Merges configuration sources in explicit precedence order, lowest to
/// highest: built-in defaults, then the optional config file, then
/// environment variables, then CLI flags. Each later source overrides
/// the earlier ones field by field. Both the serve and cleanup loaders
/// go through this single helper so the precedence is defined once.
fn layer_config_sources<D, E, C>(
    defaults: D,
    config_path: Option<&Path>,
    env_patch: E,
    cli_patch: C,
) -> anyhow::Result<figment::Figment>
where
    D: Serialize,
    E: Serialize,
    C: Serialize,
{
    use figment::providers::Serialized;

    let mut figment = figment::Figment::new().merge(Serialized::defaults(defaults));

    if let Some(path) = config_path {
        figment = merge_config_file(figment, path)?;
    }

    figment = figment.merge(Serialized::defaults(env_patch));
    figment = figment.merge(Serialized::defaults(cli_patch));

    Ok(figment)
}

pub(crate) fn load_settings_from_sources(
    config_path: Option<&Path>,
    cli_patch: SettingsPatch,
) -> anyhow::Result<Settings> {
    load_settings_layered(config_path, env_settings_patch()?, cli_patch)
}

/// Layers the serve settings with an injectable environment patch so the
/// defaults < file < env < CLI precedence is unit-testable without
/// mutating process-wide environment variables.
fn load_settings_layered(
    config_path: Option<&Path>,
    env_patch: SettingsPatch,
    cli_patch: SettingsPatch,
) -> anyhow::Result<Settings> {
    layer_config_sources(Settings::default(), config_path, env_patch, cli_patch)?
        .extract()
        .map_err(|error| anyhow::anyhow!("failed to load configuration: {error}"))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct CleanupSettings {
    pub(crate) storage: StorageConfig,
    pub(crate) state_dir: PathBuf,
    pub(crate) log_format: LogFormat,
    #[serde(default)]
    pub(crate) force: bool,
}

impl Default for CleanupSettings {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            state_dir: PathBuf::from("./state"),
            log_format: LogFormat::Text,
            force: false,
        }
    }
}

pub(crate) fn load_serve_settings(cli: &ServeCli) -> anyhow::Result<(Settings, Option<PathBuf>)> {
    let config_path = cli.config.clone();
    let settings = load_settings_from_sources(config_path.as_deref(), cli.settings_patch())?;
    Ok((settings, config_path))
}

impl CleanupSettings {
    /// Storage + state backend locations. Mirrors [`Settings::backend`]
    /// so cleanup and serve build backends from the same group.
    pub(crate) fn backend(&self) -> BackendConfig {
        BackendConfig {
            storage: self.storage.clone(),
            state_dir: self.state_dir.clone(),
        }
    }
}

pub(crate) fn load_cleanup_settings(
    cli: &CleanupCli,
) -> anyhow::Result<(CleanupSettings, Option<PathBuf>)> {
    let config_path = cli.config.clone();
    let settings = load_cleanup_settings_layered(
        config_path.as_deref(),
        cleanup_env_settings_patch()?,
        cli.settings_patch(),
    )?;
    Ok((settings, config_path))
}

/// Cleanup counterpart of [`load_settings_layered`]: layers cleanup
/// settings through the shared precedence helper with an injectable
/// environment patch.
fn load_cleanup_settings_layered(
    config_path: Option<&Path>,
    env_patch: CleanupSettingsPatch,
    cli_patch: CleanupSettingsPatch,
) -> anyhow::Result<CleanupSettings> {
    layer_config_sources(
        CleanupSettings::default(),
        config_path,
        env_patch,
        cli_patch,
    )?
    .extract()
    .map_err(|error| anyhow::anyhow!("failed to load configuration: {error}"))
}

pub(crate) fn resolved_storage_options(
    config: &StorageConfig,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut settings: Vec<(String, String)> = config
        .settings
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let uri = opendal::OperatorUri::new(&config.uri, settings.clone())
        .map_err(|e| anyhow::anyhow!("invalid storage URI: {e}"))?;

    if uri.scheme() == "fs" && uri.root().is_none() && uri.option("root").is_none() {
        settings.push(("root".to_string(), DEFAULT_FS_ROOT.to_string()));
    }

    Ok(settings)
}

pub(crate) fn build_storage_operator(
    config: &StorageConfig,
) -> anyhow::Result<(opendal::Operator, String)> {
    let settings = resolved_storage_options(config)?;
    let operator_uri = opendal::OperatorUri::new(&config.uri, settings)
        .map_err(|e| anyhow::anyhow!("invalid storage URI: {e}"))?;
    let scheme = operator_uri.scheme().to_string();

    opendal::init_default_registry();
    let operator = opendal::Operator::from_uri(operator_uri)
        .map_err(|e| anyhow::anyhow!("failed to build OpenDAL storage from configured URI: {e}"))?;

    Ok((operator, scheme))
}

pub(crate) fn build_tus_config(protocol: &ProtocolConfig) -> TusConfig {
    let mut config = if protocol.all_extensions {
        TusConfig::all_extensions()
    } else {
        TusConfig::default()
    };

    config = config.with_base_path(&protocol.base_path);

    if let Some(base_url) = &protocol.base_url {
        config = config.with_base_url(base_url);
    }

    if protocol.max_size > 0 {
        config = config.with_max_size(protocol.max_size);
    }

    if protocol.max_chunk_size > 0 {
        config = config.with_max_chunk_size(protocol.max_chunk_size);
    }

    if protocol.respect_forwarded_headers {
        config = config.with_respect_forwarded_headers();
    }

    if !protocol.expiration.is_zero() {
        config = config.with_expiration(protocol.expiration);
    }

    if protocol.disable_download {
        config = config.without_download();
    }

    if protocol.disable_concatenation_unfinished {
        config = config.without_extension(Extension::ConcatenationUnfinished);
    }

    if protocol.disable_checksum_trailer {
        config = config.without_extension(Extension::ChecksumTrailer);
    }

    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse_serve<const N: usize>(args: [&str; N]) -> ServeCli {
        let cli = Cli::parse_from(args);
        let Command::Serve(serve) = cli.command else {
            panic!("expected serve command");
        };
        *serve
    }

    #[test]
    fn defaults_bound_request_bodies_and_chunks() {
        let settings = Settings::default();

        assert_eq!(settings.max_request_body_bytes, 1024 * 1024 * 1024);
        assert_eq!(settings.request_body_read_timeout, 60);
        assert_eq!(settings.request_header_read_timeout, 30);
        assert_eq!(settings.max_chunk_size, 256 * 1024 * 1024);
        assert!(!settings.respect_forwarded_headers);
    }

    #[test]
    fn zero_opts_out_of_body_and_chunk_bounds() {
        let cli = parse_serve([
            "tus-server",
            "serve",
            "--max-request-body-bytes",
            "0",
            "--request-body-read-timeout",
            "0",
            "--request-header-read-timeout",
            "0",
            "--max-chunk-size",
            "0",
        ]);
        let (settings, _) = load_serve_settings(&cli).unwrap();

        assert_eq!(settings.max_request_body_bytes, 0);
        assert_eq!(settings.request_body_read_timeout, 0);
        assert_eq!(settings.request_header_read_timeout, 0);
        assert_eq!(settings.max_chunk_size, 0);

        let config = build_tus_config(&settings.protocol());
        assert_eq!(config.max_chunk_size(), None);
    }

    #[test]
    fn parses_max_chunk_size_flag_into_tus_config() {
        let cli = parse_serve(["tus-server", "serve", "--max-chunk-size", "1048576"]);
        let (settings, _) = load_serve_settings(&cli).unwrap();

        assert_eq!(settings.max_chunk_size, 1048576);

        let config = build_tus_config(&settings.protocol());
        assert_eq!(config.max_chunk_size(), Some(1048576));
    }

    #[test]
    fn request_header_read_timeout_reads_from_env_and_config_file() {
        let patch =
            settings_patch_from_env_vars([("TUS_REQUEST_HEADER_READ_TIMEOUT", "5")]).unwrap();
        assert_eq!(patch.request_header_read_timeout, Some(5));

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.toml");
        std::fs::write(&path, "request_header_read_timeout = 10\n").unwrap();

        let settings = load_settings_from_sources(Some(&path), SettingsPatch::default()).unwrap();
        assert_eq!(settings.request_header_read_timeout, 10);
    }

    #[test]
    fn max_chunk_size_reads_from_env_and_config_file() {
        let patch = settings_patch_from_env_vars([("TUS_MAX_CHUNK_SIZE", "2048")]).unwrap();
        assert_eq!(patch.max_chunk_size, Some(2048));

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.toml");
        std::fs::write(&path, "max_chunk_size = 4096\n").unwrap();

        let settings = load_settings_from_sources(Some(&path), SettingsPatch::default()).unwrap();
        assert_eq!(settings.max_chunk_size, 4096);
    }

    #[test]
    fn parses_respect_forwarded_headers_flag_into_tus_config() {
        let cli = parse_serve(["tus-server", "serve", "--respect-forwarded-headers"]);
        let (settings, _) = load_serve_settings(&cli).unwrap();

        assert!(settings.respect_forwarded_headers);

        let config = build_tus_config(&settings.protocol());
        assert!(config.respects_forwarded_headers());
    }

    #[test]
    fn respect_forwarded_headers_reads_from_env_and_defaults_off() {
        let patch =
            settings_patch_from_env_vars([("TUS_RESPECT_FORWARDED_HEADERS", "true")]).unwrap();
        assert_eq!(patch.respect_forwarded_headers, Some(true));

        let config = build_tus_config(&Settings::default().protocol());
        assert!(!config.respects_forwarded_headers());
    }

    #[test]
    fn repeated_hook_header_flags_preserve_commas_in_values() {
        let cli = parse_serve([
            "tus-server",
            "serve",
            "--hook-header",
            "Accept: application/json, text/plain",
            "--hook-header",
            "X-Extra: value",
        ]);

        assert_eq!(
            cli.hook_header,
            Some(vec![
                "Accept: application/json, text/plain".to_string(),
                "X-Extra: value".to_string(),
            ])
        );
    }

    #[test]
    fn hook_header_env_splits_on_newlines_not_commas() {
        let patch = settings_patch_from_env_vars([(
            "TUS_HOOK_HEADER",
            "Accept: application/json, text/plain\nX-Extra: value\n",
        )])
        .unwrap();
        let hook = patch.hook.expect("hook env should produce a hook patch");

        assert_eq!(
            hook.header,
            Some(vec![
                "Accept: application/json, text/plain".to_string(),
                "X-Extra: value".to_string(),
            ])
        );
    }

    #[test]
    fn unknown_tus_env_keys_are_reported_without_failing() {
        let unknown = unknown_tus_env_keys([
            ("TUS_HOOK_HEADERS", "typo"),
            ("TUS_HOOK_URL", "https://example.com"),
            ("TUS_STORAGE_ANYTHING", "backend-specific"),
            ("TUS_MAX_SIZE", "1"),
            ("TUS_TYPO", "1"),
            ("PATH", "/usr/bin"),
        ]);

        assert_eq!(unknown, vec!["TUS_HOOK_HEADERS", "TUS_TYPO"]);
    }

    #[test]
    fn cleanup_force_parses_from_cli_env_and_defaults_off() {
        let cli = CleanupCli::parse_from(["cleanup", "--force"]);
        assert_eq!(cli.force, Some(true));

        let patch = cleanup_settings_patch_from_env_vars([("TUS_CLEANUP_FORCE", "true")]).unwrap();
        assert_eq!(patch.force, Some(true));

        let cli = CleanupCli::parse_from(["cleanup"]);
        let (settings, _) = load_cleanup_settings(&cli).unwrap();
        assert!(!settings.force);
    }

    #[test]
    fn parses_json_log_format_flag() {
        let cli = parse_serve(["tus-server", "serve", "--log-format", "json"]);
        assert_eq!(cli.log_format, Some(LogFormat::Json));
    }

    #[test]
    fn log_format_defaults_to_text() {
        let cli = parse_serve(["tus-server", "serve"]);
        let (settings, _) = load_serve_settings(&cli).unwrap();
        assert_eq!(settings.log_format, LogFormat::Text);
    }

    #[test]
    fn parses_duration_values_from_seconds_and_units() {
        fn seconds(value: &str) -> u64 {
            value.parse::<DurationValue>().unwrap().as_secs()
        }

        assert_eq!(seconds("60"), 60);
        assert_eq!(seconds("5s"), 5);
        assert_eq!(seconds("10m"), 600);
        assert_eq!(seconds("1h"), 3600);
    }

    #[test]
    fn settings_patch_preserves_subsecond_durations() {
        let cli = parse_serve(["tus-server", "serve", "--expiration", "500ms"]);

        let (settings, _) = load_serve_settings(&cli).unwrap();

        assert_eq!(
            settings.expiration.as_duration(),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn tus_config_enables_subsecond_expiration() {
        let settings = Settings {
            expiration: "500ms".parse().unwrap(),
            ..Settings::default()
        };

        let config = build_tus_config(&settings.protocol());

        assert_eq!(config.expiration(), Some(Duration::from_millis(500)));
    }

    #[test]
    fn rejects_negative_duration_values() {
        let err = "-1".parse::<DurationValue>().unwrap_err();

        assert!(err.contains("invalid duration"), "unexpected error: {err}");
    }

    #[test]
    fn config_file_reads_grouped_storage_and_hook_values() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.toml");
        std::fs::write(
            &path,
            r#"
addr = "127.0.0.1:9000"
auth_token = ["alpha", "beta"]
expiration = "10m"

[storage]
uri = "fs://"
root = "uploads"

[hook]
url = "https://example.com/hooks"
timeout = 15
header = ["Authorization: Bearer hook-token"]
retry = true
max_retries = 4
signing_secret = "secret"
"#,
        )
        .unwrap();

        let cli = parse_serve(["tus-server", "serve", "--config", path.to_str().unwrap()]);
        let (settings, config_path) = load_serve_settings(&cli).unwrap();

        assert_eq!(config_path.as_deref(), Some(path.as_path()));
        assert_eq!(
            settings.addr,
            BindTarget::Tcp("127.0.0.1:9000".parse().unwrap())
        );
        assert_eq!(settings.auth_token, vec!["alpha", "beta"]);
        assert_eq!(settings.expiration.as_secs(), 600);
        assert_eq!(settings.storage.uri, "fs://");
        assert_eq!(
            settings.storage.settings.get("root"),
            Some(&"uploads".to_string())
        );
        assert_eq!(
            settings.hook.url.as_deref(),
            Some("https://example.com/hooks")
        );
        assert_eq!(settings.hook.timeout, 15);
        assert_eq!(
            settings.hook.header,
            vec!["Authorization: Bearer hook-token"]
        );
        assert!(settings.hook.retry);
        assert_eq!(settings.hook.max_retries, 4);
        assert_eq!(settings.hook.signing_secret.as_deref(), Some("secret"));
    }

    #[test]
    fn cleanup_settings_ignore_invalid_serve_only_config_keys() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.toml");
        std::fs::write(
            &path,
            r#"
addr = "not-an-addr"
base_path = 123

[storage]
uri = "fs://"
root = "uploads"
"#,
        )
        .unwrap();

        let cli = CleanupCli::parse_from(["cleanup", "--config", path.to_str().unwrap()]);
        let (settings, config_path) = load_cleanup_settings(&cli).unwrap();

        assert_eq!(config_path.as_deref(), Some(path.as_path()));
        assert_eq!(settings.storage.uri, "fs://");
        assert_eq!(
            settings.storage.settings.get("root"),
            Some(&"uploads".to_string())
        );
    }

    #[test]
    fn env_patch_maps_storage_prefix_without_double_underscore() {
        let patch = settings_patch_from_env_vars([
            ("TUS_STORAGE_URI", "fs://"),
            ("TUS_STORAGE_ROOT", "uploads"),
            ("TUS_STORAGE_ACCESS_KEY_ID", "abc123"),
        ])
        .unwrap();
        let storage = patch
            .storage
            .expect("storage env should produce a storage patch");

        assert_eq!(storage.uri.as_deref(), Some("fs://"));
        assert_eq!(storage.settings.get("root"), Some(&"uploads".to_string()));
        assert_eq!(
            storage.settings.get("access_key_id"),
            Some(&"abc123".to_string())
        );
    }

    #[test]
    fn cleanup_env_patch_ignores_serve_only_keys() {
        let patch = cleanup_settings_patch_from_env_vars([
            ("TUS_ADDR", "not-an-addr"),
            ("TUS_BASE_PATH", "123"),
            ("TUS_DISABLE_EXPIRATION_RECLAMATION", "not-a-bool"),
            ("TUS_STORAGE_URI", "fs://"),
            ("TUS_STORAGE_ROOT", "uploads"),
            ("TUS_STATE_DIR", "./state"),
            ("TUS_LOG_FORMAT", "json"),
        ])
        .unwrap();

        let storage = patch
            .storage
            .expect("storage env should produce a storage patch");
        assert_eq!(storage.uri.as_deref(), Some("fs://"));
        assert_eq!(storage.settings.get("root"), Some(&"uploads".to_string()));
        assert_eq!(patch.state_dir, Some(PathBuf::from("./state")));
        assert_eq!(patch.log_format, Some(LogFormat::Json));
    }

    #[test]
    fn cli_overrides_config_file_without_cli_defaults_winning() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.toml");
        std::fs::write(
            &path,
            r#"
addr = "127.0.0.1:9000"
base_path = "/from-file"
auth_token = ["file-token"]

[storage]
uri = "fs://"
root = "from-file"
"#,
        )
        .unwrap();

        let cli = parse_serve([
            "tus-server",
            "serve",
            "--config",
            path.to_str().unwrap(),
            "--base-path",
            "/from-cli",
            "--auth-token",
            "cli-token",
        ]);
        let (settings, _) = load_serve_settings(&cli).unwrap();

        assert_eq!(
            settings.addr,
            BindTarget::Tcp("127.0.0.1:9000".parse().unwrap())
        );
        assert_eq!(settings.base_path, "/from-cli");
        assert_eq!(settings.auth_token, vec!["cli-token"]);
        assert_eq!(
            settings.storage.settings.get("root"),
            Some(&"from-file".to_string())
        );
    }

    #[test]
    fn storage_uri_defaults_to_filesystem() {
        let settings = Settings::default();

        assert_eq!(settings.storage.uri, DEFAULT_STORAGE_URI);
    }

    #[test]
    fn cli_storage_uri_overrides_default_settings() {
        let cli = parse_serve(["tus-server", "serve", "--storage-uri", "s3://"]);
        let (settings, _) = load_serve_settings(&cli).unwrap();

        assert_eq!(settings.storage.uri, "s3://");
    }

    #[test]
    fn default_filesystem_storage_gets_uploads_root() {
        let config = StorageConfig::default();

        let settings = resolved_storage_options(&config).unwrap();

        assert!(settings.contains(&("root".to_string(), DEFAULT_FS_ROOT.to_string())));
    }

    #[tokio::test]
    async fn builds_default_filesystem_storage_operator() {
        let root = tempfile::tempdir().unwrap();
        let mut config = StorageConfig::default();
        config
            .settings
            .insert("root".to_string(), root.path().display().to_string());

        let (operator, scheme) = build_storage_operator(&config).unwrap();

        assert_eq!(scheme, "fs");
        operator.write("probe", "ok").await.unwrap();
        assert_eq!(
            tokio::fs::read_to_string(root.path().join("probe"))
                .await
                .unwrap(),
            "ok"
        );
    }

    #[test]
    fn explicit_filesystem_root_is_preserved() {
        let mut config = StorageConfig::default();
        config
            .settings
            .insert("root".to_string(), "/tmp/tus-uploads".to_string());

        let settings = resolved_storage_options(&config).unwrap();

        assert_eq!(
            settings,
            vec![("root".to_string(), "/tmp/tus-uploads".to_string())]
        );
    }

    #[test]
    fn filesystem_uri_query_root_prevents_default_root() {
        let config = StorageConfig {
            uri: "fs://?root=/tmp/tus-uploads".to_string(),
            settings: BTreeMap::new(),
        };

        let settings = resolved_storage_options(&config).unwrap();

        assert!(!settings.contains(&("root".to_string(), DEFAULT_FS_ROOT.to_string())));
    }

    #[test]
    fn parses_disable_download_flag() {
        let cli = parse_serve(["tus-server", "serve", "--disable-download"]);
        assert_eq!(cli.disable_download, Some(true));
    }

    #[test]
    fn parses_disable_concatenation_unfinished_flag() {
        let cli = parse_serve(["tus-server", "serve", "--disable-concatenation-unfinished"]);
        assert_eq!(cli.disable_concatenation_unfinished, Some(true));
    }

    #[test]
    fn parses_disable_checksum_trailer_flag() {
        let cli = parse_serve(["tus-server", "serve", "--disable-checksum-trailer"]);
        assert_eq!(cli.disable_checksum_trailer, Some(true));
    }

    #[test]
    fn all_extensions_can_exclude_concatenation_unfinished() {
        let settings = Settings {
            all_extensions: true,
            disable_concatenation_unfinished: true,
            ..Settings::default()
        };
        let config = build_tus_config(&settings.protocol());

        assert!(config.has_extension(Extension::Concatenation));
        assert!(!config.has_extension(Extension::ConcatenationUnfinished));
    }

    #[test]
    fn all_extensions_can_exclude_checksum_trailer() {
        let settings = Settings {
            all_extensions: true,
            disable_checksum_trailer: true,
            ..Settings::default()
        };
        let config = build_tus_config(&settings.protocol());

        assert!(config.has_extension(Extension::Checksum));
        assert!(!config.has_extension(Extension::ChecksumTrailer));
    }

    #[test]
    fn cli_parses_tcp_addr() {
        let cli = parse_serve(["tus-server", "serve", "--addr", "127.0.0.1:9000"]);
        assert_eq!(
            cli.addr.unwrap(),
            BindTarget::Tcp("127.0.0.1:9000".parse().unwrap())
        );
    }

    #[cfg(unix)]
    #[test]
    fn cli_parses_unix_addr() {
        let cli = parse_serve(["tus-server", "serve", "--addr", "unix:/tmp/tus.sock"]);
        assert_eq!(
            cli.addr.unwrap(),
            BindTarget::Unix(PathBuf::from("/tmp/tus.sock"))
        );
    }

    #[test]
    fn cli_parses_hook_signing_secret() {
        let cli = parse_serve([
            "tus-server",
            "serve",
            "--hook-url",
            "https://example.com/hooks",
            "--hook-signing-secret",
            "super-secret",
        ]);
        assert_eq!(cli.hook_signing_secret.as_deref(), Some("super-secret"));
    }

    #[test]
    fn rejects_removed_rate_limit_flags() {
        for flag in [
            "--rate-limit-requests",
            "--rate-limit-bytes",
            "--rate-limit-window",
            "--rate-limit-header",
            "--rate-limit-redis-url",
        ] {
            let err = Cli::try_parse_from(["tus-server", "serve", flag, "1"]).unwrap_err();

            assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
        }
    }

    #[test]
    fn rejects_removed_metrics_and_telemetry_flags() {
        for flag in [
            "--metrics",
            "--metrics-bearer",
            "--otlp-endpoint",
            "--otlp-service-name",
        ] {
            let err = if flag == "--metrics" {
                Cli::try_parse_from(["tus-server", "serve", flag]).unwrap_err()
            } else {
                Cli::try_parse_from(["tus-server", "serve", flag, "value"]).unwrap_err()
            };

            assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
        }
    }

    #[test]
    fn settings_ignores_removed_rate_limit_keys_from_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
rate_limit_requests = 1
cors = true
"#,
        )
        .unwrap();

        let settings = load_settings_from_sources(Some(&path), SettingsPatch::default()).unwrap();

        assert!(settings.cors);
    }

    #[test]
    fn settings_reads_cors_and_auth_values_from_file() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "tus-server-settings-{}-{}.toml",
            std::process::id(),
            unique
        ));
        std::fs::write(
            &path,
            r#"
cors = true
cors_origins = ["https://app.example.com"]
auth_token = ["alpha", "beta"]
"#,
        )
        .unwrap();

        let settings = load_settings_from_sources(Some(&path), SettingsPatch::default()).unwrap();

        assert!(settings.cors);
        assert_eq!(settings.cors_origins, vec!["https://app.example.com"]);
        assert_eq!(settings.auth_token, vec!["alpha", "beta"]);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn settings_load_rejects_invalid_startup_fields() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
addr = "not-an-addr"
cors = true
cors_origins = ["https://app.example.com"]
auth_token = ["token"]
"#,
        )
        .unwrap();

        let err = load_settings_from_sources(Some(&path), SettingsPatch::default()).unwrap_err();

        assert!(
            err.to_string().contains("failed to load configuration"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn serve_command_parses_existing_flags() {
        let cli = Cli::parse_from([
            "tus-server",
            "serve",
            "--addr",
            "127.0.0.1:9000",
            "--base-path",
            "/uploads",
            "--disable-expiration-reclamation",
        ]);

        let Command::Serve(serve) = cli.command else {
            panic!("expected serve command");
        };
        assert_eq!(
            serve.addr,
            Some(BindTarget::Tcp("127.0.0.1:9000".parse().unwrap()))
        );
        assert_eq!(serve.base_path.as_deref(), Some("/uploads"));
        assert_eq!(serve.disable_expiration_reclamation, Some(true));
    }

    #[test]
    fn expiration_reclamation_defaults_on_and_can_be_disabled() {
        // The sweeper follows expiration by default: no opt-in flag is
        // required, and the setting resolves to "enabled".
        let cli = parse_serve(["tus-server", "serve"]);
        let (settings, _) = load_serve_settings(&cli).unwrap();
        assert!(!settings.disable_expiration_reclamation);

        // Operators can still opt out via flag, env, or config file.
        let cli = parse_serve(["tus-server", "serve", "--disable-expiration-reclamation"]);
        let (settings, _) = load_serve_settings(&cli).unwrap();
        assert!(settings.disable_expiration_reclamation);

        let patch =
            settings_patch_from_env_vars([("TUS_DISABLE_EXPIRATION_RECLAMATION", "true")]).unwrap();
        assert_eq!(patch.disable_expiration_reclamation, Some(true));

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.toml");
        std::fs::write(&path, "disable_expiration_reclamation = true\n").unwrap();
        let settings = load_settings_from_sources(Some(&path), SettingsPatch::default()).unwrap();
        assert!(settings.disable_expiration_reclamation);
    }

    #[test]
    fn cleanup_command_parses_shared_flags() {
        let cli = Cli::parse_from([
            "tus-server",
            "cleanup",
            "--storage-uri",
            "fs://",
            "--state-dir",
            "./state",
        ]);

        let Command::Cleanup(cleanup) = cli.command else {
            panic!("expected cleanup command");
        };
        assert_eq!(cleanup.storage_uri.as_deref(), Some("fs://"));
        assert_eq!(cleanup.state_dir, Some(PathBuf::from("./state")));
    }

    #[test]
    fn root_level_serve_flags_are_rejected() {
        let err = Cli::try_parse_from(["tus-server", "--addr", "127.0.0.1:9000"]).unwrap_err();

        assert!(
            err.to_string().contains("serve") || err.to_string().contains("subcommand"),
            "unexpected parse error: {err}"
        );
    }

    // ---- Source precedence: defaults < config file < env < CLI ----

    #[test]
    fn cli_patch_overrides_environment_and_config_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.toml");
        std::fs::write(&path, "base_path = \"/from-file\"\n").unwrap();

        let env_patch = SettingsPatch {
            base_path: Some("/from-env".to_string()),
            ..SettingsPatch::default()
        };
        let cli_patch = SettingsPatch {
            base_path: Some("/from-cli".to_string()),
            ..SettingsPatch::default()
        };

        let settings = load_settings_layered(Some(&path), env_patch, cli_patch).unwrap();

        assert_eq!(settings.base_path, "/from-cli");
    }

    #[test]
    fn environment_patch_overrides_config_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.toml");
        std::fs::write(&path, "base_path = \"/from-file\"\nmax_size = 10\n").unwrap();

        let env_patch = SettingsPatch {
            base_path: Some("/from-env".to_string()),
            ..SettingsPatch::default()
        };

        let settings =
            load_settings_layered(Some(&path), env_patch, SettingsPatch::default()).unwrap();

        // base_path comes from env (overrides the file); max_size, which
        // env did not set, still comes from the file.
        assert_eq!(settings.base_path, "/from-env");
        assert_eq!(settings.max_size, 10);
    }

    #[test]
    fn config_file_overrides_defaults_when_no_env_or_cli() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.toml");
        std::fs::write(&path, "base_path = \"/from-file\"\n").unwrap();

        let settings = load_settings_layered(
            Some(&path),
            SettingsPatch::default(),
            SettingsPatch::default(),
        )
        .unwrap();

        assert_eq!(settings.base_path, "/from-file");
        // Untouched fields fall through to the built-in defaults.
        assert_eq!(settings.shutdown_grace, Settings::default().shutdown_grace);
    }

    // ---- Focused config groups ----

    #[test]
    fn settings_groups_extract_focused_config() {
        let settings = Settings {
            addr: BindTarget::Tcp("127.0.0.1:9000".parse().unwrap()),
            state_dir: PathBuf::from("/srv/state"),
            shutdown_grace: 12,
            drain_delay: 3,
            request_header_read_timeout: 7,
            auth_token: vec!["token".to_string()],
            max_request_body_bytes: 4096,
            request_body_read_timeout: 5,
            expiration: "1h".parse().unwrap(),
            expiration_scan_interval: "30s".parse().unwrap(),
            disable_expiration_reclamation: false,
            ..Settings::default()
        };

        let backend = settings.backend();
        assert_eq!(backend.state_dir, PathBuf::from("/srv/state"));
        assert_eq!(backend.storage.uri, DEFAULT_STORAGE_URI);

        let runtime = settings.runtime();
        assert_eq!(runtime.shutdown_grace, Duration::from_secs(12));
        assert_eq!(runtime.drain_delay, Duration::from_secs(3));
        assert_eq!(runtime.header_read_timeout, Duration::from_secs(7));

        let reclamation = settings.reclamation();
        assert!(reclamation.is_enabled());
        assert_eq!(reclamation.scan_interval, Duration::from_secs(30));

        let app = settings.app();
        assert_eq!(app.auth_token, vec!["token"]);
        assert_eq!(app.max_request_body_bytes, 4096);
        assert_eq!(app.request_body_read_timeout, 5);
    }

    #[test]
    fn reclamation_group_reflects_expiration_and_opt_out() {
        // No expiration configured: nothing to reclaim.
        let settings = Settings::default();
        assert!(!settings.reclamation().expiration_configured());
        assert!(!settings.reclamation().is_enabled());

        // Expiration configured but reclamation disabled: configured, not enabled.
        let settings = Settings {
            expiration: "1h".parse().unwrap(),
            disable_expiration_reclamation: true,
            ..Settings::default()
        };
        let reclamation = settings.reclamation();
        assert!(reclamation.expiration_configured());
        assert!(!reclamation.is_enabled());
    }

    #[test]
    fn app_group_resolves_cors_origins() {
        // Explicit origins win over the wildcard toggle.
        let settings = Settings {
            cors: true,
            cors_origins: vec!["https://app.example.com".to_string()],
            ..Settings::default()
        };
        assert_eq!(settings.app().cors_origins, vec!["https://app.example.com"]);

        // Bare `cors = true` allows any origin.
        let settings = Settings {
            cors: true,
            ..Settings::default()
        };
        assert_eq!(settings.app().cors_origins, vec!["*".to_string()]);

        // CORS off by default.
        assert!(Settings::default().app().cors_origins.is_empty());
    }

    #[test]
    fn protocol_group_converts_into_tus_config() {
        let settings = Settings {
            base_path: "/uploads".to_string(),
            max_size: 2048,
            max_chunk_size: 1024,
            expiration: "10m".parse().unwrap(),
            all_extensions: true,
            disable_checksum_trailer: true,
            ..Settings::default()
        };

        let protocol = settings.protocol();
        assert_eq!(protocol.expiration, Duration::from_secs(600));

        let config = build_tus_config(&protocol);
        assert_eq!(config.base_path(), "/uploads");
        assert_eq!(config.max_size(), Some(2048));
        assert_eq!(config.max_chunk_size(), Some(1024));
        assert_eq!(config.expiration(), Some(Duration::from_secs(600)));
        assert!(config.has_extension(Extension::Checksum));
        assert!(!config.has_extension(Extension::ChecksumTrailer));
    }

    // ---- Cleanup shares the storage/logging path ----

    #[test]
    fn cleanup_settings_backend_extracts_storage_group() {
        let settings = CleanupSettings {
            storage: StorageConfig {
                uri: "s3://bucket".to_string(),
                settings: BTreeMap::from([("region".to_string(), "us-east-1".to_string())]),
            },
            state_dir: PathBuf::from("/srv/state"),
            ..CleanupSettings::default()
        };

        let backend = settings.backend();

        assert_eq!(backend.storage.uri, "s3://bucket");
        assert_eq!(
            backend.storage.settings.get("region"),
            Some(&"us-east-1".to_string())
        );
        assert_eq!(backend.state_dir, PathBuf::from("/srv/state"));
    }

    #[test]
    fn cleanup_cli_patch_overrides_environment_and_config_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("server.toml");
        std::fs::write(&path, "state_dir = \"/from-file\"\nlog_format = \"text\"\n").unwrap();

        let env_patch = CleanupSettingsPatch {
            state_dir: Some(PathBuf::from("/from-env")),
            log_format: Some(LogFormat::Json),
            ..CleanupSettingsPatch::default()
        };
        let cli_patch = CleanupSettingsPatch {
            state_dir: Some(PathBuf::from("/from-cli")),
            ..CleanupSettingsPatch::default()
        };

        let settings = load_cleanup_settings_layered(Some(&path), env_patch, cli_patch).unwrap();

        // CLI wins on state_dir; log_format falls back to the env layer
        // since the CLI did not set it.
        assert_eq!(settings.state_dir, PathBuf::from("/from-cli"));
        assert_eq!(settings.log_format, LogFormat::Json);
    }
}
