//! HTTP webhook hook executor for `tus-protocol`.
//!
//! This crate provides an HTTP-based hook executor that sends webhook requests
//! to a configured endpoint. This enables external services to be notified of
//! TUS upload events and optionally control upload behavior.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::time::Duration;
//! use tus_hook_http::{HttpHookConfig, HttpHookExecutor};
//!
//! let config = HttpHookConfig::new("https://example.com/tus-webhook")
//!     .with_timeout(Duration::from_secs(10))
//!     .with_header("Authorization", "Bearer secret-token");
//!
//! let executor = HttpHookExecutor::new(config).expect("failed to build HTTP client");
//! ```
//!
//! # TLS
//!
//! The crate's default features enable the bundled [`reqwest`] client's default
//! TLS support. Disabling default features (`default-features = false`) drops
//! TLS from the bundled `reqwest`, so `https://` webhook URLs will fail unless
//! you supply your own TLS-capable client via [`HttpHookExecutor::with_client`].
//!
//! # Webhook Protocol
//!
//! The executor sends POST requests with the following characteristics:
//!
//! - **Content-Type**: `application/json`
//! - **Body**: JSON-serialized [`HookContext`]
//! - **Header**: `X-Tus-Hook-Event` with the event name
//! - **Optional Header**: `X-Tus-Signature-256` with `sha256=<hex-hmac>`
//!
//! ## Pre-hook Response
//!
//! For pre-hooks, the webhook endpoint should return a JSON response:
//!
//! ```json
//! {
//!     "proceed": true,
//!     "metadata": {"filename": "example.bin"},
//!     "reject_status": 403,
//!     "reject_message": "Upload rejected"
//! }
//! ```
//!
//! - `proceed`: Whether to allow the operation (default: true)
//! - `metadata`: Replacement user metadata for hook events that allow metadata changes
//! - `reject_status`: HTTP status code for rejection (default: 403)
//! - `reject_message`: Message to return to the client
//!
//! ## Post-hook Response
//!
//! Post-hooks are fire-and-forget. Any response is logged but does not affect
//! the upload operation.

#![warn(missing_docs)]

/// Re-export of the `reqwest` crate used by this executor.
///
/// Types from `reqwest` (such as [`reqwest::Client`] and [`reqwest::Error`])
/// appear in this crate's public API — for example in
/// [`HttpHookExecutor::with_client`] and [`HttpHookExecutor::new`] — so this
/// re-export lets downstream crates name the exact `reqwest` version without
/// adding their own, possibly mismatched, dependency.
pub use reqwest;

use async_trait::async_trait;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::Duration;
use tus_protocol::{Error, HookContext, HookExecutor, PreHookResult, Result, UploadMetadata};

/// Default timeout for webhook requests.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const SIGNATURE_HEADER: &str = "X-Tus-Signature-256";

/// Configuration for the HTTP webhook executor.
///
/// Construct via [`HttpHookConfig::new`] and the `with_*` builder methods.
/// The type is `#[non_exhaustive]` so new webhook knobs can be added without a
/// major version bump.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HttpHookConfig {
    /// The webhook endpoint URL.
    pub url: String,

    /// Request timeout.
    pub timeout: Duration,

    /// Additional headers to include in webhook requests.
    pub headers: HashMap<String, String>,

    /// Whether to retry failed requests.
    pub retry_enabled: bool,

    /// Maximum number of retry attempts.
    pub max_retries: u32,

    /// Shared secret used to sign webhook bodies with HMAC-SHA256.
    pub signing_secret: Option<String>,
}

impl HttpHookConfig {
    /// Creates a new configuration with the given webhook URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            timeout: DEFAULT_TIMEOUT,
            headers: HashMap::new(),
            retry_enabled: false,
            max_retries: 3,
            signing_secret: None,
        }
    }

    /// Sets the request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Adds a custom header to webhook requests.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Enables retry on failure.
    pub fn with_retry(mut self, enabled: bool) -> Self {
        self.retry_enabled = enabled;
        self
    }

    /// Sets the maximum number of retry attempts.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Enables HMAC-SHA256 signing of webhook bodies.
    pub fn with_signing_secret(mut self, secret: impl Into<String>) -> Self {
        self.signing_secret = Some(secret.into());
        self
    }
}

/// Response from a pre-hook webhook.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PreHookResponse {
    /// Whether to proceed with the operation.
    #[serde(default = "default_proceed")]
    pub proceed: bool,

    /// Replacement user metadata, if any.
    #[serde(default)]
    pub metadata: Option<UploadMetadata>,

    /// HTTP status code for rejection.
    #[serde(default)]
    pub reject_status: Option<u16>,

    /// Rejection message for the client.
    #[serde(default)]
    pub reject_message: Option<String>,

    /// Additional response headers to include.
    #[serde(default)]
    pub response_headers: Option<HashMap<String, String>>,
}

impl Default for PreHookResponse {
    fn default() -> Self {
        Self {
            proceed: default_proceed(),
            metadata: None,
            reject_status: None,
            reject_message: None,
            response_headers: None,
        }
    }
}

impl From<PreHookResponse> for PreHookResult {
    fn from(response: PreHookResponse) -> Self {
        let mut result = PreHookResult::default();
        result.proceed = response.proceed;
        result.metadata = response.metadata;
        result.reject_status = response.reject_status;
        result.reject_message = response.reject_message;
        result.response_headers = response.response_headers.unwrap_or_default();
        result
    }
}

/// HTTP webhook executor that implements the [`HookExecutor`] trait.
///
/// This executor sends webhook requests to a configured endpoint for each hook
/// event. Pre-hooks can reject operations, add response headers, or replace
/// user metadata based on the webhook response.
pub struct HttpHookExecutor {
    client: Client,
    config: HttpHookConfig,
}

impl HttpHookExecutor {
    /// Creates a new HTTP hook executor with the given configuration.
    ///
    /// Builds a [`reqwest::Client`] with the configured timeout. Returns an
    /// error if the client cannot be constructed (for example when the TLS
    /// backend fails to initialize).
    pub fn new(config: HttpHookConfig) -> std::result::Result<Self, reqwest::Error> {
        let client = Client::builder().timeout(config.timeout).build()?;

        Ok(Self { client, config })
    }

    /// Creates a new HTTP hook executor with a custom reqwest client.
    pub fn with_client(client: Client, config: HttpHookConfig) -> Self {
        Self { client, config }
    }

    fn payload(ctx: &HookContext) -> Result<Vec<u8>> {
        serde_json::to_vec(ctx).map_err(Error::hook)
    }

    fn signature_header_value(secret: &str, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .expect("HMAC accepts keys of any size");
        mac.update(body);
        let digest = mac.finalize().into_bytes();

        let mut hex = String::with_capacity(digest.len() * 2);
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
        }

        format!("sha256={hex}")
    }

    async fn send_webhook(
        &self,
        event: &str,
        payload: &[u8],
    ) -> std::result::Result<reqwest::Response, reqwest::Error> {
        let mut request = self
            .client
            .post(&self.config.url)
            .header("Content-Type", "application/json")
            .header("X-Tus-Hook-Event", event)
            .body(payload.to_vec());

        if let Some(secret) = self.config.signing_secret.as_deref() {
            request = request.header(
                SIGNATURE_HEADER,
                Self::signature_header_value(secret, payload),
            );
        }

        for (name, value) in &self.config.headers {
            request = request.header(name, value);
        }

        request.send().await
    }

    /// Returns whether a webhook response status warrants a retry.
    ///
    /// Retries `429 Too Many Requests` and all 5xx server errors; other
    /// statuses (including non-retryable client errors like 403) are returned
    /// to the caller immediately.
    fn is_retryable_status(status: StatusCode) -> bool {
        status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
    }

    async fn send_webhook_with_retry(&self, ctx: &HookContext) -> Result<reqwest::Response> {
        let payload = Self::payload(ctx)?;
        let mut last_error = None;
        let max_attempts = if self.config.retry_enabled {
            self.config.max_retries + 1
        } else {
            1
        };

        for attempt in 0..max_attempts {
            let is_last_attempt = attempt + 1 >= max_attempts;

            match self.send_webhook(ctx.event.as_str(), &payload).await {
                Ok(response) => {
                    let status = response.status();
                    if is_last_attempt || !Self::is_retryable_status(status) {
                        return Ok(response);
                    }

                    tracing::warn!(
                        attempt = attempt + 1,
                        max_attempts = max_attempts,
                        status = %status,
                        "webhook returned retryable status"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_attempts = max_attempts,
                        error = %e,
                        "webhook request failed"
                    );
                    last_error = Some(e);

                    if is_last_attempt {
                        break;
                    }
                }
            }

            let delay = Duration::from_millis(100 * (1 << attempt));
            tokio::time::sleep(delay).await;
        }

        Err(Error::Hook(Box::new(
            last_error.expect("should have at least one error"),
        )))
    }
}

impl std::fmt::Debug for HttpHookExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpHookExecutor")
            .field("config", &self.config)
            .finish()
    }
}

#[async_trait]
impl HookExecutor for HttpHookExecutor {
    async fn execute_pre(&self, ctx: &HookContext) -> Result<PreHookResult> {
        tracing::debug!(
            event = ctx.event.as_str(),
            upload_id = %ctx.upload.id(),
            url = %self.config.url,
            "executing pre-hook webhook"
        );

        let response = self.send_webhook_with_retry(ctx).await?;
        let status = response.status();

        if !status.is_success() {
            tracing::warn!(
                event = ctx.event.as_str(),
                upload_id = %ctx.upload.id(),
                status = %status,
                "pre-hook webhook returned non-success status"
            );

            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "Webhook error".to_string());

            return Err(Error::hook(HttpHookStatusError { status, body }));
        }

        let hook_response: PreHookResponse = response.json().await.map_err(|error| {
            tracing::warn!(
                event = ctx.event.as_str(),
                upload_id = %ctx.upload.id(),
                error = %error,
                "failed to parse pre-hook response"
            );
            Error::hook(error)
        })?;

        tracing::debug!(
            event = ctx.event.as_str(),
            upload_id = %ctx.upload.id(),
            proceed = hook_response.proceed,
            "pre-hook webhook completed"
        );

        Ok(hook_response.into())
    }

    async fn execute_post(&self, ctx: &HookContext) -> Result<()> {
        tracing::debug!(
            event = ctx.event.as_str(),
            upload_id = %ctx.upload.id(),
            url = %self.config.url,
            "executing post-hook webhook"
        );

        match self.send_webhook_with_retry(ctx).await {
            Ok(response) => {
                let status = response.status();
                if !status.is_success() {
                    tracing::warn!(
                        event = ctx.event.as_str(),
                        upload_id = %ctx.upload.id(),
                        status = %status,
                        "post-hook webhook returned non-success status"
                    );
                } else {
                    tracing::debug!(
                        event = ctx.event.as_str(),
                        upload_id = %ctx.upload.id(),
                        "post-hook webhook completed successfully"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    event = ctx.event.as_str(),
                    upload_id = %ctx.upload.id(),
                    error = %e,
                    "post-hook webhook failed"
                );
            }
        }

        Ok(())
    }
}

fn default_proceed() -> bool {
    true
}

#[derive(Debug)]
struct HttpHookStatusError {
    status: StatusCode,
    body: String,
}

impl std::fmt::Display for HttpHookStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unexpected response code from hook endpoint ({}): {}",
            self.status, self.body
        )
    }
}

impl std::error::Error for HttpHookStatusError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;
    use tus_protocol::hooks::HookRequestInfo;
    use tus_protocol::{HookEvent, HookExecutor, HookUpload};

    #[test]
    fn config_builder_sets_webhook_options() {
        let config = HttpHookConfig::new("https://example.com/webhook")
            .with_timeout(Duration::from_secs(5))
            .with_header("Authorization", "Bearer token")
            .with_retry(true)
            .with_max_retries(5)
            .with_signing_secret("secret");

        assert_eq!(config.url, "https://example.com/webhook");
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert_eq!(
            config.headers.get("Authorization"),
            Some(&"Bearer token".to_string())
        );
        assert!(config.retry_enabled);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.signing_secret.as_deref(), Some("secret"));
    }

    #[test]
    fn signature_header_value_uses_sha256_hmac() {
        let signature = HttpHookExecutor::signature_header_value("secret", br#"{"ok":true}"#);

        assert_eq!(
            signature,
            "sha256=f6b4a2841c93f8bf2fb8f2c13d8fb0b6c8e8019f09ee405d248daa8385fad638"
        );
    }

    #[test]
    fn pre_hook_response_defaults_to_proceeding() {
        let response: PreHookResponse = serde_json::from_str("{}").unwrap();

        assert!(response.proceed);
        assert!(response.metadata.is_none());
        assert!(response.reject_status.is_none());
        assert!(response.reject_message.is_none());
    }

    #[test]
    fn pre_hook_response_default_matches_deserialization_default() {
        let response = PreHookResponse::default();

        assert!(response.proceed);
        assert!(response.metadata.is_none());
        assert!(response.reject_status.is_none());
        assert!(response.reject_message.is_none());
    }

    #[test]
    fn pre_hook_response_converts_to_pre_hook_result() {
        let response = PreHookResponse {
            proceed: false,
            metadata: None,
            reject_status: Some(403),
            reject_message: Some("Forbidden".to_string()),
            response_headers: Some({
                let mut headers = HashMap::new();
                headers.insert("X-Custom".to_string(), "value".to_string());
                headers
            }),
        };

        let result: PreHookResult = response.into();

        assert!(!result.proceed);
        assert_eq!(result.reject_status, Some(403));
        assert_eq!(result.reject_message, Some("Forbidden".to_string()));
        assert_eq!(
            result.response_headers.get("X-Custom"),
            Some(&"value".to_string())
        );
    }

    #[test]
    fn pre_hook_response_maps_metadata_replacement() {
        let response: PreHookResponse =
            serde_json::from_str(r#"{"metadata":{"filename":"hook.txt"}}"#).unwrap();

        let result: PreHookResult = response.into();

        assert!(result.proceed);
        assert_eq!(
            result
                .metadata
                .unwrap()
                .get("filename")
                .and_then(|value| value.as_str()),
            Some("hook.txt")
        );
    }

    #[test]
    fn hook_context_serializes_for_webhook_payloads() {
        let ctx = hook_context();

        let json = serde_json::to_string(&ctx).unwrap();

        assert!(json.contains("pre-create"));
        assert!(json.contains("test-upload-id"));
    }

    #[tokio::test]
    async fn execute_pre_posts_context_and_maps_json_response() {
        let secret = "secret";
        let (url, request) = serve_once(
            200,
            r#"{"proceed":false,"reject_status":409,"reject_message":"blocked"}"#,
        )
        .await;
        let executor =
            HttpHookExecutor::new(HttpHookConfig::new(url).with_signing_secret(secret)).unwrap();

        let result = executor.execute_pre(&hook_context()).await.unwrap();
        let request = request.await.unwrap();
        let request_lower = request.to_ascii_lowercase();
        let signature = request_header(&request, "x-tus-signature-256")
            .expect("signed request must include signature header");
        let body = request_body(&request);

        assert!(!result.proceed);
        assert_eq!(result.reject_status, Some(409));
        assert_eq!(result.reject_message.as_deref(), Some("blocked"));
        assert!(request.starts_with("POST /hook HTTP/1.1"));
        assert!(request_lower.contains("content-type: application/json"));
        assert!(request_lower.contains("x-tus-hook-event: pre-create"));
        assert_eq!(
            signature,
            HttpHookExecutor::signature_header_value(secret, body.as_bytes())
        );
        assert!(request.contains(r#""event":"pre-create""#));
        assert!(request.contains("test-upload-id"));
    }

    #[tokio::test]
    async fn execute_pre_treats_non_success_response_as_hook_error() {
        let (url, request) = serve_once(403, "forbidden").await;
        let executor = HttpHookExecutor::new(HttpHookConfig::new(url)).unwrap();

        let result = executor.execute_pre(&hook_context()).await;
        let _request = request.await.unwrap();

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_pre_treats_invalid_success_body_as_hook_error() {
        let (url, request) = serve_once(200, "not-json").await;
        let executor = HttpHookExecutor::new(HttpHookConfig::new(url)).unwrap();

        let result = executor.execute_pre(&hook_context()).await;
        let _request = request.await.unwrap();

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_pre_treats_empty_success_body_as_hook_error() {
        let (url, request) = serve_once(200, "").await;
        let executor = HttpHookExecutor::new(HttpHookConfig::new(url)).unwrap();

        let result = executor.execute_pre(&hook_context()).await;
        let _request = request.await.unwrap();

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_pre_retries_retryable_statuses_until_success() {
        let (url, requests) =
            serve_sequence(vec![(503, ""), (429, ""), (200, r#"{"proceed":true}"#)]).await;
        let executor = HttpHookExecutor::new(
            HttpHookConfig::new(url)
                .with_retry(true)
                .with_max_retries(3),
        )
        .unwrap();

        let result = executor.execute_pre(&hook_context()).await.unwrap();
        let requests = requests.await.unwrap();

        assert!(result.proceed);
        assert_eq!(requests.len(), 3);
    }

    #[tokio::test]
    async fn execute_pre_returns_status_error_when_retries_are_exhausted() {
        let (url, requests) =
            serve_sequence(vec![(503, "unavailable"), (503, "unavailable")]).await;
        let executor = HttpHookExecutor::new(
            HttpHookConfig::new(url)
                .with_retry(true)
                .with_max_retries(1),
        )
        .unwrap();

        let result = executor.execute_pre(&hook_context()).await;
        let requests = requests.await.unwrap();

        assert!(result.is_err());
        assert_eq!(requests.len(), 2);
    }

    #[tokio::test]
    async fn execute_pre_does_not_retry_when_retry_is_disabled() {
        // A second response is queued so that an unexpected retry would
        // succeed and flip the assertion below.
        let (url, requests) =
            serve_sequence(vec![(503, "unavailable"), (200, r#"{"proceed":true}"#)]).await;
        let executor = HttpHookExecutor::new(HttpHookConfig::new(url)).unwrap();

        let result = executor.execute_pre(&hook_context()).await;

        assert!(result.is_err());
        requests.abort();
    }

    #[tokio::test]
    async fn execute_pre_does_not_retry_non_retryable_client_errors() {
        // A second response is queued so that an unexpected retry would
        // succeed and flip the assertion below.
        let (url, requests) =
            serve_sequence(vec![(403, "forbidden"), (200, r#"{"proceed":true}"#)]).await;
        let executor = HttpHookExecutor::new(
            HttpHookConfig::new(url)
                .with_retry(true)
                .with_max_retries(3),
        )
        .unwrap();

        let result = executor.execute_pre(&hook_context()).await;

        assert!(result.is_err());
        requests.abort();
    }

    #[tokio::test]
    async fn execute_post_retries_retryable_statuses() {
        let (url, requests) = serve_sequence(vec![(429, ""), (200, "{}")]).await;
        let executor = HttpHookExecutor::new(
            HttpHookConfig::new(url)
                .with_retry(true)
                .with_max_retries(3),
        )
        .unwrap();

        executor.execute_post(&hook_context()).await.unwrap();
        let requests = requests.await.unwrap();

        assert_eq!(requests.len(), 2);
    }

    #[tokio::test]
    async fn execute_post_ignores_non_success_response() {
        let (url, request) = serve_once(500, "failed").await;
        let executor = HttpHookExecutor::new(HttpHookConfig::new(url)).unwrap();

        executor.execute_post(&hook_context()).await.unwrap();
        let _request = request.await.unwrap();
    }

    fn hook_context() -> HookContext {
        let mut request = HookRequestInfo::default();
        request.method = "POST".to_string();
        request.path = "/uploads".to_string();
        request.remote_addr = Some("127.0.0.1".to_string());

        HookContext::new(
            HookEvent::PreCreate,
            HookUpload::new("test-upload-id"),
            request,
        )
    }

    async fn serve_once(status: u16, body: &'static str) -> (String, JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let response = format!(
            "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );

        let request = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            stream.write_all(response.as_bytes()).await.unwrap();
            request
        });

        (format!("http://{addr}/hook"), request)
    }

    /// Serves one response per incoming connection, in order, and returns the
    /// raw requests once all responses have been served.
    async fn serve_sequence(
        responses: Vec<(u16, &'static str)>,
    ) -> (String, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let requests = tokio::spawn(async move {
            let mut requests = Vec::new();

            for (status, body) in responses {
                let response = format!(
                    "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let (mut stream, _) = listener.accept().await.unwrap();
                requests.push(read_request(&mut stream).await);
                stream.write_all(response.as_bytes()).await.unwrap();
            }

            requests
        });

        (format!("http://{addr}/hook"), requests)
    }

    async fn read_request(stream: &mut TcpStream) -> String {
        let mut data = Vec::new();
        let mut buf = [0; 1024];

        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }

            data.extend_from_slice(&buf[..n]);

            if request_is_complete(&data) {
                break;
            }
        }

        String::from_utf8(data).unwrap()
    }

    fn request_header(request: &str, name: &str) -> Option<String> {
        request.lines().find_map(|line| {
            let (header_name, value) = line.split_once(':')?;
            header_name
                .eq_ignore_ascii_case(name)
                .then(|| value.trim().to_string())
        })
    }

    fn request_body(request: &str) -> &str {
        request
            .split_once("\r\n\r\n")
            .expect("request must contain header terminator")
            .1
    }

    fn request_is_complete(data: &[u8]) -> bool {
        let Some(header_end) = data.windows(4).position(|window| window == b"\r\n\r\n") else {
            return false;
        };
        let headers = String::from_utf8_lossy(&data[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())?
            })
            .unwrap_or(0);

        data.len() >= header_end + 4 + content_length
    }
}
