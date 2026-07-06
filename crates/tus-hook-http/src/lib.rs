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
//! let executor = HttpHookExecutor::new(config).expect("failed to build HTTP hook executor");
//! ```
//!
//! # TLS
//!
//! The crate's default features enable the bundled [`reqwest`] client's rustls
//! TLS stack (via the `reqwest-rustls` feature). Disabling default features
//! (`default-features = false`) drops TLS from the bundled `reqwest`, so
//! `https://` webhook URLs will fail unless you re-enable a backend via the
//! `reqwest-native-tls` or `reqwest-rustls` passthrough features, or supply
//! your own TLS-capable client via [`HttpHookExecutor::with_client`].
//!
//! # Webhook Protocol
//!
//! The executor sends POST requests with the following characteristics:
//!
//! - **Content-Type**: `application/json`
//! - **Body**: JSON-serialized [`HookContext`]
//! - **Header**: `X-Tus-Hook-Event` with the event name
//! - **Header**: `X-Tus-Hook-Delivery` with a UUID identifying the delivery.
//!   All retries of one delivery carry the same UUID, so receivers can
//!   deduplicate redelivered events.
//! - **Optional Header**: `X-Tus-Signature-256` carrying an HMAC-SHA256
//!   signature with replay protection, formatted `t=<unix_secs>,v1=<hex-hmac>`
//!   (Stripe/GitHub style). The signature is computed over the byte string
//!   `<unix_secs>.<body>` — the Unix send timestamp in seconds, a literal `.`,
//!   then the raw request body — keyed by the configured signing secret. To
//!   verify: parse `t` and `v1` from the header, recompute
//!   `hex(HMAC_SHA256(secret, "{t}.{body}"))`, compare it to `v1` in constant
//!   time, and reject deliveries whose `t` is too far from the current time.
//!   The timestamp is captured once per delivery, so it (and therefore the
//!   signature) stay stable across retries of the same delivery. Because `t`
//!   does not advance across retries, keep
//!   [`HttpHookConfig::with_retry_deadline`] below the freshness tolerance your
//!   receiver enforces (Stripe-style verifiers typically allow ±5 minutes), or
//!   a late retry of a long-running delivery can be rejected as stale.
//!
//! ## Retries
//!
//! Retries are opt-in via [`HttpHookConfig::with_retry`]. Retryable outcomes
//! (request errors, `429 Too Many Requests`, and 5xx responses) are retried
//! with capped exponential backoff plus random jitter. A `Retry-After` header
//! (seconds form) on `429`/`503` responses overrides the computed backoff.
//! Retrying stops after [`HttpHookConfig::with_max_retries`] additional
//! attempts, or as soon as the next attempt would exceed the deadline set by
//! [`HttpHookConfig::with_retry_deadline`], whichever comes first.
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
//! The response body is limited to 64 KiB; larger responses fail the hook.
//! Bodies of non-2xx responses are captured for diagnostics up to 8 KiB and
//! truncated beyond that.
//!
//! ## Post-hook Response
//!
//! Post-hooks are fire-and-forget. Any response is logged but does not affect
//! the upload operation. Because the result is discarded, post-hooks are sent
//! exactly once by default; opt in to post-hook retries with
//! [`HttpHookConfig::with_post_hook_retry`].

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

/// Re-export of the `reqwest` crate used by this executor.
///
/// Types from `reqwest` (such as [`reqwest::Client`]) appear in this crate's
/// public API — for example in [`HttpHookExecutor::with_client`] — so this
/// re-export lets downstream crates name the exact `reqwest` version without
/// adding their own, possibly mismatched, dependency.
pub use reqwest;

use async_trait::async_trait;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::{Client, StatusCode, Url, header::RETRY_AFTER};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tus_protocol::{Error, HookContext, HookExecutor, PreHookResult, Result, UploadMetadata};
use uuid::Uuid;

/// Default timeout for webhook requests.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
/// Default overall deadline for retrying a single webhook delivery.
const DEFAULT_RETRY_DEADLINE: Duration = Duration::from_secs(30);
/// Upper bound on the per-attempt webhook retry backoff (before jitter).
const MAX_RETRY_DELAY_MILLIS: u64 = 10_000;
/// Maximum number of bytes read from a non-2xx webhook response body for
/// diagnostics; longer bodies are truncated.
const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;
/// Maximum accepted size of a pre-hook decision body; larger responses fail
/// the hook.
const MAX_PRE_HOOK_BODY_BYTES: usize = 64 * 1024;
/// Marker appended to truncated error bodies.
const TRUNCATION_MARKER: &str = " [truncated]";
const DELIVERY_HEADER: &str = "X-Tus-Hook-Delivery";
const SIGNATURE_HEADER: &str = "X-Tus-Signature-256";

/// Configuration for the HTTP webhook executor.
///
/// Construct via [`HttpHookConfig::new`] and the `with_*` builder methods.
/// Fields are private so the configuration can gain invariants or change
/// representation without a breaking change; the type is also
/// `#[non_exhaustive]` so new webhook knobs can be added without a major
/// version bump.
#[derive(Clone)]
#[non_exhaustive]
pub struct HttpHookConfig {
    /// The webhook endpoint URL.
    url: String,

    /// Request timeout, applied to every webhook request (including each
    /// retry attempt individually), regardless of how the underlying client
    /// was constructed.
    timeout: Duration,

    /// Additional headers to include in webhook requests.
    headers: HashMap<String, String>,

    /// Whether to retry failed requests.
    retry_enabled: bool,

    /// Maximum number of retry attempts.
    max_retries: u32,

    /// Overall deadline for retrying a single webhook delivery.
    ///
    /// Once the next retry (including its backoff delay) would exceed this
    /// deadline, the executor stops retrying and reports the last outcome.
    /// Defaults to 30 seconds.
    retry_deadline: Duration,

    /// Whether post-hooks participate in retries.
    ///
    /// Post-hook results are logged and discarded, so retrying them only
    /// delays hook dispatch; they are sent exactly once by default. Only
    /// takes effect when retries are enabled.
    retry_post_hooks: bool,

    /// Shared secret used to sign webhook bodies with HMAC-SHA256.
    signing_secret: Option<String>,

    /// Whether an undeliverable pre-hook allows the operation to proceed.
    ///
    /// When `false` (the default), a pre-hook that cannot obtain a successful,
    /// parseable decision fails closed and blocks the operation. When `true`,
    /// such failures fail open and the operation proceeds. A structured
    /// rejection (a `2xx` response with `"proceed": false`) is always honored
    /// regardless of this flag.
    fail_open: bool,
}

impl std::fmt::Debug for HttpHookConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render header values or the signing secret: custom headers
        // routinely carry `Authorization` tokens and `signing_secret` is an
        // HMAC key. Both would otherwise leak into any log that debug-formats
        // the config (directly or via `HttpHookExecutor`). The URL is redacted
        // too: it can carry `user:pass@` userinfo or a secret query parameter,
        // exactly what `redacted_url` strips on the logging paths.
        let redacted_url = Url::parse(&self.url)
            .map(|url| HttpHookExecutor::redacted_url(&url))
            .unwrap_or_else(|_| "[unparseable url]".to_string());
        f.debug_struct("HttpHookConfig")
            .field("url", &redacted_url)
            .field("timeout", &self.timeout)
            .field(
                "headers",
                &self
                    .headers
                    .keys()
                    .collect::<std::collections::BTreeSet<_>>(),
            )
            .field("retry_enabled", &self.retry_enabled)
            .field("max_retries", &self.max_retries)
            .field("retry_deadline", &self.retry_deadline)
            .field("retry_post_hooks", &self.retry_post_hooks)
            .field(
                "signing_secret",
                &self.signing_secret.as_ref().map(|_| "[redacted]"),
            )
            .field("fail_open", &self.fail_open)
            .finish()
    }
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
            retry_deadline: DEFAULT_RETRY_DEADLINE,
            retry_post_hooks: false,
            signing_secret: None,
            fail_open: false,
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

    /// Sets the overall deadline for retrying a single webhook delivery.
    ///
    /// Once the next retry (including its backoff delay) would exceed this
    /// deadline, the executor stops retrying and reports the last outcome.
    /// Defaults to 30 seconds.
    ///
    /// When webhook signing is enabled, keep this below the freshness tolerance
    /// your receiver enforces on the signature timestamp: the timestamp is
    /// fixed per delivery, so a retry sent near a large deadline can be rejected
    /// as stale (see the crate-level signature docs).
    pub fn with_retry_deadline(mut self, deadline: Duration) -> Self {
        self.retry_deadline = deadline;
        self
    }

    /// Enables retrying post-hook webhooks.
    ///
    /// Post-hook results are logged and discarded, so retrying them only
    /// delays hook dispatch; they are sent exactly once unless this is
    /// enabled. Only takes effect when retries are enabled via
    /// [`with_retry`](Self::with_retry).
    pub fn with_post_hook_retry(mut self, enabled: bool) -> Self {
        self.retry_post_hooks = enabled;
        self
    }

    /// Enables HMAC-SHA256 signing of webhook bodies.
    pub fn with_signing_secret(mut self, secret: impl Into<String>) -> Self {
        self.signing_secret = Some(secret.into());
        self
    }

    /// Sets whether an undeliverable pre-hook fails open (allows the operation
    /// to proceed) or fails closed (blocks it).
    ///
    /// By default the executor fails closed — a pre-hook webhook that is
    /// unreachable, times out, or does not return a successful, parseable
    /// decision blocks the operation. Pass `true` for deployments that prefer
    /// to keep accepting uploads when the webhook endpoint is unavailable. A
    /// structured rejection (a `2xx` response with `"proceed": false`) is
    /// always honored regardless of this flag.
    pub fn with_fail_open(mut self, fail_open: bool) -> Self {
        self.fail_open = fail_open;
        self
    }
}

/// Response from a pre-hook webhook.
///
/// Webhook servers construct one via [`PreHookResponse::new`] (or [`Default`])
/// and the `with_*` builder methods, then serialize it as the response body.
/// Fields are private so construction always goes through the builder and the
/// representation can evolve; the JSON wire contract is defined by the serde
/// field names, which remain stable.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PreHookResponse {
    /// Whether to proceed with the operation.
    #[serde(default = "default_proceed")]
    proceed: bool,

    /// Replacement user metadata, if any.
    #[serde(default)]
    metadata: Option<UploadMetadata>,

    /// HTTP status code for rejection.
    #[serde(default)]
    reject_status: Option<u16>,

    /// Rejection message for the client.
    #[serde(default)]
    reject_message: Option<String>,

    /// Additional response headers to include.
    #[serde(default)]
    response_headers: Option<HashMap<String, String>>,
}

impl PreHookResponse {
    /// Creates a response that allows the operation to proceed.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether to proceed with the operation.
    pub fn with_proceed(mut self, proceed: bool) -> Self {
        self.proceed = proceed;
        self
    }

    /// Sets replacement user metadata.
    pub fn with_metadata(mut self, metadata: UploadMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Sets the HTTP status code used when rejecting the operation.
    pub fn with_reject_status(mut self, status: u16) -> Self {
        self.reject_status = Some(status);
        self
    }

    /// Sets the rejection message returned to the client.
    pub fn with_reject_message(mut self, message: impl Into<String>) -> Self {
        self.reject_message = Some(message.into());
        self
    }

    /// Adds a response header to include in the upstream response.
    pub fn with_response_header(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.response_headers
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value.into());
        self
    }
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
        // Build through the constructors so a `proceed: true` webhook response
        // can never smuggle a rejection status past `PreHookResult`'s invariant.
        let mut result = if response.proceed {
            PreHookResult::proceed()
        } else {
            PreHookResult::reject(
                response.reject_status.unwrap_or(403),
                response.reject_message.unwrap_or_default(),
            )
        };
        if let Some(metadata) = response.metadata {
            result = result.with_metadata(metadata);
        }
        for (name, value) in response.response_headers.unwrap_or_default() {
            result = result.with_header(name, value);
        }
        result
    }
}

/// HTTP webhook executor that implements the [`HookExecutor`] trait.
///
/// This executor sends webhook requests to a configured endpoint for each hook
/// event. Pre-hooks can reject operations, add response headers, or replace
/// user metadata based on the webhook response.
///
/// Cloning is cheap: `reqwest::Client` is itself a shared handle.
#[derive(Clone)]
pub struct HttpHookExecutor {
    client: Client,
    url: Url,
    config: HttpHookConfig,
}

impl HttpHookExecutor {
    /// Creates a new HTTP hook executor with the given configuration.
    ///
    /// Builds a [`reqwest::Client`] and validates the configured webhook URL,
    /// failing fast with a [`BuildError`] instead of erroring on the first
    /// webhook send.
    pub fn new(config: HttpHookConfig) -> std::result::Result<Self, BuildError> {
        let client = Client::builder()
            .build()
            .map_err(|error| BuildError::Client(Box::new(error)))?;

        Self::with_client(client, config)
    }

    /// Creates a new HTTP hook executor with a custom reqwest client.
    ///
    /// Validates the configured webhook URL, failing fast with a
    /// [`BuildError`] instead of erroring on the first webhook send. The
    /// timeout set via [`HttpHookConfig::with_timeout`] is applied per request,
    /// so it takes effect for custom clients as well.
    pub fn with_client(
        client: Client,
        config: HttpHookConfig,
    ) -> std::result::Result<Self, BuildError> {
        let url =
            Url::parse(&config.url).map_err(|error| BuildError::InvalidUrl(Box::new(error)))?;
        if url.scheme() != "http" && url.scheme() != "https" {
            return Err(BuildError::InvalidUrl(
                format!(
                    "unsupported webhook URL scheme `{}`; expected `http` or `https`",
                    url.scheme()
                )
                .into(),
            ));
        }

        Ok(Self {
            client,
            url,
            config,
        })
    }

    fn payload(ctx: &HookContext) -> Result<Vec<u8>> {
        serde_json::to_vec(ctx).map_err(Error::hook)
    }

    /// Renders the webhook URL for logging with any userinfo stripped.
    ///
    /// A configured URL such as `https://user:pass@host/hook` retains its
    /// `user:pass@` credentials in the parsed `Url`; logging it verbatim would
    /// leak them, defeating the `Debug` redaction elsewhere in this crate. This
    /// emits only `scheme://host[:port]/path`, dropping userinfo (and the query
    /// string, which may also carry secrets).
    fn redacted_url(url: &Url) -> String {
        use std::fmt::Write as _;

        let mut redacted = format!("{}://{}", url.scheme(), url.host_str().unwrap_or(""));
        if let Some(port) = url.port() {
            let _ = write!(redacted, ":{port}");
        }
        redacted.push_str(url.path());
        redacted
    }

    /// Builds the `X-Tus-Signature-256` value: an HMAC-SHA256 over
    /// `"{timestamp}.{body}"` in Stripe/GitHub style, formatted
    /// `t=<unix_secs>,v1=<hex-hmac>`.
    ///
    /// Signing the timestamp alongside the body gives receivers replay
    /// protection: they can reject deliveries whose `t` is too old.
    fn signature_header_value(secret: &str, timestamp: u64, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .expect("HMAC accepts keys of any size");
        mac.update(timestamp.to_string().as_bytes());
        mac.update(b".");
        mac.update(body);
        let digest = mac.finalize().into_bytes();

        let mut hex = String::with_capacity(digest.len() * 2);
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
        }

        format!("t={timestamp},v1={hex}")
    }

    /// Returns the current Unix time in whole seconds, used as the signature
    /// timestamp. Captured once per delivery so it is stable across retries.
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0)
    }

    async fn send_webhook(
        &self,
        event: &str,
        delivery_id: &str,
        timestamp: u64,
        payload: &[u8],
    ) -> std::result::Result<reqwest::Response, reqwest::Error> {
        // The HMAC signature covers `"{timestamp}.{body}"`; the signed
        // timestamp is emitted in the signature header so receivers get replay
        // protection. `X-Tus-Hook-Delivery` and the other headers are additive
        // and do not affect verification.
        let mut request = self
            .client
            .post(self.url.clone())
            .header("Content-Type", "application/json")
            .header("X-Tus-Hook-Event", event)
            .header(DELIVERY_HEADER, delivery_id)
            .timeout(self.config.timeout)
            .body(payload.to_vec());

        if let Some(secret) = self.config.signing_secret.as_deref() {
            request = request.header(
                SIGNATURE_HEADER,
                Self::signature_header_value(secret, timestamp, payload),
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

    /// Parses the `Retry-After` header (seconds form) from `429`/`503`
    /// responses.
    fn retry_after(response: &reqwest::Response) -> Option<Duration> {
        let status = response.status();
        if status != StatusCode::TOO_MANY_REQUESTS && status != StatusCode::SERVICE_UNAVAILABLE {
            return None;
        }

        let seconds: u64 = response
            .headers()
            .get(RETRY_AFTER)?
            .to_str()
            .ok()?
            .trim()
            .parse()
            .ok()?;

        Some(Duration::from_secs(seconds))
    }

    /// Computes the delay before the next retry attempt.
    ///
    /// A server-provided `Retry-After` wins (capped at the retry deadline so
    /// an absurd value cannot extend retrying past it); otherwise capped
    /// exponential backoff with random 0-50% jitter is used.
    fn retry_delay(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        if let Some(retry_after) = retry_after {
            return retry_after.min(self.config.retry_deadline);
        }

        // Exponential backoff, capped: `max_retries` is user-configurable
        // and this runs on the synchronous pre-hook path, so the delay must
        // stay bounded (and the shift must not overflow). Jitter spreads out
        // retries from concurrent uploads.
        let backoff = Duration::from_millis((100u64 << attempt.min(8)).min(MAX_RETRY_DELAY_MILLIS));

        backoff + backoff.mul_f64(fastrand::f64() * 0.5)
    }

    async fn send_webhook_with_retry(
        &self,
        ctx: &HookContext,
        retry_enabled: bool,
    ) -> Result<reqwest::Response> {
        let payload = Self::payload(ctx)?;
        let delivery_id = Uuid::new_v4().to_string();
        // Captured once per delivery, alongside the delivery id, so both the
        // signed timestamp and the signature are stable across retries.
        let timestamp = Self::current_timestamp();
        let max_attempts = if retry_enabled {
            self.config.max_retries.saturating_add(1)
        } else {
            1
        };
        // `None` means the deadline is unrepresentably far away and never
        // triggers.
        let deadline = tokio::time::Instant::now().checked_add(self.config.retry_deadline);
        let mut attempt: u32 = 0;

        loop {
            let is_last_attempt = attempt.saturating_add(1) >= max_attempts;

            let delay = match self
                .send_webhook(ctx.event.as_str(), &delivery_id, timestamp, &payload)
                .await
            {
                Ok(response) => {
                    let status = response.status();
                    if is_last_attempt || !Self::is_retryable_status(status) {
                        return Ok(response);
                    }

                    let delay = self.retry_delay(attempt, Self::retry_after(&response));
                    if Self::exceeds_deadline(delay, deadline) {
                        tracing::warn!(
                            status = %status,
                            "webhook retry deadline exceeded; returning last response"
                        );
                        return Ok(response);
                    }

                    tracing::warn!(
                        attempt = attempt + 1,
                        max_attempts = max_attempts,
                        status = %status,
                        "webhook returned retryable status"
                    );

                    delay
                }
                Err(error) => {
                    if is_last_attempt {
                        return Err(Error::hook(error));
                    }

                    let delay = self.retry_delay(attempt, None);
                    if Self::exceeds_deadline(delay, deadline) {
                        tracing::warn!(
                            error = %error,
                            "webhook retry deadline exceeded"
                        );
                        return Err(Error::hook(error));
                    }

                    tracing::warn!(
                        attempt = attempt + 1,
                        max_attempts = max_attempts,
                        error = %error,
                        "webhook request failed"
                    );

                    delay
                }
            };

            tokio::time::sleep(delay).await;
            attempt = attempt.saturating_add(1);
        }
    }

    /// Returns whether waiting `delay` would push past the retry deadline.
    fn exceeds_deadline(delay: Duration, deadline: Option<tokio::time::Instant>) -> bool {
        let Some(deadline) = deadline else {
            return false;
        };

        tokio::time::Instant::now()
            .checked_add(delay)
            .is_none_or(|next_attempt| next_attempt > deadline)
    }

    /// Reads at most `cap` bytes of the response body.
    ///
    /// Returns the collected bytes and whether the body was longer than the
    /// cap. Reads chunk by chunk so an untrusted endpoint cannot buffer an
    /// arbitrarily large body in memory.
    async fn read_body_capped(
        mut response: reqwest::Response,
        cap: usize,
    ) -> std::result::Result<(Vec<u8>, bool), reqwest::Error> {
        let mut body = Vec::new();

        while let Some(chunk) = response.chunk().await? {
            if body.len() + chunk.len() > cap {
                body.extend_from_slice(&chunk[..cap - body.len()]);
                return Ok((body, true));
            }

            body.extend_from_slice(&chunk);
        }

        Ok((body, false))
    }

    /// Reads a non-2xx response body for diagnostics, capped at
    /// [`MAX_ERROR_BODY_BYTES`] with a truncation marker.
    async fn read_error_body(response: reqwest::Response) -> String {
        match Self::read_body_capped(response, MAX_ERROR_BODY_BYTES).await {
            Ok((bytes, truncated)) => {
                let mut body = String::from_utf8_lossy(&bytes).into_owned();
                if truncated {
                    body.push_str(TRUNCATION_MARKER);
                }
                body
            }
            Err(_) => "Webhook error".to_string(),
        }
    }
}

impl std::fmt::Debug for HttpHookExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpHookExecutor")
            .field("config", &self.config)
            .finish()
    }
}

impl HttpHookExecutor {
    /// Delivers a pre-hook webhook and returns its decision, or an error when
    /// the webhook could not produce a successful, parseable decision. The
    /// fail-open policy is applied by the [`HookExecutor::execute_pre`] wrapper.
    async fn deliver_pre_hook(&self, ctx: &HookContext) -> Result<PreHookResult> {
        tracing::debug!(
            event = ctx.event.as_str(),
            upload_id = %ctx.upload.id(),
            url = %Self::redacted_url(&self.url),
            "executing pre-hook webhook"
        );

        let response = self
            .send_webhook_with_retry(ctx, self.config.retry_enabled)
            .await?;
        let status = response.status();

        if !status.is_success() {
            tracing::warn!(
                event = ctx.event.as_str(),
                upload_id = %ctx.upload.id(),
                status = %status,
                "pre-hook webhook returned non-success status"
            );

            let body = Self::read_error_body(response).await;

            return Err(Error::hook(HttpHookStatusError { status, body }));
        }

        let (body, truncated) = Self::read_body_capped(response, MAX_PRE_HOOK_BODY_BYTES)
            .await
            .map_err(|error| {
                tracing::warn!(
                    event = ctx.event.as_str(),
                    upload_id = %ctx.upload.id(),
                    error = %error,
                    "failed to read pre-hook response body"
                );
                Error::hook(error)
            })?;

        if truncated {
            tracing::warn!(
                event = ctx.event.as_str(),
                upload_id = %ctx.upload.id(),
                "pre-hook response body exceeded size limit"
            );
            return Err(Error::Hook(
                format!("pre-hook response body exceeded {MAX_PRE_HOOK_BODY_BYTES} bytes").into(),
            ));
        }

        let hook_response: PreHookResponse = serde_json::from_slice(&body).map_err(|error| {
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
}

#[async_trait]
impl HookExecutor for HttpHookExecutor {
    async fn execute_pre(&self, ctx: &HookContext) -> Result<PreHookResult> {
        match self.deliver_pre_hook(ctx).await {
            Ok(result) => Ok(result),
            Err(error) if self.config.fail_open => {
                tracing::warn!(
                    event = ctx.event.as_str(),
                    upload_id = %ctx.upload.id(),
                    error = %error,
                    "pre-hook webhook did not return a decision; failing open (proceeding)"
                );
                Ok(PreHookResult::proceed())
            }
            Err(error) => Err(error),
        }
    }

    async fn execute_post(&self, ctx: &HookContext) {
        tracing::debug!(
            event = ctx.event.as_str(),
            upload_id = %ctx.upload.id(),
            url = %Self::redacted_url(&self.url),
            "executing post-hook webhook"
        );

        // Post-hook results are logged and discarded, so retrying inline
        // only delays hook dispatch; retries are opt-in.
        let retry_enabled = self.config.retry_enabled && self.config.retry_post_hooks;

        match self.send_webhook_with_retry(ctx, retry_enabled).await {
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
    }
}

/// Error returned when an [`HttpHookExecutor`] cannot be constructed.
///
/// The underlying cause is available via [`std::error::Error::source`].
#[derive(Debug)]
#[non_exhaustive]
pub enum BuildError {
    /// The configured webhook URL is not a valid `http`/`https` URL.
    InvalidUrl(Box<dyn std::error::Error + Send + Sync>),

    /// The underlying HTTP client could not be built (for example when the
    /// TLS backend fails to initialize).
    Client(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::InvalidUrl(_) => write!(f, "invalid webhook URL"),
            BuildError::Client(_) => write!(f, "failed to build HTTP client"),
        }
    }
}

impl std::error::Error for BuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BuildError::InvalidUrl(error) | BuildError::Client(error) => Some(error.as_ref()),
        }
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
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
            .with_retry_deadline(Duration::from_secs(10))
            .with_post_hook_retry(true)
            .with_signing_secret("secret");

        assert_eq!(config.url, "https://example.com/webhook");
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert_eq!(
            config.headers.get("Authorization"),
            Some(&"Bearer token".to_string())
        );
        assert!(config.retry_enabled);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_deadline, Duration::from_secs(10));
        assert!(config.retry_post_hooks);
        assert_eq!(config.signing_secret.as_deref(), Some("secret"));
    }

    #[test]
    fn debug_redacts_signing_secret_and_header_values() {
        let config = HttpHookConfig::new("https://example.com/webhook")
            .with_header("Authorization", "Bearer super-secret-token")
            .with_signing_secret("hmac-signing-key");
        let executor = HttpHookExecutor::new(config.clone()).unwrap();

        for rendered in [format!("{config:?}"), format!("{executor:?}")] {
            assert!(
                !rendered.contains("hmac-signing-key"),
                "signing secret leaked: {rendered}"
            );
            assert!(
                !rendered.contains("Bearer super-secret-token"),
                "header value leaked: {rendered}"
            );
            // Structure is still useful: the secret's presence and the header
            // name are visible, only the sensitive values are hidden.
            assert!(
                rendered.contains("[redacted]"),
                "no redaction marker: {rendered}"
            );
            assert!(
                rendered.contains("Authorization"),
                "header name hidden: {rendered}"
            );
        }
    }

    #[test]
    fn debug_redacts_url_userinfo_and_query() {
        let config =
            HttpHookConfig::new("https://user:pass@example.com/webhook?token=super-secret");

        let rendered = format!("{config:?}");

        assert!(
            !rendered.contains("user:pass"),
            "url userinfo leaked: {rendered}"
        );
        assert!(
            !rendered.contains("super-secret"),
            "url query secret leaked: {rendered}"
        );
        // The scheme/host/path are still shown so the config stays debuggable.
        assert!(
            rendered.contains("https://example.com/webhook"),
            "redacted url missing safe parts: {rendered}"
        );
    }

    #[test]
    fn new_rejects_invalid_webhook_url() {
        let error = HttpHookExecutor::new(HttpHookConfig::new("not a url")).unwrap_err();

        assert!(matches!(error, BuildError::InvalidUrl(_)));
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn new_rejects_non_http_webhook_url() {
        let error =
            HttpHookExecutor::new(HttpHookConfig::new("ftp://example.com/webhook")).unwrap_err();

        assert!(matches!(error, BuildError::InvalidUrl(_)));
    }

    #[test]
    fn with_client_rejects_invalid_webhook_url() {
        let error = HttpHookExecutor::with_client(Client::new(), HttpHookConfig::new("::nope::"))
            .unwrap_err();

        assert!(matches!(error, BuildError::InvalidUrl(_)));
    }

    #[test]
    fn signature_header_value_signs_timestamped_body() {
        let secret = "secret";
        let timestamp = 1_700_000_000u64;
        let body = br#"{"ok":true}"#;

        let signature = HttpHookExecutor::signature_header_value(secret, timestamp, body);

        // The header carries a parseable `t=<unix_secs>,v1=<hex-hmac>` value.
        let (t_part, v1_part) = signature.split_once(',').expect("comma-separated fields");
        assert_eq!(t_part, "t=1700000000");
        let hex = v1_part.strip_prefix("v1=").expect("v1 field");

        // The signature is computed over `"{t}.{body}"`, not the body alone.
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(format!("{timestamp}.").as_bytes());
        mac.update(body);
        let expected: String = mac
            .finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(hex, expected);

        // Signing over the raw body only (the pre-replay-protection scheme)
        // must no longer match, proving the timestamp is covered.
        let mut body_only = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        body_only.update(body);
        let body_only: String = body_only
            .finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_ne!(hex, body_only);
    }

    #[test]
    fn signature_timestamp_is_recoverable_and_verifies() {
        // A receiver parses `t`, recomputes the HMAC over `"{t}.{body}"`, and
        // the documented scheme verifies.
        let secret = "shared-secret";
        let body = br#"{"event":"pre-create"}"#;
        let timestamp = HttpHookExecutor::current_timestamp();

        let header = HttpHookExecutor::signature_header_value(secret, timestamp, body);

        let (t_part, v1_part) = header.split_once(',').unwrap();
        let parsed_ts: u64 = t_part.strip_prefix("t=").unwrap().parse().unwrap();
        let signature = v1_part.strip_prefix("v1=").unwrap();

        let recomputed = HttpHookExecutor::signature_header_value(secret, parsed_ts, body);
        assert_eq!(recomputed, header);
        assert!(recomputed.ends_with(signature));
    }

    #[test]
    fn redacted_url_strips_userinfo() {
        let url =
            Url::parse("https://user:pass@hooks.example.com:8443/tus-hook?token=abc").unwrap();

        let redacted = HttpHookExecutor::redacted_url(&url);

        assert_eq!(redacted, "https://hooks.example.com:8443/tus-hook");
        // Neither the userinfo credentials nor the query secret appear verbatim.
        assert!(!redacted.contains("user"), "userinfo leaked: {redacted}");
        assert!(!redacted.contains("pass"), "password leaked: {redacted}");
        assert!(
            !redacted.contains("token"),
            "query secret leaked: {redacted}"
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
    fn pre_hook_response_builder_sets_fields() {
        let metadata: UploadMetadata =
            serde_json::from_value(serde_json::json!({"filename": "hook.txt"})).unwrap();

        let response = PreHookResponse::new()
            .with_proceed(false)
            .with_metadata(metadata)
            .with_reject_status(422)
            .with_reject_message("nope")
            .with_response_header("X-Custom", "value");

        assert!(!response.proceed);
        assert!(response.metadata.is_some());
        assert_eq!(response.reject_status, Some(422));
        assert_eq!(response.reject_message.as_deref(), Some("nope"));
        assert_eq!(
            response
                .response_headers
                .as_ref()
                .and_then(|headers| headers.get("X-Custom")),
            Some(&"value".to_string())
        );
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

        assert!(!result.proceeds());
        assert_eq!(result.reject_status(), Some(403));
        assert_eq!(result.reject_message(), Some("Forbidden"));
        assert_eq!(
            result.response_headers().get("X-Custom"),
            Some(&"value".to_string())
        );
    }

    #[test]
    fn pre_hook_response_discards_reject_fields_when_proceeding() {
        // A webhook that proceeds must not smuggle a rejection status/message
        // into the result: the constructor-based conversion drops them.
        let response = PreHookResponse {
            proceed: true,
            metadata: None,
            reject_status: Some(403),
            reject_message: Some("ignored".to_string()),
            response_headers: None,
        };

        let result: PreHookResult = response.into();

        assert!(result.proceeds());
        assert_eq!(result.reject_status(), None);
        assert_eq!(result.reject_message(), None);
    }

    #[test]
    fn pre_hook_response_maps_metadata_replacement() {
        let response: PreHookResponse =
            serde_json::from_str(r#"{"metadata":{"filename":"hook.txt"}}"#).unwrap();

        let result: PreHookResult = response.into();

        assert!(result.proceeds());
        assert_eq!(
            result
                .metadata()
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
        let delivery = request_header(&request, "x-tus-hook-delivery")
            .expect("request must include delivery header");
        let body = request_body(&request);

        assert!(!result.proceeds());
        assert_eq!(result.reject_status(), Some(409));
        assert_eq!(result.reject_message(), Some("blocked"));
        assert!(request.starts_with("POST /hook HTTP/1.1"));
        assert!(request_lower.contains("content-type: application/json"));
        assert!(request_lower.contains("x-tus-hook-event: pre-create"));
        assert!(Uuid::parse_str(&delivery).is_ok());
        // The signature header carries a parseable timestamp; recomputing the
        // documented `"{t}.{body}"` HMAC with that timestamp reproduces it.
        let (t_part, _) = signature.split_once(',').expect("comma-separated fields");
        let sent_ts: u64 = t_part
            .strip_prefix("t=")
            .expect("t field")
            .parse()
            .expect("numeric timestamp");
        assert_eq!(
            signature,
            HttpHookExecutor::signature_header_value(secret, sent_ts, body.as_bytes())
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
    async fn fail_open_proceeds_when_pre_hook_returns_error_status() {
        let (url, request) = serve_once(500, "boom").await;
        let executor = HttpHookExecutor::new(HttpHookConfig::new(url).with_fail_open(true)).unwrap();

        let result = executor.execute_pre(&hook_context()).await.unwrap();
        let _request = request.await.unwrap();

        assert!(result.proceeds());
    }

    #[tokio::test]
    async fn fail_open_still_honors_structured_rejection() {
        let (url, request) = serve_once(200, r#"{"proceed":false,"reject_status":403}"#).await;
        let executor = HttpHookExecutor::new(HttpHookConfig::new(url).with_fail_open(true)).unwrap();

        let result = executor.execute_pre(&hook_context()).await.unwrap();
        let _request = request.await.unwrap();

        assert!(!result.proceeds());
        assert_eq!(result.reject_status(), Some(403));
    }

    #[tokio::test]
    async fn execute_pre_truncates_oversized_error_bodies() {
        let (url, request) = serve_once(403, "x".repeat(9 * 1024)).await;
        let executor = HttpHookExecutor::new(HttpHookConfig::new(url)).unwrap();

        let error = executor.execute_pre(&hook_context()).await.unwrap_err();
        let _request = request.await.unwrap();

        let Error::Hook(inner) = error else {
            panic!("expected hook error, got {error:?}");
        };
        let message = inner.to_string();
        assert!(message.contains(TRUNCATION_MARKER));
        assert!(message.len() < 9 * 1024);
    }

    #[tokio::test]
    async fn execute_pre_rejects_oversized_decision_bodies() {
        let body = format!(r#"{{"proceed":true,"pad":"{}"}}"#, "x".repeat(70 * 1024));
        let (url, request) = serve_once(200, body).await;
        let executor = HttpHookExecutor::new(HttpHookConfig::new(url)).unwrap();

        let error = executor.execute_pre(&hook_context()).await.unwrap_err();
        let _request = request.await.unwrap();

        let Error::Hook(inner) = error else {
            panic!("expected hook error, got {error:?}");
        };
        assert!(inner.to_string().contains("exceeded"));
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
    async fn with_client_applies_config_timeout_per_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _request = read_request(&mut stream).await;
            // Hold the connection open without responding.
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(stream);
        });
        // `Client::new()` has no default timeout; the config timeout must
        // still apply per request.
        let executor = HttpHookExecutor::with_client(
            Client::new(),
            HttpHookConfig::new(format!("http://{addr}/hook"))
                .with_timeout(Duration::from_millis(200)),
        )
        .unwrap();

        let start = Instant::now();
        let result = executor.execute_pre(&hook_context()).await;

        assert!(result.is_err());
        assert!(start.elapsed() < Duration::from_secs(5));
        server.abort();
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

        assert!(result.proceeds());
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
    async fn execute_pre_stops_retrying_at_retry_deadline() {
        let (url, count, server) = serve_repeat(503, "unavailable").await;
        let executor = HttpHookExecutor::new(
            HttpHookConfig::new(url)
                .with_retry(true)
                .with_max_retries(100)
                .with_retry_deadline(Duration::from_millis(300)),
        )
        .unwrap();

        let start = Instant::now();
        let result = executor.execute_pre(&hook_context()).await;

        assert!(result.is_err());
        assert!(start.elapsed() < Duration::from_secs(5));
        assert!(count.load(Ordering::SeqCst) < 10);
        server.abort();
    }

    #[tokio::test]
    async fn execute_pre_does_not_panic_with_max_retries_at_u32_max() {
        // An unreachable endpoint: bind a port, then drop the listener.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let executor = HttpHookExecutor::new(
            HttpHookConfig::new(format!("http://{addr}/hook"))
                .with_retry(true)
                .with_max_retries(u32::MAX)
                .with_retry_deadline(Duration::from_millis(200)),
        )
        .unwrap();

        let result = executor.execute_pre(&hook_context()).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_pre_honors_retry_after_header() {
        let (url, requests) = serve_raw_sequence(vec![
            http_response(429, "", "retry-after: 1\r\n"),
            http_response(200, r#"{"proceed":true}"#, ""),
        ])
        .await;
        let executor = HttpHookExecutor::new(
            HttpHookConfig::new(url)
                .with_retry(true)
                .with_max_retries(2),
        )
        .unwrap();

        let start = Instant::now();
        let result = executor.execute_pre(&hook_context()).await.unwrap();
        let requests = requests.await.unwrap();

        assert!(result.proceeds());
        assert_eq!(requests.len(), 2);
        // The default backoff would retry after 100-150ms; waiting at least
        // ~1s proves `Retry-After: 1` was honored.
        assert!(start.elapsed() >= Duration::from_millis(900));
    }

    #[tokio::test]
    async fn execute_pre_caps_retry_after_at_retry_deadline() {
        let (url, requests) = serve_raw_sequence(vec![http_response(
            429,
            "slow down",
            "retry-after: 9999\r\n",
        )])
        .await;
        let executor = HttpHookExecutor::new(
            HttpHookConfig::new(url)
                .with_retry(true)
                .with_max_retries(5)
                .with_retry_deadline(Duration::from_millis(300)),
        )
        .unwrap();

        let start = Instant::now();
        let result = executor.execute_pre(&hook_context()).await;
        let requests = requests.await.unwrap();

        assert!(result.is_err());
        assert_eq!(requests.len(), 1);
        assert!(start.elapsed() < Duration::from_secs(3));
    }

    #[tokio::test]
    async fn delivery_header_is_stable_across_retries_and_unique_per_delivery() {
        let (url, requests) = serve_sequence(vec![
            (503, ""),
            (200, r#"{"proceed":true}"#),
            (503, ""),
            (200, r#"{"proceed":true}"#),
        ])
        .await;
        let executor = HttpHookExecutor::new(
            HttpHookConfig::new(url)
                .with_retry(true)
                .with_max_retries(3),
        )
        .unwrap();

        executor.execute_pre(&hook_context()).await.unwrap();
        executor.execute_pre(&hook_context()).await.unwrap();
        let requests = requests.await.unwrap();

        let ids: Vec<String> = requests
            .iter()
            .map(|request| {
                request_header(request, "x-tus-hook-delivery")
                    .expect("request must include delivery header")
            })
            .collect();
        assert!(Uuid::parse_str(&ids[0]).is_ok());
        assert_eq!(ids[0], ids[1]);
        assert_eq!(ids[2], ids[3]);
        assert_ne!(ids[0], ids[2]);
    }

    #[tokio::test]
    async fn execute_post_retries_retryable_statuses_when_opted_in() {
        let (url, requests) = serve_sequence(vec![(429, ""), (200, "{}")]).await;
        let executor = HttpHookExecutor::new(
            HttpHookConfig::new(url)
                .with_retry(true)
                .with_max_retries(3)
                .with_post_hook_retry(true),
        )
        .unwrap();

        executor.execute_post(&hook_context()).await;
        let requests = requests.await.unwrap();

        assert_eq!(requests.len(), 2);
    }

    #[tokio::test]
    async fn execute_post_does_not_retry_by_default() {
        let (url, count, server) = serve_repeat(503, "unavailable").await;
        let executor = HttpHookExecutor::new(
            HttpHookConfig::new(url)
                .with_retry(true)
                .with_max_retries(3),
        )
        .unwrap();

        executor.execute_post(&hook_context()).await;

        assert_eq!(count.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test]
    async fn execute_post_ignores_non_success_response() {
        let (url, request) = serve_once(500, "failed").await;
        let executor = HttpHookExecutor::new(HttpHookConfig::new(url)).unwrap();

        executor.execute_post(&hook_context()).await;
        let _request = request.await.unwrap();
    }

    fn hook_context() -> HookContext {
        let mut request = HookRequestInfo::default();
        request.method = "POST".to_string();
        request.path = "/uploads".to_string();

        HookContext::new(
            HookEvent::PreCreate,
            HookUpload::new("test-upload-id"),
            request,
        )
    }

    fn http_response(status: u16, body: &str, extra_headers: &str) -> String {
        format!(
            "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n{extra_headers}connection: close\r\n\r\n{body}",
            body.len()
        )
    }

    async fn serve_once(status: u16, body: impl Into<String>) -> (String, JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let response = http_response(status, &body.into(), "");

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
    async fn serve_sequence<S: Into<String>>(
        responses: Vec<(u16, S)>,
    ) -> (String, JoinHandle<Vec<String>>) {
        serve_raw_sequence(
            responses
                .into_iter()
                .map(|(status, body)| http_response(status, &body.into(), ""))
                .collect(),
        )
        .await
    }

    /// Serves one preformatted HTTP response per incoming connection, in
    /// order, and returns the raw requests once all responses have been
    /// served.
    async fn serve_raw_sequence(responses: Vec<String>) -> (String, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let requests = tokio::spawn(async move {
            let mut requests = Vec::new();

            for response in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                requests.push(read_request(&mut stream).await);
                stream.write_all(response.as_bytes()).await.unwrap();
            }

            requests
        });

        (format!("http://{addr}/hook"), requests)
    }

    /// Serves the same response for every incoming connection until aborted,
    /// counting the requests received.
    async fn serve_repeat(
        status: u16,
        body: &'static str,
    ) -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));

        let server = tokio::spawn({
            let count = Arc::clone(&count);
            async move {
                loop {
                    let response = http_response(status, body, "");
                    let (mut stream, _) = listener.accept().await.unwrap();
                    count.fetch_add(1, Ordering::SeqCst);
                    let _request = read_request(&mut stream).await;
                    let _ = stream.write_all(response.as_bytes()).await;
                }
            }
        });

        (format!("http://{addr}/hook"), count, server)
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
