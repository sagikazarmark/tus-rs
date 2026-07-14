//! Retry classification for chunk PATCH failures.
//!
//! The chunk loop in [`crate::use_tus_upload`] consults [`is_retryable_error`] to
//! decide whether a failure is transient (retry after backoff) or fatal
//! (surface to the consumer immediately).
//!
//! Lifted out of the async chunk loop so the rule is unit-testable on
//! native (no `web_sys::Blob`, `gloo_timers`, or `wasm-bindgen-test`
//! plumbing required) and so a single source of truth governs both the
//! engine and the test assertions.

use tus_uploader::Error;

/// Classifies a [`tus_uploader::Error`] as retryable or not.
///
/// Retryable conditions:
/// - 5xx responses (transient server errors).
/// - 408 Request Timeout (proxy / server side timeout, safe to retry
///   PATCH because PATCH is idempotent under TUS).
/// - 409 Conflict (offset mismatch recovery signal; retry after re-reading
///   the server offset).
/// - 429 Too Many Requests (server-applied rate limiting).
/// - Transport errors the transport itself flagged retryable (network
///   failure, CORS preflight failure, fetch abort).
///
/// Everything else (4xx other than the three above, missing headers,
/// length mismatches, URL parse errors, deterministic transport failures,
/// …) is fatal.
pub fn is_retryable_error(e: &Error) -> bool {
    match e {
        Error::UnexpectedResponse { status, .. } => is_retryable_status(status.as_u16()),
        Error::Transport { retryable, .. } => *retryable,
        _ => false,
    }
}

/// Status-code half of [`is_retryable_error`]. Exposed separately so
/// callers that already have a parsed status (e.g. surfaced through
/// [`crate::state::TusError::Server`]) can reuse the rule without
/// reconstructing a [`tus_uploader::Error`].
pub fn is_retryable_status(status: u16) -> bool {
    status >= 500 || status == 408 || status == 409 || status == 429
}
