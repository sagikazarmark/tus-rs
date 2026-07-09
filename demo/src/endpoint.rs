//! Shared TUS endpoint plumbing.
//!
//! The endpoint is resolved once at startup — preferring `?endpoint=...` in the
//! URL, then the `TUS_ENDPOINT` env var baked in at build time, then a
//! localhost default — and provided through context so every example reads the
//! same value. Changing it navigates with a fresh `?endpoint=` query so a
//! single build works against any TUS server without rebuilding.

use dioxus::prelude::*;

/// Endpoint shared with every example via context.
#[derive(Clone, PartialEq)]
pub struct Endpoint(pub String);

/// Compile-time fallback, overridable at runtime via `?endpoint=`.
const COMPILE_TIME_ENDPOINT: Option<&str> = option_env!("TUS_ENDPOINT");
const DEFAULT_ENDPOINT: &str = "http://localhost:8080/files";

/// Reads the effective endpoint: `?endpoint=` query string, then the
/// build-time `TUS_ENDPOINT`, then the localhost default.
pub fn resolve_endpoint() -> String {
    if let Some(value) = endpoint_from_query()
        && !value.is_empty()
    {
        return value;
    }
    COMPILE_TIME_ENDPOINT
        .map(str::to_string)
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string())
}

fn endpoint_from_query() -> Option<String> {
    let search = web_sys::window()?.location().search().ok()?;
    for pair in search.trim_start_matches('?').split('&') {
        if let Some(value) = pair.strip_prefix("endpoint=")
            && let Ok(decoded) = js_sys::decode_uri_component(value)
            && let Some(s) = decoded.as_string()
        {
            return Some(s);
        }
    }
    None
}

/// Reloads the app pointed at `endpoint`, encoding it into the `?endpoint=`
/// query string. Because the hook reads the endpoint once at construction, a
/// full navigation is the clean way to re-point every example at a new server.
pub fn navigate_to_endpoint(endpoint: &str) {
    let Some(window) = web_sys::window() else {
        return;
    };
    let encoded = js_sys::encode_uri_component(endpoint);
    let target = format!(
        "{}?endpoint={}",
        window
            .location()
            .pathname()
            .unwrap_or_else(|_| "/".to_string()),
        encoded.as_string().unwrap_or_default(),
    );
    let _ = window.location().assign(&target);
}

/// Convenience: read the current endpoint from context.
pub fn use_endpoint() -> String {
    use_context::<Endpoint>().0
}
