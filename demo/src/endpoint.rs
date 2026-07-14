//! Shared TUS endpoint plumbing.
//!
//! The endpoint is resolved once at startup, preferring `?endpoint=...` in the
//! URL, then the `TUS_ENDPOINT` env var baked in at build time, then a
//! localhost default, and provided through context so every example reads the
//! same value. Changing it navigates with a fresh `?endpoint=` query so a
//! single build works against any TUS server without rebuilding.

use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;
use wasm_bindgen_futures::JsFuture;
use web_sys::{RegistrationOptions, ServiceWorkerUpdateViaCache, Url};

/// Endpoint shared with every example via context.
#[derive(Clone, PartialEq)]
pub struct Endpoint(pub String);

/// Compile-time fallback, overridable at runtime via `?endpoint=`.
const COMPILE_TIME_ENDPOINT: Option<&str> = option_env!("TUS_ENDPOINT");
const SERVICE_WORKER_VERSION: &str = env!("DEMO_SERVICE_WORKER_VERSION");

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
        .or_else(browser_endpoint)
        .unwrap_or_else(|| "http://localhost:8081/files".to_string())
}

/// Returns whether this URL is the same-origin endpoint owned by the demo's
/// service worker.
pub fn is_browser_endpoint(endpoint: &str) -> bool {
    let Some(local) = browser_endpoint() else {
        return false;
    };
    let (Ok(endpoint), Ok(local)) = (Url::new(endpoint), Url::new(&local)) else {
        return false;
    };
    endpoint.origin() == local.origin()
        && endpoint.pathname().trim_end_matches('/') == local.pathname().trim_end_matches('/')
}

/// Registers the Rust service worker and waits until it controls this page.
pub async fn prepare_browser_endpoint() -> Result<(), String> {
    let window = web_sys::window().ok_or_else(|| "browser window is unavailable".to_string())?;
    let base = document_base_url().ok_or_else(|| "document base URL is unavailable".to_string())?;
    let script = Url::new_with_base(
        &format!("service-worker.js?v={SERVICE_WORKER_VERSION}"),
        &base,
    )
    .map_err(js_message)?;
    let workers = window.navigator().service_worker();
    let options = RegistrationOptions::new();
    options.set_type("module");
    options.set_update_via_cache(ServiceWorkerUpdateViaCache::None);
    JsFuture::from(workers.register_with_options(&script.href(), &options))
        .await
        .map_err(js_message)?;

    // `clients.claim()` runs in the worker's activate event. Waiting for this
    // exact version avoids accepting an unrelated or stale controller.
    for _ in 0..200 {
        if workers
            .controller()
            .is_some_and(|controller| controller.script_url() == script.href())
        {
            return Ok(());
        }
        TimeoutFuture::new(50).await;
    }
    Err("service worker did not take control within 10 seconds".to_string())
}

fn browser_endpoint() -> Option<String> {
    let base = document_base_url()?;
    Url::new_with_base("files", &base)
        .ok()
        .map(|url| url.href())
}

fn document_base_url() -> Option<String> {
    web_sys::window()?.document()?.base_uri().ok().flatten()
}

fn js_message(value: wasm_bindgen::JsValue) -> String {
    value
        .as_string()
        .unwrap_or_else(|| "browser rejected service-worker registration".to_string())
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
