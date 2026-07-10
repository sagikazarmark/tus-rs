//! Persistence of in-flight upload URLs across page reloads.
//!
//! Stores `(endpoint, filename, file_size, last_modified) -> upload_url` in
//! `localStorage` so a tab close + reopen can resume from the server's
//! current offset rather than re-uploading from zero.
//!
//! ## Match key
//!
//! Browsers don't expose stable file identifiers; when the user re-picks
//! the "same" file after reload, we rebind via a best-effort match key:
//! `(endpoint, filename, file_size, last_modified)`. Two distinct files
//! with the same name + size + mtime collide; rare but the registry name is
//! "match key", not "fingerprint", to honestly mark this.
//!
//! ## TTL + eviction
//!
//! Entries older than the storage TTL are filtered out by [`scan`]. They
//! aren't proactively garbage-collected on every read; operationally the
//! storage namespace is `dioxus-tus:v1:*`, distinct enough that stale
//! entries don't leak into other consumers.
//!
//! Entries whose `upload_url` origin doesn't match the configured
//! `endpoint` origin are dropped on read, a defence against same-origin
//! storage poisoning where a different page on the same domain writes a
//! key that, on next load, would point our wasm at an attacker-controlled
//! upload URL.

use serde::{Deserialize, Serialize};

/// Key prefix scoping our entries inside the page's localStorage namespace.
///
/// `pub(crate)`: the on-disk key format is an internal compatibility detail,
/// not a public contract.
pub(crate) const STORAGE_KEY_PREFIX: &str = "dioxus-tus:v1:";

/// Entries older than this are filtered from [`scan`]. 24 hours.
pub(crate) const STORAGE_TTL_MS: f64 = 24.0 * 60.0 * 60.0 * 1000.0;

/// One persisted in-flight upload.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct ResumableEntry {
    /// Stable derived key used for both the localStorage entry name and
    /// matching a re-picked file against the entry.
    pub match_key: String,
    /// Endpoint this URL was created against. Used for origin validation.
    pub endpoint: String,
    /// Filename at upload start (`web_sys::File::name()`).
    pub filename: String,
    /// Size at upload start (bytes).
    pub file_size: u64,
    /// `web_sys::File::last_modified()` value (ms since epoch). Best-effort
    /// match component, unreliable on some browser/OS combos.
    pub last_modified: f64,
    /// The TUS resource URL the partial upload lives at.
    pub upload_url: String,
    /// Last persisted bytes_uploaded (informational; the server's HEAD
    /// response is always treated as authoritative for the actual offset).
    pub bytes_uploaded: u64,
    /// Insertion timestamp (`js_sys::Date::now()` value, ms since epoch).
    pub stored_at_ms: f64,
}

/// Derives the stable match key that identifies a (endpoint, file) pair.
///
/// The key is opaque; consumers should treat it as a string identifier rather
/// than parsing it.
///
/// Pure function: no I/O, callable from native tests without wasm.
///
/// `pub(crate)`: the key-derivation scheme is an internal detail. Consumers
/// read the derived value via [`ResumableEntry::match_key`]; they don't
/// recompute it.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn match_key(
    endpoint: &str,
    filename: &str,
    file_size: u64,
    last_modified: f64,
) -> String {
    // last_modified can be NaN on some browsers; coerce to a stable integer
    // milliseconds value to keep the key deterministic.
    let lm_ms = if last_modified.is_finite() {
        last_modified as i64
    } else {
        0
    };

    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    fn update_hash(mut hash: u64, bytes: &[u8]) -> u64 {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    fn feed_component(hash: u64, bytes: &[u8]) -> u64 {
        let hash = update_hash(hash, &bytes.len().to_le_bytes());
        update_hash(hash, bytes)
    }

    let mut a = FNV_OFFSET;
    let mut b = FNV_OFFSET ^ 0x9e37_79b9_7f4a_7c15;
    for component in [endpoint.as_bytes(), filename.as_bytes()] {
        a = feed_component(a, component);
        b = feed_component(b.rotate_left(5), component);
    }
    a = feed_component(a, &file_size.to_le_bytes());
    b = feed_component(b.rotate_left(5), &file_size.to_le_bytes());
    a = feed_component(a, &lm_ms.to_le_bytes());
    b = feed_component(b.rotate_left(5), &lm_ms.to_le_bytes());

    format!("v2-{a:016x}{b:016x}")
}

/// Returns the localStorage key for the given match key.
///
/// `pub(crate)`: internal storage-layout detail.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn storage_key(match_key: &str) -> String {
    format!("{STORAGE_KEY_PREFIX}{match_key}")
}

fn parse_origin(s: &str) -> Option<(String, String, u16)> {
    // URL crate isn't a dep here; do a simple scheme://host[:port] split.
    // This is sufficient because we only compare/log tus-server URLs, and
    // TUS URLs are always plain http(s). Any URL the parse can't recognise
    // returns None and callers fail closed or log an opaque marker.
    let scheme_end = s.find("://")?;
    let scheme = s[..scheme_end].to_ascii_lowercase();
    let rest = &s[scheme_end + 3..];
    // The authority ends at the first '/', '?', or '#'. Without the
    // query/fragment terminators a URL like `https://x.test?token=...`
    // would parse the host as `x.test?token=...`.
    let path_start = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..path_start];
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if host_port.is_empty() {
        return None;
    }
    let (host, explicit_port) = if let Some(colon) = host_port.rfind(':') {
        // Watch out for IPv6 brackets, but we don't support those here;
        // tus URLs are typically named hosts. Conservative: take the last
        // colon as the port separator only if everything after parses as u16.
        let after = &host_port[colon + 1..];
        if let Ok(p) = after.parse::<u16>() {
            (host_port[..colon].to_ascii_lowercase(), Some(p))
        } else {
            (host_port.to_ascii_lowercase(), None)
        }
    } else {
        (host_port.to_ascii_lowercase(), None)
    };
    // Canonicalise an omitted port to the scheme default so
    // `https://x/files` and `https://x:443/files` compare equal.
    let port = explicit_port.or(match scheme.as_str() {
        "http" | "ws" => Some(80),
        "https" | "wss" => Some(443),
        _ => None,
    })?;
    Some((scheme, host, port))
}

/// Validates that `upload_url`'s origin matches `endpoint`'s origin.
///
/// Defends against same-origin localStorage poisoning where a stored
/// entry's `upload_url` points at an attacker-controlled host.
///
/// Returns `true` when the URLs share scheme + host + port, OR when
/// either URL fails to parse (in which case we conservatively reject by
/// returning false).
///
/// `pub(crate)`: internal same-origin defence, not part of the public surface.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn origin_matches(endpoint: &str, upload_url: &str) -> bool {
    match (parse_origin(endpoint), parse_origin(upload_url)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

/// Returns a log-safe description of a TUS upload URL.
///
/// TUS resource URLs are often capability URLs. Never emit the path tail,
/// query, or fragment, because those may contain bearer-equivalent tokens.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn redact_upload_url_for_log(upload_url: &str) -> String {
    match parse_origin(upload_url) {
        Some((scheme, host, port)) => format!("{scheme}://{host}:{port}/<upload-url-redacted>"),
        None => "<invalid-upload-url-redacted>".to_string(),
    }
}

/// Returns a log-safe endpoint URL, preserving the route but stripping
/// credentials, query strings, and fragments.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn redact_endpoint_for_log(endpoint: &str) -> String {
    let Some((scheme, host, port)) = parse_origin(endpoint) else {
        return "<invalid-endpoint-redacted>".to_string();
    };

    let path = endpoint
        .find("://")
        .and_then(|scheme_end| {
            let rest = &endpoint[scheme_end + 3..];
            let path_start = rest.find('/')?;
            Some(&rest[path_start..])
        })
        .and_then(|path| path.split(['?', '#']).next())
        .filter(|path| !path.is_empty())
        .unwrap_or("");

    format!("{scheme}://{host}:{port}{path}")
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn entry_is_resumable_for_file_at(
    endpoint: &str,
    entry: &ResumableEntry,
    filename: &str,
    file_size: u64,
    last_modified: f64,
    now_ms: f64,
) -> bool {
    let age_ms = now_ms - entry.stored_at_ms;
    age_ms.is_finite()
        && (0.0..=STORAGE_TTL_MS).contains(&age_ms)
        && entry.endpoint == endpoint
        && origin_matches(endpoint, &entry.upload_url)
        && entry.match_key == match_key(endpoint, filename, file_size, last_modified)
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn entry_is_resumable_for_file(
    endpoint: &str,
    entry: &ResumableEntry,
    filename: &str,
    file_size: u64,
    last_modified: f64,
) -> bool {
    entry_is_resumable_for_file_at(
        endpoint,
        entry,
        filename,
        file_size,
        last_modified,
        js_sys::Date::now(),
    )
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    //! `localStorage` access is wasm-only; native test builds skip this.
    use super::*;
    use crate::state::TusError;

    /// Returns the page's `Window.localStorage`. None if unavailable
    /// (Workers, SSR, or sandbox modes that disable storage).
    fn local_storage() -> Option<web_sys::Storage> {
        web_sys::window()?.local_storage().ok()?
    }

    /// Lists every stored entry not older than [`STORAGE_TTL_MS`], filtering
    /// any whose `upload_url` origin doesn't match the supplied endpoint.
    ///
    /// Diagnostics emit at `tracing::debug!`; set `RUST_LOG=dioxus_tus=debug`
    /// (or wire up `tracing-wasm`) to inspect why entries are being filtered.
    ///
    /// Public so consumers can inspect persisted entries via
    /// [`crate::TusUploadHandle::scan_resumable`] /
    /// [`crate::TusQueueHandle::scan_resumable`]; the read-only filter
    /// surface is safe to expose. The mutating counterparts (`put`,
    /// `remove`, `get`) are crate-internal; see their visibility.
    pub fn scan(endpoint: &str) -> Vec<ResumableEntry> {
        let Some(storage) = local_storage() else {
            tracing::debug!("scan: localStorage unavailable");
            return Vec::new();
        };
        let now = js_sys::Date::now();

        // Collect matching key names *first*, then read values by name.
        // The Web Storage spec doesn't guarantee index stability under
        // concurrent modification by other same-origin pages; `key(i)`
        // can shift if another tab inserts/removes a key mid-loop, which
        // would make us skip or double-visit entries. Iterating by name
        // is benign under that race (a vanished key just yields None).
        let len = storage.length().unwrap_or(0);
        let mut keys = Vec::with_capacity(len as usize);
        for i in 0..len {
            let Some(key) = storage.key(i).ok().flatten() else {
                continue;
            };
            if key.starts_with(STORAGE_KEY_PREFIX) {
                keys.push(key);
            }
        }

        let mut out = Vec::new();
        for key in keys {
            let Some(value) = storage.get_item(&key).ok().flatten() else {
                tracing::debug!(%key, "scan: get_item returned no value");
                continue;
            };
            let entry: ResumableEntry = match serde_json::from_str(&value) {
                Ok(e) => e,
                Err(e) => {
                    // Drop malformed entries instead of skipping. Without
                    // this, a schema mismatch from an older crate version
                    // (or a same-origin app colliding on the prefix) keeps
                    // the entry in storage for the full 24h TTL while
                    // never being usable, consuming quota and spamming
                    // warns on every scan.
                    tracing::warn!(%key, error = %e, "scan: removing malformed entry");
                    let _ = storage.remove_item(&key);
                    continue;
                }
            };
            let Some(stored_match_key) = key.strip_prefix(STORAGE_KEY_PREFIX) else {
                continue;
            };
            if entry.match_key != stored_match_key {
                tracing::debug!(%key, "scan: skipping match_key mismatch");
                continue;
            }
            let age_ms = now - entry.stored_at_ms;
            // Reject negative / non-finite ages alongside expired ones.
            // A clock step backwards (NTP, VM resume, manual change) or
            // a future-dated `stored_at_ms` would otherwise pass the TTL
            // check unconditionally and live forever.
            if !age_ms.is_finite() || !(0.0..=STORAGE_TTL_MS).contains(&age_ms) {
                tracing::debug!(%key, age_ms, "scan: skipping stale or clock-skewed entry");
                let _ = storage.remove_item(&key);
                continue;
            }
            // Endpoint exact-match: two endpoints sharing the same origin
            // (e.g. /api/files-eu vs /api/files-us, or /api/v1 vs /api/v2)
            // would otherwise cross-contaminate each other's resume offers.
            // The origin check below stays; it defends against a stored
            // upload_url pointing at a different host than the configured
            // endpoint, which the endpoint check alone wouldn't catch.
            if entry.endpoint != endpoint {
                let configured_endpoint = redact_endpoint_for_log(endpoint);
                let stored_endpoint = redact_endpoint_for_log(&entry.endpoint);
                tracing::debug!(
                    %key,
                    configured_endpoint = %configured_endpoint,
                    stored_endpoint = %stored_endpoint,
                    "scan: skipping endpoint mismatch",
                );
                continue;
            }
            if !origin_matches(endpoint, &entry.upload_url) {
                let stored_upload_url = redact_upload_url_for_log(&entry.upload_url);
                let configured_endpoint = redact_endpoint_for_log(endpoint);
                let stored_endpoint = redact_endpoint_for_log(&entry.endpoint);
                tracing::debug!(
                    %key,
                    configured_endpoint = %configured_endpoint,
                    stored_endpoint = %stored_endpoint,
                    stored_upload_url = %stored_upload_url,
                    "scan: skipping origin mismatch",
                );
                continue;
            }
            out.push(entry);
        }
        out
    }

    /// Loads the entry for `match_key` if present, fresh, and origin-valid.
    /// Crate-internal; consumers go through `TusUploadHandle::resume_persisted`
    /// or `TusQueueHandle::add` (which calls this internally).
    pub(crate) fn get(endpoint: &str, match_key: &str) -> Option<ResumableEntry> {
        let storage = local_storage()?;
        let raw = storage.get_item(&storage_key(match_key)).ok()??;
        let entry: ResumableEntry = serde_json::from_str(&raw).ok()?;
        if entry.match_key != match_key {
            return None;
        }
        let now = js_sys::Date::now();
        let age_ms = now - entry.stored_at_ms;
        if !age_ms.is_finite() || !(0.0..=STORAGE_TTL_MS).contains(&age_ms) {
            return None;
        }
        // Endpoint mismatch can happen even with the same match_key: two
        // endpoints on the same origin produce different match_keys (the
        // endpoint is part of the key), but a malicious / corrupted entry
        // could carry a different `entry.endpoint` than the storage_key
        // was derived against. Reject it.
        if entry.endpoint != endpoint {
            return None;
        }
        if !origin_matches(endpoint, &entry.upload_url) {
            return None;
        }
        Some(entry)
    }

    /// Inserts or replaces the entry. Errors are mapped to
    /// [`TusError::Transport`] since localStorage failures are observable
    /// to the consumer (quota exceeded, sandboxed iframe, etc.).
    ///
    /// Crate-internal: only the engine writes entries (with engine-supplied
    /// `stored_at_ms`). Exposing `put` would let a consumer forge a future
    /// `stored_at_ms` and bypass the TTL filter in [`scan`].
    pub(crate) fn put(entry: &ResumableEntry) -> Result<(), TusError> {
        let storage = local_storage()
            .ok_or_else(|| TusError::Transport("localStorage unavailable".into()))?;
        let json = serde_json::to_string(entry)
            .map_err(|e| TusError::Transport(format!("serialize entry: {e}")))?;
        let key = storage_key(&entry.match_key);
        if storage.set_item(&key, &json).is_ok() {
            return Ok(());
        }
        // Quota likely exhausted. Sweep the namespace for stale and
        // malformed entries (which `scan` filters but never deletes) and
        // try once more. Without this, a quota-full localStorage stays
        // wedged for the full 24h TTL; the `scan`-side filter prevents
        // them from being served but cannot reclaim the quota they
        // occupy, so every future `put` silently fails.
        evict_stale_entries(&storage);
        storage.set_item(&key, &json).map_err(|e| {
            TusError::Transport(format!(
                "localStorage write failed after eviction (likely quota): {e:?}"
            ))
        })
    }

    /// Sweeps the `dioxus-tus:v1:*` namespace and deletes entries that are
    /// either (a) past their TTL, (b) non-finite or future-dated, or (c)
    /// malformed JSON. Best-effort: storage failures during the sweep are
    /// swallowed because the caller has already failed once; partial
    /// progress is still useful.
    fn evict_stale_entries(storage: &web_sys::Storage) {
        let now = js_sys::Date::now();
        let len = storage.length().unwrap_or(0);
        // Snapshot keys before mutating: the index would shift under us
        // otherwise, just like the scan iteration safety fix from pass 6.
        let mut keys = Vec::with_capacity(len as usize);
        for i in 0..len {
            if let Ok(Some(k)) = storage.key(i)
                && k.starts_with(STORAGE_KEY_PREFIX)
            {
                keys.push(k);
            }
        }
        for key in keys {
            let value = match storage.get_item(&key).ok().flatten() {
                Some(v) => v,
                None => continue,
            };
            let drop = match serde_json::from_str::<ResumableEntry>(&value) {
                Ok(entry) => {
                    let age_ms = now - entry.stored_at_ms;
                    !age_ms.is_finite() || !(0.0..=STORAGE_TTL_MS).contains(&age_ms)
                }
                Err(_) => true,
            };
            if drop {
                let _ = storage.remove_item(&key);
            }
        }
    }

    /// Removes the entry for `match_key`. No-op when missing.
    /// Crate-internal; see `put` for the rationale.
    pub(crate) fn remove(match_key: &str) {
        if let Some(storage) = local_storage() {
            let _ = storage.remove_item(&storage_key(match_key));
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm::scan;
#[cfg(target_arch = "wasm32")]
pub(crate) use wasm::{get, put, remove};

// =====================================================================
// Wasm-only integration tests for the localStorage layer. Each test
// carefully scopes its match_keys so it doesn't collide with
// concurrently-running tests via the shared localStorage namespace.
// (wasm-bindgen-test is single-threaded but the whole test binary
// shares one localStorage; cleanup-after-self is the contract.)
// =====================================================================
#[cfg(target_arch = "wasm32")]
#[cfg(test)]
mod wasm_tests {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    /// `scan` returns only entries that are (a) fresh per TTL, (b) origin-
    /// matching, and (c) under the right namespace key prefix. Seeds three
    /// entries, fresh-matching / stale / origin-mismatch, and asserts
    /// only the first surfaces. Closes the largest blind spot in the
    /// persistence layer's tests: pre-this, no test ever drove `scan`
    /// against seeded entries, so a refactor that dropped the TTL or
    /// origin filter would land silently.
    #[wasm_bindgen_test]
    fn scan_filters_stale_and_origin_mismatched_entries() {
        let endpoint = "http://test.local/scan-filter-test";

        // Clean any leftovers from prior tests sharing localStorage.
        let storage = web_sys::window()
            .and_then(|w| w.local_storage().ok().flatten())
            .expect("localStorage available");
        let len = storage.length().unwrap_or(0);
        let mut to_remove = Vec::new();
        for i in 0..len {
            if let Ok(Some(k)) = storage.key(i)
                && k.starts_with(STORAGE_KEY_PREFIX)
            {
                to_remove.push(k);
            }
        }
        for k in to_remove {
            let _ = storage.remove_item(&k);
        }

        let now = js_sys::Date::now();

        // Entry A: fresh, origin-matching. Should surface.
        let mk_a = match_key(endpoint, "a.bin", 100, 1.0);
        let entry_a = ResumableEntry {
            match_key: mk_a.clone(),
            endpoint: endpoint.into(),
            filename: "a.bin".into(),
            file_size: 100,
            last_modified: 1.0,
            upload_url: format!("{endpoint}/a-id"),
            bytes_uploaded: 50,
            stored_at_ms: now,
        };
        put(&entry_a).expect("seed A");

        // Entry B: stale (>24h old). Should be filtered.
        let mk_b = match_key(endpoint, "b.bin", 200, 2.0);
        let entry_b = ResumableEntry {
            match_key: mk_b.clone(),
            endpoint: endpoint.into(),
            filename: "b.bin".into(),
            file_size: 200,
            last_modified: 2.0,
            upload_url: format!("{endpoint}/b-id"),
            bytes_uploaded: 0,
            stored_at_ms: now - STORAGE_TTL_MS - 60_000.0,
        };
        put(&entry_b).expect("seed B");

        // Entry C: fresh, but origin-mismatched (upload_url points at a
        // different host). Should be filtered.
        let mk_c = match_key(endpoint, "c.bin", 300, 3.0);
        let entry_c = ResumableEntry {
            match_key: mk_c.clone(),
            endpoint: endpoint.into(),
            filename: "c.bin".into(),
            file_size: 300,
            last_modified: 3.0,
            upload_url: "http://attacker.example/c-id".into(),
            bytes_uploaded: 0,
            stored_at_ms: now,
        };
        put(&entry_c).expect("seed C");

        let surfaced = scan(endpoint);
        let surfaced_names: Vec<&str> = surfaced.iter().map(|e| e.filename.as_str()).collect();

        assert!(
            surfaced_names.contains(&"a.bin"),
            "fresh + origin-matching A must surface; got {surfaced_names:?}",
        );
        assert!(
            !surfaced_names.contains(&"b.bin"),
            "stale B must be filtered; got {surfaced_names:?}",
        );
        assert!(
            !surfaced_names.contains(&"c.bin"),
            "origin-mismatched C must be filtered (defends against \
             same-origin localStorage poisoning); got {surfaced_names:?}",
        );

        // Cleanup so we don't leak into other tests.
        remove(&mk_a);
        remove(&mk_b);
        remove(&mk_c);
    }

    /// Two distinct endpoints sharing the same origin must not see each
    /// other's resume entries. Pre-fix, `scan` only filtered on origin;
    /// `/api/files-eu` and `/api/files-us` would surface each other's
    /// uploads, and a downstream `resume_persisted` call would HEAD the
    /// wrong URL or silently drop the resume hint.
    #[wasm_bindgen_test]
    fn scan_filters_same_origin_different_endpoint() {
        let ep_eu = "http://test.local/files-eu";
        let ep_us = "http://test.local/files-us";

        let storage = web_sys::window()
            .and_then(|w| w.local_storage().ok().flatten())
            .expect("localStorage available");
        let len = storage.length().unwrap_or(0);
        let mut to_remove = Vec::new();
        for i in 0..len {
            if let Ok(Some(k)) = storage.key(i)
                && k.starts_with(STORAGE_KEY_PREFIX)
            {
                to_remove.push(k);
            }
        }
        for k in to_remove {
            let _ = storage.remove_item(&k);
        }

        let now = js_sys::Date::now();
        let mk_eu = match_key(ep_eu, "eu.bin", 100, 1.0);
        put(&ResumableEntry {
            match_key: mk_eu.clone(),
            endpoint: ep_eu.into(),
            filename: "eu.bin".into(),
            file_size: 100,
            last_modified: 1.0,
            upload_url: format!("{ep_eu}/eu-id"),
            bytes_uploaded: 0,
            stored_at_ms: now,
        })
        .unwrap();

        let mk_us = match_key(ep_us, "us.bin", 200, 2.0);
        put(&ResumableEntry {
            match_key: mk_us.clone(),
            endpoint: ep_us.into(),
            filename: "us.bin".into(),
            file_size: 200,
            last_modified: 2.0,
            upload_url: format!("{ep_us}/us-id"),
            bytes_uploaded: 0,
            stored_at_ms: now,
        })
        .unwrap();

        let surfaced_eu = scan(ep_eu);
        let surfaced_us = scan(ep_us);
        assert_eq!(
            surfaced_eu.len(),
            1,
            "EU scan must not see US entry: {surfaced_eu:?}"
        );
        assert_eq!(surfaced_eu[0].filename, "eu.bin");
        assert_eq!(
            surfaced_us.len(),
            1,
            "US scan must not see EU entry: {surfaced_us:?}"
        );
        assert_eq!(surfaced_us[0].filename, "us.bin");

        remove(&mk_eu);
        remove(&mk_us);
    }

    /// `put` followed by `get` round-trips through JSON serialisation.
    /// Also verifies `get` honours the origin filter; an origin-mismatched
    /// entry returns None even when the localStorage key is present.
    #[wasm_bindgen_test]
    fn put_then_get_round_trips_and_filters_origin() {
        let endpoint = "http://test.local/put-get-roundtrip-test";
        let mk = match_key(endpoint, "rt.bin", 42, 7.0);
        remove(&mk);

        let entry = ResumableEntry {
            match_key: mk.clone(),
            endpoint: endpoint.into(),
            filename: "rt.bin".into(),
            file_size: 42,
            last_modified: 7.0,
            upload_url: format!("{endpoint}/rt-id"),
            bytes_uploaded: 21,
            stored_at_ms: js_sys::Date::now(),
        };
        put(&entry).expect("put round-trip");

        let read_back = get(endpoint, &mk).expect("get round-trip");
        assert_eq!(read_back, entry);

        // Origin-filter: same key, but a different endpoint with mismatched
        // origin must NOT find the entry.
        let other = "http://other.example/files";
        assert!(
            get(other, &mk).is_none(),
            "get must reject origin-mismatched entries even when the key exists",
        );

        remove(&mk);
        assert!(
            get(endpoint, &mk).is_none(),
            "get after remove returns None"
        );
    }

    #[wasm_bindgen_test]
    fn get_rejects_payload_match_key_mismatch() {
        let endpoint = "http://test.local/payload-mismatch-test";
        let mk = match_key(endpoint, "good.bin", 42, 7.0);
        remove(&mk);

        let forged = ResumableEntry {
            match_key: match_key(endpoint, "evil.bin", 42, 7.0),
            endpoint: endpoint.into(),
            filename: "good.bin".into(),
            file_size: 42,
            last_modified: 7.0,
            upload_url: format!("{endpoint}/evil-id"),
            bytes_uploaded: 21,
            stored_at_ms: js_sys::Date::now(),
        };
        let storage = web_sys::window()
            .and_then(|w| w.local_storage().ok().flatten())
            .expect("localStorage available");
        storage
            .set_item(&storage_key(&mk), &serde_json::to_string(&forged).unwrap())
            .expect("seed forged entry under different key");

        assert!(
            get(endpoint, &mk).is_none(),
            "get must reject a payload whose match_key disagrees with the storage key",
        );

        remove(&mk);
    }

    #[wasm_bindgen_test]
    fn scan_rejects_payload_match_key_mismatch() {
        let endpoint = "http://test.local/scan-payload-mismatch-test";
        let mk = match_key(endpoint, "good.bin", 42, 7.0);
        remove(&mk);

        let forged = ResumableEntry {
            match_key: match_key(endpoint, "evil.bin", 42, 7.0),
            endpoint: endpoint.into(),
            filename: "good.bin".into(),
            file_size: 42,
            last_modified: 7.0,
            upload_url: format!("{endpoint}/evil-id"),
            bytes_uploaded: 21,
            stored_at_ms: js_sys::Date::now(),
        };
        let storage = web_sys::window()
            .and_then(|w| w.local_storage().ok().flatten())
            .expect("localStorage available");
        storage
            .set_item(&storage_key(&mk), &serde_json::to_string(&forged).unwrap())
            .expect("seed forged entry under different key");

        let surfaced = scan(endpoint);
        assert!(
            surfaced.iter().all(|entry| entry.match_key == mk),
            "scan must not surface payloads whose match_key disagrees with the storage key: {surfaced:?}",
        );
        assert!(
            surfaced
                .iter()
                .all(|entry| entry.upload_url != forged.upload_url),
            "scan surfaced forged entry: {surfaced:?}",
        );

        remove(&mk);
    }

    /// `scan` and `get` must reject entries with a future-dated
    /// `stored_at_ms`. Pre-fix, `now - stored_at_ms` produced a negative
    /// `f64`, which is `< STORAGE_TTL_MS`, so the entry passed the TTL
    /// filter and lived until the wall clock caught up, possibly never.
    /// Repros: NTP step-back, manual clock change, multi-tab clock skew.
    #[wasm_bindgen_test]
    fn scan_rejects_future_dated_entry() {
        let endpoint = "http://test.local/clock-skew-test";
        let mk = match_key(endpoint, "future.bin", 1, 1.0);
        remove(&mk);

        let now = js_sys::Date::now();
        let entry = ResumableEntry {
            match_key: mk.clone(),
            endpoint: endpoint.into(),
            filename: "future.bin".into(),
            file_size: 1,
            last_modified: 1.0,
            upload_url: format!("{endpoint}/future-id"),
            bytes_uploaded: 0,
            // Stored "1 hour from now", clock skew scenario.
            stored_at_ms: now + 3_600_000.0,
        };
        put(&entry).expect("seed future-dated");

        // get must reject.
        assert!(
            get(endpoint, &mk).is_none(),
            "get must reject future-dated entry (clock-skew guard)",
        );

        // scan must also reject AND remove the offending entry as part
        // of the eviction policy added alongside the guard.
        let surfaced = scan(endpoint);
        assert!(
            !surfaced.iter().any(|e| e.filename == "future.bin"),
            "scan must filter future-dated entry; got {:?}",
            surfaced.iter().map(|e| &e.filename).collect::<Vec<_>>(),
        );

        remove(&mk);
    }

    /// `scan` removes malformed entries so they don't accumulate and
    /// consume quota for the full TTL. Pre-fix, malformed entries were
    /// skipped silently and stayed in storage forever.
    #[wasm_bindgen_test]
    fn scan_removes_malformed_entries() {
        let endpoint = "http://test.local/malformed-test";
        let mk = match_key(endpoint, "bad.bin", 1, 1.0);
        let key = storage_key(&mk);

        let storage = web_sys::window()
            .and_then(|w| w.local_storage().ok().flatten())
            .expect("localStorage available");
        // Direct write of malformed JSON, bypassing put().
        storage
            .set_item(&key, "{not valid json")
            .expect("seed malformed");
        assert!(
            storage.get_item(&key).ok().flatten().is_some(),
            "precondition: malformed entry written",
        );

        let _ = scan(endpoint);

        assert!(
            storage.get_item(&key).ok().flatten().is_none(),
            "scan must delete malformed entry, not just skip it",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_key_is_deterministic() {
        let a = match_key("https://x.test/files", "photo.jpg", 1024, 1_700_000_000.0);
        let b = match_key("https://x.test/files", "photo.jpg", 1024, 1_700_000_000.0);
        assert_eq!(a, b);
    }

    #[test]
    fn match_key_differs_on_endpoint() {
        let a = match_key("https://a.test/files", "photo.jpg", 1024, 1.0);
        let b = match_key("https://b.test/files", "photo.jpg", 1024, 1.0);
        assert_ne!(a, b);
    }

    #[test]
    fn match_key_differs_on_size() {
        let a = match_key("https://x.test/files", "photo.jpg", 1024, 1.0);
        let b = match_key("https://x.test/files", "photo.jpg", 2048, 1.0);
        assert_ne!(a, b);
    }

    #[test]
    fn match_key_differs_on_filename() {
        let a = match_key("https://x.test/files", "a.jpg", 1024, 1.0);
        let b = match_key("https://x.test/files", "b.jpg", 1024, 1.0);
        assert_ne!(a, b);
    }

    #[test]
    fn match_key_handles_nan_last_modified() {
        // Some browsers report NaN for unknown last_modified; the key
        // should still be deterministic.
        let a = match_key("https://x.test/files", "p.jpg", 1, f64::NAN);
        let b = match_key("https://x.test/files", "p.jpg", 1, f64::NAN);
        assert_eq!(a, b);
    }

    #[test]
    fn match_key_does_not_reveal_endpoint_material() {
        fn hex_of(value: &str) -> String {
            let mut out = String::with_capacity(value.len() * 2);
            for byte in value.as_bytes() {
                use std::fmt::Write;
                let _ = write!(&mut out, "{byte:02x}");
            }
            out
        }

        let endpoint = "https://user:password@x.test/files?token=super-secret-token#frag";
        let key = match_key(endpoint, "p.jpg", 1, 1.0);

        assert!(
            !key.contains(&hex_of(endpoint)),
            "endpoint bytes leaked through match key: {key}"
        );
        assert!(
            !key.contains(&hex_of("super-secret-token")),
            "query token leaked through match key: {key}"
        );
        assert!(
            !key.contains(&hex_of("password")),
            "userinfo leaked through match key: {key}"
        );
    }

    #[test]
    fn storage_key_prepends_namespace() {
        let key = match_key("https://x.test/files", "p.jpg", 1, 1.0);
        let storage = storage_key(&key);
        assert!(storage.starts_with(STORAGE_KEY_PREFIX));
        assert!(storage.ends_with(&key));
    }

    #[test]
    fn origin_matches_same_scheme_host_port() {
        assert!(origin_matches(
            "https://tus.example.com/files",
            "https://tus.example.com/files/abc",
        ));
    }

    #[test]
    fn origin_matches_with_explicit_port() {
        assert!(origin_matches(
            "http://localhost:8080/files",
            "http://localhost:8080/files/abc",
        ));
    }

    #[test]
    fn origin_does_not_match_different_host() {
        assert!(!origin_matches(
            "https://tus.example.com/files",
            "https://attacker.example.com/files/abc",
        ));
    }

    #[test]
    fn origin_does_not_match_different_scheme() {
        assert!(!origin_matches(
            "https://tus.example.com/files",
            "http://tus.example.com/files/abc",
        ));
    }

    #[test]
    fn origin_does_not_match_different_port() {
        assert!(!origin_matches(
            "http://localhost:8080/files",
            "http://localhost:9090/files/abc",
        ));
    }

    #[test]
    fn origin_matches_default_https_port_alias() {
        // A server that emits Location with explicit :443 still matches
        // an endpoint configured without one. Same for the reverse.
        assert!(origin_matches(
            "https://tus.example.com/files",
            "https://tus.example.com:443/files/abc",
        ));
        assert!(origin_matches(
            "https://tus.example.com:443/files",
            "https://tus.example.com/files/abc",
        ));
    }

    #[test]
    fn origin_matches_default_http_port_alias() {
        assert!(origin_matches(
            "http://tus.example.com/files",
            "http://tus.example.com:80/files/abc",
        ));
        assert!(origin_matches(
            "http://tus.example.com:80/files",
            "http://tus.example.com/files/abc",
        ));
    }

    #[test]
    fn origin_does_not_match_https_with_explicit_http_port() {
        // 443 alias is scheme-specific; explicit 80 on https is still
        // a different origin.
        assert!(!origin_matches(
            "https://tus.example.com/files",
            "https://tus.example.com:80/files/abc",
        ));
    }

    #[test]
    fn origin_does_not_match_malformed() {
        assert!(!origin_matches("not a url", "also not a url"));
        assert!(!origin_matches("http://x/files", "https:/x/files"));
    }

    #[test]
    fn origin_matches_when_query_string_present() {
        // Without the '?' terminator in parse_origin, the host would parse
        // as `tus.example.com?token=abc` and the comparison would fail.
        assert!(origin_matches(
            "https://tus.example.com?token=abc",
            "https://tus.example.com/files/xyz",
        ));
        assert!(origin_matches(
            "https://tus.example.com",
            "https://tus.example.com?next=/files/xyz",
        ));
    }

    #[test]
    fn origin_matches_when_fragment_present() {
        assert!(origin_matches(
            "https://tus.example.com#section",
            "https://tus.example.com/files/xyz",
        ));
    }

    #[test]
    fn upload_url_redaction_removes_query_fragment_and_tail() {
        let redacted = redact_upload_url_for_log(
            "https://tus.example.com/files/upload-abc?token=super-secret#frag",
        );

        assert!(redacted.contains("tus.example.com"));
        assert!(
            !redacted.contains("super-secret"),
            "query leaked: {redacted}"
        );
        assert!(!redacted.contains("frag"), "fragment leaked: {redacted}");
        assert!(
            !redacted.contains("upload-abc"),
            "resource id leaked: {redacted}"
        );
    }

    #[test]
    fn upload_url_redaction_removes_userinfo() {
        let redacted =
            redact_upload_url_for_log("https://user:super-secret@tus.example.com/files/upload-abc");

        assert!(redacted.contains("tus.example.com"));
        assert!(!redacted.contains("user"), "username leaked: {redacted}");
        assert!(
            !redacted.contains("super-secret"),
            "password leaked: {redacted}"
        );
        assert!(
            !redacted.contains('@'),
            "userinfo separator leaked: {redacted}"
        );
    }

    #[test]
    fn endpoint_redaction_removes_userinfo_query_and_fragment() {
        let redacted = redact_endpoint_for_log(
            "https://user:super-secret@tus.example.com/files?token=super-secret-token#frag",
        );

        assert!(redacted.contains("tus.example.com"));
        assert!(redacted.contains("/files"));
        assert!(!redacted.contains("user"), "username leaked: {redacted}");
        assert!(
            !redacted.contains("super-secret"),
            "secret leaked: {redacted}"
        );
        assert!(!redacted.contains("token"), "query key leaked: {redacted}");
        assert!(!redacted.contains("frag"), "fragment leaked: {redacted}");
        assert!(
            !redacted.contains('@'),
            "userinfo separator leaked: {redacted}"
        );
    }

    #[test]
    fn entry_is_resumable_only_for_matching_fresh_same_origin_file() {
        let endpoint = "https://tus.example.com/files";
        let filename = "report.pdf";
        let file_size = 50_000_000;
        let last_modified = 1_700_000_000_000.0;
        let now_ms = 1_700_000_500_000.0;
        let match_key = match_key(endpoint, filename, file_size, last_modified);
        let entry = ResumableEntry {
            match_key: match_key.clone(),
            endpoint: endpoint.into(),
            filename: filename.into(),
            file_size,
            last_modified,
            upload_url: "https://tus.example.com/files/upload-abc".into(),
            bytes_uploaded: 12_345_678,
            stored_at_ms: now_ms,
        };

        assert!(entry_is_resumable_for_file_at(
            endpoint,
            &entry,
            filename,
            file_size,
            last_modified,
            now_ms,
        ));

        let mut wrong_origin = entry.clone();
        wrong_origin.upload_url = "https://attacker.example/files/upload-abc".into();
        assert!(!entry_is_resumable_for_file_at(
            endpoint,
            &wrong_origin,
            filename,
            file_size,
            last_modified,
            now_ms,
        ));

        let mut wrong_endpoint = entry.clone();
        wrong_endpoint.endpoint = "https://tus.example.com/other-files".into();
        assert!(!entry_is_resumable_for_file_at(
            endpoint,
            &wrong_endpoint,
            filename,
            file_size,
            last_modified,
            now_ms,
        ));

        let mut stale = entry.clone();
        stale.stored_at_ms = now_ms - STORAGE_TTL_MS - 1.0;
        assert!(!entry_is_resumable_for_file_at(
            endpoint,
            &stale,
            filename,
            file_size,
            last_modified,
            now_ms,
        ));

        assert!(!entry_is_resumable_for_file_at(
            endpoint,
            &entry,
            "other.pdf",
            file_size,
            last_modified,
            now_ms,
        ));
    }

    #[test]
    fn resumable_entry_serde_roundtrip() {
        let entry = ResumableEntry {
            match_key: "abc123".into(),
            endpoint: "https://tus.example.com/files".into(),
            filename: "report.pdf".into(),
            file_size: 50_000_000,
            last_modified: 1_700_000_000_000.0,
            upload_url: "https://tus.example.com/files/xyz".into(),
            bytes_uploaded: 12_345_678,
            stored_at_ms: 1_700_000_500_000.0,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: ResumableEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry, back);
    }
}
