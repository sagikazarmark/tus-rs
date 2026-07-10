//! Per-endpoint cache for [`tus_client::ServerCapabilities`] with a 60-second TTL.
//!
//! The TUS hook calls OPTIONS once on the first upload to learn which
//! extensions the server advertises (creation-with-upload, termination,
//! checksum, etc.) and caches the result so subsequent uploads in the same
//! session don't pay the round trip. A 60-second TTL covers most cases of
//! operator extension toggles.
//!
//! Cache keys on the endpoint URL only. The hook bypasses this cache for
//! config-level bearer tokens, per-upload bearer-token overrides, and extra
//! headers because those request contexts can change server capabilities.
//!
//! Wasm32-only: depends on `js_sys::Date` for monotonic-ish time. The crate
//! is wasm32-gated overall so this is fine.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use futures::channel::oneshot;
use tus_client::{Client, ServerCapabilities, Transport};

use crate::state::TusError;

/// 60 seconds in milliseconds, per the autoplan eng review.
const TTL_MS: f64 = 60_000.0;

struct Entry {
    info: ServerCapabilities,
    /// `js_sys::Date::now()` value at insertion (milliseconds since epoch).
    inserted_at_ms: f64,
}

/// Process-global cache state. The mutex is uncontended in practice (wasm
/// is single-threaded; the `Mutex` is for the API contract more than the
/// concurrency model) and is only ever held synchronously, never across
/// an `await`.
#[derive(Default)]
struct CacheState {
    cached: HashMap<String, Entry>,
    /// In-flight OPTIONS fetches: callers that arrive while a fetch is
    /// underway register a oneshot here and `await` it instead of issuing
    /// their own request. This prevents the TOCTOU duplication where N
    /// concurrent uploads all see a cache miss in the same tick and each
    /// fire OPTIONS. Drains to empty when the fetch resolves.
    in_flight: HashMap<String, Vec<oneshot::Sender<Result<ServerCapabilities, TusError>>>>,
}

fn cache_state() -> &'static Mutex<CacheState> {
    static CACHE: OnceLock<Mutex<CacheState>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(CacheState::default()))
}

/// Outcome of inspecting the cache state.
enum Lookup {
    /// Fresh entry served from the cache.
    Hit(ServerCapabilities),
    /// Another caller is fetching; await on the receiver for the result.
    Wait(oneshot::Receiver<Result<ServerCapabilities, TusError>>),
    /// No fresh entry, no in-flight fetch; *this* caller must fire OPTIONS.
    /// The state has been mutated to mark `endpoint` as in-flight.
    Fetch,
}

/// Single point of mutex acquisition: returns Hit/Wait/Fetch and never
/// holds the guard across an `await`. The dispatch logic in
/// `get_or_fetch` matches on the result and executes accordingly.
fn lookup_or_register(endpoint: &str, now: f64) -> Lookup {
    let Ok(mut guard) = cache_state().lock() else {
        // Mutex poisoned (cannot happen on wasm, no panic-across-thread
        // path); fall through to a fresh fetch. The fetcher will
        // overwrite the state on completion; data integrity is preserved.
        return Lookup::Fetch;
    };
    if let Some(entry) = guard.cached.get(endpoint) {
        // Reject negative or non-finite age; a clock step backwards
        // (NTP correction, manual change) or a future-dated insertion
        // would otherwise make a stale entry pass the TTL check and
        // serve forever.
        let age_ms = now - entry.inserted_at_ms;
        if age_ms.is_finite() && (0.0..TTL_MS).contains(&age_ms) {
            return Lookup::Hit(entry.info.clone());
        }
    }
    if let Some(waiters) = guard.in_flight.get_mut(endpoint) {
        let (tx, rx) = oneshot::channel();
        waiters.push(tx);
        return Lookup::Wait(rx);
    }
    guard.in_flight.insert(endpoint.to_string(), Vec::new());
    Lookup::Fetch
}

/// Stores the fetch result and notifies all waiters. Called once per
/// fetch on the `Fetch` arm.
fn finalize_fetch(
    endpoint: &str,
    result: Result<ServerCapabilities, TusError>,
    inserted_at_ms: f64,
) -> Result<ServerCapabilities, TusError> {
    let waiters = match cache_state().lock() {
        Ok(mut guard) => {
            if let Ok(info) = &result {
                guard.cached.insert(
                    endpoint.to_string(),
                    Entry {
                        info: info.clone(),
                        inserted_at_ms,
                    },
                );
            }
            // Always remove the in-flight marker, success or failure. On
            // failure we don't cache (transient blips shouldn't poison
            // subsequent calls), but we must still wake any waiters so
            // they propagate the error rather than hanging.
            guard.in_flight.remove(endpoint).unwrap_or_default()
        }
        Err(_) => Vec::new(),
    };
    for tx in waiters {
        let _ = tx.send(result.clone());
    }
    result
}

/// Returns the cached [`ServerCapabilities`] for `endpoint` if it's fresher than
/// [`TTL_MS`]; otherwise fetches via `client.options()`, stores, and returns.
///
/// Concurrent callers for the same endpoint coalesce: the first caller
/// fires the OPTIONS request, others `await` its result instead of
/// duplicating the round-trip. On fetch failure nothing is cached and
/// every waiter sees the same error; a transient network blip doesn't
/// poison subsequent calls.
pub(crate) async fn get_or_fetch<T: Transport>(
    endpoint: &str,
    client: &Client<T>,
) -> Result<ServerCapabilities, TusError> {
    let now = js_sys::Date::now();
    match lookup_or_register(endpoint, now) {
        Lookup::Hit(info) => {
            let endpoint = crate::persistence::redact_endpoint_for_log(endpoint);
            tracing::debug!(%endpoint, "options cache hit");
            Ok(info)
        }
        Lookup::Wait(rx) => {
            let endpoint = crate::persistence::redact_endpoint_for_log(endpoint);
            tracing::debug!(%endpoint, "options cache: awaiting in-flight fetch");
            // If the sender is dropped (only possible if the originator
            // panicked before finalize_fetch, unreachable in practice)
            // surface a typed transport error rather than `.unwrap()`.
            rx.await.unwrap_or_else(|_| {
                Err(TusError::Transport(
                    "options cache: in-flight fetch was dropped".into(),
                ))
            })
        }
        Lookup::Fetch => {
            let endpoint_log = crate::persistence::redact_endpoint_for_log(endpoint);
            tracing::debug!(endpoint = %endpoint_log, "options cache miss; fetching");
            // RAII guard: if the future is dropped (component unmount, outer
            // select! cancellation, abort during cwu_fut) before we reach
            // finalize_fetch, the in_flight registration would otherwise
            // leak forever, wedging every subsequent caller for this
            // endpoint on a oneshot::Receiver whose Sender lives in a
            // 'static Mutex and is never dropped. Drain on drop with an
            // error so waiters propagate, then clear in_flight.
            struct DropGuard<'a> {
                endpoint: &'a str,
                fired: bool,
            }
            impl Drop for DropGuard<'_> {
                fn drop(&mut self) {
                    if !self.fired {
                        let _ = finalize_fetch(
                            self.endpoint,
                            Err(TusError::Transport(
                                "options cache: in-flight fetch was dropped".into(),
                            )),
                            0.0,
                        );
                    }
                }
            }
            let mut guard = DropGuard {
                endpoint,
                fired: false,
            };
            let result = client.server_capabilities().await.map_err(TusError::from);
            guard.fired = true;
            finalize_fetch(endpoint, result, now)
        }
    }
}

/// Drops any cached entry for `endpoint`. Call when the server may have
/// changed capabilities, e.g. on a 405 Method Not Allowed or 412
/// Precondition Failed response from a subsequent request that suggests the
/// cached extension list is stale.
///
/// Does not affect in-flight fetches; they will complete and notify
/// their waiters with whatever the OPTIONS call returned.
pub(crate) fn invalidate(endpoint: &str) {
    if let Ok(mut guard) = cache_state().lock()
        && guard.cached.remove(endpoint).is_some()
    {
        let endpoint = crate::persistence::redact_endpoint_for_log(endpoint);
        tracing::debug!(%endpoint, "options cache invalidated");
    }
}

/// Returns a fresh cached entry without touching the network. `None` when
/// the cache has no entry, the entry has expired, or the cache state is
/// unreachable. Cheap, just a HashMap lookup behind a mutex.
///
/// Used to enforce `Tus-Max-Size` on the plain-create path: if an earlier
/// upload in this session populated the cache, the limit is enforced; if
/// nothing is cached we skip the check rather than adding an OPTIONS
/// round trip to every plain upload.
pub(crate) fn peek_fresh(endpoint: &str) -> Option<ServerCapabilities> {
    let now = js_sys::Date::now();
    let guard = cache_state().lock().ok()?;
    let entry = guard.cached.get(endpoint)?;
    let age_ms = now - entry.inserted_at_ms;
    if age_ms.is_finite() && (0.0..TTL_MS).contains(&age_ms) {
        Some(entry.info.clone())
    } else {
        None
    }
}

// =====================================================================
// Cache behaviour tests: wasm-bindgen because the implementation reads
// `js_sys::Date::now()` for the TTL bookkeeping. The cache itself is
// process-global so each test uses a unique endpoint to stay isolated.
// TTL expiry is not exercised here (would require a 60s sleep or a time-
// mocking refactor); the cache hit/miss/invalidate paths are.
// =====================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    use async_trait::async_trait;
    use http::{HeaderMap, HeaderName, HeaderValue};
    use wasm_bindgen_test::*;

    use tus_client::url::Url;
    use tus_client::{Error, Transport, TransportRequest, TransportResponse};

    wasm_bindgen_test_configure!(run_in_browser);

    #[derive(Clone, Default)]
    struct CountingTransport(Rc<RefCell<CountingInner>>);

    #[derive(Default)]
    struct CountingInner {
        responses: VecDeque<TransportResponse>,
        request_count: usize,
    }

    impl CountingTransport {
        fn new() -> Self {
            Self::default()
        }
        fn push(&self, resp: TransportResponse) {
            self.0.borrow_mut().responses.push_back(resp);
        }
        fn request_count(&self) -> usize {
            self.0.borrow().request_count
        }
    }

    #[async_trait(?Send)]
    impl Transport for CountingTransport {
        async fn send(&self, _req: TransportRequest) -> tus_client::Result<TransportResponse> {
            let mut inner = self.0.borrow_mut();
            inner.request_count += 1;
            inner
                .responses
                .pop_front()
                .ok_or_else(|| Error::transport("no mock response"))
        }
    }

    /// Forces a single yield-then-Ready before consuming a queued response.
    /// Necessary to keep an in-flight OPTIONS observable to a concurrent
    /// caller; the default `CountingTransport` returns synchronously, so
    /// two `join`ed futures would never see each other's in-flight
    /// registration.
    #[derive(Clone, Default)]
    struct SlowTransport(Rc<RefCell<CountingInner>>);

    impl SlowTransport {
        fn new() -> Self {
            Self::default()
        }
        fn push(&self, resp: TransportResponse) {
            self.0.borrow_mut().responses.push_back(resp);
        }
        fn request_count(&self) -> usize {
            self.0.borrow().request_count
        }
    }

    #[async_trait(?Send)]
    impl Transport for SlowTransport {
        async fn send(&self, _req: TransportRequest) -> tus_client::Result<TransportResponse> {
            // Force one yield so a peer caller in the same `join` group
            // gets a poll-window in between our register-as-in-flight and
            // our cache-write-on-completion.
            yield_once().await;
            let mut inner = self.0.borrow_mut();
            inner.request_count += 1;
            inner
                .responses
                .pop_front()
                .ok_or_else(|| Error::transport("no mock response"))
        }
    }

    /// Returns Pending once, then Ready.
    fn yield_once() -> impl std::future::Future<Output = ()> {
        use std::pin::Pin;
        use std::task::{Context, Poll};
        struct Y(bool);
        impl std::future::Future for Y {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                if self.0 {
                    Poll::Ready(())
                } else {
                    self.0 = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
        Y(false)
    }

    fn options_response(extensions: &str) -> TransportResponse {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("tus-version"),
            HeaderValue::from_static("1.0.0"),
        );
        headers.insert(
            HeaderName::from_static("tus-extension"),
            HeaderValue::from_str(extensions).unwrap(),
        );
        resp(204, headers, Vec::new())
    }

    /// Builds a canned `TransportResponse` (`http::Response<Vec<u8>>`) from a
    /// status, header map, and body.
    fn resp(status: u16, headers: HeaderMap, body: Vec<u8>) -> TransportResponse {
        let mut response = http::Response::new(body);
        *response.status_mut() = http::StatusCode::from_u16(status).unwrap();
        *response.headers_mut() = headers;
        response
    }

    /// First call fetches; second call within TTL is served from cache and
    /// must not touch the transport.
    #[wasm_bindgen_test]
    async fn second_call_within_ttl_is_cache_hit() {
        let endpoint = "http://test.local/options-cache-hit";
        invalidate(endpoint); // ensure clean slate

        let transport = CountingTransport::new();
        transport.push(options_response("creation,creation-with-upload"));
        let client = Client::with_transport(Url::parse(endpoint).unwrap(), transport.clone());

        let first = get_or_fetch(endpoint, &client).await.expect("first fetch");
        assert!(first.has_extension("creation-with-upload"));
        assert_eq!(transport.request_count(), 1);

        // Second call: no new response queued; if the cache misses, the
        // mock transport returns "no mock response" Err.
        let second = get_or_fetch(endpoint, &client).await.expect("second hit");
        assert_eq!(first, second, "cached value must equal first fetch");
        assert_eq!(
            transport.request_count(),
            1,
            "second call must not trigger a transport request",
        );
    }

    /// Distinct endpoints get distinct cache entries; the cache key is
    /// the endpoint URL (not the transport instance).
    #[wasm_bindgen_test]
    async fn different_endpoints_do_not_share_cache() {
        let ep_a = "http://test.local/options-cache-ep-a";
        let ep_b = "http://test.local/options-cache-ep-b";
        invalidate(ep_a);
        invalidate(ep_b);

        let t_a = CountingTransport::new();
        t_a.push(options_response("creation"));
        let client_a = Client::with_transport(Url::parse(ep_a).unwrap(), t_a.clone());

        let t_b = CountingTransport::new();
        t_b.push(options_response("creation,termination"));
        let client_b = Client::with_transport(Url::parse(ep_b).unwrap(), t_b.clone());

        let info_a = get_or_fetch(ep_a, &client_a).await.expect("fetch A");
        let info_b = get_or_fetch(ep_b, &client_b).await.expect("fetch B");

        assert!(info_a.has_extension("creation"));
        assert!(!info_a.has_extension("termination"));
        assert!(info_b.has_extension("termination"));
        assert_eq!(t_a.request_count(), 1);
        assert_eq!(t_b.request_count(), 1);
    }

    /// `invalidate` drops the entry; the next `get_or_fetch` re-fetches.
    #[wasm_bindgen_test]
    async fn invalidate_forces_refetch() {
        let endpoint = "http://test.local/options-cache-invalidate";
        invalidate(endpoint);

        let transport = CountingTransport::new();
        transport.push(options_response("creation"));
        transport.push(options_response("creation,termination"));
        let client = Client::with_transport(Url::parse(endpoint).unwrap(), transport.clone());

        let first = get_or_fetch(endpoint, &client).await.expect("first");
        assert!(!first.has_extension("termination"));
        assert_eq!(transport.request_count(), 1);

        invalidate(endpoint);

        let second = get_or_fetch(endpoint, &client).await.expect("second");
        assert!(
            second.has_extension("termination"),
            "after invalidate, the next fetch must hit the transport again",
        );
        assert_eq!(transport.request_count(), 2);
    }

    /// Concurrent callers for the same endpoint coalesce: only one OPTIONS
    /// fires; the others wait on the in-flight fetch and receive the same
    /// result. Pre-fix, N concurrent uploads in the same tick all observed
    /// a cache miss and each issued OPTIONS, wasting N-1 round-trips.
    #[wasm_bindgen_test]
    async fn concurrent_calls_coalesce_to_one_fetch() {
        let endpoint = "http://test.local/options-cache-coalesce";
        invalidate(endpoint);

        // Transport with a single response. If both calls miss the
        // coalescing path, the second will see "no mock response".
        let transport = SlowTransport::new();
        transport.push(options_response("creation,creation-with-upload"));
        let client = Client::with_transport(Url::parse(endpoint).unwrap(), transport.clone());

        // Kick off two concurrent fetches. Both enter `lookup_or_register`
        // before either yields to the network; wasm is single-threaded so
        // the second sees an `in_flight` registration from the first and
        // takes the Wait arm. The transport delays its response one tick
        // to keep the in-flight window open across the join.
        let (a, b) = futures::future::join(
            get_or_fetch(endpoint, &client),
            get_or_fetch(endpoint, &client),
        )
        .await;
        let a = a.expect("first call");
        let b = b.expect("second call");
        assert_eq!(a, b, "both callers must see the same ServerCapabilities");
        assert_eq!(
            transport.request_count(),
            1,
            "concurrent miss must coalesce to one OPTIONS, got {}",
            transport.request_count(),
        );
    }

    /// If the leader's `get_or_fetch` future is dropped mid-OPTIONS (e.g.
    /// component unmount or outer cancellation), the in-flight registration
    /// must be cleaned up by the RAII guard so subsequent callers don't
    /// hang forever on a `oneshot::Receiver` whose `Sender` lives in a
    /// `'static Mutex` and would never be dropped.
    ///
    /// Pre-fix repro: drop the leader → in_flight entry leaks → next call
    /// takes `Lookup::Wait`, pushes a `tx` that nobody sends to → `rx.await`
    /// hangs forever.
    #[wasm_bindgen_test]
    async fn dropping_leader_future_does_not_wedge_endpoint() {
        let endpoint = "http://test.local/options-cache-leader-drop";
        invalidate(endpoint);

        let transport = SlowTransport::new();
        transport.push(options_response("creation"));
        let client = Client::with_transport(Url::parse(endpoint).unwrap(), transport.clone());

        // Spawn the leader future, advance it past the in_flight insert
        // (which happens synchronously inside lookup_or_register before the
        // first network yield), then drop it without awaiting completion.
        let mut leader = Box::pin(get_or_fetch(endpoint, &client));
        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let _ = std::future::Future::poll(leader.as_mut(), &mut cx);
        drop(leader); // RAII guard fires here

        assert_eq!(
            transport.request_count(),
            0,
            "the dropped leader yielded before consuming a mock response",
        );

        // A subsequent caller must not hang. The leader's Drop ran
        // finalize_fetch with an error, which clears in_flight. This
        // call therefore takes the Fetch arm and consumes the queued
        // response that the dropped leader never reached.
        let result = get_or_fetch(endpoint, &client)
            .await
            .expect("must not hang");
        assert!(result.has_extension("creation"));
        assert_eq!(
            transport.request_count(),
            1,
            "post-drop caller must issue a fresh OPTIONS request",
        );
    }

    /// Fetch failure must NOT poison the cache; the next call retries.
    #[wasm_bindgen_test]
    async fn fetch_error_does_not_cache() {
        let endpoint = "http://test.local/options-cache-error";
        invalidate(endpoint);

        let transport = CountingTransport::new();
        // First response: 500 Internal; get_or_fetch propagates the error.
        transport.push(resp(500, HeaderMap::new(), b"down".to_vec()));
        // Second response: success.
        transport.push(options_response("creation"));
        let client = Client::with_transport(Url::parse(endpoint).unwrap(), transport.clone());

        let first = get_or_fetch(endpoint, &client).await;
        assert!(first.is_err(), "5xx must propagate, not be cached");

        let second = get_or_fetch(endpoint, &client)
            .await
            .expect("retry succeeds");
        assert!(second.has_extension("creation"));
        assert_eq!(transport.request_count(), 2);
    }
}
