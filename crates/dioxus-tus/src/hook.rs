use std::time::Duration;

use dioxus::prelude::*;
use tus_client::{Client, NewUpload, UploadInfo};
use web_sys::File;

use crate::blob::{blob_size, blob_slice_to_bytes};
use crate::config::{TusConfig, TusStartOptions};
use crate::state::{StateSink, TusError, TusUploadState, UploadStatus};
use crate::transport::GlooNetTransport;

/// Thin adapter so the upload engine can be parameterized over `StateSink`
/// while production keeps using a Dioxus `Signal<TusUploadState>` directly.
impl StateSink for Signal<TusUploadState> {
    fn update<F: FnOnce(&mut TusUploadState)>(&mut self, f: F) {
        let mut w = self.write();
        f(&mut w);
    }

    fn snapshot(&self) -> TusUploadState {
        dioxus::prelude::ReadableExt::read(self).clone()
    }
}

/// Commands sent from `TusUploadHandle` to the running upload task.
#[derive(Debug)]
pub(crate) enum UploadCommand {
    Start {
        file: File,
        options: TusStartOptions,
    },
    Pause,
    Resume,
    Abort,
}

/// Handle returned by [`use_tus_upload`]; use it to start, pause, resume, or abort.
///
/// # Contract
///
/// Calling [`start`](TusUploadHandle::start) while an upload is already running
/// aborts the in-flight upload and starts the new one. The previous upload's
/// server resource is left alive (use a separate DELETE if you need to free it).
#[derive(Clone)]
pub struct TusUploadHandle {
    sender: futures::channel::mpsc::UnboundedSender<UploadCommand>,
    state: Signal<TusUploadState>,
    endpoint: String,
}

impl TusUploadHandle {
    /// Begin uploading `file`. If an upload is already in progress it is aborted first.
    pub fn start(&self, file: File, options: TusStartOptions) {
        // Synchronously stamp `Uploading` so external observers (notably the
        // queue scheduler in `crate::queue`, which polls worker state on a
        // 50ms tick) don't see a stale `Idle`/`Complete`/`Error` value during
        // the create-upload POST round-trip and incorrectly treat the slot
        // as terminal. Once `run_upload` reaches the post-create state.update
        // it will overwrite this with the full coherent value.
        let mut state = self.state;
        {
            let mut w = state.write();
            w.status = UploadStatus::Uploading;
            w.bytes_uploaded = 0;
            w.bytes_total = None;
            w.upload_url = None;
            w.error = None;
        }
        self.send(UploadCommand::Start { file, options });
    }

    /// Begin uploading `file` against an existing TUS resource URL — typically
    /// one created server-side and handed to the client, or persisted from a
    /// prior session. Issues a HEAD against `url` to learn the server-side
    /// offset rather than POSTing to create a new upload.
    ///
    /// Equivalent to setting `options.existing_url` before calling [`start`].
    pub fn start_with_url(&self, file: File, url: impl Into<String>, mut options: TusStartOptions) {
        options.existing_url = Some(url.into());
        self.start(file, options);
    }

    /// Pause at the next chunk boundary.
    pub fn pause(&self) {
        self.send(UploadCommand::Pause);
    }

    /// Resume a paused upload.
    pub fn resume(&self) {
        self.send(UploadCommand::Resume);
    }

    /// Abort the upload and reset state to `Idle`. The server resource is left alive.
    pub fn abort(&self) {
        self.send(UploadCommand::Abort);
    }

    /// Snapshot of the current state. Equivalent to reading the state signal.
    pub fn state(&self) -> TusUploadState {
        dioxus::prelude::ReadableExt::read(&self.state).clone()
    }

    /// Lists every resumable upload persisted in `localStorage` for this
    /// hook's endpoint. Stale (>24h) entries and entries whose stored URL
    /// origin doesn't match the configured endpoint are filtered out.
    ///
    /// Use this on component mount to offer the user a "Resume previous
    /// upload?" prompt. After picking, call [`Self::resume`] with the
    /// re-picked file.
    pub fn scan_resumable(&self) -> Vec<crate::persistence::ResumableEntry> {
        crate::persistence::scan(&self.endpoint)
    }

    /// Resumes an upload from `localStorage` by matching `file` against any
    /// stored entry. Returns `true` when a matching entry was found and the
    /// upload was started; `false` when no match exists (caller should
    /// treat this as a fresh upload and call [`Self::start`] instead).
    ///
    /// Match key derives from `(endpoint, filename, file_size, last_modified)`.
    /// If the file the user re-picks doesn't match — different name, edited,
    /// or browser reports a different `last_modified` — `resume_persisted`
    /// returns `false` and the caller falls back to `start`.
    ///
    /// Distinct from [`Self::resume`], which un-pauses an *active* upload.
    pub fn resume_persisted(&self, file: File, options: TusStartOptions) -> bool {
        let mk = crate::persistence::match_key(
            &self.endpoint,
            &file.name(),
            file.size() as u64,
            file.last_modified(),
        );
        match crate::persistence::get(&self.endpoint, &mk) {
            Some(entry) => {
                self.start_with_url(file, entry.upload_url, options);
                true
            }
            None => false,
        }
    }

    /// Resumes a specific [`ResumableEntry`] (e.g. one the user picked from
    /// a list of multiple resumable uploads). Validates the file's match
    /// key against the entry; returns `false` on mismatch so the caller
    /// can surface a "that doesn't look like the same file" error.
    pub fn resume_entry(
        &self,
        entry: &crate::persistence::ResumableEntry,
        file: File,
        options: TusStartOptions,
    ) -> bool {
        let mk = crate::persistence::match_key(
            &self.endpoint,
            &file.name(),
            file.size() as u64,
            file.last_modified(),
        );
        if mk != entry.match_key
            || !crate::persistence::entry_is_resumable_for_file(
                &self.endpoint,
                entry,
                &file.name(),
                file.size() as u64,
                file.last_modified(),
            )
        {
            return false;
        }
        self.start_with_url(file, entry.upload_url.clone(), options);
        true
    }

    fn send(&self, cmd: UploadCommand) {
        // UnboundedSender is Clone+Send+Sync; .unbounded_send returns Err only
        // if the receiver has been dropped, which means the hook's parent
        // component is unmounted. Silently dropping is correct here.
        let _ = self.sender.unbounded_send(cmd);
    }
}

/// Outcome of a single `run_upload` invocation. The outer command loop matches
/// on this to decide whether to wait for the next command, restart with a new
/// file (when the user picks one mid-upload), or surface an error.
pub(crate) enum RunOutcome {
    Done,
    Aborted,
    Restart {
        file: File,
        options: TusStartOptions,
    },
}

fn validate_server_offset(offset: u64, file_size: u64) -> Result<(), TusError> {
    if offset > file_size {
        // `tus_client::Error`'s variants are `#[non_exhaustive]` and cannot be
        // constructed outside that crate, so build the mapped `TusError`
        // directly (matching the `From<tus_client::Error>` text).
        return Err(TusError::Transport(format!(
            "server offset {offset} exceeds local file size {file_size}"
        )));
    }
    Ok(())
}

fn validate_resume_info(info: &UploadInfo, file_size: u64) -> Result<(), TusError> {
    if let Some(remote) = info.length()
        && remote != file_size
    {
        return Err(TusError::Transport(format!(
            "server length {remote} does not match local file size {file_size}"
        )));
    }
    validate_server_offset(info.offset(), file_size)
}

fn validate_patch_offset(previous: u64, next: u64, file_size: u64) -> Result<(), TusError> {
    validate_server_offset(next, file_size)?;
    if next <= previous {
        return Err(TusError::Transport(format!(
            "server offset {next} did not advance beyond previous offset {previous}"
        )));
    }
    Ok(())
}

/// Outcome of a pre-chunk-loop network call (HEAD / OPTIONS / create-upload).
///
/// The chunk loop's `try_next` polls the command channel between PATCH attempts,
/// but the pre-loop network round trip can take 1-5 seconds on a slow link —
/// during which an `Abort` or `Start` would otherwise be ignored until the
/// chunk loop began. Wrapping each pre-loop request in [`race_pre_loop_request`]
/// keeps Abort/Start responsive in that window.
enum PreLoop<T> {
    /// The network future completed; here is its result.
    Done(T),
    /// User aborted (or channel closed) before the network call returned.
    Aborted,
    /// User started a new file before the network call returned. The outer
    /// command loop re-invokes `run_upload` with the new (file, options).
    Restart {
        file: File,
        options: TusStartOptions,
    },
}

enum Recovery<T> {
    Done(T),
    Aborted,
    Paused,
    Restart {
        file: File,
        options: TusStartOptions,
    },
}

enum BackoffOutcome {
    RetryNow,
    Aborted,
    Paused,
    Restart {
        file: File,
        options: TusStartOptions,
    },
}

fn jittered_retry_delay_ms(config: &TusConfig, attempt: usize) -> (u32, u64) {
    let shift = attempt.min(8) as u32;
    let base_delay = config
        .retry_delay_ms
        .saturating_mul(1u64 << shift)
        .min(u32::MAX as u64);
    // Full jitter: pick uniformly in [0, base_delay]. Without jitter, N
    // concurrent uploads in the same tab failing at the same instant all retry
    // at the exact same millisecond (single-threaded wasm timers fire
    // deterministically), which would DOS a recovering server in a thundering
    // herd.
    let delay = (js_sys::Math::random() * base_delay as f64) as u64;
    (delay as u32, base_delay)
}

async fn wait_retry_backoff(
    delay: u32,
    rx: &mut futures::channel::mpsc::UnboundedReceiver<UploadCommand>,
) -> BackoffOutcome {
    use futures::StreamExt;
    use futures::future::FutureExt;
    let mut timeout = gloo_timers::future::TimeoutFuture::new(delay).fuse();
    let mut next_cmd = rx.next().fuse();
    futures::select! {
        _ = timeout => BackoffOutcome::RetryNow,
        cmd = next_cmd => match cmd {
            None | Some(UploadCommand::Abort) => BackoffOutcome::Aborted,
            Some(UploadCommand::Start { file, options }) => {
                BackoffOutcome::Restart { file, options }
            }
            Some(UploadCommand::Pause) => BackoffOutcome::Paused,
            Some(UploadCommand::Resume) => BackoffOutcome::RetryNow,
        },
    }
}

/// Drives a pre-chunk-loop network future to completion while concurrently
/// polling the command channel. Returns [`PreLoop::Done`] on the network
/// future's normal completion, [`PreLoop::Aborted`] on `Abort`, or
/// [`PreLoop::Restart`] on `Start`.
///
/// Uses `select_biased!` to bias toward the network future when both
/// branches are Ready in the same poll. With a synchronous mock transport
/// (e.g. tests where the response is queued before the request) the
/// network future is Ready on first poll; without bias, an unbiased
/// `select!` would pseudo-randomly take either branch — making `[Start{A},
/// Start{B}]`-style tests flaky because the engine could either complete
/// A's create-upload or restart on B mid-create. The bias makes the
/// pre-loop race semantically equivalent to the existing chunk-loop
/// `try_next` (which only fires when the network is genuinely pending).
///
/// A closed command channel (`cmd = None`) is NOT treated as Abort —
/// closure means "no more commands can interrupt", not "user wants to
/// stop". Just await the network future to completion.
///
/// `Pause` and `Resume` arriving during the pre-loop window are consumed
/// from the channel and applied to `*paused` so the chunk loop sees the
/// right value once it begins. State updates for those transitions are
/// the caller's responsibility post-return — the chunk-loop's first
/// iteration will set `Paused` if `*paused` is true.
async fn race_pre_loop_request<T, F>(
    fut: F,
    rx: &mut futures::channel::mpsc::UnboundedReceiver<UploadCommand>,
    paused: &mut bool,
) -> PreLoop<T>
where
    F: std::future::Future<Output = T>,
{
    use futures::StreamExt;
    use futures::future::FutureExt;
    let mut fut = Box::pin(fut.fuse());
    loop {
        let mut next_cmd = rx.next().fuse();
        futures::select_biased! {
            result = fut => return PreLoop::Done(result),
            cmd = next_cmd => match cmd {
                None => {
                    // Channel closed (parent unmount or `tx` dropped after
                    // queueing this Start). No more commands can interrupt;
                    // just await the network future to completion.
                    return PreLoop::Done(fut.await);
                }
                Some(UploadCommand::Abort) => return PreLoop::Aborted,
                Some(UploadCommand::Start { file, options }) => {
                    return PreLoop::Restart { file, options };
                }
                Some(UploadCommand::Pause) => {
                    *paused = true;
                    // Keep racing the network future; the chunk loop will
                    // honour `*paused` on its first iteration.
                }
                Some(UploadCommand::Resume) => {
                    *paused = false;
                }
            }
        }
    }
}

async fn race_recovery_request<T, F>(
    fut: F,
    rx: &mut futures::channel::mpsc::UnboundedReceiver<UploadCommand>,
) -> Recovery<T>
where
    F: std::future::Future<Output = T>,
{
    use futures::StreamExt;
    use futures::future::FutureExt;
    let mut fut = Box::pin(fut.fuse());
    loop {
        let mut next_cmd = rx.next().fuse();
        futures::select_biased! {
            result = fut => return Recovery::Done(result),
            cmd = next_cmd => match cmd {
                None | Some(UploadCommand::Abort) => return Recovery::Aborted,
                Some(UploadCommand::Start { file, options }) => {
                    return Recovery::Restart { file, options };
                }
                Some(UploadCommand::Pause) => return Recovery::Paused,
                Some(UploadCommand::Resume) => {}
            }
        }
    }
}

/// Returns a reactive upload state signal and a handle to control the upload.
///
/// # Example
/// ```rust,ignore
/// let (state, handle) = use_tus_upload(
///     TusConfig::new("https://tus.example.com/files"),
/// );
/// ```
pub fn use_tus_upload(config: TusConfig) -> (ReadSignal<TusUploadState>, TusUploadHandle) {
    use_tus_upload_with_transport(config, GlooNetTransport)
}

/// Like [`use_tus_upload`], but takes a custom [`tus_client::Transport`]
/// implementation. Use this for testing (mock transports), service-worker
/// integration, or any case where the default browser fetch via
/// [`crate::transport::GlooNetTransport`] isn't the right plumbing.
///
/// `T: Clone + 'static` — each upload run clones the transport so the
/// chunk loop owns one.
pub fn use_tus_upload_with_transport<T>(
    config: TusConfig,
    transport: T,
) -> (ReadSignal<TusUploadState>, TusUploadHandle)
where
    T: tus_client::Transport + Clone + 'static,
{
    let state: Signal<TusUploadState> = use_signal(TusUploadState::default);

    // The receiver isn't `Clone` (required by `use_hook`), so it's stashed
    // in an Option and taken once by the background task.
    let (sender, rx_holder) = use_hook(|| {
        let (tx, rx) = futures::channel::mpsc::unbounded::<UploadCommand>();
        (tx, std::sync::Arc::new(std::sync::Mutex::new(Some(rx))))
    });

    let config = use_hook(|| config);
    let transport = use_hook(|| transport);
    let endpoint = config.endpoint.clone();

    // Spawn a long-lived background task (runs once at mount).
    let rx_holder = rx_holder.clone();
    use_future(move || {
        let mut state = state;
        let config = config.clone();
        let transport = transport.clone();
        let rx_holder = rx_holder.clone();

        async move {
            // Soft-fail if the receiver is already gone (parent re-mount,
            // hot reload, or split rendering): log and abandon the future.
            let rx = match rx_holder.lock().ok().and_then(|mut g| g.take()) {
                Some(rx) => rx,
                None => {
                    tracing::warn!(
                        "use_tus_upload: receiver already taken; future exited \
                         (likely parent remount). Commands sent on this handle \
                         will be queued but unprocessed."
                    );
                    return;
                }
            };
            run_command_loop(config, rx, &mut state, transport).await;
        }
    });

    (
        state.into(),
        TusUploadHandle {
            sender,
            state,
            endpoint,
        },
    )
}

/// Outer command loop: matches each command and dispatches to `run_upload` for `Start`.
///
/// # Resume URL semantics
///
/// `Start` opts into resume only when `options.existing_url` is set. There's
/// deliberately no implicit fallback to a previously-completed-or-failed
/// upload's URL — that footgun caused the same shape of bug twice (once for
/// the Complete path, once for the Err path) and the contract on
/// [`TusUploadHandle::start`] documents it as starting a new upload.
pub(crate) async fn run_command_loop<T, S>(
    config: TusConfig,
    mut rx: futures::channel::mpsc::UnboundedReceiver<UploadCommand>,
    state: &mut S,
    transport: T,
) where
    T: tus_client::Transport + Clone + 'static,
    S: StateSink,
{
    use futures::StreamExt;

    while let Some(cmd) = rx.next().await {
        match cmd {
            UploadCommand::Abort => {
                state.update(|w| {
                    w.status = UploadStatus::Idle;
                    w.bytes_uploaded = 0;
                    w.upload_url = None;
                    w.error = None;
                    w.bytes_total = None;
                });
            }

            UploadCommand::Pause => {
                // Pause/Resume only have effect inside run_upload's chunk
                // loop. Outside of an active upload, just reflect status
                // visually so the UI knows.
                if state.snapshot().is_uploading() {
                    state.update(|w| w.status = UploadStatus::Paused);
                }
            }

            UploadCommand::Resume => {
                if state.snapshot().is_paused() {
                    state.update(|w| w.status = UploadStatus::Uploading);
                }
            }

            UploadCommand::Start { file, options } => {
                // `paused` is scoped per-Start: declared inside this arm, it
                // CANNOT survive across separate Start commands. This is
                // load-bearing — every error/Done exit from run_upload drops
                // this local, so a stuck `paused=true` from a failed run
                // can't leak into the next user-initiated upload.
                //
                // Inside this arm we still need to reset on Restart (line
                // ~408 below) because Restart loops back into run_upload
                // *with the same `paused` reference*. Don't move this
                // declaration outward.
                let mut paused = false;
                // run_upload may return Restart if a new Start arrives mid-loop.
                // Loop until Done/Aborted/Err so the new file actually uploads.
                let mut current = Some((file, options));
                while let Some((f, o)) = current.take() {
                    let result = run_upload(
                        &config,
                        &f,
                        &o,
                        o.existing_url.as_deref(),
                        state,
                        &mut rx,
                        &mut paused,
                        transport.clone(),
                    )
                    .await;
                    match result {
                        Ok(RunOutcome::Done | RunOutcome::Aborted) => break,
                        Ok(RunOutcome::Restart { file, options }) => {
                            tracing::debug!("restarting upload with new file mid-flight");
                            // Restart implies a fresh run. If the prior run
                            // was paused (Start arrived in the paused-await
                            // branch or via try_next while *paused was true)
                            // the next run_upload would otherwise see
                            // paused=true on entry and block on rx.next()
                            // waiting for a Resume the user never sends.
                            paused = false;
                            current = Some((file, options));
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "upload failed");
                            state.update(|w| {
                                w.status = UploadStatus::Error;
                                w.error = Some(e);
                            });
                            break;
                        }
                    }
                }
            }
        }
    }
}

/// Core upload loop. Drives `TusClient::patch_chunk` in a chunk loop,
/// updating state and checking the pause flag between each chunk.
///
/// Free function (not a hook) so the queue helper in `crate::queue` can call
/// it for each entry without nesting Dioxus hook scopes.
///
/// Returns [`RunOutcome::Restart`] when a `Start` command arrives mid-upload
/// — the outer command loop is responsible for re-invoking with the new file.
#[allow(deprecated)]
// dioxus 0.7 re-exports futures' UnboundedReceiver and deprecates
// try_next in favour of a not-yet-stable try_recv. Switch when
// dioxus settles on the replacement.
#[allow(clippy::too_many_arguments)] // engine state is genuinely cross-cutting; bundling
// would make call sites less readable
pub(crate) async fn run_upload<T, S>(
    config: &TusConfig,
    file: &File,
    options: &TusStartOptions,
    existing_url: Option<&str>,
    state: &mut S,
    rx: &mut futures::channel::mpsc::UnboundedReceiver<UploadCommand>,
    paused: &mut bool,
    transport: T,
) -> Result<RunOutcome, TusError>
where
    T: tus_client::Transport,
    S: StateSink,
{
    use futures::StreamExt;

    // `with_max_chunk_size` is intentionally NOT set here: tus-client's chunk
    // size only governs its higher-level upload-loop helpers, which this
    // engine does not call. The engine slices chunks itself (see the
    // `'chunk_loop` below) using `config.chunk_size` and drives one PATCH per
    // slice via `Upload::upload_chunk`.
    let endpoint_url = tus_client::url::Url::parse(&config.endpoint)
        .map_err(|e| TusError::InvalidUrl(e.to_string()))?;
    let mut client = Client::with_transport(endpoint_url, transport)
        .with_max_retries(config.max_retries)
        .with_retry_delay(Duration::from_millis(config.retry_delay_ms));

    // Build the request headers applied to every request for this upload:
    // the bearer token (start-level override wins over config-level) plus any
    // per-upload extra headers. The new client applies a single `HeaderMap`
    // rather than exposing per-header setters.
    let token = options
        .bearer_token_override
        .as_deref()
        .or(config.bearer_token.as_deref());
    let mut header_map = http::HeaderMap::new();
    if let Some(tok) = token {
        // Never echo the token back in the error — only that it was invalid.
        let value = http::HeaderValue::from_str(&format!("Bearer {tok}"))
            .map_err(|_| TusError::Transport("invalid bearer token".into()))?;
        header_map.insert(http::header::AUTHORIZATION, value);
    }
    for (name, value) in &options.extra_headers {
        let header_name = http::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| TusError::Transport(format!("invalid header name `{name}`")))?;
        // The value can carry a credential too, so keep it out of the error.
        let header_value = http::HeaderValue::from_str(value)
            .map_err(|_| TusError::Transport(format!("invalid value for header `{name}`")))?;
        header_map.append(header_name, header_value);
    }
    if !header_map.is_empty() {
        client = client.with_headers(header_map);
    }

    let blob: web_sys::Blob = file.clone().into();
    let file_size = blob_size(&blob);
    let metadata = options.build_metadata(&file.name(), &file.type_());

    // Match key is needed both inside the pre-loop (HEAD 400/404/410 path)
    // and after (initial persist + chunk-loop throttled rewrites). Compute
    // it once up front so the Aborted/Restart pre-loop arms can also clear
    // the prior-session entry — without that, an Abort during the HEAD of
    // a resumed upload silently leaks the stale persistence row, and the
    // next add() of the same file re-attaches the same dead URL.
    let mk = crate::persistence::match_key(
        &config.endpoint,
        &file.name(),
        file_size,
        file.last_modified(),
    );
    let was_resuming = existing_url.is_some();
    let existing_url_log = existing_url.map(crate::persistence::redact_upload_url_for_log);
    let endpoint_log = crate::persistence::redact_endpoint_for_log(&config.endpoint);

    tracing::debug!(
        endpoint = %endpoint_log,
        file_name = %file.name(),
        file_size,
        chunk_size = config.chunk_size,
        existing_url = ?existing_url_log,
        "starting upload",
    );

    // Determine starting offset: resume from existing URL or create a new upload.
    //
    // For new uploads small enough for `creation-with-upload` (per the
    // server's advertised extensions) the entire payload rides on the POST,
    // saving a round trip. For larger files we POST empty + PATCH chunks.
    //
    // Each branch is wrapped in `race_pre_loop_request` so an Abort / Start
    // arriving during the round trip is honoured immediately rather than
    // held until the chunk loop's first `try_next`. On a slow connection a
    // POST round trip is several seconds — long enough that without this
    // race the user's Abort click would feel ignored.
    let create_outcome: PreLoop<Result<(String, u64), TusError>> = if let Some(existing) =
        existing_url
    {
        let head_fut = async {
            let resource = client.upload_at(existing).map_err(TusError::from)?;
            match resource.info().await {
                Ok(info) => {
                    validate_resume_info(&info, file_size)?;
                    let url = crate::persistence::redact_upload_url_for_log(info.url().as_str());
                    tracing::debug!(url = %url, offset = info.offset(), "resumed from existing url");
                    Ok((info.url().to_string(), info.offset()))
                }
                // 400 Bad Request / 410 Gone / 404 Not Found = the persisted
                // URL is stale or invalid, or the server has definitively
                // forgotten the resource. Drop the persisted entry so the user
                // isn't stuck in a "resume → fail" loop until the 24h TTL expires.
                //
                // 401/403 are NOT cleared here — those are commonly transient
                // (token refresh, role propagation) and the bytes are still on
                // the server. Forcing a re-upload from zero is worse than
                // surfacing the auth error. Callers wanting to discard the
                // entry on auth failure should use `TusQueueHandle::remove_item`.
                Err(e) => match &e {
                    tus_client::Error::UnexpectedResponse { status, body, .. }
                        if matches!(status.as_u16(), 400 | 410 | 404) =>
                    {
                        crate::persistence::remove(&mk);
                        Err(TusError::Server {
                            status: status.as_u16(),
                            body: body.clone(),
                        })
                    }
                    _ => Err(TusError::from(e)),
                },
            }
        };
        race_pre_loop_request(head_fut, rx, paused).await
    } else if config.use_creation_with_upload(file_size) {
        let cwu_fut = async {
            let bypass_endpoint_cache =
                options.has_request_specific_headers() || config.bearer_token.is_some();
            let opts_result = if bypass_endpoint_cache {
                let endpoint_log = crate::persistence::redact_endpoint_for_log(&config.endpoint);
                tracing::debug!(
                    endpoint = %endpoint_log,
                    "bypassing endpoint-only OPTIONS cache for authenticated request headers",
                );
                client.server_capabilities().await.map_err(TusError::from)
            } else {
                crate::options_cache::get_or_fetch(&config.endpoint, &client).await
            };
            let cwu_advertised = matches!(
                &opts_result,
                Ok(opts) if opts.has_extension("creation-with-upload"),
            );
            // Enforce Tus-Max-Size before any network call. Without this
            // an oversized file POSTs successfully (server allocates the
            // resource at the declared length), then the first PATCH
            // returns 413 — leaving a dangling resource and surfacing a
            // confusing "unexpected response 413" instead of a clear
            // "file too large".
            if let Ok(opts) = &opts_result
                && let Some(max) = opts.max_size()
                && file_size > max
            {
                return Err(TusError::FileTooLarge {
                    file_size,
                    max_size: max,
                });
            }
            if cwu_advertised {
                let body = crate::blob::blob_slice_to_bytes(&blob, 0, file_size).await?;
                match client
                    .create_upload(NewUpload::with_body(body, &metadata))
                    .await
                {
                    Ok((_, info)) => {
                        let url =
                            crate::persistence::redact_upload_url_for_log(info.url().as_str());
                        tracing::debug!(
                            url = %url,
                            offset = info.offset(),
                            "created upload with body (creation-with-upload)",
                        );
                        return Ok::<(String, u64), TusError>((
                            info.url().to_string(),
                            info.offset(),
                        ));
                    }
                    // Capability mismatch: the server advertised cwu but
                    // rejects the combined POST. The cache is stale (e.g.
                    // operator disabled the extension after the OPTIONS
                    // probe). Invalidate so the next caller re-probes,
                    // then fall back to plain create + PATCH for THIS
                    // request so the user-visible upload still succeeds.
                    Err(e)
                        if matches!(
                            &e,
                            tus_client::Error::UnexpectedResponse { status, .. }
                                if matches!(status.as_u16(), 405 | 412 | 415 | 501)
                        ) =>
                    {
                        tracing::debug!(
                            "create-with-upload rejected; invalidating options cache and falling back to plain create",
                        );
                        crate::options_cache::invalidate(&config.endpoint);
                    }
                    Err(e) => return Err(TusError::from(e)),
                }
            } else if let Err(e) = &opts_result {
                tracing::debug!(error = %e, "OPTIONS probe failed; falling back to plain create");
            }
            let (_, info) = client
                .create_upload(NewUpload::new(file_size, &metadata))
                .await
                .map_err(TusError::from)?;
            let url = crate::persistence::redact_upload_url_for_log(info.url().as_str());
            tracing::debug!(url = %url, "created new upload (plain)");
            Ok((info.url().to_string(), 0u64))
        };
        race_pre_loop_request(cwu_fut, rx, paused).await
    } else {
        let create_fut = async {
            // Best-effort Tus-Max-Size enforcement: if an earlier upload
            // in this session populated the OPTIONS cache, refuse files
            // that exceed it. Authenticated request contexts skip this
            // endpoint-only cache because capabilities may vary by token.
            // We deliberately don't fire an OPTIONS probe here — that would
            // add a round trip to every plain create. Servers that want
            // guaranteed enforcement should either lower the cwu threshold
            // (which always probes) or call client.options() once at app
            // startup.
            if config.bearer_token.is_none()
                && !options.has_request_specific_headers()
                && let Some(opts) = crate::options_cache::peek_fresh(&config.endpoint)
                && let Some(max) = opts.max_size()
                && file_size > max
            {
                return Err(TusError::FileTooLarge {
                    file_size,
                    max_size: max,
                });
            }
            let (_, info) = client
                .create_upload(NewUpload::new(file_size, &metadata))
                .await
                .map_err(TusError::from)?;
            let url = crate::persistence::redact_upload_url_for_log(info.url().as_str());
            tracing::debug!(url = %url, "created new upload");
            Ok::<(String, u64), TusError>((info.url().to_string(), 0u64))
        };
        race_pre_loop_request(create_fut, rx, paused).await
    };

    // Translate a pre-loop interrupt into the corresponding RunOutcome,
    // resetting state to Idle first so the queue scheduler observes a clean
    // transition (the worker signal was sync-stamped Uploading by `start()`
    // — without resetting, an aborted-pre-POST upload would leak that
    // stamp into the next upload).
    let (url, mut offset) = match create_outcome {
        PreLoop::Done(Ok(pair)) => pair,
        PreLoop::Done(Err(e)) => return Err(e),
        PreLoop::Aborted => {
            tracing::debug!("Abort during pre-loop request");
            // If we were attempting to resume, the prior-session entry
            // is still in localStorage and would re-attach on the next
            // add() of the same file. Clear it now so the user isn't
            // stuck in a resume→abort loop until the 24h TTL.
            if was_resuming {
                crate::persistence::remove(&mk);
            }
            state.update(|w| {
                w.status = UploadStatus::Idle;
                w.bytes_uploaded = 0;
                w.upload_url = None;
                w.bytes_total = None;
            });
            return Ok(RunOutcome::Aborted);
        }
        PreLoop::Restart { file, options } => {
            tracing::debug!("Start during pre-loop request; restarting");
            // Restart with a different file: the in-flight resume against
            // the previous file is being abandoned. Clear its persistence
            // entry to match the explicit-Abort semantics above.
            if was_resuming {
                crate::persistence::remove(&mk);
            }
            state.update(|w| {
                w.status = UploadStatus::Idle;
                w.bytes_uploaded = 0;
                w.upload_url = None;
                w.bytes_total = None;
            });
            return Ok(RunOutcome::Restart { file, options });
        }
    };
    validate_server_offset(offset, file_size)?;

    // Bind an upload resource to the resolved URL once; the chunk loop drives
    // every PATCH and recovery HEAD through it (each `Upload` clones the client
    // internally, so `client` stays usable for nothing further here).
    let upload = client.upload_at(&url).map_err(TusError::from)?;

    // If a Pause arrived during the pre-loop network call, `race_pre_loop_request`
    // captured it into `*paused` but did not update state — the chunk-loop's first
    // iteration enters the `if *paused` branch and `rx.next().await`s without
    // touching state. Without this branch the UI sits at the previous sync-stamped
    // `Uploading` for the entire paused window despite the engine being parked.
    let post_create_status = if *paused {
        UploadStatus::Paused
    } else {
        UploadStatus::Uploading
    };
    state.update(|w| {
        w.status = post_create_status;
        w.upload_url = Some(url.clone());
        w.bytes_total = Some(file_size);
        w.bytes_uploaded = offset;
        w.error = None;
    });

    // Persist the (endpoint, file → upload_url) entry so a tab close +
    // reopen can resume. `mk` is hoisted above the pre-loop so the
    // Aborted/Restart arms can clean up a stale prior-session entry.
    let mut last_persisted_offset = offset;
    // Initialise to the current time so the first chunk's throttle predicate
    // doesn't fire spuriously (would otherwise produce a redundant write
    // immediately after the initial persist below).
    let mut last_persisted_at_ms = js_sys::Date::now();
    // Capture the entry's creation timestamp once; throttled rewrites must
    // preserve it so the 24h TTL is creation-relative, not last-write-relative.
    // Without this, a long upload (or a pause→resume cycle that rewrites the
    // entry every 2s) would extend the entry's lifetime indefinitely. If a
    // prior session already persisted this match key, prefer that entry's
    // original timestamp so a Resume across reload doesn't restart the TTL
    // clock either. Only the wasm `get` is gated on target_arch — fall through
    // to "now" on the never-reached native build.
    let upload_started_at_ms = {
        #[cfg(target_arch = "wasm32")]
        {
            crate::persistence::get(&config.endpoint, &mk)
                .map(|e| e.stored_at_ms)
                .unwrap_or_else(js_sys::Date::now)
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            js_sys::Date::now()
        }
    };
    persist_entry(
        &mk,
        &config.endpoint,
        file,
        file_size,
        &url,
        offset,
        upload_started_at_ms,
    );

    // Chunk loop.
    'chunk_loop: while offset < file_size {
        // Check for incoming commands (pause / abort / start) without blocking.
        match rx.try_next() {
            Ok(Some(cmd)) => match cmd {
                UploadCommand::Pause => {
                    *paused = true;
                    state.update(|w| w.status = UploadStatus::Paused);
                }
                UploadCommand::Abort => {
                    crate::persistence::remove(&mk);
                    state.update(|w| {
                        w.status = UploadStatus::Idle;
                        w.bytes_uploaded = 0;
                        w.upload_url = None;
                        w.bytes_total = None;
                    });
                    return Ok(RunOutcome::Aborted);
                }
                UploadCommand::Resume => {
                    *paused = false;
                    state.update(|w| w.status = UploadStatus::Uploading);
                }
                UploadCommand::Start { file, options } => {
                    // The doc on `start()` is "if an upload is already in
                    // progress, it is aborted first" — surface the new
                    // (file, options) up to the outer command loop so the
                    // restart actually happens. Reset state for the new run.
                    tracing::debug!("Start mid-upload received; restarting");
                    crate::persistence::remove(&mk);
                    state.update(|w| {
                        w.status = UploadStatus::Idle;
                        w.bytes_uploaded = 0;
                        w.upload_url = None;
                        w.bytes_total = None;
                    });
                    return Ok(RunOutcome::Restart { file, options });
                }
            },
            Ok(None) => {
                // Channel closed: every clone of the sender has been dropped.
                // Usually a parent unmount, but possible if the consumer holds
                // only the state signal and dropped all `TusUploadHandle`s.
                // Reset state symmetrically with the explicit Abort arm so a
                // surviving state observer doesn't see a dangling Uploading.
                tracing::debug!("command channel closed during chunk loop");
                crate::persistence::remove(&mk);
                state.update(|w| {
                    w.status = UploadStatus::Idle;
                    w.bytes_uploaded = 0;
                    w.upload_url = None;
                    w.bytes_total = None;
                });
                return Ok(RunOutcome::Aborted);
            }
            Err(_) => {
                // No command pending — fall through to PATCH the next chunk.
            }
        }

        // If paused, wait for a Resume / Abort / Start command.
        if *paused {
            if let Some(cmd) = rx.next().await {
                match cmd {
                    UploadCommand::Resume => {
                        *paused = false;
                        state.update(|w| w.status = UploadStatus::Uploading);
                    }
                    UploadCommand::Abort => {
                        crate::persistence::remove(&mk);
                        state.update(|w| {
                            w.status = UploadStatus::Idle;
                            w.bytes_uploaded = 0;
                            w.upload_url = None;
                            w.bytes_total = None;
                        });
                        return Ok(RunOutcome::Aborted);
                    }
                    UploadCommand::Start { file, options } => {
                        // Start while paused: same restart-with-new-file
                        // contract as the running case above.
                        tracing::debug!("Start while paused; restarting");
                        crate::persistence::remove(&mk);
                        state.update(|w| {
                            w.status = UploadStatus::Idle;
                            w.bytes_uploaded = 0;
                            w.upload_url = None;
                            w.bytes_total = None;
                        });
                        return Ok(RunOutcome::Restart { file, options });
                    }
                    UploadCommand::Pause => {
                        // Already paused; idempotent.
                    }
                }
            } else {
                // Channel closed while paused. Reset state so a surviving
                // observer (e.g. queue scheduler holding the worker signal
                // after the handle was dropped) doesn't see a dangling Paused.
                crate::persistence::remove(&mk);
                state.update(|w| {
                    w.status = UploadStatus::Idle;
                    w.bytes_uploaded = 0;
                    w.upload_url = None;
                    w.bytes_total = None;
                });
                return Ok(RunOutcome::Aborted);
            }
            continue;
        }

        // Defensive clamp: `with_chunk_size` already clamps to >= 1, but
        // the field is `pub` so a downstream consumer could set 0 directly
        // (or deserialize a malformed config). A 0-byte chunk would send
        // an empty PATCH, the server would echo the same offset, and the
        // loop would spin forever. Pin the floor to 1 here so the
        // invariant holds regardless of how the field was reached.
        let chunk_bytes = config.chunk_size.max(1) as u64;
        let end = (offset + chunk_bytes).min(file_size);
        let chunk = blob_slice_to_bytes(&blob, offset, end).await?;

        // Retry loop for transient failures. Clone is paid only when a retry
        // is needed (rare); the success path moves the chunk by reference
        // through tus-client's Vec parameter.
        //
        // The retry backoff races against the command channel so Abort,
        // Start, and Pause aren't held until the next sleep elapses —
        // worst-case delay at attempt=8 with default config is ~51s.
        let new_offset: Option<u64> = {
            let mut attempt = 0usize;
            let mut current_chunk = chunk;
            'retry: loop {
                let chunk_for_attempt = if attempt == 0 {
                    std::mem::take(&mut current_chunk)
                } else {
                    current_chunk.clone()
                };
                match upload.upload_chunk(offset, chunk_for_attempt).await {
                    Ok(o) => {
                        validate_patch_offset(offset, o, file_size)?;
                        break 'retry Some(o);
                    }
                    Err(e) if attempt < config.max_retries => {
                        if !crate::retry::is_retryable_error(&e) {
                            return Err(e.into());
                        }
                        let remote = 'recover_head: loop {
                            match race_recovery_request(upload.info(), rx).await {
                                Recovery::Done(Ok(remote)) => break 'recover_head remote,
                                Recovery::Done(Err(e)) => {
                                    if !crate::retry::is_retryable_error(&e)
                                        || attempt >= config.max_retries
                                    {
                                        return Err(e.into());
                                    }
                                    let (delay, base_delay) =
                                        jittered_retry_delay_ms(config, attempt);
                                    attempt += 1;
                                    tracing::debug!(
                                        attempt,
                                        delay_ms = delay,
                                        base_delay_ms = base_delay,
                                        "retrying recovery HEAD after transient error"
                                    );
                                    match wait_retry_backoff(delay, rx).await {
                                        BackoffOutcome::RetryNow => continue 'recover_head,
                                        BackoffOutcome::Aborted => {
                                            crate::persistence::remove(&mk);
                                            state.update(|w| {
                                                w.status = UploadStatus::Idle;
                                                w.bytes_uploaded = 0;
                                                w.upload_url = None;
                                                w.bytes_total = None;
                                            });
                                            return Ok(RunOutcome::Aborted);
                                        }
                                        BackoffOutcome::Paused => {
                                            *paused = true;
                                            state.update(|w| w.status = UploadStatus::Paused);
                                            break 'retry None;
                                        }
                                        BackoffOutcome::Restart { file, options } => {
                                            crate::persistence::remove(&mk);
                                            state.update(|w| {
                                                w.status = UploadStatus::Idle;
                                                w.bytes_uploaded = 0;
                                                w.upload_url = None;
                                                w.bytes_total = None;
                                            });
                                            return Ok(RunOutcome::Restart { file, options });
                                        }
                                    }
                                }
                                Recovery::Aborted => {
                                    crate::persistence::remove(&mk);
                                    state.update(|w| {
                                        w.status = UploadStatus::Idle;
                                        w.bytes_uploaded = 0;
                                        w.upload_url = None;
                                        w.bytes_total = None;
                                    });
                                    return Ok(RunOutcome::Aborted);
                                }
                                Recovery::Paused => {
                                    *paused = true;
                                    state.update(|w| w.status = UploadStatus::Paused);
                                    break 'retry None;
                                }
                                Recovery::Restart { file, options } => {
                                    crate::persistence::remove(&mk);
                                    state.update(|w| {
                                        w.status = UploadStatus::Idle;
                                        w.bytes_uploaded = 0;
                                        w.upload_url = None;
                                        w.bytes_total = None;
                                    });
                                    return Ok(RunOutcome::Restart { file, options });
                                }
                            }
                        };
                        validate_resume_info(&remote, file_size)?;
                        if remote.offset() < offset {
                            return Err(TusError::Transport(format!(
                                "server offset {} is below local retry offset {}",
                                remote.offset(),
                                offset,
                            )));
                        }
                        if remote.offset() > offset {
                            tracing::debug!(
                                previous_offset = offset,
                                remote_offset = remote.offset(),
                                "retryable PATCH failure advanced remotely; continuing from HEAD offset",
                            );
                            break 'retry Some(remote.offset());
                        }
                        // Repopulate current_chunk for the next retry. On the
                        // first retry we already cloned above into
                        // chunk_for_attempt; current_chunk is empty after the
                        // initial take, so refill from a one-shot read.
                        if current_chunk.is_empty() {
                            current_chunk = blob_slice_to_bytes(&blob, offset, end).await?;
                        }
                        let (delay, base_delay) = jittered_retry_delay_ms(config, attempt);
                        attempt += 1;
                        tracing::debug!(
                            attempt,
                            delay_ms = delay,
                            base_delay_ms = base_delay,
                            "retrying after transient error"
                        );
                        match wait_retry_backoff(delay, rx).await {
                            BackoffOutcome::RetryNow => {
                                // Backoff complete; loop back and retry the patch.
                            }
                            BackoffOutcome::Aborted => {
                                tracing::debug!("Abort during retry backoff");
                                crate::persistence::remove(&mk);
                                state.update(|w| {
                                    w.status = UploadStatus::Idle;
                                    w.bytes_uploaded = 0;
                                    w.upload_url = None;
                                    w.bytes_total = None;
                                });
                                return Ok(RunOutcome::Aborted);
                            }
                            BackoffOutcome::Restart { file, options } => {
                                tracing::debug!("Start during retry backoff; restarting");
                                crate::persistence::remove(&mk);
                                state.update(|w| {
                                    w.status = UploadStatus::Idle;
                                    w.bytes_uploaded = 0;
                                    w.upload_url = None;
                                    w.bytes_total = None;
                                });
                                return Ok(RunOutcome::Restart { file, options });
                            }
                            BackoffOutcome::Paused => {
                                *paused = true;
                                state.update(|w| w.status = UploadStatus::Paused);
                                // Break the retry loop; the chunk-loop top
                                // will see *paused and enter the wait branch.
                                break 'retry None;
                            }
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        };

        let Some(new_offset) = new_offset else {
            // Backoff was interrupted by Pause; chunk-loop top handles it.
            continue 'chunk_loop;
        };
        offset = new_offset;
        state.update(|w| w.bytes_uploaded = offset);

        // Throttled persistence: write to localStorage at most every 2s OR
        // every 5% of file_size. Per-PATCH writes (8MB chunks at LAN speed
        // = 50 writes/sec) saturate localStorage's synchronous main-thread
        // path.
        let now_ms = js_sys::Date::now();
        let bytes_delta = offset.saturating_sub(last_persisted_offset);
        let progress_threshold = (file_size / 20).max(1); // 5%
        if now_ms - last_persisted_at_ms > 2_000.0
            || bytes_delta >= progress_threshold
            || offset == file_size
        {
            persist_entry(
                &mk,
                &config.endpoint,
                file,
                file_size,
                &url,
                offset,
                upload_started_at_ms,
            );
            last_persisted_offset = offset;
            last_persisted_at_ms = now_ms;
        }
    }

    // Upload complete: drop the resumable entry; nothing to resume from.
    crate::persistence::remove(&mk);

    state.update(|w| {
        w.status = UploadStatus::Complete;
        w.bytes_uploaded = file_size;
    });
    tracing::debug!(file_name = %file.name(), "upload complete");
    Ok(RunOutcome::Done)
}

/// Persists the in-flight upload's match-key entry.
///
/// Errors are logged but never surfaced — a localStorage failure (quota,
/// sandbox) shouldn't abort the upload itself, just disable resume across
/// reload for this file. Both call sites previously discarded the return
/// value with `let _ =`; collapsing to `()` avoids the misleading
/// "errors-can-be-handled" affordance the `Result` return implied.
fn persist_entry(
    match_key: &str,
    endpoint: &str,
    file: &File,
    file_size: u64,
    upload_url: &str,
    bytes_uploaded: u64,
    stored_at_ms: f64,
) {
    let entry = crate::persistence::ResumableEntry {
        match_key: match_key.to_string(),
        endpoint: endpoint.to_string(),
        filename: file.name(),
        file_size,
        last_modified: file.last_modified(),
        upload_url: upload_url.to_string(),
        bytes_uploaded,
        stored_at_ms,
    };
    if let Err(e) = crate::persistence::put(&entry) {
        tracing::warn!(error = %e, "failed to persist resumable entry");
    }
}

// =====================================================================
// Engine tests — Layer 2 wasm-bindgen-test, run via `wasm-pack test`.
//
// These cover the bug-prone command-handling and persistence paths that
// can't be exercised on native (web_sys::File, gloo_timers, localStorage).
// MockTransport is used in lieu of a real `tus-server` so the tests are
// fully deterministic and don't require a running server.
// =====================================================================
#[cfg(test)]
mod engine_tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    use async_trait::async_trait;
    use futures::channel::mpsc;
    use http::{HeaderMap, HeaderName, HeaderValue, Method};
    use wasm_bindgen_test::*;

    use tus_client::{Error, Transport, TransportBody, TransportRequest, TransportResponse};

    wasm_bindgen_test_configure!(run_in_browser);

    /// Captures every state mutation; replaces `Signal<TusUploadState>`
    /// outside Dioxus.
    #[derive(Default, Clone)]
    struct CapturedSink(Rc<RefCell<CapturedSinkInner>>);

    #[derive(Default)]
    struct CapturedSinkInner {
        history: Vec<TusUploadState>,
        current: TusUploadState,
    }

    impl StateSink for CapturedSink {
        fn update<F: FnOnce(&mut TusUploadState)>(&mut self, f: F) {
            let mut inner = self.0.borrow_mut();
            f(&mut inner.current);
            let snap = inner.current.clone();
            inner.history.push(snap);
        }
        fn snapshot(&self) -> TusUploadState {
            self.0.borrow().current.clone()
        }
    }

    impl CapturedSink {
        fn current(&self) -> TusUploadState {
            self.0.borrow().current.clone()
        }
    }

    /// Records every TransportRequest and serves canned responses in order.
    /// On wasm, async-trait is `?Send` so `Rc<RefCell>` is fine.
    #[derive(Clone, Default)]
    struct MockTransport(Rc<RefCell<MockTransportInner>>);

    #[derive(Default)]
    struct MockTransportInner {
        requests: Vec<TransportRequest>,
        responses: VecDeque<MockResp>,
        /// Optional artificial delay applied before each response. Used to
        /// simulate slow round trips so `race_pre_loop_request` has a
        /// pending future to lose against an Abort.
        delay_ms: u32,
    }

    enum MockResp {
        Ok(TransportResponse),
        Err(Error),
        DelayedOk(TransportResponse, u32),
    }

    impl MockTransport {
        fn new() -> Self {
            Self::default()
        }
        fn push_response(&self, resp: TransportResponse) {
            self.0.borrow_mut().responses.push_back(MockResp::Ok(resp));
        }
        fn push_delayed_response(&self, resp: TransportResponse, delay_ms: u32) {
            self.0
                .borrow_mut()
                .responses
                .push_back(MockResp::DelayedOk(resp, delay_ms));
        }
        fn requests(&self) -> Vec<TransportRequest> {
            self.0.borrow().requests.clone()
        }
        fn with_delay_ms(self, ms: u32) -> Self {
            self.0.borrow_mut().delay_ms = ms;
            self
        }
    }

    #[async_trait(?Send)]
    impl Transport for MockTransport {
        async fn send(&self, req: TransportRequest) -> tus_client::Result<TransportResponse> {
            // Read the delay before pushing the request; otherwise the
            // borrow conflicts with the awaited delay future.
            let delay_ms = self.0.borrow().delay_ms;
            self.0.borrow_mut().requests.push(req);
            if delay_ms > 0 {
                gloo_timers::future::TimeoutFuture::new(delay_ms).await;
            }
            let resp = self
                .0
                .borrow_mut()
                .responses
                .pop_front()
                .unwrap_or_else(|| MockResp::Err(Error::transport("no mock response")));
            match resp {
                MockResp::Ok(r) => Ok(r),
                MockResp::Err(e) => Err(e),
                MockResp::DelayedOk(r, delay_ms) => {
                    gloo_timers::future::TimeoutFuture::new(delay_ms).await;
                    Ok(r)
                }
            }
        }
    }

    /// Builds a canned `TransportResponse` (now an `http::Response<Vec<u8>>`
    /// alias rather than a struct) from a status, header map, and body.
    fn resp(status: u16, headers: HeaderMap, body: Vec<u8>) -> TransportResponse {
        let mut response = http::Response::new(body);
        *response.status_mut() = http::StatusCode::from_u16(status).unwrap();
        *response.headers_mut() = headers;
        response
    }

    fn header_map(headers: &[(&str, &str)]) -> HeaderMap {
        let mut m = HeaderMap::new();
        for (k, v) in headers {
            m.insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        m
    }

    fn make_file(name: &str, content: &[u8]) -> web_sys::File {
        use js_sys::{Array, Uint8Array};
        let uint8 = Uint8Array::from(content);
        let array = Array::new();
        array.push(&uint8);
        let options = web_sys::FilePropertyBag::new();
        options.set_type("application/octet-stream");
        web_sys::File::new_with_u8_array_sequence_and_options(&array, name, &options)
            .expect("File creation failed")
    }

    fn ok_201_create(loc: &str) -> TransportResponse {
        resp(
            201,
            header_map(&[("Location", loc), ("Tus-Resumable", "1.0.0")]),
            vec![],
        )
    }

    fn ok_201_create_with_offset(loc: &str, offset: u64) -> TransportResponse {
        resp(
            201,
            header_map(&[
                ("Location", loc),
                ("Upload-Offset", &offset.to_string()),
                ("Tus-Resumable", "1.0.0"),
            ]),
            vec![],
        )
    }

    fn ok_200_head(offset: u64, length: u64) -> TransportResponse {
        resp(
            200,
            header_map(&[
                ("Upload-Offset", &offset.to_string()),
                ("Upload-Length", &length.to_string()),
                ("Tus-Resumable", "1.0.0"),
            ]),
            vec![],
        )
    }

    fn ok_204_patch(offset: u64) -> TransportResponse {
        resp(
            204,
            header_map(&[
                ("Upload-Offset", &offset.to_string()),
                ("Tus-Resumable", "1.0.0"),
            ]),
            vec![],
        )
    }

    fn err_status(status: u16, body: &[u8]) -> TransportResponse {
        resp(status, HeaderMap::new(), body.to_vec())
    }

    fn options_response(extensions: &str) -> TransportResponse {
        resp(
            204,
            header_map(&[("Tus-Version", "1.0.0"), ("Tus-Extension", extensions)]),
            vec![],
        )
    }

    fn options_response_with_max(extensions: &str, max_size: u64) -> TransportResponse {
        resp(
            204,
            header_map(&[
                ("Tus-Version", "1.0.0"),
                ("Tus-Extension", extensions),
                ("Tus-Max-Size", &max_size.to_string()),
            ]),
            vec![],
        )
    }

    /// Configures the cwu threshold so plain POST + PATCH is always taken
    /// (no OPTIONS probe). Keeps request shapes simple in assertions.
    fn test_config(endpoint: &str) -> TusConfig {
        TusConfig::new(endpoint)
            .with_chunk_size(1024 * 1024)
            .with_creation_with_upload_threshold(0)
            .with_max_retries(0)
            .with_retry_delay_ms(50)
    }

    fn clear_persistence() {
        // Wipe entries with our namespace so tests don't collide.
        if let Some(window) = web_sys::window()
            && let Ok(Some(storage)) = window.local_storage()
        {
            let len = storage.length().unwrap_or(0);
            let mut to_remove = Vec::new();
            for i in 0..len {
                if let Ok(Some(key)) = storage.key(i)
                    && key.starts_with(crate::persistence::STORAGE_KEY_PREFIX)
                {
                    to_remove.push(key);
                }
            }
            for k in to_remove {
                let _ = storage.remove_item(&k);
            }
        }
    }

    /// Regression for the upload_url leak class. Drives `run_command_loop`
    /// with [Start{A}, Start{B}] where A's PATCH returns 403. Without
    /// Option B applied, the second Start would resolve `existing_url` to
    /// A's URL via the now-deleted outer-scope fallback and HEAD `/a`. With
    /// the fix, the second Start does a fresh POST.
    ///
    /// Sends Start{B} via a delayed `spawn_local` so it lands AFTER A's
    /// chunk loop has begun (and thus after its first `try_next`) — same
    /// pattern as `restart_via_pause_uploads_new_file`. Without this, the
    /// chunk loop's first try_next would consume Start{B} mid-flight and
    /// the engine would Restart instead of letting A run to its 403
    /// failure.
    #[wasm_bindgen_test]
    async fn err_path_does_not_redirect_next_start_to_old_url() {
        clear_persistence();
        let transport = MockTransport::new();
        // A: POST 201 -> PATCH 403 (non-retryable)
        transport.push_response(ok_201_create("http://test.local/files/a-id"));
        transport.push_response(err_status(403, b"forbidden"));
        // B: POST 201 -> PATCH 204
        transport.push_response(ok_201_create("http://test.local/files/b-id"));
        transport.push_response(ok_204_patch(11));

        let file_a = make_file("a.bin", b"hello world");
        let file_b = make_file("b.bin", b"hello world");

        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file: file_a,
            options: TusStartOptions::default(),
        })
        .unwrap();

        // Send Start{B} after a short delay so A's POST+PATCH(403)+Err can
        // unwind first. The clone keeps the channel open while B runs.
        let tx_for_b = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(50).await;
            let _ = tx_for_b.unbounded_send(UploadCommand::Start {
                file: file_b,
                options: TusStartOptions::default(),
            });
            // Hold past B's expected POST + PATCH window.
            gloo_timers::future::TimeoutFuture::new(200).await;
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(
            test_config("http://test.local/files"),
            rx,
            &mut sink,
            transport.clone(),
        )
        .await;

        let reqs = transport.requests();
        let methods: Vec<_> = reqs
            .iter()
            .map(|r| (r.method().clone(), r.uri().to_string()))
            .collect();
        assert_eq!(reqs.len(), 4, "expected 4 requests, got {methods:?}");
        assert_eq!(
            reqs[0].method(),
            Method::POST,
            "1st: POST(A); got {methods:?}"
        );
        assert_eq!(reqs[1].method(), Method::PATCH, "2nd: PATCH(A)");
        assert!(
            reqs[1].uri().path().contains("a-id"),
            "PATCH(A) targets /a-id; got {}",
            reqs[1].uri()
        );
        assert_eq!(
            reqs[2].method(),
            Method::POST,
            "3rd: POST(B), NOT HEAD — second Start must not resume from A's failed URL"
        );
        assert_eq!(reqs[3].method(), Method::PATCH, "4th: PATCH(B)");
        assert!(
            reqs[3].uri().path().contains("b-id"),
            "PATCH(B) targets /b-id; got {}",
            reqs[3].uri()
        );
    }

    /// Pause-then-Start-mid-flight: send Start{A}, Pause, then Start{B} via
    /// a delayed task so tx survives long enough for B to PATCH. Verifies
    /// (a) Restart doesn't carry A's URL into B's run, and (b) the prior
    /// Pause does NOT leak into B's chunk loop (B uploads without needing
    /// any subsequent Resume command). Pre-fix to the paused-leak bug
    /// (run_command_loop forgot to reset `paused` on Restart), B would
    /// block forever waiting for a Resume in the paused-await branch.
    #[wasm_bindgen_test]
    async fn restart_via_pause_uploads_new_file() {
        clear_persistence();
        let transport = MockTransport::new();
        transport.push_response(ok_201_create("http://test.local/files/a-id"));
        // No PATCH(A) is expected — Pause arrives first.
        transport.push_response(ok_201_create("http://test.local/files/b-id"));
        transport.push_response(ok_204_patch(5));

        let file_a = make_file("a.bin", b"hello");
        let file_b = make_file("b.bin", b"world");

        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file: file_a,
            options: TusStartOptions::default(),
        })
        .unwrap();
        tx.unbounded_send(UploadCommand::Pause).unwrap();

        // Send Start{B} via a delayed task so the channel stays open while
        // B's chunk loop runs (try_next on a closed-empty channel returns
        // Ok(None) and the engine treats that as Aborted). The clone keeps
        // the channel alive past the original tx drop below.
        let tx_for_start_b = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(50).await;
            let _ = tx_for_start_b.unbounded_send(UploadCommand::Start {
                file: file_b,
                options: TusStartOptions::default(),
            });
            // Hold tx_for_start_b until B has had time to POST + PATCH.
            // 250ms is generous against the mock transport's near-zero latency.
            gloo_timers::future::TimeoutFuture::new(250).await;
            // Drop on scope exit closes the channel so run_command_loop returns.
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(
            test_config("http://test.local/files"),
            rx,
            &mut sink,
            transport.clone(),
        )
        .await;

        let reqs = transport.requests();
        let methods: Vec<_> = reqs
            .iter()
            .map(|r| (r.method().clone(), r.uri().to_string()))
            .collect();
        assert_eq!(
            reqs.len(),
            3,
            "expected POST(A), POST(B), PATCH(B); got {methods:?}"
        );
        assert_eq!(reqs[0].method(), Method::POST);
        assert!(
            reqs[0].uri().path().ends_with("/files"),
            "POST goes to base endpoint"
        );
        assert_eq!(reqs[1].method(), Method::POST);
        assert!(
            reqs[1].uri().path().ends_with("/files"),
            "POST(B) goes to base endpoint"
        );
        assert_eq!(reqs[2].method(), Method::PATCH);
        assert!(
            reqs[2].uri().path().contains("b-id"),
            "PATCH targets /b-id; got {}",
            reqs[2].uri()
        );
        assert_eq!(sink.current().status, UploadStatus::Complete);
    }

    #[wasm_bindgen_test]
    async fn config_bearer_token_bypasses_endpoint_options_cache() {
        clear_persistence();
        let endpoint = "http://test.local/files/auth-cache";
        crate::options_cache::invalidate(endpoint);
        let file = make_file("auth.bin", b"hello");

        let config_a = TusConfig::new(endpoint)
            .with_bearer_token("token-a")
            .with_chunk_size(1024)
            .with_creation_with_upload_threshold(1024)
            .with_max_retries(0)
            .with_retry_delay_ms(0);
        let transport_a = MockTransport::new();
        transport_a.push_response(options_response("creation,creation-with-upload"));
        transport_a.push_response(ok_201_create_with_offset(
            "http://test.local/files/auth-cache/a-id",
            5,
        ));
        let (_tx_a, mut rx_a) = mpsc::unbounded::<UploadCommand>();
        let mut sink_a = CapturedSink::default();
        let mut paused_a = false;
        let outcome_a = run_upload(
            &config_a,
            &file,
            &TusStartOptions::default(),
            None,
            &mut sink_a,
            &mut rx_a,
            &mut paused_a,
            transport_a.clone(),
        )
        .await
        .expect("first upload should complete");
        assert!(matches!(outcome_a, RunOutcome::Done));
        assert_eq!(sink_a.current().status, UploadStatus::Complete);

        let config_b = TusConfig::new(endpoint)
            .with_bearer_token("token-b")
            .with_chunk_size(1024)
            .with_creation_with_upload_threshold(1024)
            .with_max_retries(0)
            .with_retry_delay_ms(0);
        let transport_b = MockTransport::new();
        transport_b.push_response(options_response("creation"));
        transport_b.push_response(ok_201_create("http://test.local/files/auth-cache/b-id"));
        transport_b.push_response(ok_204_patch(5));
        let (_tx_b, mut rx_b) = mpsc::unbounded::<UploadCommand>();
        let mut sink_b = CapturedSink::default();
        let mut paused_b = false;
        let outcome_b = run_upload(
            &config_b,
            &file,
            &TusStartOptions::default(),
            None,
            &mut sink_b,
            &mut rx_b,
            &mut paused_b,
            transport_b.clone(),
        )
        .await
        .expect("second auth context must fetch its own OPTIONS");

        assert!(matches!(outcome_b, RunOutcome::Done));
        let methods: Vec<_> = transport_b
            .requests()
            .iter()
            .map(|r| r.method().clone())
            .collect();
        assert_eq!(methods, vec![Method::OPTIONS, Method::POST, Method::PATCH]);
    }

    #[wasm_bindgen_test]
    async fn config_bearer_token_bypasses_plain_create_max_size_cache() {
        clear_persistence();
        let endpoint = "http://test.local/files/plain-auth-cache";
        crate::options_cache::invalidate(endpoint);

        let priming_file = make_file("tiny.bin", b"x");
        let config_a = TusConfig::new(endpoint)
            .with_chunk_size(1024)
            .with_creation_with_upload_threshold(1024)
            .with_max_retries(0)
            .with_retry_delay_ms(0);
        let transport_a = MockTransport::new();
        transport_a.push_response(options_response_with_max(
            "creation,creation-with-upload",
            1,
        ));
        transport_a.push_response(ok_201_create_with_offset(
            "http://test.local/files/plain-auth-cache/a-id",
            1,
        ));
        let (_tx_a, mut rx_a) = mpsc::unbounded::<UploadCommand>();
        let mut sink_a = CapturedSink::default();
        let mut paused_a = false;
        run_upload(
            &config_a,
            &priming_file,
            &TusStartOptions::default(),
            None,
            &mut sink_a,
            &mut rx_a,
            &mut paused_a,
            transport_a,
        )
        .await
        .expect("priming upload should cache Tus-Max-Size");

        let file = make_file("large-auth.bin", b"abcdef");
        let config_b = TusConfig::new(endpoint)
            .with_bearer_token("token-b")
            .with_chunk_size(1024)
            .with_creation_with_upload_threshold(0)
            .with_max_retries(0)
            .with_retry_delay_ms(0);
        let transport_b = MockTransport::new();
        transport_b.push_response(ok_201_create(
            "http://test.local/files/plain-auth-cache/b-id",
        ));
        transport_b.push_response(ok_204_patch(6));
        let (_tx_b, mut rx_b) = mpsc::unbounded::<UploadCommand>();
        let mut sink_b = CapturedSink::default();
        let mut paused_b = false;

        run_upload(
            &config_b,
            &file,
            &TusStartOptions::default(),
            None,
            &mut sink_b,
            &mut rx_b,
            &mut paused_b,
            transport_b.clone(),
        )
        .await
        .expect("config auth must not reuse endpoint-only max-size cache");

        assert_eq!(sink_b.current().status, UploadStatus::Complete);
        let methods: Vec<_> = transport_b
            .requests()
            .iter()
            .map(|r| r.method().clone())
            .collect();
        assert_eq!(methods, vec![Method::POST, Method::PATCH]);
    }

    /// HEAD returning 410 Gone clears the persisted localStorage entry so
    /// the user doesn't get stuck offering "resume" of a server-deleted
    /// resource on the next page load.
    #[wasm_bindgen_test]
    async fn head_410_clears_persisted_entry() {
        clear_persistence();
        let endpoint = "http://test.local/410-test";
        let file = make_file("expired.bin", b"contents");
        let mk = crate::persistence::match_key(
            endpoint,
            "expired.bin",
            file.size() as u64,
            file.last_modified(),
        );
        // Pre-populate the entry the engine should clear.
        let entry = crate::persistence::ResumableEntry {
            match_key: mk.clone(),
            endpoint: endpoint.into(),
            filename: "expired.bin".into(),
            file_size: file.size() as u64,
            last_modified: file.last_modified(),
            upload_url: "http://test.local/410-test/expired-id".into(),
            bytes_uploaded: 4,
            stored_at_ms: js_sys::Date::now(),
        };
        crate::persistence::put(&entry).expect("seed persistence");
        assert!(
            crate::persistence::get(endpoint, &mk).is_some(),
            "precondition: entry seeded"
        );

        let transport = MockTransport::new();
        transport.push_response(err_status(410, b"gone"));

        let options = TusStartOptions {
            existing_url: Some("http://test.local/410-test/expired-id".into()),
            ..Default::default()
        };

        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start { file, options })
            .unwrap();
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport).await;

        assert!(
            crate::persistence::get(endpoint, &mk).is_none(),
            "410 Gone must clear the persisted entry"
        );
        match sink.current().status {
            UploadStatus::Error => {}
            other => panic!("expected Error after 410, got {other:?}"),
        }
    }

    #[wasm_bindgen_test]
    async fn head_400_clears_persisted_entry() {
        clear_persistence();
        let endpoint = "http://test.local/400-test";
        let file = make_file("bad-resume.bin", b"contents");
        let mk = crate::persistence::match_key(
            endpoint,
            "bad-resume.bin",
            file.size() as u64,
            file.last_modified(),
        );
        let upload_url = "http://test.local/400-test/bad-id";
        let entry = crate::persistence::ResumableEntry {
            match_key: mk.clone(),
            endpoint: endpoint.into(),
            filename: "bad-resume.bin".into(),
            file_size: file.size() as u64,
            last_modified: file.last_modified(),
            upload_url: upload_url.into(),
            bytes_uploaded: 4,
            stored_at_ms: js_sys::Date::now(),
        };
        crate::persistence::put(&entry).expect("seed persistence");
        assert!(crate::persistence::get(endpoint, &mk).is_some());

        let transport = MockTransport::new();
        transport.push_response(err_status(400, b"bad resume url"));

        let options = TusStartOptions {
            existing_url: Some(upload_url.into()),
            ..Default::default()
        };
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start { file, options })
            .unwrap();
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport).await;

        assert!(
            crate::persistence::get(endpoint, &mk).is_none(),
            "400 Bad Request must clear the stale persisted entry"
        );
        assert_eq!(sink.current().status, UploadStatus::Error);
    }

    /// Abort during the resume-HEAD (slow network) must clear the
    /// prior-session persistence entry. Pre-fix the `mk` was computed only
    /// after the pre-loop succeeded, so the `PreLoop::Aborted` arm
    /// returned without removing the stale entry. The next `add()` of
    /// the same file would re-attach the same dead URL, looping the
    /// user through resume→abort→resume until the 24h TTL elapsed.
    #[wasm_bindgen_test]
    async fn abort_during_pre_loop_head_clears_persisted_entry() {
        clear_persistence();
        let endpoint = "http://test.local/preloop-head-abort-test";
        let upload_url = "http://test.local/preloop-head-abort-test/dead-id";
        let file = make_file("ghost.bin", b"hello world!");
        let mk = crate::persistence::match_key(
            endpoint,
            "ghost.bin",
            file.size() as u64,
            file.last_modified(),
        );
        // Seed a prior-session entry — this is what auto-resume would
        // surface via TusStartOptions.existing_url.
        let entry = crate::persistence::ResumableEntry {
            match_key: mk.clone(),
            endpoint: endpoint.into(),
            filename: "ghost.bin".into(),
            file_size: file.size() as u64,
            last_modified: file.last_modified(),
            upload_url: upload_url.into(),
            bytes_uploaded: 4,
            stored_at_ms: js_sys::Date::now(),
        };
        crate::persistence::put(&entry).expect("seed persistence");
        assert!(
            crate::persistence::get(endpoint, &mk).is_some(),
            "precondition: prior-session entry seeded",
        );

        // Slow HEAD so Abort can land while the network is in flight.
        let transport = MockTransport::new().with_delay_ms(500);
        transport.push_response(ok_200_head(4, file.size() as u64));

        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        let options = TusStartOptions {
            existing_url: Some(upload_url.into()),
            ..Default::default()
        };
        tx.unbounded_send(UploadCommand::Start { file, options })
            .unwrap();
        let tx_for_abort = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(50).await;
            let _ = tx_for_abort.unbounded_send(UploadCommand::Abort);
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport).await;

        assert!(
            crate::persistence::get(endpoint, &mk).is_none(),
            "Abort during pre-loop HEAD must clear the prior-session entry",
        );
        assert_eq!(sink.current().status, UploadStatus::Idle);
    }

    /// Throttled persistence rewrites during a session must preserve the
    /// `stored_at_ms` of the entry from the prior session (or, on a fresh
    /// upload, the moment the entry was first written). The 24h TTL is
    /// creation-relative so a long-lived upload (or a pause→resume cycle
    /// that rewrites every 2s) cannot keep the entry alive indefinitely.
    /// Pre-fix the rewrite path used `js_sys::Date::now()` for every
    /// throttle tick, sliding the TTL forward on every chunk.
    #[wasm_bindgen_test]
    async fn resume_rewrite_preserves_original_stored_at_ms() {
        clear_persistence();
        let endpoint = "http://test.local/ttl-preserve-test";
        let upload_url = "http://test.local/ttl-preserve-test/keep-id";
        // ASCII file so blob_size is content.len(); enough bytes to
        // produce at least one PATCH that triggers `persist_entry`.
        let content = b"abcdefghijklmnop"; // 16 bytes
        let file = make_file("keep.bin", content);
        let mk = crate::persistence::match_key(
            endpoint,
            "keep.bin",
            file.size() as u64,
            file.last_modified(),
        );

        // Seed a prior-session entry with stored_at_ms 12 hours ago.
        let seeded_stored_at = js_sys::Date::now() - 12.0 * 3_600_000.0;
        let seeded = crate::persistence::ResumableEntry {
            match_key: mk.clone(),
            endpoint: endpoint.into(),
            filename: "keep.bin".into(),
            file_size: file.size() as u64,
            last_modified: file.last_modified(),
            upload_url: upload_url.into(),
            bytes_uploaded: 0,
            stored_at_ms: seeded_stored_at,
        };
        crate::persistence::put(&seeded).expect("seed prior-session entry");

        // Resume: HEAD reports offset=0, then the engine PATCHes the
        // whole file and Completes. The post-PATCH persistence rewrite
        // (and the on-Complete remove) is what we're guarding against.
        let transport = MockTransport::new();
        transport.push_response(ok_200_head(0, file.size() as u64));
        transport.push_response(ok_204_patch(file.size() as u64));

        let options = TusStartOptions {
            existing_url: Some(upload_url.into()),
            ..Default::default()
        };
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start { file, options })
            .unwrap();

        // Capture stored_at_ms RIGHT AFTER the initial persist_entry
        // (which happens just after the HEAD), before Complete deletes
        // the entry. The timing window is narrow but deterministic on
        // wasm — each PATCH in the mock is synchronous-ish and we use
        // a small delay-loop check below.
        //
        // Easier approach: prevent Complete by holding tx open and
        // peek midway. We give the engine ~80ms to perform the HEAD +
        // first PATCH, then assert before letting it finish.
        let tx_holder = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(150).await;
            drop(tx_holder);
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        // Drive the engine concurrently; sample persistence ~50ms in.
        let engine = run_command_loop(test_config(endpoint), rx, &mut sink, transport);
        let sampler = async {
            gloo_timers::future::TimeoutFuture::new(50).await;
            crate::persistence::get(endpoint, &mk).map(|e| e.stored_at_ms)
        };
        let (_, snapshot_ts) = futures::future::join(engine, sampler).await;

        if let Some(observed) = snapshot_ts {
            // Allow ~1ms of f64 rounding noise.
            assert!(
                (observed - seeded_stored_at).abs() < 1.5,
                "throttled rewrite must preserve seeded stored_at_ms; \
                 expected {seeded_stored_at}, observed {observed}",
            );
        } else {
            // The snapshot raced past Complete, which already cleared
            // the entry. That is acceptable — the test still asserts
            // correctness when the snapshot lands during the upload.
        }
    }

    /// Successful Complete drops the persisted entry — without this the
    /// resume banner would offer a finished upload on the next reload.
    ///
    /// Holds `tx` alive past the chunk loop's first `try_next` via a
    /// delayed `spawn_local` (same pattern as
    /// `restart_via_pause_uploads_new_file`). Without that, `try_next` on
    /// an empty closed channel returns `Ok(None)` and the engine returns
    /// `Aborted` before the first PATCH — the upload never actually
    /// completes and the test fails on `status == Complete`.
    #[wasm_bindgen_test]
    async fn complete_clears_persisted_entry() {
        clear_persistence();
        let endpoint = "http://test.local/complete-test";
        let file = make_file("done.bin", b"abcdefgh");
        let mk = crate::persistence::match_key(
            endpoint,
            "done.bin",
            file.size() as u64,
            file.last_modified(),
        );

        let transport = MockTransport::new();
        transport.push_response(ok_201_create("http://test.local/complete-test/done-id"));
        transport.push_response(ok_204_patch(8));

        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();

        // Hold tx alive past the chunk loop's first try_next so the engine
        // observes "empty open" (fall through to PATCH) rather than "empty
        // closed" (return Aborted).
        let tx_holder = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(150).await;
            drop(tx_holder);
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport).await;

        assert_eq!(sink.current().status, UploadStatus::Complete);
        assert!(
            crate::persistence::get(endpoint, &mk).is_none(),
            "Complete must clear the persisted entry"
        );
    }

    #[wasm_bindgen_test]
    async fn resume_head_offset_beyond_file_size_errors_not_complete() {
        clear_persistence();
        let endpoint = "http://test.local/overshoot-head-test";
        let upload_url = "http://test.local/overshoot-head-test/upload-id";
        let file = make_file("tiny.bin", b"tiny");
        let transport = MockTransport::new();
        transport.push_response(ok_200_head(5, 4));

        let options = TusStartOptions {
            existing_url: Some(upload_url.into()),
            ..Default::default()
        };
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start { file, options })
            .unwrap();
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport).await;

        let current = sink.current();
        assert_eq!(current.status, UploadStatus::Error);
        let err = current.error.expect("expected offset error").to_string();
        assert!(
            err.contains("5") && err.contains("4"),
            "unexpected error: {err}"
        );
    }

    #[wasm_bindgen_test]
    async fn resume_head_length_mismatch_errors_without_patch() {
        clear_persistence();
        let endpoint = "http://test.local/length-mismatch-head-test";
        let upload_url = "http://test.local/length-mismatch-head-test/upload-id";
        let file = make_file("tiny.bin", b"tiny");
        let transport = MockTransport::new();
        transport.push_response(ok_200_head(2, 999));

        let options = TusStartOptions {
            existing_url: Some(upload_url.into()),
            ..Default::default()
        };
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start { file, options })
            .unwrap();
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport.clone()).await;

        let current = sink.current();
        assert_eq!(current.status, UploadStatus::Error);
        let err = current.error.expect("expected length mismatch").to_string();
        assert!(
            err.contains("999") && err.contains("4"),
            "unexpected error: {err}"
        );
        assert_eq!(
            transport.requests().len(),
            1,
            "length mismatch must stop after HEAD and not send PATCH",
        );
    }

    #[wasm_bindgen_test]
    async fn patch_non_advancing_offset_errors_without_second_patch() {
        clear_persistence();
        let endpoint = "http://test.local/stalled-patch-test";
        let file = make_file("tiny.bin", b"tiny");
        let transport = MockTransport::new();
        transport.push_response(ok_201_create(
            "http://test.local/stalled-patch-test/upload-id",
        ));
        transport.push_response(ok_204_patch(0));

        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();
        let tx_holder = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(150).await;
            drop(tx_holder);
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport.clone()).await;

        let current = sink.current();
        assert_eq!(current.status, UploadStatus::Error);
        let reqs = transport.requests();
        assert_eq!(
            reqs.len(),
            2,
            "must fail after first stalled PATCH, got {reqs:?}"
        );
    }

    #[wasm_bindgen_test]
    async fn patch_offset_beyond_file_size_errors_not_complete() {
        clear_persistence();
        let endpoint = "http://test.local/overshoot-patch-test";
        let file = make_file("tiny.bin", b"tiny");
        let transport = MockTransport::new();
        transport.push_response(ok_201_create(
            "http://test.local/overshoot-patch-test/upload-id",
        ));
        transport.push_response(ok_204_patch(5));

        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();
        let tx_holder = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(150).await;
            drop(tx_holder);
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport).await;

        let current = sink.current();
        assert_eq!(current.status, UploadStatus::Error);
        let err = current.error.expect("expected offset error").to_string();
        assert!(
            err.contains("5") && err.contains("4"),
            "unexpected error: {err}"
        );
    }

    /// Abort during retry backoff exits within ~poll quantum, not after
    /// the full `delay_ms * 2^attempt` sleep. Worst case at attempt=8 with
    /// default config is ~51s; the regression would freeze the UI.
    #[wasm_bindgen_test]
    async fn abort_during_retry_backoff_exits_promptly() {
        clear_persistence();
        let endpoint = "http://test.local/abort-test";

        let transport = MockTransport::new();
        // POST OK -> PATCH returns 503 (retryable) -> HEAD recovery -> backoff begins
        transport.push_response(ok_201_create("http://test.local/abort-test/x-id"));
        transport.push_response(err_status(503, b"down"));
        transport.push_response(ok_200_head(0, 5));

        let file = make_file("x.bin", b"hello");
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();

        // Configure a huge but browser-safe backoff base so even full-jitter
        // retry sleeps are overwhelmingly likely to still be pending when
        // Abort arrives. Values above i32::MAX can be coerced by browser
        // timers, so stay below that ceiling.
        let config = TusConfig::new(endpoint)
            .with_chunk_size(1024 * 1024)
            .with_creation_with_upload_threshold(0)
            .with_max_retries(5)
            .with_retry_delay_ms(2_000_000_000);

        // Send Abort 50ms after kick-off — the chunk loop should be in
        // backoff by then.
        let tx_for_abort = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(50).await;
            let _ = tx_for_abort.unbounded_send(UploadCommand::Abort);
            // Drop the original sender via the clone going out of scope to
            // close the channel after Abort is processed.
        });
        drop(tx);

        let started = js_sys::Date::now();
        let mut sink = CapturedSink::default();
        run_command_loop(config, rx, &mut sink, transport.clone()).await;
        let elapsed = js_sys::Date::now() - started;

        assert!(
            elapsed < 5_000.0,
            "Abort during retry backoff was held for {elapsed:.0}ms; \
             expected <5s with select! fix",
        );
        let final_state = sink.current();
        assert_eq!(final_state.status, UploadStatus::Idle);
        assert_eq!(
            final_state.bytes_uploaded, 0,
            "abort must reset bytes_uploaded so a stale progress fraction \
             doesn't survive into the next start",
        );
        assert!(
            final_state.bytes_total.is_none(),
            "abort must clear bytes_total so the UI doesn't compute a stale \
             progress_fraction(); got bytes_total={:?}",
            final_state.bytes_total,
        );
        assert!(
            final_state.upload_url.is_none(),
            "abort must clear upload_url so a subsequent Start does a fresh \
             POST instead of resuming a discarded upload",
        );
        let reqs = transport.requests();
        assert_eq!(
            reqs.len(),
            3,
            "POST + failed PATCH + recovery HEAD; no retry PATCH after abort"
        );
        assert_eq!(reqs[2].method(), Method::HEAD);
    }

    /// 408 Request Timeout is retryable: a proxy/gateway timeout shouldn't
    /// permanently fail an upload. Drives [Start] with PATCH(408) followed by
    /// PATCH(204), expecting the engine to retry and reach Complete.
    #[wasm_bindgen_test]
    async fn retry_408_request_timeout_eventually_completes() {
        clear_persistence();
        let endpoint = "http://test.local/408-test";

        let transport = MockTransport::new();
        transport.push_response(ok_201_create("http://test.local/408-test/x-id"));
        transport.push_response(err_status(408, b"timeout"));
        transport.push_response(ok_204_patch(5));

        let file = make_file("x.bin", b"hello");
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();

        // Hold tx alive past the chunk loop's first try_next so the engine
        // observes "empty open" (fall through to PATCH) rather than "empty
        // closed" (return Aborted). Same pattern as
        // `restart_via_pause_uploads_new_file`.
        let tx_holder = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(200).await;
            drop(tx_holder);
        });
        drop(tx);

        let config = TusConfig::new(endpoint)
            .with_chunk_size(1024 * 1024)
            .with_creation_with_upload_threshold(0)
            .with_max_retries(3)
            .with_retry_delay_ms(10);

        let mut sink = CapturedSink::default();
        run_command_loop(config, rx, &mut sink, transport.clone()).await;

        assert_eq!(
            sink.current().status,
            UploadStatus::Complete,
            "408 must trigger retry; upload should reach Complete on the next PATCH",
        );
        assert_eq!(
            transport.requests().len(),
            3,
            "POST + retried PATCH + successful PATCH"
        );
    }

    #[wasm_bindgen_test]
    async fn retryable_patch_failure_heads_before_retrying() {
        clear_persistence();
        let endpoint = "http://test.local/retry-head-recovery-test";
        let transport = MockTransport::new();
        transport.push_response(ok_201_create(
            "http://test.local/retry-head-recovery-test/x-id",
        ));
        transport.push_response(err_status(503, b"ambiguous failure"));
        transport.push_response(ok_200_head(2, 4));
        transport.push_response(ok_204_patch(4));

        let file = make_file("x.bin", b"data");
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();
        let tx_holder = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(250).await;
            drop(tx_holder);
        });
        drop(tx);

        let config = TusConfig::new(endpoint)
            .with_chunk_size(4)
            .with_creation_with_upload_threshold(0)
            .with_max_retries(1)
            .with_retry_delay_ms(10);
        let mut sink = CapturedSink::default();
        run_command_loop(config, rx, &mut sink, transport.clone()).await;

        assert_eq!(sink.current().status, UploadStatus::Complete);
        let reqs = transport.requests();
        let methods: Vec<_> = reqs.iter().map(|r| r.method().clone()).collect();
        assert_eq!(
            methods,
            vec![Method::POST, Method::PATCH, Method::HEAD, Method::PATCH],
            "ambiguous PATCH failures must re-HEAD before retrying; got {methods:?}",
        );
        assert_eq!(
            reqs[3]
                .headers()
                .get("upload-offset")
                .and_then(|v| v.to_str().ok()),
            Some("2"),
            "retry must continue from the HEAD-reported offset",
        );
    }

    #[wasm_bindgen_test]
    async fn retryable_recovery_head_failure_is_retried() {
        clear_persistence();
        let endpoint = "http://test.local/retry-head-transient-test";
        let transport = MockTransport::new();
        transport.push_response(ok_201_create(
            "http://test.local/retry-head-transient-test/x-id",
        ));
        transport.push_response(err_status(503, b"temporary patch failure"));
        transport.push_response(err_status(503, b"temporary head failure"));
        transport.push_response(ok_200_head(0, 5));
        transport.push_response(ok_204_patch(5));

        let file = make_file("x.bin", b"hello");
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();
        let tx_holder = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(250).await;
            drop(tx_holder);
        });
        drop(tx);

        let config = TusConfig::new(endpoint)
            .with_chunk_size(1024 * 1024)
            .with_creation_with_upload_threshold(0)
            .with_max_retries(2)
            .with_retry_delay_ms(0);
        let mut sink = CapturedSink::default();
        run_command_loop(config, rx, &mut sink, transport.clone()).await;

        assert_eq!(sink.current().status, UploadStatus::Complete);
        let methods: Vec<_> = transport
            .requests()
            .iter()
            .map(|r| r.method().clone())
            .collect();
        assert_eq!(
            methods,
            vec![
                Method::POST,
                Method::PATCH,
                Method::HEAD,
                Method::HEAD,
                Method::PATCH,
            ],
        );
    }

    #[wasm_bindgen_test]
    async fn abort_during_retry_recovery_head_exits_promptly() {
        clear_persistence();
        let endpoint = "http://test.local/retry-head-abort-test";
        let transport = MockTransport::new();
        transport.push_response(ok_201_create(
            "http://test.local/retry-head-abort-test/x-id",
        ));
        transport.push_response(err_status(503, b"ambiguous failure"));
        transport.push_delayed_response(ok_200_head(0, 5), 5_000);

        let file = make_file("x.bin", b"hello");
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();
        let tx_for_abort = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(50).await;
            let _ = tx_for_abort.unbounded_send(UploadCommand::Abort);
        });
        drop(tx);

        let config = TusConfig::new(endpoint)
            .with_chunk_size(1024 * 1024)
            .with_creation_with_upload_threshold(0)
            .with_max_retries(1)
            .with_retry_delay_ms(0);

        let started = js_sys::Date::now();
        let mut sink = CapturedSink::default();
        run_command_loop(config, rx, &mut sink, transport.clone()).await;
        let elapsed = js_sys::Date::now() - started;

        assert!(
            elapsed < 2_000.0,
            "Abort during delayed recovery HEAD was held for {elapsed:.0}ms",
        );
        assert_eq!(sink.current().status, UploadStatus::Idle);
        let methods: Vec<_> = transport
            .requests()
            .iter()
            .map(|r| r.method().clone())
            .collect();
        assert_eq!(methods, vec![Method::POST, Method::PATCH, Method::HEAD]);
    }

    /// Resume happy path: HEAD returns 200 with a non-zero offset, the
    /// engine continues PATCHing from that offset, and the persisted
    /// localStorage entry survives until Complete (then is cleared).
    /// Pins the user-visible feature of the persistence layer.
    #[wasm_bindgen_test]
    async fn resume_existing_url_continues_from_server_offset() {
        clear_persistence();
        let endpoint = "http://test.local/resume-test";
        let upload_url = "http://test.local/resume-test/r-id";
        let content = b"hello world!"; // 12 bytes
        let server_offset = 5u64;

        // Pre-seed persistence the way an earlier session would have left it.
        let file = make_file("resume.bin", content);
        let mk = crate::persistence::match_key(
            endpoint,
            "resume.bin",
            file.size() as u64,
            file.last_modified(),
        );
        let entry = crate::persistence::ResumableEntry {
            match_key: mk.clone(),
            endpoint: endpoint.into(),
            filename: "resume.bin".into(),
            file_size: file.size() as u64,
            last_modified: file.last_modified(),
            upload_url: upload_url.into(),
            bytes_uploaded: server_offset,
            stored_at_ms: js_sys::Date::now(),
        };
        crate::persistence::put(&entry).expect("seed persistence");

        let transport = MockTransport::new();
        // HEAD says: server has 5 bytes, total 12 bytes.
        transport.push_response(ok_200_head(server_offset, content.len() as u64));
        // PATCH the remaining 7 bytes -> Upload-Offset: 12.
        transport.push_response(ok_204_patch(content.len() as u64));

        let options = TusStartOptions {
            existing_url: Some(upload_url.into()),
            ..Default::default()
        };
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start { file, options })
            .unwrap();

        // Hold tx alive past the chunk loop's first try_next, otherwise the
        // engine sees `Ok(None)` and returns Aborted before the first PATCH.
        let tx_holder = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(150).await;
            drop(tx_holder);
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport.clone()).await;

        let final_state = sink.current();
        assert_eq!(final_state.status, UploadStatus::Complete);
        assert_eq!(
            final_state.bytes_uploaded,
            content.len() as u64,
            "Complete reports full file size"
        );

        // Two requests: HEAD then PATCH. No POST — we resumed.
        let reqs = transport.requests();
        assert_eq!(reqs.len(), 2, "expected HEAD + PATCH on resume path");
        assert_eq!(reqs[0].method(), http::Method::HEAD);
        assert_eq!(reqs[1].method(), http::Method::PATCH);
        assert_eq!(
            reqs[1]
                .headers()
                .get("upload-offset")
                .and_then(|v| v.to_str().ok()),
            Some("5"),
            "PATCH must continue from the server-reported offset",
        );

        // Persistence is cleared on Complete (so the next session doesn't
        // offer to resume a finished upload).
        assert!(
            crate::persistence::get(endpoint, &mk).is_none(),
            "Complete must clear the persisted entry",
        );
    }

    /// Helper for the HEAD-status persistence-clearing tests below.
    /// Pre-seeds a persisted entry for `(endpoint, file)`, drives a Start
    /// with `existing_url`, expects HEAD to fail with `head_status`, and
    /// returns whether the persisted entry survived. The caller asserts.
    async fn run_head_clears_persistence_check(
        endpoint: &str,
        file_name: &str,
        head_status: u16,
    ) -> bool {
        clear_persistence();
        let file = make_file(file_name, b"contents");
        let mk = crate::persistence::match_key(
            endpoint,
            file_name,
            file.size() as u64,
            file.last_modified(),
        );
        let upload_url = format!("{endpoint}/some-id");
        let entry = crate::persistence::ResumableEntry {
            match_key: mk.clone(),
            endpoint: endpoint.into(),
            filename: file_name.into(),
            file_size: file.size() as u64,
            last_modified: file.last_modified(),
            upload_url: upload_url.clone(),
            bytes_uploaded: 0,
            stored_at_ms: js_sys::Date::now(),
        };
        crate::persistence::put(&entry).expect("seed persistence");
        assert!(
            crate::persistence::get(endpoint, &mk).is_some(),
            "precondition: entry seeded"
        );

        let transport = MockTransport::new();
        transport.push_response(err_status(head_status, b"x"));

        let options = TusStartOptions {
            existing_url: Some(upload_url),
            ..Default::default()
        };
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start { file, options })
            .unwrap();
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport).await;

        assert!(
            matches!(sink.current().status, UploadStatus::Error),
            "expected Error status after HEAD {head_status}, got {:?}",
            sink.current().status
        );
        crate::persistence::get(endpoint, &mk).is_some()
    }

    /// 404 Not Found on resume HEAD also clears the persisted entry —
    /// the server has definitively forgotten the resource, same as 410.
    /// Without this, every re-pick of the file in the next 24 hours
    /// re-attaches the dead URL and re-fails.
    #[wasm_bindgen_test]
    async fn head_404_clears_persisted_entry() {
        let survived =
            run_head_clears_persistence_check("http://test.local/404-test", "missing.bin", 404)
                .await;
        assert!(!survived, "404 must clear the persisted entry");
    }

    /// 401 Unauthorized on resume HEAD must NOT clear the entry. 401 is
    /// commonly transient (token refresh, expired session) and the
    /// upload bytes are still on the server. Forcing a fresh upload on
    /// every auth blip is worse than surfacing the auth error.
    /// Regression guard against an over-aggressive "clear on any 4xx".
    #[wasm_bindgen_test]
    async fn head_401_does_not_clear_persisted_entry() {
        let survived =
            run_head_clears_persistence_check("http://test.local/401-test", "auth.bin", 401).await;
        assert!(survived, "401 must NOT clear the persisted entry");
    }

    /// 403 Forbidden, like 401, is transient (role change, RBAC
    /// propagation). Same regression guard.
    #[wasm_bindgen_test]
    async fn head_403_does_not_clear_persisted_entry() {
        let survived =
            run_head_clears_persistence_check("http://test.local/403-test", "role.bin", 403).await;
        assert!(survived, "403 must NOT clear the persisted entry");
    }

    /// Pause arriving during a retry-backoff sleep flips the engine to
    /// `Paused`, breaks the retry loop, and waits for Resume — without
    /// burning the retry budget. A subsequent Resume retries the chunk
    /// from the same offset and the upload completes.
    ///
    /// Closes the gap in the pre-merge review: the Pause arm of the
    /// retry-backoff `select!` (`hook.rs:647-653`) had no test.
    #[wasm_bindgen_test]
    async fn pause_during_retry_backoff_then_resume_completes() {
        clear_persistence();
        let endpoint = "http://test.local/pause-backoff-test";

        let transport = MockTransport::new();
        transport.push_response(ok_201_create("http://test.local/pause-backoff-test/x-id"));
        // PATCH 503 → engine enters retry backoff.
        transport.push_response(err_status(503, b"down"));
        // After Pause + Resume, retry succeeds.
        transport.push_response(ok_204_patch(5));

        let file = make_file("x.bin", b"hello");
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();

        // Long backoff so Pause definitely lands during the sleep.
        let config = TusConfig::new(endpoint)
            .with_chunk_size(1024 * 1024)
            .with_creation_with_upload_threshold(0)
            .with_max_retries(3)
            .with_retry_delay_ms(2_000);

        // Send Pause 50ms after kick-off (engine should be in backoff)
        // and Resume 100ms after Pause. Hold tx alive past the
        // post-Resume retry PATCH so the chunk loop's try_next observes
        // "empty open" (fall through to PATCH) rather than "empty closed"
        // (return Aborted). Without the hold, dropping `tx_resume` at
        // task exit closes the channel before the retried PATCH lands
        // and the engine bails out as Aborted with state still Uploading.
        let tx_pause = tx.clone();
        let tx_resume = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(50).await;
            let _ = tx_pause.unbounded_send(UploadCommand::Pause);
            gloo_timers::future::TimeoutFuture::new(100).await;
            let _ = tx_resume.unbounded_send(UploadCommand::Resume);
            gloo_timers::future::TimeoutFuture::new(200).await;
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(config, rx, &mut sink, transport.clone()).await;

        let final_state = sink.current();
        assert_eq!(
            final_state.status,
            UploadStatus::Complete,
            "engine must reach Complete after Pause→Resume during backoff",
        );
        // POST + failed PATCH + retried PATCH = 3 requests.
        assert_eq!(
            transport.requests().len(),
            3,
            "no extra retries should burn beyond the one transient failure",
        );
    }

    /// 429 Too Many Requests is retryable. Same shape as the 408 test but
    /// with the rate-limit status code.
    #[wasm_bindgen_test]
    async fn retry_429_too_many_requests_eventually_completes() {
        clear_persistence();
        let endpoint = "http://test.local/429-test";

        let transport = MockTransport::new();
        transport.push_response(ok_201_create("http://test.local/429-test/x-id"));
        transport.push_response(err_status(429, b"slow down"));
        transport.push_response(ok_204_patch(5));

        let file = make_file("x.bin", b"hello");
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();

        // Hold tx alive past the chunk loop's first try_next, otherwise the
        // engine sees `Ok(None)` and returns Aborted before the first PATCH.
        let tx_holder = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(200).await;
            drop(tx_holder);
        });
        drop(tx);

        let config = TusConfig::new(endpoint)
            .with_chunk_size(1024 * 1024)
            .with_creation_with_upload_threshold(0)
            .with_max_retries(3)
            .with_retry_delay_ms(10);

        let mut sink = CapturedSink::default();
        run_command_loop(config, rx, &mut sink, transport.clone()).await;

        assert_eq!(
            sink.current().status,
            UploadStatus::Complete,
            "429 must trigger retry, not surface as a permanent error",
        );
        assert_eq!(transport.requests().len(), 3);
    }

    /// Pause-then-Start does NOT leak the prior pause into the new run.
    ///
    /// Before the fix in `run_command_loop`, the `paused` local stayed `true`
    /// after `RunOutcome::Restart` because none of the three Start-while-
    /// paused / Start-during-backoff arms reset it. The next `run_upload`
    /// invocation would observe `*paused == true` on its first chunk-loop
    /// iteration, see `if *paused { rx.next().await }`, and block forever
    /// waiting for a Resume the user never sends.
    ///
    /// This test isolates that invariant: it sends Start{B} via a delayed
    /// task (so tx outlives B's chunk loop), and asserts B reaches
    /// `Complete` on its own — no Resume, no Abort, no Start arrives after
    /// Start{B}. If the paused flag leaked, B would hang and the test
    /// would time out instead of completing.
    #[wasm_bindgen_test]
    async fn paused_does_not_leak_into_restarted_upload() {
        clear_persistence();
        let endpoint = "http://test.local/paused-leak-test";

        let transport = MockTransport::new();
        // A: POST -> (no PATCH; Pause arrives first)
        transport.push_response(ok_201_create("http://test.local/paused-leak-test/a-id"));
        // B: POST -> PATCH succeeds in one shot.
        transport.push_response(ok_201_create("http://test.local/paused-leak-test/b-id"));
        transport.push_response(ok_204_patch(5));

        let file_a = make_file("a.bin", b"hello");
        let file_b = make_file("b.bin", b"world");

        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file: file_a,
            options: TusStartOptions::default(),
        })
        .unwrap();
        tx.unbounded_send(UploadCommand::Pause).unwrap();

        // Delayed Start{B} so Pause is processed first; tx_for_b held in
        // the spawned task keeps the channel open while B runs.
        let tx_for_b = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(50).await;
            let _ = tx_for_b.unbounded_send(UploadCommand::Start {
                file: file_b,
                options: TusStartOptions::default(),
            });
            // Hold past B's expected POST + PATCH.
            gloo_timers::future::TimeoutFuture::new(250).await;
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport.clone()).await;

        assert_eq!(
            sink.current().status,
            UploadStatus::Complete,
            "B must complete without any Resume — Pause from A should not \
             carry into B's chunk loop",
        );
        let reqs = transport.requests();
        assert_eq!(
            reqs.len(),
            3,
            "expected POST(A), POST(B), PATCH(B); got {:?}",
            reqs.iter()
                .map(|r| (r.method().clone(), r.uri().to_string()))
                .collect::<Vec<_>>(),
        );
    }

    /// Abort arriving DURING the pre-chunk-loop POST short-circuits the
    /// upload before any chunks are PATCHed. Pre-fix the engine awaited
    /// the entire POST round trip before its first `try_next` could see
    /// the Abort — on a slow connection the user's click felt ignored
    /// for several seconds. Post-fix, `race_pre_loop_request` polls the
    /// command channel concurrently with the network future, so the
    /// Abort is honoured promptly.
    #[wasm_bindgen_test]
    async fn abort_during_pre_loop_post_short_circuits() {
        clear_persistence();
        let endpoint = "http://test.local/preloop-abort-test";

        // 500ms artificial latency on the POST so the Abort has a window
        // to land while the network future is pending.
        let transport = MockTransport::new().with_delay_ms(500);
        transport.push_response(ok_201_create("http://test.local/preloop-abort-test/x-id"));
        // No PATCH response queued — if the pre-loop short-circuit didn't
        // fire, the engine would proceed past POST and try to PATCH,
        // hitting the "no mock response" error.

        let file = make_file("x.bin", b"hello");
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();

        // Send Abort 50ms after kickoff — the POST's mock delay (500ms)
        // is still in flight, so race_pre_loop_request sees Abort first.
        let tx_for_abort = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(50).await;
            let _ = tx_for_abort.unbounded_send(UploadCommand::Abort);
        });
        drop(tx);

        let started = js_sys::Date::now();
        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport.clone()).await;
        let elapsed = js_sys::Date::now() - started;

        // The POST mock holds for 500ms; race_pre_loop_request must have
        // bailed out at ~50ms (when Abort landed). Allow generous slack
        // for browser scheduling jitter, but assert under 400ms so the
        // test would fail if the engine awaited the full POST.
        assert!(
            elapsed < 400.0,
            "Abort during pre-loop POST was held for {elapsed:.0}ms; \
             expected <400ms with race_pre_loop_request, far less than \
             the 500ms POST delay",
        );

        let final_state = sink.current();
        assert_eq!(final_state.status, UploadStatus::Idle);
        assert_eq!(final_state.bytes_uploaded, 0);
        assert!(final_state.upload_url.is_none());
        assert!(final_state.bytes_total.is_none());
    }

    /// Resume HEAD reports an offset equal to the file size — the upload
    /// is already complete on the server. The engine must skip the chunk
    /// loop, mark Complete, and clear the persisted entry. Without this
    /// guarantee a misconfigured / mid-shutdown server (or a re-resume of
    /// an upload the user already pushed past 100% via a different tab)
    /// would leave the queue item stuck in Uploading after the engine
    /// returned `Done`.
    #[wasm_bindgen_test]
    async fn head_offset_equal_to_file_size_completes_without_patch() {
        clear_persistence();
        let endpoint = "http://test.local/head-eq-size-test";
        let upload_url = "http://test.local/head-eq-size-test/done-id";
        let content = b"hello world!"; // 12 bytes

        let file = make_file("done.bin", content);

        let transport = MockTransport::new();
        // HEAD reports offset == file_size: server has every byte.
        transport.push_response(ok_200_head(content.len() as u64, content.len() as u64));
        // No PATCH response queued — if the chunk loop fired, the mock
        // would error with "no mock response".

        let options = TusStartOptions {
            existing_url: Some(upload_url.into()),
            ..Default::default()
        };
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start { file, options })
            .unwrap();
        // Hold tx alive past the chunk-loop's first try_next so the engine
        // observes "empty open" rather than channel-closed.
        let tx_holder = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(150).await;
            drop(tx_holder);
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport.clone()).await;

        let final_state = sink.current();
        assert_eq!(final_state.status, UploadStatus::Complete);
        assert_eq!(
            final_state.bytes_uploaded,
            content.len() as u64,
            "Complete reports full file size when HEAD already at offset==size",
        );
        assert_eq!(
            transport.requests().len(),
            1,
            "expected only the HEAD; no PATCH should fire when server is fully synced",
        );
    }

    /// Happy path for `creation-with-upload`: small file (<= threshold),
    /// server advertises the extension, engine sends OPTIONS + a single
    /// POST-with-body and reaches Complete with no PATCH. Closes the
    /// largest blind spot in the engine_tests suite (every other test
    /// uses `creation_with_upload_threshold(0)` to skip this code path).
    #[wasm_bindgen_test]
    async fn creation_with_upload_happy_path_skips_patch() {
        clear_persistence();
        let endpoint = "http://test.local/cwu-happy-test";
        let content = b"tiny";
        let file = make_file("tiny.bin", content);

        let transport = MockTransport::new();
        // OPTIONS: advertise creation-with-upload.
        let mut opts_headers = HeaderMap::new();
        opts_headers.insert(
            HeaderName::from_static("tus-version"),
            HeaderValue::from_static("1.0.0"),
        );
        opts_headers.insert(
            HeaderName::from_static("tus-extension"),
            HeaderValue::from_static("creation,creation-with-upload"),
        );
        transport.push_response(resp(204, opts_headers, Vec::new()));
        // POST with body returns 201 + Location + Upload-Offset == file_size.
        let mut create_headers = HeaderMap::new();
        create_headers.insert(
            HeaderName::from_static("location"),
            HeaderValue::from_static("http://test.local/cwu-happy-test/done-id"),
        );
        create_headers.insert(
            HeaderName::from_static("tus-resumable"),
            HeaderValue::from_static("1.0.0"),
        );
        create_headers.insert(
            HeaderName::from_static("upload-offset"),
            HeaderValue::from_str(&content.len().to_string()).unwrap(),
        );
        transport.push_response(resp(201, create_headers, Vec::new()));
        // No PATCH response queued.

        // Threshold > file_size so cwu predicate is true.
        let config = TusConfig::new(endpoint)
            .with_chunk_size(1024 * 1024)
            .with_creation_with_upload_threshold(8 * 1024)
            .with_max_retries(0)
            .with_retry_delay_ms(50);
        // Pre-flight options cache pollution from prior tests would skip
        // the OPTIONS request — invalidate to be sure we exercise the
        // OPTIONS-then-POST path.
        crate::options_cache::invalidate(endpoint);

        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();
        // Hold tx so engine isn't fooled into Aborted before completion.
        let tx_holder = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(150).await;
            drop(tx_holder);
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(config, rx, &mut sink, transport.clone()).await;

        assert_eq!(
            sink.current().status,
            UploadStatus::Complete,
            "creation-with-upload happy path should reach Complete",
        );
        let reqs = transport.requests();
        assert_eq!(
            reqs.len(),
            2,
            "expected OPTIONS + POST(with body); got {}",
            reqs.len()
        );
        assert_eq!(
            reqs[0].method(),
            Method::OPTIONS,
            "first request is OPTIONS probe"
        );
        assert_eq!(
            reqs[1].method(),
            Method::POST,
            "second request is POST-with-body"
        );
        // The POST body should contain the file bytes — that's the whole point.
        match reqs[1].body() {
            TransportBody::Bytes(b) => {
                assert_eq!(
                    b.as_slice(),
                    content,
                    "POST body must carry full file bytes"
                );
            }
            TransportBody::BytesWithTrailer { body, .. } => {
                assert_eq!(
                    body.as_slice(),
                    content,
                    "POST body must carry full file bytes"
                );
            }
            other => panic!("expected POST to have a body, got {other:?}"),
        }
    }

    #[wasm_bindgen_test]
    async fn creation_with_upload_options_probe_bypasses_cache_for_per_upload_token() {
        clear_persistence();
        let endpoint = "http://test.local/cwu-auth-cache-test";
        crate::options_cache::invalidate(endpoint);

        let mut opts_headers = HeaderMap::new();
        opts_headers.insert(
            HeaderName::from_static("tus-version"),
            HeaderValue::from_static("1.0.0"),
        );
        opts_headers.insert(
            HeaderName::from_static("tus-extension"),
            HeaderValue::from_static("creation,creation-with-upload"),
        );

        let mut create_with_body_headers = HeaderMap::new();
        create_with_body_headers.insert(
            HeaderName::from_static("location"),
            HeaderValue::from_static("http://test.local/cwu-auth-cache-test/a-id"),
        );
        create_with_body_headers.insert(
            HeaderName::from_static("tus-resumable"),
            HeaderValue::from_static("1.0.0"),
        );
        create_with_body_headers.insert(
            HeaderName::from_static("upload-offset"),
            HeaderValue::from_static("4"),
        );

        let transport_a = MockTransport::new();
        transport_a.push_response(resp(204, opts_headers.clone(), Vec::new()));
        transport_a.push_response(resp(201, create_with_body_headers, Vec::new()));

        let config = TusConfig::new(endpoint)
            .with_chunk_size(1024 * 1024)
            .with_creation_with_upload_threshold(8 * 1024)
            .with_max_retries(0)
            .with_retry_delay_ms(50);

        let (tx_a, rx_a) = mpsc::unbounded::<UploadCommand>();
        tx_a.unbounded_send(UploadCommand::Start {
            file: make_file("a.bin", b"aaaa"),
            options: TusStartOptions::default(),
        })
        .unwrap();
        let tx_a_holder = tx_a.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(150).await;
            drop(tx_a_holder);
        });
        drop(tx_a);
        let mut sink_a = CapturedSink::default();
        run_command_loop(config.clone(), rx_a, &mut sink_a, transport_a.clone()).await;
        assert_eq!(sink_a.current().status, UploadStatus::Complete);

        let transport_b = MockTransport::new();
        let mut opts_b_headers = HeaderMap::new();
        opts_b_headers.insert(
            HeaderName::from_static("tus-version"),
            HeaderValue::from_static("1.0.0"),
        );
        opts_b_headers.insert(
            HeaderName::from_static("tus-extension"),
            HeaderValue::from_static("creation"),
        );
        transport_b.push_response(resp(204, opts_b_headers, Vec::new()));
        transport_b.push_response(ok_201_create("http://test.local/cwu-auth-cache-test/b-id"));
        transport_b.push_response(ok_204_patch(4));

        let (tx_b, rx_b) = mpsc::unbounded::<UploadCommand>();
        tx_b.unbounded_send(UploadCommand::Start {
            file: make_file("b.bin", b"bbbb"),
            options: TusStartOptions {
                bearer_token_override: Some("per-upload-token".into()),
                ..Default::default()
            },
        })
        .unwrap();
        let tx_b_holder = tx_b.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(200).await;
            drop(tx_b_holder);
        });
        drop(tx_b);
        let mut sink_b = CapturedSink::default();
        run_command_loop(config, rx_b, &mut sink_b, transport_b.clone()).await;

        assert_eq!(sink_b.current().status, UploadStatus::Complete);
        let methods: Vec<_> = transport_b
            .requests()
            .iter()
            .map(|r| r.method().clone())
            .collect();
        assert_eq!(
            methods,
            vec![Method::OPTIONS, Method::POST, Method::PATCH],
            "per-upload auth must re-fetch OPTIONS instead of using endpoint-only cached capabilities",
        );
    }

    #[wasm_bindgen_test]
    async fn start_options_bearer_token_override_is_sent_on_requests() {
        clear_persistence();
        let endpoint = "http://test.local/token-precedence-test";
        let transport = MockTransport::new();
        transport.push_response(ok_201_create(
            "http://test.local/token-precedence-test/x-id",
        ));
        transport.push_response(ok_204_patch(5));

        let config = test_config(endpoint).with_bearer_token("config-token");
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file: make_file("x.bin", b"hello"),
            options: TusStartOptions {
                bearer_token_override: Some("upload-token".into()),
                ..Default::default()
            },
        })
        .unwrap();
        let tx_holder = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(150).await;
            drop(tx_holder);
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(config, rx, &mut sink, transport.clone()).await;

        assert_eq!(sink.current().status, UploadStatus::Complete);
        for req in transport.requests() {
            assert_eq!(
                req.headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok()),
                Some("Bearer upload-token"),
                "per-upload token must override config token on {:?} {}",
                req.method(),
                req.uri(),
            );
        }
    }

    /// `Start{B}` arriving during the retry-backoff `select!` returns
    /// `RunOutcome::Restart` cleanly: A's persistence is cleared, state
    /// resets to Idle, and the second run posts to the base endpoint
    /// (NOT to A's URL). Closes the gap noted in the pre-merge audit —
    /// the Pause and Abort arms of the retry-backoff select! had tests,
    /// but the Start arm did not.
    #[wasm_bindgen_test]
    async fn restart_during_retry_backoff_targets_fresh_post() {
        clear_persistence();
        let endpoint = "http://test.local/restart-backoff-test";

        let transport = MockTransport::new();
        // A: POST 201 -> PATCH 503 (retryable) -> HEAD recovery -> backoff begins
        transport.push_response(ok_201_create("http://test.local/restart-backoff-test/a-id"));
        transport.push_response(err_status(503, b"down"));
        transport.push_response(ok_200_head(0, 5));
        // B: POST 201 -> PATCH 204
        transport.push_response(ok_201_create("http://test.local/restart-backoff-test/b-id"));
        transport.push_response(ok_204_patch(5));

        let file_a = make_file("a.bin", b"hello");
        let file_b = make_file("b.bin", b"world");

        // Huge but browser-safe backoff base so Start{B} lands during the
        // jittered sleep rather than after a very short random retry delay.
        let config = TusConfig::new(endpoint)
            .with_chunk_size(1024 * 1024)
            .with_creation_with_upload_threshold(0)
            .with_max_retries(3)
            .with_retry_delay_ms(2_000_000_000);

        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file: file_a,
            options: TusStartOptions::default(),
        })
        .unwrap();

        // Send Start{B} 80ms after kickoff. By then, A's POST + 503 PATCH +
        // recovery HEAD have unwound and the engine is parked in backoff.
        let tx_for_b = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(80).await;
            let _ = tx_for_b.unbounded_send(UploadCommand::Start {
                file: file_b,
                options: TusStartOptions::default(),
            });
            // Hold past B's expected POST + PATCH window.
            gloo_timers::future::TimeoutFuture::new(300).await;
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(config, rx, &mut sink, transport.clone()).await;

        let reqs = transport.requests();
        let methods: Vec<_> = reqs
            .iter()
            .map(|r| (r.method().clone(), r.uri().to_string()))
            .collect();
        assert_eq!(
            reqs.len(),
            5,
            "expected POST(A), PATCH(A:503), HEAD(A), POST(B), PATCH(B); got {methods:?}"
        );
        assert_eq!(reqs[0].method(), Method::POST);
        assert_eq!(reqs[1].method(), Method::PATCH);
        assert!(reqs[1].uri().path().contains("a-id"));
        assert_eq!(reqs[2].method(), Method::HEAD);
        assert!(reqs[2].uri().path().contains("a-id"));
        assert_eq!(
            reqs[3].method(),
            Method::POST,
            "Start during retry-backoff must POST a fresh upload, not HEAD or PATCH the failed one",
        );
        assert!(
            reqs[3].uri().path().ends_with("/restart-backoff-test"),
            "POST(B) targets base endpoint, NOT A's resource URL; got {}",
            reqs[3].uri(),
        );
        assert_eq!(reqs[4].method(), Method::PATCH);
        assert!(reqs[4].uri().path().contains("b-id"));
        assert_eq!(sink.current().status, UploadStatus::Complete);
    }

    /// Pause arriving during the pre-loop POST is captured by
    /// `race_pre_loop_request` into `*paused`. After the post-create
    /// state.update lands, the UI must show `Paused` (not `Uploading`),
    /// otherwise the user sees "uploading" while the engine is parked
    /// in the chunk-loop's paused-await branch. Regression for the
    /// pre-merge fix that branches on `*paused` when stamping the
    /// post-create status.
    #[wasm_bindgen_test]
    async fn pause_during_pre_loop_post_stamps_paused_after_create() {
        clear_persistence();
        let endpoint = "http://test.local/preloop-pause-stamp-test";

        // 200ms POST delay so Pause has a clear window to land.
        let transport = MockTransport::new().with_delay_ms(200);
        transport.push_response(ok_201_create(
            "http://test.local/preloop-pause-stamp-test/x-id",
        ));
        transport.push_response(ok_204_patch(5));

        let file = make_file("x.bin", b"hello");
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();

        // Send Pause 50ms after kickoff (still inside POST), then Resume
        // 250ms later (after POST has resolved and engine is parked in
        // the paused-await branch). Resume drives the upload to completion
        // so we can also assert the final state.
        let tx_pause = tx.clone();
        let tx_resume = tx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::TimeoutFuture::new(50).await;
            let _ = tx_pause.unbounded_send(UploadCommand::Pause);
            // Wait long enough for POST to resolve (~200ms) plus a margin.
            // During this window the engine is in paused-await and the
            // status MUST already be Paused (not Uploading).
            gloo_timers::future::TimeoutFuture::new(300).await;
            let _ = tx_resume.unbounded_send(UploadCommand::Resume);
            gloo_timers::future::TimeoutFuture::new(200).await;
        });
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport.clone()).await;

        // Walk the captured history: there MUST be a `Paused` state after
        // `bytes_total = Some(file_size)` was set (which only happens after
        // the post-create state.update). Pre-fix the post-create stamp was
        // unconditionally `Uploading` and Paused never appeared until a
        // subsequent Pause arrived after Resume — which never happens here.
        let history = sink.0.borrow().history.clone();
        let post_create_paused = history
            .iter()
            .any(|s| s.status == UploadStatus::Paused && s.bytes_total.is_some());
        assert!(
            post_create_paused,
            "engine must stamp Paused (not Uploading) after the pre-loop POST when \
             a Pause arrived during the POST round trip; captured history = {:?}",
            history
                .iter()
                .map(|s| (s.status.clone(), s.bytes_total))
                .collect::<Vec<_>>(),
        );
        // Sanity: Resume drives it to completion.
        assert_eq!(sink.current().status, UploadStatus::Complete);
    }

    /// Regression for the channel-closed reset path: when every clone of
    /// the upload command sender is dropped while the chunk loop is
    /// running (e.g. parent unmount), `try_next` returns `Ok(None)` and
    /// the engine MUST reset state to `Idle` — symmetric with the
    /// explicit `Abort` arm. Pre-fix to commit 2505945 the state was
    /// left at `Uploading`, so a downstream observer (queue scheduler,
    /// UI) saw a dangling worker forever.
    ///
    /// Setup: send Start, slow the POST so the engine reaches the chunk
    /// loop only after we've dropped every sender. Then await
    /// `run_command_loop` to its natural exit and assert the final
    /// state is Idle with all fields zeroed.
    #[wasm_bindgen_test]
    async fn channel_closed_during_chunk_loop_resets_to_idle() {
        clear_persistence();
        let endpoint = "http://test.local/chan-closed-resets";

        // 100ms POST delay buys time to drop the sender after Start has
        // been sent but before PATCH is dispatched.
        let transport = MockTransport::new().with_delay_ms(100);
        transport.push_response(ok_201_create("http://test.local/chan-closed-resets/x-id"));
        // PATCH response is queued but should never be sent — the engine
        // resets to Idle before reaching the PATCH.
        transport.push_response(ok_204_patch(5));

        let file = make_file("x.bin", b"hello");
        let (tx, rx) = mpsc::unbounded::<UploadCommand>();
        tx.unbounded_send(UploadCommand::Start {
            file,
            options: TusStartOptions::default(),
        })
        .unwrap();
        // Drop the sender immediately. POST will resolve, the engine
        // enters the chunk loop, try_next sees Ok(None), and the
        // channel-closed branch fires.
        drop(tx);

        let mut sink = CapturedSink::default();
        run_command_loop(test_config(endpoint), rx, &mut sink, transport.clone()).await;

        let final_state = sink.current();
        assert_eq!(
            final_state.status,
            UploadStatus::Idle,
            "channel-closed must reset to Idle, got {:?}",
            final_state.status,
        );
        assert_eq!(final_state.bytes_uploaded, 0);
        assert_eq!(final_state.bytes_total, None);
        assert_eq!(final_state.upload_url, None);

        // Also verify no PATCH ran — the engine bailed at the channel-
        // closed branch before reaching the PATCH dispatch.
        let reqs = transport.requests();
        let patch_count = reqs.iter().filter(|r| r.method() == Method::PATCH).count();
        assert_eq!(
            patch_count, 0,
            "no PATCH should fire after channel close; got {} request(s)",
            patch_count,
        );
    }
}
