//! Multi-file upload queue helper for Dioxus web.
//!
//! Composes on the single-file [`crate::use_tus_upload`] primitive. Maintains
//! a queue of items and a fixed pool of two workers (concurrency limit = 2 for
//! v1; the worker count is hardcoded so the call positions in the hook list
//! satisfy Dioxus's rules-of-hooks).
//!
//! # Quick start
//!
//! ```rust,ignore
//! use dioxus::prelude::*;
//! use dioxus_tus::{
//!     files_from_event, use_tus_upload_queue, TusConfig, TusStartOptions,
//! };
//!
//! #[component]
//! fn QueueUploader() -> Element {
//!     let (queue, handle) = use_tus_upload_queue(
//!         TusConfig::new("https://your-tus-server/files"),
//!     );
//!
//!     rsx! {
//!         input {
//!             r#type: "file",
//!             multiple: true,
//!             onchange: move |evt| {
//!                 let files = files_from_event(&evt);
//!                 handle.add_all(files, TusStartOptions::default());
//!             }
//!         }
//!         for item in queue.read().items.iter() {
//!             div { "{item.file_name}: {item.status:?}" }
//!         }
//!     }
//! }
//! ```
//!
//! # Per-upload bearer-token overrides
//!
//! Each [`TusQueueHandle::add`] call accepts its own [`TusStartOptions`], so
//! `bearer_token_override` (and other per-upload knobs) is honoured per item
//! even though the underlying workers are reused across the queue.

use dioxus::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;
use web_sys::File;

use crate::config::{TusConfig, TusStartOptions};
use crate::hook::{TusUploadHandle, use_tus_upload};
use crate::state::{TusError, TusUploadState, UploadStatus};

/// Hardcoded concurrency limit for 0.1.x. Each worker is a separate
/// [`use_tus_upload`] hook called at a fixed position in the parent
/// component's hook list.
const WORKER_COUNT: usize = 2;

/// Lifecycle of a single queue entry.
///
/// Marked `#[non_exhaustive]` so future variants (e.g. `Cancelled`,
/// `Retrying`) can be added without breaking exhaustive matches in
/// downstream UI code.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum QueueItemStatus {
    /// In the queue, waiting for an idle worker.
    Queued,
    /// Currently assigned to a worker and uploading.
    Uploading,
    /// Paused on its worker (worker-bound; queued items can't be paused).
    Paused,
    /// Finished successfully.
    Complete,
    /// Failed with the captured error.
    Error,
    /// User-aborted; left in the list so the UI can show the outcome.
    Aborted,
}

/// One row in the queue.
///
/// `#[non_exhaustive]`: consumers read queue rows rather than constructing
/// them, and this struct is expected to grow (e.g. speed/ETA fields), so
/// adding fields must not be a breaking change.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TusQueueItem {
    pub id: u64,
    pub file_name: String,
    pub file_size: u64,
    pub status: QueueItemStatus,
    pub bytes_uploaded: u64,
    pub error: Option<TusError>,
    pub upload_url: Option<String>,
    /// The browser file. Held so the scheduler can hand it to a worker when
    /// a slot frees up. `web_sys::File` is `Clone`, so passing to the worker
    /// is cheap.
    pub file: File,
    /// Per-upload options captured at `add` time.
    pub options: TusStartOptions,
    /// `js_sys::Date::now()` value at the moment this item entered
    /// `Uploading`. Used to compute average upload speed and ETA. `None`
    /// while the item is queued.
    pub started_at_ms: Option<f64>,
    /// `js_sys::Date::now()` value when this item entered `Paused`.
    pub paused_at_ms: Option<f64>,
    /// Total milliseconds spent paused after the item first entered `Uploading`.
    pub paused_accumulated_ms: f64,
}

impl TusQueueItem {
    /// Average upload speed in bytes/sec since this item started. Returns
    /// `None` if the item hasn't started yet or insufficient time has
    /// elapsed for a meaningful number.
    pub fn speed_bytes_per_sec(&self, now_ms: f64) -> Option<f64> {
        let started = self.started_at_ms?;
        let current_pause_ms = self
            .paused_at_ms
            .map(|paused_at| (now_ms - paused_at).max(0.0))
            .unwrap_or(0.0);
        let elapsed_ms = now_ms - started - self.paused_accumulated_ms - current_pause_ms;
        if elapsed_ms < 250.0 {
            return None; // smooth out the first few hundred ms of jitter
        }
        Some(self.bytes_uploaded as f64 * 1000.0 / elapsed_ms)
    }

    /// Estimated seconds-to-completion at the current average speed.
    /// Returns `None` for queued, paused, completed, aborted, or errored
    /// items, or when speed can't yet be determined.
    ///
    /// Paused items are excluded because `started_at_ms` keeps ticking while
    /// `bytes_uploaded` is frozen — `speed_bytes_per_sec` trends toward zero
    /// and ETA grows without bound, which renders as "47 hours" in the UI.
    pub fn eta_seconds(&self, now_ms: f64) -> Option<f64> {
        if matches!(
            self.status,
            QueueItemStatus::Complete
                | QueueItemStatus::Aborted
                | QueueItemStatus::Error
                | QueueItemStatus::Queued
                | QueueItemStatus::Paused
        ) {
            return None;
        }
        let speed = self.speed_bytes_per_sec(now_ms)?;
        if speed <= 0.0 {
            return None;
        }
        let remaining = self.file_size.saturating_sub(self.bytes_uploaded);
        Some(remaining as f64 / speed)
    }
}

/// Reactive snapshot of the whole queue.
///
/// `#[non_exhaustive]`: read by consumers, not constructed, and likely to grow
/// aggregate fields (e.g. totals/throughput), so new fields must stay additive.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct TusQueueState {
    pub items: Vec<TusQueueItem>,
}

impl TusQueueState {
    /// Number of items currently in `Queued` status.
    pub fn queued_count(&self) -> usize {
        self.items
            .iter()
            .filter(|i| i.status == QueueItemStatus::Queued)
            .count()
    }

    /// Number of items currently in `Uploading` or `Paused` status (i.e. occupying a worker slot).
    pub fn active_count(&self) -> usize {
        self.items
            .iter()
            .filter(|i| {
                matches!(
                    i.status,
                    QueueItemStatus::Uploading | QueueItemStatus::Paused
                )
            })
            .count()
    }

    /// Number of items in `Complete` status.
    pub fn complete_count(&self) -> usize {
        self.items
            .iter()
            .filter(|i| i.status == QueueItemStatus::Complete)
            .count()
    }

    /// Lookup an item by id.
    pub fn get(&self, id: u64) -> Option<&TusQueueItem> {
        self.items.iter().find(|i| i.id == id)
    }
}

fn is_terminal(status: &QueueItemStatus) -> bool {
    matches!(
        status,
        QueueItemStatus::Complete | QueueItemStatus::Aborted | QueueItemStatus::Error
    )
}

fn has_active_file_match(items: &[TusQueueItem], endpoint: &str, file: &File) -> bool {
    let mk = crate::persistence::match_key(
        endpoint,
        &file.name(),
        file.size() as u64,
        file.last_modified(),
    );
    items.iter().any(|i| {
        !is_terminal(&i.status)
            && crate::persistence::match_key(
                endpoint,
                &i.file_name,
                i.file_size,
                i.file.last_modified(),
            ) == mk
    })
}

/// Handle returned by [`use_tus_upload_queue`].
#[derive(Clone)]
pub struct TusQueueHandle {
    state: Signal<TusQueueState>,
    next_id: Rc<RefCell<u64>>,
    /// One handle per worker slot. Indexed by slot index `0..WORKER_COUNT`.
    workers: Rc<Vec<TusUploadHandle>>,
    /// Slot -> item id currently being uploaded by that slot. Shared with the
    /// scheduler future so it can release a slot when its worker reports
    /// terminal state.
    slot_assignments: Rc<RefCell<Vec<Option<u64>>>>,
    /// Endpoint URL — captured from the [`TusConfig`] at hook-call time.
    /// Used by [`Self::add`] to look up persisted resumable entries and by
    /// [`Self::scan_resumable`] for the resume-across-reload UX.
    endpoint: String,
}

impl TusQueueHandle {
    /// Add a single file to the queue with the given options. Returns the
    /// freshly allocated item id.
    ///
    /// If a [persisted entry](crate::persistence::ResumableEntry) for this
    /// `(endpoint, file)` pair exists in `localStorage` (e.g. from a prior
    /// session before a tab reload), `add` automatically picks up the
    /// stored `upload_url` and continues from the server's offset rather
    /// than starting a fresh upload. The progress bar will jump straight
    /// to the resumed position once the worker's HEAD probe lands.
    ///
    /// To inspect what's persisted before adding, call
    /// [`Self::scan_resumable`].
    pub fn add(&self, file: File, mut options: TusStartOptions) -> u64 {
        // Auto-resume: look for a persisted entry matching this file. If
        // the caller already passed an explicit `existing_url`, respect
        // that and skip the lookup.
        //
        // Skip auto-resume when a non-terminal queue item already shares
        // this match key — otherwise dropping the same file twice (or
        // submitting the same multi-file selection twice) would assign the
        // same `existing_url` to both items, race them onto two worker
        // slots, and one of them would 409 from the server. The duplicate
        // still gets queued, just as a fresh upload.
        if options.existing_url.is_none() {
            let mk = crate::persistence::match_key(
                &self.endpoint,
                &file.name(),
                file.size() as u64,
                file.last_modified(),
            );
            let already_in_flight =
                has_active_file_match(&self.state.read().items, &self.endpoint, &file);
            if already_in_flight {
                tracing::debug!(
                    file_name = %file.name(),
                    "queue: skipping auto-resume; same file already in-flight",
                );
            } else if let Some(entry) = crate::persistence::get(&self.endpoint, &mk) {
                let upload_url = crate::persistence::redact_upload_url_for_log(&entry.upload_url);
                tracing::debug!(
                    file_name = %file.name(),
                    upload_url = %upload_url,
                    bytes_uploaded = entry.bytes_uploaded,
                    "queue: auto-resuming from persisted entry",
                );
                options.existing_url = Some(entry.upload_url.clone());
            }
        }

        let id = {
            let mut n = self.next_id.borrow_mut();
            let id = *n;
            *n += 1;
            id
        };
        let item = TusQueueItem {
            id,
            file_name: file.name(),
            file_size: file.size() as u64,
            status: QueueItemStatus::Queued,
            bytes_uploaded: 0,
            error: None,
            upload_url: options.existing_url.clone(),
            file,
            options,
            started_at_ms: None,
            paused_at_ms: None,
            paused_accumulated_ms: 0.0,
        };
        let mut state = self.state;
        state.write().items.push(item);
        tracing::debug!(id, "queue: added item");
        id
    }

    /// Lists every persisted resumable upload for this queue's endpoint.
    /// Stale (>24h) entries and entries whose stored URL origin doesn't
    /// match the configured endpoint are filtered out.
    ///
    /// Use this on component mount to surface a "resume previous upload"
    /// affordance — e.g. show a banner listing the resumable filenames so
    /// the user knows which file to re-pick.
    pub fn scan_resumable(&self) -> Vec<crate::persistence::ResumableEntry> {
        crate::persistence::scan(&self.endpoint)
    }

    /// Add multiple files with the same options. Returns the new ids in input order.
    pub fn add_all(&self, files: Vec<File>, options: TusStartOptions) -> Vec<u64> {
        files
            .into_iter()
            .map(|f| self.add(f, options.clone()))
            .collect()
    }

    /// Pause every item currently held by a worker.
    pub fn pause_all(&self) {
        for w in self.workers.iter() {
            w.pause();
        }
    }

    /// Resume every paused worker. Does not start queued items — the scheduler
    /// pulls those automatically when a worker is idle.
    pub fn resume_all(&self) {
        for w in self.workers.iter() {
            w.resume();
        }
    }

    /// Abort every active worker and mark every queued item as Aborted.
    pub fn abort_all(&self) {
        for w in self.workers.iter() {
            w.abort();
        }
        let mut state = self.state;
        let mut s = state.write();
        let endpoint = self.endpoint.as_str();
        apply_abort_all(&mut s.items, |item| {
            remove_persisted_for_file(endpoint, &item.file)
        });
    }

    /// Add `file` by resuming the specific persisted entry the user picked.
    /// Returns `None` when the file no longer matches that entry.
    pub fn resume_entry(
        &self,
        entry: &crate::persistence::ResumableEntry,
        file: File,
        options: TusStartOptions,
    ) -> Option<u64> {
        if has_active_file_match(&self.state.read().items, &self.endpoint, &file) {
            tracing::debug!(
                file_name = %file.name(),
                "queue: resume_entry ignored; same file already in-flight",
            );
            return None;
        }
        let options = resume_options_for_entry(&self.endpoint, entry, &file, options)?;
        Some(self.add(file, options))
    }

    /// Pause an item if it is currently in a worker slot. No-op for queued items.
    pub fn pause_item(&self, id: u64) {
        if let Some(slot) = self.slot_for(id) {
            self.workers[slot].pause();
        } else {
            tracing::debug!(id, "queue: pause_item ignored (not in a worker slot)");
        }
    }

    /// Resume a paused item if it is currently in a worker slot.
    pub fn resume_item(&self, id: u64) {
        if let Some(slot) = self.slot_for(id) {
            self.workers[slot].resume();
        } else {
            tracing::debug!(id, "queue: resume_item ignored (not in a worker slot)");
        }
    }

    /// Abort an item. If it's in a worker slot, abort the worker. Otherwise
    /// mark the queued item as `Aborted` so it never starts.
    ///
    /// For slot-held items the engine's chunk-loop Abort handler clears any
    /// persisted localStorage entry. For queued items there's no engine in
    /// the loop to do that — so this method clears the entry directly.
    /// Without this, a queued item that was auto-resumed (i.e. `add()` set
    /// `existing_url` from a stored entry) would leave the entry behind on
    /// abort, and re-adding the same file would re-attach the dead URL.
    pub fn abort_item(&self, id: u64) {
        if let Some(slot) = self.slot_for(id) {
            self.workers[slot].abort();
            // The scheduler will observe the worker going Idle and clear the
            // slot + flip the item to Aborted on the next poll. The engine
            // clears persistence on its way through the chunk loop's Abort
            // arm.
            return;
        }
        let mut state = self.state;
        let mut s = state.write();
        if let Some(item) = s.items.iter_mut().find(|i| i.id == id)
            && item.status == QueueItemStatus::Queued
        {
            remove_persisted_for_file(&self.endpoint, &item.file);
            item.status = QueueItemStatus::Aborted;
            tracing::debug!(id, "queue: aborted queued item");
        }
    }

    /// Drop every `Complete` item from the queue.
    pub fn clear_complete(&self) {
        let mut state = self.state;
        state
            .write()
            .items
            .retain(|i| i.status != QueueItemStatus::Complete);
    }

    /// Drop every terminal item (`Complete`, `Aborted`, `Error`) from the
    /// queue. Useful for a single "Clear finished" button that handles
    /// the failure case alongside successes.
    ///
    /// Also clears each dropped item's persisted localStorage entry. The
    /// engine clears persistence on Complete and on user-driven Abort
    /// inside the chunk loop; an item that errored from the resume HEAD
    /// path (e.g. 401/403/404) bypasses both, so without this sweep the
    /// localStorage row would survive the queue row and re-attach on the
    /// next `add()`.
    pub fn clear_finished(&self) {
        let mut state = self.state;
        let mut s = state.write();
        let endpoint = self.endpoint.as_str();
        apply_clear_finished(&mut s.items, |item| {
            remove_persisted_for_file(endpoint, &item.file)
        });
    }

    /// Remove a specific item from the queue by id, regardless of status.
    /// If the item is in a worker slot, aborts the worker first; the
    /// scheduler frees the slot on its next poll.
    ///
    /// Also clears the persisted localStorage entry for the file so a
    /// subsequent `add()` of the same file doesn't re-attach a known-bad
    /// `existing_url` (the failure mode for items stuck in `Error` after
    /// an auth-failed HEAD on resume).
    pub fn remove_item(&self, id: u64) {
        if let Some(slot) = self.slot_for(id) {
            self.workers[slot].abort();
        }
        let mut state = self.state;
        let mut s = state.write();
        if let Some(item) = s.items.iter().find(|i| i.id == id) {
            self.remove_persisted_for(item);
        }
        s.items.retain(|i| i.id != id);
    }

    fn remove_persisted_for(&self, item: &TusQueueItem) {
        remove_persisted_for_file(&self.endpoint, &item.file);
    }

    /// Re-run a failed or aborted item from scratch. Clears the cached
    /// `upload_url` (the previous one may be a completed-or-expired
    /// resource that triggered the failure) and the bytes-uploaded
    /// counter, then flips the item back to `Queued` so the scheduler
    /// picks it up. Auto-resume via persistence still applies — if a
    /// stored entry exists it'll be honoured on the next start.
    ///
    /// No-op when the item is currently held by a worker (use
    /// [`Self::abort_item`] first if you really mean to retry from
    /// zero) or when the id doesn't exist.
    pub fn retry_item(&self, id: u64) {
        let mut state = self.state;
        let mut s = state.write();
        let slots = self.slot_assignments.borrow();
        match apply_retry_item(&mut s, &slots, id) {
            RetryDecision::Reset => {
                tracing::debug!(id, "queue: retrying item from scratch")
            }
            RetryDecision::ItemActive => tracing::warn!(
                id,
                "queue: retry_item ignored — item is active; abort first"
            ),
            RetryDecision::ItemNotFound => {}
            RetryDecision::WrongStatus => tracing::warn!(
                id,
                "queue: retry_item ignored — item is not in a terminal state"
            ),
        }
    }

    fn slot_for(&self, id: u64) -> Option<usize> {
        self.slot_assignments
            .borrow()
            .iter()
            .position(|s| *s == Some(id))
    }
}

/// Removes the localStorage entry keyed on `(endpoint, file)`. Best-effort
/// — a failure (no localStorage, quota issues) is logged but not surfaced.
///
/// Free function so it's testable without constructing a Dioxus-runtime-
/// scoped `TusQueueHandle`.
pub(crate) fn remove_persisted_for_file(endpoint: &str, file: &File) {
    let mk = crate::persistence::match_key(
        endpoint,
        &file.name(),
        file.size() as u64,
        file.last_modified(),
    );
    crate::persistence::remove(&mk);
}

pub(crate) fn resume_options_for_entry(
    endpoint: &str,
    entry: &crate::persistence::ResumableEntry,
    file: &File,
    mut options: TusStartOptions,
) -> Option<TusStartOptions> {
    if !crate::persistence::entry_is_resumable_for_file(
        endpoint,
        entry,
        &file.name(),
        file.size() as u64,
        file.last_modified(),
    ) {
        return None;
    }
    options.existing_url = Some(entry.upload_url.clone());
    Some(options)
}

pub(crate) fn apply_abort_all<F>(items: &mut [TusQueueItem], mut on_queued: F)
where
    F: FnMut(&TusQueueItem),
{
    for item in items.iter_mut() {
        if item.status == QueueItemStatus::Queued {
            on_queued(item);
            item.status = QueueItemStatus::Aborted;
        }
    }
}

/// Pure-logic core of [`TusQueueHandle::clear_finished`].
///
/// For every terminal item (`Complete`, `Aborted`, `Error`) calls
/// `on_terminal` (used by the handle to drop the item's persisted
/// localStorage entry), then drops the item from `items`. Extracted as
/// a free function so the persistence-clearing contract is testable
/// without a Dioxus-runtime-scoped `TusQueueHandle`.
pub(crate) fn apply_clear_finished<F>(items: &mut Vec<TusQueueItem>, mut on_terminal: F)
where
    F: FnMut(&TusQueueItem),
{
    for item in items.iter() {
        if matches!(
            item.status,
            QueueItemStatus::Complete | QueueItemStatus::Aborted | QueueItemStatus::Error
        ) {
            on_terminal(item);
        }
    }
    items.retain(|i| {
        !matches!(
            i.status,
            QueueItemStatus::Complete | QueueItemStatus::Aborted | QueueItemStatus::Error
        )
    });
}

/// One slot the scheduler wants to start on the next render cycle.
/// Returned by [`reconcile_tick`] so the caller (the `use_future` body in
/// [`use_tus_upload_queue`]) can call `workers[slot].start(file, options)`
/// without the tick logic itself depending on Dioxus signals — that's
/// what makes the tick unit-testable.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TickStart {
    pub slot: usize,
    pub item_id: u64,
}

/// Outcome of [`apply_retry_item`]. Lets the caller log the right warning
/// without the pure logic depending on `tracing`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RetryDecision {
    /// Item was reset to `Queued` and is now schedulable.
    Reset,
    /// Item is currently held by a worker — caller should abort first.
    ItemActive,
    /// No item with this id exists in the queue.
    ItemNotFound,
    /// Item exists but isn't in a terminal status (Error/Aborted/Complete).
    WrongStatus,
}

/// Pure-logic core of [`TusQueueHandle::retry_item`]. Resets a terminal
/// queue item back to `Queued` (clearing bytes_uploaded, upload_url,
/// existing_url, error, started_at_ms, and pause accounting) so the scheduler can re-pick it.
///
/// Free function so the rules — "active items can't be retried", "non-
/// terminal items can't be retried", "missing items are silent no-ops" —
/// are testable without spinning up a Dioxus runtime.
pub(crate) fn apply_retry_item(
    queue: &mut TusQueueState,
    slot_assignments: &[Option<u64>],
    id: u64,
) -> RetryDecision {
    if slot_assignments.contains(&Some(id)) {
        return RetryDecision::ItemActive;
    }
    let Some(item) = queue.items.iter_mut().find(|i| i.id == id) else {
        return RetryDecision::ItemNotFound;
    };
    if !matches!(
        item.status,
        QueueItemStatus::Error | QueueItemStatus::Aborted | QueueItemStatus::Complete
    ) {
        return RetryDecision::WrongStatus;
    }
    item.status = QueueItemStatus::Queued;
    item.bytes_uploaded = 0;
    item.error = None;
    item.upload_url = None;
    item.options.existing_url = None;
    item.started_at_ms = None;
    item.paused_at_ms = None;
    item.paused_accumulated_ms = 0.0;
    RetryDecision::Reset
}

/// One scheduler step. Mirrors per-worker progress into the queue items,
/// frees slots whose worker has reached a terminal status (Complete /
/// Error / Idle), and returns the list of free-slot → queued-item
/// assignments the caller should kick off.
///
/// All inputs are plain data structures so a wasm-bindgen test can drive
/// this directly without spinning up a Dioxus VirtualDom.
pub(crate) fn reconcile_tick(
    queue: &mut TusQueueState,
    slot_assignments: &mut [Option<u64>],
    worker_snapshots: &[TusUploadState],
    now_ms: f64,
) -> Vec<TickStart> {
    // Hard assert (not debug_assert): the loop indexes `worker_snapshots[slot]`
    // unguarded, so a length mismatch would index-panic in release with no
    // diagnostic. The cost is one length compare per tick (50ms cadence) —
    // negligible. Construction in `use_tus_upload_queue` makes the lengths
    // equal by construction, so this only catches future refactor mistakes.
    assert_eq!(
        slot_assignments.len(),
        worker_snapshots.len(),
        "reconcile_tick: slot_assignments and worker_snapshots must have equal length",
    );

    // Phase 1: reconcile each currently-assigned slot with its worker.
    for slot in 0..slot_assignments.len() {
        let Some(item_id) = slot_assignments[slot] else {
            continue;
        };
        let ws = &worker_snapshots[slot];

        let Some(item) = queue.items.iter_mut().find(|i| i.id == item_id) else {
            // Item vanished — typically `remove_item` of a slot-held item.
            //
            // Don't free the slot until the worker reports terminal state.
            // Otherwise Phase 2 below would reassign this slot to the next
            // queued item BEFORE the worker has drained its still-pending
            // Abort command, and that stale Abort would then write Idle to
            // the worker signal AFTER `start()`'s sync-stamp — fooling the
            // next tick's Idle arm into marking the freshly-assigned item
            // Aborted (a ghost-abort of a never-uploaded file).
            //
            // Once the worker reports Idle / Complete / Error its channel
            // has been drained past the Abort, so the next tick can safely
            // reassign.
            if matches!(
                ws.status,
                UploadStatus::Idle | UploadStatus::Complete | UploadStatus::Error
            ) {
                slot_assignments[slot] = None;
            }
            continue;
        };

        item.bytes_uploaded = ws.bytes_uploaded;
        if ws.upload_url.is_some() {
            item.upload_url = ws.upload_url.clone();
        }

        match ws.status {
            UploadStatus::Uploading => {
                if item.status == QueueItemStatus::Paused
                    && let Some(paused_at) = item.paused_at_ms.take()
                {
                    let paused_ms = now_ms - paused_at;
                    if paused_ms.is_finite() && paused_ms > 0.0 {
                        item.paused_accumulated_ms += paused_ms;
                    }
                }
                item.status = QueueItemStatus::Uploading;
            }
            UploadStatus::Paused => {
                if item.status != QueueItemStatus::Paused && item.paused_at_ms.is_none() {
                    item.paused_at_ms = Some(now_ms);
                }
                item.status = QueueItemStatus::Paused;
            }
            UploadStatus::Complete => {
                item.status = QueueItemStatus::Complete;
                slot_assignments[slot] = None;
            }
            UploadStatus::Error => {
                item.status = QueueItemStatus::Error;
                item.error = ws.error.clone();
                slot_assignments[slot] = None;
            }
            UploadStatus::Idle => {
                // Worker reverted to Idle — abort path. Mark Aborted (unless
                // already terminal) and free the slot.
                if !matches!(
                    item.status,
                    QueueItemStatus::Complete | QueueItemStatus::Error | QueueItemStatus::Aborted
                ) {
                    item.status = QueueItemStatus::Aborted;
                }
                slot_assignments[slot] = None;
            }
        }
    }

    // Phase 2: fill free slots from the queue head.
    let mut starts = Vec::new();
    for (slot, assignment) in slot_assignments.iter_mut().enumerate() {
        if assignment.is_some() {
            continue;
        }
        let next = queue
            .items
            .iter_mut()
            .find(|i| i.status == QueueItemStatus::Queued);
        let Some(item) = next else { continue };
        let id = item.id;
        item.status = QueueItemStatus::Uploading;
        item.started_at_ms = Some(now_ms);
        item.paused_at_ms = None;
        item.paused_accumulated_ms = 0.0;
        *assignment = Some(id);
        starts.push(TickStart { slot, item_id: id });
    }
    starts
}

/// Returns a reactive queue state signal and a handle to manage the queue.
///
/// Concurrency is fixed at 2 for 0.1.x.
///
/// # Example
/// ```rust,ignore
/// let (queue, handle) = use_tus_upload_queue(
///     TusConfig::new("https://tus.example.com/files"),
/// );
/// ```
pub fn use_tus_upload_queue(config: TusConfig) -> (ReadSignal<TusQueueState>, TusQueueHandle) {
    let endpoint = config.endpoint.clone();
    // Two single-file hooks at fixed positions. Calling `use_tus_upload`
    // twice statically is what keeps Dioxus's rules-of-hooks happy: the same
    // pair of hook calls fires on every render in the same order.
    let (worker0_state, worker0_handle) = use_tus_upload(config.clone());
    let (worker1_state, worker1_handle) = use_tus_upload(config);

    let queue_state: Signal<TusQueueState> = use_signal(TusQueueState::default);

    // Cross-render-stable bookkeeping. `use_hook` runs the closure exactly
    // once per component instance, so these `Rc`s persist for the component's
    // lifetime and are safely shared with the scheduler future.
    let next_id: Rc<RefCell<u64>> = use_hook(|| Rc::new(RefCell::new(0u64)));
    let slot_assignments: Rc<RefCell<Vec<Option<u64>>>> =
        use_hook(|| Rc::new(RefCell::new(vec![None; WORKER_COUNT])));
    let workers: Rc<Vec<TusUploadHandle>> =
        use_hook(|| Rc::new(vec![worker0_handle.clone(), worker1_handle.clone()]));

    // Scheduler future. Polls the worker states every 50ms and:
    //   - mirrors per-worker progress into the matching queue item
    //   - releases a slot when its worker reaches Idle (Aborted) / Complete /
    //     Error, flipping the queue item's status accordingly
    //   - assigns the next Queued item to any free slot
    {
        let slot_assignments = slot_assignments.clone();
        let workers = workers.clone();
        use_future(move || {
            let mut queue_state = queue_state;
            let worker_states = [worker0_state, worker1_state];
            let slot_assignments = slot_assignments.clone();
            let workers = workers.clone();
            async move {
                loop {
                    gloo_timers::future::TimeoutFuture::new(50).await;

                    let snapshots = [
                        worker_states[0].read().clone(),
                        worker_states[1].read().clone(),
                    ];
                    let starts = {
                        let mut state = queue_state.write();
                        let mut slots = slot_assignments.borrow_mut();
                        reconcile_tick(&mut state, &mut slots, &snapshots, js_sys::Date::now())
                    };

                    // Apply each start outside the borrow scope above so the
                    // worker.start() call doesn't hold the queue write lock.
                    for start in starts {
                        let file_and_options = queue_state
                            .read()
                            .items
                            .iter()
                            .find(|i| i.id == start.item_id)
                            .map(|i| (i.file.clone(), i.options.clone()));
                        if let Some((file, options)) = file_and_options {
                            tracing::debug!(
                                slot = start.slot,
                                id = start.item_id,
                                "queue: assigned item to slot"
                            );
                            workers[start.slot].start(file, options);
                        }
                    }
                }
            }
        });
    }

    let handle = TusQueueHandle {
        state: queue_state,
        next_id,
        workers,
        slot_assignments,
        endpoint,
    };
    (queue_state.into(), handle)
}

// =====================================================================
// Scheduler tick tests — wasm-bindgen-test because TusQueueItem holds a
// `web_sys::File`, which only constructs in a browser. The tests drive
// `reconcile_tick` directly with synthetic worker snapshots so they don't
// need a Dioxus VirtualDom or a real upload to flow through.
// =====================================================================
#[cfg(test)]
mod queue_tests {
    use super::*;
    use crate::state::{TusError, TusUploadState, UploadStatus};
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    fn make_file(name: &str, content: &[u8]) -> File {
        use js_sys::{Array, Uint8Array};
        let uint8 = Uint8Array::from(content);
        let array = Array::new();
        array.push(&uint8);
        let options = web_sys::FilePropertyBag::new();
        options.set_type("application/octet-stream");
        File::new_with_u8_array_sequence_and_options(&array, name, &options)
            .expect("File creation failed")
    }

    fn item(id: u64, name: &str, content: &[u8], status: QueueItemStatus) -> TusQueueItem {
        let file = make_file(name, content);
        let size = file.size() as u64;
        TusQueueItem {
            id,
            file_name: name.to_string(),
            file_size: size,
            status,
            bytes_uploaded: 0,
            error: None,
            upload_url: None,
            file,
            options: TusStartOptions::default(),
            started_at_ms: None,
            paused_at_ms: None,
            paused_accumulated_ms: 0.0,
        }
    }

    /// Regression: when the active worker reports `Error`, the scheduler
    /// MUST free the slot and assign the next queued item on the same
    /// tick. Without this, a single failing upload wedges the whole
    /// queue (see commit 24ebad9: "failed items are stuck").
    #[wasm_bindgen_test]
    fn error_frees_slot_and_starts_next_queued() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(1, "a.bin", b"hello", QueueItemStatus::Uploading));
        queue
            .items
            .push(item(2, "b.bin", b"world", QueueItemStatus::Queued));

        let mut slots = vec![Some(1u64), None];
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Error,
                error: Some(TusError::Server {
                    status: 403,
                    body: "denied".into(),
                }),
                ..Default::default()
            },
            TusUploadState::default(),
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 1_000.0);

        assert_eq!(slots[0], Some(2), "slot 0 reassigned to b.bin");
        assert_eq!(slots[1], None, "slot 1 still free");
        assert_eq!(queue.items[0].status, QueueItemStatus::Error);
        assert!(queue.items[0].error.is_some(), "error must be mirrored");
        assert_eq!(queue.items[1].status, QueueItemStatus::Uploading);
        assert_eq!(
            starts,
            vec![TickStart {
                slot: 0,
                item_id: 2
            }]
        );
    }

    /// Symmetric test for the Complete path: completed worker frees its
    /// slot and the next queued item flows onto the slot.
    #[wasm_bindgen_test]
    fn complete_frees_slot_and_starts_next_queued() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(1, "a.bin", b"hello", QueueItemStatus::Uploading));
        queue
            .items
            .push(item(2, "b.bin", b"world", QueueItemStatus::Queued));

        let mut slots = vec![Some(1u64), None];
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Complete,
                bytes_uploaded: 5,
                bytes_total: Some(5),
                ..Default::default()
            },
            TusUploadState::default(),
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 2_000.0);

        assert_eq!(slots[0], Some(2));
        assert_eq!(queue.items[0].status, QueueItemStatus::Complete);
        assert_eq!(queue.items[1].status, QueueItemStatus::Uploading);
        assert_eq!(
            starts,
            vec![TickStart {
                slot: 0,
                item_id: 2
            }]
        );
    }

    /// Idle from a non-terminal state means the user aborted. The slot
    /// must free; the item flips to Aborted (not Idle, which isn't a
    /// queue status).
    #[wasm_bindgen_test]
    fn idle_after_uploading_marks_aborted_and_frees_slot() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(1, "a.bin", b"hello", QueueItemStatus::Uploading));

        let mut slots = vec![Some(1u64), None];
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Idle,
                ..Default::default()
            },
            TusUploadState::default(),
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 0.0);

        assert_eq!(slots[0], None, "Idle must free the slot");
        assert_eq!(queue.items[0].status, QueueItemStatus::Aborted);
        assert!(starts.is_empty(), "no Queued items, no starts");
    }

    /// Both slots fill from the queue head when both workers are idle.
    #[wasm_bindgen_test]
    fn empty_slots_pick_up_queued_items_in_order() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(1, "a.bin", b"hello", QueueItemStatus::Queued));
        queue
            .items
            .push(item(2, "b.bin", b"world", QueueItemStatus::Queued));
        queue
            .items
            .push(item(3, "c.bin", b"!", QueueItemStatus::Queued));

        let mut slots = vec![None, None];
        let snapshots = [TusUploadState::default(), TusUploadState::default()];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 5_000.0);

        assert_eq!(slots[0], Some(1));
        assert_eq!(slots[1], Some(2));
        assert_eq!(queue.items[0].status, QueueItemStatus::Uploading);
        assert_eq!(queue.items[0].started_at_ms, Some(5_000.0));
        assert_eq!(queue.items[1].status, QueueItemStatus::Uploading);
        assert_eq!(
            queue.items[2].status,
            QueueItemStatus::Queued,
            "third item waits"
        );
        assert_eq!(
            starts,
            vec![
                TickStart {
                    slot: 0,
                    item_id: 1
                },
                TickStart {
                    slot: 1,
                    item_id: 2
                },
            ],
        );
    }

    /// Concurrency cap is respected: with both slots occupied, no new
    /// starts are issued even if a queued item is waiting.
    #[wasm_bindgen_test]
    fn full_slots_do_not_start_new_items() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(1, "a.bin", b"hello", QueueItemStatus::Uploading));
        queue
            .items
            .push(item(2, "b.bin", b"world", QueueItemStatus::Uploading));
        queue
            .items
            .push(item(3, "c.bin", b"!", QueueItemStatus::Queued));

        let mut slots = vec![Some(1u64), Some(2u64)];
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Uploading,
                bytes_uploaded: 2,
                ..Default::default()
            },
            TusUploadState {
                status: UploadStatus::Uploading,
                bytes_uploaded: 3,
                ..Default::default()
            },
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 0.0);

        assert!(starts.is_empty());
        assert_eq!(slots[0], Some(1));
        assert_eq!(slots[1], Some(2));
        assert_eq!(queue.items[2].status, QueueItemStatus::Queued);
        // Progress is mirrored.
        assert_eq!(queue.items[0].bytes_uploaded, 2);
        assert_eq!(queue.items[1].bytes_uploaded, 3);
    }

    /// A slot whose item has been removed from the queue (e.g. by
    /// `remove_item` after the worker already finished) frees cleanly when
    /// the worker reports a terminal status. Pairs with the regression test
    /// below for the in-flight case.
    #[wasm_bindgen_test]
    fn vanished_item_with_terminal_worker_frees_orphan_slot() {
        let mut queue = TusQueueState::default();
        // Item #1 is gone; the slot still references id=1.
        queue
            .items
            .push(item(2, "b.bin", b"world", QueueItemStatus::Queued));

        let mut slots = vec![Some(1u64), None];
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Complete,
                ..Default::default()
            },
            TusUploadState::default(),
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 0.0);

        // Slot 0 freed (orphan, worker terminal), slot 0 now picks up item 2.
        assert_eq!(slots[0], Some(2));
        assert_eq!(queue.items[0].status, QueueItemStatus::Uploading);
        assert_eq!(
            starts,
            vec![TickStart {
                slot: 0,
                item_id: 2
            }]
        );
    }

    /// Regression for the `remove_item` ghost-abort race.
    ///
    /// User clicks ✕ on the in-flight upload. `remove_item` enqueues Abort
    /// on the worker's command channel and synchronously drops the item
    /// from `queue.items`. The engine is mid-PATCH and hasn't yet processed
    /// the Abort. On this tick the worker still reports Uploading.
    ///
    /// Pre-fix, Phase 1's orphan branch unconditionally freed the slot; on
    /// the same tick Phase 2 picked the next queued item and called
    /// `start()` (sync-stamping Uploading) — but Abort was still in the
    /// worker's channel. The engine then processed Abort (state→Idle),
    /// then Start, then awaited POST. During the Idle window between Abort
    /// and POST, the next scheduler tick observed `slot=Some(B)` with
    /// worker Idle, hit the Idle arm, and marked B Aborted — a never-
    /// uploaded item flipped to a terminal state.
    ///
    /// Post-fix, Phase 1 only frees an orphan slot when the worker reports
    /// terminal status. With the worker still Uploading, the slot stays
    /// assigned and Phase 2 does NOT reassign THAT slot — buying time for
    /// the engine to drain Abort. (Phase 2 may still fill OTHER free slots,
    /// which is fine — the bug is specifically about reassigning the
    /// orphan slot whose worker still has a stale Abort in flight.)
    #[wasm_bindgen_test]
    fn vanished_item_with_uploading_worker_keeps_slot() {
        let mut queue = TusQueueState::default();
        // Item id=1 was just removed by remove_item; only the next queued item exists.
        queue
            .items
            .push(item(2, "next.bin", b"x", QueueItemStatus::Queued));

        let mut slots = vec![Some(1u64), None];
        // Worker on slot 0 hasn't processed Abort yet — still reports Uploading.
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Uploading,
                bytes_uploaded: 1024,
                ..Default::default()
            },
            TusUploadState::default(),
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 0.0);

        assert_eq!(
            slots[0],
            Some(1u64),
            "orphan slot 0 must NOT be reassigned while its worker is \
             still Uploading — the channel still has the stale Abort \
             that would race the next start()",
        );
        assert!(
            !starts.iter().any(|s| s.slot == 0),
            "Phase 2 must NOT start a new upload on the orphan slot \
             before the worker drains Abort; got {starts:?}",
        );
        // Pre-fix this assertion would fail: slot 0 would have been freed
        // and item 2 reassigned to it, so queue.items[0] (item 2) would
        // be Uploading on slot 0. Post-fix, item 2 is either still Queued
        // (if Phase 2 was blocked) or Uploading on slot 1.
        if let Some(start) = starts.first() {
            assert_ne!(
                start.slot, 0,
                "ghost-abort race regression: orphan slot reassigned"
            );
            assert_eq!(
                start.slot, 1,
                "queued item should fill the genuinely-free slot 1"
            );
            assert_eq!(start.item_id, 2);
        }
    }

    /// Companion to the test above: once the worker drains Abort and reports
    /// Idle, the orphan slot frees and Phase 2 picks up the next queued item.
    /// Pre-fix this would have happened on the FIRST tick (with a still-
    /// Uploading worker, racing the stale Abort); post-fix it correctly
    /// happens on the second tick.
    #[wasm_bindgen_test]
    fn vanished_item_orphan_slot_frees_after_worker_reaches_idle() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(2, "next.bin", b"x", QueueItemStatus::Queued));

        let mut slots = vec![Some(1u64), None];
        // Worker has now processed Abort; channel is drained.
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Idle,
                ..Default::default()
            },
            TusUploadState::default(),
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 50.0);

        assert_eq!(
            slots[0],
            Some(2u64),
            "Idle worker → orphan slot frees → Phase 2 reassigns"
        );
        assert_eq!(queue.items[0].status, QueueItemStatus::Uploading);
        assert_eq!(
            starts,
            vec![TickStart {
                slot: 0,
                item_id: 2
            }]
        );
    }

    /// Worker reports `Paused` → queue item mirrors `Paused`. Slot stays
    /// occupied. Closes the gap noted in the pre-merge review (the
    /// Paused arm of `reconcile_tick` had no test).
    #[wasm_bindgen_test]
    fn paused_status_is_mirrored_to_queue_item() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(1, "a.bin", b"hello", QueueItemStatus::Uploading));

        let mut slots = vec![Some(1u64), None];
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Paused,
                bytes_uploaded: 2,
                ..Default::default()
            },
            TusUploadState::default(),
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 1_000.0);

        assert_eq!(slots[0], Some(1), "paused worker keeps its slot");
        assert_eq!(queue.items[0].status, QueueItemStatus::Paused);
        assert_eq!(
            queue.items[0].bytes_uploaded, 2,
            "progress still mirrored while paused"
        );
        assert!(starts.is_empty());
    }

    #[wasm_bindgen_test]
    fn resumed_upload_speed_excludes_completed_pause_duration() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(1, "a.bin", b"hello world", QueueItemStatus::Uploading));
        queue.items[0].started_at_ms = Some(0.0);
        queue.items[0].bytes_uploaded = 4;

        let mut slots = vec![Some(1u64), None];
        let paused = [
            TusUploadState {
                status: UploadStatus::Paused,
                bytes_uploaded: 4,
                ..Default::default()
            },
            TusUploadState::default(),
        ];
        reconcile_tick(&mut queue, &mut slots, &paused, 1_000.0);

        let resumed = [
            TusUploadState {
                status: UploadStatus::Uploading,
                bytes_uploaded: 4,
                ..Default::default()
            },
            TusUploadState::default(),
        ];
        reconcile_tick(&mut queue, &mut slots, &resumed, 11_000.0);

        let speed = queue.items[0]
            .speed_bytes_per_sec(12_000.0)
            .expect("resumed item should have an active speed");
        assert!(
            (1.9..=2.1).contains(&speed),
            "speed should use 2 active seconds, not 12 wall-clock seconds: {speed}",
        );
    }

    #[wasm_bindgen_test]
    fn active_match_key_detects_specific_resume_duplicate() {
        let endpoint = "https://tus.example.com/files";
        let file = make_file("a.bin", b"hello");
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(1, "a.bin", b"hello", QueueItemStatus::Queued));

        assert!(
            has_active_file_match(&queue.items, endpoint, &file),
            "specific resume should see the existing non-terminal matching file",
        );

        queue.items[0].status = QueueItemStatus::Complete;
        assert!(
            !has_active_file_match(&queue.items, endpoint, &file),
            "terminal rows must not block a later resume",
        );
    }

    /// User aborts a paused item. The worker's Abort handler flips state
    /// from Paused → Idle; on the next tick the scheduler observes
    /// Idle on a non-terminal item and marks it `Aborted`. Closes the
    /// gap noted in the pre-merge review.
    #[wasm_bindgen_test]
    fn abort_during_paused_marks_aborted_and_frees_slot() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(1, "a.bin", b"hello", QueueItemStatus::Paused));

        let mut slots = vec![Some(1u64), None];
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Idle,
                ..Default::default()
            },
            TusUploadState::default(),
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 0.0);

        assert_eq!(slots[0], None);
        assert_eq!(queue.items[0].status, QueueItemStatus::Aborted);
        assert!(starts.is_empty());
    }

    #[wasm_bindgen_test]
    fn abort_all_marks_queued_items_and_invokes_cleanup() {
        let mut items = vec![
            item(1, "queued-a.bin", b"hello", QueueItemStatus::Queued),
            item(2, "uploading.bin", b"world", QueueItemStatus::Uploading),
            item(3, "queued-b.bin", b"again", QueueItemStatus::Queued),
            item(4, "done.bin", b"done", QueueItemStatus::Complete),
        ];
        let mut cleaned = Vec::new();

        apply_abort_all(&mut items, |item| cleaned.push(item.file_name.clone()));

        assert_eq!(items[0].status, QueueItemStatus::Aborted);
        assert_eq!(items[1].status, QueueItemStatus::Uploading);
        assert_eq!(items[2].status, QueueItemStatus::Aborted);
        assert_eq!(items[3].status, QueueItemStatus::Complete);
        assert_eq!(cleaned, vec!["queued-a.bin", "queued-b.bin"]);
    }

    #[wasm_bindgen_test]
    fn resume_entry_options_require_the_clicked_entry_to_match_the_file() {
        let endpoint = "https://tus.example.com/files";
        let file = make_file("report.pdf", b"contents");
        let other = make_file("other.pdf", b"contents");
        let entry = crate::persistence::ResumableEntry {
            match_key: crate::persistence::match_key(
                endpoint,
                &file.name(),
                file.size() as u64,
                file.last_modified(),
            ),
            endpoint: endpoint.into(),
            filename: file.name(),
            file_size: file.size() as u64,
            last_modified: file.last_modified(),
            upload_url: "https://tus.example.com/files/upload-id".into(),
            bytes_uploaded: 4,
            stored_at_ms: js_sys::Date::now(),
        };

        let options = resume_options_for_entry(endpoint, &entry, &file, TusStartOptions::default())
            .expect("matching file should produce resume options");
        assert_eq!(
            options.existing_url.as_deref(),
            Some("https://tus.example.com/files/upload-id"),
        );

        assert!(
            resume_options_for_entry(endpoint, &entry, &other, TusStartOptions::default())
                .is_none(),
            "picking a different file for the clicked row must not auto-resume another entry",
        );
    }

    // =====================================================================
    // Re-assignment race regression tests.
    //
    // Background: `TusUploadHandle::start()` (in src/hook.rs) only enqueues
    // a Start command — the worker's signal stays at its previous value
    // until `run_upload`'s post-create state.update lands. That post-create
    // window can be 50–300ms in production (one HTTP POST round trip),
    // longer than the scheduler's 50ms tick.
    //
    // The fix in hook.rs has `start()` synchronously stamp `Uploading` on
    // the worker signal so the next scheduler tick observes the correct
    // value. These tests exercise the post-fix invariant from the
    // scheduler side: when start() has correctly stamped `Uploading`, the
    // scheduler must NOT free the slot or mutate the item's status. They
    // pair with the existing `idle_after_uploading_marks_aborted_and_frees_slot`
    // test which pins the user-abort path (worker actively flips to Idle).
    //
    // Three variants because all three terminal states (Idle/Complete/Error)
    // could leak into a freshly-assigned slot's snapshot if `start()`
    // didn't stamp synchronously.
    // =====================================================================

    /// Post-fix invariant: a tick that observes a slot's worker as
    /// `Uploading` must not free the slot or mutate the item's status.
    /// (Pre-fix: worker would have been stale-Idle → wrongly aborted.)
    #[wasm_bindgen_test]
    fn second_tick_with_uploading_snapshot_keeps_slot() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(1, "a.bin", b"hello", QueueItemStatus::Uploading));
        queue.items[0].started_at_ms = Some(1_000.0);

        let mut slots = vec![Some(1u64), None];
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Uploading,
                bytes_uploaded: 0,
                ..Default::default()
            },
            TusUploadState::default(),
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 1_050.0);

        assert_eq!(slots[0], Some(1), "Uploading snapshot must not free a slot");
        assert_eq!(queue.items[0].status, QueueItemStatus::Uploading);
        assert!(starts.is_empty(), "no spurious starts");
    }

    /// Post-fix invariant for the post-Complete re-assignment case: when
    /// a slot was freed by a Complete worker on tick N and reassigned to
    /// a fresh item, on tick N+1 the worker MUST report Uploading (set
    /// synchronously by start()). The scheduler then mirrors progress
    /// without entering the Complete arm.
    ///
    /// Pre-fix failure mode: worker still reports Complete on tick N+1
    /// (run_upload hasn't reached its first state.update yet); the
    /// Complete arm fires, marks the freshly-assigned item Complete,
    /// frees the slot — i.e. ghost-completes a never-uploaded file.
    #[wasm_bindgen_test]
    fn reassignment_after_complete_keeps_slot_when_uploading() {
        let mut queue = TusQueueState::default();
        // Simulate state right after a reassignment tick:
        //  - prior item already gone (clear_finished or remove_item),
        //  - new item in slot 0, status Uploading.
        queue
            .items
            .push(item(2, "new.bin", b"fresh", QueueItemStatus::Uploading));
        queue.items[0].started_at_ms = Some(2_000.0);

        let mut slots = vec![Some(2u64), None];
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Uploading,
                bytes_uploaded: 0,
                ..Default::default()
            },
            TusUploadState::default(),
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 2_050.0);

        assert_eq!(slots[0], Some(2));
        assert_eq!(queue.items[0].status, QueueItemStatus::Uploading);
        assert!(starts.is_empty());
    }

    /// Post-fix invariant for the post-Error case. Without the fix in
    /// hook.rs, a worker that previously errored would still report
    /// `Error` on the first tick after re-assignment, and the Error arm
    /// would mark the freshly assigned item Error and free the slot.
    #[wasm_bindgen_test]
    fn reassignment_after_error_keeps_slot_when_uploading() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(3, "retry.bin", b"data", QueueItemStatus::Uploading));
        queue.items[0].started_at_ms = Some(3_000.0);

        let mut slots = vec![Some(3u64), None];
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Uploading,
                bytes_uploaded: 0,
                ..Default::default()
            },
            TusUploadState::default(),
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 3_050.0);

        assert_eq!(slots[0], Some(3));
        assert_eq!(queue.items[0].status, QueueItemStatus::Uploading);
        assert!(starts.is_empty());
    }

    /// `apply_retry_item` resets a failed item back to `Queued` so the
    /// scheduler picks it up again on the next tick. Pins the four
    /// outcomes (Reset / ItemActive / ItemNotFound / WrongStatus) that
    /// the public `TusQueueHandle::retry_item` method dispatches on.
    #[wasm_bindgen_test]
    fn retry_item_resets_failed_item_to_queued() {
        let mut queue = TusQueueState::default();
        let mut failed = item(1, "fail.bin", b"x", QueueItemStatus::Error);
        failed.bytes_uploaded = 99;
        failed.upload_url = Some("http://test.local/files/old-id".into());
        failed.options.existing_url = Some("http://test.local/files/old-id".into());
        failed.error = Some(TusError::Server {
            status: 500,
            body: "down".into(),
        });
        failed.started_at_ms = Some(123_456.0);
        queue.items.push(failed);

        let slots: Vec<Option<u64>> = vec![None, None];
        let decision = apply_retry_item(&mut queue, &slots, 1);

        assert_eq!(decision, RetryDecision::Reset);
        let item = &queue.items[0];
        assert_eq!(item.status, QueueItemStatus::Queued);
        assert_eq!(item.bytes_uploaded, 0);
        assert!(
            item.upload_url.is_none(),
            "stale upload_url must be dropped"
        );
        assert!(
            item.options.existing_url.is_none(),
            "stale existing_url must be dropped"
        );
        assert!(item.error.is_none());
        assert!(item.started_at_ms.is_none());
    }

    /// Retry from `Aborted` and from `Complete` are both allowed (the user
    /// might want to redo a previously-aborted upload, or replay a
    /// finished one against a new server). Same Reset path.
    #[wasm_bindgen_test]
    fn retry_item_works_from_aborted_and_complete() {
        for status in [QueueItemStatus::Aborted, QueueItemStatus::Complete] {
            let mut queue = TusQueueState::default();
            queue.items.push(item(7, "x.bin", b"y", status.clone()));
            let slots: Vec<Option<u64>> = vec![None, None];
            let decision = apply_retry_item(&mut queue, &slots, 7);
            assert_eq!(
                decision,
                RetryDecision::Reset,
                "expected Reset from {status:?}",
            );
            assert_eq!(queue.items[0].status, QueueItemStatus::Queued);
        }
    }

    /// Items currently held by a worker can't be retried — the user
    /// should abort_item first. Without this guard a re-Start command
    /// would race with the in-flight engine.
    #[wasm_bindgen_test]
    fn retry_item_refuses_active_item() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(2, "active.bin", b"z", QueueItemStatus::Uploading));
        let slots: Vec<Option<u64>> = vec![Some(2), None];

        let decision = apply_retry_item(&mut queue, &slots, 2);
        assert_eq!(decision, RetryDecision::ItemActive);
        // Item state untouched.
        assert_eq!(queue.items[0].status, QueueItemStatus::Uploading);
    }

    /// Non-terminal items (Queued, Paused) can't be retried — there's
    /// nothing to retry FROM. Caller gets WrongStatus and the item is
    /// left alone.
    #[wasm_bindgen_test]
    fn retry_item_refuses_non_terminal_status() {
        for status in [QueueItemStatus::Queued, QueueItemStatus::Paused] {
            let mut queue = TusQueueState::default();
            queue.items.push(item(3, "wait.bin", b"q", status.clone()));
            let slots: Vec<Option<u64>> = vec![None, None];
            let decision = apply_retry_item(&mut queue, &slots, 3);
            assert_eq!(
                decision,
                RetryDecision::WrongStatus,
                "expected WrongStatus from {status:?}",
            );
            assert_eq!(queue.items[0].status, status);
        }
    }

    /// Missing id is a silent no-op — the caller doesn't get a panic
    /// or an error, the queue is unchanged.
    #[wasm_bindgen_test]
    fn retry_item_missing_id_is_noop() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(1, "real.bin", b"x", QueueItemStatus::Error));
        let slots: Vec<Option<u64>> = vec![None, None];

        let decision = apply_retry_item(&mut queue, &slots, 999);
        assert_eq!(decision, RetryDecision::ItemNotFound);
        assert_eq!(queue.items.len(), 1);
        assert_eq!(queue.items[0].status, QueueItemStatus::Error);
    }

    /// Both workers complete on the same tick. Phase 1 frees both slots
    /// (Complete arm); Phase 2 fills them from the head of the queue.
    /// Matters for short, parallel uploads on a fast LAN where two
    /// chunks complete in the same 50ms window.
    #[wasm_bindgen_test]
    fn both_slots_complete_same_tick_pulls_two_queued_items() {
        let mut queue = TusQueueState::default();
        queue
            .items
            .push(item(1, "done-a.bin", b"hello", QueueItemStatus::Uploading));
        queue
            .items
            .push(item(2, "done-b.bin", b"world", QueueItemStatus::Uploading));
        queue
            .items
            .push(item(3, "next-a.bin", b"!!!", QueueItemStatus::Queued));
        queue
            .items
            .push(item(4, "next-b.bin", b"???", QueueItemStatus::Queued));

        let mut slots = vec![Some(1u64), Some(2u64)];
        let snapshots = [
            TusUploadState {
                status: UploadStatus::Complete,
                bytes_uploaded: 5,
                bytes_total: Some(5),
                ..Default::default()
            },
            TusUploadState {
                status: UploadStatus::Complete,
                bytes_uploaded: 5,
                bytes_total: Some(5),
                ..Default::default()
            },
        ];

        let starts = reconcile_tick(&mut queue, &mut slots, &snapshots, 9_000.0);

        assert_eq!(slots[0], Some(3), "slot 0 picks the first queued");
        assert_eq!(slots[1], Some(4), "slot 1 picks the second queued");
        assert_eq!(queue.items[0].status, QueueItemStatus::Complete);
        assert_eq!(queue.items[1].status, QueueItemStatus::Complete);
        assert_eq!(queue.items[2].status, QueueItemStatus::Uploading);
        assert_eq!(queue.items[2].started_at_ms, Some(9_000.0));
        assert_eq!(queue.items[3].status, QueueItemStatus::Uploading);
        assert_eq!(queue.items[3].started_at_ms, Some(9_000.0));
        assert_eq!(
            starts,
            vec![
                TickStart {
                    slot: 0,
                    item_id: 3
                },
                TickStart {
                    slot: 1,
                    item_id: 4
                },
            ],
        );
    }

    /// Regression for the `abort_item` queued-item persistence leak.
    ///
    /// When `add()` auto-resumes a queued item from a stored entry, calling
    /// `abort_item` on it must also clear the entry — otherwise re-adding
    /// the same file would re-attach the dead URL on the next session. The
    /// engine's chunk-loop Abort handler covers slot-held items; queued
    /// items never reach the engine, so the queue layer has to handle them.
    ///
    /// This test simulates the queued-abort code path: seed an entry, push
    /// a Queued item with the matching file, run the abort transition, and
    /// verify the entry is gone.
    #[wasm_bindgen_test]
    fn abort_queued_item_clears_persisted_entry() {
        let endpoint = "http://test.local/abort-queued-test";
        let file = make_file("queued.bin", b"abc");
        let mk = crate::persistence::match_key(
            endpoint,
            "queued.bin",
            file.size() as u64,
            file.last_modified(),
        );

        // Seed: a stored entry from a prior session that `add()` would have
        // auto-resumed.
        crate::persistence::remove(&mk); // clean slate
        let entry = crate::persistence::ResumableEntry {
            match_key: mk.clone(),
            endpoint: endpoint.into(),
            filename: "queued.bin".into(),
            file_size: file.size() as u64,
            last_modified: file.last_modified(),
            upload_url: format!("{endpoint}/queued-id"),
            bytes_uploaded: 0,
            stored_at_ms: js_sys::Date::now(),
        };
        crate::persistence::put(&entry).expect("seed entry");
        assert!(
            crate::persistence::get(endpoint, &mk).is_some(),
            "precondition"
        );

        // Build a Queued item whose file matches the seeded entry.
        let mut queue = TusQueueState::default();
        queue.items.push(TusQueueItem {
            id: 1,
            file_name: "queued.bin".into(),
            file_size: file.size() as u64,
            status: QueueItemStatus::Queued,
            bytes_uploaded: 0,
            error: None,
            upload_url: Some(entry.upload_url.clone()),
            file: file.clone(),
            options: TusStartOptions {
                existing_url: Some(entry.upload_url.clone()),
                ..Default::default()
            },
            started_at_ms: None,
            paused_at_ms: None,
            paused_accumulated_ms: 0.0,
        });

        // Mirror what `abort_item` does for the queued branch.
        if let Some(item) = queue.items.iter_mut().find(|i| i.id == 1) {
            assert_eq!(item.status, QueueItemStatus::Queued);
            remove_persisted_for_file(endpoint, &item.file);
            item.status = QueueItemStatus::Aborted;
        }

        assert!(
            crate::persistence::get(endpoint, &mk).is_none(),
            "abort on a queued item must clear its persisted entry, otherwise \
             re-add of the same file would re-attach the dead URL",
        );
        assert_eq!(queue.items[0].status, QueueItemStatus::Aborted);
    }

    /// `remove_persisted_for_file` clears the localStorage entry keyed
    /// on `(endpoint, file)`. Pins the escape hatch that
    /// `TusQueueHandle::remove_item` and `clear_finished` rely on for
    /// items stuck in `Error` after a non-410/404 HEAD on resume — the
    /// engine doesn't clear those, so the queue must.
    #[wasm_bindgen_test]
    fn remove_persisted_for_file_clears_localstorage_entry() {
        let endpoint = "http://test.local/remove-test";
        let file = make_file("stuck.bin", b"abc");
        let mk = crate::persistence::match_key(
            endpoint,
            "stuck.bin",
            file.size() as u64,
            file.last_modified(),
        );
        // Wipe any leftover from a prior test, then seed.
        crate::persistence::remove(&mk);
        let entry = crate::persistence::ResumableEntry {
            match_key: mk.clone(),
            endpoint: endpoint.into(),
            filename: "stuck.bin".into(),
            file_size: file.size() as u64,
            last_modified: file.last_modified(),
            upload_url: format!("{endpoint}/stuck-id"),
            bytes_uploaded: 0,
            stored_at_ms: js_sys::Date::now(),
        };
        crate::persistence::put(&entry).expect("seed entry");
        assert!(
            crate::persistence::get(endpoint, &mk).is_some(),
            "precondition"
        );

        remove_persisted_for_file(endpoint, &file);

        assert!(
            crate::persistence::get(endpoint, &mk).is_none(),
            "remove_persisted_for_file must drop the entry",
        );
    }

    /// `eta_seconds` returns `None` for `Paused` items. Without this, the
    /// elapsed clock keeps ticking while `bytes_uploaded` is frozen, so
    /// `speed_bytes_per_sec` trends toward zero and ETA grows without
    /// bound (UI shows "47 hours" on a paused upload). Regression for the
    /// queue-fix that adds `Paused` to the early-return guard.
    #[wasm_bindgen_test]
    fn eta_seconds_returns_none_for_paused_item() {
        let mut paused_item = item(1, "p.bin", b"hello world", QueueItemStatus::Paused);
        // Simulate: started 10s ago, already pushed 4 bytes, now paused.
        paused_item.started_at_ms = Some(0.0);
        paused_item.bytes_uploaded = 4;
        // `now_ms = 10_000` → elapsed_ms = 10_000.
        // Pre-fix: speed = 4 * 1000 / 10000 = 0.4 B/s, eta = (11-4)/0.4 = 17.5s.
        // Pre-fix shape: returns Some(17.5). After 10× longer pause, eta would
        // be 175s; after a full minute, ~6 min "ETA" — pure nonsense for a
        // paused upload. Post-fix: returns None.
        let eta = paused_item.eta_seconds(10_000.0);
        assert!(
            eta.is_none(),
            "eta_seconds on Paused must be None; got {eta:?}. \
             Without this guard the ETA grows unboundedly while paused.",
        );

        // Sanity: same item but Uploading → eta is Some.
        let mut uploading = paused_item.clone();
        uploading.status = QueueItemStatus::Uploading;
        assert!(
            uploading.eta_seconds(10_000.0).is_some(),
            "eta_seconds on Uploading should return Some when speed > 0",
        );
    }

    /// Regression for the persistence escape-hatch: an item stuck in
    /// `Error` (e.g. after a 401/403/404 on the resume HEAD) bypasses
    /// both the engine's own persistence-clear paths (Complete + user
    /// Abort), so its localStorage row would otherwise survive the queue
    /// row and re-attach on the next `add()`. `clear_finished` must
    /// sweep the row alongside the item itself for *every* terminal
    /// status, including Error. Pre-fix to commit 1613ed9 the row stuck
    /// around; this test pins the post-fix behaviour.
    #[wasm_bindgen_test]
    fn clear_finished_drops_persistence_for_error_items() {
        let endpoint = "http://test.local/clear-finished-error";

        // Three items: one Complete, one Error, one Queued (survivor).
        let complete_item = item(1, "complete.bin", b"done", QueueItemStatus::Complete);
        let error_item = item(2, "error.bin", b"failed", QueueItemStatus::Error);
        let queued_item = item(3, "queued.bin", b"waiting", QueueItemStatus::Queued);

        // Seed persisted rows for the two terminal items.
        for f in [&complete_item.file, &error_item.file] {
            let mk = crate::persistence::match_key(
                endpoint,
                &f.name(),
                f.size() as u64,
                f.last_modified(),
            );
            crate::persistence::remove(&mk);
            crate::persistence::put(&crate::persistence::ResumableEntry {
                match_key: mk.clone(),
                endpoint: endpoint.into(),
                filename: f.name(),
                file_size: f.size() as u64,
                last_modified: f.last_modified(),
                upload_url: format!("{endpoint}/{}-id", f.name()),
                bytes_uploaded: 0,
                stored_at_ms: js_sys::Date::now(),
            })
            .expect("seed persistence");
        }

        let mut items = vec![complete_item, error_item, queued_item];

        let mut cleared_files: Vec<String> = Vec::new();
        apply_clear_finished(&mut items, |it| {
            cleared_files.push(it.file_name.clone());
            remove_persisted_for_file(endpoint, &it.file);
        });

        // Exactly two items reported as terminal; the Error item must be
        // among them — that's the regression.
        assert_eq!(cleared_files.len(), 2, "got {cleared_files:?}");
        assert!(
            cleared_files.iter().any(|n| n == "error.bin"),
            "Error item must be reported terminal: {cleared_files:?}",
        );

        // Both terminal localStorage rows are gone.
        for name in ["complete.bin", "error.bin"] {
            // Match key is endpoint+name+size+lastmod; we don't have the
            // exact size/lastmod handy, but we can scan and assert no
            // row with this filename remains for our endpoint.
            let surviving: Vec<_> = crate::persistence::scan(endpoint)
                .into_iter()
                .filter(|e| e.filename == name)
                .collect();
            assert!(
                surviving.is_empty(),
                "{name} row must be cleared by clear_finished, found {surviving:?}",
            );
        }

        // Queued item is the only survivor.
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].file_name, "queued.bin");
    }
}
