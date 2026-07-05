//! Shared conformance scenarios for [`Locker`] implementations.
//!
//! Adapter crates can enable the `conformance-lock` feature in their
//! `dev-dependencies` and call this helper from their own async tests:
//!
//! ```toml
//! [dev-dependencies]
//! tus-protocol = { version = "...", features = ["conformance-lock"] }
//! ```
//!
//! The suite covers exclusivity, waited acquisition, timeout behavior,
//! release-on-drop or lease-timeout release expectations, and isolation
//! between independent upload IDs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::join;
use tokio::time::sleep;

use super::Locker;
use crate::error::Error;

static NEXT_UPLOAD_ID: AtomicU64 = AtomicU64::new(1);

/// How a held lock is expected to become available after its guard is dropped.
///
/// `#[non_exhaustive]` so the suite can grow new release models (matches on it
/// downstream must use a wildcard arm); existing variants stay constructible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReleaseExpectation {
    /// Dropping the returned [`LockGuard`](super::LockGuard) releases the lock
    /// immediately enough for a following `try_lock` to acquire it.
    GuardDrop,

    /// The backend relies on a real lease or timeout after guard drop.
    ///
    /// The duration is the maximum time the conformance suite should wait for
    /// the lease to expire and the upload ID to become lockable again.
    LeaseTimeout(Duration),
}

/// Timing and release behavior used by the locker conformance suite.
///
/// Start from [`LockerConformanceConfig::default`] and refine with the `with_*`
/// builders. The type is `#[non_exhaustive]` so new timing knobs can be added
/// without breaking adapters that construct it; fields stay public for reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct LockerConformanceConfig {
    /// Timeout used for lock acquisitions that should eventually succeed.
    pub acquisition_timeout: Duration,
    /// Timeout used when asserting that a contended lock times out.
    pub blocked_timeout: Duration,
    /// Delay before dropping a held guard in the waited-lock scenario.
    pub release_delay: Duration,
    /// Expected release behavior after a guard is dropped.
    pub release_expectation: ReleaseExpectation,
}

impl Default for LockerConformanceConfig {
    fn default() -> Self {
        Self {
            acquisition_timeout: Duration::from_secs(1),
            blocked_timeout: Duration::from_millis(50),
            release_delay: Duration::from_millis(25),
            release_expectation: ReleaseExpectation::GuardDrop,
        }
    }
}

impl LockerConformanceConfig {
    /// Sets the timeout used for lock acquisitions that should succeed.
    #[must_use]
    pub fn with_acquisition_timeout(mut self, timeout: Duration) -> Self {
        self.acquisition_timeout = timeout;
        self
    }

    /// Sets the timeout used when asserting that a contended lock times out.
    #[must_use]
    pub fn with_blocked_timeout(mut self, timeout: Duration) -> Self {
        self.blocked_timeout = timeout;
        self
    }

    /// Sets the delay before dropping a held guard in the waited-lock scenario.
    #[must_use]
    pub fn with_release_delay(mut self, delay: Duration) -> Self {
        self.release_delay = delay;
        self
    }

    /// Sets the expected release behavior after a guard is dropped.
    #[must_use]
    pub fn with_release_expectation(mut self, expectation: ReleaseExpectation) -> Self {
        self.release_expectation = expectation;
        self
    }
}

/// Asserts the complete `Locker` conformance suite.
///
/// This uses [`ReleaseExpectation::GuardDrop`], which is the expected behavior
/// for lockers that return guards owning the lock lifetime.
pub async fn assert_locker_semantics<L>(locker: &L)
where
    L: Locker + ?Sized,
{
    assert_locker_semantics_with(locker, LockerConformanceConfig::default()).await;
}

/// Asserts the complete `Locker` conformance suite with explicit timing and
/// release expectations.
///
/// Use this for adapters backed by a real lease where dropping the guard does
/// not release immediately but the lock becomes available after a bounded lease
/// timeout.
pub async fn assert_locker_semantics_with<L>(locker: &L, config: LockerConformanceConfig)
where
    L: Locker + ?Sized,
{
    try_lock_is_exclusive_and_releases(locker, config).await;
    lock_waits_until_current_guard_releases(locker, config).await;
    lock_times_out_while_guard_is_held(locker, config).await;
    dropped_guard_releases_or_expires(locker, config).await;
    independent_upload_ids_do_not_interfere(locker, config).await;
}

async fn try_lock_is_exclusive_and_releases<L>(locker: &L, config: LockerConformanceConfig)
where
    L: Locker + ?Sized,
{
    let id = upload_id("try-lock-exclusive");

    let guard = locker
        .try_lock(&id)
        .await
        .expect("first try_lock should succeed")
        .expect("first try_lock should acquire an unlocked upload");

    let second = locker
        .try_lock(&id)
        .await
        .expect("second try_lock should not error");
    assert!(
        second.is_none(),
        "try_lock must be exclusive for a single upload ID"
    );

    drop(guard);
    assert_released_or_expired(locker, &id, config, "try_lock guard").await;
}

async fn lock_waits_until_current_guard_releases<L>(locker: &L, config: LockerConformanceConfig)
where
    L: Locker + ?Sized,
{
    let id = upload_id("waited-lock");
    let guard = locker
        .lock(&id, config.acquisition_timeout)
        .await
        .expect("initial lock should acquire an unlocked upload");

    let waiter = locker.lock(&id, config.acquisition_timeout);
    let releaser = async move {
        sleep(config.release_delay).await;
        drop(guard);
    };

    let (waited, ()) = join!(waiter, releaser);
    let waited = waited.expect("lock should wait and then acquire after release");
    assert_eq!(waited.upload_id(), id);
    drop(waited);
}

async fn lock_times_out_while_guard_is_held<L>(locker: &L, config: LockerConformanceConfig)
where
    L: Locker + ?Sized,
{
    let id = upload_id("timeout");
    let _guard = locker
        .lock(&id, config.acquisition_timeout)
        .await
        .expect("initial lock should acquire an unlocked upload");

    let result = locker.lock(&id, config.blocked_timeout).await;
    assert!(
        matches!(result, Err(Error::LockTimeout(_))),
        "lock should return Error::LockTimeout when acquisition exceeds the timeout, got {result:?}"
    );
}

async fn dropped_guard_releases_or_expires<L>(locker: &L, config: LockerConformanceConfig)
where
    L: Locker + ?Sized,
{
    let id = upload_id("drop-release");
    let guard = locker
        .lock(&id, config.acquisition_timeout)
        .await
        .expect("initial lock should acquire an unlocked upload");

    drop(guard);

    assert_released_or_expired(locker, &id, config, "lock guard").await;
}

async fn assert_released_or_expired<L>(
    locker: &L,
    id: &str,
    config: LockerConformanceConfig,
    guard_kind: &str,
) where
    L: Locker + ?Sized,
{
    match config.release_expectation {
        ReleaseExpectation::GuardDrop => {
            let reacquired = locker
                .try_lock(id)
                .await
                .expect("try_lock after guard drop should not error");
            assert!(
                reacquired.is_some(),
                "dropping the {guard_kind} should release the lock immediately"
            );
        }
        ReleaseExpectation::LeaseTimeout(wait) => {
            let reacquired = locker.lock(id, wait).await.expect(
                "lock should become available after guard drop and the configured lease timeout",
            );
            drop(reacquired);
        }
    }
}

async fn independent_upload_ids_do_not_interfere<L>(locker: &L, config: LockerConformanceConfig)
where
    L: Locker + ?Sized,
{
    let first_id = upload_id("independent-a");
    let second_id = upload_id("independent-b");

    let first = locker
        .lock(&first_id, config.acquisition_timeout)
        .await
        .expect("first upload should lock");
    let second = locker
        .try_lock(&second_id)
        .await
        .expect("try_lock for a different upload should not error")
        .expect("different upload ID should lock independently");

    drop(first);
    let second_probe = locker
        .try_lock(&second_id)
        .await
        .expect("try_lock probe for second upload should not error");
    assert!(
        second_probe.is_none(),
        "releasing one upload ID must not release an independent upload ID"
    );

    drop(second);
}

fn upload_id(scenario: &str) -> String {
    let next = NEXT_UPLOAD_ID.fetch_add(1, Ordering::Relaxed);
    format!("conformance-lock-{scenario}-{next}")
}
