//! Locking trait for upload coordination.
//!
//! This module defines the `Locker` trait that ensures only one operation
//! can modify an upload at a time, preventing data corruption from concurrent
//! PATCH requests.
//!
//! # Implementations
//!
//! - `memory::MemoryLocker` - In-memory locking (feature: `lock-memory`)
//! - `file::FileLocker` - File-based locking (feature: `lock-file`)

// Feature-gated implementations
// Native implementations are not available in local-futures builds.
#[cfg(any(test, feature = "conformance-lock"))]
pub mod conformance;

#[cfg(all(feature = "lock-memory", not(feature = "local-futures")))]
pub mod memory;

#[cfg(all(feature = "lock-file", not(feature = "local-futures")))]
pub mod file;

use async_trait::async_trait;
use std::time::Duration;

use crate::error::Result;
use crate::runtime::{MaybeSend, MaybeSendSync};

/// Trait for coordinating access to uploads.
///
/// Implementations must ensure that:
/// 1. Only one caller can hold a lock for a given upload ID at a time
/// 2. Locks are automatically released after timeout (to prevent deadlocks)
/// 3. Lock acquisition is fair (roughly FIFO ordering preferred)
///
/// # Platform Support
///
/// This trait uses conditional bounds:
/// - On native platforms: implementations and returned futures must be `Send + Sync`
/// - With `local-futures`: `Send + Sync` is not required
#[cfg_attr(not(feature = "local-futures"), async_trait)]
#[cfg_attr(feature = "local-futures", async_trait(?Send))]
pub trait Locker: MaybeSendSync {
    /// Returns the locker backend name for logging/debugging.
    fn name(&self) -> &'static str;

    /// Acquires a lock for the given upload ID.
    ///
    /// This will wait up to `timeout` for the lock to become available.
    /// If the timeout expires before the lock is acquired, returns
    /// `Error::LockTimeout`.
    ///
    /// Returns a `LockGuard` that releases the lock when dropped.
    /// Custom locker implementations outside this crate can return
    /// [`LockGuard::new`] or [`LockGuard::with_release`] here.
    ///
    /// # Errors
    /// * `Error::LockTimeout` - If the lock cannot be acquired within timeout
    /// * `Error::Locked` - If the upload is locked and cannot be waited on
    async fn lock(&self, upload_id: &str, timeout: Duration) -> Result<LockGuard>;

    /// Attempts to acquire a lock without waiting.
    ///
    /// `Some(LockGuard)` if the lock was acquired, `None` if already locked.
    async fn try_lock(&self, upload_id: &str) -> Result<Option<LockGuard>>;

    /// Explicitly releases a lock.
    ///
    /// This is called automatically when the `LockGuard` is dropped, but
    /// can also be called explicitly. It's a no-op if the lock is not held.
    async fn unlock(&self, upload_id: &str) -> Result<()>;

    /// Checks if an upload is currently locked.
    ///
    /// This is primarily for debugging/monitoring. The result may be stale
    /// by the time it's used due to concurrent operations.
    async fn is_locked(&self, upload_id: &str) -> Result<bool>;
}

/// A guard that holds a lock on an upload.
///
/// The lock is automatically released when the guard is dropped.
/// Implementations should ensure the lock is released even if the
/// guard is not explicitly dropped (e.g., via timeout).
///
/// Third-party [`Locker`] implementations can construct this guard with
/// [`LockGuard::new`] or [`LockGuard::with_release`].
#[must_use = "dropping the LockGuard immediately releases the lock"]
pub struct LockGuard {
    /// The upload ID this lock is for.
    upload_id: String,
    /// Optional release callback (for custom cleanup).
    release_fn: Option<ReleaseFn>,
}

impl LockGuard {
    /// Creates a new lock guard.
    ///
    /// Intended for [`Locker`] implementations that track unlocks through a
    /// separate code path and only need an opaque RAII guard value.
    pub fn new(upload_id: impl Into<String>) -> Self {
        Self {
            upload_id: upload_id.into(),
            release_fn: None,
        }
    }

    /// Creates a lock guard with a release callback.
    ///
    /// Native builds require a `Send` callback. `local-futures` builds allow
    /// callbacks that are local to a single-threaded runtime.
    pub fn with_release<F>(upload_id: impl Into<String>, release_fn: F) -> Self
    where
        F: FnOnce() + MaybeSend + 'static,
    {
        Self {
            upload_id: upload_id.into(),
            release_fn: Some(Box::new(release_fn)),
        }
    }

    /// Returns the upload ID.
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }

    /// Disables the drop-time release callback.
    ///
    /// Callers that perform an explicit unlock should disarm the guard before
    /// dropping it so the backend does not observe a duplicate release.
    pub fn disarm(&mut self) {
        self.release_fn = None;
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(release_fn) = self.release_fn.take() {
            release_fn();
        }
    }
}

impl std::fmt::Debug for LockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockGuard")
            .field("upload_id", &self.upload_id)
            .finish()
    }
}

/// A no-op locker that doesn't actually lock anything.
///
/// Useful for single-threaded environments (like local runtime per-upload
/// Durable Object) where locking isn't needed, or for testing.
#[derive(Debug, Clone, Default)]
pub struct NoopLocker;

impl NoopLocker {
    /// Creates a new no-op locker.
    pub fn new() -> Self {
        Self
    }
}

#[cfg_attr(not(feature = "local-futures"), async_trait)]
#[cfg_attr(feature = "local-futures", async_trait(?Send))]
impl Locker for NoopLocker {
    fn name(&self) -> &'static str {
        "noop"
    }

    async fn lock(&self, upload_id: &str, _timeout: Duration) -> Result<LockGuard> {
        Ok(LockGuard::new(upload_id))
    }

    async fn try_lock(&self, upload_id: &str) -> Result<Option<LockGuard>> {
        Ok(Some(LockGuard::new(upload_id)))
    }

    async fn unlock(&self, _upload_id: &str) -> Result<()> {
        Ok(())
    }

    async fn is_locked(&self, _upload_id: &str) -> Result<bool> {
        Ok(false)
    }
}

#[cfg(feature = "local-futures")]
type ReleaseFn = Box<dyn FnOnce()>;

#[cfg(not(feature = "local-futures"))]
type ReleaseFn = Box<dyn FnOnce() + Send>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn test_lock_guard_basic() {
        let guard = LockGuard::new("test-upload");
        assert_eq!(guard.upload_id(), "test-upload");
    }

    #[test]
    fn test_lock_guard_with_release() {
        let released = Arc::new(AtomicBool::new(false));
        let released_clone = released.clone();

        {
            let _guard = LockGuard::with_release("test-upload", move || {
                released_clone.store(true, Ordering::SeqCst);
            });
            assert!(!released.load(Ordering::SeqCst));
        }

        assert!(released.load(Ordering::SeqCst));
    }

    #[cfg(feature = "local-futures")]
    #[test]
    fn test_lock_guard_release_accepts_non_send_callback_in_local_mode() {
        use std::cell::Cell;
        use std::rc::Rc;

        let released = Rc::new(Cell::new(false));
        let released_for_callback = released.clone();

        drop(LockGuard::with_release("upload", move || {
            released_for_callback.set(true);
        }));

        assert!(released.get());
    }

    #[tokio::test]
    async fn test_noop_locker() {
        let locker = NoopLocker::new();

        // Lock should always succeed
        let guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert_eq!(guard.upload_id(), "test");

        // Try lock should also succeed
        let guard2 = locker.try_lock("test").await.unwrap();
        assert!(guard2.is_some());

        // Is locked should always return false
        assert!(!locker.is_locked("test").await.unwrap());

        // Unlock should be a no-op
        assert!(locker.unlock("test").await.is_ok());
    }
}
