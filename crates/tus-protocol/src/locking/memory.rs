//! In-memory locking implementation.
//!
//! This locker uses in-process synchronization for coordination. Suitable for
//! single-process use but does not work across multiple processes.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

use super::{LockGuard, Locker};
use crate::error::{Error, Result};

/// In-memory locker using tokio synchronization primitives.
///
/// Provides upload-level locking within a single process. Each upload ID maps
/// to a single-permit [`Semaphore`], so waiters are queued (FIFO) inside the
/// semaphore itself and a release always wakes the next waiter; there is no
/// wakeup window to lose between checking the lock and starting to wait.
///
/// Entries are removed from the internal map as soon as an upload ID has no
/// holder and no waiters, so the map does not grow with the set of requested
/// IDs.
pub struct MemoryLocker {
    locks: Arc<std::sync::Mutex<HashMap<String, Arc<Semaphore>>>>,
}

impl MemoryLocker {
    /// Creates a new memory locker.
    pub fn new() -> Self {
        Self {
            locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Reports whether an upload is currently locked.
    ///
    /// Monitoring/test helper; the answer may be stale as soon as it is
    /// returned because other tasks can lock or release concurrently.
    pub fn is_locked(&self, upload_id: &str) -> bool {
        self.locks
            .lock()
            .map(|locks| {
                locks
                    .get(upload_id)
                    .is_some_and(|semaphore| semaphore.available_permits() == 0)
            })
            .unwrap_or(false)
    }

    /// Returns the semaphore for an upload ID, creating the entry on demand.
    fn entry(&self, upload_id: &str) -> Result<Arc<Semaphore>> {
        let mut locks = self
            .locks
            .lock()
            .map_err(|_| Error::Internal("memory locker mutex poisoned".to_string()))?;

        Ok(Arc::clone(
            locks
                .entry(upload_id.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(1))),
        ))
    }

    /// Removes the map entry for an upload ID when it is idle.
    ///
    /// The caller must have dropped every `Arc<Semaphore>` clone it held for
    /// this ID before calling. Under the map mutex, a strong count of one means
    /// the map holds the only reference (no holder, no waiters), so removal
    /// cannot strand anyone: new interest can only be registered through the
    /// map, which requires the same mutex.
    fn remove_if_idle(locks: &std::sync::Mutex<HashMap<String, Arc<Semaphore>>>, upload_id: &str) {
        if let Ok(mut locks) = locks.lock()
            && let Some(entry) = locks.get(upload_id)
            && Arc::strong_count(entry) == 1
            && entry.available_permits() == 1
        {
            locks.remove(upload_id);
        }
    }
}

impl Default for MemoryLocker {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MemoryLocker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryLocker").finish()
    }
}

#[async_trait]
impl Locker for MemoryLocker {
    fn name(&self) -> &'static str {
        "memory"
    }

    async fn lock(&self, upload_id: &str, timeout: Duration) -> Result<LockGuard> {
        let semaphore = self.entry(upload_id)?;

        // `acquire_owned` registers the waiter inside the semaphore before any
        // release can happen, so a concurrent release is never lost: it either
        // hands the permit to this waiter or leaves the permit available for
        // the acquire to take immediately.
        let permit =
            match tokio::time::timeout(timeout, Arc::clone(&semaphore).acquire_owned()).await {
                Ok(Ok(permit)) => permit,
                Ok(Err(_closed)) => {
                    drop(semaphore);
                    Self::remove_if_idle(&self.locks, upload_id);
                    return Err(Error::Internal(format!(
                        "memory locker semaphore closed for upload: {upload_id}"
                    )));
                }
                Err(_elapsed) => {
                    drop(semaphore);
                    Self::remove_if_idle(&self.locks, upload_id);
                    return Err(Error::LockTimeout(upload_id.to_string()));
                }
            };
        drop(semaphore);

        let locks = Arc::clone(&self.locks);
        let id = upload_id.to_string();
        Ok(LockGuard::with_release(upload_id, move || {
            // Dropping the permit releases the lock (and drops the permit's
            // own semaphore reference); then the idle entry can be reaped.
            drop(permit);
            Self::remove_if_idle(&locks, &id);
        }))
    }

    async fn try_lock(&self, upload_id: &str) -> Result<Option<LockGuard>> {
        let semaphore = self.entry(upload_id)?;

        match Arc::clone(&semaphore).try_acquire_owned() {
            Ok(permit) => {
                drop(semaphore);
                let locks = Arc::clone(&self.locks);
                let id = upload_id.to_string();
                Ok(Some(LockGuard::with_release(upload_id, move || {
                    drop(permit);
                    Self::remove_if_idle(&locks, &id);
                })))
            }
            Err(_) => {
                drop(semaphore);
                Self::remove_if_idle(&self.locks, upload_id);
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::time::sleep;

    fn entry_count(locker: &MemoryLocker) -> usize {
        locker.locks.lock().unwrap().len()
    }

    #[tokio::test]
    async fn locker_conformance() {
        let locker = MemoryLocker::new();

        crate::locking::conformance::assert_locker_semantics(&locker).await;
    }

    #[tokio::test]
    async fn test_lock_and_unlock() {
        let locker = MemoryLocker::new();

        assert!(!locker.is_locked("test"));

        let guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert!(locker.is_locked("test"));

        drop(guard);
        // Lock should be automatically released when guard is dropped
        assert!(!locker.is_locked("test"));
    }

    #[tokio::test]
    async fn test_lock_auto_release_on_drop() {
        let locker = MemoryLocker::new();

        // Acquire lock in a scope
        {
            let _guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
            assert!(locker.is_locked("test"));
        }

        // Lock should be automatically released when guard goes out of scope
        assert!(!locker.is_locked("test"));

        // Should be able to acquire the lock again immediately
        let _guard2 = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert!(locker.is_locked("test"));
    }

    #[tokio::test]
    async fn test_try_lock() {
        let locker = MemoryLocker::new();

        // First try_lock should succeed
        let guard1 = locker.try_lock("test").await.unwrap();
        assert!(guard1.is_some());

        // Second try_lock should fail (lock is held)
        let guard2 = locker.try_lock("test").await.unwrap();
        assert!(guard2.is_none());

        // After dropping guard1, try_lock should succeed again (auto-release)
        drop(guard1);
        let guard3 = locker.try_lock("test").await.unwrap();
        assert!(guard3.is_some());
    }

    #[tokio::test]
    async fn test_lock_timeout() {
        let locker = MemoryLocker::new();

        // Acquire lock
        let _guard = locker.lock("test", Duration::from_secs(10)).await.unwrap();

        // Try to acquire with short timeout - should fail
        let result = locker.lock("test", Duration::from_millis(50)).await;
        assert!(matches!(result, Err(Error::LockTimeout(_))));
    }

    #[tokio::test]
    async fn test_concurrent_locks() {
        let locker = Arc::new(MemoryLocker::new());
        let counter = Arc::new(AtomicU32::new(0));

        let handles: Vec<_> = (0..5)
            .map(|_i| {
                let locker = Arc::clone(&locker);
                let counter = Arc::clone(&counter);
                tokio::spawn(async move {
                    let _guard = locker.lock("shared", Duration::from_secs(5)).await.unwrap();

                    // Critical section
                    let val = counter.load(Ordering::SeqCst);
                    sleep(Duration::from_millis(10)).await;
                    counter.store(val + 1, Ordering::SeqCst);

                    // Lock is automatically released when _guard goes out of scope
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        // All 5 increments should have completed without race conditions
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn test_different_upload_ids() {
        let locker = MemoryLocker::new();

        // Lock different uploads - should not interfere
        let guard1 = locker
            .lock("upload-1", Duration::from_secs(1))
            .await
            .unwrap();
        let _guard2 = locker
            .lock("upload-2", Duration::from_secs(1))
            .await
            .unwrap();

        assert!(locker.is_locked("upload-1"));
        assert!(locker.is_locked("upload-2"));

        drop(guard1);
        assert!(!locker.is_locked("upload-1"));
        assert!(locker.is_locked("upload-2"));
    }

    /// Regression test for the lost-wakeup race in the old Notify-based
    /// implementation: a release between a waiter registering interest and
    /// starting to wait was dropped, leaving the waiter to sleep until the
    /// full lock timeout and then fail with `LockTimeout` even though the
    /// lock was free.
    ///
    /// With heavy contention and short hold times, every acquisition must
    /// succeed well within the (generous) timeout. Under the old
    /// implementation this test failed within a few hundred iterations.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn contended_lock_release_cycles_never_time_out_while_lock_is_free() {
        const TASKS: usize = 8;
        const ITERATIONS: usize = 500;

        let locker = Arc::new(MemoryLocker::new());

        let handles: Vec<_> = (0..TASKS)
            .map(|_| {
                let locker = Arc::clone(&locker);
                tokio::spawn(async move {
                    for _ in 0..ITERATIONS {
                        // Locks are only ever held momentarily, so a timeout
                        // here means a wakeup was lost, not real contention.
                        let guard = locker
                            .lock("contended", Duration::from_secs(5))
                            .await
                            .expect("lock must not time out while locks are only held briefly");
                        drop(guard);
                        tokio::task::yield_now().await;
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        assert!(!locker.is_locked("contended"));
        assert_eq!(entry_count(&locker), 0);
    }

    #[tokio::test]
    async fn lock_release_removes_map_entries() {
        let locker = MemoryLocker::new();

        for i in 0..64 {
            let id = format!("upload-{i}-{}", uuid::Uuid::new_v4().simple());
            let guard = locker.lock(&id, Duration::from_secs(1)).await.unwrap();
            drop(guard);
        }

        assert_eq!(entry_count(&locker), 0);
    }

    #[tokio::test]
    async fn try_lock_of_missing_id_leaves_no_map_entry_behind() {
        let locker = MemoryLocker::new();

        let guard = locker.try_lock("probe").await.unwrap();
        assert_eq!(entry_count(&locker), 1);
        drop(guard);
        assert_eq!(entry_count(&locker), 0);

        // A failed try_lock against a held lock must not leak either.
        let _held = locker.lock("held", Duration::from_secs(1)).await.unwrap();
        let missed = locker.try_lock("held").await.unwrap();
        assert!(missed.is_none());
        drop(_held);
        assert_eq!(entry_count(&locker), 0);
    }

    #[tokio::test]
    async fn timed_out_waiter_does_not_remove_entry_held_by_owner() {
        let locker = MemoryLocker::new();

        let guard = locker.lock("test", Duration::from_secs(10)).await.unwrap();
        let result = locker.lock("test", Duration::from_millis(20)).await;
        assert!(matches!(result, Err(Error::LockTimeout(_))));

        // The holder is unaffected and the entry is reaped on its release.
        assert!(locker.is_locked("test"));
        drop(guard);
        assert!(!locker.is_locked("test"));
        assert_eq!(entry_count(&locker), 0);
    }
}
