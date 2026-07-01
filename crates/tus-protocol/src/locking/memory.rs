//! In-memory locking implementation.
//!
//! This locker uses in-process mutexes for coordination. Suitable for
//! single-process use but does not work across multiple processes.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

use super::{LockGuard, Locker};
use crate::error::{Error, Result};

/// In-memory locker using tokio synchronization primitives.
///
/// Provides upload-level locking within a single process. Uses async
/// notification to efficiently wait for locks without busy-polling.
///
/// This implementation uses a `std::sync::Mutex` internally so that locks
/// can be released synchronously when the `LockGuard` is dropped.
pub struct MemoryLocker {
    locks: Arc<std::sync::Mutex<HashMap<String, LockEntry>>>,
}

impl MemoryLocker {
    /// Creates a new memory locker.
    pub fn new() -> Self {
        Self {
            locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Internal method to release a lock synchronously.
    /// This is called from the LockGuard's Drop implementation.
    fn release_lock(locks: &std::sync::Mutex<HashMap<String, LockEntry>>, upload_id: &str) {
        if let Ok(mut guard) = locks.lock()
            && let Some(entry) = guard.get_mut(upload_id)
        {
            entry.locked = false;
            entry.notify.notify_waiters();
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
        let deadline = Instant::now() + timeout;

        loop {
            // Try to acquire the lock
            let notify = {
                let mut locks = self
                    .locks
                    .lock()
                    .map_err(|_| Error::LockTimeout(upload_id.to_string()))?;
                let entry = locks.entry(upload_id.to_string()).or_default();

                if !entry.locked {
                    entry.locked = true;
                    // Create guard with release callback that will unlock when dropped
                    let locks_clone = Arc::clone(&self.locks);
                    let id = upload_id.to_string();

                    return Ok(LockGuard::with_release(upload_id, move || {
                        Self::release_lock(&locks_clone, &id);
                    }));
                }

                // Lock is held, get notify handle for waiting
                Arc::clone(&entry.notify)
            };

            // Check timeout
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::LockTimeout(upload_id.to_string()));
            }

            // Wait for notification or timeout
            let wait_result = tokio::time::timeout(remaining, notify.notified()).await;
            if wait_result.is_err() {
                return Err(Error::LockTimeout(upload_id.to_string()));
            }
            // Loop and try again
        }
    }

    async fn try_lock(&self, upload_id: &str) -> Result<Option<LockGuard>> {
        let mut locks = self
            .locks
            .lock()
            .map_err(|_| Error::LockTimeout(upload_id.to_string()))?;
        let entry = locks.entry(upload_id.to_string()).or_default();

        if !entry.locked {
            entry.locked = true;
            // Create guard with release callback
            let locks_clone = Arc::clone(&self.locks);
            let id = upload_id.to_string();

            Ok(Some(LockGuard::with_release(upload_id, move || {
                Self::release_lock(&locks_clone, &id);
            })))
        } else {
            Ok(None)
        }
    }

    async fn unlock(&self, upload_id: &str) -> Result<()> {
        let mut locks = self
            .locks
            .lock()
            .map_err(|_| Error::LockTimeout(upload_id.to_string()))?;
        if let Some(entry) = locks.get_mut(upload_id) {
            entry.locked = false;
            entry.notify.notify_waiters();
        }
        Ok(())
    }

    async fn is_locked(&self, upload_id: &str) -> Result<bool> {
        let locks = self
            .locks
            .lock()
            .map_err(|_| Error::LockTimeout(upload_id.to_string()))?;
        Ok(locks.get(upload_id).map(|e| e.locked).unwrap_or(false))
    }
}

/// Entry for a single upload's lock state.
struct LockEntry {
    /// Whether the lock is currently held.
    locked: bool,
    /// Notification for waiters when the lock is released.
    notify: Arc<Notify>,
}

impl Default for LockEntry {
    fn default() -> Self {
        Self {
            locked: false,
            notify: Arc::new(Notify::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::time::sleep;

    #[tokio::test]
    async fn locker_conformance() {
        let locker = MemoryLocker::new();

        crate::locking::conformance::assert_locker_semantics(&locker).await;
    }

    #[tokio::test]
    async fn test_lock_and_unlock() {
        let locker = MemoryLocker::new();

        assert!(!locker.is_locked("test").await.unwrap());

        let guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert!(locker.is_locked("test").await.unwrap());

        drop(guard);
        // Lock should be automatically released when guard is dropped
        assert!(!locker.is_locked("test").await.unwrap());
    }

    #[tokio::test]
    async fn test_lock_auto_release_on_drop() {
        let locker = MemoryLocker::new();

        // Acquire lock in a scope
        {
            let _guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
            assert!(locker.is_locked("test").await.unwrap());
        }

        // Lock should be automatically released when guard goes out of scope
        assert!(!locker.is_locked("test").await.unwrap());

        // Should be able to acquire the lock again immediately
        let _guard2 = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert!(locker.is_locked("test").await.unwrap());
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

        assert!(locker.is_locked("upload-1").await.unwrap());
        assert!(locker.is_locked("upload-2").await.unwrap());

        drop(guard1);
        assert!(!locker.is_locked("upload-1").await.unwrap());
        assert!(locker.is_locked("upload-2").await.unwrap());
    }
}
