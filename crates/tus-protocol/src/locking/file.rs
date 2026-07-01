//! File-based locking implementation.
//!
//! This locker uses OS advisory locks on per-upload lock files for
//! coordination between processes on the same filesystem. The lock file may
//! remain after a process exits, but the OS releases the advisory lock when the
//! owning file descriptor is closed or the process dies.

use async_trait::async_trait;
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::time::sleep;

use super::{LockGuard, Locker};
use crate::error::{Error, Result};

/// File-based locker using OS advisory locks.
///
/// Creates `.lock` files in a specified directory to coordinate access between
/// multiple processes. The lock file is only a rendezvous point; advisory lock
/// ownership is held by an open file descriptor captured by [`LockGuard`].
pub struct FileLocker {
    directory: Arc<PathBuf>,
    /// How long to wait between lock attempts.
    retry_interval: Duration,
    /// Retained for API compatibility. OS advisory locks are released by the
    /// operating system when the owning process exits, so this implementation
    /// does not steal locks based on file age.
    lease_ttl: Duration,
}

impl FileLocker {
    /// Creates a new file locker.
    pub async fn new(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory).await.map_err(Error::Io)?;

        Ok(Self {
            directory: Arc::new(directory),
            retry_interval: Duration::from_millis(50),
            lease_ttl: Duration::from_secs(60),
        })
    }

    /// Creates a new file locker synchronously.
    pub fn new_sync(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&directory).map_err(Error::Io)?;

        Ok(Self {
            directory: Arc::new(directory),
            retry_interval: Duration::from_millis(50),
            lease_ttl: Duration::from_secs(60),
        })
    }

    /// Sets the retry interval for lock acquisition.
    #[must_use]
    pub fn with_retry_interval(mut self, interval: Duration) -> Self {
        self.retry_interval = interval;
        self
    }

    /// Sets the retained lease TTL value.
    ///
    /// OS advisory locks are released when the owning process exits, so this
    /// implementation does not steal locks based on file age. The setter is
    /// retained for callers that already configure it.
    #[must_use]
    pub fn with_lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }

    /// Returns the path to the lock file for an upload ID.
    fn lock_path(&self, upload_id: &str) -> Result<PathBuf> {
        validate_upload_id(upload_id)?;
        Ok(self.directory.join(format!("{}.lock", upload_id)))
    }

    fn open_lock_file(path: &Path) -> Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(Error::Io)
    }

    fn release_lock_file(file: File) {
        let _ = file.unlock();
    }
}

impl std::fmt::Debug for FileLocker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileLocker")
            .field("directory", &self.directory)
            .field("lease_ttl", &self.lease_ttl)
            .finish()
    }
}

#[async_trait]
impl Locker for FileLocker {
    fn name(&self) -> &'static str {
        "file"
    }

    async fn lock(&self, upload_id: &str, timeout: Duration) -> Result<LockGuard> {
        let path = self.lock_path(upload_id)?;
        let deadline = Instant::now() + timeout;

        loop {
            let file = Self::open_lock_file(&path)?;
            match file.try_lock_exclusive() {
                Ok(()) => {
                    return Ok(LockGuard::with_release(upload_id, move || {
                        Self::release_lock_file(file);
                    }));
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(Error::Io(error)),
            }

            if Instant::now() >= deadline {
                return Err(Error::LockTimeout(upload_id.to_string()));
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait_time = remaining.min(self.retry_interval);
            sleep(wait_time).await;
        }
    }

    async fn try_lock(&self, upload_id: &str) -> Result<Option<LockGuard>> {
        let path = self.lock_path(upload_id)?;
        let file = Self::open_lock_file(&path)?;

        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(LockGuard::with_release(upload_id, move || {
                Self::release_lock_file(file);
            }))),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(Error::Io(error)),
        }
    }

    async fn unlock(&self, upload_id: &str) -> Result<()> {
        let path = self.lock_path(upload_id)?;
        match Self::open_lock_file(&path) {
            Ok(file) => {
                let _ = file.unlock();
                Ok(())
            }
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn is_locked(&self, upload_id: &str) -> Result<bool> {
        let path = self.lock_path(upload_id)?;
        let file = Self::open_lock_file(&path)?;

        match file.try_lock_exclusive() {
            Ok(()) => {
                let _ = file.unlock();
                Ok(false)
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
            Err(error) => Err(Error::Io(error)),
        }
    }
}

fn validate_upload_id(id: &str) -> Result<()> {
    id.parse::<crate::protocol::UploadId>()?;
    if id.len() + ".lock".len() > MAX_LOCK_FILE_NAME_LEN {
        return Err(Error::InvalidUploadId(format!(
            "id plus .lock suffix is {} bytes; max {}",
            id.len() + ".lock".len(),
            MAX_LOCK_FILE_NAME_LEN
        )));
    }

    Ok(())
}

const MAX_LOCK_FILE_NAME_LEN: usize = 255;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::time::sleep;

    async fn create_test_locker() -> (FileLocker, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let locker = FileLocker::new(temp_dir.path()).await.unwrap();
        (locker, temp_dir)
    }

    #[tokio::test]
    async fn locker_conformance() {
        let (locker, _dir) = create_test_locker().await;

        crate::locking::conformance::assert_locker_semantics(&locker).await;
    }

    #[tokio::test]
    async fn test_lock_and_unlock() {
        let (locker, _dir) = create_test_locker().await;

        assert!(!locker.is_locked("test").await.unwrap());

        let guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert!(locker.is_locked("test").await.unwrap());

        drop(guard);
        assert!(!locker.is_locked("test").await.unwrap());
    }

    #[tokio::test]
    async fn test_lock_auto_release_on_drop() {
        let (locker, _dir) = create_test_locker().await;

        {
            let _guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
            assert!(locker.is_locked("test").await.unwrap());
        }

        assert!(!locker.is_locked("test").await.unwrap());

        let _guard2 = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert!(locker.is_locked("test").await.unwrap());
    }

    #[tokio::test]
    async fn test_try_lock() {
        let (locker, _dir) = create_test_locker().await;

        let guard1 = locker.try_lock("test").await.unwrap();
        assert!(guard1.is_some());

        let guard2 = locker.try_lock("test").await.unwrap();
        assert!(guard2.is_none());

        drop(guard1);
        let guard3 = locker.try_lock("test").await.unwrap();
        assert!(guard3.is_some());
    }

    #[tokio::test]
    async fn test_lock_timeout() {
        let (locker, _dir) = create_test_locker().await;

        let _guard = locker.lock("test", Duration::from_secs(10)).await.unwrap();

        let result = locker.lock("test", Duration::from_millis(100)).await;
        assert!(matches!(result, Err(Error::LockTimeout(_))));
    }

    #[tokio::test]
    async fn test_different_upload_ids() {
        let (locker, _dir) = create_test_locker().await;

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

    #[tokio::test]
    async fn test_unlock_nonexistent() {
        let (locker, _dir) = create_test_locker().await;

        locker.unlock("nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn rejects_path_traversal_ids() {
        let (locker, dir) = create_test_locker().await;
        let escape_id = format!("escape-{}", uuid::Uuid::new_v4().simple());
        let escape_path = dir.path().join(format!("../{escape_id}.lock"));
        let _ = std::fs::remove_file(&escape_path);

        let err = locker
            .try_lock(&format!("../{escape_id}"))
            .await
            .unwrap_err();

        assert!(matches!(err, Error::InvalidUploadId(_)));
        assert!(!escape_path.exists());
    }

    #[tokio::test]
    async fn rejects_ids_that_would_exceed_lock_file_name_limit() {
        let (locker, _dir) = create_test_locker().await;
        let err = locker.try_lock(&"a".repeat(251)).await.unwrap_err();

        assert!(matches!(err, Error::InvalidUploadId(_)));
    }

    #[tokio::test]
    async fn test_explicit_unlock_releases_held_guard_lock() {
        let (locker, _dir) = create_test_locker().await;

        let mut guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert!(locker.is_locked("test").await.unwrap());

        locker.unlock("test").await.unwrap();
        guard.disarm();

        assert!(!locker.is_locked("test").await.unwrap());
        let reacquired = locker.try_lock("test").await.unwrap();
        assert!(reacquired.is_some());
    }

    #[tokio::test]
    async fn test_active_lock_is_not_stolen_before_ttl() {
        let (locker, _dir) = create_test_locker().await;
        let locker = locker.with_lease_ttl(Duration::from_secs(1));

        let _guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        sleep(Duration::from_millis(25)).await;

        let guard = locker.try_lock("test").await.unwrap();
        assert!(guard.is_none());
    }

    #[tokio::test]
    async fn test_active_lock_is_not_stolen_after_ttl() {
        let (locker, _dir) = create_test_locker().await;
        let locker = locker.with_lease_ttl(Duration::from_millis(50));

        let _guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        sleep(Duration::from_millis(125)).await;

        let guard = locker.try_lock("test").await.unwrap();
        assert!(guard.is_none());
    }

    #[tokio::test]
    async fn test_abandoned_lock_file_does_not_block_acquisition() {
        let (locker, dir) = create_test_locker().await;

        std::fs::write(dir.path().join("test.lock"), "abandoned").unwrap();
        assert!(!locker.is_locked("test").await.unwrap());

        let recovered = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert!(locker.is_locked("test").await.unwrap());

        drop(recovered);
        assert!(!locker.is_locked("test").await.unwrap());
    }
}
