//! File-based locking implementation.
//!
//! This locker uses OS advisory locks on per-upload lock files for
//! coordination between processes on the same filesystem. Lock files are
//! removed on release (while still holding the advisory lock), so the lock
//! directory is bounded by in-flight lock activity rather than by every upload
//! ID ever requested. A release racing with a concurrent contender can leave a
//! freshly recreated lock file behind; such residue is bounded by concurrent
//! contention and is reused or removed by the next lock cycle for that ID.

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
/// Lock files are unlinked on release while the advisory lock is still held.
pub struct FileLocker {
    directory: Arc<PathBuf>,
    /// How long to wait between lock attempts.
    retry_interval: Duration,
}

impl FileLocker {
    /// Creates a new file locker.
    pub async fn new(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory).await.map_err(Error::Io)?;

        Ok(Self {
            directory: Arc::new(directory),
            retry_interval: Duration::from_millis(50),
        })
    }

    /// Creates a new file locker synchronously.
    pub fn new_sync(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&directory).map_err(Error::Io)?;

        Ok(Self {
            directory: Arc::new(directory),
            retry_interval: Duration::from_millis(50),
        })
    }

    /// Sets the retry interval for lock acquisition.
    #[must_use]
    pub fn with_retry_interval(mut self, interval: Duration) -> Self {
        self.retry_interval = interval;
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

    /// Attempts a single non-blocking exclusive acquisition on the lock file.
    ///
    /// Returns the locked file on success and `None` while another holder owns
    /// the advisory lock. Because releases unlink the lock file, a successful
    /// flock may land on an inode that was already unlinked (or replaced) by a
    /// concurrent release; such stale acquisitions are detected and retried so
    /// two holders can never coexist on different inodes of the same path.
    fn try_acquire(path: &Path) -> Result<Option<File>> {
        loop {
            let file = Self::open_lock_file(path)?;
            match file.try_lock_exclusive() {
                Ok(()) => {
                    if Self::is_current_lock_file(path, &file)? {
                        return Ok(Some(file));
                    }
                    // Stale inode: the file was unlinked/replaced between our
                    // open and flock. Drop it and race again on the fresh path.
                    let _ = fs2::FileExt::unlock(&file);
                    continue;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(error) => return Err(Error::Io(error)),
            }
        }
    }

    /// Returns whether the locked descriptor still refers to the file at
    /// `path`.
    #[cfg(unix)]
    fn is_current_lock_file(path: &Path, file: &File) -> Result<bool> {
        use std::os::unix::fs::MetadataExt;

        let held = file.metadata().map_err(Error::Io)?;
        match std::fs::metadata(path) {
            Ok(current) => Ok(current.dev() == held.dev() && current.ino() == held.ino()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(Error::Io(error)),
        }
    }

    /// Non-unix fallback: inode identity is not portably observable, so
    /// acquisitions are accepted as-is (releases there also skip the unlink,
    /// keeping the single-inode invariant).
    #[cfg(not(unix))]
    fn is_current_lock_file(_path: &Path, _file: &File) -> Result<bool> {
        Ok(true)
    }

    /// Releases a held lock: best-effort unlink of the lock file while the
    /// exclusive advisory lock is still held, then unlock.
    ///
    /// Unlinking under the exclusive lock means a concurrent contender either
    /// still races on this inode (and then detects the unlink and retries) or
    /// recreates the file fresh; at worst an empty lock file is recreated,
    /// which the next cycle reuses or removes.
    fn release_lock_file(path: &Path, file: File) {
        #[cfg(unix)]
        if let Err(error) = std::fs::remove_file(path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::debug!(path = %path.display(), %error, "failed to remove lock file on release");
        }
        #[cfg(not(unix))]
        let _ = path;

        let _ = fs2::FileExt::unlock(&file);
    }

    /// Reports whether an upload is currently locked.
    ///
    /// Monitoring/test helper; probes by attempting to take (and immediately
    /// releasing) the advisory lock, so the answer may be stale as soon as it
    /// is returned. A missing lock file means unlocked; the probe never
    /// creates lock files.
    pub fn is_locked(&self, upload_id: &str) -> Result<bool> {
        let path = self.lock_path(upload_id)?;
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(Error::Io(error)),
        };

        match file.try_lock_exclusive() {
            Ok(()) => {
                let _ = fs2::FileExt::unlock(&file);
                Ok(false)
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
            Err(error) => Err(Error::Io(error)),
        }
    }
}

impl std::fmt::Debug for FileLocker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileLocker")
            .field("directory", &self.directory)
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
            if let Some(file) = Self::try_acquire(&path)? {
                let release_path = path.clone();
                return Ok(LockGuard::with_release(upload_id, move || {
                    Self::release_lock_file(&release_path, file);
                }));
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

        match Self::try_acquire(&path)? {
            Some(file) => {
                let release_path = path.clone();
                Ok(Some(LockGuard::with_release(upload_id, move || {
                    Self::release_lock_file(&release_path, file);
                })))
            }
            None => Ok(None),
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

        assert!(!locker.is_locked("test").unwrap());

        let guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert!(locker.is_locked("test").unwrap());

        drop(guard);
        assert!(!locker.is_locked("test").unwrap());
    }

    #[tokio::test]
    async fn test_lock_auto_release_on_drop() {
        let (locker, _dir) = create_test_locker().await;

        {
            let _guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
            assert!(locker.is_locked("test").unwrap());
        }

        assert!(!locker.is_locked("test").unwrap());

        let _guard2 = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert!(locker.is_locked("test").unwrap());
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

        assert!(locker.is_locked("upload-1").unwrap());
        assert!(locker.is_locked("upload-2").unwrap());

        drop(guard1);
        assert!(!locker.is_locked("upload-1").unwrap());
        assert!(locker.is_locked("upload-2").unwrap());
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

    #[cfg(unix)]
    #[tokio::test]
    async fn release_removes_lock_file() {
        let (locker, dir) = create_test_locker().await;
        let lock_path = dir.path().join("test.lock");

        let guard = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert!(lock_path.exists());

        drop(guard);
        assert!(!lock_path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn probing_never_creates_lock_files() {
        let (locker, dir) = create_test_locker().await;

        assert!(!locker.is_locked("probe-only").unwrap());
        assert!(!dir.path().join("probe-only.lock").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn lock_release_cycles_leave_directory_empty() {
        let (locker, dir) = create_test_locker().await;

        for i in 0..32 {
            let id = format!("upload-{i}");
            let guard = locker.lock(&id, Duration::from_secs(1)).await.unwrap();
            drop(guard);
        }

        let residue: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert!(residue.is_empty(), "leftover lock files: {residue:?}");
    }

    #[tokio::test]
    async fn test_abandoned_lock_file_does_not_block_acquisition() {
        let (locker, dir) = create_test_locker().await;

        std::fs::write(dir.path().join("test.lock"), "abandoned").unwrap();
        assert!(!locker.is_locked("test").unwrap());

        let recovered = locker.lock("test", Duration::from_secs(1)).await.unwrap();
        assert!(locker.is_locked("test").unwrap());

        drop(recovered);
        assert!(!locker.is_locked("test").unwrap());
    }
}
