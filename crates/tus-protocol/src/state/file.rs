//! File-based state store implementation.
//!
//! This state store persists upload metadata as JSON files on disk.
//! Each upload gets its own `.json` file in the configured directory.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::error::{Error, Result};
use crate::state::{StateStore, UploadInventory, UploadState};

/// File-based state store.
///
/// Stores each upload's state as a JSON file in a directory.
/// Thread-safe through filesystem operations.
pub struct FileStateStore {
    /// Directory where state files are stored.
    directory: PathBuf,
}

impl FileStateStore {
    /// Creates a new file state store.
    ///
    /// # Errors
    /// Returns an error if the directory cannot be created.
    pub async fn new(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        fs::create_dir_all(&directory).await.map_err(Error::Io)?;

        Ok(Self { directory })
    }

    /// Creates a new file state store synchronously.
    ///
    /// Use this when you need to create the store outside an async context.
    pub fn new_sync(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        std::fs::create_dir_all(&directory).map_err(Error::Io)?;

        Ok(Self { directory })
    }

    /// Returns the path to the state file for an upload ID.
    fn state_path(&self, id: &str) -> Result<PathBuf> {
        validate_upload_id(id)?;
        Ok(self.directory.join(format!("{}.json", id)))
    }

    /// Reads a state file.
    async fn read_state(&self, path: &Path) -> Result<Option<UploadState>> {
        match fs::read_to_string(path).await {
            Ok(content) => {
                let state: UploadState =
                    serde_json::from_str(&content).map_err(Error::state_store)?;
                Ok(Some(state))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Io(e)),
        }
    }

    /// Writes a state file atomically.
    async fn write_state(&self, path: &Path, state: &UploadState, create: bool) -> Result<()> {
        let content = serde_json::to_string_pretty(state).map_err(Error::state_store)?;

        // Write to temp file first, then rename for atomicity
        let temp_path = unique_temp_path(path);

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .await
            .map_err(Error::Io)?;
        file.write_all(content.as_bytes())
            .await
            .map_err(Error::Io)?;
        file.sync_all().await.map_err(Error::Io)?;
        drop(file);

        if create {
            match fs::hard_link(&temp_path, path).await {
                Ok(()) => {
                    let _ = fs::remove_file(&temp_path).await;
                    Ok(())
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let _ = fs::remove_file(&temp_path).await;
                    Err(Error::AlreadyExists(state.id().to_string()))
                }
                Err(error) => {
                    let _ = fs::remove_file(&temp_path).await;
                    Err(Error::Io(error))
                }
            }
        } else {
            fs::rename(&temp_path, path).await.map_err(Error::Io)
        }
    }
}

fn validate_upload_id(id: &str) -> Result<()> {
    id.parse::<crate::protocol::UploadId>()?;
    if id.len() + ".json".len() > MAX_STATE_FILE_NAME_LEN {
        return Err(Error::InvalidUploadId(format!(
            "id plus .json suffix is {} bytes; max {}",
            id.len() + ".json".len(),
            MAX_STATE_FILE_NAME_LEN
        )));
    }

    Ok(())
}

const MAX_STATE_FILE_NAME_LEN: usize = 255;

fn unique_temp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "state.json".into());
    path.with_file_name(format!("{file_name}.{}.tmp", uuid::Uuid::new_v4().simple()))
}

impl std::fmt::Debug for FileStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStateStore")
            .field("directory", &self.directory)
            .finish()
    }
}

#[async_trait]
impl StateStore for FileStateStore {
    fn name(&self) -> &'static str {
        "file"
    }

    async fn set(&self, state: &UploadState, create: bool) -> Result<()> {
        let path = self.state_path(state.id())?;

        self.write_state(&path, state, create).await
    }

    async fn get(&self, id: &str) -> Result<Option<UploadState>> {
        let path = self.state_path(id)?;
        self.read_state(&path).await
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let path = self.state_path(id)?;

        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Io(e)),
        }
    }

    async fn list_expired(&self, before: DateTime<Utc>) -> Result<Vec<String>> {
        let mut expired = Vec::new();

        let mut entries = fs::read_dir(&self.directory).await.map_err(Error::Io)?;

        while let Some(entry) = entries.next_entry().await.map_err(Error::Io)? {
            let path = entry.path();

            if path.extension().is_none_or(|e| e != "json") {
                continue;
            }

            // A single unreadable or malformed state file (foreign, truncated,
            // or tampered) must not abort the whole expiration scan and stall
            // reclamation for every other upload: skip it and log instead.
            match self.read_state(&path).await {
                Ok(Some(state)) if state.expires_before(before) => {
                    expired.push(state.id().to_string());
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %error,
                        "skipping unreadable upload state file during expiration scan",
                    );
                }
            }
        }

        Ok(expired)
    }
}

#[async_trait]
impl UploadInventory for FileStateStore {
    async fn list_upload_ids(&self, limit: usize, offset: usize) -> Result<Vec<String>> {
        let mut ids = Vec::new();

        let mut entries = fs::read_dir(&self.directory).await.map_err(Error::Io)?;

        while let Some(entry) = entries.next_entry().await.map_err(Error::Io)? {
            let path = entry.path();

            if path.extension().is_some_and(|e| e == "json")
                && let Some(stem) = path.file_stem()
            {
                ids.push(stem.to_string_lossy().to_string());
            }
        }

        ids.sort();
        Ok(ids.into_iter().skip(offset).take(limit).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::UploadMetadata;
    use chrono::Duration;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::Barrier;

    async fn create_test_store() -> (FileStateStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = FileStateStore::new(temp_dir.path()).await.unwrap();
        (store, temp_dir)
    }

    #[tokio::test]
    async fn state_store_conformance() {
        let (store, _dir) = create_test_store().await;

        crate::state::conformance::assert_state_store_semantics(&store).await;
    }

    #[tokio::test]
    async fn upload_inventory_conformance() {
        let (store, _dir) = create_test_store().await;

        crate::state::conformance::assert_upload_inventory_semantics(&store).await;
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let (store, _dir) = create_test_store().await;

        let state = UploadState::new("test-1").with_length(1000);
        store.set(&state, true).await.unwrap();

        let retrieved = store.get("test-1").await.unwrap().unwrap();
        assert_eq!(retrieved.id(), "test-1");
        assert_eq!(retrieved.length(), Some(1000));
    }

    #[tokio::test]
    async fn list_expired_skips_malformed_state_files() {
        let (store, dir) = create_test_store().await;

        // A valid, already-expired candidate.
        let expired = UploadState::new("expired-1")
            .with_expiration(Utc::now() - Duration::hours(1));
        store.set(&expired, true).await.unwrap();

        // A malformed `.json` file that must not abort the scan.
        fs::write(dir.path().join("garbage.json"), b"{ not valid json")
            .await
            .unwrap();

        let ids = store.list_expired(Utc::now()).await.unwrap();
        assert_eq!(ids, vec!["expired-1".to_string()]);
    }

    #[tokio::test]
    async fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();

        // Create store and write state
        {
            let store = FileStateStore::new(temp_dir.path()).await.unwrap();
            let state = UploadState::new("persistent").with_length(5000);
            store.set(&state, true).await.unwrap();
        }

        // Create new store instance and verify state persisted
        {
            let store = FileStateStore::new(temp_dir.path()).await.unwrap();
            let state = store.get("persistent").await.unwrap().unwrap();
            assert_eq!(state.id(), "persistent");
            assert_eq!(state.length(), Some(5000));
        }
    }

    #[tokio::test]
    async fn test_create_duplicate() {
        let (store, _dir) = create_test_store().await;

        let state = UploadState::new("test-1");
        store.set(&state, true).await.unwrap();

        let result = store.set(&state, true).await;
        assert!(matches!(result, Err(Error::AlreadyExists(_))));
    }

    #[tokio::test]
    async fn rejects_path_traversal_ids() {
        let (store, dir) = create_test_store().await;
        let escape_id = format!("escape-{}", uuid::Uuid::new_v4().simple());
        let escape_path = dir.path().join(format!("../{escape_id}.json"));
        let _ = std::fs::remove_file(&escape_path);
        let state = UploadState::new(format!("../{escape_id}"));

        let err = store.set(&state, true).await.unwrap_err();

        assert!(matches!(err, Error::InvalidUploadId(_)));
        assert!(!escape_path.exists());
    }

    #[tokio::test]
    async fn rejects_ids_that_would_exceed_state_file_name_limit() {
        let (store, _dir) = create_test_store().await;
        let state = UploadState::new("a".repeat(251));

        let err = store.set(&state, true).await.unwrap_err();

        assert!(matches!(err, Error::InvalidUploadId(_)));
    }

    #[tokio::test]
    async fn concurrent_create_allows_exactly_one_writer() {
        let (store, _dir) = create_test_store().await;
        let store = Arc::new(store);
        let workers = 16;
        let barrier = Arc::new(Barrier::new(workers));

        let handles: Vec<_> = (0..workers)
            .map(|worker| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    let mut metadata = UploadMetadata::new();
                    metadata.insert("worker", worker.to_string());
                    metadata.insert("payload", "x".repeat(32 * 1024));
                    let state = UploadState::new("same-id").with_metadata(metadata);
                    barrier.wait().await;
                    store.set(&state, true).await
                })
            })
            .collect();

        let mut successes = 0;
        let mut already_exists = 0;
        for handle in handles {
            match handle.await.unwrap() {
                Ok(()) => successes += 1,
                Err(Error::AlreadyExists(_)) => already_exists += 1,
                Err(err) => panic!("unexpected error: {err:?}"),
            }
        }

        assert_eq!(successes, 1);
        assert_eq!(already_exists, workers - 1);
    }

    #[tokio::test]
    async fn test_update_existing() {
        let (store, _dir) = create_test_store().await;

        let state = UploadState::new("test-1").with_length(1000);
        store.set(&state, true).await.unwrap();

        let mut updated = state.clone();
        updated.set_offset(500);
        store.set(&updated, false).await.unwrap();

        let retrieved = store.get("test-1").await.unwrap().unwrap();
        assert_eq!(retrieved.offset(), 500);
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let (store, _dir) = create_test_store().await;
        let result = store.get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete() {
        let (store, _dir) = create_test_store().await;

        let state = UploadState::new("test-1");
        store.set(&state, true).await.unwrap();

        store.delete("test-1").await.unwrap();
        assert!(store.get("test-1").await.unwrap().is_none());

        // Delete non-existent should not error
        store.delete("nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn test_list_expired() {
        let (store, _dir) = create_test_store().await;

        // Not expired
        let state1 =
            UploadState::new("not-expired").with_expiration(Utc::now() + Duration::hours(1));
        store.set(&state1, true).await.unwrap();

        // Expired
        let state2 = UploadState::new("expired").with_expiration(Utc::now() - Duration::hours(1));
        store.set(&state2, true).await.unwrap();

        // No expiration
        let state3 = UploadState::new("no-expiration");
        store.set(&state3, true).await.unwrap();

        let expired = store.list_expired(Utc::now()).await.unwrap();
        assert_eq!(expired.len(), 1);
        assert!(expired.contains(&"expired".to_string()));
    }

    #[tokio::test]
    async fn test_atomic_write() {
        let (store, dir) = create_test_store().await;

        let state = UploadState::new("atomic-test").with_length(1000);
        store.set(&state, true).await.unwrap();

        // Verify no temp file remains
        let temp_path = dir.path().join("atomic-test.json.tmp");
        assert!(!temp_path.exists());

        // Verify main file exists
        let main_path = dir.path().join("atomic-test.json");
        assert!(main_path.exists());
    }

    #[tokio::test]
    async fn test_metadata_preservation() {
        let (store, _dir) = create_test_store().await;

        let mut metadata = UploadMetadata::new();
        metadata.insert("filename".to_string(), "test.txt");
        metadata.insert("mimetype".to_string(), "text/plain");

        let state = UploadState::new("with-metadata")
            .with_length(1000)
            .with_metadata(metadata);
        store.set(&state, true).await.unwrap();

        let retrieved = store.get("with-metadata").await.unwrap().unwrap();
        assert_eq!(
            retrieved
                .metadata()
                .get("filename")
                .and_then(|v| v.as_str()),
            Some("test.txt")
        );
        assert_eq!(
            retrieved
                .metadata()
                .get("mimetype")
                .and_then(|v| v.as_str()),
            Some("text/plain")
        );
    }
}
