//! In-memory state store implementation.
//!
//! This state store keeps all upload metadata in memory. Useful for testing
//! and development, but state is lost when the process exits.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::RwLock;

use crate::error::{Error, Result};
use crate::state::{StateStore, UploadInventory, UploadState};

/// In-memory state store.
///
/// Stores all upload state in a HashMap protected by a RwLock.
/// Thread-safe and suitable for single-process use.
pub struct MemoryStateStore {
    states: RwLock<HashMap<String, UploadState>>,
}

impl MemoryStateStore {
    /// Creates a new empty state store.
    pub fn new() -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
        }
    }

    /// Returns the number of uploads currently stored.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.states.read().unwrap().len()
    }

    /// Returns whether the store is empty.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.states.read().unwrap().is_empty()
    }

    /// Clears all stored state.
    #[cfg(test)]
    pub fn clear(&self) {
        self.states.write().unwrap().clear();
    }
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MemoryStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let states = self.states.read().unwrap();
        f.debug_struct("MemoryStateStore")
            .field("uploads", &states.len())
            .finish()
    }
}

fn validate_upload_id(id: &str) -> Result<()> {
    id.parse::<crate::protocol::UploadId>()?;
    Ok(())
}

#[async_trait]
impl StateStore for MemoryStateStore {
    fn name(&self) -> &'static str {
        "memory"
    }

    async fn set(&self, state: &UploadState, create: bool) -> Result<()> {
        validate_upload_id(state.id())?;

        let mut states = self.states.write().unwrap();

        if create && states.contains_key(state.id()) {
            return Err(Error::AlreadyExists(state.id().to_string()));
        }

        states.insert(state.id().to_string(), state.clone());
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<UploadState>> {
        validate_upload_id(id)?;

        let states = self.states.read().unwrap();
        Ok(states.get(id).cloned())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        validate_upload_id(id)?;

        self.states.write().unwrap().remove(id);
        Ok(())
    }

    async fn list_expired(&self, before: DateTime<Utc>) -> Result<Vec<String>> {
        let states = self.states.read().unwrap();
        let expired: Vec<String> = states
            .values()
            .filter(|s| s.expires_at().map(|exp| *exp < before).unwrap_or(false))
            .map(|s| s.id().to_string())
            .collect();
        Ok(expired)
    }
}

#[async_trait]
impl UploadInventory for MemoryStateStore {
    async fn list_upload_ids(&self, limit: usize, offset: usize) -> Result<Vec<String>> {
        let states = self.states.read().unwrap();
        let mut ids: Vec<String> = states.keys().cloned().collect();
        ids.sort();
        Ok(ids.into_iter().skip(offset).take(limit).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[tokio::test]
    async fn state_store_conformance() {
        let store = MemoryStateStore::new();

        crate::state::conformance::assert_state_store_semantics(&store).await;
    }

    #[tokio::test]
    async fn upload_inventory_conformance() {
        let store = MemoryStateStore::new();

        crate::state::conformance::assert_upload_inventory_semantics(&store).await;
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let store = MemoryStateStore::new();

        let state = UploadState::new("test-1").with_length(1000);
        store.set(&state, true).await.unwrap();

        let retrieved = store.get("test-1").await.unwrap().unwrap();
        assert_eq!(retrieved.id(), "test-1");
        assert_eq!(retrieved.length(), Some(1000));
    }

    #[tokio::test]
    async fn test_create_duplicate() {
        let store = MemoryStateStore::new();

        let state = UploadState::new("test-1");
        store.set(&state, true).await.unwrap();

        let result = store.set(&state, true).await;
        assert!(matches!(result, Err(Error::AlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_update_existing() {
        let store = MemoryStateStore::new();

        let state = UploadState::new("test-1").with_length(1000);
        store.set(&state, true).await.unwrap();

        let mut updated = state.clone();
        updated.set_offset(500);
        store.set(&updated, false).await.unwrap(); // create=false allows update

        let retrieved = store.get("test-1").await.unwrap().unwrap();
        assert_eq!(retrieved.offset(), 500);
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let store = MemoryStateStore::new();
        let result = store.get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete() {
        let store = MemoryStateStore::new();

        let state = UploadState::new("test-1");
        store.set(&state, true).await.unwrap();
        assert_eq!(store.len(), 1);

        store.delete("test-1").await.unwrap();
        assert_eq!(store.len(), 0);
        assert!(store.get("test-1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_expired() {
        let store = MemoryStateStore::new();

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
    async fn test_clear() {
        let store = MemoryStateStore::new();

        for i in 0..5 {
            let state = UploadState::new(format!("upload-{}", i));
            store.set(&state, true).await.unwrap();
        }

        assert_eq!(store.len(), 5);
        store.clear();
        assert_eq!(store.len(), 0);
    }
}
