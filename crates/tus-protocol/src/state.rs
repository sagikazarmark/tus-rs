//! State store trait and upload state types.
//!
//! This module defines the `StateStore` trait for persisting upload metadata
//! and the `UploadState` struct that represents an upload's current state.
//!
//! # Implementations
//!
//! - `memory::MemoryStateStore` - In-memory storage (feature: `state-memory`)
//! - `file::FileStateStore` - File-based storage (feature: `state-file`)

// Feature-gated implementations
// Native implementations are not available in local-futures builds.
#[cfg(all(feature = "state-memory", not(feature = "local-futures")))]
pub mod memory;

#[cfg(all(feature = "state-file", not(feature = "local-futures")))]
pub mod file;

use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::borrow::Cow;
use std::collections::HashMap;
use std::iter::FromIterator;

use crate::error::Result;
use crate::runtime::MaybeSendSync;

/// Trait for persisting upload state.
///
/// Implementors should provide atomic operations for storing and retrieving
/// upload metadata. The actual file data is stored by `Storage`.
/// Returned [`UploadState`] values are snapshots; callers persist mutations by
/// calling [`StateStore::set`] again.
///
/// `delete` should be idempotent, and `list` / `list_expired` do not guarantee
/// a stable ordering unless an implementation documents one. When `create` is
/// true, implementations should reject an already existing upload ID; backends
/// with compare-and-set or conditional-create support should make that check
/// atomic with the write.
///
/// # Platform Support
///
/// This trait uses conditional bounds:
/// - On native platforms: implementations and returned futures must be `Send + Sync`
/// - With `local-futures`: `Send + Sync` is not required
#[cfg_attr(not(feature = "local-futures"), async_trait)]
#[cfg_attr(feature = "local-futures", async_trait(?Send))]
pub trait StateStore: MaybeSendSync {
    /// Returns the state store backend name for logging/debugging.
    fn name(&self) -> &'static str;

    /// Stores or updates upload state.
    ///
    /// If `create` is true, this fails when the upload already exists.
    ///
    /// # Errors
    /// Returns `Error::AlreadyExists` if `create` is true and the upload exists.
    async fn set(&self, state: &UploadState, create: bool) -> Result<()>;

    /// Retrieves upload state by ID.
    async fn get(&self, id: &str) -> Result<Option<UploadState>>;

    /// Deletes upload state.
    async fn delete(&self, id: &str) -> Result<()>;

    /// Lists upload IDs that have expired before the given timestamp.
    ///
    /// Used by expiration cleanup jobs.
    async fn list_expired(&self, before: DateTime<Utc>) -> Result<Vec<String>>;

    /// Lists all upload IDs (for admin/debugging).
    async fn list(&self, limit: usize, offset: usize) -> Result<Vec<String>>;
}

/// Represents the state of an upload.
///
/// This is the core data structure that tracks upload progress and metadata.
/// It's stored by the `StateStore` and updated during upload operations.
///
/// # Examples
///
/// ```rust
/// use tus_protocol::{UploadMetadata, UploadState};
///
/// let metadata: UploadMetadata = [("filename".to_string(), "photo.jpg".to_string())]
///     .into_iter()
///     .collect();
///
/// let state = UploadState::new("upload-1")
///     .with_length(1024)
///     .with_metadata(metadata);
///
/// assert_eq!(state.id(), "upload-1");
/// assert_eq!(state.length(), Some(1024));
/// assert_eq!(state.metadata().get("filename").unwrap().as_str(), Some("photo.jpg"));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadState {
    // === Core TUS Fields ===
    /// Unique upload identifier.
    id: String,

    /// Bytes successfully uploaded (current offset).
    offset: u64,

    /// Total size in bytes. None if deferred (Upload-Defer-Length).
    length: Option<u64>,

    /// Storage backend key/path. Set by Storage::create().
    storage_key: Option<String>,

    // === Lifecycle ===
    /// When the upload was created.
    created_at: DateTime<Utc>,

    /// When the upload expires. None if expiration is disabled.
    expires_at: Option<DateTime<Utc>>,

    // === Concatenation Extension ===
    /// Whether this is a partial upload (for concatenation).
    is_partial: bool,

    /// Whether this is a final concatenated upload.
    is_final: bool,

    /// Part IDs for final uploads (Concatenation extension).
    parts: Option<Vec<String>>,

    // === User Metadata ===
    /// User-provided metadata from Upload-Metadata header.
    metadata: UploadMetadata,

    // === Storage-Specific Internal State ===
    /// Internal state for storage backends (e.g., R2 upload ID, S3 part ETags).
    /// This should not be exposed to clients.
    #[serde(default)]
    internal: HashMap<String, String>,
}

impl UploadState {
    /// Creates a new upload state with the given ID.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            offset: 0,
            length: None,
            storage_key: None,
            created_at: Utc::now(),
            expires_at: None,
            is_partial: false,
            is_final: false,
            parts: None,
            metadata: UploadMetadata::new(),
            internal: HashMap::new(),
        }
    }

    /// Creates a new upload state with a generated UUID.
    pub fn with_uuid() -> Self {
        Self::new(uuid::Uuid::new_v4().to_string())
    }

    /// Sets the upload length.
    #[must_use]
    pub fn with_length(mut self, length: u64) -> Self {
        self.length = Some(length);
        self
    }

    /// Sets the expiration time.
    #[must_use]
    pub fn with_expiration(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Sets the metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: impl Into<UploadMetadata>) -> Self {
        self.metadata = metadata.into();
        self
    }

    /// Marks as a partial upload.
    #[must_use]
    pub fn as_partial(mut self) -> Self {
        self.is_partial = true;
        self
    }

    /// Marks as a final concatenated upload.
    #[must_use]
    pub fn as_final(mut self, parts: Vec<String>) -> Self {
        self.is_final = true;
        self.parts = Some(parts);
        self
    }

    /// Returns the upload identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the current offset.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Sets the current offset.
    pub fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }

    /// Returns the declared upload length, if any.
    pub fn length(&self) -> Option<u64> {
        self.length
    }

    /// Sets the declared upload length.
    pub fn set_length(&mut self, length: u64) {
        self.length = Some(length);
    }

    /// Returns the storage backend key/path, if assigned.
    pub fn storage_key(&self) -> Option<&str> {
        self.storage_key.as_deref()
    }

    /// Sets the storage backend key/path.
    pub fn set_storage_key(&mut self, storage_key: impl Into<String>) {
        self.storage_key = Some(storage_key.into());
    }

    /// Returns when the upload was created.
    pub fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    /// Returns when the upload expires, if expiration is enabled.
    pub fn expires_at(&self) -> Option<&DateTime<Utc>> {
        self.expires_at.as_ref()
    }

    /// Sets the expiration time.
    pub fn set_expiration(&mut self, expires_at: DateTime<Utc>) {
        self.expires_at = Some(expires_at);
    }

    /// Returns whether the upload is marked as partial.
    pub fn is_partial(&self) -> bool {
        self.is_partial
    }

    /// Marks the upload as partial.
    pub fn mark_partial(&mut self) {
        self.is_partial = true;
    }

    /// Returns whether the upload is marked as final.
    pub fn is_final(&self) -> bool {
        self.is_final
    }

    /// Marks the upload as final and stores the concatenated part IDs.
    pub fn mark_final(&mut self, parts: Vec<String>) {
        self.is_final = true;
        self.parts = Some(parts);
    }

    /// Returns the concatenated part IDs for final uploads.
    pub fn parts(&self) -> Option<&[String]> {
        self.parts.as_deref()
    }

    /// Returns the user metadata map.
    pub fn metadata(&self) -> &UploadMetadata {
        &self.metadata
    }

    /// Returns the user metadata map mutably.
    pub fn metadata_mut(&mut self) -> &mut UploadMetadata {
        &mut self.metadata
    }

    /// Replaces the user metadata map.
    pub fn set_metadata(&mut self, metadata: impl Into<UploadMetadata>) {
        self.metadata = metadata.into();
    }

    /// Returns whether the upload is complete.
    pub fn is_complete(&self) -> bool {
        match self.length {
            Some(length) => self.offset >= length,
            None => false, // Deferred length is never "complete" until length is set
        }
    }

    /// Returns whether the upload has expired.
    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expires) => Utc::now() > expires,
            None => false,
        }
    }

    /// Returns the remaining bytes to upload.
    pub fn remaining(&self) -> Option<u64> {
        self.length.map(|len| len.saturating_sub(self.offset))
    }

    /// Stashes backend-specific bookkeeping alongside the upload.
    ///
    /// Intended for [`Storage`](crate::storage::Storage) implementations that
    /// need to persist opaque identifiers between calls, for example an R2
    /// multipart upload id, an S3 ETag list, or a staging path. The keys are
    /// never exposed on the wire and are not part of the TUS protocol.
    /// Application code and hooks should not read or write this map.
    pub fn set_internal(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.internal.insert(key.into(), value.into());
    }

    /// Reads a backend-specific value previously stored by
    /// [`set_internal`](Self::set_internal).
    ///
    /// See [`set_internal`](Self::set_internal) for the intended audience.
    pub fn get_internal(&self, key: &str) -> Option<&str> {
        self.internal.get(key).map(|s| s.as_str())
    }

    /// Removes a backend-specific value previously stored by
    /// [`set_internal`](Self::set_internal), returning it if present.
    ///
    /// See [`set_internal`](Self::set_internal) for the intended audience.
    pub fn remove_internal(&mut self, key: &str) -> Option<String> {
        self.internal.remove(key)
    }

    /// Formats the expiration time as an RFC 7231 date for the Upload-Expires header.
    pub fn expires_header(&self) -> Option<String> {
        self.expires_at
            .map(|dt| dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
    }
}

impl Default for UploadState {
    fn default() -> Self {
        Self::with_uuid()
    }
}

/// User-provided upload metadata keyed by TUS metadata name.
///
/// This type intentionally hides its internal map representation so the crate
/// can evolve metadata validation, ordering, or canonicalization without
/// exposing every `HashMap` method as stable API.
///
/// # Examples
///
/// ```rust
/// use tus_protocol::{MetadataValue, UploadMetadata};
///
/// let mut metadata = UploadMetadata::new();
/// metadata.insert("filename", "report.pdf");
/// metadata.insert("raw", MetadataValue::from(&b"\x00\xff"[..]));
///
/// assert_eq!(metadata.get("filename").unwrap().as_str(), Some("report.pdf"));
/// assert_eq!(metadata.get("raw").unwrap().as_bytes(), b"\x00\xff");
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UploadMetadata(HashMap<String, MetadataValue>);

impl UploadMetadata {
    /// Creates an empty metadata map.
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Inserts a metadata key/value pair, returning any previous value.
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<MetadataValue>,
    ) -> Option<MetadataValue> {
        self.0.insert(key.into(), value.into())
    }

    /// Returns a metadata value by key.
    pub fn get(&self, key: &str) -> Option<&MetadataValue> {
        self.0.get(key)
    }

    /// Returns true if no metadata entries are present.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of metadata entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Iterates over metadata key/value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &MetadataValue)> {
        self.0.iter()
    }

    /// Consumes the metadata and returns the backing map.
    pub fn into_inner(self) -> HashMap<String, MetadataValue> {
        self.0
    }
}

impl From<HashMap<String, MetadataValue>> for UploadMetadata {
    fn from(metadata: HashMap<String, MetadataValue>) -> Self {
        Self(metadata)
    }
}

impl From<HashMap<String, String>> for UploadMetadata {
    fn from(metadata: HashMap<String, String>) -> Self {
        metadata
            .into_iter()
            .map(|(key, value)| (key, MetadataValue::from(value)))
            .collect()
    }
}

impl FromIterator<(String, MetadataValue)> for UploadMetadata {
    fn from_iter<T: IntoIterator<Item = (String, MetadataValue)>>(iter: T) -> Self {
        Self(HashMap::from_iter(iter))
    }
}

impl FromIterator<(String, String)> for UploadMetadata {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        iter.into_iter()
            .map(|(key, value)| (key, MetadataValue::from(value)))
            .collect::<HashMap<_, _>>()
            .into()
    }
}

impl IntoIterator for UploadMetadata {
    type Item = (String, MetadataValue);
    type IntoIter = std::collections::hash_map::IntoIter<String, MetadataValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a UploadMetadata {
    type Item = (&'a String, &'a MetadataValue);
    type IntoIter = std::collections::hash_map::Iter<'a, String, MetadataValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// A byte-preserving TUS metadata value.
///
/// TUS metadata values are Base64-encoded on the wire and may contain arbitrary
/// binary bytes. Use [`MetadataValue::as_str`] only when the value is known to be
/// UTF-8 text.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct MetadataValue(Vec<u8>);

impl MetadataValue {
    /// Creates a metadata value from raw bytes.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Returns the raw metadata bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the value and returns the raw metadata bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Returns the metadata value as UTF-8 text if possible.
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }

    /// Returns a displayable string, replacing invalid UTF-8 bytes.
    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.0)
    }
}

impl From<Vec<u8>> for MetadataValue {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

impl From<&[u8]> for MetadataValue {
    fn from(bytes: &[u8]) -> Self {
        Self::new(bytes.to_vec())
    }
}

impl From<String> for MetadataValue {
    fn from(value: String) -> Self {
        Self::new(value.into_bytes())
    }
}

impl From<&str> for MetadataValue {
    fn from(value: &str) -> Self {
        Self::new(value.as_bytes().to_vec())
    }
}

impl Serialize for MetadataValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(value) = self.as_str() {
            return serializer.serialize_str(value);
        }

        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(
            "base64",
            &base64::engine::general_purpose::STANDARD.encode(&self.0),
        )?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for MetadataValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum PersistedMetadataValue {
            Text(String),
            Binary { base64: String },
        }

        match PersistedMetadataValue::deserialize(deserializer)? {
            PersistedMetadataValue::Text(value) => Ok(Self::from(value)),
            PersistedMetadataValue::Binary { base64 } => {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(base64.as_bytes())
                    .map_err(de::Error::custom)?;
                Ok(Self(bytes))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_state_new() {
        let state = UploadState::new("test-id");
        assert_eq!(state.id(), "test-id");
        assert_eq!(state.offset(), 0);
        assert!(state.length().is_none());
        assert!(!state.is_partial());
        assert!(!state.is_final());
    }

    #[test]
    fn test_upload_state_with_uuid() {
        let state = UploadState::with_uuid();
        assert!(!state.id().is_empty());
        assert_eq!(state.id().len(), 36); // UUID format
    }

    #[test]
    fn test_upload_state_builder() {
        let mut metadata = HashMap::new();
        metadata.insert("filename".to_string(), MetadataValue::from("test.txt"));

        let state = UploadState::new("test")
            .with_length(1024)
            .with_metadata(metadata);

        assert_eq!(state.length(), Some(1024));
        assert_eq!(
            state.metadata().get("filename").and_then(|v| v.as_str()),
            Some("test.txt")
        );
    }

    #[test]
    fn upload_metadata_exposes_intentional_escape_hatch() {
        let metadata =
            UploadMetadata::from_iter([("filename".to_string(), MetadataValue::from("test.txt"))]);

        let inner = metadata.into_inner();
        assert_eq!(
            inner.get("filename").and_then(|v| v.as_str()),
            Some("test.txt")
        );
    }

    #[test]
    fn metadata_value_deserializes_legacy_text_json() {
        let value: MetadataValue = serde_json::from_str(r#""test.txt""#).unwrap();

        assert_eq!(value.as_bytes(), b"test.txt");
    }

    #[test]
    fn metadata_value_serializes_text_as_plain_json_string() {
        let json = serde_json::to_string(&MetadataValue::from("test.txt")).unwrap();

        assert_eq!(json, r#""test.txt""#);
    }

    #[test]
    fn metadata_value_serializes_binary_as_base64_object() {
        let json = serde_json::to_string(&MetadataValue::from(vec![0xFF, 0xFE, 0xFD])).unwrap();

        assert_eq!(json, r#"{"base64":"//79"}"#);
        let decoded: MetadataValue = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.as_bytes(), [0xFF, 0xFE, 0xFD]);
    }

    #[test]
    fn test_is_complete() {
        let mut state = UploadState::new("test").with_length(100);
        assert!(!state.is_complete());

        state.set_offset(50);
        assert!(!state.is_complete());

        state.set_offset(100);
        assert!(state.is_complete());

        // Deferred length is never complete until set
        let deferred = UploadState::new("test2");
        assert!(!deferred.is_complete());
    }

    #[test]
    fn test_is_expired() {
        let state = UploadState::new("test");
        assert!(!state.is_expired());

        let expired =
            UploadState::new("test2").with_expiration(Utc::now() - chrono::Duration::hours(1));
        assert!(expired.is_expired());

        let future =
            UploadState::new("test3").with_expiration(Utc::now() + chrono::Duration::hours(1));
        assert!(!future.is_expired());
    }

    #[test]
    fn test_remaining() {
        let mut state = UploadState::new("test").with_length(1000);
        assert_eq!(state.remaining(), Some(1000));

        state.set_offset(300);
        assert_eq!(state.remaining(), Some(700));

        state.set_offset(1000);
        assert_eq!(state.remaining(), Some(0));

        // Deferred length has no remaining
        let deferred = UploadState::new("test2");
        assert_eq!(deferred.remaining(), None);
    }

    #[test]
    fn test_internal_state() {
        let mut state = UploadState::new("test");
        state.set_internal("r2_upload_id", "abc123");
        assert_eq!(state.get_internal("r2_upload_id"), Some("abc123"));
        assert_eq!(
            state.remove_internal("r2_upload_id"),
            Some("abc123".to_string())
        );
        assert_eq!(state.get_internal("r2_upload_id"), None);
        assert_eq!(state.get_internal("nonexistent"), None);
    }

    #[test]
    fn test_serialization() {
        let state = UploadState::new("test-id").with_length(1024).as_partial();

        let json = serde_json::to_string(&state).unwrap();
        let deserialized: UploadState = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id(), "test-id");
        assert_eq!(deserialized.length(), Some(1024));
        assert!(deserialized.is_partial());
    }

    #[test]
    fn test_partial_and_final() {
        let partial = UploadState::new("part1").as_partial();
        assert!(partial.is_partial());
        assert!(!partial.is_final());

        let final_upload =
            UploadState::new("final").as_final(vec!["part1".to_string(), "part2".to_string()]);
        assert!(!final_upload.is_partial());
        assert!(final_upload.is_final());
        assert_eq!(
            final_upload.parts(),
            Some(vec!["part1".to_string(), "part2".to_string()]).as_deref()
        );
    }

    #[test]
    fn test_expires_header_rfc7231_format() {
        use chrono::TimeZone;

        // Test with a specific date: Wed, 25 Jun 2025 14:30:00 GMT
        let dt = Utc.with_ymd_and_hms(2025, 6, 25, 14, 30, 0).unwrap();
        let state = UploadState::new("test").with_expiration(dt);
        let header = state.expires_header().unwrap();

        // RFC 7231 format: Day, DD Mon YYYY HH:MM:SS GMT
        assert_eq!(header, "Wed, 25 Jun 2025 14:30:00 GMT");

        // Test another date from RFC 7231 examples
        let dt2 = Utc.with_ymd_and_hms(1994, 11, 6, 8, 49, 37).unwrap();
        let state2 = UploadState::new("test2").with_expiration(dt2);
        let header2 = state2.expires_header().unwrap();
        assert_eq!(header2, "Sun, 06 Nov 1994 08:49:37 GMT");
    }

    #[test]
    fn test_expires_header_none_when_no_expiration() {
        let state = UploadState::new("test");
        assert!(state.expires_header().is_none());
    }
}
