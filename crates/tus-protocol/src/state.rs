//! State store trait and upload state types.
//!
//! This module defines the required [`StateStore`] trait for persisting upload
//! metadata, the optional [`UploadInventory`] trait for operational upload-ID
//! enumeration, and the [`UploadState`] struct that represents an upload's
//! current state.
//!
//! # Implementations
//!
//! - `memory::MemoryStateStore` - In-memory storage (feature: `state-memory`)
//! - `file::FileStateStore` - File-based storage (feature: `state-file`)

// Feature-gated implementations
// Native implementations are not available on wasm32.
#[cfg(any(test, feature = "conformance-state"))]
pub mod conformance;

#[cfg(all(feature = "state-memory", not(target_arch = "wasm32")))]
pub mod memory;

#[cfg(all(feature = "state-file", not(target_arch = "wasm32")))]
pub mod file;

use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::borrow::Cow;
use std::collections::HashMap;
use std::iter::FromIterator;

use crate::error::{Error, Result};
use crate::runtime::MaybeSendSync;
use crate::storage::StorageHandle;

/// Trait for persisting upload state.
///
/// Implementors should provide atomic operations for storing and retrieving
/// upload state. Upload bytes live behind `Storage`; storage-owned locator and
/// bookkeeping facts are persisted here only as an opaque `StorageHandle` snapshot.
/// Returned [`UploadState`] values are snapshots; callers persist mutations by
/// calling [`StateStore::set`] again.
///
/// `delete` should be idempotent, and `list_expired` does not guarantee a stable
/// ordering unless an implementation documents one. When `create` is true,
/// implementations should reject an already existing upload ID; backends with
/// compare-and-set or conditional-create support should make that check atomic
/// with the write.
///
/// Implementations should reject upload IDs that fail
/// [`UploadId`](crate::protocol::UploadId) validation on `set`, `get`, and
/// `delete` so adapters cannot accidentally turn an unsafe path segment into a
/// backend key or path component.
///
/// # Platform Support
///
/// This trait uses conditional bounds:
/// - On native platforms: implementations and returned futures must be `Send + Sync`
/// - On `wasm32`: `Send + Sync` is not required
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

    /// Lists upload IDs that are protocol-expired before the given timestamp.
    ///
    /// Used by expiration cleanup jobs.
    async fn list_expired(&self, before: DateTime<Utc>) -> Result<Vec<String>>;
}

/// Optional trait for operational upload inventory.
///
/// Inventory is not part of the core TUS protocol lifecycle. Adapters that can
/// enumerate upload IDs implement this trait in addition to [`StateStore`],
/// while upload-only adapters can satisfy protocol workflows with [`StateStore`]
/// alone.
///
/// Implementations return all known persisted upload IDs, including uploads
/// that protocol requests may reject until reclamation removes them. IDs are
/// returned in deterministic upload-ID order for each call, but pagination is
/// not a multi-call snapshot: concurrent creates or deletes may affect later
/// pages.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait UploadInventory: MaybeSendSync {
    /// Lists known upload IDs in deterministic upload-ID order.
    async fn list_upload_ids(&self, limit: usize, offset: usize) -> Result<Vec<String>>;
}

/// Current persistence schema version written by [`UploadState`].
pub const UPLOAD_STATE_SCHEMA_VERSION: u32 = 1;

fn default_schema_version() -> u32 {
    // Files written before the field existed are version 1.
    1
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
    /// Persistence schema version for serialized upload state.
    ///
    /// State files written before this field existed deserialize as
    /// version 1. Bump [`UPLOAD_STATE_SCHEMA_VERSION`] when a change to this
    /// struct cannot be represented for older readers; loaders can use the
    /// version to migrate or reject newer state. New fields must carry
    /// `#[serde(default)]` so older state files keep loading.
    #[serde(default = "default_schema_version")]
    schema_version: u32,

    // === Core TUS Fields ===
    /// Unique upload identifier.
    id: String,

    /// Bytes successfully uploaded (current offset).
    offset: u64,

    /// Total size in bytes. None if deferred (Upload-Defer-Length).
    length: Option<u64>,

    /// Opaque storage locator from the persisted storage handle.
    storage_key: Option<String>,

    // === Lifecycle ===
    /// When the upload was created.
    created_at: DateTime<Utc>,

    /// Advertised protocol expiration deadline. None if expiration is disabled.
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
    /// Storage-owned handle facts. This should not be exposed to clients or
    /// interpreted by protocol lifecycle code.
    #[serde(default)]
    internal: HashMap<String, String>,
}

impl UploadState {
    /// Creates a new upload state with the given ID.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            schema_version: UPLOAD_STATE_SCHEMA_VERSION,
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
    pub fn new_random() -> Self {
        Self::new(uuid::Uuid::new_v4().to_string())
    }

    /// Returns the persistence schema version this state was loaded with.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
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
    pub(crate) fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }

    /// Returns the declared upload length, if any.
    pub fn length(&self) -> Option<u64> {
        self.length
    }

    /// Sets the declared upload length.
    pub(crate) fn set_length(&mut self, length: u64) {
        self.length = Some(length);
    }

    /// Returns the persisted storage handle, if assigned.
    pub fn storage_handle(&self) -> Option<StorageHandle> {
        self.storage_key
            .clone()
            .map(|key| StorageHandle::from_parts(key, self.internal.clone()))
    }

    /// Returns the persisted storage handle or a storage-key error.
    pub(crate) fn require_storage_handle(&self) -> Result<StorageHandle> {
        self.storage_handle().ok_or(Error::StorageKeyMissing)
    }

    /// Persists storage addressing and backend-specific facts on the upload.
    ///
    /// The handle is treated as the complete current storage fact set: existing
    /// internal values are replaced by the handle's internal values.
    pub fn set_storage_handle(&mut self, handle: StorageHandle) {
        let (key, internal) = handle.into_parts();
        self.storage_key = Some(key);
        self.internal = internal;
    }

    /// Returns when the upload was created.
    pub fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }

    /// Returns the advertised protocol expiration deadline, if expiration is enabled.
    ///
    /// This timestamp is not, by itself, a completed-upload retention deadline;
    /// use [`UploadState::is_expired`] or [`UploadState::expires_before`] for
    /// protocol expiration semantics.
    pub fn expires_at(&self) -> Option<&DateTime<Utc>> {
        self.expires_at.as_ref()
    }

    /// Sets the expiration time.
    pub(crate) fn set_expiration(&mut self, expires_at: DateTime<Utc>) {
        self.expires_at = Some(expires_at);
    }

    /// Returns whether the upload is marked as partial.
    pub fn is_partial(&self) -> bool {
        self.is_partial
    }

    /// Marks the upload as partial.
    pub(crate) fn mark_partial(&mut self) {
        self.is_partial = true;
    }

    /// Returns whether the upload is marked as final.
    pub fn is_final(&self) -> bool {
        self.is_final
    }

    /// Marks the upload as final and stores the concatenated part IDs.
    pub(crate) fn mark_final(&mut self, parts: Vec<String>) {
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

    /// Returns whether this upload expires before the given cutoff.
    ///
    /// TUS expiration applies to unfinished upload resources. Completed
    /// non-partial uploads are deliverable content and do not expire through
    /// this policy.
    pub fn expires_before(&self, before: DateTime<Utc>) -> bool {
        crate::expiration::ProtocolExpiration::for_upload(self).expires_before(before)
    }

    /// Returns whether the upload is protocol-expired.
    pub fn is_expired(&self) -> bool {
        crate::expiration::ProtocolExpiration::for_upload(self).is_expired()
    }

    /// Returns the remaining bytes to upload.
    pub fn remaining(&self) -> Option<u64> {
        self.length.map(|len| len.saturating_sub(self.offset))
    }

    /// Formats the expiration time as an RFC 7231 date for the Upload-Expires header.
    pub fn expires_header(&self) -> Option<String> {
        crate::expiration::ProtocolExpiration::for_upload(self).header()
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

impl From<&HashMap<String, MetadataValue>> for UploadMetadata {
    fn from(metadata: &HashMap<String, MetadataValue>) -> Self {
        metadata.clone().into()
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

impl From<&HashMap<String, String>> for UploadMetadata {
    fn from(metadata: &HashMap<String, String>) -> Self {
        metadata.clone().into()
    }
}

impl From<&UploadMetadata> for UploadMetadata {
    fn from(metadata: &UploadMetadata) -> Self {
        metadata.clone()
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
    fn legacy_state_without_schema_version_deserializes_as_v1() {
        // Simulate a state file written before the field existed by
        // stripping it from a freshly serialized state.
        let mut value = serde_json::to_value(UploadState::new("upload-1")).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("schema_version")
            .expect("serialized state should include schema_version");

        let state: UploadState = serde_json::from_value(value).unwrap();

        assert_eq!(state.schema_version(), 1);
    }

    #[test]
    fn serialized_state_includes_current_schema_version() {
        let value = serde_json::to_value(UploadState::new("upload-1")).unwrap();

        assert_eq!(
            value.get("schema_version").and_then(|v| v.as_u64()),
            Some(u64::from(UPLOAD_STATE_SCHEMA_VERSION))
        );
    }

    #[test]
    fn test_upload_state_new_random() {
        let state = UploadState::new_random();
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
    fn upload_metadata_clones_from_borrowed_upload_metadata() {
        let mut metadata = UploadMetadata::new();
        metadata.insert("filename", "test.txt");

        let cloned = UploadMetadata::from(&metadata);

        assert_eq!(cloned, metadata);
    }

    #[test]
    fn upload_metadata_converts_from_borrowed_text_map() {
        let metadata = HashMap::from([("filename".to_string(), "test.txt".to_string())]);

        let converted = UploadMetadata::from(&metadata);

        assert_eq!(
            converted.get("filename").and_then(|value| value.as_str()),
            Some("test.txt")
        );
    }

    #[test]
    fn upload_metadata_converts_from_borrowed_binary_map() {
        let metadata = HashMap::from([("bin".to_string(), MetadataValue::from(&b"\xFF\xFE"[..]))]);

        let converted = UploadMetadata::from(&metadata);

        assert_eq!(converted.get("bin").unwrap().as_bytes(), b"\xFF\xFE");
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

    #[test]
    fn completed_upload_expiration_policy_distinguishes_deliverable_and_partial_uploads() {
        use chrono::TimeZone;

        let expires_at = Utc.with_ymd_and_hms(2030, 6, 25, 14, 30, 0).unwrap();
        let mut deliverable = UploadState::new("deliverable")
            .with_length(5)
            .with_expiration(expires_at);
        deliverable.set_offset(5);

        let mut partial = UploadState::new("partial")
            .with_length(5)
            .with_expiration(expires_at)
            .as_partial();
        partial.set_offset(5);

        assert_eq!(deliverable.expires_header(), None);
        assert_eq!(
            partial.expires_header().as_deref(),
            Some("Tue, 25 Jun 2030 14:30:00 GMT")
        );
    }
}
