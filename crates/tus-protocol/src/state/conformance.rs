//! Shared conformance scenarios for [`StateStore`] implementations.
//!
//! Adapter crates can enable the `conformance-state` feature in their
//! `dev-dependencies` and call this helper from their own async tests:
//!
//! ```toml
//! [dev-dependencies]
//! tus-protocol = { version = "...", features = ["conformance-state"] }
//! ```
//!
//! The required suite covers behavior the protocol lifecycle depends on:
//! create versus update semantics, duplicate create handling, snapshot reads,
//! idempotent delete, expiration listing, upload-id safety, persistence of
//! required protocol upload state, and round-tripping storage handle facts.
//! Optional upload-inventory behavior is covered by a separate helper.

use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{Duration, Utc};

use super::{MetadataValue, StateStore, UploadInventory, UploadMetadata, UploadState};
use crate::StorageHandle;
use crate::error::{Error, Result};

static NEXT_UPLOAD_ID: AtomicU64 = AtomicU64::new(1);

/// Asserts the complete `StateStore` conformance suite.
///
/// These scenarios use only the public [`StateStore`] API. The helper creates
/// unique upload IDs, but adapter tests should still prefer an isolated backend
/// namespace so old state cannot affect expiration-list assertions.
pub async fn assert_state_store_semantics<S>(store: &S)
where
    S: StateStore + ?Sized,
{
    create_then_update_overwrites_state(store).await;
    duplicate_create_is_rejected_and_preserves_existing_state(store).await;
    get_returns_snapshot_state(store).await;
    delete_is_idempotent(store).await;
    list_expired_returns_only_uploads_before_cutoff(store).await;
    rejects_unsafe_upload_ids(store).await;
    persists_protocol_state_and_storage_handle_facts(store).await;
}

/// Asserts optional [`UploadInventory`] behavior.
///
/// These scenarios use only the public [`UploadInventory`] API plus
/// [`StateStore::set`] to create fixture state. Adapter tests must provide an
/// isolated empty backend namespace because inventory intentionally lists every
/// known upload ID.
pub async fn assert_upload_inventory_semantics<S>(store: &S)
where
    S: StateStore + UploadInventory + ?Sized,
{
    upload_inventory_lists_all_known_upload_ids_in_order(store).await;
}

async fn create_then_update_overwrites_state<S>(store: &S)
where
    S: StateStore + ?Sized,
{
    let id = upload_id("create-update");
    let state = UploadState::new(&id).with_length(100);

    store
        .set(&state, true)
        .await
        .expect("create=true should create a new upload state");

    let mut updated = state.clone();
    updated.set_offset(40);
    store
        .set(&updated, false)
        .await
        .expect("create=false should update an existing upload state");

    let retrieved = store
        .get(&id)
        .await
        .expect("get should succeed after update")
        .expect("updated state should exist");
    assert_eq!(retrieved.offset(), 40, "update should persist new offset");
    assert_eq!(
        retrieved.length(),
        Some(100),
        "update should preserve persisted protocol state"
    );
}

async fn duplicate_create_is_rejected_and_preserves_existing_state<S>(store: &S)
where
    S: StateStore + ?Sized,
{
    let id = upload_id("duplicate-create");
    let original = UploadState::new(&id).with_length(10);
    let replacement = UploadState::new(&id).with_length(20);

    store
        .set(&original, true)
        .await
        .expect("initial create should succeed");

    let result = store.set(&replacement, true).await;
    assert!(
        matches!(result, Err(Error::AlreadyExists(_))),
        "duplicate create should return Error::AlreadyExists, got {result:?}"
    );

    let retrieved = store
        .get(&id)
        .await
        .expect("get should succeed after duplicate create")
        .expect("original state should still exist");
    assert_eq!(
        retrieved.length(),
        Some(10),
        "failed duplicate create must not replace existing state"
    );
}

async fn get_returns_snapshot_state<S>(store: &S)
where
    S: StateStore + ?Sized,
{
    let id = upload_id("snapshot");
    let mut metadata = UploadMetadata::new();
    metadata.insert("filename", "original.txt");

    let mut state = UploadState::new(&id)
        .with_length(100)
        .with_metadata(metadata);
    state.set_storage_key(format!("objects/{id}"));

    store
        .set(&state, true)
        .await
        .expect("create should succeed");

    let mut snapshot = store
        .get(&id)
        .await
        .expect("first get should succeed")
        .expect("state should exist");
    snapshot.set_offset(90);
    snapshot.set_storage_key("mutated-key");
    snapshot.metadata_mut().insert("filename", "mutated.txt");

    let retrieved = store
        .get(&id)
        .await
        .expect("second get should succeed")
        .expect("state should still exist");
    assert_eq!(
        retrieved.offset(),
        0,
        "mutating a retrieved UploadState must not mutate persisted state before set"
    );
    assert_eq!(
        retrieved.storage_key(),
        Some(format!("objects/{id}").as_str()),
        "mutating a retrieved storage key must not mutate persisted state before set"
    );
    assert_eq!(
        retrieved
            .metadata()
            .get("filename")
            .and_then(|v| v.as_str()),
        Some("original.txt"),
        "mutating retrieved metadata must not mutate persisted state before set"
    );
}

async fn delete_is_idempotent<S>(store: &S)
where
    S: StateStore + ?Sized,
{
    let id = upload_id("delete");

    store
        .delete(&id)
        .await
        .expect("delete should ignore a missing upload");

    let state = UploadState::new(&id);
    store
        .set(&state, true)
        .await
        .expect("create before delete should succeed");

    store
        .delete(&id)
        .await
        .expect("delete should remove an existing upload");
    assert!(
        store
            .get(&id)
            .await
            .expect("get after delete should succeed")
            .is_none(),
        "deleted upload should no longer be returned"
    );

    store
        .delete(&id)
        .await
        .expect("repeated delete should be idempotent");
}

async fn list_expired_returns_only_uploads_before_cutoff<S>(store: &S)
where
    S: StateStore + ?Sized,
{
    let cutoff = Utc::now();
    let expired_id = upload_id("expired");
    let active_id = upload_id("active");
    let no_expiration_id = upload_id("no-expiration");
    let exact_cutoff_id = upload_id("exact-cutoff");

    store
        .set(
            &UploadState::new(&expired_id).with_expiration(cutoff - Duration::seconds(1)),
            true,
        )
        .await
        .expect("creating expired state should succeed");
    store
        .set(
            &UploadState::new(&active_id).with_expiration(cutoff + Duration::seconds(1)),
            true,
        )
        .await
        .expect("creating active state should succeed");
    store
        .set(&UploadState::new(&no_expiration_id), true)
        .await
        .expect("creating state without expiration should succeed");
    store
        .set(
            &UploadState::new(&exact_cutoff_id).with_expiration(cutoff),
            true,
        )
        .await
        .expect("creating exact-cutoff state should succeed");

    let expired = store
        .list_expired(cutoff)
        .await
        .expect("list_expired should succeed");
    assert_contains(&expired, &expired_id, "expired upload should be listed");
    assert_not_contains(
        &expired,
        &active_id,
        "upload expiring after cutoff should not be listed",
    );
    assert_not_contains(
        &expired,
        &no_expiration_id,
        "upload without expiration should not be listed",
    );
    assert_not_contains(
        &expired,
        &exact_cutoff_id,
        "list_expired should use a strict before-cutoff comparison",
    );
}

async fn rejects_unsafe_upload_ids<S>(store: &S)
where
    S: StateStore + ?Sized,
{
    for id in ["", "../escape", "nested/id", "nested\\id", "bad\nnewline"] {
        assert_invalid_upload_id(store.set(&UploadState::new(id), true).await, "set", id);
        assert_invalid_upload_id(
            store.set(&UploadState::new(id), false).await,
            "set update",
            id,
        );
        assert_invalid_upload_id(store.get(id).await, "get", id);
        assert_invalid_upload_id(store.delete(id).await, "delete", id);
    }
}

async fn persists_protocol_state_and_storage_handle_facts<S>(store: &S)
where
    S: StateStore + ?Sized,
{
    let partial_id = upload_id("protocol-state-partial");
    let expires_at = Utc::now() + Duration::minutes(30);
    let mut metadata = UploadMetadata::new();
    metadata.insert("filename", "photo.jpg");
    metadata.insert("raw", MetadataValue::from(&b"\x00\xff"[..]));

    let mut handle = StorageHandle::new(format!("objects/{partial_id}"));
    handle.set_internal("multipart-upload-id", "upload-session-1");
    handle.set_internal("etag-1", "etag-value-1");

    let mut partial = UploadState::new(&partial_id)
        .with_length(123)
        .with_expiration(expires_at)
        .with_metadata(metadata)
        .as_partial();
    partial.set_offset(45);
    partial.set_storage_handle(handle);

    store
        .set(&partial, true)
        .await
        .expect("create should persist rich protocol state");

    let retrieved = store
        .get(&partial_id)
        .await
        .expect("get should succeed for rich protocol state")
        .expect("rich protocol state should exist");
    assert_eq!(retrieved.id(), partial_id);
    assert_eq!(retrieved.offset(), 45);
    assert_eq!(retrieved.length(), Some(123));
    assert_eq!(retrieved.created_at(), partial.created_at());
    assert_eq!(retrieved.expires_at(), Some(&expires_at));
    assert!(retrieved.is_partial(), "partial flag should persist");
    assert!(
        !retrieved.is_final(),
        "partial state should not become final"
    );
    let retrieved_handle = retrieved
        .storage_handle()
        .expect("storage handle facts should persist with upload state");
    assert_eq!(retrieved_handle.key(), format!("objects/{partial_id}"));
    assert_eq!(
        retrieved_handle.get_internal("multipart-upload-id"),
        Some("upload-session-1")
    );
    assert_eq!(
        retrieved_handle.get_internal("etag-1"),
        Some("etag-value-1")
    );
    assert_eq!(
        retrieved
            .metadata()
            .get("filename")
            .and_then(|v| v.as_str()),
        Some("photo.jpg")
    );
    assert_eq!(
        retrieved.metadata().get("raw").map(|v| v.as_bytes()),
        Some(&b"\x00\xff"[..])
    );

    let final_id = upload_id("protocol-state-final");
    let parts = vec![upload_id("part-a"), upload_id("part-b")];
    let final_state = UploadState::new(&final_id)
        .with_length(123)
        .as_final(parts.clone());

    store
        .set(&final_state, true)
        .await
        .expect("create should persist final upload state");

    let retrieved = store
        .get(&final_id)
        .await
        .expect("get should succeed for final upload state")
        .expect("final upload state should exist");
    assert!(retrieved.is_final(), "final flag should persist");
    assert!(
        !retrieved.is_partial(),
        "final state should not become partial"
    );
    assert_eq!(retrieved.parts(), Some(parts.as_slice()));
}

async fn upload_inventory_lists_all_known_upload_ids_in_order<S>(store: &S)
where
    S: StateStore + UploadInventory + ?Sized,
{
    let root = upload_id("inventory");
    let active_id = format!("{root}-z-active");
    let expired_id = format!("{root}-a-expired");
    let partial_id = format!("{root}-m-partial");

    store
        .set(&UploadState::new(&active_id), true)
        .await
        .expect("creating active state should succeed");
    store
        .set(
            &UploadState::new(&expired_id).with_expiration(Utc::now() - Duration::seconds(1)),
            true,
        )
        .await
        .expect("creating expired state should succeed");
    store
        .set(&UploadState::new(&partial_id).as_partial(), true)
        .await
        .expect("creating partial state should succeed");

    let page1 = store
        .list_upload_ids(2, 0)
        .await
        .expect("upload inventory page 1 should succeed");
    assert_eq!(
        page1,
        vec![expired_id.clone(), partial_id.clone()],
        "upload inventory should return deterministic upload-id ordered pages"
    );

    let page2 = store
        .list_upload_ids(2, 2)
        .await
        .expect("upload inventory page 2 should succeed");
    assert_eq!(
        page2,
        vec![active_id],
        "upload inventory offset should continue deterministic upload-id ordering"
    );
}

fn upload_id(scenario: &str) -> String {
    let next = NEXT_UPLOAD_ID.fetch_add(1, Ordering::Relaxed);
    format!("conformance-state-{scenario}-{next}")
}

fn assert_contains(ids: &[String], expected: &str, message: &str) {
    assert!(
        ids.iter().any(|id| id == expected),
        "{message}: expected {expected:?} in {ids:?}"
    );
}

fn assert_not_contains(ids: &[String], unexpected: &str, message: &str) {
    assert!(
        ids.iter().all(|id| id != unexpected),
        "{message}: did not expect {unexpected:?} in {ids:?}"
    );
}

fn assert_invalid_upload_id<T: std::fmt::Debug>(result: Result<T>, operation: &str, id: &str) {
    assert!(
        matches!(result, Err(Error::InvalidUploadId(_))),
        "{operation} should reject unsafe upload id {id:?}, got {result:?}"
    );
}
