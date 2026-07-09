//! Shared conformance scenarios for [`Storage`] implementations.
//!
//! Adapter crates can enable the `conformance-storage` feature in their
//! `dev-dependencies` and call these helpers from their own async tests:
//!
//! ```toml
//! [dev-dependencies]
//! tus-protocol = { version = "...", features = ["conformance-storage"] }
//! ```
//!
//! Upload-write conformance covers behavior required by the protocol lifecycle
//! for accepting PATCH bytes: creating handles, appending at exact offsets,
//! rejecting stale offsets without consuming the body, PATCH-boundary atomicity
//! on body stream errors, size-based recovery, failed concatenation visibility,
//! and idempotent cleanup. Read/download conformance covers behavior needed
//! only when an integration exposes stored upload bytes back to callers:
//! full streams, byte ranges, and observable concatenation ordering.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_util::StreamExt;

use super::{
    AppendRequest, ByteStream, ChunkStream, ConcatRequest, Storage, StorageHandle, StorageReader,
};

static NEXT_UPLOAD_ID: AtomicU64 = AtomicU64::new(1);

/// Asserts the complete `Storage` conformance suite.
///
/// Use this for adapters that support both upload writes and read/download
/// behavior through [`StorageReader::stream`] and
/// [`StorageReader::stream_range`].
pub async fn assert_full_semantics<S>(storage: &S)
where
    S: Storage + StorageReader + ?Sized,
{
    assert_upload_write_semantics(storage).await;
    assert_read_download_semantics(storage).await;
}

/// Asserts upload-write semantics required by the protocol lifecycle.
///
/// These scenarios avoid any storage-internal facts and only observe behavior
/// through [`Storage::append`], [`Storage::concat`], [`Storage::delete`], and
/// [`Storage::size`].
pub async fn assert_upload_write_semantics<S>(storage: &S)
where
    S: Storage + ?Sized,
{
    create_and_append_accept_upload_bytes(storage).await;
    append_preserves_existing_handle_internal_facts(storage).await;
    append_rejects_stale_offset_without_consuming_body(storage).await;
    append_stream_error_leaves_previous_bytes_visible(storage).await;
    completing_append_stream_error_does_not_reach_length(storage).await;
    size_supports_recovery_from_stale_handle(storage).await;
    failed_concat_preserves_existing_target_size(storage).await;
    delete_is_idempotent_for_completed_upload(storage).await;
    delete_cleans_unfinished_upload(storage).await;
}

/// Asserts optional read/download semantics.
///
/// Run these scenarios only for adapters that intend to expose download or
/// inspection behavior through [`StorageReader::stream`] and
/// [`StorageReader::stream_range`].
pub async fn assert_read_download_semantics<S>(storage: &S)
where
    S: Storage + StorageReader + ?Sized,
{
    get_stream_returns_uploaded_bytes(storage).await;
    get_range_returns_requested_bytes(storage).await;
    concat_preserves_part_order(storage).await;
    failed_concat_preserves_existing_target_body(storage).await;
}

async fn create_and_append_accept_upload_bytes<S>(storage: &S)
where
    S: Storage + ?Sized,
{
    let handle = storage
        .create(&upload_id("create-append"))
        .await
        .expect("create should return a usable storage handle");
    assert!(
        !handle.key().is_empty(),
        "storage handle key must not be empty"
    );

    let handle = append_bytes(storage, handle, 0, Bytes::from_static(b"hello "), false).await;
    assert_eq!(
        storage.size(&handle).await.expect("size should succeed"),
        Some(6),
        "size should reflect the first append"
    );

    let handle = append_bytes(storage, handle, 6, Bytes::from_static(b"world"), true).await;
    assert_eq!(
        storage.size(&handle).await.expect("size should succeed"),
        Some(11),
        "size should reflect all appended bytes"
    );
}

async fn append_preserves_existing_handle_internal_facts<S>(storage: &S)
where
    S: Storage + ?Sized,
{
    let mut handle = storage
        .create(&upload_id("append-handle-internals"))
        .await
        .expect("create should succeed");
    handle.set_internal("conformance_fact", "keep-me");

    let handle = append_bytes(storage, handle, 0, Bytes::from_static(b"hello"), false).await;
    assert_eq!(
        handle.internal("conformance_fact"),
        Some("keep-me"),
        "append should preserve existing StorageHandle internal facts"
    );

    let handle = append_bytes(storage, handle, 5, Bytes::from_static(b"world"), true).await;
    assert_eq!(
        handle.internal("conformance_fact"),
        Some("keep-me"),
        "completion append should preserve existing StorageHandle internal facts"
    );
}

async fn append_rejects_stale_offset_without_consuming_body<S>(storage: &S)
where
    S: Storage + ?Sized,
{
    let handle =
        create_with_bytes(storage, "stale-offset", Bytes::from_static(b"seed"), false).await;

    let stream: ByteStream = Box::pin(futures_util::stream::once(async {
        panic!("body stream should not be read when storage offset is stale");
        #[allow(unreachable_code)]
        Ok(Bytes::from_static(b"must not be consumed"))
    }));

    let result = storage
        .append(AppendRequest::new(
            handle.clone(),
            0,
            ChunkStream::from_stream(stream),
            false,
        ))
        .await;

    assert!(
        result.is_err(),
        "append should reject an expected offset that does not match storage size"
    );
    assert_eq!(
        storage.size(&handle).await.expect("size should succeed"),
        Some(4),
        "failed stale-offset append must not change visible storage size"
    );
}

async fn append_stream_error_leaves_previous_bytes_visible<S>(storage: &S)
where
    S: Storage + ?Sized,
{
    let handle = create_with_bytes(
        storage,
        "stream-error",
        Bytes::from_static(b"intact "),
        false,
    )
    .await;

    let stream: ByteStream = Box::pin(futures_util::stream::iter(vec![
        Ok(Bytes::from_static(b"partial-")),
        Err(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "client gone",
        )),
        Ok(Bytes::from_static(b"trailer-that-must-not-commit")),
    ]));

    let result = storage
        .append(AppendRequest::new(
            handle.clone(),
            7,
            ChunkStream::from_stream(stream),
            false,
        ))
        .await;

    assert!(
        result.is_err(),
        "append should fail when the request body stream fails"
    );
    assert_eq!(
        storage.size(&handle).await.expect("size should succeed"),
        Some(7),
        "a failed PATCH body stream must leave the previous size visible"
    );
}

/// A terminal stream error on a *completing* append must roll the write back so
/// `size()` does not reach the upload length.
///
/// This is the completing-chunk counterpart to
/// [`append_stream_error_leaves_previous_bytes_visible`]: the failing chunk
/// arrives at full byte count but the append fails (a checksum mismatch on the
/// final chunk surfaces exactly this way). Recovery adopts `size()` without
/// re-validating content and completes an upload once it reaches the declared
/// length, so a backend that finalizes incrementally must not let the completing
/// chunk's bytes become visible at the length until the append succeeds.
///
/// The crash window — a process that dies after the completing bytes are durably
/// flushed but before `append` returns — cannot be exercised from a conformance
/// test; a zero-window backend stages the completing write and promotes it
/// atomically (see the OpenDAL adapter).
async fn completing_append_stream_error_does_not_reach_length<S>(storage: &S)
where
    S: Storage + ?Sized,
{
    let handle = create_with_bytes(
        storage,
        "completing-stream-error",
        Bytes::from_static(b"intact "),
        false,
    )
    .await;

    // A completing append whose final frame is an error. The good frame would
    // bring the upload to its declared length; the backend must not expose it.
    let stream: ByteStream = Box::pin(futures_util::stream::iter(vec![
        Ok(Bytes::from_static(b"final-content-that-must-not-commit")),
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "checksum mismatch",
        )),
    ]));

    let result = storage
        .append(AppendRequest::new(
            handle.clone(),
            7,
            ChunkStream::from_stream(stream),
            true,
        ))
        .await;

    assert!(
        result.is_err(),
        "a completing append with a terminal stream error must fail"
    );
    assert_eq!(
        storage.size(&handle).await.expect("size should succeed"),
        Some(7),
        "a failed completing append must roll back to the pre-append offset, \
         never expose the full-length bytes recovery would adopt as complete"
    );
}

async fn size_supports_recovery_from_stale_handle<S>(storage: &S)
where
    S: Storage + ?Sized,
{
    let persisted_handle = storage
        .create(&upload_id("size-recovery"))
        .await
        .expect("create should succeed");
    let active_handle = persisted_handle.clone();

    let _updated_handle = append_bytes(
        storage,
        active_handle,
        0,
        Bytes::from_static(b"hello"),
        false,
    )
    .await;

    let recovered_offset = storage
        .size(&persisted_handle)
        .await
        .expect("size should be recoverable from a persisted handle");
    assert_eq!(
        recovered_offset,
        Some(5),
        "size should report bytes accepted before updated handle facts were persisted"
    );

    let recovered_handle = append_bytes(
        storage,
        persisted_handle,
        recovered_offset.unwrap(),
        Bytes::from_static(b"world"),
        true,
    )
    .await;
    assert_eq!(
        storage
            .size(&recovered_handle)
            .await
            .expect("size should succeed after recovery append"),
        Some(10),
        "appending from the recovered offset should preserve previously accepted bytes"
    );
}

async fn failed_concat_preserves_existing_target_size<S>(storage: &S)
where
    S: Storage + ?Sized,
{
    let part = create_with_bytes(
        storage,
        "concat-size-part",
        Bytes::from_static(b"new"),
        true,
    )
    .await;
    let missing_part = create_missing_handle(storage, "concat-size-missing").await;
    let target = create_with_bytes(
        storage,
        "concat-size-target",
        Bytes::from_static(b"original"),
        true,
    )
    .await;
    let original_size = storage
        .size(&target)
        .await
        .expect("target size should be readable before concat");

    let result = storage
        .concat(ConcatRequest::new(target.clone(), vec![part, missing_part]))
        .await;

    assert!(result.is_err(), "concat should fail when a part is missing");
    assert_eq!(
        storage
            .size(&target)
            .await
            .expect("target size should remain readable after failed concat"),
        original_size,
        "failed concat must not expose a partial target size"
    );
}

async fn delete_is_idempotent_for_completed_upload<S>(storage: &S)
where
    S: Storage + ?Sized,
{
    let handle = create_with_bytes(
        storage,
        "delete-completed",
        Bytes::from_static(b"data"),
        true,
    )
    .await;
    assert_eq!(
        storage.size(&handle).await.expect("size should succeed"),
        Some(4),
        "completed upload should be visible before delete"
    );

    storage
        .delete(&handle)
        .await
        .expect("first delete should succeed");
    assert_eq!(
        storage
            .size(&handle)
            .await
            .expect("size should succeed after delete"),
        None,
        "deleted completed upload should no longer be visible"
    );
    storage
        .delete(&handle)
        .await
        .expect("second delete should be idempotent");
}

async fn delete_cleans_unfinished_upload<S>(storage: &S)
where
    S: Storage + ?Sized,
{
    let handle = create_with_bytes(
        storage,
        "delete-unfinished",
        Bytes::from_static(b"staged"),
        false,
    )
    .await;
    assert_eq!(
        storage.size(&handle).await.expect("size should succeed"),
        Some(6),
        "unfinished upload should be visible before delete"
    );

    storage
        .delete(&handle)
        .await
        .expect("delete should clean unfinished upload data");
    assert_eq!(
        storage
            .size(&handle)
            .await
            .expect("size should succeed after deleting unfinished upload"),
        None,
        "unfinished upload data should not remain visible after delete"
    );
    storage
        .delete(&handle)
        .await
        .expect("unfinished upload delete should be idempotent");
}

async fn get_stream_returns_uploaded_bytes<S>(storage: &S)
where
    S: Storage + StorageReader + ?Sized,
{
    let handle = storage
        .create(&upload_id("get-stream"))
        .await
        .expect("create should succeed");
    let handle = append_bytes(storage, handle, 0, Bytes::from_static(b"hello "), false).await;
    let handle = append_bytes(storage, handle, 6, Bytes::from_static(b"world"), true).await;

    let body = collect_stream(
        storage
            .stream(&handle)
            .await
            .expect("get_stream should succeed for a completed upload"),
    )
    .await;
    assert_eq!(
        body, b"hello world",
        "get_stream should return all accepted bytes in order"
    );
}

async fn get_range_returns_requested_bytes<S>(storage: &S)
where
    S: Storage + StorageReader + ?Sized,
{
    let handle = create_with_bytes(
        storage,
        "get-range",
        Bytes::from_static(b"alpha beta gamma"),
        true,
    )
    .await;

    let range = collect_stream(
        storage
            .stream_range(&handle, 6, Some(10))
            .await
            .expect("bounded get_range should succeed"),
    )
    .await;
    assert_eq!(range, b"beta", "bounded get_range should return the slice");

    let suffix = collect_stream(
        storage
            .stream_range(&handle, 11, None)
            .await
            .expect("suffix get_range should succeed"),
    )
    .await;
    assert_eq!(
        suffix, b"gamma",
        "open-ended get_range should return the suffix"
    );
}

async fn concat_preserves_part_order<S>(storage: &S)
where
    S: Storage + StorageReader + ?Sized,
{
    let part1 =
        create_with_bytes(storage, "concat-order-1", Bytes::from_static(b"left"), true).await;
    let part2 = create_with_bytes(
        storage,
        "concat-order-2",
        Bytes::from_static(b"middle"),
        true,
    )
    .await;
    let part3 = create_with_bytes(
        storage,
        "concat-order-3",
        Bytes::from_static(b"right"),
        true,
    )
    .await;
    let target = storage
        .create(&upload_id("concat-order-target"))
        .await
        .expect("target create should succeed");

    let target = storage
        .concat(ConcatRequest::new(target, vec![part1, part2, part3]))
        .await
        .expect("concat should succeed");

    let body = collect_stream(
        storage
            .stream(&target)
            .await
            .expect("concatenated target should be readable"),
    )
    .await;
    assert_eq!(
        body, b"leftmiddleright",
        "concat should preserve the order of the supplied part handles"
    );
}

async fn failed_concat_preserves_existing_target_body<S>(storage: &S)
where
    S: Storage + StorageReader + ?Sized,
{
    let part = create_with_bytes(
        storage,
        "concat-body-part",
        Bytes::from_static(b"new"),
        true,
    )
    .await;
    let missing_part = create_missing_handle(storage, "concat-body-missing").await;
    let target = create_with_bytes(
        storage,
        "concat-body-target",
        Bytes::from_static(b"original"),
        true,
    )
    .await;

    let result = storage
        .concat(ConcatRequest::new(target.clone(), vec![part, missing_part]))
        .await;

    assert!(result.is_err(), "concat should fail when a part is missing");
    let body = collect_stream(
        storage
            .stream(&target)
            .await
            .expect("failed concat should leave the original target readable"),
    )
    .await;
    assert_eq!(
        body, b"original",
        "failed concat must not expose partially concatenated bytes"
    );
}

async fn create_with_bytes<S>(
    storage: &S,
    scenario: &str,
    bytes: Bytes,
    completes_upload: bool,
) -> StorageHandle
where
    S: Storage + ?Sized,
{
    let handle = storage
        .create(&upload_id(scenario))
        .await
        .expect("create should succeed");
    append_bytes(storage, handle, 0, bytes, completes_upload).await
}

async fn append_bytes<S>(
    storage: &S,
    handle: StorageHandle,
    expected_offset: u64,
    bytes: Bytes,
    completes_upload: bool,
) -> StorageHandle
where
    S: Storage + ?Sized,
{
    storage
        .append(AppendRequest::new(
            handle,
            expected_offset,
            ChunkStream::from_bytes(bytes),
            completes_upload,
        ))
        .await
        .expect("append should succeed")
}

async fn create_missing_handle<S>(storage: &S, scenario: &str) -> StorageHandle
where
    S: Storage + ?Sized,
{
    let handle = storage
        .create(&upload_id(scenario))
        .await
        .expect("create should succeed");
    storage
        .delete(&handle)
        .await
        .expect("delete should make the handle missing");
    handle
}

async fn collect_stream(mut stream: ByteStream) -> Vec<u8> {
    let mut data = Vec::new();
    while let Some(chunk) = stream.next().await {
        data.extend_from_slice(&chunk.expect("storage byte stream should not fail"));
    }
    data
}

fn upload_id(scenario: &str) -> String {
    let id = NEXT_UPLOAD_ID.fetch_add(1, Ordering::Relaxed);
    format!("conformance-{scenario}-{id}")
}
