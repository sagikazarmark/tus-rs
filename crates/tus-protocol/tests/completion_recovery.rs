#![cfg(all(
    feature = "storage-memory",
    feature = "state-memory",
    feature = "lock-memory",
    not(target_arch = "wasm32")
))]

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use tokio::sync::{Notify, oneshot};
use tus_protocol::locking::memory::MemoryLocker;
use tus_protocol::state::memory::MemoryStateStore;
use tus_protocol::storage::memory::MemoryStorage;
use tus_protocol::{
    Config, DownloadRequest, Error, Extension, Headers, HookChain, PreHookResult, Protocol,
    RequestBody, Response, StateStore, UploadConcat, UploadId,
};

#[derive(Clone)]
struct Server {
    config: Arc<Config>,
    storage: Arc<MemoryStorage>,
    store: Arc<MemoryStateStore>,
    locker: Arc<MemoryLocker>,
    hooks: Arc<HookChain>,
}

impl Server {
    fn new(config: Config, hooks: HookChain) -> Self {
        Self {
            config: Arc::new(config),
            storage: Arc::new(MemoryStorage::new()),
            store: Arc::new(MemoryStateStore::new()),
            locker: Arc::new(MemoryLocker::new()),
            hooks: Arc::new(hooks),
        }
    }

    fn protocol(&self) -> Protocol<'_, MemoryStorage, MemoryStateStore, MemoryLocker, HookChain> {
        Protocol::new(
            &self.config,
            &*self.storage,
            &*self.store,
            &*self.locker,
            &*self.hooks,
        )
    }

    async fn create(&self, headers: Headers) -> UploadId {
        upload_id(
            &self
                .protocol()
                .post(headers, RequestBody::absent())
                .await
                .unwrap(),
        )
    }
}

fn upload_id(response: &Response) -> UploadId {
    response
        .headers
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

fn creation_headers(length: Option<u64>) -> Headers {
    let mut headers = Headers::default();
    headers.upload_length = length;
    headers.upload_defer_length = length.is_none();
    headers
}

fn patch_headers(offset: u64, size: u64) -> Headers {
    let mut headers = Headers::default();
    headers.upload_offset = Some(offset);
    headers.content_length = Some(size);
    headers.content_type = Some("application/offset+octet-stream".into());
    headers
}

fn assert_rejected(error: Error) {
    assert!(matches!(
        error,
        Error::HookRejected {
            status_code: 403,
            ..
        }
    ));
}

#[tokio::test]
async fn rejected_finish_is_retried_before_head_download_or_patch() {
    for deferred in [false, true] {
        let allow = Arc::new(AtomicBool::new(false));
        let gates = Arc::new(AtomicUsize::new(0));
        let posts = Arc::new(AtomicUsize::new(0));
        let hooks = HookChain::new()
            .on_pre_finish({
                let (allow, gates) = (allow.clone(), gates.clone());
                move |_| {
                    let (allow, gates) = (allow.clone(), gates.clone());
                    async move {
                        gates.fetch_add(1, Ordering::SeqCst);
                        Ok(if allow.load(Ordering::SeqCst) {
                            PreHookResult::proceed()
                        } else {
                            PreHookResult::reject(403, "finish blocked")
                        })
                    }
                }
            })
            .on_post_finish({
                let posts = posts.clone();
                move |_| {
                    let posts = posts.clone();
                    async move {
                        posts.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }
            });
        let server = Server::new(Config::default(), hooks);
        let id = server
            .create(creation_headers((!deferred).then_some(5)))
            .await;
        let mut headers = patch_headers(0, 5);
        headers.upload_length = deferred.then_some(5);
        assert_rejected(
            server
                .protocol()
                .patch(headers, &id, RequestBody::from_bytes("Hello".into()))
                .await
                .unwrap_err(),
        );
        let state = server.store.get(id.as_str()).await.unwrap().unwrap();
        assert_eq!(state.offset(), 5);
        assert_eq!(state.length(), Some(5));
        assert!(state.is_finish_pending());

        assert_rejected(server.protocol().head(&id).await.unwrap_err());
        assert_rejected(
            server
                .protocol()
                .download(DownloadRequest::new(&id))
                .await
                .err()
                .unwrap(),
        );
        assert_rejected(
            server
                .protocol()
                .patch(patch_headers(5, 0), &id, RequestBody::from_bytes("".into()))
                .await
                .unwrap_err(),
        );
        assert_eq!(gates.load(Ordering::SeqCst), 4);
        assert_eq!(posts.load(Ordering::SeqCst), 0);

        allow.store(true, Ordering::SeqCst);
        let head = server.protocol().head(&id).await.unwrap();
        assert_eq!(head.headers.get("upload-offset").unwrap(), "5");
        assert!(
            !server
                .store
                .get(id.as_str())
                .await
                .unwrap()
                .unwrap()
                .is_finish_pending()
        );
        server.protocol().head(&id).await.unwrap();
        server
            .protocol()
            .download(DownloadRequest::new(&id))
            .await
            .unwrap();
        assert_eq!(gates.load(Ordering::SeqCst), 5);
        assert_eq!(posts.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn pending_partial_cannot_bypass_finish_validation_through_concatenation() {
    for allow_unfinished in [false, true] {
        let allow = Arc::new(AtomicBool::new(false));
        let hooks = HookChain::new().on_pre_finish({
            let allow = allow.clone();
            move |_| {
                let allow = allow.clone();
                async move {
                    Ok(if allow.load(Ordering::SeqCst) {
                        PreHookResult::proceed()
                    } else {
                        PreHookResult::reject(403, "finish blocked")
                    })
                }
            }
        });
        let mut config = Config::default().with_extension(Extension::Concatenation);
        if allow_unfinished {
            config = config.with_extension(Extension::ConcatenationUnfinished);
        }
        let server = Server::new(config, hooks);
        let mut headers = creation_headers(Some(5));
        headers.upload_concat = Some(UploadConcat::Partial);
        let id = server.create(headers).await;
        let mut final_headers = Headers::default();
        final_headers.upload_concat = Some(UploadConcat::Final(vec![format!("/files/{id}")]));
        let planned_final = if allow_unfinished {
            Some(server.create(final_headers.clone()).await)
        } else {
            None
        };
        assert_rejected(
            server
                .protocol()
                .patch(
                    patch_headers(0, 5),
                    &id,
                    RequestBody::from_bytes("Hello".into()),
                )
                .await
                .unwrap_err(),
        );
        assert!(matches!(
            server
                .protocol()
                .post(final_headers.clone(), RequestBody::absent())
                .await,
            Err(Error::IncompleteUpload(_))
        ));
        if let Some(final_id) = &planned_final {
            assert!(matches!(
                server.protocol().head(final_id).await,
                Err(Error::IncompleteUpload(_))
            ));
            assert!(matches!(
                server
                    .protocol()
                    .download(DownloadRequest::new(final_id))
                    .await,
                Err(Error::IncompleteUpload(_))
            ));
        }

        allow.store(true, Ordering::SeqCst);
        server.protocol().head(&id).await.unwrap();
        if let Some(final_id) = &planned_final {
            server
                .protocol()
                .download(DownloadRequest::new(final_id))
                .await
                .unwrap();
        }
        let response = server
            .protocol()
            .post(final_headers, RequestBody::absent())
            .await
            .unwrap();
        server
            .protocol()
            .download(DownloadRequest::new(&upload_id(&response)))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn partial_completion_is_recovered_if_crash_precedes_offset_write() {
    use tus_protocol::{AppendRequest, ChunkStream, Storage};

    let allow = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let hooks = HookChain::new().on_pre_finish({
        let (allow, calls) = (allow.clone(), calls.clone());
        move |_| {
            let (allow, calls) = (allow.clone(), calls.clone());
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(if allow.load(Ordering::SeqCst) {
                    PreHookResult::proceed()
                } else {
                    PreHookResult::reject(403, "finish blocked")
                })
            }
        }
    });
    let server = Server::new(
        Config::default().with_extension(Extension::Concatenation),
        hooks,
    );
    let mut headers = creation_headers(Some(5));
    headers.upload_concat = Some(UploadConcat::Partial);
    let id = server.create(headers).await;
    let state = server.store.get(id.as_str()).await.unwrap().unwrap();
    // Model durable append followed by a crash before the offset/phase write.
    server
        .storage
        .append(AppendRequest::new(
            state.storage_handle().unwrap(),
            0,
            ChunkStream::from_bytes("Hello".into()),
            true,
        ))
        .await
        .unwrap();
    assert_rejected(server.protocol().head(&id).await.unwrap_err());
    assert_rejected(server.protocol().head(&id).await.unwrap_err());
    assert_eq!(
        server
            .store
            .get(id.as_str())
            .await
            .unwrap()
            .unwrap()
            .offset(),
        0
    );
    allow.store(true, Ordering::SeqCst);
    assert_eq!(
        server
            .protocol()
            .head(&id)
            .await
            .unwrap()
            .headers
            .get("upload-offset")
            .unwrap(),
        "5"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn cancellation_during_finish_or_post_receive_keeps_validation_pending() {
    for cancel_in_post_receive in [false, true] {
        let entered = Arc::new(Notify::new());
        let gates = Arc::new(AtomicUsize::new(0));
        let hooks = HookChain::new()
            .on_post_receive({
                let entered = entered.clone();
                move |_| {
                    let entered = entered.clone();
                    async move {
                        if cancel_in_post_receive {
                            entered.notify_one();
                            std::future::pending::<()>().await;
                        }
                        Ok(())
                    }
                }
            })
            .on_pre_finish({
                let (entered, gates) = (entered.clone(), gates.clone());
                move |_| {
                    let (entered, gates) = (entered.clone(), gates.clone());
                    async move {
                        if gates.fetch_add(1, Ordering::SeqCst) == 0 && !cancel_in_post_receive {
                            entered.notify_one();
                            std::future::pending::<()>().await;
                        }
                        Ok(PreHookResult::proceed())
                    }
                }
            });
        let server = Server::new(Config::default(), hooks);
        let id = server.create(creation_headers(Some(5))).await;
        let task = tokio::spawn({
            let (server, id) = (server.clone(), id.clone());
            async move {
                server
                    .protocol()
                    .patch(
                        patch_headers(0, 5),
                        &id,
                        RequestBody::from_bytes("Hello".into()),
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        assert!(
            server
                .store
                .get(id.as_str())
                .await
                .unwrap()
                .unwrap()
                .is_finish_pending()
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!server.locker.is_locked(id.as_str()));
        let head = server.protocol().head(&id).await.unwrap();
        assert_eq!(head.headers.get("upload-offset").unwrap(), "5");
        assert!(
            !server
                .store
                .get(id.as_str())
                .await
                .unwrap()
                .unwrap()
                .is_finish_pending()
        );
        assert_eq!(
            gates.load(Ordering::SeqCst),
            if cancel_in_post_receive { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn cancelled_zero_length_or_creation_with_upload_finish_is_recoverable() {
    for length in [0, 5] {
        let (send_id, receive_id) = oneshot::channel::<UploadId>();
        let send_id = Arc::new(std::sync::Mutex::new(Some(send_id)));
        let calls = Arc::new(AtomicUsize::new(0));
        let hooks = HookChain::new().on_pre_finish({
            let calls = calls.clone();
            move |ctx| {
                let (send_id, calls) = (send_id.clone(), calls.clone());
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        send_id
                            .lock()
                            .unwrap()
                            .take()
                            .unwrap()
                            .send(ctx.upload().id().parse().unwrap())
                            .unwrap();
                        std::future::pending::<()>().await;
                    }
                    Ok(PreHookResult::proceed())
                }
            }
        });
        let server = Server::new(
            Config::default().with_extension(Extension::CreationWithUpload),
            hooks,
        );
        let task = tokio::spawn({
            let server = server.clone();
            async move {
                let mut headers = creation_headers(Some(length));
                let body = if length == 0 {
                    RequestBody::absent()
                } else {
                    headers.content_length = Some(length);
                    headers.content_type = Some("application/offset+octet-stream".into());
                    RequestBody::from_bytes("Hello".into())
                };
                server.protocol().post(headers, body).await
            }
        });
        let id = tokio::time::timeout(Duration::from_secs(5), receive_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            server
                .store
                .get(id.as_str())
                .await
                .unwrap()
                .unwrap()
                .is_finish_pending()
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let head = server.protocol().head(&id).await.unwrap();
        assert_eq!(
            head.headers.get("upload-offset").unwrap(),
            length.to_string().as_str()
        );
        assert!(
            !server
                .store
                .get(id.as_str())
                .await
                .unwrap()
                .unwrap()
                .is_finish_pending()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

#[cfg(all(feature = "storage-file", feature = "checksum"))]
#[tokio::test]
async fn cancelled_checksumming_body_never_becomes_visible_to_file_recovery() {
    use bytes::Bytes;
    use futures_util::{StreamExt, stream};
    use tus_protocol::storage::file::FileStorage;
    use tus_protocol::{
        BodyFrame, ChecksumAlgorithm, Storage, UploadChecksum, UploadState, WriteMode,
        calculate_checksum,
    };

    for offset in [0, 2] {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(FileStorage::new(directory.path()).await.unwrap());
        let server = Server::new(
            Config::default().with_checksum(ChecksumAlgorithm::Sha256),
            HookChain::new(),
        );
        let mut state = UploadState::new("unverified-body").with_length(offset + 5);
        let handle = storage.create(state.id()).await.unwrap();
        state.set_storage_handle(handle.clone());
        server
            .store
            .set(&state, WriteMode::CreateNew)
            .await
            .unwrap();
        let id: UploadId = state.id().parse().unwrap();
        if offset != 0 {
            Protocol::new(
                &server.config,
                &*storage,
                &*server.store,
                &*server.locker,
                &*server.hooks,
            )
            .patch(
                patch_headers(0, offset),
                &id,
                RequestBody::from_bytes("Hi".into()),
            )
            .await
            .unwrap();
        }
        let drained = Arc::new(Notify::new());
        let task = tokio::spawn({
            let (server, storage, id, drained) =
                (server.clone(), storage.clone(), id.clone(), drained.clone());
            async move {
                let mut headers = patch_headers(offset, 5);
                headers.upload_checksum = Some(UploadChecksum::new(
                    ChecksumAlgorithm::Sha256,
                    calculate_checksum(ChecksumAlgorithm::Sha256, b"Hello"),
                ));
                let frames =
                    stream::once(async { Ok(BodyFrame::Data(Bytes::from_static(b"Wrong"))) })
                        .chain(stream::once(async move {
                            drained.notify_one();
                            std::future::pending().await
                        }));
                Protocol::new(
                    &server.config,
                    &*storage,
                    &*server.store,
                    &*server.locker,
                    &*server.hooks,
                )
                .patch(headers, &id, RequestBody::from_stream(Box::pin(frames)))
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), drained.notified())
            .await
            .unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), Some(offset));
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let protocol = Protocol::new(
            &server.config,
            &*storage,
            &*server.store,
            &*server.locker,
            &*server.hooks,
        );
        let head = protocol.head(&id).await.unwrap();
        assert_eq!(
            head.headers.get("upload-offset").unwrap(),
            offset.to_string().as_str()
        );
        let prefix = if offset == 0 {
            b"".as_slice()
        } else {
            b"Hi".as_slice()
        };
        assert_eq!(
            tokio::fs::read(directory.path().join(handle.key()))
                .await
                .unwrap(),
            prefix
        );
        assert_eq!(
            std::fs::read_dir(directory.path()).unwrap().count(),
            1,
            "cancelled staging file was not removed"
        );

        let mut checked_headers = patch_headers(offset, 5);
        checked_headers.upload_checksum = Some(UploadChecksum::new(
            ChecksumAlgorithm::Sha256,
            calculate_checksum(ChecksumAlgorithm::Sha256, b"Hello"),
        ));
        let wrong = stream::iter([Ok(BodyFrame::Data(Bytes::from_static(b"Wrong")))]);
        assert!(matches!(
            protocol
                .patch(
                    checked_headers.clone(),
                    &id,
                    RequestBody::from_stream(Box::pin(wrong))
                )
                .await,
            Err(Error::ChecksumMismatch { .. })
        ));
        assert_eq!(storage.size(&handle).await.unwrap(), Some(offset));
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);

        let frames = stream::iter([Ok(BodyFrame::Data(Bytes::from_static(b"Hello")))]);
        protocol
            .patch(
                checked_headers,
                &id,
                RequestBody::from_stream(Box::pin(frames)),
            )
            .await
            .unwrap();
        assert_eq!(
            protocol
                .head(&id)
                .await
                .unwrap()
                .headers
                .get("upload-offset")
                .unwrap(),
            (offset + 5).to_string().as_str()
        );
        assert_eq!(
            tokio::fs::read(directory.path().join(handle.key()))
                .await
                .unwrap(),
            [prefix, b"Hello"].concat()
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }
}
