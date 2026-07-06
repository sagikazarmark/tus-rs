//! Core DELETE handler (TUS Termination extension).

use http::StatusCode;

use crate::config::Extension;
use crate::error::Error;
use crate::hooks::HookExecutor;
use crate::lifecycle::UploadTerminator;
use crate::locking::Locker;
use crate::state::StateStore;
use crate::storage::Storage;

use super::hook_context::{HookContextBuilder, HookRequestFacts};
use super::{Headers, Protocol, Response, UploadId};

/// Terminates an upload: removes the state and the stored bytes.
///
/// Errors:
/// - [`Error::ExtensionNotSupported`] if `Termination` isn't enabled.
/// - [`Error::NotFound`] if the upload doesn't exist.
/// - [`Error::HookRejected`] if a pre-terminate hook rejects.
///
/// Storage deletion failures are returned and the upload state is preserved,
/// so a retried DELETE can still reach the storage object. Backends treat a
/// missing object as success, keeping the retry idempotent.
impl<'a, S, St, L, H> Protocol<'a, S, St, L, H>
where
    S: Storage + ?Sized,
    St: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    /// Terminates an upload: removes the state and the stored bytes.
    ///
    /// Storage deletion failures are returned and the state is left intact, so
    /// a retried DELETE can still reach the storage object rather than leaving
    /// orphaned bytes behind.
    ///
    /// # Errors
    ///
    /// Returns an error if the Termination extension is disabled, the upload is
    /// missing or protocol-expired, lock acquisition or state deletion fails,
    /// or a pre-terminate hook rejects the request.
    pub async fn delete(&self, headers: Headers, upload_id: &UploadId) -> Result<Response, Error> {
        let hook_contexts =
            HookContextBuilder::new(self.config, HookRequestFacts::delete(&headers, upload_id));
        let upload_id = upload_id.as_str();
        if !self.config.has_extension(Extension::Termination) {
            return Err(Error::ExtensionNotSupported("termination".to_string()));
        }

        // Cheap unlocked existence pre-check (DoS guard, not authoritative):
        // requests for unknown IDs must not exercise the locker, which may
        // allocate per-ID resources. The authoritative state load below still
        // happens under the lock.
        if self.state_store.get(upload_id).await?.is_none() {
            return Err(Error::NotFound(upload_id.to_string()));
        }

        let _guard = self
            .locker
            .lock(upload_id, self.config.lock_timeout())
            .await?;

        let state = self
            .state_store
            .get(upload_id)
            .await?
            .ok_or_else(|| Error::NotFound(upload_id.to_string()))?;

        // Consistent with HEAD/PATCH/download: protocol-expired uploads are
        // gone for clients (tus Expiration extension); reclamation, not
        // client-driven DELETE, cleans them up.
        crate::lifecycle::ensure_active(&state)?;

        let terminated = UploadTerminator::new(
            self.storage,
            self.state_store,
            self.hooks,
            hook_contexts.request_info(),
        )
        .terminate(state)
        .await?;

        let mut response = Response::new(StatusCode::NO_CONTENT);
        for (name, value) in terminated.response_headers {
            response = response.with_header(name, value);
        }
        Ok(response)
    }
}

#[cfg(all(
    test,
    feature = "storage-memory",
    feature = "state-memory",
    not(target_arch = "wasm32")
))]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::hooks::{HookChain, HookEvent, NoopHookExecutor, PreHookResult};
    use crate::locking::NoopLocker;
    use crate::state::{UploadState, WriteMode, memory::MemoryStateStore};
    use crate::storage::{StorageHandle, memory::MemoryStorage};
    use std::sync::{Arc, Mutex};

    fn config() -> Config {
        Config::default().with_extension(Extension::Termination)
    }

    async fn setup(state: UploadState) -> (MemoryStorage, MemoryStateStore) {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        store.set(&state, WriteMode::CreateNew).await.unwrap();
        (storage, store)
    }

    async fn call(
        config: &Config,
        storage: &MemoryStorage,
        state_store: &MemoryStateStore,
        headers: Headers,
        upload_id: &str,
    ) -> Result<Response, Error> {
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = upload_id.parse().unwrap();
        Protocol::new(config, storage, state_store, &locker, &hooks)
            .delete(headers, &upload_id)
            .await
    }

    /// Locker that fails every call; used to prove handlers do not exercise
    /// the locker for unknown upload IDs.
    struct RejectingLocker;

    #[async_trait::async_trait]
    impl crate::locking::Locker for RejectingLocker {
        fn name(&self) -> &'static str {
            "rejecting"
        }

        async fn lock(
            &self,
            _upload_id: &str,
            _timeout: std::time::Duration,
        ) -> Result<crate::locking::LockGuard, Error> {
            Err(Error::Internal("locker must not be exercised".to_string()))
        }

        async fn try_lock(
            &self,
            _upload_id: &str,
        ) -> Result<Option<crate::locking::LockGuard>, Error> {
            Err(Error::Internal("locker must not be exercised".to_string()))
        }
    }

    #[tokio::test]
    async fn delete_of_unknown_upload_does_not_touch_locker() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = "missing".parse().unwrap();

        let err = Protocol::new(&config(), &storage, &store, &RejectingLocker, &hooks)
            .delete(Headers::default(), &upload_id)
            .await
            .unwrap_err();

        assert!(matches!(err, Error::NotFound(_)));
    }

    /// Storage whose `delete` always fails; everything else delegates to
    /// [`MemoryStorage`].
    struct DeleteFailingStorage(MemoryStorage);

    #[async_trait::async_trait]
    impl crate::storage::Storage for DeleteFailingStorage {
        fn name(&self) -> &'static str {
            "delete-failing"
        }

        async fn create(&self, upload_id: &str) -> Result<StorageHandle, Error> {
            self.0.create(upload_id).await
        }

        async fn append(
            &self,
            request: crate::storage::AppendRequest,
        ) -> Result<StorageHandle, Error> {
            self.0.append(request).await
        }

        async fn concat(
            &self,
            request: crate::storage::ConcatRequest,
        ) -> Result<StorageHandle, Error> {
            self.0.concat(request).await
        }

        async fn delete(&self, _handle: &StorageHandle) -> Result<(), Error> {
            Err(Error::storage(std::io::Error::other("disk on fire")))
        }

        async fn size(&self, handle: &StorageHandle) -> Result<Option<u64>, Error> {
            self.0.size(handle).await
        }
    }

    #[tokio::test]
    async fn storage_delete_failure_propagates_and_keeps_state() {
        let mut state = UploadState::new("test-id").with_length(1000);
        state.set_storage_handle(StorageHandle::new("uploads/test-id"));
        let storage = DeleteFailingStorage(MemoryStorage::new());
        let store = MemoryStateStore::new();
        store.set(&state, WriteMode::CreateNew).await.unwrap();
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = "test-id".parse().unwrap();

        let err = Protocol::new(&config(), &storage, &store, &locker, &hooks)
            .delete(Headers::default(), &upload_id)
            .await
            .unwrap_err();

        // The failure surfaces and state survives, so a retried DELETE can
        // still reach the storage object.
        assert!(matches!(err, Error::Storage(_)));
        assert!(store.get("test-id").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn basic_delete() {
        let mut state = UploadState::new("test-id").with_length(1000);
        state.set_storage_handle(StorageHandle::new("uploads/test-id"));
        let (storage, store) = setup(state).await;

        let response = call(&config(), &storage, &store, Headers::default(), "test-id")
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::NO_CONTENT);
        assert!(store.get("test-id").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn not_found() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let err = call(&config(), &storage, &store, Headers::default(), "missing")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn extension_disabled() {
        let config = Config::default().without_extension(Extension::Termination);
        let (storage, store) = setup(UploadState::new("test-id")).await;
        let err = call(&config, &storage, &store, Headers::default(), "test-id")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ExtensionNotSupported(_)));
    }

    #[tokio::test]
    async fn expired_upload_is_rejected() {
        use chrono::{Duration, Utc};

        let mut state = UploadState::new("test-id")
            .with_length(1000)
            .with_expiration(Utc::now() - Duration::hours(1));
        state.set_storage_handle(StorageHandle::new("uploads/test-id"));
        let (storage, store) = setup(state).await;

        let err = call(&config(), &storage, &store, Headers::default(), "test-id")
            .await
            .unwrap_err();

        // Expired uploads are reclaimed by the server, not client DELETE;
        // clients observe the same 410 as HEAD/PATCH/download.
        assert!(matches!(err, Error::Expired(id) if id == "test-id"));
        assert!(store.get("test-id").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn missing_storage_key_is_not_fatal() {
        // Upload without a storage_key: state is deleted but storage delete is skipped.
        let (storage, store) = setup(UploadState::new("test-id")).await;
        let response = call(&config(), &storage, &store, Headers::default(), "test-id")
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn termination_runs_hooks_around_state_delete() {
        let mut state = UploadState::new("test-id").with_length(1000);
        state.set_storage_handle(StorageHandle::new("uploads/test-id"));
        let (storage, store) = setup(state).await;
        let locker = NoopLocker::new();
        let events = Arc::new(Mutex::new(Vec::new()));
        let hooks = HookChain::new()
            .on_pre_terminate({
                let events = Arc::clone(&events);
                move |_| {
                    let events = Arc::clone(&events);
                    async move {
                        events.lock().unwrap().push(HookEvent::PreTerminate);
                        Ok(PreHookResult::proceed().with_header("x-terminate", "accepted"))
                    }
                }
            })
            .on_post_terminate({
                let events = Arc::clone(&events);
                move |_| {
                    let events = Arc::clone(&events);
                    async move {
                        events.lock().unwrap().push(HookEvent::PostTerminate);
                        Ok(())
                    }
                }
            });
        let upload_id: UploadId = "test-id".parse().unwrap();

        let response = Protocol::new(&config(), &storage, &store, &locker, &hooks)
            .delete(Headers::default(), &upload_id)
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::NO_CONTENT);
        assert_eq!(response.headers.get("x-terminate").unwrap(), "accepted");
        assert!(store.get("test-id").await.unwrap().is_none());
        assert_eq!(
            *events.lock().unwrap(),
            vec![HookEvent::PreTerminate, HookEvent::PostTerminate]
        );
    }
}
