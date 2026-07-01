//! Core DELETE handler (TUS Termination extension).

use http::StatusCode;

use crate::config::Extension;
use crate::error::Error;
use crate::hooks::{HookEvent, HookExecutor, execute_post_best_effort};
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
/// Storage deletion errors are logged and ignored; state deletion always
/// proceeds, because otherwise a failing storage backend would leak
/// undeletable upload IDs.
impl<'a, S, I, L, H> Protocol<'a, S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    /// Terminates an upload: removes the state and the stored bytes.
    ///
    /// Storage deletion errors are logged and ignored; state deletion always
    /// proceeds, because otherwise a failing storage backend would leak
    /// undeletable upload IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if the Termination extension is disabled, the upload is
    /// missing, lock acquisition or state deletion fails, or a pre-terminate
    /// hook rejects the request.
    pub async fn delete(&self, headers: &Headers, upload_id: &UploadId) -> Result<Response, Error> {
        let hook_contexts =
            HookContextBuilder::new(self.config, HookRequestFacts::delete(headers, upload_id));
        let upload_id = upload_id.as_str();
        if !self.config.has_extension(Extension::Termination) {
            return Err(Error::ExtensionNotSupported("termination".to_string()));
        }

        let _guard = self
            .locker
            .lock(upload_id, self.config.lock_timeout_duration())
            .await?;

        let state = self
            .state_store
            .get(upload_id)
            .await?
            .ok_or_else(|| Error::NotFound(upload_id.to_string()))?;

        let pre_ctx = hook_contexts.context(HookEvent::PreTerminate, state.clone());
        let pre_result = self.hooks.execute_pre(&pre_ctx).await?;

        if !pre_result.proceed {
            return Err(Error::HookRejected {
                status_code: pre_result.reject_status.unwrap_or(400),
                message: pre_result.reject_message.unwrap_or_default(),
            });
        }

        if let Some(handle) = state.storage_handle()
            && let Err(e) = self.storage.delete(&handle).await
        {
            tracing::warn!(
                upload_id = %upload_id,
                error = %e,
                "failed to delete upload data from storage"
            );
        }

        self.state_store.delete(upload_id).await?;

        let post_ctx = hook_contexts.context(HookEvent::PostTerminate, state);
        execute_post_best_effort(self.hooks, &post_ctx).await;

        let mut response = Response::new(StatusCode::NO_CONTENT);
        for (name, value) in pre_result.response_headers {
            response = response.with_header_owned(name, value);
        }
        Ok(response)
    }
}

#[cfg(all(
    test,
    feature = "storage-memory",
    feature = "state-memory",
    not(feature = "local-futures")
))]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::hooks::NoopHookExecutor;
    use crate::locking::NoopLocker;
    use crate::state::{UploadState, memory::MemoryStateStore};
    use crate::storage::memory::MemoryStorage;

    fn config() -> Config {
        Config::default().with_extension(Extension::Termination)
    }

    async fn setup(state: UploadState) -> (MemoryStorage, MemoryStateStore) {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        store.set(&state, true).await.unwrap();
        (storage, store)
    }

    async fn call(
        config: &Config,
        storage: &MemoryStorage,
        state_store: &MemoryStateStore,
        headers: &Headers,
        upload_id: &str,
    ) -> Result<Response, Error> {
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        let upload_id: UploadId = upload_id.parse().unwrap();
        Protocol::new(config, storage, state_store, &locker, &hooks)
            .delete(headers, &upload_id)
            .await
    }

    #[tokio::test]
    async fn basic_delete() {
        let mut state = UploadState::new("test-id").with_length(1000);
        state.set_storage_key("uploads/test-id");
        let (storage, store) = setup(state).await;

        let response = call(&config(), &storage, &store, &Headers::default(), "test-id")
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::NO_CONTENT);
        assert!(store.get("test-id").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn not_found() {
        let storage = MemoryStorage::new();
        let store = MemoryStateStore::new();
        let err = call(&config(), &storage, &store, &Headers::default(), "missing")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn extension_disabled() {
        let config = Config::default().without_extension(Extension::Termination);
        let (storage, store) = setup(UploadState::new("test-id")).await;
        let err = call(&config, &storage, &store, &Headers::default(), "test-id")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ExtensionNotSupported(_)));
    }

    #[tokio::test]
    async fn missing_storage_key_is_not_fatal() {
        // Upload without a storage_key: state is deleted but storage delete is skipped.
        let (storage, store) = setup(UploadState::new("test-id")).await;
        let response = call(&config(), &storage, &store, &Headers::default(), "test-id")
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::NO_CONTENT);
    }
}
