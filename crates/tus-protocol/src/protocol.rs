//! Framework-neutral TUS protocol implementation.
//!
//! This module contains the TUS protocol logic without any dependency on a
//! specific HTTP framework. Adapters construct [`Protocol`] with their
//! storage, state, locking, hooks, and configuration, then call the method
//! matching the incoming HTTP request.
//!
//! # Shape
//!
//! Handler methods take already-parsed request inputs:
//!
//! - [`Headers`]: a typed view over TUS-specific request headers
//! - A validated [`UploadId`] path parameter
//! - A [`RequestBody`] for PATCH and POST request bodies, usually created from bytes,
//!   a body-frame stream, or a [`ChunkStream`](crate::ChunkStream)
//!
//! They return `Result<Response, Error>`; adapters convert the
//! response into their framework's response type.
//!
//! Upload lifecycle decisions live behind this facade. Protocol handlers map
//! parsed HTTP inputs into lifecycle requests, then adapt lifecycle results into
//! storage, state-store, hook, and response operations.
//!
//! Upload ID validation is exposed through [`UploadId`] parsing, not through
//! lower-level validation helpers.
//!
//! # Example
//!
//! Framework adapters typically parse the raw HTTP headers, validate path
//! parameters as [`UploadId`], then call the matching [`Protocol`] method.
//!
//! ```rust
//! # fn main() -> tus_protocol::Result<()> {
//! use http::{HeaderMap, HeaderValue};
//! use tus_protocol::{Headers, UploadId};
//!
//! let mut raw_headers = HeaderMap::new();
//! raw_headers.insert("tus-resumable", HeaderValue::from_static("1.0.0"));
//!
//! let headers = Headers::from_headers(&raw_headers)?;
//! let upload_id: UploadId = "01H8XGJWBWBAQ4SHN3JPHQM6JZ".parse()?;
//!
//! # let _ = (headers, upload_id);
//! # Ok(())
//! # }
//! ```

pub(crate) mod body;
mod delete;
mod download;
mod head;
pub(crate) mod headers;
mod hook_context;
mod options;
mod patch;
mod post;
mod response;
mod upload_id;

pub use body::{BodyFrame, BodyStream, RequestBody};
pub use download::{DownloadRequest, DownloadResponse};
pub use headers::{Headers, UploadChecksum};
pub use response::{Response, TUS_SUCCESS_RESPONSE_HEADERS};
pub use upload_id::UploadId;

#[cfg(feature = "fuzzing")]
pub use headers::{
    fuzz_parse_upload_checksum, fuzz_parse_upload_concat, fuzz_parse_upload_metadata,
};

use std::sync::Arc;

use crate::config::Config;
use crate::hooks::HookExecutor;
use crate::locking::Locker;
use crate::state::StateStore;
use crate::storage::{Storage, StorageReader};

/// Framework-neutral TUS protocol facade.
///
/// This type bundles the long-lived protocol dependencies so each handler
/// method only takes request-specific inputs.
pub struct Protocol<'a, S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    config: &'a Config,
    storage: &'a S,
    state_store: &'a I,
    locker: &'a L,
    hooks: &'a H,
}

impl<'a, S, I, L, H> Protocol<'a, S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    /// Creates a protocol facade over the provided dependencies.
    pub fn new(
        config: &'a Config,
        storage: &'a S,
        state_store: &'a I,
        locker: &'a L,
        hooks: &'a H,
    ) -> Self {
        Self {
            config,
            storage,
            state_store,
            locker,
            hooks,
        }
    }
}

impl<S, I, L, H> std::fmt::Debug for Protocol<'_, S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Protocol")
            .field("storage", &self.storage.name())
            .field("state_store", &self.state_store.name())
            .field("locker", &self.locker.name())
            .finish_non_exhaustive()
    }
}

/// Owned, cloneable handle for framework adapters and services.
///
/// [`Protocol`] is a lightweight borrowed facade. Frameworks usually need an
/// owned, cheap-to-clone state value; this type owns shared handles to the
/// protocol dependencies and exposes the same protocol methods directly.
pub struct ProtocolHandle<S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    config: Arc<Config>,
    storage: Arc<S>,
    state_store: Arc<I>,
    locker: Arc<L>,
    hooks: Arc<H>,
}

impl<S, I, L, H> std::fmt::Debug for ProtocolHandle<S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolHandle")
            .field("storage", &self.storage.name())
            .field("state_store", &self.state_store.name())
            .field("locker", &self.locker.name())
            .finish_non_exhaustive()
    }
}

impl<S, I, L, H> ProtocolHandle<S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    /// Creates a new protocol handle from owned dependencies.
    ///
    /// Takes the backends by value, so they must be `Sized`. To build a handle
    /// over type-erased `Arc<dyn Storage>` (etc.) backends, use
    /// [`ProtocolHandle::from_arcs`], which accepts `?Sized` backends.
    pub fn new(config: Config, storage: S, state_store: I, locker: L, hooks: H) -> Self
    where
        S: Sized,
        I: Sized,
        L: Sized,
        H: Sized,
    {
        Self {
            config: Arc::new(config),
            storage: Arc::new(storage),
            state_store: Arc::new(state_store),
            locker: Arc::new(locker),
            hooks: Arc::new(hooks),
        }
    }

    /// Creates a new protocol handle from Arc-wrapped dependencies.
    pub fn from_arcs(
        config: Arc<Config>,
        storage: Arc<S>,
        state_store: Arc<I>,
        locker: Arc<L>,
        hooks: Arc<H>,
    ) -> Self {
        Self {
            config,
            storage,
            state_store,
            locker,
            hooks,
        }
    }

    /// Returns the configuration.
    pub fn config(&self) -> &Config {
        self.config.as_ref()
    }

    /// Returns the shared configuration handle.
    pub fn config_arc(&self) -> Arc<Config> {
        self.config.clone()
    }

    /// Returns the shared storage handle.
    pub fn storage_arc(&self) -> Arc<S> {
        self.storage.clone()
    }

    /// Returns the shared state-store handle.
    pub fn state_store_arc(&self) -> Arc<I> {
        self.state_store.clone()
    }

    /// Returns the shared locker handle.
    pub fn locker_arc(&self) -> Arc<L> {
        self.locker.clone()
    }

    /// Returns the shared hook-executor handle.
    pub fn hooks_arc(&self) -> Arc<H> {
        self.hooks.clone()
    }

    /// Returns a borrowed protocol facade over the stored components.
    pub fn protocol(&self) -> Protocol<'_, S, I, L, H> {
        Protocol::new(
            self.config.as_ref(),
            self.storage.as_ref(),
            self.state_store.as_ref(),
            self.locker.as_ref(),
            self.hooks.as_ref(),
        )
    }

    /// Builds the OPTIONS response advertising server capabilities.
    pub fn options(&self) -> Response {
        self.protocol().options()
    }

    /// Creates a new upload.
    pub async fn post(
        &self,
        headers: Headers,
        body: RequestBody,
    ) -> Result<Response, crate::Error> {
        self.protocol().post(headers, body).await
    }

    /// Returns the status of an upload.
    pub async fn head(&self, upload_id: &UploadId) -> Result<Response, crate::Error> {
        self.protocol().head(upload_id).await
    }

    /// Downloads a completed upload.
    ///
    /// This is a non-standard convenience endpoint, not part of the core tus
    /// protocol.
    pub async fn download(
        &self,
        request: DownloadRequest<'_>,
    ) -> Result<DownloadResponse, crate::Error>
    where
        S: StorageReader,
    {
        self.protocol().download(request).await
    }

    /// Appends data to an upload.
    pub async fn patch(
        &self,
        headers: Headers,
        upload_id: &UploadId,
        body: RequestBody,
    ) -> Result<Response, crate::Error> {
        self.protocol().patch(headers, upload_id, body).await
    }

    /// Terminates an upload.
    pub async fn delete(
        &self,
        headers: Headers,
        upload_id: &UploadId,
    ) -> Result<Response, crate::Error> {
        self.protocol().delete(headers, upload_id).await
    }
}

impl<S, I, L, H> Clone for ProtocolHandle<S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            storage: self.storage.clone(),
            state_store: self.state_store.clone(),
            locker: self.locker.clone(),
            hooks: self.hooks.clone(),
        }
    }
}

#[cfg(all(
    test,
    feature = "storage-memory",
    feature = "state-memory",
    feature = "lock-memory",
    not(target_arch = "wasm32")
))]
mod handle_tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;
    use http::StatusCode;

    use crate::hooks::NoopHookExecutor;
    use crate::locking::memory::MemoryLocker;
    use crate::state::{StateStore, UploadState, memory::MemoryStateStore};
    use crate::storage::{AppendRequest, ChunkStream, Storage, memory::MemoryStorage};
    use crate::{Config, DownloadRequest, ProtocolHandle, UploadId};

    #[test]
    fn handle_clone_shares_backend_arcs_without_cloning_backends() {
        let storage = Arc::new(MemoryStorage::new());
        let state_store = Arc::new(MemoryStateStore::new());
        let locker = Arc::new(MemoryLocker::new());
        let hooks = Arc::new(NoopHookExecutor::new());

        let handle = ProtocolHandle::from_arcs(
            Arc::new(Config::default()),
            storage.clone(),
            state_store.clone(),
            locker.clone(),
            hooks.clone(),
        );
        let clone = handle.clone();

        assert!(Arc::ptr_eq(&handle.storage_arc(), &clone.storage_arc()));
        assert!(Arc::ptr_eq(&storage, &clone.storage_arc()));
        assert_eq!(Arc::strong_count(&storage), 3);
    }

    #[tokio::test]
    async fn handle_exposes_protocol_methods_directly() {
        let storage = MemoryStorage::new();
        let state_store = MemoryStateStore::new();
        let upload = UploadState::new("test-id").with_length(42);
        state_store.set(&upload, true).await.unwrap();

        let handle = ProtocolHandle::new(
            Config::default(),
            storage,
            state_store,
            MemoryLocker::new(),
            NoopHookExecutor::new(),
        );
        let upload_id: UploadId = "test-id".parse().unwrap();

        let response = handle.head(&upload_id).await.unwrap();

        assert_eq!(response.headers.get("upload-length").unwrap(), "42");
    }

    #[tokio::test]
    async fn handle_download_delegates_to_protocol() {
        let storage = MemoryStorage::new();
        let state_store = MemoryStateStore::new();
        let mut upload = UploadState::new("test-id").with_length(5);
        let handle = storage.create(upload.id()).await.unwrap();
        upload.set_storage_handle(handle);
        let handle = storage
            .append(AppendRequest::new(
                upload.require_storage_handle().unwrap(),
                upload.offset(),
                ChunkStream::from_bytes(Bytes::from_static(b"hello")),
                true,
            ))
            .await
            .unwrap();
        upload.set_storage_handle(handle);
        upload.set_offset(5);
        state_store.set(&upload, true).await.unwrap();

        let handle = ProtocolHandle::new(
            Config::default(),
            storage,
            state_store,
            MemoryLocker::new(),
            NoopHookExecutor::new(),
        );
        let upload_id: UploadId = "test-id".parse().unwrap();

        let mut response = handle
            .download(DownloadRequest::new(&upload_id))
            .await
            .unwrap();

        let mut body = Vec::new();
        while let Some(chunk) = response.body.next().await {
            body.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.headers.get("content-length").unwrap(), "5");
        assert_eq!(body, b"hello");
    }
}
