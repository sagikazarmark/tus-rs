//! Core OPTIONS handler.
//!
//! Returns the server's TUS capabilities: supported protocol versions,
//! enabled extensions, maximum upload size, and (if the Checksum extension
//! is enabled) the list of supported checksum algorithms.

use http::StatusCode;

use crate::config::{Extension, TUS_RESUMABLE};
use crate::hooks::HookExecutor;
use crate::locking::Locker;
use crate::state::StateStore;
use crate::storage::Storage;

use super::{Protocol, Response};

/// Builds the OPTIONS response advertising server capabilities.
///
/// Per the TUS spec, this request does not require `Tus-Resumable`.
impl<'a, S, I, L, H> Protocol<'a, S, I, L, H>
where
    S: Storage + ?Sized,
    I: StateStore + ?Sized,
    L: Locker + ?Sized,
    H: HookExecutor + ?Sized,
{
    /// Builds the OPTIONS response advertising server capabilities.
    ///
    /// Per the TUS spec, this request does not require `Tus-Resumable`.
    pub fn options(&self) -> Response {
        let mut response = Response::new(StatusCode::OK).with_header("tus-version", TUS_RESUMABLE);

        let extensions = self.config.extensions_string();
        if !extensions.is_empty() {
            response = response.with_header("tus-extension", extensions);
        }

        if let Some(max_size) = self.config.max_size() {
            response = response.with_header("tus-max-size", max_size.to_string());
        }

        if self.config.has_extension(Extension::Checksum) {
            let algorithms = self.config.checksum_algorithms_string();
            if !algorithms.is_empty() {
                response = response.with_header("tus-checksum-algorithm", algorithms);
            }
        }

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "checksum")]
    use crate::config::ChecksumAlgorithm;
    use crate::config::Config;
    use crate::error::Result;
    use crate::hooks::NoopHookExecutor;
    use crate::locking::NoopLocker;
    use crate::state::{StateStore, UploadState};
    use crate::storage::{AppendRequest, ConcatRequest, Storage, StorageHandle};
    use chrono::{DateTime, Utc};

    struct TestStorage;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Storage for TestStorage {
        fn name(&self) -> &'static str {
            "test"
        }

        async fn create(&self, _upload_id: &str) -> Result<StorageHandle> {
            unreachable!()
        }

        async fn append(&self, _request: AppendRequest) -> Result<StorageHandle> {
            unreachable!()
        }

        async fn concat(&self, _request: ConcatRequest) -> Result<StorageHandle> {
            unreachable!()
        }

        async fn delete(&self, _handle: &StorageHandle) -> Result<()> {
            unreachable!()
        }

        async fn size(&self, _handle: &StorageHandle) -> Result<Option<u64>> {
            unreachable!()
        }
    }

    struct TestStateStore;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl StateStore for TestStateStore {
        fn name(&self) -> &'static str {
            "test"
        }

        async fn set(&self, _state: &UploadState, _create: bool) -> Result<()> {
            unreachable!()
        }

        async fn get(&self, _id: &str) -> Result<Option<UploadState>> {
            unreachable!()
        }

        async fn delete(&self, _id: &str) -> Result<()> {
            unreachable!()
        }

        async fn list_expired(&self, _before: DateTime<Utc>) -> Result<Vec<String>> {
            unreachable!()
        }
    }

    fn options(config: &Config) -> Response {
        let storage = TestStorage;
        let state_store = TestStateStore;
        let locker = NoopLocker::new();
        let hooks = NoopHookExecutor::new();
        Protocol::new(config, &storage, &state_store, &locker, &hooks).options()
    }

    #[test]
    fn basic_response() {
        let response = options(&Config::default());
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.headers.get("tus-resumable").unwrap(),
            TUS_RESUMABLE
        );
        assert_eq!(response.headers.get("tus-version").unwrap(), TUS_RESUMABLE);
    }

    #[test]
    fn includes_max_size() {
        let config = Config::default().with_max_size(1024 * 1024 * 100);
        let response = options(&config);
        assert_eq!(response.headers.get("tus-max-size").unwrap(), "104857600");
    }

    #[test]
    fn includes_extensions() {
        let config = Config::default()
            .with_extension(Extension::Creation)
            .with_extension(Extension::Termination)
            .with_extension(Extension::Expiration);
        let response = options(&config);
        let ext = response
            .headers
            .get("tus-extension")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ext.contains("creation"));
        assert!(ext.contains("termination"));
        assert!(ext.contains("expiration"));
    }

    #[cfg(feature = "checksum")]
    #[test]
    fn includes_checksum_algorithms() {
        let config = Config::default()
            .with_checksum(ChecksumAlgorithm::Sha256)
            .with_checksum(ChecksumAlgorithm::Md5);
        let response = options(&config);
        let algs = response
            .headers
            .get("tus-checksum-algorithm")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(algs.contains("sha256"));
        assert!(algs.contains("md5"));
    }

    #[cfg(feature = "checksum")]
    #[test]
    fn checksum_support_always_advertises_sha1() {
        let config = Config::default().with_checksum(ChecksumAlgorithm::Crc32);
        let response = options(&config);
        let algs = response
            .headers
            .get("tus-checksum-algorithm")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(algs.contains("sha1"));
        assert!(algs.contains("crc32"));
    }

    #[cfg(feature = "checksum")]
    #[test]
    fn checksum_extension_adds_default_algorithms() {
        let config = Config::default().with_extension(Extension::Checksum);
        let response = options(&config);
        let algs = response
            .headers
            .get("tus-checksum-algorithm")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(algs.contains("sha1"));
    }

    #[cfg(not(feature = "checksum"))]
    #[test]
    fn checksum_extension_is_not_advertised_without_feature() {
        // Without the `checksum` feature the extension cannot be enabled at
        // all (Config::with_extension panics), so a default configuration
        // must not advertise it.
        let config = Config::default();
        let response = options(&config);
        assert!(response.headers.get("tus-checksum-algorithm").is_none());
        let extensions = response
            .headers
            .get("tus-extension")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(!extensions.contains("checksum"));
    }
}
