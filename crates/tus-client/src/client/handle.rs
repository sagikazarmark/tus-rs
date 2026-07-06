use url::Url;

use super::{Client, NewUpload, UploadInfo};
use super::{UploadProgress, UploadSource};
use crate::error::Result;
use crate::helpers::resolve_upload_url;
use crate::transport::Transport;

/// A remote upload resource.
#[derive(Clone)]
pub struct Upload<T> {
    client: Client<T>,
    url: Url,
}

// Manual impl so `Upload<T>: Debug` does not require `T: Debug`, matching
// `Client<T>`'s manual impl.
impl<T> std::fmt::Debug for Upload<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Upload")
            .field("client", &self.client)
            .field("url", &self.url)
            .finish()
    }
}

impl<T> Upload<T>
where
    T: Transport,
{
    /// Returns the remote upload URL represented by this resource.
    #[must_use]
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Fetches the current server-side upload state.
    pub async fn info(&self) -> Result<UploadInfo> {
        self.client.upload_info_at(&self.url).await
    }

    /// Terminates this upload resource.
    pub async fn terminate(&self) -> Result<()> {
        self.client.terminate_upload_at(&self.url).await
    }

    /// Uploads one chunk at the given offset to this upload resource and
    /// returns the new server offset.
    pub async fn upload_chunk(&self, offset: u64, chunk: Vec<u8>) -> Result<u64> {
        self.client.upload_chunk_at(&self.url, chunk, offset).await
    }

    /// Uploads a source to this upload resource from the server's current offset.
    pub async fn upload<S>(&self, source: S) -> Result<UploadInfo>
    where
        S: UploadSource,
    {
        self.client.resume_at(&self.url, source).await
    }

    /// Uploads a source to this upload resource and reports progress updates.
    pub async fn upload_with_progress<S, P>(
        &self,
        source: S,
        progress: &mut P,
    ) -> Result<UploadInfo>
    where
        S: UploadSource,
        P: UploadProgress,
    {
        self.client
            .resume_at_with_progress(&self.url, source, progress)
            .await
    }
}

impl<T> Client<T>
where
    T: Transport,
{
    /// Creates a resource reference for an existing remote upload URL reference.
    ///
    /// Absolute URLs are used as-is. Absolute paths resolve against the endpoint
    /// origin, and relative paths resolve under the endpoint path.
    pub fn upload_at(&self, upload_url: impl AsRef<str>) -> Result<Upload<T>> {
        Ok(Upload {
            client: self.clone(),
            url: resolve_upload_url(&self.endpoint, upload_url.as_ref())?,
        })
    }

    /// Creates a new remote upload resource.
    ///
    /// Returns the resource reference alongside the [`UploadInfo`] observed
    /// at creation (URL, initial offset — nonzero for creation-with-upload —
    /// declared length, and metadata), so callers do not need an extra HEAD
    /// roundtrip to learn the creation state.
    pub async fn create_upload(&self, upload: NewUpload) -> Result<(Upload<T>, UploadInfo)> {
        let info = self.create_upload_info(upload).await?;
        let handle = self.upload_at(info.url.as_str())?;

        Ok((handle, info))
    }
}

#[cfg(test)]
mod tests {
    use crate::TransportBody;
    use crate::client::test_support::*;
    use crate::client::{Client, NewUpload};
    use http::Method;

    #[cfg(not(target_arch = "wasm32"))]
    use tokio::test as async_test;
    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as async_test;

    fn upload_url() -> url::Url {
        url::Url::parse("http://example.test/files/upload-1").unwrap()
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn upload_exposes_url() {
        let client = Client::with_transport(endpoint_url(), MockTransport::default());
        let url = upload_url();

        let handle: crate::Upload<_> = client.upload_at(url.clone()).unwrap();

        assert_eq!(handle.url(), &url);
    }

    /// `Upload<T>` must be debuggable even when the transport is not
    /// (`MockTransport` does not implement `Debug`).
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn upload_debug_does_not_require_transport_debug() {
        let client = Client::with_transport(endpoint_url(), MockTransport::default());
        let handle = client.upload_at(upload_url()).unwrap();

        let debug = format!("{handle:?}");

        assert!(debug.starts_with("Upload"), "{debug}");
        assert!(debug.contains("/files/upload-1"), "{debug}");
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn upload_accepts_absolute_path_and_relative_path() {
        let client = Client::with_transport(endpoint_url(), MockTransport::default());

        assert_eq!(
            client.upload_at("/files/upload-1").unwrap().url().as_str(),
            "http://example.test/files/upload-1"
        );
        assert_eq!(
            client.upload_at("upload-1").unwrap().url().as_str(),
            "http://example.test/files/upload-1"
        );
    }

    #[async_test]
    async fn create_upload_returns_resolved_resource() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                header_map(&[("Location", "upload-1")]),
                Vec::new(),
            )));
        let client = Client::with_transport(endpoint_url(), transport.clone());

        let (handle, info) = client
            .create_upload(NewUpload::new(5, tus_protocol::UploadMetadata::new()))
            .await
            .unwrap();

        assert_eq!(handle.url().as_str(), "http://example.test/files/upload-1");
        assert_eq!(info.url, *handle.url());
        assert_eq!(info.offset, 0);
        assert_eq!(info.length, Some(5));
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.first().unwrap().method(), Method::POST);
    }

    #[async_test]
    async fn upload_info_uses_resource_url() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(mock_head_response(3, 5)));
        let client = Client::with_transport(endpoint_url(), transport.clone());

        let info = client
            .upload_at(upload_url())
            .unwrap()
            .info()
            .await
            .unwrap();

        assert_eq!(info.url.as_str(), "http://example.test/files/upload-1");
        assert_eq!(info.offset, 3);
        assert_eq!(info.length, Some(5));
        let requests = transport.requests.lock().unwrap();
        let request = requests.first().unwrap();
        assert_eq!(request.method(), Method::HEAD);
        assert_eq!(
            request.uri().to_string(),
            "http://example.test/files/upload-1"
        );
        assert_eq!(
            request
                .headers()
                .get("tus-resumable")
                .and_then(|value| value.to_str().ok()),
            Some(tus_protocol::TUS_RESUMABLE)
        );
        assert!(matches!(request.body(), TransportBody::Empty));
    }

    #[async_test]
    async fn upload_terminate_uses_resource_url() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                204,
                http::HeaderMap::new(),
                Vec::new(),
            )));
        let client = Client::with_transport(endpoint_url(), transport.clone());

        client
            .upload_at(upload_url())
            .unwrap()
            .terminate()
            .await
            .unwrap();

        let requests = transport.requests.lock().unwrap();
        let request = requests.first().unwrap();
        assert_eq!(request.method(), Method::DELETE);
        assert_eq!(
            request.uri().to_string(),
            "http://example.test/files/upload-1"
        );
        assert_eq!(
            request
                .headers()
                .get("tus-resumable")
                .and_then(|value| value.to_str().ok()),
            Some(tus_protocol::TUS_RESUMABLE)
        );
        assert!(matches!(request.body(), TransportBody::Empty));
    }

    #[async_test]
    async fn upload_upload_chunk_uses_resource_url() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(mock_patch_response(5)));
        let client = Client::with_transport(endpoint_url(), transport.clone());

        let offset = client
            .upload_at(upload_url())
            .unwrap()
            .upload_chunk(0, b"hello".to_vec())
            .await
            .unwrap();

        assert_eq!(offset, 5);
        let requests = transport.requests.lock().unwrap();
        let request = requests.first().unwrap();
        assert_eq!(request.method(), Method::PATCH);
        assert_eq!(
            request.uri().to_string(),
            "http://example.test/files/upload-1"
        );
        assert_eq!(
            request
                .headers()
                .get("upload-offset")
                .and_then(|value| value.to_str().ok()),
            Some("0")
        );
        assert_eq!(
            request
                .headers()
                .get("tus-resumable")
                .and_then(|value| value.to_str().ok()),
            Some(tus_protocol::TUS_RESUMABLE)
        );
        assert_eq!(
            request
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/offset+octet-stream")
        );
        assert_eq!(body_bytes(request.body()).as_slice(), b"hello");
    }

    #[async_test]
    async fn upload_upload_resumes_against_resource_url() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(mock_patch_response(4)));
        }
        let client =
            Client::with_transport(endpoint_url(), transport.clone()).with_max_chunk_size(4);

        let info = client
            .upload_at(upload_url())
            .unwrap()
            .upload(b"data".to_vec())
            .await
            .unwrap();

        assert_eq!(info.url.as_str(), "http://example.test/files/upload-1");
        assert_eq!(info.offset, 4);
        let requests = transport.requests.lock().unwrap();
        let methods: Vec<_> = requests
            .iter()
            .map(|request| request.method().clone())
            .collect();
        // A genuine resume probes with the initial HEAD, but the PATCH that
        // drains the source needs no confirming HEAD afterward.
        assert_eq!(methods, vec![Method::HEAD, Method::PATCH]);
        assert!(
            requests
                .iter()
                .all(|request| *request.uri() == "http://example.test/files/upload-1")
        );
    }

    #[async_test]
    async fn upload_upload_with_progress_reports_offsets() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(mock_patch_response(2)));
            responses.push_back(Ok(mock_patch_response(4)));
            responses.push_back(Ok(mock_head_response(4, 4)));
        }
        let client = Client::with_transport(endpoint_url(), transport).with_max_chunk_size(2);
        let mut progress = Vec::new();

        let info = client
            .upload_at(upload_url())
            .unwrap()
            .upload_with_progress(b"data".to_vec(), &mut |uploaded, total| {
                progress.push((uploaded, total));
            })
            .await
            .unwrap();

        assert_eq!(info.offset, 4);
        assert_eq!(progress, vec![(2, 4), (4, 4)]);
    }
}
