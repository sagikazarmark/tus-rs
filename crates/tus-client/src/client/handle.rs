use url::Url;

use super::{Client, NewUpload, UploadInfo};
use super::{UploadProgress, UploadSource};
use crate::error::Result;
use crate::transport::Transport;

/// A remote upload resource.
#[derive(Clone, Debug)]
pub struct Upload<T> {
    client: Client<T>,
    url: Url,
}

impl<T> Upload<T>
where
    T: Transport,
{
    /// Returns the remote upload URL represented by this resource.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Fetches the current server-side upload state.
    pub async fn info(&self) -> Result<UploadInfo> {
        self.client.upload_info_at(&self.url).await
    }

    /// Terminates this upload resource.
    pub async fn delete(&self) -> Result<()> {
        self.client.delete_upload_at(&self.url).await
    }

    /// Uploads one chunk to this upload resource and returns the new server offset.
    pub async fn upload_chunk(&self, chunk: Vec<u8>, offset: u64) -> Result<u64> {
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
    /// Creates a resource reference for an existing remote upload URL.
    pub fn upload(&self, upload_url: Url) -> Upload<T> {
        Upload {
            client: (*self).clone(),
            url: upload_url,
        }
    }

    /// Creates a new remote upload resource and returns a resource reference for it.
    pub async fn create_upload(&self, upload: NewUpload) -> Result<Upload<T>> {
        let upload = self.create_upload_info(upload).await?;

        Ok(self.upload(upload.url))
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

        let handle: crate::Upload<_> = client.upload(url.clone());

        assert_eq!(handle.url(), &url);
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

        let handle = client
            .create_upload(NewUpload::new(5, tus_protocol::UploadMetadata::new()))
            .await
            .unwrap();

        assert_eq!(handle.url().as_str(), "http://example.test/upload-1");
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

        let info = client.upload(upload_url()).info().await.unwrap();

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
    async fn upload_delete_uses_resource_url() {
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

        client.upload(upload_url()).delete().await.unwrap();

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
            .upload(upload_url())
            .upload_chunk(b"hello".to_vec(), 0)
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
            responses.push_back(Ok(mock_head_response(4, 4)));
        }
        let client =
            Client::with_transport(endpoint_url(), transport.clone()).with_max_chunk_size(4);

        let info = client
            .upload(upload_url())
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
        assert_eq!(methods, vec![Method::HEAD, Method::PATCH, Method::HEAD]);
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
            .upload(upload_url())
            .upload_with_progress(b"data".to_vec(), &mut |uploaded, total| {
                progress.push((uploaded, total));
            })
            .await
            .unwrap();

        assert_eq!(info.offset, 4);
        assert_eq!(progress, vec![(2, 4), (4, 4)]);
    }
}
