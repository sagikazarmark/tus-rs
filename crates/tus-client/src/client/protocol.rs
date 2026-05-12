use http::{Method, header::CONTENT_TYPE};
use tus_protocol::UploadMetadata;
use url::Url;

use super::{Client, insert_request_header};
use crate::error::{Error, Result};
use crate::helpers::{
    decode_metadata, encode_metadata, header_string, header_u64, optional_header_u64,
    parse_csv_header, resolve_upload_location, unexpected_response,
    validate_offset_not_beyond_source,
};
use crate::transport::Transport;

/// The current state of a remote upload.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UploadInfo {
    /// Absolute upload URL.
    pub url: Url,
    /// Current server-side offset.
    pub offset: u64,
    /// Declared upload length, if known.
    pub length: Option<u64>,
    /// Decoded `Upload-Metadata` values.
    pub metadata: UploadMetadata,
}

/// Parameters for creating a new upload resource.
#[derive(Debug)]
pub struct NewUpload {
    pub(crate) metadata: UploadMetadata,
    pub(crate) content: NewUploadContent,
    pub(crate) partial: bool,
}

impl NewUpload {
    /// Creates an empty upload resource with a known length.
    pub fn new(length: u64, metadata: impl Into<UploadMetadata>) -> Self {
        Self {
            metadata: metadata.into(),
            content: NewUploadContent::Length(length),
            partial: false,
        }
    }

    /// Creates an upload resource and sends the initial body in the POST.
    pub fn with_body(body: Vec<u8>, metadata: impl Into<UploadMetadata>) -> Self {
        Self {
            metadata: metadata.into(),
            content: NewUploadContent::Body(body),
            partial: false,
        }
    }

    /// Marks the upload as partial for later concatenation.
    #[must_use]
    pub fn partial(mut self) -> Self {
        self.partial = true;
        self
    }
}

#[derive(Debug)]
pub(crate) enum NewUploadContent {
    Length(u64),
    Body(Vec<u8>),
}

/// Server capabilities discovered via the TUS `OPTIONS` request.
///
/// Returned by [`crate::Client::server_capabilities`]. Use
/// [`ServerCapabilities::has_extension`]
/// to gate features that require a specific TUS extension.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerCapabilities {
    /// Protocol versions the server supports (`Tus-Version`), most preferred
    /// first.
    pub versions: Vec<String>,
    /// Maximum upload size in bytes, if the server publishes one
    /// (`Tus-Max-Size`). `None` means unlimited or not advertised.
    pub max_size: Option<u64>,
    /// Extensions the server supports (`Tus-Extension`), parsed from the
    /// comma-separated header.
    pub extensions: Vec<String>,
    /// Checksum algorithms the server accepts (`Tus-Checksum-Algorithm`),
    /// when the `checksum` extension is enabled.
    pub checksum_algorithms: Vec<String>,
}

impl ServerCapabilities {
    /// Reports whether the server advertises the named TUS extension.
    ///
    /// Names are case-insensitive and match the TUS spec — e.g. `creation`,
    /// `creation-with-upload`, `termination`, `concatenation`, `expiration`.
    pub fn has_extension(&self, name: &str) -> bool {
        self.extensions.iter().any(|e| e.eq_ignore_ascii_case(name))
    }

    /// Reports whether the server advertises the named protocol version.
    pub fn supports_version(&self, version: &str) -> bool {
        self.versions
            .iter()
            .any(|v| v.eq_ignore_ascii_case(version))
    }
}

impl<T> Client<T>
where
    T: Transport,
{
    /// Probes the TUS server's capabilities via OPTIONS.
    ///
    /// Returns the parsed `Tus-Version`, `Tus-Extension`, `Tus-Max-Size`,
    /// and `Tus-Checksum-Algorithm` headers. Use [`ServerCapabilities::has_extension`]
    /// to gate features at runtime, e.g. picking
    /// `creation-with-upload` over `creation` for small files.
    ///
    /// Note: this is a separate roundtrip from any browser CORS preflight —
    /// modern browsers cache preflight responses but a manual OPTIONS does
    /// not benefit from that cache. Cache the result on your end if you call
    /// it for every upload.
    pub async fn server_capabilities(&self) -> Result<ServerCapabilities> {
        // TUS 1.0.0 §2.3 explicitly excludes OPTIONS from the
        // "Tus-Resumable required on every request" rule. Strict server
        // implementations (some `tusd` deployments, custom impls) reject
        // OPTIONS with `Tus-Resumable` set as a 400. The shared `request`
        // helper inserts that header unconditionally; strip it back out
        // for OPTIONS to match the spec and stay compatible with strict
        // servers.
        let mut req = self.request(Method::OPTIONS, self.endpoint.as_str())?;
        req.headers_mut().remove("tus-resumable");
        let response = self.transport.send(req).await?;
        // TUS allows 200 or 204 here; other statuses are not valid OPTIONS responses.
        if response.status().as_u16() != 200 && response.status().as_u16() != 204 {
            return Err(unexpected_response("server capabilities", response).await);
        }

        let versions = response
            .headers()
            .get("tus-version")
            .and_then(|v| v.to_str().ok())
            .map(parse_csv_header)
            .unwrap_or_default();
        // TUS 1.0.0 §3.1: a compliant server MUST advertise at least one
        // protocol version via Tus-Version. An empty/absent header on a
        // 2xx/3xx response means the endpoint is not a TUS server (plain
        // nginx, CDN default page, health-check). Surface that as a typed
        // error rather than returning Ok with empty capabilities, where
        // callers would silently fall back to plain create + PATCH and
        // then fail with a confusing UnexpectedResponse on POST.
        if versions.is_empty() {
            return Err(Error::MissingHeader("Tus-Version"));
        }
        let extensions = response
            .headers()
            .get("tus-extension")
            .and_then(|v| v.to_str().ok())
            .map(parse_csv_header)
            .unwrap_or_default();
        let checksum_algorithms = response
            .headers()
            .get("tus-checksum-algorithm")
            .and_then(|v| v.to_str().ok())
            .map(parse_csv_header)
            .unwrap_or_default();
        let max_size = optional_header_u64(response.headers(), "tus-max-size", "Tus-Max-Size")?;

        Ok(ServerCapabilities {
            versions,
            max_size,
            extensions,
            checksum_algorithms,
        })
    }

    /// Creates a final concatenated upload from already completed partials.
    pub async fn concatenate_uploads(
        &self,
        part_urls: &[String],
        metadata: impl Into<UploadMetadata>,
    ) -> Result<UploadInfo> {
        let metadata = metadata.into();
        let upload_concat = format!("final;{}", part_urls.join(" "));
        let mut request = self.request(Method::POST, self.endpoint.as_str())?;
        insert_request_header(&mut request, "upload-concat", upload_concat)?;
        let encoded_metadata = encode_metadata(&metadata)?;
        if !encoded_metadata.is_empty() {
            insert_request_header(&mut request, "upload-metadata", encoded_metadata)?;
        }

        let response = self.transport.send(request).await?;
        if response.status().as_u16() != 201 {
            return Err(unexpected_response("concatenate uploads", response).await);
        }

        let location = header_string(response.headers(), http::header::LOCATION, "Location")?;
        let final_url = resolve_upload_location(&self.endpoint, &location)?;
        self.upload_info_at(&final_url).await
    }

    /// Creates a new upload resource and returns its protocol state.
    pub(super) async fn create_upload_info(&self, upload: NewUpload) -> Result<UploadInfo> {
        let metadata = upload.metadata;
        let partial = upload.partial;
        match upload.content {
            NewUploadContent::Length(length) => {
                self.create_upload_request(length, None, &metadata, partial)
                    .await
            }
            NewUploadContent::Body(body) => {
                self.create_upload_request(body.len() as u64, Some(body), &metadata, partial)
                    .await
            }
        }
    }

    async fn create_upload_request(
        &self,
        length: u64,
        body: Option<Vec<u8>>,
        metadata: &UploadMetadata,
        partial: bool,
    ) -> Result<UploadInfo> {
        let has_body = body.is_some();
        let mut request = self.request(Method::POST, self.endpoint.as_str())?;
        insert_request_header(&mut request, "upload-length", length)?;
        if has_body {
            insert_request_header(
                &mut request,
                CONTENT_TYPE.as_str(),
                "application/offset+octet-stream",
            )?;
        }
        let encoded_metadata = encode_metadata(metadata)?;
        if !encoded_metadata.is_empty() {
            insert_request_header(&mut request, "upload-metadata", encoded_metadata)?;
        }
        if partial {
            insert_request_header(&mut request, "upload-concat", "partial")?;
        }
        if let Some(body) = body {
            request = self.apply_checksum(request, body)?;
        }

        let response = self.transport.send(request).await?;
        if response.status().as_u16() != 201 {
            let operation = if has_body {
                "create upload with body"
            } else {
                "create upload"
            };
            return Err(unexpected_response(operation, response).await);
        }

        let location = header_string(response.headers(), http::header::LOCATION, "Location")?;
        let url = resolve_upload_location(&self.endpoint, &location)?;
        let offset = if has_body {
            let offset = header_u64(response.headers(), "upload-offset", "Upload-Offset")?;
            validate_offset_not_beyond_source(offset, length)?;
            offset
        } else {
            0
        };

        Ok(UploadInfo {
            url,
            offset,
            length: Some(length),
            metadata: metadata.clone(),
        })
    }

    /// Fetches the current server-side upload state.
    pub(super) async fn upload_info_at(&self, upload_url: &Url) -> Result<UploadInfo> {
        let response = self
            .transport
            .send(self.request(Method::HEAD, upload_url.as_str())?)
            .await?;
        if response.status().as_u16() != 200 && response.status().as_u16() != 204 {
            return Err(unexpected_response("upload info", response).await);
        }

        let offset = header_u64(response.headers(), "upload-offset", "Upload-Offset")?;
        let length = optional_header_u64(response.headers(), "upload-length", "Upload-Length")?;
        let metadata = decode_metadata(response.headers().get("upload-metadata"))?;

        Ok(UploadInfo {
            url: upload_url.clone(),
            offset,
            length,
            metadata,
        })
    }

    /// Terminates an existing upload.
    pub(super) async fn delete_upload_at(&self, upload_url: &Url) -> Result<()> {
        let response = self
            .transport
            .send(self.request(Method::DELETE, upload_url.as_str())?)
            .await?;
        if response.status().as_u16() != 204 {
            Err(unexpected_response("delete upload", response).await)
        } else {
            Ok(())
        }
    }

    /// Uploads a single chunk to an existing upload and returns the new server offset.
    ///
    /// This is a low-level primitive for callers that already have chunk bytes in memory
    /// (e.g., from a browser Blob slice). The caller is responsible for tracking `offset`
    /// and calling in order.
    pub(super) async fn upload_chunk_at(
        &self,
        upload_url: &Url,
        chunk: Vec<u8>,
        offset: u64,
    ) -> Result<u64> {
        self.send_upload_chunk_at(upload_url, chunk, offset, "upload chunk")
            .await
    }

    pub(super) async fn send_upload_chunk_at(
        &self,
        upload_url: &Url,
        chunk: Vec<u8>,
        offset: u64,
        operation: &'static str,
    ) -> Result<u64> {
        let mut request = self.request(Method::PATCH, upload_url.as_str())?;
        insert_request_header(&mut request, "upload-offset", offset)?;
        insert_request_header(
            &mut request,
            CONTENT_TYPE.as_str(),
            "application/offset+octet-stream",
        )?;
        let response = self
            .transport
            .send(self.apply_checksum(request, chunk)?)
            .await?;
        if response.status().as_u16() != 204 {
            return Err(unexpected_response(operation, response).await);
        }
        header_u64(response.headers(), "upload-offset", "Upload-Offset")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Client;
    use crate::client::test_support::*;
    use crate::{Error, TransportBody};
    use http::Method;
    use http::header::{HeaderMap, HeaderValue, LOCATION};
    use std::collections::HashMap;

    #[cfg(not(target_arch = "wasm32"))]
    use tokio::test as async_test;
    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as async_test;

    #[async_test]
    async fn create_upload_rejects_invalid_metadata_keys_before_request() {
        for key in ["", "bad key", "bad,key", "bad\nkey"] {
            let transport = MockTransport::default();
            let client = Client::with_transport(endpoint_url(), transport.clone());
            let mut metadata = HashMap::new();
            metadata.insert(key.to_string(), "value".to_string());

            let result = client
                .create_upload_info(NewUpload::new(1, &metadata))
                .await;

            match result {
                Err(Error::InvalidHeader { header, value }) => {
                    assert_eq!(header, "Upload-Metadata");
                    assert!(
                        value.contains(key),
                        "invalid key should be named in error: {value}"
                    );
                }
                other => panic!("expected InvalidHeader for metadata key {key:?}, got {other:?}"),
            }
            assert!(
                transport.requests.lock().unwrap().is_empty(),
                "invalid metadata key {key:?} must fail before sending a request",
            );
        }
    }

    #[async_test]
    async fn upload_info_decodes_bare_metadata_key_as_empty_value() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                200,
                header_map(&[
                    ("Upload-Offset", "0"),
                    ("Upload-Length", "4"),
                    ("Upload-Metadata", "filename ZGF0YS50eHQ=,emptykey"),
                ]),
                vec![],
            )));
        }

        let client = Client::with_transport(endpoint_url(), transport);
        let info = client
            .upload_info_at(&upload_url("upload-1"))
            .await
            .expect("HEAD metadata should decode");

        assert_eq!(
            info.metadata.get("filename").and_then(|v| v.as_str()),
            Some("data.txt")
        );
        assert_eq!(
            info.metadata.get("emptykey").and_then(|v| v.as_str()),
            Some("")
        );
    }

    #[async_test]
    async fn upload_info_preserves_binary_metadata_values() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                200,
                header_map(&[
                    ("Upload-Offset", "0"),
                    ("Upload-Length", "3"),
                    ("Upload-Metadata", "bin //79"),
                ]),
                vec![],
            )));
        }

        let client = Client::with_transport(endpoint_url(), transport);
        let info = client
            .upload_info_at(&upload_url("upload-1"))
            .await
            .expect("HEAD metadata should decode");

        assert_eq!(
            info.metadata.get("bin").unwrap().as_bytes(),
            [0xFF, 0xFE, 0xFD]
        );
    }

    #[async_test]
    async fn create_upload_resolves_relative_location_with_standard_url_resolution() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(LOCATION, HeaderValue::from_static("upload-1"));
                    headers
                },
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), transport);
        let upload = client
            .create_upload_info(NewUpload::new(5, UploadMetadata::new()))
            .await
            .unwrap();

        assert_eq!(upload.url.as_str(), "http://example.test/upload-1");
    }

    #[async_test]
    async fn create_upload_returns_typed_upload_url() {
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

        let client = Client::with_transport(endpoint_url(), transport);
        let upload = client
            .create_upload_info(NewUpload::new(5, UploadMetadata::new()))
            .await
            .unwrap();
        let url: &url::Url = &upload.url;

        assert_eq!(url.as_str(), "http://example.test/upload-1");
    }

    #[async_test]
    async fn create_upload_accepts_binary_upload_metadata() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                header_map(&[("Location", "/files/upload-1")]),
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), transport.clone());
        let mut metadata = UploadMetadata::new();
        metadata.insert("bin", tus_protocol::MetadataValue::from(&b"\xFF\xFE"[..]));

        let upload = client
            .create_upload_info(NewUpload::new(2, metadata.clone()))
            .await
            .unwrap();

        assert_eq!(upload.metadata, metadata);
        let requests = transport.requests.lock().unwrap();
        assert_eq!(
            requests
                .first()
                .unwrap()
                .headers()
                .get("upload-metadata")
                .and_then(|value| value.to_str().ok()),
            Some("bin //4=")
        );
    }

    #[async_test]
    async fn new_upload_body_requires_upload_offset_header() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                {
                    let mut headers = HeaderMap::new();
                    headers.insert(LOCATION, HeaderValue::from_static("/files/upload-1"));
                    headers
                },
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), transport);
        let result = client
            .create_upload_info(NewUpload::with_body(
                b"hello".to_vec(),
                UploadMetadata::new(),
            ))
            .await;

        assert!(matches!(result, Err(Error::MissingHeader("Upload-Offset"))));
    }

    #[async_test]
    async fn create_upload_rejects_non_201_success_status() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                204,
                header_map(&[("Location", "/files/upload-1")]),
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), transport);
        let result = client
            .create_upload_info(NewUpload::new(5, UploadMetadata::new()))
            .await;

        expect_unexpected_status(result, "create upload", 204);
    }

    #[async_test]
    async fn new_upload_body_rejects_non_201_success_status() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                204,
                header_map(&[("Location", "/files/upload-1"), ("Upload-Offset", "5")]),
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), transport);
        let result = client
            .create_upload_info(NewUpload::with_body(
                b"hello".to_vec(),
                UploadMetadata::new(),
            ))
            .await;

        expect_unexpected_status(result, "create upload with body", 204);
    }

    #[async_test]
    async fn create_upload_accepts_partial_new_upload_request() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                header_map(&[("Location", "/files/upload-1")]),
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), transport.clone());
        let upload = client
            .create_upload_info(NewUpload::new(5, UploadMetadata::new()).partial())
            .await
            .unwrap();

        assert_eq!(upload.url.as_str(), "http://example.test/files/upload-1");
        let requests = transport.requests.lock().unwrap();
        assert_eq!(
            requests
                .first()
                .unwrap()
                .headers()
                .get("upload-concat")
                .and_then(|value| value.to_str().ok()),
            Some("partial")
        );
    }

    #[async_test]
    async fn upload_info_accepts_204_success_status() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                204,
                header_map(&[("Upload-Offset", "5"), ("Upload-Length", "5")]),
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), transport);
        let upload = client
            .upload_info_at(&upload_url("upload-1"))
            .await
            .unwrap();

        assert_eq!(upload.offset, 5);
        assert_eq!(upload.length, Some(5));
    }

    #[async_test]
    async fn upload_info_rejects_non_200_or_204_success_status() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                206,
                header_map(&[("Upload-Offset", "5"), ("Upload-Length", "5")]),
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), transport);
        let result = client.upload_info_at(&upload_url("upload-1")).await;

        expect_unexpected_status(result, "upload info", 206);
    }

    #[async_test]
    async fn upload_chunk_rejects_non_204_success_status() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                200,
                header_map(&[("Upload-Offset", "5")]),
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), transport);
        let result = client
            .upload_chunk_at(&upload_url("upload-1"), b"hello".to_vec(), 0)
            .await;

        expect_unexpected_status(result, "upload chunk", 200);
    }

    #[async_test]
    async fn delete_upload_rejects_non_204_success_status() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(200, HeaderMap::new(), Vec::new())));

        let client = Client::with_transport(endpoint_url(), transport);
        let result = client.delete_upload_at(&upload_url("upload-1")).await;

        expect_unexpected_status(result, "delete upload", 200);
    }

    #[async_test]
    async fn concatenate_uploads_rejects_non_201_success_status() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                204,
                header_map(&[("Location", "/files/final")]),
                Vec::new(),
            )));
            responses.push_back(Ok(mock_head_response(8, 8)));
        }

        let client = Client::with_transport(endpoint_url(), transport);
        let result = client
            .concatenate_uploads(
                &["http://example.test/files/part-1".to_string()],
                UploadMetadata::new(),
            )
            .await;

        expect_unexpected_status(result, "concatenate uploads", 204);
    }

    #[async_test]
    async fn upload_chunk_sends_correct_headers_and_returns_new_offset() {
        let transport = MockTransport::default();
        {
            let mut responses = transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_patch_response(8)));
        }

        let client = Client::with_transport(endpoint_url(), transport.clone());

        let new_offset = client
            .upload_chunk_at(&upload_url("abc"), b"12345678".to_vec(), 0)
            .await
            .unwrap();

        assert_eq!(new_offset, 8);

        let requests = transport.requests.lock().unwrap();
        let req = requests.first().unwrap();
        assert_eq!(req.method(), Method::PATCH);
        assert_eq!(
            req.headers()
                .get("upload-offset")
                .and_then(|v| v.to_str().ok()),
            Some("0")
        );
        assert_eq!(
            req.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/offset+octet-stream")
        );
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn server_capabilities_has_extension_is_case_insensitive() {
        let info = ServerCapabilities {
            versions: vec!["1.0.0".into()],
            max_size: None,
            extensions: vec!["Creation".into(), "CREATION-WITH-UPLOAD".into()],
            checksum_algorithms: vec![],
        };
        assert!(info.has_extension("creation"));
        assert!(info.has_extension("Creation"));
        assert!(info.has_extension("creation-with-upload"));
        assert!(!info.has_extension("checksum"));
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn server_capabilities_supports_version_lookup() {
        let info = ServerCapabilities {
            versions: vec!["1.0.0".into(), "0.2.2".into()],
            max_size: Some(1024),
            extensions: vec![],
            checksum_algorithms: vec![],
        };
        assert!(info.supports_version("1.0.0"));
        assert!(info.supports_version("0.2.2"));
        assert!(!info.supports_version("2.0.0"));
    }

    #[async_test]
    async fn server_capabilities_uses_configured_endpoint_url() {
        let transport = MockTransport::default();
        {
            let mut responses = transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                204,
                header_map(&[
                    ("tus-version", "1.0.0"),
                    ("tus-extension", "creation,termination"),
                ]),
                Vec::new(),
            )));
        }
        let client = Client::with_transport(endpoint_url(), transport.clone());
        let info = client.server_capabilities().await.unwrap();
        assert_eq!(info.versions, vec!["1.0.0"]);
        assert_eq!(info.extensions, vec!["creation", "termination"]);
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.first().unwrap().method(), Method::OPTIONS);
        assert_eq!(
            requests.first().unwrap().uri().to_string(),
            "http://example.test/files"
        );
    }

    /// TUS 1.0.0 §2.3: the `Tus-Resumable` header is required on every
    /// request *except* OPTIONS. Strict server implementations (some
    /// `tusd` deployments) return 400 when they see it. Pin the
    /// no-Tus-Resumable contract for OPTIONS so a regression in
    /// `request()` (or in our strip logic) doesn't reintroduce it.
    #[async_test]
    async fn server_capabilities_does_not_send_tus_resumable_header() {
        let transport = MockTransport::default();
        {
            let mut responses = transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                204,
                header_map(&[("tus-version", "1.0.0")]),
                Vec::new(),
            )));
        }
        let client = Client::with_transport(endpoint_url(), transport.clone());
        client.server_capabilities().await.unwrap();
        let requests = transport.requests.lock().unwrap();
        let options_req = requests.first().expect("one OPTIONS request");
        assert_eq!(options_req.method(), Method::OPTIONS);
        assert!(
            !options_req.headers().contains_key("tus-resumable"),
            "OPTIONS must not carry Tus-Resumable per TUS 1.0.0 §2.3"
        );
    }

    #[async_test]
    async fn server_capabilities_rejects_non_200_or_204_success_status() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                202,
                header_map(&[("tus-version", "1.0.0")]),
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), transport);
        let result = client.server_capabilities().await;

        expect_unexpected_status(result, "server capabilities", 202);
    }

    /// A 2xx/3xx OPTIONS response without `Tus-Version` means the endpoint
    /// is not a TUS server (plain nginx, CDN default, health-check). The
    /// client must surface that as a typed error rather than `Ok(empty)`,
    /// so the caller doesn't silently fall back to plain create + PATCH
    /// and then fail with a confusing `UnexpectedResponse` on POST.
    #[async_test]
    async fn server_capabilities_rejects_response_without_tus_version() {
        let transport = MockTransport::default();
        {
            let mut responses = transport.responses.lock().unwrap();
            // 200 OK but no Tus-Version header — nginx default page.
            responses.push_back(Ok(transport_response(
                200,
                HeaderMap::new(),
                b"<html>nginx</html>".to_vec(),
            )));
        }
        let client = Client::with_transport(endpoint_url(), transport.clone());
        let result = client.server_capabilities().await;
        match result {
            Err(Error::MissingHeader(h)) => {
                assert_eq!(h, "Tus-Version");
            }
            other => panic!("expected MissingHeader(Tus-Version), got {other:?}"),
        }
    }

    #[async_test]
    async fn create_upload_with_body_sends_bytes() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                201,
                header_map(&[("Location", "/files/upload-1"), ("Upload-Offset", "5")]),
                Vec::new(),
            )));

        let client = Client::with_transport(endpoint_url(), transport.clone());
        let upload = client
            .create_upload_info(NewUpload::with_body(
                b"hello".to_vec(),
                UploadMetadata::new(),
            ))
            .await
            .unwrap();

        assert_eq!(upload.offset, 5);
        let requests = transport.requests.lock().unwrap();
        let body = match requests.first().unwrap().body() {
            TransportBody::Bytes(bytes) => bytes,
            other => panic!("expected byte body, got {other:?}"),
        };
        assert_eq!(body, b"hello");
    }
}
