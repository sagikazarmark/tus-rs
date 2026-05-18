use std::collections::HashMap;

use http::HeaderMap;
use http::header::AUTHORIZATION;
use tus_protocol::{ChecksumAlgorithm, UploadMetadata};
use url::Url;

use crate::{ChecksumMode, Client, Error, NewUpload, ParallelUpload, ReqwestTransport, Result};

/// Backwards-compatible error alias used by older integration tests.
pub type ClientError = Error;

/// Backwards-compatible upload state returned by [`TusClient`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TusUpload {
    /// Absolute upload URL.
    pub url: String,
    /// Current server-side offset.
    pub offset: u64,
    /// Declared upload length, if known.
    pub length: Option<u64>,
    /// Decoded upload metadata.
    pub metadata: UploadMetadata,
}

impl From<crate::UploadInfo> for TusUpload {
    fn from(info: crate::UploadInfo) -> Self {
        Self {
            url: info.url.to_string(),
            offset: info.offset,
            length: info.length,
            metadata: info.metadata,
        }
    }
}

/// Backwards-compatible reqwest-backed TUS client facade.
#[derive(Debug, Clone)]
pub struct TusClient {
    inner: Client<ReqwestTransport>,
}

impl TusClient {
    /// Creates a client from a TUS collection endpoint URL.
    pub fn new(endpoint: impl AsRef<str>) -> Result<Self> {
        Ok(Self {
            inner: Client::new(Url::parse(endpoint.as_ref())?),
        })
    }

    /// Adds an `Authorization: Bearer ...` header to every request.
    pub fn with_bearer_token(mut self, token: impl AsRef<str>) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", token.as_ref()).parse().map_err(|_| {
                Error::InvalidDefaultHeader {
                    name: AUTHORIZATION.to_string(),
                    value: token.as_ref().to_string(),
                }
            })?,
        );
        self.inner = self.inner.with_headers(headers);
        Ok(self)
    }

    /// Sets the largest body sent in the initial creation request.
    pub fn with_creation_with_upload_threshold(mut self, threshold: usize) -> Self {
        self.inner = self.inner.with_max_initial_upload_size(threshold);
        self
    }

    /// Sets the maximum PATCH chunk size.
    pub fn with_patch_chunk_size(mut self, chunk_size: usize) -> Self {
        self.inner = self.inner.with_max_chunk_size(chunk_size);
        self
    }

    /// Sets the number of PATCH retries.
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.inner = self.inner.with_max_retries(max_retries);
        self
    }

    /// Enables checksum headers.
    pub fn with_checksum(mut self, algorithm: ChecksumAlgorithm) -> Self {
        self.inner = self.inner.with_checksum(algorithm);
        self
    }

    /// Enables checksum trailers.
    pub fn with_checksum_trailer(mut self, algorithm: ChecksumAlgorithm) -> Self {
        self.inner = self.inner.with_checksum(ChecksumMode::Trailer(algorithm));
        self
    }

    /// Creates an empty upload resource.
    pub async fn create_upload(
        &self,
        length: u64,
        metadata: &HashMap<String, String>,
    ) -> Result<TusUpload> {
        let upload = self
            .inner
            .create_upload(NewUpload::new(length, metadata))
            .await?;
        upload.info().await.map(Into::into)
    }

    /// Uploads a complete file.
    pub async fn upload_file(
        &self,
        path: impl AsRef<std::path::Path>,
        metadata: &HashMap<String, String>,
    ) -> Result<TusUpload> {
        let bytes = tokio::fs::read(path).await?;
        self.inner
            .upload_from(bytes, metadata)
            .await
            .map(Into::into)
    }

    /// Uploads a complete file as multiple partial uploads.
    pub async fn upload_file_parallel(
        &self,
        path: impl AsRef<std::path::Path>,
        metadata: &HashMap<String, String>,
        options: ParallelUpload,
    ) -> Result<TusUpload> {
        let bytes = tokio::fs::read(path).await?;
        self.inner
            .upload_parallel(bytes, metadata, options)
            .await
            .map(Into::into)
    }

    /// Fetches upload state from a resource URL.
    pub async fn head(&self, upload_url: impl AsRef<str>) -> Result<TusUpload> {
        self.inner
            .upload(upload_url.as_ref())?
            .info()
            .await
            .map(Into::into)
    }

    /// Resumes a file upload at a resource URL.
    pub async fn resume_file(
        &self,
        upload_url: impl AsRef<str>,
        path: impl AsRef<std::path::Path>,
    ) -> Result<TusUpload> {
        let bytes = tokio::fs::read(path).await?;
        self.inner
            .upload(upload_url.as_ref())?
            .upload(bytes)
            .await
            .map(Into::into)
    }

    /// Deletes an upload resource.
    pub async fn delete_upload(&self, upload_url: impl AsRef<str>) -> Result<()> {
        self.inner.upload(upload_url.as_ref())?.terminate().await
    }
}
