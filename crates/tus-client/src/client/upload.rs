use async_trait::async_trait;

#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinSet;

use super::{Client, NewUpload, ServerCapabilities, UploadInfo};
use crate::error::Error;
use crate::error::Result;

use crate::helpers::{
    is_retryable_resume_error, jittered_backoff_delay, validate_patch_advance,
    validate_remote_for_resume,
};
use crate::runtime::MaybeSend;

use crate::transport::Transport;
use tus_protocol::UploadMetadata;

/// Offset-addressable upload content.
#[cfg_attr(
    all(not(feature = "local-futures"), not(target_arch = "wasm32")),
    async_trait
)]
#[cfg_attr(any(feature = "local-futures", target_arch = "wasm32"), async_trait(?Send))]
pub trait UploadSource: MaybeSend {
    /// Total source length in bytes.
    fn len(&self) -> u64;

    /// Reports whether the source has no bytes.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reads up to `max_len` bytes starting at `offset`.
    async fn read_chunk(&mut self, offset: u64, max_len: usize) -> Result<Vec<u8>>;
}

#[cfg_attr(
    all(not(feature = "local-futures"), not(target_arch = "wasm32")),
    async_trait
)]
#[cfg_attr(any(feature = "local-futures", target_arch = "wasm32"), async_trait(?Send))]
impl UploadSource for Vec<u8> {
    fn len(&self) -> u64 {
        self.as_slice().len() as u64
    }

    async fn read_chunk(&mut self, offset: u64, max_len: usize) -> Result<Vec<u8>> {
        let Ok(offset) = usize::try_from(offset) else {
            return Ok(Vec::new());
        };
        let Some(bytes) = self.get(offset..) else {
            return Ok(Vec::new());
        };
        let len = bytes.len().min(max_len);
        Ok(bytes[..len].to_vec())
    }
}

/// Parameters for parallel concatenation uploads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ParallelUpload {
    /// Number of bytes to place into each partial upload.
    pub part_size: usize,
    /// Maximum number of partial uploads to run at once.
    pub max_concurrency: usize,
}

impl ParallelUpload {
    /// Creates a new parallel-upload configuration.
    pub fn new(part_size: usize) -> Self {
        Self {
            part_size: part_size.max(1),
            max_concurrency: 4,
        }
    }

    /// Sets the maximum number of partial uploads to run at once.
    #[must_use]
    pub fn with_max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = max_concurrency.max(1);
        self
    }
}

/// Progress callback for uploads.
pub trait UploadProgress {
    /// Called after the client successfully advances the remote offset.
    fn on_progress(&mut self, uploaded: u64, total: u64);
}

impl<F> UploadProgress for F
where
    F: FnMut(u64, u64),
{
    fn on_progress(&mut self, uploaded: u64, total: u64) {
        self(uploaded, total);
    }
}

pub(crate) struct NoopProgress;

impl UploadProgress for NoopProgress {
    fn on_progress(&mut self, _uploaded: u64, _total: u64) {}
}

impl<T> Client<T>
where
    T: Transport,
{
    /// Creates a new upload and sends the entire source to it.
    pub async fn upload_from<S>(
        &self,
        source: S,
        metadata: impl Into<UploadMetadata>,
    ) -> Result<UploadInfo>
    where
        S: UploadSource,
    {
        self.upload_from_with_progress(source, metadata, &mut NoopProgress)
            .await
    }

    /// Creates a new upload and reports progress as the remote offset advances.
    pub async fn upload_from_with_progress<S, P>(
        &self,
        mut source: S,
        metadata: impl Into<UploadMetadata>,
        progress: &mut P,
    ) -> Result<UploadInfo>
    where
        S: UploadSource,
        P: UploadProgress,
    {
        let length = source.len();
        let metadata = metadata.into();
        let capabilities = self.creation_capabilities(length).await;

        if self.should_use_creation_with_upload(length, capabilities.as_ref()) {
            let body = Self::read_source_exact(&mut source, 0, length).await?;
            let upload = self
                .create_upload_info(NewUpload::with_body(body, metadata))
                .await?;
            progress.on_progress(upload.offset, length);
            if upload.offset == length {
                return Ok(upload);
            }
            return self
                .resume_at_with_progress(&upload.url, source, progress)
                .await;
        }

        let handle = self.create_upload(NewUpload::new(length, metadata)).await?;
        handle.upload_with_progress(source, progress).await
    }

    /// Uploads a source as multiple partial uploads and concatenates them.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn upload_parallel<S>(
        &self,
        source: S,
        metadata: impl Into<UploadMetadata>,
        options: ParallelUpload,
    ) -> Result<UploadInfo>
    where
        S: UploadSource + Clone + 'static,
    {
        let source_length = source.len();
        if source_length == 0 {
            return self.upload_from(source, metadata).await;
        }

        let metadata = metadata.into();
        let capabilities = if self.max_initial_upload_size > 0 {
            self.server_capabilities().await.ok()
        } else {
            None
        };
        let part_count = source_length.div_ceil(options.part_size as u64) as usize;
        let max_concurrency = options.max_concurrency.min(part_count).max(1);
        #[cfg(not(feature = "local-futures"))]
        let part_urls = {
            let mut join_set = JoinSet::new();
            let mut next_index = 0;

            while next_index < part_count && join_set.len() < max_concurrency {
                let client = self.clone();
                let source = source.clone();
                let capabilities = capabilities.clone();
                let index = next_index;
                next_index += 1;
                let task = async move {
                    client
                        .upload_parallel_part(
                            source,
                            index,
                            source_length,
                            options.part_size,
                            capabilities,
                        )
                        .await
                };

                join_set.spawn(task);
            }

            let mut part_urls = vec![String::new(); part_count];
            while let Some((index, url)) = Self::join_next_parallel_part(&mut join_set).await? {
                part_urls[index] = url;

                if next_index < part_count {
                    let client = self.clone();
                    let source = source.clone();
                    let capabilities = capabilities.clone();
                    let index = next_index;
                    next_index += 1;
                    let task = async move {
                        client
                            .upload_parallel_part(
                                source,
                                index,
                                source_length,
                                options.part_size,
                                capabilities,
                            )
                            .await
                    };

                    join_set.spawn(task);
                }
            }

            part_urls
        };

        #[cfg(feature = "local-futures")]
        let part_urls = tokio::task::LocalSet::new()
            .run_until(async {
                let mut join_set = JoinSet::new();
                let mut next_index = 0;

                while next_index < part_count && join_set.len() < max_concurrency {
                    let client = self.clone();
                    let source = source.clone();
                    let capabilities = capabilities.clone();
                    let index = next_index;
                    next_index += 1;
                    let task = async move {
                        client
                            .upload_parallel_part(
                                source,
                                index,
                                source_length,
                                options.part_size,
                                capabilities,
                            )
                            .await
                    };

                    join_set.spawn_local(task);
                }

                let mut part_urls = vec![String::new(); part_count];
                while let Some((index, url)) = Self::join_next_parallel_part(&mut join_set).await? {
                    part_urls[index] = url;

                    if next_index < part_count {
                        let client = self.clone();
                        let source = source.clone();
                        let capabilities = capabilities.clone();
                        let index = next_index;
                        next_index += 1;
                        let task = async move {
                            client
                                .upload_parallel_part(
                                    source,
                                    index,
                                    source_length,
                                    options.part_size,
                                    capabilities,
                                )
                                .await
                        };

                        join_set.spawn_local(task);
                    }
                }

                Ok::<_, Error>(part_urls)
            })
            .await?;

        self.concatenate_uploads(&part_urls, metadata).await
    }

    /// Resumes a previously created upload from the server's current offset.
    pub(super) async fn resume_at<S>(&self, upload_url: &url::Url, source: S) -> Result<UploadInfo>
    where
        S: UploadSource,
    {
        self.resume_at_with_progress(upload_url, source, &mut NoopProgress)
            .await
    }

    /// Resumes a remote upload and reports progress updates.
    pub(super) async fn resume_at_with_progress<S, P>(
        &self,
        upload_url: &url::Url,
        mut source: S,
        progress: &mut P,
    ) -> Result<UploadInfo>
    where
        S: UploadSource,
        P: UploadProgress,
    {
        let source_length = source.len();

        for attempt in 0..=self.max_retries {
            let remote = match self.upload_info_at(upload_url).await {
                Ok(remote) => remote,
                Err(error) => {
                    if !is_retryable_resume_error(&error) || attempt == self.max_retries {
                        return Err(error);
                    }
                    if let Some(hook) = &self.retry_hook
                        && !hook.before_retry(attempt + 1, &error).await?
                    {
                        return Err(error);
                    }
                    sleep_before_retry(self.retry_delay, attempt).await;
                    continue;
                }
            };
            validate_remote_for_resume(&remote, source_length)?;

            if remote.offset == source_length {
                return Ok(remote);
            }

            let patch_result = self
                .patch_source(
                    upload_url,
                    &mut source,
                    remote.offset,
                    source_length,
                    progress,
                )
                .await;
            match patch_result {
                Ok(()) => {
                    let remote = match self.upload_info_at(upload_url).await {
                        Ok(remote) => remote,
                        Err(error) => {
                            if !is_retryable_resume_error(&error) || attempt == self.max_retries {
                                return Err(error);
                            }
                            if let Some(hook) = &self.retry_hook
                                && !hook.before_retry(attempt + 1, &error).await?
                            {
                                return Err(error);
                            }
                            sleep_before_retry(self.retry_delay, attempt).await;
                            continue;
                        }
                    };
                    validate_remote_for_resume(&remote, source_length)?;
                    if remote.offset == source_length {
                        return Ok(remote);
                    }
                    return Err(Error::Transport(format!(
                        "server offset {} is below local source length {} after upload",
                        remote.offset, source_length,
                    )));
                }
                Err(error) => {
                    if !is_retryable_resume_error(&error) {
                        return Err(error);
                    }

                    if attempt == self.max_retries {
                        let remote = self.upload_info_at(upload_url).await?;
                        validate_remote_for_resume(&remote, source_length)?;
                        if remote.offset == source_length {
                            return Ok(remote);
                        }
                        return Err(error);
                    }

                    if let Some(hook) = &self.retry_hook
                        && !hook.before_retry(attempt + 1, &error).await?
                    {
                        return Err(error);
                    }
                }
            }

            sleep_before_retry(self.retry_delay, attempt).await;
        }

        self.upload_info_at(upload_url).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn upload_parallel_part<S>(
        &self,
        mut source: S,
        index: usize,
        source_length: u64,
        part_size: usize,
        capabilities: Option<ServerCapabilities>,
    ) -> Result<(usize, String)>
    where
        S: UploadSource,
    {
        let start = index as u64 * part_size as u64;
        let length = (source_length - start).min(part_size as u64);
        let bytes = Self::read_source_exact(&mut source, start, length).await?;
        let upload = self
            .upload_partial_with_capabilities(bytes, UploadMetadata::new(), capabilities.as_ref())
            .await?;

        Ok((index, upload.url.to_string()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn join_next_parallel_part(
        join_set: &mut JoinSet<Result<(usize, String)>>,
    ) -> Result<Option<(usize, String)>> {
        match join_set.join_next().await {
            Some(result) => Ok(Some(
                result.map_err(|e| Error::Io(std::io::Error::other(e)))??,
            )),
            None => Ok(None),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn upload_partial_with_capabilities<S>(
        &self,
        mut source: S,
        metadata: UploadMetadata,
        capabilities: Option<&ServerCapabilities>,
    ) -> Result<UploadInfo>
    where
        S: UploadSource,
    {
        let length = source.len();

        if self.should_use_creation_with_upload(length, capabilities) {
            let body = Self::read_source_exact(&mut source, 0, length).await?;
            let upload = self
                .create_upload_info(NewUpload::with_body(body, metadata).partial())
                .await?;
            if upload.offset == length {
                return Ok(upload);
            }
            return self.resume_at(&upload.url, source).await;
        }

        let upload = self
            .create_upload_info(NewUpload::new(length, metadata).partial())
            .await?;
        self.resume_at(&upload.url, source).await
    }

    async fn creation_capabilities(&self, length: u64) -> Option<ServerCapabilities> {
        if length == 0 || length > self.max_initial_upload_size as u64 {
            return None;
        }

        self.server_capabilities().await.ok()
    }

    fn should_use_creation_with_upload(
        &self,
        length: u64,
        capabilities: Option<&ServerCapabilities>,
    ) -> bool {
        length > 0
            && length <= self.max_initial_upload_size as u64
            && capabilities
                .map(|capabilities| capabilities.has_extension("creation-with-upload"))
                .unwrap_or(false)
    }

    async fn read_source_exact<S>(source: &mut S, offset: u64, length: u64) -> Result<Vec<u8>>
    where
        S: UploadSource,
    {
        let capacity = usize::try_from(length).map_err(|_| {
            Error::Transport(format!(
                "source range length {length} does not fit in memory"
            ))
        })?;
        let mut body = Vec::with_capacity(capacity);

        while body.len() < capacity {
            let current = offset + body.len() as u64;
            let remaining = capacity - body.len();
            let chunk = source.read_chunk(current, remaining).await?;
            if chunk.is_empty() {
                return Err(Error::OffsetBeyondSource {
                    offset: current,
                    source_len: source.len(),
                });
            }
            if chunk.len() > remaining {
                return Err(Error::Transport(format!(
                    "source returned {} bytes for a {remaining}-byte read",
                    chunk.len()
                )));
            }
            body.extend(chunk);
        }

        Ok(body)
    }

    async fn patch_source<S, P>(
        &self,
        upload_url: &url::Url,
        source: &mut S,
        offset: u64,
        total_length: u64,
        progress: &mut P,
    ) -> Result<()>
    where
        S: UploadSource,
        P: UploadProgress,
    {
        let mut sent = offset;
        let mut remaining = total_length - offset;

        while remaining > 0 {
            let chunk_len = remaining.min(self.max_chunk_size as u64) as usize;
            let chunk = source.read_chunk(sent, chunk_len).await?;
            if chunk.is_empty() {
                return Err(Error::OffsetBeyondSource {
                    offset: sent,
                    source_len: total_length,
                });
            }
            if chunk.len() > chunk_len {
                return Err(Error::Transport(format!(
                    "source returned {} bytes for a {chunk_len}-byte read",
                    chunk.len()
                )));
            }
            let new_offset = self
                .send_upload_chunk_at(upload_url, chunk, sent, "patch upload")
                .await?;
            validate_patch_advance(sent, new_offset, total_length)?;
            sent = new_offset;
            remaining = total_length.saturating_sub(sent);
            progress.on_progress(sent, total_length);
        }

        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn sleep_before_retry(base: std::time::Duration, attempt: usize) {
    tokio::time::sleep(jittered_backoff_delay(base, attempt)).await;
}

#[cfg(target_arch = "wasm32")]
async fn sleep_before_retry(base: std::time::Duration, attempt: usize) {
    gloo_timers::future::sleep(jittered_backoff_delay(base, attempt)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Client;
    use crate::client::test_support::*;
    use crate::{Error, TransportBody};
    use http::Method;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    #[cfg(not(target_arch = "wasm32"))]
    use tokio::time::sleep;
    use tus_protocol::UploadMetadata;

    #[cfg(not(target_arch = "wasm32"))]
    use tokio::test as async_test;
    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as async_test;

    #[async_test]
    async fn upload_creation_with_upload_collects_short_source_reads() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                204,
                header_map(&[
                    ("tus-version", "1.0.0"),
                    ("tus-extension", "creation-with-upload"),
                ]),
                Vec::new(),
            )));
            responses.push_back(Ok(transport_response(
                201,
                header_map(&[("location", "/files/upload-1"), ("upload-offset", "5")]),
                Vec::new(),
            )));
        }
        let source = ShortReadSource {
            bytes: b"hello".to_vec(),
            max_read: 2,
        };
        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_max_initial_upload_size(1024);

        let upload = client
            .upload_from(source, UploadMetadata::new())
            .await
            .unwrap();

        assert_eq!(upload.offset, 5);
        let requests = transport.requests.lock().unwrap();
        let body = match requests.last().unwrap().body() {
            TransportBody::Bytes(bytes) => bytes,
            other => panic!("expected byte body, got {other:?}"),
        };
        assert_eq!(body, b"hello");
    }

    #[async_test]
    async fn upload_creation_with_upload_resumes_when_initial_post_is_partial() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                204,
                header_map(&[
                    ("tus-version", "1.0.0"),
                    ("tus-extension", "creation-with-upload"),
                ]),
                Vec::new(),
            )));
            responses.push_back(Ok(transport_response(
                201,
                header_map(&[("location", "/files/upload-1"), ("upload-offset", "2")]),
                Vec::new(),
            )));
            responses.push_back(Ok(mock_head_response(2, 5)));
            responses.push_back(Ok(mock_patch_response(5)));
            responses.push_back(Ok(mock_head_response(5, 5)));
        }
        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_max_initial_upload_size(1024);

        let upload = client
            .upload_from(b"hello".to_vec(), UploadMetadata::new())
            .await
            .unwrap();

        assert_eq!(upload.offset, 5);
        let requests = transport.requests.lock().unwrap();
        let methods: Vec<_> = requests
            .iter()
            .map(|request| request.method().clone())
            .collect();
        assert_eq!(
            methods,
            vec![
                Method::OPTIONS,
                Method::POST,
                Method::HEAD,
                Method::PATCH,
                Method::HEAD
            ]
        );
        let patch = requests
            .iter()
            .find(|request| request.method() == Method::PATCH)
            .expect("PATCH request");
        assert_eq!(body_bytes(patch.body()).as_slice(), b"llo");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_test]
    async fn upload_parallel_reuses_capabilities_for_partial_uploads() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                204,
                header_map(&[
                    ("tus-version", "1.0.0"),
                    ("tus-extension", "creation-with-upload"),
                ]),
                Vec::new(),
            )));
            responses.push_back(Ok(transport_response(
                201,
                header_map(&[("location", "/files/part-1"), ("upload-offset", "4")]),
                Vec::new(),
            )));
            responses.push_back(Ok(transport_response(
                201,
                header_map(&[("location", "/files/part-2"), ("upload-offset", "4")]),
                Vec::new(),
            )));
            responses.push_back(Ok(transport_response(
                201,
                header_map(&[("location", "/files/final")]),
                Vec::new(),
            )));
            responses.push_back(Ok(mock_head_response(8, 8)));
        }
        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_max_initial_upload_size(1024);

        let upload = client
            .upload_parallel(
                b"abcdefgh".to_vec(),
                UploadMetadata::new(),
                ParallelUpload::new(4).with_max_concurrency(2),
            )
            .await
            .unwrap();

        assert_eq!(upload.offset, 8);
        let requests = transport.requests.lock().unwrap();
        let options_count = requests
            .iter()
            .filter(|request| request.method() == Method::OPTIONS)
            .count();
        assert_eq!(options_count, 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[derive(Clone, Default)]
    struct ActiveCountingTransport {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        partial_uploads: Arc<AtomicUsize>,
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[cfg_attr(not(feature = "local-futures"), async_trait)]
    #[cfg_attr(feature = "local-futures", async_trait(?Send))]
    impl Transport for ActiveCountingTransport {
        async fn send(&self, request: crate::TransportRequest) -> Result<crate::TransportResponse> {
            let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
            self.max_active.fetch_max(active, Ordering::Relaxed);
            sleep(Duration::from_millis(10)).await;
            self.active.fetch_sub(1, Ordering::Relaxed);

            match *request.method() {
                Method::OPTIONS => Ok(transport_response(
                    204,
                    header_map(&[
                        ("tus-version", "1.0.0"),
                        ("tus-extension", "creation-with-upload"),
                    ]),
                    Vec::new(),
                )),
                Method::POST => {
                    let concat = request
                        .headers()
                        .get("upload-concat")
                        .and_then(|value| value.to_str().ok());
                    if concat == Some("partial") {
                        let index = self.partial_uploads.fetch_add(1, Ordering::Relaxed) + 1;
                        let body_len = body_bytes(request.body()).len().to_string();
                        let location = format!("/files/part-{index}");
                        let mut headers = http::HeaderMap::new();
                        headers.insert(
                            http::header::LOCATION,
                            http::HeaderValue::from_str(&location).unwrap(),
                        );
                        headers.insert(
                            http::HeaderName::from_static("upload-offset"),
                            http::HeaderValue::from_str(&body_len).unwrap(),
                        );
                        Ok(transport_response(201, headers, Vec::new()))
                    } else {
                        Ok(transport_response(
                            201,
                            header_map(&[("location", "/files/final")]),
                            Vec::new(),
                        ))
                    }
                }
                Method::HEAD => Ok(mock_head_response(4, 4)),
                _ => Err(Error::Transport(format!(
                    "unexpected method {}",
                    request.method()
                ))),
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_test]
    async fn upload_parallel_obeys_max_concurrency() {
        let transport = ActiveCountingTransport::default();
        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_max_initial_upload_size(1024);

        let upload = client
            .upload_parallel(
                b"abcd".to_vec(),
                UploadMetadata::new(),
                ParallelUpload::new(2).with_max_concurrency(1),
            )
            .await
            .unwrap();

        assert_eq!(upload.offset, 4);
        assert_eq!(transport.partial_uploads.load(Ordering::Relaxed), 2);
        assert_eq!(transport.max_active.load(Ordering::Relaxed), 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_test]
    async fn upload_parallel_collects_short_source_reads_for_parts() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                204,
                header_map(&[
                    ("tus-version", "1.0.0"),
                    ("tus-extension", "creation-with-upload"),
                ]),
                Vec::new(),
            )));
            responses.push_back(Ok(transport_response(
                201,
                header_map(&[("location", "/files/part-1"), ("upload-offset", "4")]),
                Vec::new(),
            )));
            responses.push_back(Ok(transport_response(
                201,
                header_map(&[("location", "/files/final")]),
                Vec::new(),
            )));
            responses.push_back(Ok(mock_head_response(4, 4)));
        }
        let source = ShortReadSource {
            bytes: b"data".to_vec(),
            max_read: 1,
        };
        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_max_initial_upload_size(1024);

        let upload = client
            .upload_parallel(source, UploadMetadata::new(), ParallelUpload::new(8))
            .await
            .unwrap();

        assert_eq!(upload.offset, 4);
        let requests = transport.requests.lock().unwrap();
        let partial_post = requests
            .iter()
            .find(|request| {
                request
                    .headers()
                    .get("upload-concat")
                    .and_then(|v| v.to_str().ok())
                    == Some("partial")
            })
            .expect("partial POST");
        let body = match partial_post.body() {
            TransportBody::Bytes(bytes) => bytes,
            other => panic!("expected byte body, got {other:?}"),
        };
        assert_eq!(body, b"data");
    }

    #[async_test]
    async fn resume_at_rejects_patch_non_204_success_status() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(transport_response(
                200,
                header_map(&[("Upload-Offset", "4")]),
                Vec::new(),
            )));
            responses.push_back(Ok(mock_head_response(4, 4)));
        }

        let client = Client::with_transport(endpoint_url(), transport);
        let result = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await;

        expect_unexpected_status(result, "patch upload", 200);
    }

    #[async_test]
    async fn retry_hook_runs_before_retrying_server_errors() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(transport_response(
                503,
                http::HeaderMap::new(),
                b"temporary failure".to_vec(),
            )));
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(mock_patch_response(4)));
            responses.push_back(Ok(mock_head_response(4, 4)));
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let client = Client::with_transport(endpoint_url(), transport)
            .with_retry_hook({
                let calls = calls.clone();
                move |_attempt: usize, _error: &Error| {
                    calls.fetch_add(1, Ordering::Relaxed);
                    std::future::ready(Ok::<bool, Error>(true))
                }
            })
            .with_max_retries(1)
            .with_retry_delay(Duration::from_millis(0));

        let upload = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await
            .unwrap();

        assert_eq!(upload.offset, 4);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    /// 408 Request Timeout and 429 Too Many Requests are transient and
    /// PATCH is idempotent under TUS, so they MUST be retried — pre-fix
    /// the native classifier matched only `>= 500`, leaving native callers
    /// stuck on rate-limited servers while the wasm classifier in
    /// `dioxus-tus` would have retried with backoff.
    #[async_test]
    async fn resume_at_retries_429_too_many_requests() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(transport_response(
                429,
                http::HeaderMap::new(),
                b"slow down".to_vec(),
            )));
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(mock_patch_response(4)));
            responses.push_back(Ok(mock_head_response(4, 4)));
        }

        let client = Client::with_transport(endpoint_url(), transport)
            .with_max_retries(1)
            .with_retry_delay(Duration::from_millis(0));

        let upload = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await
            .expect("429 must be retried");
        assert_eq!(upload.offset, 4);
    }

    #[async_test]
    async fn resume_at_retries_408_request_timeout() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(transport_response(
                408,
                http::HeaderMap::new(),
                b"timeout".to_vec(),
            )));
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(mock_patch_response(4)));
            responses.push_back(Ok(mock_head_response(4, 4)));
        }

        let client = Client::with_transport(endpoint_url(), transport)
            .with_max_retries(1)
            .with_retry_delay(Duration::from_millis(0));

        let upload = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await
            .expect("408 must be retried");
        assert_eq!(upload.offset, 4);
    }

    #[async_test]
    async fn resume_at_retries_409_conflict_after_reheading() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(transport_response(
                409,
                http::HeaderMap::new(),
                b"offset mismatch".to_vec(),
            )));
            responses.push_back(Ok(mock_head_response(2, 4)));
            responses.push_back(Ok(mock_patch_response(4)));
            responses.push_back(Ok(mock_head_response(4, 4)));
        }

        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_max_retries(1)
            .with_retry_delay(Duration::from_millis(0));

        let upload = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await
            .expect("409 offset mismatch should be recovered after HEAD");
        assert_eq!(upload.offset, 4);

        let requests = transport.requests.lock().unwrap();
        let methods: Vec<_> = requests.iter().map(|req| req.method().clone()).collect();
        assert_eq!(
            methods,
            vec![
                Method::HEAD,
                Method::PATCH,
                Method::HEAD,
                Method::PATCH,
                Method::HEAD
            ]
        );
        assert_eq!(
            requests[3]
                .headers()
                .get("upload-offset")
                .and_then(|value| value.to_str().ok()),
            Some("2"),
        );
    }

    #[async_test]
    async fn resume_at_retries_custom_transport_failure() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Err(Error::Transport("connection reset".into())));
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(mock_patch_response(4)));
            responses.push_back(Ok(mock_head_response(4, 4)));
        }

        let client = Client::with_transport(endpoint_url(), transport)
            .with_max_retries(1)
            .with_retry_delay(Duration::from_millis(0));

        let upload = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await
            .expect("custom transport failures should be retried");
        assert_eq!(upload.offset, 4);
    }

    #[async_test]
    async fn resume_at_retries_transient_recovery_head_failure() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(transport_response(
                503,
                http::HeaderMap::new(),
                b"temporary patch failure".to_vec(),
            )));
            responses.push_back(Ok(transport_response(
                503,
                http::HeaderMap::new(),
                b"temporary head failure".to_vec(),
            )));
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(mock_patch_response(4)));
            responses.push_back(Ok(mock_head_response(4, 4)));
        }

        let client = Client::with_transport(endpoint_url(), transport)
            .with_max_retries(2)
            .with_retry_delay(Duration::from_millis(0));

        let upload = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await
            .expect("transient recovery HEAD failures should be retried");
        assert_eq!(upload.offset, 4);
    }

    #[async_test]
    async fn resume_at_final_retry_does_not_report_partial_head_as_success() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(transport_response(
                503,
                http::HeaderMap::new(),
                b"temporary failure".to_vec(),
            )));
            responses.push_back(Ok(mock_head_response(0, 4)));
        }

        let client = Client::with_transport(endpoint_url(), transport).with_max_retries(0);

        let result = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await;

        assert!(matches!(
            result,
            Err(Error::UnexpectedResponse { status: 503, .. })
        ));
    }

    #[async_test]
    async fn resume_at_final_retry_recovers_completed_408() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(transport_response(
                408,
                http::HeaderMap::new(),
                b"timeout after commit".to_vec(),
            )));
            responses.push_back(Ok(mock_head_response(4, 4)));
        }

        let client = Client::with_transport(endpoint_url(), transport).with_max_retries(0);

        let upload = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await
            .expect("final 408 should recover if HEAD shows completion");
        assert_eq!(upload.offset, 4);
    }

    #[async_test]
    async fn resume_at_rejects_non_advancing_patch_offset() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(mock_patch_response(0)));
        }

        let client = Client::with_transport(endpoint_url(), transport).with_max_chunk_size(4);

        let result = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await;

        match result {
            Err(Error::Transport(message)) => {
                assert!(
                    message.contains("did not advance"),
                    "unexpected error: {message}"
                );
            }
            other => panic!("expected non-advancing offset error, got {other:?}"),
        }
    }

    #[async_test]
    async fn resume_at_rejects_patch_offset_beyond_length() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(mock_patch_response(5)));
            responses.push_back(Ok(mock_head_response(5, 4)));
        }

        let client = Client::with_transport(endpoint_url(), transport).with_max_chunk_size(4);

        let result = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await;

        assert!(matches!(
            result,
            Err(Error::OffsetBeyondSource {
                offset: 5,
                source_len: 4,
            })
        ));
    }

    #[async_test]
    async fn resume_at_rejects_source_chunks_larger_than_requested() {
        #[derive(Clone)]
        struct OversizedSource;

        #[cfg_attr(
            all(not(feature = "local-futures"), not(target_arch = "wasm32")),
            async_trait
        )]
        #[cfg_attr(
            any(feature = "local-futures", target_arch = "wasm32"),
            async_trait(?Send)
        )]
        impl UploadSource for OversizedSource {
            fn len(&self) -> u64 {
                4
            }

            async fn read_chunk(&mut self, _offset: u64, max_len: usize) -> Result<Vec<u8>> {
                Ok(vec![b'x'; max_len + 1])
            }
        }

        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(mock_head_response(0, 4)));
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(mock_head_response(0, 4)));
        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_max_chunk_size(4)
            .with_max_retries(0);

        let result = client
            .resume_at(&upload_url("upload-1"), OversizedSource)
            .await;

        match result {
            Err(Error::Transport(message)) => {
                assert!(message.contains("source returned 5 bytes for a 4-byte read"));
            }
            other => panic!("expected oversized source chunk error, got {other:?}"),
        }
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| request.method() == Method::HEAD)
        );
    }

    #[async_test]
    async fn resume_at_continues_from_partial_patch_offset() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(mock_patch_response(2)));
            responses.push_back(Ok(mock_patch_response(4)));
            responses.push_back(Ok(mock_head_response(4, 4)));
        }

        let client =
            Client::with_transport(endpoint_url(), transport.clone()).with_max_chunk_size(4);

        let upload = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await
            .expect("partial offset advance should seek and continue");
        assert_eq!(upload.offset, 4);

        let requests = transport.requests.lock().unwrap();
        let patch_bodies: Vec<Vec<u8>> = requests
            .iter()
            .filter(|req| req.method() == Method::PATCH)
            .map(|req| match req.body() {
                TransportBody::Bytes(bytes) => bytes.clone(),
                other => panic!("expected byte body, got {other:?}"),
            })
            .collect();
        assert_eq!(patch_bodies, vec![b"data".to_vec(), b"ta".to_vec()]);
    }
}
