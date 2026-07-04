use async_trait::async_trait;

#[cfg(all(feature = "source-file", not(target_arch = "wasm32")))]
use std::path::{Path, PathBuf};

#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinSet;
#[cfg(all(feature = "source-file", not(target_arch = "wasm32")))]
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use super::{Client, NewUpload, ServerCapabilities, UploadInfo};
use crate::error::Error;
use crate::error::Result;

use crate::helpers::{jittered_backoff_delay, validate_patch_advance, validate_remote_for_resume};
use crate::runtime::MaybeSend;

use crate::transport::Transport;
use tus_protocol::UploadMetadata;

/// Offset-addressable upload content.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

/// Path-backed upload content for native Tokio applications.
///
/// The file is opened once at [`FileSource::open`] time and the handle is
/// reused for every chunk read. If the file's length changes after open,
/// reads fail with a permanent [`Error::Source`] instead of silently
/// uploading torn content.
#[cfg(all(feature = "source-file", not(target_arch = "wasm32")))]
#[derive(Debug)]
pub struct FileSource {
    file: File,
    len: u64,
    path: PathBuf,
}

#[cfg(all(feature = "source-file", not(target_arch = "wasm32")))]
impl FileSource {
    /// Opens a file source and records its current length.
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = File::open(&path).await?;
        let len = file.metadata().await?.len();
        Ok(Self { file, len, path })
    }

    /// Returns the path read by this source.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(all(feature = "source-file", not(target_arch = "wasm32")))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl UploadSource for FileSource {
    fn len(&self) -> u64 {
        self.len
    }

    async fn read_chunk(&mut self, offset: u64, max_len: usize) -> Result<Vec<u8>> {
        if max_len == 0 {
            return Ok(Vec::new());
        }

        let current_len = self.file.metadata().await?.len();
        if current_len != self.len {
            return Err(Error::Source {
                message: format!(
                    "file {} changed length from {} to {current_len} after open",
                    self.path.display(),
                    self.len,
                ),
            });
        }

        self.file.seek(std::io::SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0; max_len];
        let read = self.file.read(&mut buffer).await?;
        buffer.truncate(read);
        Ok(buffer)
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
        let capabilities = self.creation_capabilities(length).await?;

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

        let (handle, _info) = self.create_upload(NewUpload::new(length, metadata)).await?;
        handle.upload_with_progress(source, progress).await
    }

    /// Uploads a source as multiple partial uploads and concatenates them.
    ///
    /// Requires the server to advertise the `concatenation` extension; the
    /// call fails fast with [`Error::UnsupportedExtension`] before creating
    /// any partial uploads otherwise. If a part fails mid-flight, partial
    /// uploads that were already created are terminated on a best-effort
    /// basis before the error is returned.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn upload_parallel<S>(
        &self,
        mut source: S,
        metadata: impl Into<UploadMetadata>,
        options: ParallelUpload,
    ) -> Result<UploadInfo>
    where
        S: UploadSource,
    {
        let source_length = source.len();
        if source_length == 0 {
            return self.upload_from(source, metadata).await;
        }

        let metadata = metadata.into();
        let capabilities = self.server_capabilities().await?;
        if !capabilities.has_extension("concatenation") {
            return Err(Error::UnsupportedExtension("concatenation"));
        }
        let capabilities = Some(capabilities);
        let part_count = source_length.div_ceil(options.part_size as u64) as usize;
        let max_concurrency = options.max_concurrency.min(part_count).max(1);

        let mut join_set: JoinSet<Result<(usize, String)>> = JoinSet::new();
        let mut part_urls: Vec<Option<String>> = vec![None; part_count];
        let mut next_index = 0;
        let mut failure: Option<Error> = None;

        let spawn_part =
            |join_set: &mut JoinSet<Result<(usize, String)>>, index: usize, bytes: Vec<u8>| {
                let client = self.clone();
                let capabilities = capabilities.clone();
                join_set.spawn(async move {
                    client
                        .upload_parallel_part(index, bytes, capabilities)
                        .await
                });
            };

        while next_index < part_count && join_set.len() < max_concurrency {
            match Self::read_parallel_part(
                &mut source,
                next_index,
                source_length,
                options.part_size,
            )
            .await
            {
                Ok(bytes) => {
                    spawn_part(&mut join_set, next_index, bytes);
                    next_index += 1;
                }
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }

        while failure.is_none() {
            match join_set.join_next().await {
                None => break,
                Some(Err(join_error)) => {
                    failure = Some(Error::Internal(format!(
                        "parallel upload task failed: {join_error}"
                    )));
                }
                Some(Ok(Err(error))) => failure = Some(error),
                Some(Ok(Ok((index, url)))) => {
                    part_urls[index] = Some(url);

                    if next_index < part_count {
                        match Self::read_parallel_part(
                            &mut source,
                            next_index,
                            source_length,
                            options.part_size,
                        )
                        .await
                        {
                            Ok(bytes) => {
                                spawn_part(&mut join_set, next_index, bytes);
                                next_index += 1;
                            }
                            Err(error) => failure = Some(error),
                        }
                    }
                }
            }
        }

        if let Some(error) = failure {
            join_set.abort_all();
            // Collect parts that still completed so cleanup can cover them.
            while let Some(result) = join_set.join_next().await {
                if let Ok(Ok((index, url))) = result {
                    part_urls[index] = Some(url);
                }
            }
            self.cleanup_partial_uploads(part_urls).await;
            return Err(error);
        }

        let part_urls: Vec<String> = part_urls
            .into_iter()
            .map(|url| url.expect("every joined part records its upload URL"))
            .collect();
        self.concatenate_uploads(&part_urls, metadata).await
    }

    /// Best-effort termination of partial uploads left behind by a failed
    /// parallel upload. Cleanup errors are ignored: the caller's original
    /// failure is the actionable one, and servers expire stray partials.
    #[cfg(not(target_arch = "wasm32"))]
    async fn cleanup_partial_uploads(&self, part_urls: Vec<Option<String>>) {
        for url in part_urls.into_iter().flatten() {
            if let Ok(url) = url::Url::parse(&url) {
                let _ = self.terminate_upload_at(&url).await;
            }
        }
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

        // The retry budget covers *consecutive attempts without progress*:
        // whenever the server offset advances, the counter resets, because
        // progress proves the link works and a long transfer should not run
        // out of budget from unrelated hiccups spread across its lifetime.
        let mut attempt = 0;
        let mut last_observed_offset: Option<u64> = None;

        loop {
            let remote = match self.upload_info_at(upload_url).await {
                Ok(remote) => remote,
                Err(error) => {
                    if !error.is_retryable() || attempt == self.max_retries {
                        return Err(error);
                    }
                    self.consult_retry_hook(attempt + 1, error).await?;
                    sleep_before_retry(self.retry_delay, attempt).await;
                    attempt += 1;
                    continue;
                }
            };
            validate_remote_for_resume(&remote, source_length)?;

            if remote.offset == source_length {
                return Ok(remote);
            }

            if last_observed_offset.is_some_and(|previous| remote.offset > previous) {
                attempt = 0;
            }
            last_observed_offset = Some(
                last_observed_offset.map_or(remote.offset, |previous| previous.max(remote.offset)),
            );

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
                            if !error.is_retryable() || attempt == self.max_retries {
                                return Err(error);
                            }
                            self.consult_retry_hook(attempt + 1, error).await?;
                            sleep_before_retry(self.retry_delay, attempt).await;
                            attempt += 1;
                            continue;
                        }
                    };
                    validate_remote_for_resume(&remote, source_length)?;
                    if remote.offset == source_length {
                        return Ok(remote);
                    }
                    return Err(Error::OffsetDesync {
                        expected: source_length,
                        actual: remote.offset,
                    });
                }
                Err(error) => {
                    if !error.is_retryable() {
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

                    self.consult_retry_hook(attempt + 1, error).await?;
                }
            }

            sleep_before_retry(self.retry_delay, attempt).await;
            attempt += 1;
        }
    }

    /// Consults the configured retry hook before the next attempt.
    ///
    /// Returns `Err` with the *original* upload error both when the hook
    /// vetoes the retry and when the hook itself fails — a broken hook must
    /// not mask the error that triggered the retry, so its own error is
    /// discarded.
    async fn consult_retry_hook(&self, attempt: usize, error: Error) -> Result<()> {
        let Some(hook) = &self.retry_hook else {
            return Ok(());
        };
        match hook.before_retry(attempt, &error).await {
            Ok(true) => Ok(()),
            Ok(false) | Err(_) => Err(error),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_parallel_part<S>(
        source: &mut S,
        index: usize,
        source_length: u64,
        part_size: usize,
    ) -> Result<Vec<u8>>
    where
        S: UploadSource,
    {
        let start = index as u64 * part_size as u64;
        let length = (source_length - start).min(part_size as u64);
        Self::read_source_exact(source, start, length).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn upload_parallel_part(
        &self,
        index: usize,
        bytes: Vec<u8>,
        capabilities: Option<ServerCapabilities>,
    ) -> Result<(usize, String)> {
        let upload = self
            .upload_partial_with_capabilities(bytes, UploadMetadata::new(), capabilities.as_ref())
            .await?;

        Ok((index, upload.url.to_string()))
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

    /// Fetches server capabilities ahead of creation when the source is
    /// small enough for creation-with-upload to matter.
    ///
    /// Auth-style OPTIONS failures (401/403/407) are propagated so callers
    /// see the real problem instead of a doomed follow-up POST. Any other
    /// failure (endpoint without OPTIONS support, non-TUS proxy) degrades
    /// to "no known capabilities".
    async fn creation_capabilities(&self, length: u64) -> Result<Option<ServerCapabilities>> {
        if length == 0 || length > self.max_initial_upload_size as u64 {
            return Ok(None);
        }

        match self.server_capabilities().await {
            Ok(capabilities) => Ok(Some(capabilities)),
            Err(
                error @ Error::UnexpectedResponse {
                    status: 401 | 403 | 407,
                    ..
                },
            ) => Err(error),
            Err(_) => Ok(None),
        }
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
        let capacity = usize::try_from(length).map_err(|_| Error::Source {
            message: format!("source range length {length} does not fit in memory"),
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
                return Err(Error::Source {
                    message: format!(
                        "source returned {} bytes for a {remaining}-byte read",
                        chunk.len()
                    ),
                });
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
                return Err(Error::Source {
                    message: format!(
                        "source returned {} bytes for a {chunk_len}-byte read",
                        chunk.len()
                    ),
                });
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
                    ("tus-extension", "creation-with-upload,concatenation"),
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
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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
                        ("tus-extension", "creation-with-upload,concatenation"),
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
                    ("tus-extension", "creation-with-upload,concatenation"),
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

    #[cfg(not(target_arch = "wasm32"))]
    struct NonCloneSource<'a> {
        bytes: &'a [u8],
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl UploadSource for NonCloneSource<'_> {
        fn len(&self) -> u64 {
            self.bytes.len() as u64
        }

        async fn read_chunk(&mut self, offset: u64, max_len: usize) -> Result<Vec<u8>> {
            let offset = offset as usize;
            let Some(bytes) = self.bytes.get(offset..) else {
                return Ok(Vec::new());
            };
            let len = bytes.len().min(max_len);
            Ok(bytes[..len].to_vec())
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_test]
    async fn upload_parallel_accepts_non_clone_sources() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                204,
                header_map(&[
                    ("tus-version", "1.0.0"),
                    ("tus-extension", "creation-with-upload,concatenation"),
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
        let bytes = *b"abcdefgh";
        let source = NonCloneSource { bytes: &bytes };
        let client =
            Client::with_transport(endpoint_url(), transport).with_max_initial_upload_size(1024);

        let upload = client
            .upload_parallel(
                source,
                UploadMetadata::new(),
                ParallelUpload::new(4).with_max_concurrency(2),
            )
            .await
            .unwrap();

        assert_eq!(upload.offset, 8);
    }

    #[cfg(all(feature = "source-file", not(target_arch = "wasm32")))]
    #[async_test]
    async fn file_source_reads_offset_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upload.bin");
        tokio::fs::write(&path, b"abcdef").await.unwrap();

        let mut source = FileSource::open(&path).await.unwrap();

        assert_eq!(source.len(), 6);
        assert_eq!(source.path(), path.as_path());
        assert_eq!(source.read_chunk(2, 3).await.unwrap(), b"cde");
        assert_eq!(source.read_chunk(6, 3).await.unwrap(), b"");
    }

    #[cfg(all(feature = "source-file", not(target_arch = "wasm32")))]
    #[async_test]
    async fn upload_parallel_accepts_file_sources() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upload.bin");
        tokio::fs::write(&path, b"abcdefgh").await.unwrap();

        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                204,
                header_map(&[
                    ("tus-version", "1.0.0"),
                    ("tus-extension", "creation-with-upload,concatenation"),
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
        let source = FileSource::open(&path).await.unwrap();
        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_max_initial_upload_size(1024);

        let upload = client
            .upload_parallel(
                source,
                UploadMetadata::new(),
                ParallelUpload::new(4).with_max_concurrency(2),
            )
            .await
            .unwrap();

        assert_eq!(upload.offset, 8);
        let requests = transport.requests.lock().unwrap();
        let mut bodies: Vec<Vec<u8>> = requests
            .iter()
            .filter(|request| {
                request
                    .headers()
                    .get("upload-concat")
                    .and_then(|value| value.to_str().ok())
                    == Some("partial")
            })
            .map(|request| body_bytes(request.body()).clone())
            .collect();
        bodies.sort();
        assert_eq!(bodies, vec![b"abcd".to_vec(), b"efgh".to_vec()]);
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

        assert!(matches!(
            result,
            Err(Error::OffsetDesync {
                expected: 1,
                actual: 0,
            })
        ));
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

        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        #[cfg_attr(
            target_arch = "wasm32",
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
        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_max_chunk_size(4)
            .with_max_retries(3);

        let result = client
            .resume_at(&upload_url("upload-1"), OversizedSource)
            .await;

        match result {
            Err(Error::Source { message }) => {
                assert!(message.contains("source returned 5 bytes for a 4-byte read"));
            }
            other => panic!("expected oversized source chunk error, got {other:?}"),
        }
        // A misbehaving source is a deterministic bug: no retry, no
        // recovery HEAD — the initial HEAD must be the only request.
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests.first().unwrap().method(), Method::HEAD);
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

    /// The retry budget must reset whenever the server offset advances:
    /// three transient failures spread across a transfer that keeps making
    /// progress must not exhaust a `max_retries` of 1.
    #[async_test]
    async fn resume_at_resets_retry_budget_when_offset_advances() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 6)));
            responses.push_back(Ok(transport_response(
                503,
                http::HeaderMap::new(),
                b"failure 1".to_vec(),
            )));
            responses.push_back(Ok(mock_head_response(2, 6)));
            responses.push_back(Ok(transport_response(
                503,
                http::HeaderMap::new(),
                b"failure 2".to_vec(),
            )));
            responses.push_back(Ok(mock_head_response(4, 6)));
            responses.push_back(Ok(transport_response(
                503,
                http::HeaderMap::new(),
                b"failure 3".to_vec(),
            )));
            responses.push_back(Ok(mock_head_response(6, 6)));
        }

        let client = Client::with_transport(endpoint_url(), transport)
            .with_max_chunk_size(2)
            .with_max_retries(1)
            .with_retry_delay(Duration::from_millis(0));

        let upload = client
            .resume_at(&upload_url("upload-1"), b"abcdef".to_vec())
            .await
            .expect("progress between failures must reset the retry budget");
        assert_eq!(upload.offset, 6);
    }

    /// A failing retry hook must not mask the upload error that triggered
    /// the retry.
    #[async_test]
    async fn retry_hook_failure_returns_original_upload_error() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(transport_response(
                503,
                http::HeaderMap::new(),
                b"temporary failure".to_vec(),
            )));
        }

        let client = Client::with_transport(endpoint_url(), transport)
            .with_retry_hook(|_attempt: usize, _error: &Error| {
                std::future::ready(Err::<bool, Error>(Error::Internal("hook broke".into())))
            })
            .with_max_retries(3)
            .with_retry_delay(Duration::from_millis(0));

        let result = client
            .resume_at(&upload_url("upload-1"), b"data".to_vec())
            .await;

        assert!(
            matches!(result, Err(Error::UnexpectedResponse { status: 503, .. })),
            "hook failure must surface the original 503, got {result:?}"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_test]
    async fn upload_parallel_requires_concatenation_extension() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                204,
                header_map(&[
                    ("tus-version", "1.0.0"),
                    ("tus-extension", "creation,creation-with-upload"),
                ]),
                Vec::new(),
            )));
        let client = Client::with_transport(endpoint_url(), transport.clone());

        let result = client
            .upload_parallel(
                b"abcdefgh".to_vec(),
                UploadMetadata::new(),
                ParallelUpload::new(4),
            )
            .await;

        assert!(matches!(
            result,
            Err(Error::UnsupportedExtension("concatenation"))
        ));
        let requests = transport.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "no partial uploads may be created without concatenation support"
        );
        assert_eq!(requests.first().unwrap().method(), Method::OPTIONS);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_test]
    async fn upload_parallel_terminates_created_partials_when_a_part_fails() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                204,
                header_map(&[
                    ("tus-version", "1.0.0"),
                    ("tus-extension", "creation-with-upload,concatenation"),
                ]),
                Vec::new(),
            )));
            responses.push_back(Ok(transport_response(
                201,
                header_map(&[("location", "/files/part-1"), ("upload-offset", "4")]),
                Vec::new(),
            )));
            responses.push_back(Ok(transport_response(
                400,
                http::HeaderMap::new(),
                b"part rejected".to_vec(),
            )));
            // Best-effort DELETE of the surviving partial.
            responses.push_back(Ok(transport_response(
                204,
                http::HeaderMap::new(),
                Vec::new(),
            )));
        }
        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_max_initial_upload_size(1024)
            .with_max_retries(0);

        let result = client
            .upload_parallel(
                b"abcdefgh".to_vec(),
                UploadMetadata::new(),
                ParallelUpload::new(4).with_max_concurrency(1),
            )
            .await;

        assert!(matches!(
            result,
            Err(Error::UnexpectedResponse { status: 400, .. })
        ));
        let requests = transport.requests.lock().unwrap();
        let delete = requests
            .iter()
            .find(|request| request.method() == Method::DELETE)
            .expect("failed parallel upload must terminate created partials");
        assert_eq!(delete.uri().to_string(), "http://example.test/files/part-1");
    }

    /// An auth failure on the capabilities OPTIONS must surface instead of
    /// being swallowed into "no capabilities" followed by a doomed POST.
    #[async_test]
    async fn upload_from_propagates_options_auth_failures() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                401,
                http::HeaderMap::new(),
                b"missing token".to_vec(),
            )));
        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_max_initial_upload_size(1024);

        let result = client
            .upload_from(b"data".to_vec(), UploadMetadata::new())
            .await;

        assert!(matches!(
            result,
            Err(Error::UnexpectedResponse { status: 401, .. })
        ));
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "no POST may follow a 401 OPTIONS");
    }

    /// Non-auth OPTIONS failures (e.g. an endpoint without OPTIONS support)
    /// still degrade to the plain creation path.
    #[async_test]
    async fn upload_from_falls_back_to_plain_creation_when_options_unsupported() {
        let transport = MockTransport::default();
        {
            let responses = &mut *transport.responses.lock().unwrap();
            responses.push_back(Ok(transport_response(
                405,
                http::HeaderMap::new(),
                b"method not allowed".to_vec(),
            )));
            responses.push_back(Ok(transport_response(
                201,
                header_map(&[("location", "/files/upload-1")]),
                Vec::new(),
            )));
            responses.push_back(Ok(mock_head_response(0, 4)));
            responses.push_back(Ok(mock_patch_response(4)));
            responses.push_back(Ok(mock_head_response(4, 4)));
        }
        let client = Client::with_transport(endpoint_url(), transport.clone())
            .with_max_initial_upload_size(1024);

        let upload = client
            .upload_from(b"data".to_vec(), UploadMetadata::new())
            .await
            .expect("plain creation must proceed without OPTIONS support");
        assert_eq!(upload.offset, 4);
    }

    /// Capabilities are cached per client: repeated probes reuse the first
    /// successful OPTIONS response.
    #[async_test]
    async fn server_capabilities_are_cached_per_client() {
        let transport = MockTransport::default();
        transport
            .responses
            .lock()
            .unwrap()
            .push_back(Ok(transport_response(
                204,
                header_map(&[("tus-version", "1.0.0"), ("tus-extension", "creation")]),
                Vec::new(),
            )));
        let client = Client::with_transport(endpoint_url(), transport.clone());

        let first = client.server_capabilities().await.unwrap();
        let second = client.server_capabilities().await.unwrap();
        let via_clone = client.clone().server_capabilities().await.unwrap();

        assert_eq!(first, second);
        assert_eq!(first, via_clone);
        assert_eq!(
            transport.requests.lock().unwrap().len(),
            1,
            "capability probes after the first must be served from cache"
        );
    }
}
