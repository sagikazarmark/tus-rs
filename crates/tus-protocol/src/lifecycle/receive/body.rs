use std::io;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use http::HeaderMap;

use crate::config::{Config, Extension};
use crate::error::{Error, Result};
use crate::protocol::headers::{UploadChecksum, parse_upload_checksum};
use crate::protocol::{BodyFrame, BodyStream, Headers, RequestBody};
use crate::storage::{ByteStream, ChunkStream};

type BodyChecksum = Option<UploadChecksum>;

#[derive(Debug)]
pub(super) struct IntakeBody {
    pub(super) data: ChunkStream,
    pub(super) size: u64,
    pub(super) supplied: bool,
    pub(super) deferred_error: DeferredBodyError,
}

#[derive(Clone, Debug, Default)]
pub(super) struct DeferredBodyError {
    error: Arc<Mutex<Option<Error>>>,
}

impl DeferredBodyError {
    // The guarded value is a plain `Option<Error>`, always safe to touch, so a
    // poisoned mutex (a panic while another holder — e.g. a storage backend
    // polling the body stream — held the guard) is recovered rather than
    // re-panicked. This keeps a recoverable request failure from escalating
    // into a panic on the request path.
    pub(super) fn take(&self) -> Option<Error> {
        self.error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    fn set(&self, error: Error) {
        *self.error.lock().unwrap_or_else(PoisonError::into_inner) = Some(error);
    }
}

pub(super) async fn prepare(
    config: &Config,
    headers: &Headers,
    body_limit: Option<u64>,
    body: RequestBody,
) -> Result<IntakeBody> {
    let supplied = body.is_supplied();

    validate_before_polling(config, headers, body_limit)?;

    match body {
        RequestBody::Absent => {
            collect_buffered(config, headers, body_limit, supplied, Bytes::new(), None)
        }
        RequestBody::Bytes(bytes) => {
            collect_buffered(config, headers, body_limit, supplied, bytes, None)
        }
        RequestBody::Stream(stream) => {
            if let Some(content_length) = headers.content_length {
                let deferred_error = DeferredBodyError::default();
                Ok(IntakeBody {
                    data: ChunkStream::from_stream(validated_stream(
                        config,
                        headers,
                        body_limit,
                        stream,
                        deferred_error.clone(),
                    )),
                    size: content_length,
                    supplied,
                    deferred_error,
                })
            } else {
                // Without Content-Length (chunked transfer, e.g. checksum
                // trailers) the body size is unknown until the stream is
                // drained, so intake must buffer the whole body in memory to
                // discover its size before committing. Two bounds apply:
                //
                // - `body_limit` already encodes every protocol bound for this
                //   path: the caller folds the per-PATCH `max_chunk_size` cap
                //   into it for PATCH and deliberately excludes it for
                //   Creation-With-Upload (whose initial body travels on the
                //   POST, not a PATCH). Re-reading `max_chunk_size` here would
                //   wrongly re-impose the PATCH cap on a chunked CwU body.
                // - `config.max_intake_buffer()` caps the memory this buffer may
                //   consume regardless of the declared upload length, which is
                //   what stops a large fixed-length upload sent chunked from
                //   buffering gigabytes of RAM (the default `body_limit` for a
                //   PATCH is only `length - offset`, i.e. the whole remainder).
                //
                // Buffer up to the tighter of the two; refuse only when neither
                // bounds it (deferred length with no intake cap).
                let effective_limit = [body_limit, config.max_intake_buffer()]
                    .into_iter()
                    .flatten()
                    .min();
                let Some(effective_limit) = effective_limit else {
                    return Err(Error::LengthRequired);
                };
                let (bytes, trailers) =
                    collect_frames(RequestBody::Stream(stream), Some(effective_limit)).await?;
                collect_buffered(
                    config,
                    headers,
                    Some(effective_limit),
                    supplied,
                    bytes,
                    trailers,
                )
            }
        }
    }
}

fn validated_stream(
    config: &Config,
    headers: &Headers,
    body_limit: Option<u64>,
    stream: BodyStream,
    deferred_error: DeferredBodyError,
) -> ByteStream {
    let state = StreamingBodyState::new(
        stream,
        BodyPolicy {
            config: config.clone(),
            headers: headers.clone(),
            body_limit,
        },
        deferred_error.clone(),
    );

    Box::pin(futures_util::stream::unfold(
        state,
        |mut state| async move { state.next_chunk().await.map(|chunk| (chunk, state)) },
    ))
}

fn collect_buffered(
    config: &Config,
    headers: &Headers,
    body_limit: Option<u64>,
    supplied: bool,
    bytes: Bytes,
    trailers: Option<HeaderMap>,
) -> Result<IntakeBody> {
    enforce_body_limit(0, bytes.len(), body_limit)?;
    let size = bytes.len() as u64;
    validate_content_length(headers, size)?;

    let checksum = effective_checksum(config, headers, trailers.as_ref())?;
    verify_checksum(config, checksum, &bytes)?;

    Ok(IntakeBody {
        data: ChunkStream::Buffered(bytes),
        size,
        supplied,
        deferred_error: DeferredBodyError::default(),
    })
}

async fn collect_frames(
    body: RequestBody,
    body_limit: Option<u64>,
) -> Result<(Bytes, Option<HeaderMap>)> {
    match body {
        RequestBody::Absent => Ok((Bytes::new(), None)),
        RequestBody::Bytes(bytes) => {
            enforce_body_limit(0, bytes.len(), body_limit)?;
            Ok((bytes, None))
        }
        RequestBody::Stream(mut stream) => {
            let mut buffer = BytesMut::new();
            let mut trailers = None;
            while let Some(frame) = stream.next().await {
                match frame.map_err(Error::Io)? {
                    BodyFrame::Data(bytes) => {
                        enforce_body_limit(buffer.len(), bytes.len(), body_limit)?;
                        buffer.extend_from_slice(&bytes);
                    }
                    BodyFrame::Trailers(headers) => {
                        trailers = Some(headers);
                    }
                }
            }
            Ok((buffer.freeze(), trailers))
        }
    }
}

#[derive(Debug, Clone)]
struct BodyPolicy {
    config: Config,
    headers: Headers,
    body_limit: Option<u64>,
}

struct StreamingBodyState {
    stream: BodyStream,
    policy: BodyPolicy,
    bytes_seen: u64,
    trailers: Option<HeaderMap>,
    done: bool,
    deferred_error: DeferredBodyError,
    /// Incremental digests over the streamed body; constant memory per chunk.
    ///
    /// With a header checksum, only that algorithm is hashed. Without one, a
    /// trailer checksum may still arrive with any supported algorithm, so all
    /// configured algorithms are hashed concurrently.
    #[cfg(feature = "checksum")]
    hashers: Vec<crate::checksum::Hasher>,
}

impl StreamingBodyState {
    fn new(stream: BodyStream, policy: BodyPolicy, deferred_error: DeferredBodyError) -> Self {
        #[cfg(feature = "checksum")]
        let hashers = streaming_hashers(&policy);

        Self {
            stream,
            policy,
            bytes_seen: 0,
            trailers: None,
            done: false,
            deferred_error,
            #[cfg(feature = "checksum")]
            hashers,
        }
    }

    async fn next_chunk(&mut self) -> Option<io::Result<Bytes>> {
        if self.done {
            return None;
        }

        loop {
            match self.stream.next().await {
                Some(Ok(BodyFrame::Data(bytes))) => match self.accept_data(&bytes) {
                    Ok(()) => return Some(Ok(bytes)),
                    Err(error) => return Some(self.finish_with_error(error)),
                },
                Some(Ok(BodyFrame::Trailers(headers))) => {
                    self.trailers = Some(headers);
                }
                Some(Err(error)) => {
                    self.done = true;
                    self.deferred_error.set(Error::Internal(error.to_string()));
                    return Some(Err(error));
                }
                None => {
                    self.done = true;
                    return match self.finish() {
                        Ok(()) => None,
                        Err(error) => Some(self.store_validation_error(error)),
                    };
                }
            }
        }
    }

    fn accept_data(&mut self, bytes: &Bytes) -> Result<()> {
        let next_size = self.bytes_seen.saturating_add(bytes.len() as u64);

        if let Some(limit) = self.policy.body_limit
            && next_size > limit
        {
            return Err(Error::SizeExceeded {
                size: next_size,
                max: limit,
            });
        }

        if let Some(content_length) = self.policy.headers.content_length
            && next_size > content_length
        {
            return Err(content_length_mismatch(content_length, next_size));
        }

        self.bytes_seen = next_size;

        #[cfg(feature = "checksum")]
        for hasher in &mut self.hashers {
            hasher.update(bytes);
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        validate_content_length(&self.policy.headers, self.bytes_seen)?;

        let checksum = effective_checksum(
            &self.policy.config,
            &self.policy.headers,
            self.trailers.as_ref(),
        )?;

        #[cfg(feature = "checksum")]
        {
            let hashers = std::mem::take(&mut self.hashers);
            verify_streamed_checksum(&self.policy.config, checksum, hashers)?;
        }

        #[cfg(not(feature = "checksum"))]
        verify_checksum(&self.policy.config, checksum, &[])?;

        Ok(())
    }

    fn finish_with_error(&mut self, error: Error) -> io::Result<Bytes> {
        self.done = true;
        self.store_validation_error(error)
    }

    fn store_validation_error(&self, error: Error) -> io::Result<Bytes> {
        let message = error.to_string();
        self.deferred_error.set(error);
        Err(io::Error::new(io::ErrorKind::InvalidData, message))
    }
}

/// Builds the incremental hashers a streamed body needs for its checksum mode.
#[cfg(feature = "checksum")]
fn streaming_hashers(policy: &BodyPolicy) -> Vec<crate::checksum::Hasher> {
    if !policy.config.has_extension(Extension::Checksum) {
        return Vec::new();
    }

    if let Some(checksum) = &policy.headers.upload_checksum {
        return vec![crate::checksum::Hasher::new(checksum.algorithm)];
    }

    if policy.config.has_extension(Extension::ChecksumTrailer) {
        return policy
            .config
            .checksum_algorithms()
            .into_iter()
            .map(crate::checksum::Hasher::new)
            .collect();
    }

    Vec::new()
}

/// Verifies a streamed body's checksum against its incremental digests.
#[cfg(feature = "checksum")]
fn verify_streamed_checksum(
    config: &Config,
    checksum: BodyChecksum,
    hashers: Vec<crate::checksum::Hasher>,
) -> Result<()> {
    let Some(checksum) = checksum else {
        return Ok(());
    };
    if !config.has_extension(Extension::Checksum) {
        return Ok(());
    }

    let (algorithm, expected) = (checksum.algorithm, checksum.digest);
    validate_checksum_algorithm(config, algorithm)?;
    let calculated = hashers
        .into_iter()
        .find(|hasher| hasher.algorithm() == algorithm)
        .map(crate::checksum::Hasher::finalize)
        .ok_or_else(|| {
            Error::Internal(format!(
                "no streaming digest was computed for checksum algorithm {}",
                algorithm.as_str()
            ))
        })?;
    if calculated != expected {
        use base64::Engine;
        return Err(Error::ChecksumMismatch {
            expected: base64::engine::general_purpose::STANDARD.encode(&expected),
            actual: base64::engine::general_purpose::STANDARD.encode(&calculated),
        });
    }

    Ok(())
}

fn validate_before_polling(
    config: &Config,
    headers: &Headers,
    body_limit: Option<u64>,
) -> Result<()> {
    // Upload-Checksum is only meaningful when the Checksum extension is
    // enabled; otherwise the header (including unparseable values recorded
    // during lenient header parsing) is ignored.
    if config.has_extension(Extension::Checksum) {
        if let Some(deferred) = headers.upload_checksum_error.as_ref() {
            return Err(deferred.to_error());
        }

        if let Some(checksum) = headers.upload_checksum.as_ref() {
            validate_checksum_algorithm(config, checksum.algorithm)?;
        }
    }

    if let Some(content_length) = headers.content_length
        && let Some(limit) = body_limit
        && content_length > limit
    {
        return Err(Error::SizeExceeded {
            size: content_length,
            max: limit,
        });
    }

    Ok(())
}

fn enforce_body_limit(current_len: usize, next_len: usize, body_limit: Option<u64>) -> Result<()> {
    let Some(limit) = body_limit else {
        return Ok(());
    };
    let next_total = (current_len as u64).saturating_add(next_len as u64);
    if next_total > limit {
        return Err(Error::SizeExceeded {
            size: next_total,
            max: limit,
        });
    }

    Ok(())
}

fn validate_content_length(headers: &Headers, actual_len: u64) -> Result<()> {
    if let Some(content_length) = headers.content_length
        && content_length != actual_len
    {
        return Err(content_length_mismatch(content_length, actual_len));
    }

    Ok(())
}

fn content_length_mismatch(content_length: u64, actual_len: u64) -> Error {
    Error::InvalidHeader {
        header: "Content-Length",
        message: format!(
            "declared content length {content_length} does not match body size {actual_len}"
        ),
    }
}

fn effective_checksum(
    config: &Config,
    headers: &Headers,
    trailers: Option<&HeaderMap>,
) -> Result<BodyChecksum> {
    if headers.upload_checksum.is_some() {
        return Ok(headers.upload_checksum.clone());
    }

    if !config.has_extension(Extension::ChecksumTrailer) {
        return Ok(None);
    }

    trailers
        .map(parse_upload_checksum)
        .transpose()
        .map(Option::flatten)
}

fn validate_checksum_algorithm(
    config: &Config,
    algorithm: crate::config::ChecksumAlgorithm,
) -> Result<()> {
    #[cfg(feature = "checksum")]
    {
        if config.has_extension(Extension::Checksum)
            && !config.supports_checksum_algorithm(algorithm)
        {
            return Err(Error::UnsupportedChecksum(algorithm.as_str().to_string()));
        }
    }
    #[cfg(not(feature = "checksum"))]
    let _ = (config, algorithm);

    Ok(())
}

fn verify_checksum(config: &Config, checksum: BodyChecksum, bytes: &[u8]) -> Result<()> {
    #[cfg(feature = "checksum")]
    if let Some(checksum) = checksum {
        if !config.has_extension(Extension::Checksum) {
            return Ok(());
        }

        let (algorithm, expected) = (checksum.algorithm, checksum.digest);
        validate_checksum_algorithm(config, algorithm)?;
        let calculated = crate::checksum::calculate(algorithm, bytes);
        if calculated != expected {
            use base64::Engine;
            return Err(Error::ChecksumMismatch {
                expected: base64::engine::general_purpose::STANDARD.encode(&expected),
                actual: base64::engine::general_purpose::STANDARD.encode(&calculated),
            });
        }
    }

    #[cfg(not(feature = "checksum"))]
    let _ = (config, checksum, bytes);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::error::Error;
    use bytes::Bytes;
    use futures::StreamExt;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    async fn collect_data(data: ChunkStream) -> std::result::Result<Bytes, io::Error> {
        match data {
            ChunkStream::Buffered(bytes) => Ok(bytes),
            ChunkStream::Stream(mut stream) => {
                let mut buffer = BytesMut::new();
                while let Some(chunk) = stream.next().await {
                    buffer.extend_from_slice(&chunk?);
                }
                Ok(buffer.freeze())
            }
        }
    }

    #[tokio::test]
    async fn collection_reports_presence_and_size_facts() {
        let absent = prepare(
            &Config::default(),
            &Headers::default(),
            Some(10),
            RequestBody::absent(),
        )
        .await
        .unwrap();
        assert!(!absent.supplied);
        assert_eq!(absent.size, 0);
        assert!(collect_data(absent.data).await.unwrap().is_empty());

        let supplied = prepare(
            &Config::default(),
            &Headers::default(),
            Some(10),
            RequestBody::empty(),
        )
        .await
        .unwrap();
        assert!(supplied.supplied);
        assert_eq!(supplied.size, 0);
        assert!(collect_data(supplied.data).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn body_limit_is_enforced_while_collecting_stream_without_declared_length() {
        let stream: BodyStream = Box::pin(futures::stream::iter([
            Ok(BodyFrame::Data(Bytes::from_static(b"abc"))),
            Ok(BodyFrame::Data(Bytes::from_static(b"def"))),
        ]));

        let err = prepare(
            &Config::default(),
            &Headers::default(),
            Some(5),
            RequestBody::from_stream(stream),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::SizeExceeded { size: 6, max: 5 }));
    }

    #[tokio::test]
    async fn declared_content_length_over_limit_fails_before_polling_stream() {
        let polled = Arc::new(AtomicBool::new(false));
        let polled_for_stream = Arc::clone(&polled);
        let stream: BodyStream = Box::pin(futures::stream::once(async move {
            polled_for_stream.store(true, Ordering::SeqCst);
            Ok(BodyFrame::Data(Bytes::from_static(b"abcdef")))
        }));
        let headers = Headers {
            content_length: Some(6),
            ..Default::default()
        };

        let err = prepare(
            &Config::default(),
            &headers,
            Some(5),
            RequestBody::from_stream(stream),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::SizeExceeded { size: 6, max: 5 }));
        assert!(!polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn content_length_mismatch_fails_after_buffered_collection() {
        let headers = Headers {
            content_length: Some(4),
            ..Default::default()
        };

        let err = prepare(
            &Config::default(),
            &headers,
            Some(10),
            RequestBody::from_bytes(Bytes::from_static(b"abc")),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            Error::InvalidHeader {
                header: "Content-Length",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn unparseable_upload_checksum_is_ignored_when_checksum_extension_disabled() {
        // Simulates `Upload-Checksum: whirlpool AAAA` arriving at a server
        // that does not advertise the Checksum extension: the header must be
        // ignored, not rejected.
        let mut raw = HeaderMap::new();
        raw.insert("tus-resumable", "1.0.0".parse().unwrap());
        raw.insert("upload-checksum", "whirlpool AAAA".parse().unwrap());
        let headers = Headers::from_headers(&raw).unwrap();

        let collected = prepare(
            &Config::default(),
            &headers,
            Some(10),
            RequestBody::from_bytes(Bytes::from_static(b"hello")),
        )
        .await
        .unwrap();

        assert_eq!(collected.size, 5);
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn unparseable_upload_checksum_is_rejected_when_checksum_extension_enabled() {
        let mut raw = HeaderMap::new();
        raw.insert("tus-resumable", "1.0.0".parse().unwrap());
        raw.insert("upload-checksum", "whirlpool AAAA".parse().unwrap());
        let headers = Headers::from_headers(&raw).unwrap();
        let config = Config::default().with_extension(Extension::Checksum);

        let err = prepare(
            &config,
            &headers,
            Some(10),
            RequestBody::from_bytes(Bytes::from_static(b"hello")),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::UnsupportedChecksum(algorithm) if algorithm == "whirlpool"));
    }

    #[tokio::test]
    async fn chunked_body_respects_caller_limit_not_config_max_chunk_size() {
        // Regression: a chunked body (no Content-Length) must be bounded by the
        // `body_limit` the caller resolved, not by re-reading
        // `config.max_chunk_size()`. For Creation-With-Upload the caller
        // deliberately excludes `max_chunk_size`, so a chunked CwU body larger
        // than `max_chunk_size` (but within the caller's limit) must be
        // accepted, matching the Content-Length CwU path.
        let config = Config::default().with_max_chunk_size(2);
        let headers = Headers::default();
        let stream: BodyStream = Box::pin(futures::stream::iter([Ok(BodyFrame::Data(
            Bytes::from_static(b"hello"),
        ))]));

        let collected = prepare(
            &config,
            &headers,
            Some(10),
            RequestBody::from_stream(stream),
        )
        .await
        .expect("chunked body within the caller limit must be accepted");

        assert_eq!(collected.size, 5);
        assert_eq!(
            collect_data(collected.data).await.unwrap(),
            Bytes::from_static(b"hello")
        );
    }

    #[tokio::test]
    async fn chunked_body_without_any_limit_requires_length() {
        // With no caller limit AND no intake-buffer cap, a chunked body cannot
        // be safely buffered, so intake refuses it with 411 Length Required.
        let config = Config::default().without_intake_buffer_limit();
        let headers = Headers::default();
        let stream: BodyStream = Box::pin(futures::stream::iter([Ok(BodyFrame::Data(
            Bytes::from_static(b"hello"),
        ))]));

        let err = prepare(&config, &headers, None, RequestBody::from_stream(stream))
            .await
            .unwrap_err();

        assert!(matches!(err, Error::LengthRequired));
    }

    #[tokio::test]
    async fn chunked_body_without_caller_limit_is_bounded_by_intake_buffer() {
        // The default config caps intake buffering, so a chunked body with no
        // caller limit is accepted (up to the cap) rather than refused with
        // 411. This is the counterpart to the `without_intake_buffer_limit`
        // case above.
        let config = Config::default();
        let headers = Headers::default();
        let stream: BodyStream = Box::pin(futures::stream::iter([Ok(BodyFrame::Data(
            Bytes::from_static(b"hello"),
        ))]));

        let collected = prepare(&config, &headers, None, RequestBody::from_stream(stream))
            .await
            .expect("chunked body under the intake cap must be accepted");

        assert_eq!(collected.size, 5);
    }

    #[tokio::test]
    async fn intake_buffer_cap_bounds_below_caller_limit() {
        // Even when the caller limit (e.g. `length - offset` for a large
        // fixed-length PATCH) is huge, the intake-buffer cap bounds the
        // buffered chunked body and rejects the overflow with 413.
        let config = Config::default().with_max_intake_buffer(4);
        let headers = Headers::default();
        let stream: BodyStream = Box::pin(futures::stream::iter([Ok(BodyFrame::Data(
            Bytes::from_static(b"abcde"),
        ))]));

        let err = prepare(
            &config,
            &headers,
            Some(1_000_000),
            RequestBody::from_stream(stream),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::SizeExceeded { size: 5, max: 4 }));
    }

    #[tokio::test]
    async fn large_chunked_body_does_not_buffer_full_declared_length_on_default_config() {
        // Regression for the memory-exhaustion DoS: a large fixed-length upload
        // sent with a chunked body (no Content-Length) must not buffer the whole
        // declared length. Under default config the only caller limit is
        // `length - offset` (here ~5 GiB), but the intake cap bounds the buffer
        // to `DEFAULT_MAX_INTAKE_BUFFER` and stops draining once it is exceeded.
        use crate::config::DEFAULT_MAX_INTAKE_BUFFER;

        let five_gib: u64 = 5 * 1024 * 1024 * 1024;
        let polled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polled_for_stream = Arc::clone(&polled);

        // Yield 1 MiB frames lazily; the stream would produce far more than the
        // cap, but intake must stop shortly after crossing it.
        let stream: BodyStream = Box::pin(futures::stream::unfold(0usize, move |n| {
            let polled = Arc::clone(&polled_for_stream);
            async move {
                polled.fetch_add(1, Ordering::SeqCst);
                let chunk = Bytes::from(vec![0u8; 1024 * 1024]);
                Some((Ok(BodyFrame::Data(chunk)), n + 1))
            }
        }));

        let err = prepare(
            &Config::default(),
            &Headers::default(),
            Some(five_gib),
            RequestBody::from_stream(stream),
        )
        .await
        .unwrap_err();

        match err {
            Error::SizeExceeded { max, .. } => assert_eq!(max, DEFAULT_MAX_INTAKE_BUFFER),
            other => panic!("expected SizeExceeded at the intake cap, got {other:?}"),
        }

        // Intake stopped near the 8 MiB cap, nowhere near the 5 GiB length: only
        // a handful of 1 MiB frames were pulled (cap/1 MiB + 1 = 9).
        let frames = polled.load(Ordering::SeqCst);
        assert!(
            frames <= (DEFAULT_MAX_INTAKE_BUFFER / (1024 * 1024)) as usize + 1,
            "intake pulled {frames} frames, expected it to stop at the cap",
        );
    }

    #[cfg(feature = "checksum")]
    fn sha1_header_for(data: &[u8]) -> String {
        use crate::config::ChecksumAlgorithm;
        use base64::Engine;

        let checksum = crate::checksum::calculate(ChecksumAlgorithm::Sha1, data);
        format!(
            "sha1 {}",
            base64::engine::general_purpose::STANDARD.encode(checksum)
        )
    }

    #[cfg(feature = "checksum")]
    fn trailers(value: &str) -> HeaderMap {
        let mut trailers = HeaderMap::new();
        trailers.insert("upload-checksum", value.parse().unwrap());
        trailers
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn checksum_header_is_accepted_when_extension_enabled() {
        use crate::config::ChecksumAlgorithm;

        let config = Config::default().with_extension(Extension::Checksum);
        let headers = Headers {
            upload_checksum: Some(crate::UploadChecksum::new(
                ChecksumAlgorithm::Sha1,
                crate::checksum::calculate(ChecksumAlgorithm::Sha1, b"hello"),
            )),
            ..Default::default()
        };

        let collected = prepare(
            &config,
            &headers,
            Some(10),
            RequestBody::from_bytes(Bytes::from_static(b"hello")),
        )
        .await
        .unwrap();

        assert_eq!(
            collect_data(collected.data).await.unwrap(),
            Bytes::from_static(b"hello")
        );
        assert_eq!(collected.size, 5);
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn checksum_trailer_is_accepted_when_extension_enabled() {
        let config = Config::default().with_extension(Extension::ChecksumTrailer);
        let headers = Headers {
            content_length: Some(5),
            ..Default::default()
        };
        let trailer_value = sha1_header_for(b"hello");
        let stream: BodyStream = Box::pin(futures::stream::iter([
            Ok(BodyFrame::Data(Bytes::from_static(b"hello"))),
            Ok(BodyFrame::Trailers(trailers(&trailer_value))),
        ]));

        let collected = prepare(
            &config,
            &headers,
            Some(10),
            RequestBody::from_stream(stream),
        )
        .await
        .unwrap();

        assert_eq!(
            collect_data(collected.data).await.unwrap(),
            Bytes::from_static(b"hello")
        );
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn header_checksum_wins_over_malformed_trailer() {
        use crate::config::ChecksumAlgorithm;

        let config = Config::default().with_extension(Extension::ChecksumTrailer);
        let headers = Headers {
            content_length: Some(5),
            upload_checksum: Some(crate::UploadChecksum::new(
                ChecksumAlgorithm::Sha1,
                crate::checksum::calculate(ChecksumAlgorithm::Sha1, b"hello"),
            )),
            ..Default::default()
        };
        let stream: BodyStream = Box::pin(futures::stream::iter([
            Ok(BodyFrame::Data(Bytes::from_static(b"hello"))),
            Ok(BodyFrame::Trailers(trailers("sha1 not-base64"))),
        ]));

        let collected = prepare(
            &config,
            &headers,
            Some(10),
            RequestBody::from_stream(stream),
        )
        .await
        .unwrap();

        assert_eq!(
            collect_data(collected.data).await.unwrap(),
            Bytes::from_static(b"hello")
        );
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn unsupported_header_checksum_algorithm_fails_before_polling_stream() {
        use crate::config::ChecksumAlgorithm;

        let config = Config::default().with_extension(Extension::Checksum);
        let polled = Arc::new(AtomicBool::new(false));
        let polled_for_stream = Arc::clone(&polled);
        let stream: BodyStream = Box::pin(futures::stream::once(async move {
            polled_for_stream.store(true, Ordering::SeqCst);
            Ok(BodyFrame::Data(Bytes::from_static(b"hello")))
        }));
        let headers = Headers {
            upload_checksum: Some(crate::UploadChecksum::new(
                ChecksumAlgorithm::Sha256,
                vec![0u8; 32],
            )),
            ..Default::default()
        };

        let err = prepare(
            &config,
            &headers,
            Some(10),
            RequestBody::from_stream(stream),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::UnsupportedChecksum(algorithm) if algorithm == "sha256"));
        assert!(!polled.load(Ordering::SeqCst));
    }

    #[cfg(feature = "checksum")]
    #[tokio::test]
    async fn checksum_mismatch_fails_after_buffered_collection() {
        use crate::config::ChecksumAlgorithm;

        let config = Config::default().with_extension(Extension::Checksum);
        let headers = Headers {
            upload_checksum: Some(crate::UploadChecksum::new(
                ChecksumAlgorithm::Sha1,
                vec![0u8; 20],
            )),
            ..Default::default()
        };

        let err = prepare(
            &config,
            &headers,
            Some(10),
            RequestBody::from_bytes(Bytes::from_static(b"hello")),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::ChecksumMismatch { .. }));
    }
}
