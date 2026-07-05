//! Internal helpers for the TUS client.

use base64::Engine;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use std::time::Duration;
use tus_protocol::{MetadataValue, UploadMetadata};
use url::Url;

use crate::client::UploadInfo;
use crate::error::{Error, Result};
use crate::transport::TransportResponse;

pub(crate) fn encode_metadata(metadata: &UploadMetadata) -> Result<String> {
    let mut pairs = Vec::with_capacity(metadata.len());
    for (key, value) in metadata {
        if !is_valid_metadata_key(key) {
            return Err(Error::InvalidDefaultHeader {
                name: "Upload-Metadata".to_string(),
                value: key.clone(),
            });
        }
        let encoded = base64::engine::general_purpose::STANDARD.encode(value.as_bytes());
        pairs.push(format!("{key} {encoded}"));
    }
    Ok(pairs.join(","))
}

fn is_valid_metadata_key(key: &str) -> bool {
    !key.is_empty() && key.bytes().all(|b| (0x21..=0x7E).contains(&b) && b != b',')
}

pub(crate) fn decode_metadata(value: Option<&HeaderValue>) -> Result<UploadMetadata> {
    let Some(value) = value else {
        return Ok(UploadMetadata::new());
    };
    let value = value.to_str().map_err(|_| Error::InvalidHeader {
        header: "Upload-Metadata",
        value: String::from_utf8_lossy(value.as_bytes()).into_owned(),
    })?;

    let mut metadata = UploadMetadata::new();
    if value.is_empty() {
        return Ok(metadata);
    }

    for pair in value.split(',') {
        let pair = pair.trim();
        let (key, decoded_value) = if let Some((key, encoded)) = pair.split_once(' ') {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| Error::InvalidHeader {
                    header: "Upload-Metadata",
                    value: pair.to_string(),
                })?;
            (key, MetadataValue::from(decoded))
        } else {
            (pair, MetadataValue::default())
        };
        if !is_valid_metadata_key(key) {
            return Err(Error::InvalidHeader {
                header: "Upload-Metadata",
                value: pair.to_string(),
            });
        }
        metadata.insert(key.to_string(), decoded_value);
    }

    Ok(metadata)
}

pub(crate) fn header_string(
    headers: &HeaderMap,
    name: HeaderName,
    label: &'static str,
) -> Result<String> {
    headers
        .get(name)
        .ok_or(Error::MissingHeader(label))?
        .to_str()
        .map(|value| value.to_string())
        .map_err(|_| Error::InvalidHeader {
            header: label,
            value: String::from("<non-utf8>"),
        })
}

pub(crate) fn header_u64(
    headers: &HeaderMap,
    name: &'static str,
    label: &'static str,
) -> Result<u64> {
    optional_header_u64(headers, name, label)?.ok_or(Error::MissingHeader(label))
}

pub(crate) fn optional_header_u64(
    headers: &HeaderMap,
    name: &'static str,
    label: &'static str,
) -> Result<Option<u64>> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| Error::InvalidHeader {
        header: label,
        value: String::from("<non-utf8>"),
    })?;
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| Error::InvalidHeader {
            header: label,
            value: value.to_string(),
        })
}

pub(crate) fn resolve_upload_url(endpoint: &Url, reference: &str) -> Result<Url> {
    let mut base = endpoint.clone();
    if !base.path().ends_with('/') {
        let path = format!("{}/", base.path());
        base.set_path(&path);
    }

    Ok(base.join(reference)?)
}

pub(crate) fn resolve_upload_location(endpoint: &Url, location: &str) -> Result<Url> {
    resolve_upload_url(endpoint, location)
}

pub(crate) fn validate_offset_not_beyond_source(offset: u64, source_len: u64) -> Result<()> {
    if offset > source_len {
        return Err(Error::OffsetBeyondSource { offset, source_len });
    }
    Ok(())
}

pub(crate) fn validate_remote_for_resume(remote: &UploadInfo, file_length: u64) -> Result<()> {
    if let Some(remote_length) = remote.length
        && remote_length != file_length
    {
        return Err(Error::LengthMismatch {
            remote: remote_length,
            local: file_length,
        });
    }
    validate_offset_not_beyond_source(remote.offset, file_length)
}

/// Validates the offset a server acknowledged for a PATCH that sent
/// `chunk_len` bytes starting at `previous`.
///
/// The acknowledged offset must advance (`next > previous`) but can never
/// exceed `previous + chunk_len`: a server acking beyond the bytes actually
/// transmitted would make the client silently skip source bytes and report
/// a corrupt upload as successful.
pub(crate) fn validate_patch_advance(
    previous: u64,
    next: u64,
    chunk_len: u64,
    source_len: u64,
) -> Result<()> {
    validate_offset_not_beyond_source(next, source_len)?;
    if next <= previous {
        return Err(Error::OffsetDesync {
            expected: previous + 1,
            actual: next,
        });
    }
    if next > previous.saturating_add(chunk_len) {
        return Err(Error::OffsetDesync {
            expected: previous + chunk_len,
            actual: next,
        });
    }
    Ok(())
}

/// Splits a comma-separated TUS header value into trimmed, non-empty entries.
///
/// `Tus-Version` and `Tus-Extension` use comma-separated lists (RFC 7230
/// `#rule`), with optional whitespace around items. The TUS spec is silent on
/// duplicate handling, so we preserve order and let callers dedupe.
pub(crate) fn parse_csv_header(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

pub(crate) fn backoff_delay(base: Duration, attempt: usize) -> Duration {
    let multiplier = 1_u32.checked_shl(attempt.min(8) as u32).unwrap_or(u32::MAX);
    base.saturating_mul(multiplier)
}

/// Apply full jitter to the exponential backoff: pick uniformly in
/// `[0, backoff_delay(base, attempt)]`. Without jitter, every client
/// hitting the same outage retries on the exact same schedule and
/// thundering-herds the recovering server.
pub(crate) fn jittered_backoff_delay(base: Duration, attempt: usize) -> Duration {
    let max = backoff_delay(base, attempt);
    let max_nanos = max.as_nanos() as u64;
    if max_nanos == 0 {
        return max;
    }

    #[cfg(target_arch = "wasm32")]
    let nanos = (js_sys::Math::random() * (max_nanos as f64 + 1.0)) as u64;

    #[cfg(not(target_arch = "wasm32"))]
    let nanos = fastrand::u64(0..=max_nanos);

    Duration::from_nanos(nanos.min(max_nanos))
}

/// The ceiling of the exponential backoff schedule. `backoff_delay`
/// saturates its multiplier at `1 << 8`, so this is the longest delay the
/// schedule can ever produce; it bounds a server-provided `Retry-After` so
/// a hostile or misconfigured value cannot pin the client asleep.
pub(crate) fn max_backoff_delay(base: Duration) -> Duration {
    backoff_delay(base, 8)
}

/// Computes the delay before the next retry attempt.
///
/// A valid server `Retry-After` hint wins — clamped to
/// [`max_backoff_delay`] so an abusive value cannot stall the client
/// indefinitely — otherwise the client falls back to its own full-jitter
/// exponential backoff.
pub(crate) fn next_retry_delay(
    retry_after: Option<Duration>,
    base: Duration,
    attempt: usize,
) -> Duration {
    match retry_after {
        Some(hint) => hint.min(max_backoff_delay(base)),
        None => jittered_backoff_delay(base, attempt),
    }
}

/// Parses a `Retry-After` header value into a delay.
///
/// Handles the `delay-seconds` form (a non-negative integer) on every
/// target, and the preferred IMF-fixdate HTTP-date form on native targets
/// by differencing against the system clock; a date already in the past
/// yields a zero delay. Anything unparseable yields `None`, so the caller
/// falls back to its computed backoff.
pub(crate) fn parse_retry_after(value: &str) -> Option<Duration> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    parse_retry_after_date(value)
}

/// Differences an HTTP-date `Retry-After` against the system clock.
///
/// Native only: computing the delay needs a wall clock, which wasm32
/// targets lack, so date-form values there fall back to jittered backoff.
#[cfg(not(target_arch = "wasm32"))]
fn parse_retry_after_date(value: &str) -> Option<Duration> {
    let target = parse_imf_fixdate_unix(value)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs() as i64;
    Some(Duration::from_secs(target.saturating_sub(now).max(0) as u64))
}

#[cfg(target_arch = "wasm32")]
fn parse_retry_after_date(_value: &str) -> Option<Duration> {
    None
}

/// Parses an IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`, RFC 7231
/// §7.1.1.1) into seconds since the Unix epoch. Only the fixed-width
/// preferred form is accepted; the two obsolete date forms are not worth
/// the surface for a `Retry-After` hint.
#[cfg(not(target_arch = "wasm32"))]
fn parse_imf_fixdate_unix(value: &str) -> Option<i64> {
    let value = value.strip_suffix(" GMT")?;
    let (_weekday, rest) = value.split_once(", ")?;
    let mut parts = rest.split(' ');
    let day: i64 = parts.next()?.parse().ok()?;
    let month = match parts.next()? {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year: i64 = parts.next()?.parse().ok()?;
    let time = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let mut hms = time.split(':');
    let hour: i64 = hms.next()?.parse().ok()?;
    let minute: i64 = hms.next()?.parse().ok()?;
    let second: i64 = hms.next()?.parse().ok()?;
    if hms.next().is_some() {
        return None;
    }
    if !(1..=31).contains(&day) || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(days * 86_400 + hour * 3_600 + minute * 60 + second)
}

/// Days from the Unix epoch (1970-01-01) for a civil date, via Howard
/// Hinnant's `days_from_civil` algorithm.
#[cfg(not(target_arch = "wasm32"))]
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(feature = "checksum")]
pub(crate) fn encode_checksum(algorithm: tus_protocol::ChecksumAlgorithm, body: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD
        .encode(tus_protocol::calculate_checksum(algorithm, body))
}

/// Maximum number of error-response body bytes captured into
/// [`Error::UnexpectedResponse`]. Error bodies exist for diagnostics only;
/// an unbounded capture would let a misbehaving server balloon client
/// memory (and logs).
pub(crate) const MAX_CAPTURED_ERROR_BODY_BYTES: usize = 8 * 1024;

pub(crate) const TRUNCATED_ERROR_BODY_MARKER: &str = "...[truncated]";

pub(crate) async fn unexpected_response(
    operation: &'static str,
    response: TransportResponse,
) -> Error {
    let status = response.status().as_u16();
    // Capture any `Retry-After` before consuming the body so a retryable
    // response can honor the server's own backoff instead of the client's.
    let retry_after = response
        .headers()
        .get(http::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_retry_after);
    let mut bytes = response.into_body();
    let truncated = bytes.len() > MAX_CAPTURED_ERROR_BODY_BYTES;
    if truncated {
        bytes.truncate(MAX_CAPTURED_ERROR_BODY_BYTES);
    }
    let mut body = String::from_utf8(bytes)
        .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned());
    if truncated {
        body.push_str(TRUNCATED_ERROR_BODY_MARKER);
    }
    Error::UnexpectedResponse {
        operation,
        status,
        body,
        retry_after,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn resolve_upload_url_accepts_absolute_url_absolute_path_and_relative_path() {
        let endpoint = Url::parse("http://example.test/files").unwrap();
        let cases = [
            (
                "http://uploads.example.test/upload-1",
                "http://uploads.example.test/upload-1",
            ),
            ("/files/upload-1", "http://example.test/files/upload-1"),
            ("upload-1", "http://example.test/files/upload-1"),
            (
                "nested/upload-1",
                "http://example.test/files/nested/upload-1",
            ),
        ];

        for (reference, expected) in cases {
            let url = resolve_upload_url(&endpoint, reference).unwrap();

            assert_eq!(url.as_str(), expected);
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn parse_csv_header_handles_trim_and_empties() {
        assert_eq!(
            parse_csv_header("creation, creation-with-upload, termination"),
            vec!["creation", "creation-with-upload", "termination"],
        );
        assert_eq!(parse_csv_header(""), Vec::<String>::new());
        assert_eq!(parse_csv_header(",,creation,,"), vec!["creation"]);
        assert_eq!(parse_csv_header(" 1.0.0 "), vec!["1.0.0"]);
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn resolve_upload_location_uses_endpoint_collection_semantics() {
        let cases = [
            (
                "http://example.test/files",
                "upload-1",
                "http://example.test/files/upload-1",
            ),
            (
                "http://example.test/files/",
                "upload-1",
                "http://example.test/files/upload-1",
            ),
            (
                "http://example.test/files",
                "/files/upload-1",
                "http://example.test/files/upload-1",
            ),
            (
                "http://example.test/files",
                "https://uploads.example.test/upload-1",
                "https://uploads.example.test/upload-1",
            ),
        ];

        for (endpoint, location, expected) in cases {
            let endpoint = Url::parse(endpoint).unwrap();

            let url = resolve_upload_location(&endpoint, location).unwrap();

            assert_eq!(url.as_str(), expected);
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn validate_patch_advance_accepts_offsets_up_to_the_bytes_sent() {
        // Full ack and partial ack of a 4-byte chunk sent at offset 2.
        validate_patch_advance(2, 6, 4, 10).unwrap();
        validate_patch_advance(2, 3, 4, 10).unwrap();
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn validate_patch_advance_rejects_non_advancing_offsets() {
        let result = validate_patch_advance(2, 2, 4, 10);

        assert!(matches!(
            result,
            Err(Error::OffsetDesync {
                expected: 3,
                actual: 2,
            })
        ));
    }

    /// A server acking beyond `previous + chunk_len` would make the client
    /// skip source bytes and report a corrupt upload as successful.
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn validate_patch_advance_rejects_offsets_beyond_the_bytes_sent() {
        let result = validate_patch_advance(2, 7, 4, 10);

        assert!(matches!(
            result,
            Err(Error::OffsetDesync {
                expected: 6,
                actual: 7,
            })
        ));
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn validate_patch_advance_rejects_offsets_beyond_the_source() {
        let result = validate_patch_advance(2, 5, 4, 4);

        assert!(matches!(
            result,
            Err(Error::OffsetBeyondSource {
                offset: 5,
                source_len: 4,
            })
        ));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn unexpected_response_caps_the_captured_body() {
        let body = vec![b'x'; MAX_CAPTURED_ERROR_BODY_BYTES + 1024];
        let response = http::Response::builder().status(502).body(body).unwrap();

        let error = unexpected_response("patch upload", response).await;

        match error {
            Error::UnexpectedResponse {
                status: 502, body, ..
            } => {
                assert!(body.ends_with(TRUNCATED_ERROR_BODY_MARKER), "{body:?}");
                assert_eq!(
                    body.len(),
                    MAX_CAPTURED_ERROR_BODY_BYTES + TRUNCATED_ERROR_BODY_MARKER.len()
                );
            }
            other => panic!("expected UnexpectedResponse, got {other:?}"),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn unexpected_response_captures_a_valid_retry_after() {
        let response = http::Response::builder()
            .status(503)
            .header(http::header::RETRY_AFTER, "3")
            .body(b"slow down".to_vec())
            .unwrap();

        let error = unexpected_response("patch upload", response).await;

        assert_eq!(error.retry_after(), Some(Duration::from_secs(3)));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn unexpected_response_ignores_an_invalid_retry_after() {
        let response = http::Response::builder()
            .status(503)
            .header(http::header::RETRY_AFTER, "whenever")
            .body(Vec::new())
            .unwrap();

        let error = unexpected_response("patch upload", response).await;

        // An unparseable hint leaves the client to fall back to backoff.
        assert_eq!(error.retry_after(), None);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn unexpected_response_keeps_small_bodies_intact() {
        let response = http::Response::builder()
            .status(502)
            .body(b"bad gateway".to_vec())
            .unwrap();

        let error = unexpected_response("patch upload", response).await;

        match error {
            Error::UnexpectedResponse { body, .. } => assert_eq!(body, "bad gateway"),
            other => panic!("expected UnexpectedResponse, got {other:?}"),
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn backoff_grows_exponentially() {
        assert_eq!(
            backoff_delay(Duration::from_millis(50), 0),
            Duration::from_millis(50)
        );
        assert_eq!(
            backoff_delay(Duration::from_millis(50), 1),
            Duration::from_millis(100)
        );
        assert_eq!(
            backoff_delay(Duration::from_millis(50), 2),
            Duration::from_millis(200)
        );
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn parse_retry_after_reads_delay_seconds() {
        assert_eq!(parse_retry_after("0"), Some(Duration::from_secs(0)));
        assert_eq!(parse_retry_after("120"), Some(Duration::from_secs(120)));
        assert_eq!(parse_retry_after("  30 "), Some(Duration::from_secs(30)));
    }

    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn parse_retry_after_rejects_garbage() {
        assert_eq!(parse_retry_after(""), None);
        assert_eq!(parse_retry_after("   "), None);
        assert_eq!(parse_retry_after("soon"), None);
        assert_eq!(parse_retry_after("-5"), None);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn parse_imf_fixdate_matches_the_canonical_example() {
        // RFC 7231's canonical IMF-fixdate example is 784111777 seconds
        // since the Unix epoch.
        assert_eq!(
            parse_imf_fixdate_unix("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(784_111_777)
        );
        assert_eq!(parse_imf_fixdate_unix("Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
        assert_eq!(parse_imf_fixdate_unix("not a date"), None);
        assert_eq!(parse_imf_fixdate_unix("Sun, 06 Nov 1994 08:49:37 PST"), None);
    }

    /// A valid server hint is honored verbatim when it sits under the
    /// backoff ceiling; an absent one falls back to jittered backoff, which
    /// never exceeds the schedule for that attempt.
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    fn next_retry_delay_honors_hint_then_falls_back_to_backoff() {
        let base = Duration::from_millis(200);

        // A hint below the ceiling is used exactly as given.
        assert_eq!(
            next_retry_delay(Some(Duration::from_secs(2)), base, 0),
            Duration::from_secs(2)
        );

        // A hostile hint is clamped to the backoff ceiling.
        assert_eq!(
            next_retry_delay(Some(Duration::from_secs(86_400)), base, 0),
            max_backoff_delay(base)
        );

        // No hint: jittered fallback stays within the schedule for the attempt.
        for attempt in 0..4 {
            let delay = next_retry_delay(None, base, attempt);
            assert!(
                delay <= backoff_delay(base, attempt),
                "jittered fallback {delay:?} exceeded backoff for attempt {attempt}"
            );
        }
    }
}
