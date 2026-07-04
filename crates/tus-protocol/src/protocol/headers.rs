//! Framework-neutral parsing of TUS-specific request headers.
//!
//! Adapters extract an [`http::HeaderMap`] from the underlying framework's
//! request and call [`Headers::from_headers`] to get a typed view over the
//! TUS-relevant headers.

use http::HeaderMap;
#[cfg(feature = "fuzzing")]
use http::HeaderValue;

use crate::config::{ChecksumAlgorithm, Config, TUS_RESUMABLE};
use crate::error::Error;
use crate::extensions::UploadConcat;
use crate::state::{MetadataValue, UploadMetadata};

/// Parsed TUS-specific request headers.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct Headers {
    /// Upload-Offset header value.
    pub upload_offset: Option<u64>,
    /// Upload-Length header value.
    pub upload_length: Option<u64>,
    /// Whether Upload-Defer-Length: 1 was present.
    pub upload_defer_length: bool,
    /// Parsed Upload-Metadata values.
    pub upload_metadata: Option<UploadMetadata>,
    /// Parsed Upload-Checksum (algorithm, checksum bytes).
    pub upload_checksum: Option<(ChecksumAlgorithm, Vec<u8>)>,
    /// Parsed Upload-Concat header.
    pub upload_concat: Option<UploadConcat>,
    /// Content-Length header value.
    pub content_length: Option<u64>,
    /// Content-Type header value.
    pub content_type: Option<String>,
    /// Transfer-Encoding header value.
    pub transfer_encoding: Option<String>,
    /// Host header (direct).
    pub host_header: Option<String>,
    /// X-Forwarded-Host header (trusted only when enabled via
    /// [`Config::with_respect_forwarded_headers`]).
    pub forwarded_host: Option<String>,
    /// X-Forwarded-Proto header (trusted only when enabled via
    /// [`Config::with_respect_forwarded_headers`]).
    pub forwarded_proto: Option<String>,
}

impl Headers {
    /// Parses TUS headers from an HTTP [`HeaderMap`] and validates that the
    /// `Tus-Resumable` header is present and supported.
    ///
    /// Use this for handlers where Tus-Resumable is mandatory (POST, PATCH,
    /// HEAD, DELETE). OPTIONS does not parse headers at all.
    ///
    /// # Errors
    ///
    /// Returns an error if `Tus-Resumable` is missing or unsupported, or if any
    /// TUS-specific header value cannot be parsed or validated.
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, Error> {
        // Validate Tus-Resumable header
        match headers.get("tus-resumable").and_then(|v| v.to_str().ok()) {
            Some(version) if version == TUS_RESUMABLE => {}
            Some(version) => return Err(Error::UnsupportedTusVersion(version.to_string())),
            None => return Err(Error::MissingTusResumable),
        }

        let upload_offset = parse_u64_header(headers, "upload-offset")?;
        let upload_length = parse_u64_header(headers, "upload-length")?;
        let upload_defer_length = match headers.get("upload-defer-length") {
            Some(value) => match value.to_str() {
                Ok("1") => true,
                Ok(value) => {
                    return Err(Error::InvalidHeader {
                        header: "Upload-Defer-Length",
                        message: format!("expected 1, got {value}"),
                    });
                }
                Err(error) => {
                    return Err(Error::InvalidHeader {
                        header: "Upload-Defer-Length",
                        message: error.to_string(),
                    });
                }
            },
            None => false,
        };
        let content_length = parse_u64_header(headers, "content-length")?;
        let content_type = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let transfer_encoding_values: Vec<_> = headers
            .get_all("transfer-encoding")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .collect();
        let transfer_encoding = if transfer_encoding_values.is_empty() {
            None
        } else {
            Some(transfer_encoding_values.join(","))
        };
        let upload_metadata = parse_upload_metadata(headers)?;
        let upload_checksum = parse_upload_checksum(headers)?;
        let upload_concat = parse_upload_concat(headers)?;

        let host_header = headers
            .get("host")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let forwarded_host = headers
            .get("x-forwarded-host")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let forwarded_proto = headers
            .get("x-forwarded-proto")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        Ok(Self {
            upload_offset,
            upload_length,
            upload_defer_length,
            upload_metadata,
            upload_checksum,
            upload_concat,
            content_length,
            content_type,
            transfer_encoding,
            host_header,
            forwarded_host,
            forwarded_proto,
        })
    }

    /// Returns the base URL derived from scheme and host, honoring
    /// [`Config::respects_forwarded_headers`].
    ///
    /// When forwarded headers are not trusted and no other scheme signal is
    /// available, returns `None`. Callers should fall back to a
    /// relative Location (spec-legal per RFC 7231).
    pub fn base_url(&self, config: &Config) -> Option<String> {
        let (host, scheme) = if config.respects_forwarded_headers() {
            let host = self
                .forwarded_host
                .as_deref()
                .or(self.host_header.as_deref());
            let scheme = self.forwarded_proto.as_deref();
            (host, scheme)
        } else {
            (self.host_header.as_deref(), None)
        };

        match (scheme, host) {
            (Some(s), Some(h)) => Some(format!("{}://{}", s, h)),
            _ => None,
        }
    }

    /// Validates Content-Type for PATCH requests.
    pub fn validate_patch_content_type(&self) -> Result<(), Error> {
        match &self.content_type {
            Some(ct) if ct.starts_with("application/offset+octet-stream") => Ok(()),
            Some(ct) => Err(Error::InvalidContentType {
                expected: "application/offset+octet-stream".to_string(),
                actual: ct.clone(),
            }),
            None => Err(Error::InvalidContentType {
                expected: "application/offset+octet-stream".to_string(),
                actual: "missing".to_string(),
            }),
        }
    }
}

fn parse_u64_header(headers: &HeaderMap, name: &'static str) -> Result<Option<u64>, Error> {
    match headers.get(name).and_then(|v| v.to_str().ok()) {
        Some(value) => value.parse().map(Some).map_err(|_| Error::InvalidHeader {
            header: name,
            message: format!("invalid integer: {}", value),
        }),
        None => Ok(None),
    }
}

fn parse_upload_metadata(headers: &HeaderMap) -> Result<Option<UploadMetadata>, Error> {
    let value = match headers.get("upload-metadata").and_then(|v| v.to_str().ok()) {
        Some(v) if !v.is_empty() => v,
        _ => return Ok(None),
    };

    let mut metadata = UploadMetadata::new();
    for pair in value.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }

        let parts: Vec<&str> = pair.splitn(2, ' ').collect();
        let key = parts[0].to_string();
        if key.is_empty() {
            return Err(Error::InvalidMetadata("empty key".to_string()));
        }
        if !is_valid_metadata_key(&key) {
            return Err(Error::InvalidMetadata(format!(
                "invalid characters in key {:?}",
                key
            )));
        }

        let decoded_value = if parts.len() > 1 {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(parts[1])
                .map_err(|e| {
                    Error::InvalidMetadata(format!("invalid base64 for key {}: {}", key, e))
                })?
        } else {
            Vec::new()
        };

        if metadata
            .insert(key.clone(), MetadataValue::from(decoded_value))
            .is_some()
        {
            return Err(Error::InvalidMetadata(format!("duplicate key: {}", key)));
        }
    }

    Ok(Some(metadata))
}

/// Validates a metadata key per the tus spec: non-empty, ASCII, and
/// containing no space or comma (which are structural separators).
fn is_valid_metadata_key(key: &str) -> bool {
    !key.is_empty()
        && key.bytes().all(|b| {
            // Printable ASCII (0x21-0x7E) excluding space (0x20, already
            // excluded by the lower bound) and comma (0x2C).
            (0x21..=0x7E).contains(&b) && b != b','
        })
}

pub(crate) fn parse_upload_checksum(
    headers: &HeaderMap,
) -> Result<Option<(ChecksumAlgorithm, Vec<u8>)>, Error> {
    let value = match headers.get("upload-checksum").and_then(|v| v.to_str().ok()) {
        Some(v) => v,
        None => return Ok(None),
    };

    parse_upload_checksum_value(value).map(Some)
}

pub(crate) fn parse_upload_checksum_value(
    value: &str,
) -> Result<(ChecksumAlgorithm, Vec<u8>), Error> {
    let parts: Vec<&str> = value.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return Err(Error::InvalidHeader {
            header: "Upload-Checksum",
            message: "expected 'algorithm checksum' format".to_string(),
        });
    }

    let algorithm: ChecksumAlgorithm = parts[0].parse()?;

    use base64::Engine;
    let checksum = base64::engine::general_purpose::STANDARD
        .decode(parts[1])
        .map_err(|e| Error::InvalidHeader {
            header: "Upload-Checksum",
            message: format!("invalid base64: {}", e),
        })?;

    Ok((algorithm, checksum))
}

fn parse_upload_concat(headers: &HeaderMap) -> Result<Option<UploadConcat>, Error> {
    let value = match headers.get("upload-concat").and_then(|v| v.to_str().ok()) {
        Some(v) => v,
        None => return Ok(None),
    };

    if value == "partial" {
        return Ok(Some(UploadConcat::Partial));
    }

    if let Some(urls_str) = value.strip_prefix("final;") {
        let urls: Vec<String> = urls_str.split_whitespace().map(|s| s.to_string()).collect();
        if urls.is_empty() {
            return Err(Error::InvalidHeader {
                header: "Upload-Concat",
                message: "final upload requires at least one partial URL".to_string(),
            });
        }
        return Ok(Some(UploadConcat::Final(urls)));
    }

    Err(Error::InvalidHeader {
        header: "Upload-Concat",
        message: format!("invalid value: {}", value),
    })
}

#[cfg(feature = "fuzzing")]
fn fuzz_header_map(header: &'static str, value: &[u8]) -> Result<HeaderMap, Error> {
    let mut headers = HeaderMap::new();
    let value = HeaderValue::from_bytes(value).map_err(|err| Error::InvalidHeader {
        header,
        message: err.to_string(),
    })?;
    headers.insert(header, value);
    Ok(headers)
}

/// Fuzz-only entry point for `Upload-Metadata` parsing.
#[cfg(feature = "fuzzing")]
pub fn fuzz_parse_upload_metadata(value: &[u8]) -> Result<Option<UploadMetadata>, Error> {
    let headers = fuzz_header_map("upload-metadata", value)?;
    parse_upload_metadata(&headers)
}

/// Fuzz-only entry point for `Upload-Checksum` parsing.
#[cfg(feature = "fuzzing")]
pub fn fuzz_parse_upload_checksum(
    value: &[u8],
) -> Result<Option<(ChecksumAlgorithm, Vec<u8>)>, Error> {
    let headers = fuzz_header_map("upload-checksum", value)?;
    parse_upload_checksum(&headers)
}

/// Fuzz-only entry point for `Upload-Concat` parsing.
#[cfg(feature = "fuzzing")]
pub fn fuzz_parse_upload_concat(value: &[u8]) -> Result<Option<UploadConcat>, Error> {
    let headers = fuzz_header_map("upload-concat", value)?;
    parse_upload_concat(&headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn make_headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                http::HeaderName::try_from(*k).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    #[test]
    fn from_headers_parses_basic() {
        let headers = make_headers(&[
            ("tus-resumable", "1.0.0"),
            ("upload-offset", "100"),
            ("upload-length", "1000"),
        ]);
        let tus = Headers::from_headers(&headers).unwrap();
        assert_eq!(tus.upload_offset, Some(100));
        assert_eq!(tus.upload_length, Some(1000));
    }

    #[test]
    fn from_headers_combines_multiple_transfer_encoding_values() {
        let mut headers = make_headers(&[("tus-resumable", "1.0.0")]);
        headers.append("transfer-encoding", HeaderValue::from_static("gzip"));
        headers.append("transfer-encoding", HeaderValue::from_static("chunked"));

        let tus = Headers::from_headers(&headers).unwrap();
        let transfer_encoding = tus.transfer_encoding.expect("transfer encoding");

        assert!(
            transfer_encoding
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked")),
            "chunked value missing from parsed Transfer-Encoding: {transfer_encoding}"
        );
    }

    #[test]
    fn from_headers_rejects_missing_tus_resumable() {
        let headers = HeaderMap::new();
        assert!(matches!(
            Headers::from_headers(&headers),
            Err(Error::MissingTusResumable)
        ));
    }

    #[test]
    fn from_headers_rejects_wrong_version() {
        let headers = make_headers(&[("tus-resumable", "0.9.0")]);
        assert!(matches!(
            Headers::from_headers(&headers),
            Err(Error::UnsupportedTusVersion(_))
        ));
    }

    #[test]
    fn defer_length_flag() {
        let headers = make_headers(&[("tus-resumable", "1.0.0"), ("upload-defer-length", "1")]);
        let tus = Headers::from_headers(&headers).unwrap();
        assert!(tus.upload_defer_length);
    }

    fn parse_concat(value: &str) -> Result<Option<UploadConcat>, Error> {
        let headers = make_headers(&[("upload-concat", value)]);
        parse_upload_concat(&headers)
    }

    #[test]
    fn concat_partial() {
        assert!(matches!(
            parse_concat("partial").unwrap(),
            Some(UploadConcat::Partial)
        ));
    }

    #[test]
    fn concat_final_with_urls() {
        let parsed = parse_concat("final;/files/a /files/b").unwrap();
        match parsed {
            Some(UploadConcat::Final(urls)) => {
                assert_eq!(urls, vec!["/files/a".to_string(), "/files/b".to_string()]);
            }
            other => panic!("expected Final, got {:?}", other),
        }
    }

    #[test]
    fn concat_final_empty_returns_error() {
        let err = parse_concat("final;").unwrap_err();
        assert!(matches!(err, Error::InvalidHeader { header, .. } if header == "Upload-Concat"));
    }

    #[test]
    fn concat_final_whitespace_only_returns_error() {
        let err = parse_concat("final;   ").unwrap_err();
        assert!(matches!(err, Error::InvalidHeader { header, .. } if header == "Upload-Concat"));
    }

    #[test]
    fn concat_malformed_never_panics() {
        for bad in [
            "",
            "final",
            "Final;/files/a",
            "bogus",
            "final ;/files/a",
            "partial;",
            ";final",
        ] {
            let result = parse_concat(bad);
            assert!(
                matches!(&result, Err(Error::InvalidHeader { .. }) | Ok(None)),
                "expected error or None for {:?}, got {:?}",
                bad,
                result
            );
        }
    }

    #[test]
    fn base_url_ignores_forwarded_headers_by_default() {
        let headers = make_headers(&[
            ("tus-resumable", "1.0.0"),
            ("host", "internal.local"),
            ("x-forwarded-host", "attacker.example"),
            ("x-forwarded-proto", "https"),
        ]);
        let tus = Headers::from_headers(&headers).unwrap();
        // Default config does not trust forwarded headers, and with no scheme
        // signal the base URL falls back to None (relative Location).
        let config = Config::new();
        assert_eq!(tus.base_url(&config), None);
    }

    #[test]
    fn base_url_uses_forwarded_headers_when_trusted() {
        let headers = make_headers(&[
            ("tus-resumable", "1.0.0"),
            ("host", "internal.local"),
            ("x-forwarded-host", "public.example"),
            ("x-forwarded-proto", "https"),
        ]);
        let tus = Headers::from_headers(&headers).unwrap();
        let config = Config::new().with_respect_forwarded_headers();
        assert_eq!(
            tus.base_url(&config),
            Some("https://public.example".to_string())
        );
    }

    #[test]
    fn metadata_rejects_duplicate_keys() {
        use base64::Engine;
        let enc = |s: &str| base64::engine::general_purpose::STANDARD.encode(s);
        let value = format!("name {},name {}", enc("a"), enc("b"));
        let headers = make_headers(&[("tus-resumable", "1.0.0"), ("upload-metadata", &value)]);
        let err = Headers::from_headers(&headers).unwrap_err();
        assert!(matches!(err, Error::InvalidMetadata(ref m) if m.contains("duplicate")));
    }

    #[test]
    fn metadata_key_validator_rejects_non_printable_and_separators() {
        // Validator-level test: a defense-in-depth check for parsers built on
        // top of the protocol module that may not rely on HTTP's own visible-
        // ASCII guarantee (e.g. parsing headers from stored state).
        for bad in ["", "na me", "na,me", "na\x01me", "na\x7fme", "café"] {
            assert!(
                !is_valid_metadata_key(bad),
                "expected {:?} to be rejected",
                bad
            );
        }
        for good in ["name", "filename", "x-custom_1", "a.b-c~d"] {
            assert!(
                is_valid_metadata_key(good),
                "expected {:?} to be accepted",
                good
            );
        }
    }

    #[test]
    fn metadata_preserves_binary_value_bytes() {
        use base64::Engine;
        // base64 of 0xFF 0xFE 0xFD, not valid UTF-8.
        let bytes = [0xFFu8, 0xFE, 0xFD];
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        let value = format!("bin {}", encoded);
        let headers = make_headers(&[("tus-resumable", "1.0.0"), ("upload-metadata", &value)]);
        let tus = Headers::from_headers(&headers).unwrap();
        let md = tus.upload_metadata.expect("metadata present");
        assert_eq!(md.get("bin").unwrap().as_bytes(), bytes);
    }

    #[test]
    fn upload_checksum_value_parser_accepts_valid_value() {
        let parsed = parse_upload_checksum_value("sha1 AAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap();

        assert_eq!(parsed.0, ChecksumAlgorithm::Sha1);
        assert_eq!(parsed.1.len(), 20);
    }

    #[test]
    fn upload_checksum_value_parser_rejects_invalid_shape() {
        let err = parse_upload_checksum_value("sha1").unwrap_err();

        assert!(matches!(
            err,
            Error::InvalidHeader {
                header: "Upload-Checksum",
                ..
            }
        ));
    }

    #[test]
    fn upload_checksum_value_parser_rejects_invalid_base64() {
        let err = parse_upload_checksum_value("sha1 not-base64").unwrap_err();

        assert!(matches!(
            err,
            Error::InvalidHeader {
                header: "Upload-Checksum",
                ..
            }
        ));
    }

    #[test]
    fn base_url_none_without_scheme_signal() {
        let headers = make_headers(&[("tus-resumable", "1.0.0"), ("host", "internal.local")]);
        let tus = Headers::from_headers(&headers).unwrap();
        // Even with forwarded headers trusted, no X-Forwarded-Proto means
        // we refuse to guess the scheme.
        let config = Config::new().with_respect_forwarded_headers();
        assert_eq!(tus.base_url(&config), None);
    }
}
