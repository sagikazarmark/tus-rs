use async_trait::async_trait;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use super::UploadSource;
use crate::{
    Error, Result, Transport, TransportRequest, TransportResponse, transport::TransportBody,
};

pub(super) fn endpoint_url() -> url::Url {
    url::Url::parse("http://example.test/files").unwrap()
}

pub(super) fn upload_url(path: &str) -> url::Url {
    url::Url::parse(&format!("http://example.test/files/{path}")).unwrap()
}

#[derive(Clone, Default)]
pub(super) struct MockTransport {
    pub(super) requests: Arc<Mutex<Vec<TransportRequest>>>,
    pub(super) responses: Arc<Mutex<VecDeque<std::result::Result<TransportResponse, Error>>>>,
}

#[cfg_attr(
    all(not(feature = "local-futures"), not(target_arch = "wasm32")),
    async_trait
)]
#[cfg_attr(
    any(feature = "local-futures", target_arch = "wasm32"),
    async_trait(?Send)
)]
impl Transport for MockTransport {
    async fn send(&self, request: TransportRequest) -> Result<TransportResponse> {
        self.requests.lock().unwrap().push(request);
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err(Error::Transport("missing mock response".to_string())))
    }
}

#[derive(Clone)]
pub(super) struct ShortReadSource {
    pub(super) bytes: Vec<u8>,
    pub(super) max_read: usize,
}

#[cfg_attr(
    all(not(feature = "local-futures"), not(target_arch = "wasm32")),
    async_trait
)]
#[cfg_attr(
    any(feature = "local-futures", target_arch = "wasm32"),
    async_trait(?Send)
)]
impl UploadSource for ShortReadSource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    async fn read_chunk(&mut self, offset: u64, max_len: usize) -> Result<Vec<u8>> {
        let offset = offset as usize;
        let Some(bytes) = self.bytes.get(offset..) else {
            return Ok(Vec::new());
        };
        let len = bytes.len().min(max_len).min(self.max_read);
        Ok(bytes[..len].to_vec())
    }
}

pub(super) fn mock_head_response(offset: u64, length: u64) -> TransportResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("upload-offset"),
        HeaderValue::from_str(&offset.to_string()).unwrap(),
    );
    headers.insert(
        HeaderName::from_static("upload-length"),
        HeaderValue::from_str(&length.to_string()).unwrap(),
    );
    transport_response(200, headers, Vec::new())
}

pub(super) fn mock_patch_response(offset: u64) -> TransportResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("upload-offset"),
        HeaderValue::from_str(&offset.to_string()).unwrap(),
    );
    transport_response(204, headers, Vec::new())
}

pub(super) fn header_map(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_static(value),
        );
    }
    headers
}

pub(super) fn transport_response(
    status: u16,
    headers: HeaderMap,
    body: Vec<u8>,
) -> TransportResponse {
    let mut response = http::Response::builder().status(status).body(body).unwrap();
    *response.headers_mut() = headers;
    response
}

pub(super) fn expect_unexpected_status<T>(
    result: std::result::Result<T, Error>,
    expected_operation: &'static str,
    expected_status: u16,
) {
    match result {
        Err(Error::UnexpectedResponse {
            operation, status, ..
        }) => {
            assert_eq!(operation, expected_operation);
            assert_eq!(status, expected_status);
        }
        _ => panic!("expected UnexpectedResponse({expected_operation}, {expected_status})"),
    }
}

#[allow(dead_code)]
pub(super) fn body_bytes(body: &TransportBody) -> &Vec<u8> {
    match body {
        TransportBody::Bytes(bytes) => bytes,
        other => panic!("expected byte body, got {other:?}"),
    }
}
