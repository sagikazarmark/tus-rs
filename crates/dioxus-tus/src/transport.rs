use async_trait::async_trait;
use gloo_net::http::RequestBuilder;
use http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode};
use std::str::FromStr;
use tus_client::{Error, Transport, TransportBody, TransportRequest, TransportResponse};

#[derive(Clone)]
pub struct GlooNetTransport;

#[async_trait(?Send)]
impl Transport for GlooNetTransport {
    async fn send(&self, request: TransportRequest) -> tus_client::Result<TransportResponse> {
        let (parts, body) = request.into_parts();
        let url = parts.uri.to_string();
        let mut builder = RequestBuilder::new(&url).method(parts.method.clone());

        for (name, value) in parts.headers.iter() {
            let v = value.to_str().map_err(|_| {
                Error::transport_permanent(format!("non-utf8 header value for {name}"))
            })?;
            builder = builder.header(name.as_str(), v);
        }

        // Browser Fetch cannot send HTTP trailers. Failing closed (a
        // permanent, non-retryable error) is safer than silently dropping a
        // checksum the caller explicitly requested.
        let body_bytes: Option<Vec<u8>> = match body {
            TransportBody::Empty => None,
            TransportBody::Bytes(b) => Some(b),
            TransportBody::BytesWithTrailer { .. } => {
                return Err(Error::transport_permanent(
                    "browser Fetch does not support HTTP trailers; use checksum header mode",
                ));
            }
            // `TransportBody` is `#[non_exhaustive]`; fail closed on a body
            // shape this transport was not written to send rather than
            // silently dropping bytes.
            _ => {
                return Err(Error::transport_permanent(
                    "unsupported transport body variant for browser Fetch",
                ));
            }
        };

        let response = if let Some(bytes) = body_bytes {
            builder
                .body(bytes)
                .map_err(Error::transport_permanent)?
                .send()
                .await
                .map_err(Error::transport)?
        } else {
            builder.send().await.map_err(Error::transport)?
        };

        let status = StatusCode::from_u16(response.status()).map_err(Error::transport_permanent)?;
        let mut headers = HeaderMap::new();
        for (name, value) in response.headers().entries() {
            if let (Ok(n), Ok(v)) = (
                HeaderName::from_str(&name),
                HeaderValue::from_bytes(value.as_bytes()),
            ) {
                // append (not insert) preserves repeated headers like
                // Set-Cookie / Link. TUS itself doesn't repeat headers, but
                // the underlying response can carry non-TUS headers a proxy
                // injects, and silently dropping the second copy is wrong.
                headers.append(n, v);
            }
        }

        let body = response.binary().await.map_err(Error::transport)?;

        let mut http_response = Response::new(body);
        *http_response.status_mut() = status;
        *http_response.headers_mut() = headers;
        Ok(http_response)
    }
}

// The `BytesWithTrailer` fail-closed path is intentionally not unit-tested
// here: `tus_client::TransportBody` is `#[non_exhaustive]`, so an external
// crate (this one) cannot construct the `BytesWithTrailer` variant to feed it
// in, and `GlooNetTransport::send` performs a real browser `fetch`, which needs
// a live server and a browser runtime to exercise. The trailer rejection is a
// simple `match` arm in `send` above; end-to-end coverage lives in the
// server-backed integration tests (`tests/wasm_transport.rs`).
