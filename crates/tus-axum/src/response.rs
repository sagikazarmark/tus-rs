//! Conversion between [`tus_protocol::Response`] and axum responses.

use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
};

/// Newtype around [`tus_protocol::Response`] that carries axum's [`IntoResponse`] impl.
///
/// The inner value is crate-private: `TusResponse` is only ever produced from a
/// [`tus_protocol::Response`] (via [`From`]) and consumed by axum (via
/// [`IntoResponse`]), so keeping the field private lets the wrapper evolve
/// without a breaking change.
#[derive(Debug)]
pub struct TusResponse(pub(crate) tus_protocol::Response);

impl From<tus_protocol::Response> for TusResponse {
    fn from(response: tus_protocol::Response) -> Self {
        Self(response)
    }
}

impl IntoResponse for TusResponse {
    fn into_response(self) -> Response {
        let tus_protocol::Response {
            status,
            headers,
            body,
            ..
        } = self.0;

        let mut builder = Response::builder().status(status);

        if let Some(out_headers) = builder.headers_mut() {
            *out_headers = headers;
        }

        builder
            .body(Body::from(body))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use bytes::Bytes;
    use http::StatusCode;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn into_response_preserves_protocol_parts() {
        let response = tus_protocol::Response::new(StatusCode::CREATED)
            .with_header("location", "/files/test-id")
            .with_body(Bytes::from_static(b"created"));

        let response = TusResponse(response).into_response();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers().get("location").unwrap(),
            "/files/test-id"
        );

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"created");
    }
}
