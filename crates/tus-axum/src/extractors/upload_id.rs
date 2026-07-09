use axum::{
    extract::{FromRequestParts, Path},
    http::request::Parts,
};

use crate::error::TusRejection;

/// Axum path extractor for a validated TUS upload ID.
///
/// The `pub` field is a deliberate 1.0 commitment: it lets handlers destructure
/// `TusUploadId(id)` in the signature, matching every other axum extractor. See
/// ADR 0006.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TusUploadId(pub tus_protocol::UploadId);

impl TusUploadId {
    pub(crate) fn from_string(upload_id: String) -> Result<Self, TusRejection> {
        Ok(tus_protocol::UploadId::try_from(upload_id).map(Self)?)
    }
}

impl<S> FromRequestParts<S> for TusUploadId
where
    S: Send + Sync,
{
    type Rejection = TusRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Path(upload_id): Path<String> =
            Path::from_request_parts(parts, state)
                .await
                .map_err(|err| {
                    tus_protocol::Error::InvalidUploadId(format!("path extraction failed: {err}"))
                })?;

        Self::from_string(upload_id)
    }
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use tower::ServiceExt;

    use super::TusUploadId;

    async fn echo(TusUploadId(upload_id): TusUploadId) -> impl IntoResponse {
        upload_id.to_string()
    }

    async fn dispatch(uri: &str) -> StatusCode {
        let app = Router::new().route("/uploads/{upload_id}", get(echo));
        app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn extracts_valid_upload_id() {
        let app = Router::new().route("/uploads/{upload_id}", get(echo));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/uploads/test-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn rejects_invalid_upload_id() {
        assert_eq!(
            dispatch("/uploads/foo%2Fbar").await,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn malformed_path_capture_returns_bad_request() {
        assert_eq!(dispatch("/uploads/%FF").await, StatusCode::BAD_REQUEST);
    }
}
