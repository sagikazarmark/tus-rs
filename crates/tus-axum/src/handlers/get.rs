//! Axum adapter for download GET requests.

use axum::{body::Body, extract::State, response::IntoResponse, response::Response};
use http::{HeaderMap, HeaderValue, header};

use tus_protocol::{DownloadRequest, HookExecutor, Locker, StateStore, Storage, StorageReader};

use crate::error::TusRejection;
use crate::extractors::TusUploadId;
use crate::router::UPLOAD_ALLOW;
use crate::state::TusProtocol;

/// Handles GET requests that download an uploaded file.
///
/// This is a native-server convenience endpoint, not part of the core tus
/// upload protocol. It is available unless disabled in [`tus_protocol::Config`].
pub(crate) async fn handle_get<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    headers: HeaderMap,
    TusUploadId(upload_id): TusUploadId,
) -> Result<Response, TusRejection>
where
    S: Storage + StorageReader + Send + Sync + 'static,
    I: StateStore + Send + Sync + 'static,
    L: Locker + Send + Sync + 'static,
    H: HookExecutor + Send + Sync + 'static,
{
    let range = headers
        .get(header::RANGE)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| tus_protocol::Error::InvalidHeader {
                    header: "Range",
                    message: "header must be valid ASCII".to_string(),
                })
        })
        .transpose()?;

    let response = match protocol
        .handle()
        .download(DownloadRequest::new(&upload_id).with_range(range))
        .await
    {
        Ok(response) => response,
        // Downloads disabled in config: RFC 9110 requires 405 responses to
        // carry an `Allow` header. GET is excluded because the config
        // rejects it even though the route is registered.
        Err(err @ tus_protocol::Error::MethodNotAllowed(_)) => {
            let mut response = TusRejection::from(err).into_response();
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static(UPLOAD_ALLOW));
            return Ok(response);
        }
        Err(err) => return Err(err.into()),
    };

    let tus_protocol::DownloadResponse {
        status,
        headers,
        body,
        ..
    } = response;

    let mut builder = Response::builder().status(status);
    if let Some(out_headers) = builder.headers_mut() {
        *out_headers = headers;
    }

    let response = builder.body(Body::from_stream(body)).map_err(|err| {
        tus_protocol::Error::Internal(format!("failed to build download response: {err}"))
    })?;

    Ok(response)
}
