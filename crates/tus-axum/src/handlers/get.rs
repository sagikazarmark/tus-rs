//! Axum adapter for download GET requests.

use axum::{body::Body, extract::State, response::Response};
use http::{HeaderMap, header};

use tus_protocol::{DownloadRequest, HookExecutor, Locker, StateStore, Storage, StorageReader};

use crate::error::Error;
use crate::extractors::UploadId;
use crate::state::TusProtocol;

/// Handles GET requests that download an uploaded file.
///
/// This is a native-server convenience endpoint, not part of the core tus
/// upload protocol. It is available unless disabled in [`tus_protocol::Config`].
pub async fn handle_get<S, I, L, H>(
    State(protocol): State<TusProtocol<S, I, L, H>>,
    headers: HeaderMap,
    UploadId(upload_id): UploadId,
) -> Result<Response, Error>
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

    let response = protocol
        .download(DownloadRequest {
            upload_id: &upload_id,
            range,
        })
        .await?;

    let tus_protocol::DownloadResponse {
        status,
        headers,
        body,
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
