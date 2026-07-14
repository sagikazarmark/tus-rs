use std::{io, str::FromStr};

use futures_util::StreamExt;
use js_sys::{Uint8Array, decode_uri_component};
use tus_protocol::{
    BodyFrame, Error, Headers as TusHeaders, NoopHookExecutor, NoopLocker, ProtocolHandle,
    RequestBody, Response as TusResponse, UploadId,
    bytes::Bytes,
    http::{HeaderMap, HeaderName, HeaderValue, Method},
};
use wasm_bindgen::JsValue;
use wasm_streams::ReadableStream;
use web_sys::{Headers, Request, Response, ResponseInit, Url};

use crate::database::BrowserDatabase;

type BrowserProtocol =
    ProtocolHandle<BrowserDatabase, BrowserDatabase, NoopLocker, NoopHookExecutor>;

const COLLECTION_ALLOW: &str = "OPTIONS, POST";
const UPLOAD_ALLOW: &str = "OPTIONS, HEAD, POST, PATCH, DELETE";
const REQUEST_HEADER_NAMES: &[&str] = &[
    "tus-resumable",
    "upload-offset",
    "upload-length",
    "upload-defer-length",
    "upload-metadata",
    "upload-checksum",
    "upload-concat",
    "content-length",
    "content-type",
    "transfer-encoding",
    "host",
    "x-forwarded-host",
    "x-forwarded-proto",
];

pub(crate) async fn dispatch(
    protocol: &BrowserProtocol,
    base_path: &str,
    request: Request,
) -> Result<Response, JsValue> {
    let method = effective_method(&request)?;
    let route = route(&Url::new(&request.url())?.pathname(), base_path);
    let suppress_body = method == Method::HEAD;
    let allow = match &route {
        Route::Collection => Some(COLLECTION_ALLOW),
        Route::Upload(_) => Some(UPLOAD_ALLOW),
        Route::NotFound => None,
    };

    let result = dispatch_protocol(protocol, method.clone(), route, &request).await;

    match result {
        Ok(response) => success_response(response, suppress_body),
        Err(error) => protocol_error_response(error, allow, suppress_body),
    }
}

async fn dispatch_protocol(
    protocol: &BrowserProtocol,
    method: Method,
    route: Route,
    request: &Request,
) -> tus_protocol::Result<TusResponse> {
    match (method.clone(), route) {
        (Method::OPTIONS, Route::Collection | Route::Upload(_)) => Ok(protocol.options()),
        (Method::POST, Route::Collection) => {
            let headers = tus_headers(&request.headers())?;
            let body = request_body(request);
            protocol.post(headers, body).await
        }
        (Method::HEAD, Route::Upload(id)) => {
            tus_headers(&request.headers())?;
            protocol.head(&id).await
        }
        (Method::PATCH, Route::Upload(id)) => {
            let headers = tus_headers(&request.headers())?;
            let body = request_body(request);
            protocol.patch(headers, &id, body).await
        }
        (Method::DELETE, Route::Upload(id)) => {
            let headers = tus_headers(&request.headers())?;
            protocol.delete(headers, &id).await
        }
        (_, Route::NotFound) => Err(Error::NotFound("browser endpoint route".to_string())),
        _ => Err(Error::MethodNotAllowed(method.to_string())),
    }
}

fn effective_method(request: &Request) -> Result<Method, JsValue> {
    let mut method = Method::from_bytes(request.method().as_bytes())
        .map_err(|_| JsValue::from_str("invalid HTTP method"))?;
    if method == Method::POST
        && let Some(value) = request.headers().get("x-http-method-override")?
    {
        method = Method::from_bytes(value.as_bytes())
            .map_err(|_| JsValue::from_str("invalid X-HTTP-Method-Override"))?;
    }
    Ok(method)
}

fn route(path: &str, base_path: &str) -> Route {
    if path == base_path || path == format!("{base_path}/") {
        return Route::Collection;
    }
    let Some(segment) = path.strip_prefix(&format!("{base_path}/")) else {
        return Route::NotFound;
    };
    if segment.is_empty() || segment.contains('/') {
        return Route::NotFound;
    }
    let Ok(decoded) = decode_uri_component(segment) else {
        return Route::NotFound;
    };
    decoded
        .as_string()
        .and_then(|id| UploadId::from_str(&id).ok())
        .map(Route::Upload)
        .unwrap_or(Route::NotFound)
}

fn tus_headers(headers: &Headers) -> Result<TusHeaders, Error> {
    let mut values = HeaderMap::new();
    for name in REQUEST_HEADER_NAMES {
        let value = headers.get(name).map_err(|_| Error::InvalidHeader {
            header: "browser-header",
            message: format!("could not read {name}"),
        })?;
        let Some(value) = value else {
            continue;
        };
        values.insert(
            HeaderName::from_static(name),
            HeaderValue::from_str(&value).map_err(|_| Error::InvalidHeader {
                header: "browser-header",
                message: format!("invalid value for {name}"),
            })?,
        );
    }
    TusHeaders::from_headers(&values)
}

fn request_body(request: &Request) -> RequestBody {
    let Some(body) = request.body() else {
        return RequestBody::absent();
    };
    let stream = ReadableStream::from_raw(body).into_stream().map(|chunk| {
        chunk
            .map(|value| BodyFrame::Data(Bytes::from(Uint8Array::new(&value).to_vec())))
            .map_err(|_| io::Error::other("could not read browser request body"))
    });
    RequestBody::from_stream(Box::pin(stream))
}

fn success_response(response: TusResponse, suppress_body: bool) -> Result<Response, JsValue> {
    let TusResponse {
        status,
        headers,
        body,
        ..
    } = response;
    response_from_parts(
        status.as_u16(),
        headers
            .iter()
            .filter_map(|(name, value)| value.to_str().ok().map(|value| (name.as_str(), value))),
        body.as_ref(),
        suppress_body,
    )
}

fn protocol_error_response(
    error: Error,
    allow: Option<&str>,
    suppress_body: bool,
) -> Result<Response, JsValue> {
    let parts = error.error_response();
    let mut headers: Vec<(&str, &str)> = parts
        .headers
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .collect();
    if matches!(error, Error::MethodNotAllowed(_))
        && let Some(allow) = allow
    {
        headers.push(("allow", allow));
    }
    response_from_parts(
        parts.status,
        headers.into_iter(),
        parts.body.as_bytes(),
        suppress_body,
    )
}

fn response_from_parts<'a>(
    status: u16,
    headers: impl Iterator<Item = (&'a str, &'a str)>,
    body: &[u8],
    suppress_body: bool,
) -> Result<Response, JsValue> {
    let web_headers = Headers::new()?;
    for (name, value) in headers {
        web_headers.append(name, value)?;
    }
    let init = ResponseInit::new();
    init.set_status(status);
    init.set_headers(web_headers.as_ref());
    if suppress_body || body.is_empty() || status == 204 {
        Response::new_with_opt_str_and_init(None, &init)
    } else {
        let mut body = body.to_vec();
        Response::new_with_opt_u8_array_and_init(Some(&mut body), &init)
    }
}

enum Route {
    Collection,
    Upload(UploadId),
    NotFound,
}
