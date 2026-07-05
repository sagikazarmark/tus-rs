//! Protocol-level hook context request facts.

use std::collections::HashMap;

use crate::config::Config;
use crate::hooks::{HookContext, HookEvent, HookRequestInfo};
use crate::state::UploadState;

use super::{Headers, UploadId};

/// Builds hook contexts from parsed request facts.
#[derive(Debug, Clone)]
pub(super) struct HookContextBuilder {
    request_info: HookRequestInfo,
}

impl HookContextBuilder {
    pub(super) fn new(config: &Config, facts: HookRequestFacts<'_>) -> Self {
        Self {
            request_info: facts.into_request_info(config),
        }
    }

    pub(super) fn request_info(&self) -> &HookRequestInfo {
        &self.request_info
    }

    pub(super) fn context(&self, event: HookEvent, upload: UploadState) -> HookContext {
        HookContext::new(event, upload, self.request_info.clone())
    }
}

/// Parsed HTTP facts needed for hook request metadata.
#[derive(Debug, Clone, Copy)]
pub(super) struct HookRequestFacts<'a> {
    method: HookRequestMethod,
    upload_id: Option<&'a UploadId>,
    headers: Option<&'a Headers>,
}

impl<'a> HookRequestFacts<'a> {
    pub(super) fn post(headers: &'a Headers) -> Self {
        Self {
            method: HookRequestMethod::Post,
            upload_id: None,
            headers: Some(headers),
        }
    }

    pub(super) fn patch(headers: &'a Headers, upload_id: &'a UploadId) -> Self {
        Self {
            method: HookRequestMethod::Patch,
            upload_id: Some(upload_id),
            headers: Some(headers),
        }
    }

    pub(super) fn head(upload_id: &'a UploadId) -> Self {
        Self {
            method: HookRequestMethod::Head,
            upload_id: Some(upload_id),
            headers: None,
        }
    }

    pub(super) fn get(upload_id: &'a UploadId) -> Self {
        Self {
            method: HookRequestMethod::Get,
            upload_id: Some(upload_id),
            headers: None,
        }
    }

    pub(super) fn delete(headers: &'a Headers, upload_id: &'a UploadId) -> Self {
        Self {
            method: HookRequestMethod::Delete,
            upload_id: Some(upload_id),
            headers: Some(headers),
        }
    }

    fn into_request_info(self, config: &Config) -> HookRequestInfo {
        HookRequestInfo {
            method: self.method.as_str().to_string(),
            path: self.path(config),
            headers: self.selected_headers(),
        }
    }

    fn path(&self, config: &Config) -> String {
        match self.upload_id {
            Some(upload_id) => format!("{}/{}", config.base_path(), upload_id.as_str()),
            None => config.base_path().to_string(),
        }
    }

    fn selected_headers(&self) -> HashMap<String, String> {
        let Some(headers) = self.headers else {
            return HashMap::new();
        };

        match self.method {
            HookRequestMethod::Post => post_headers(headers),
            HookRequestMethod::Patch => patch_headers(headers),
            HookRequestMethod::Delete => delete_headers(headers),
            HookRequestMethod::Head | HookRequestMethod::Get => HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum HookRequestMethod {
    Post,
    Patch,
    Head,
    Get,
    Delete,
}

impl HookRequestMethod {
    fn as_str(self) -> &'static str {
        match self {
            HookRequestMethod::Post => "POST",
            HookRequestMethod::Patch => "PATCH",
            HookRequestMethod::Head => "HEAD",
            HookRequestMethod::Get => "GET",
            HookRequestMethod::Delete => "DELETE",
        }
    }
}

fn post_headers(headers: &Headers) -> HashMap<String, String> {
    let mut hook_headers = HashMap::new();
    insert_upload_offset(headers, &mut hook_headers);
    insert_upload_length(headers, &mut hook_headers);
    if headers.upload_defer_length {
        hook_headers.insert("upload-defer-length".to_string(), "1".to_string());
    }
    insert_content_type(headers, &mut hook_headers);
    insert_content_length(headers, &mut hook_headers);
    hook_headers
}

fn patch_headers(headers: &Headers) -> HashMap<String, String> {
    let mut hook_headers = HashMap::new();
    insert_upload_offset(headers, &mut hook_headers);
    insert_upload_length(headers, &mut hook_headers);
    insert_content_type(headers, &mut hook_headers);
    insert_content_length(headers, &mut hook_headers);
    hook_headers
}

fn delete_headers(headers: &Headers) -> HashMap<String, String> {
    let mut hook_headers = HashMap::new();
    insert_content_type(headers, &mut hook_headers);
    hook_headers
}

fn insert_upload_offset(headers: &Headers, hook_headers: &mut HashMap<String, String>) {
    if let Some(offset) = headers.upload_offset {
        hook_headers.insert("upload-offset".to_string(), offset.to_string());
    }
}

fn insert_upload_length(headers: &Headers, hook_headers: &mut HashMap<String, String>) {
    if let Some(length) = headers.upload_length {
        hook_headers.insert("upload-length".to_string(), length.to_string());
    }
}

fn insert_content_type(headers: &Headers, hook_headers: &mut HashMap<String, String>) {
    if let Some(content_type) = &headers.content_type {
        hook_headers.insert("content-type".to_string(), content_type.clone());
    }
}

fn insert_content_length(headers: &Headers, hook_headers: &mut HashMap<String, String>) {
    if let Some(content_length) = headers.content_length {
        hook_headers.insert("content-length".to_string(), content_length.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upload_id() -> UploadId {
        "test-id".parse().unwrap()
    }

    fn request_info(config: &Config, facts: HookRequestFacts<'_>) -> HookRequestInfo {
        HookContextBuilder::new(config, facts)
            .request_info()
            .clone()
    }

    #[test]
    fn post_request_path_is_the_configured_base_path() {
        let config = Config::default().with_base_path("/uploads");
        let headers = Headers::default();

        let request = request_info(&config, HookRequestFacts::post(&headers));

        // Intentional behavior change: POST hook payloads now identify the
        // collection resource instead of leaving request.path empty.
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/uploads");
        assert!(request.headers.is_empty());
    }

    #[test]
    fn upload_request_paths_follow_the_configured_base_path() {
        let config = Config::default().with_base_path("/uploads");
        let headers = Headers::default();
        let upload_id = upload_id();

        let requests = [
            request_info(&config, HookRequestFacts::patch(&headers, &upload_id)),
            request_info(&config, HookRequestFacts::head(&upload_id)),
            request_info(&config, HookRequestFacts::get(&upload_id)),
            request_info(&config, HookRequestFacts::delete(&headers, &upload_id)),
        ];

        // Intentional behavior change: PATCH, HEAD, and DELETE no longer
        // hard-code /files when the server is mounted elsewhere.
        assert_eq!(requests[0].method, "PATCH");
        assert_eq!(requests[1].method, "HEAD");
        assert_eq!(requests[2].method, "GET");
        assert_eq!(requests[3].method, "DELETE");
        assert!(
            requests
                .iter()
                .all(|request| request.path == "/uploads/test-id")
        );
    }

    #[test]
    fn selected_headers_are_method_specific() {
        let config = Config::default();
        let headers = Headers {
            upload_offset: Some(5),
            upload_length: Some(42),
            upload_defer_length: true,
            content_length: Some(11),
            content_type: Some("application/offset+octet-stream".to_string()),
            ..Default::default()
        };
        let upload_id = upload_id();

        let post = request_info(&config, HookRequestFacts::post(&headers));
        assert_eq!(post.headers.get("upload-offset").unwrap(), "5");
        assert_eq!(post.headers.get("upload-length").unwrap(), "42");
        assert_eq!(post.headers.get("upload-defer-length").unwrap(), "1");
        assert_eq!(post.headers.get("content-length").unwrap(), "11");
        assert_eq!(
            post.headers.get("content-type").unwrap(),
            "application/offset+octet-stream"
        );

        let patch = request_info(&config, HookRequestFacts::patch(&headers, &upload_id));
        assert_eq!(patch.headers.get("upload-offset").unwrap(), "5");
        assert_eq!(patch.headers.get("upload-length").unwrap(), "42");
        assert_eq!(patch.headers.get("content-length").unwrap(), "11");
        assert_eq!(
            patch.headers.get("content-type").unwrap(),
            "application/offset+octet-stream"
        );
        assert!(!patch.headers.contains_key("upload-defer-length"));

        let delete = request_info(&config, HookRequestFacts::delete(&headers, &upload_id));
        assert_eq!(delete.headers.len(), 1);
        assert_eq!(
            delete.headers.get("content-type").unwrap(),
            "application/offset+octet-stream"
        );

        let head = request_info(&config, HookRequestFacts::head(&upload_id));
        let get = request_info(&config, HookRequestFacts::get(&upload_id));
        assert!(head.headers.is_empty());
        assert!(get.headers.is_empty());
    }

    #[test]
    fn builder_creates_contexts_with_shared_request_facts() {
        let config = Config::default().with_base_path("/uploads");
        let headers = Headers {
            upload_length: Some(42),
            ..Default::default()
        };
        let builder = HookContextBuilder::new(&config, HookRequestFacts::post(&headers));
        let upload = UploadState::new("test-id");

        let pre = builder.context(HookEvent::PreCreate, upload.clone());
        let post = builder.context(HookEvent::PostCreate, upload);

        assert_eq!(pre.event, HookEvent::PreCreate);
        assert_eq!(post.event, HookEvent::PostCreate);
        assert_eq!(pre.request.method, "POST");
        assert_eq!(post.request.method, "POST");
        assert_eq!(pre.request.path, "/uploads");
        assert_eq!(post.request.path, "/uploads");
        assert_eq!(pre.request.headers, post.request.headers);
    }
}
