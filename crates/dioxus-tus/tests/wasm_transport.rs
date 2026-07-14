#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

fn test_endpoint() -> String {
    // Set at wasm-pack invocation time:
    //   TUS_TEST_ENDPOINT=http://localhost:8080/files wasm-pack test --headless --chrome
    // Falls back to a local default for convenience.
    option_env!("TUS_TEST_ENDPOINT")
        .unwrap_or("http://localhost:8080/files")
        .to_string()
}

/// Create a synthetic web_sys::File from bytes.
fn make_file(name: &str, content: &[u8]) -> web_sys::File {
    use js_sys::{Array, Uint8Array};
    let uint8 = Uint8Array::from(content);
    let array = Array::new();
    array.push(&uint8);
    let options = web_sys::FilePropertyBag::new();
    options.set_type("application/octet-stream");
    web_sys::File::new_with_u8_array_sequence_and_options(&array, name, &options)
        .expect("File creation failed")
}

#[wasm_bindgen_test]
async fn gloo_transport_creates_upload_and_gets_location() {
    use dioxus_tus::transport::GlooNetTransport;
    use tus_uploader::url::Url;
    use tus_uploader::{Client, NewUpload, UploadMetadata};

    let endpoint = test_endpoint();
    let client = Client::with_transport(Url::parse(&endpoint).unwrap(), GlooNetTransport);

    let (_upload, info) = client
        .create_upload(NewUpload::new(11, UploadMetadata::new()))
        .await
        .expect("create_upload failed");

    assert!(
        info.url().as_str().starts_with(&endpoint),
        "upload url {} not rooted at endpoint {}",
        info.url(),
        endpoint
    );
    assert_eq!(info.offset(), 0);
}

#[wasm_bindgen_test]
async fn patch_chunk_delivers_bytes_and_advances_offset() {
    use dioxus_tus::transport::GlooNetTransport;
    use tus_uploader::url::Url;
    use tus_uploader::{Client, NewUpload, UploadMetadata};

    let endpoint = test_endpoint();
    let client = Client::with_transport(Url::parse(&endpoint).unwrap(), GlooNetTransport);

    let content = b"hello world";
    let (upload, info) = client
        .create_upload(NewUpload::new(content.len() as u64, UploadMetadata::new()))
        .await
        .expect("create");

    let new_offset = upload
        .upload_chunk(0, content.to_vec())
        .await
        .expect("patch");

    assert_eq!(new_offset, content.len() as u64);

    let final_info = client
        .upload_at(info.url().as_str())
        .unwrap()
        .info()
        .await
        .expect("head");
    assert_eq!(final_info.offset(), content.len() as u64);

    let url = info.url().to_string();
    let downloaded = gloo_net::http::Request::get(&url)
        .send()
        .await
        .expect("download")
        .binary()
        .await
        .expect("download bytes");
    assert_eq!(downloaded, content);
}

/// Reads bytes `[start, end)` from `blob` via the public browser APIs. Mirrors
/// the crate-internal `blob::blob_slice_to_bytes` (which is `pub(crate)` and
/// unit-tested in `src/blob.rs`); reproduced here so this integration test can
/// drive a real chunked upload loop without reaching into crate internals.
async fn read_blob_slice(blob: &web_sys::Blob, start: u64, end: u64) -> Vec<u8> {
    use js_sys::Uint8Array;
    use wasm_bindgen_futures::JsFuture;

    let sliced = blob
        .slice_with_f64_and_f64(start as f64, end as f64)
        .expect("slice");
    let array_buffer = JsFuture::from(sliced.array_buffer())
        .await
        .expect("array_buffer");
    Uint8Array::new(&array_buffer).to_vec()
}

#[wasm_bindgen_test]
async fn full_upload_via_client_and_blob_slice() {
    use dioxus_tus::transport::GlooNetTransport;
    use tus_uploader::url::Url;
    use tus_uploader::{Client, NewUpload, UploadMetadata};

    let endpoint = test_endpoint();
    let content = b"wasm hook test data";
    let file = make_file("wasm_test.bin", content);
    let blob: web_sys::Blob = file.clone().into();

    let client = Client::with_transport(Url::parse(&endpoint).unwrap(), GlooNetTransport);

    let (upload, info) = client
        .create_upload(NewUpload::new(content.len() as u64, UploadMetadata::new()))
        .await
        .expect("create");

    let mut offset = 0u64;
    let chunk_size = 8u64;

    while offset < content.len() as u64 {
        let end = (offset + chunk_size).min(content.len() as u64);
        let chunk = read_blob_slice(&blob, offset, end).await;
        offset = upload.upload_chunk(offset, chunk).await.expect("patch");
    }

    let final_info = client
        .upload_at(info.url().as_str())
        .unwrap()
        .info()
        .await
        .expect("head");
    assert_eq!(final_info.offset(), content.len() as u64);

    let url = info.url().to_string();
    let downloaded = gloo_net::http::Request::get(&url)
        .send()
        .await
        .expect("download")
        .binary()
        .await
        .expect("download bytes");
    assert_eq!(downloaded, content);
}

/// Pins the resume-from-existing-URL contract: after a partial upload, a
/// fresh client can pick up the same URL via HEAD and continue from the
/// server's stored offset. This is what `TusUploadHandle::start_with_url`
/// and the resume-across-reload (PR 3) flow rely on.
#[wasm_bindgen_test]
async fn resume_from_existing_url_continues_from_server_offset() {
    use dioxus_tus::transport::GlooNetTransport;
    use tus_uploader::url::Url;
    use tus_uploader::{Client, NewUpload, UploadMetadata};

    let endpoint = test_endpoint();
    let content = b"resume-target-bytes-padding-padding";
    let chunk_split = 12u64;

    // Phase 1: create + upload a partial.
    let url = {
        let client = Client::with_transport(Url::parse(&endpoint).unwrap(), GlooNetTransport);
        let (upload, info) = client
            .create_upload(NewUpload::new(content.len() as u64, UploadMetadata::new()))
            .await
            .expect("create");
        let new_offset = upload
            .upload_chunk(0, content[..chunk_split as usize].to_vec())
            .await
            .expect("partial patch");
        assert_eq!(new_offset, chunk_split);
        info.url().to_string()
    };

    // Phase 2: fresh client, no internal state, resumes via HEAD.
    let client2 = Client::with_transport(Url::parse(&endpoint).unwrap(), GlooNetTransport);
    let upload2 = client2.upload_at(&url).unwrap();
    let resume = upload2.info().await.expect("head on existing url");
    assert_eq!(resume.offset(), chunk_split, "server-side offset preserved");
    assert_eq!(resume.length(), Some(content.len() as u64));

    // Phase 3: continue from the resumed offset.
    let final_offset = upload2
        .upload_chunk(chunk_split, content[chunk_split as usize..].to_vec())
        .await
        .expect("resume patch");
    assert_eq!(final_offset, content.len() as u64);

    // Phase 4: HEAD confirms full upload.
    let done = upload2.info().await.expect("final head");
    assert_eq!(done.offset(), content.len() as u64);
}
