use crate::state::TusError;
use js_sys::Uint8Array;
use wasm_bindgen_futures::JsFuture;
use web_sys::Blob;

/// Reads bytes `[start, end)` from `blob` into a `Vec<u8>`.
///
/// Uses `Blob.slice()` so only the requested range is loaded; the full
/// file is never buffered unless the caller requests the entire range.
pub async fn blob_slice_to_bytes(blob: &Blob, start: u64, end: u64) -> Result<Vec<u8>, TusError> {
    let sliced = blob
        .slice_with_f64_and_f64(start as f64, end as f64)
        .map_err(|e| TusError::BlobRead(format!("{e:?}")))?;

    let array_buffer = JsFuture::from(sliced.array_buffer())
        .await
        .map_err(|e| TusError::BlobRead(format!("{e:?}")))?;

    let uint8 = Uint8Array::new(&array_buffer);
    Ok(uint8.to_vec())
}

/// Total byte size of `blob`, as declared by the browser.
pub fn blob_size(blob: &Blob) -> u64 {
    blob.size() as u64
}
