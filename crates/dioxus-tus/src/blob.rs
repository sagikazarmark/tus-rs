use crate::state::TusError;
use js_sys::Uint8Array;
use wasm_bindgen_futures::JsFuture;
use web_sys::Blob;

/// Reads bytes `[start, end)` from `blob` into a `Vec<u8>`.
///
/// Uses `Blob.slice()` so only the requested range is loaded; the full
/// file is never buffered unless the caller requests the entire range.
pub(crate) async fn blob_slice_to_bytes(
    blob: &Blob,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, TusError> {
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
pub(crate) fn blob_size(blob: &Blob) -> u64 {
    blob.size() as u64
}

#[cfg(test)]
mod tests {
    // `blob_slice_to_bytes` is `pub(crate)`, so this unit test lives with the
    // helper rather than in the `tests/` integration suite. Browser-only.
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    /// Create a synthetic `web_sys::File` from bytes.
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
    async fn blob_slice_reads_correct_bytes() {
        let file = make_file("test.bin", b"abcdefghij");
        let blob: web_sys::Blob = file.into();

        let chunk = blob_slice_to_bytes(&blob, 2, 6).await.expect("slice");
        assert_eq!(chunk, b"cdef");
    }
}
