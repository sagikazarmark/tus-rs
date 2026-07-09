use dioxus::prelude::*;
use dioxus_tus::{TusConfig, TusStartOptions, file_from_event, use_tus_upload};

use crate::endpoint::use_endpoint;

/// The smallest useful uploader: pick a file, watch it upload.
#[component]
pub fn MinimalExample() -> Element {
    let endpoint = use_endpoint();
    let (state, handle) = use_tus_upload(TusConfig::new(endpoint));

    let snap = state.read();
    let pct = snap
        .progress_fraction()
        .map(|f| (f * 100.0) as i64)
        .unwrap_or(0);

    rsx! {
        div { class: "space-y-4",
            input {
                r#type: "file",
                class: "file-input file-input-bordered file-input-sm w-full",
                aria_label: "Choose a file to upload",
                onchange: move |evt| {
                    if let Some(file) = file_from_event(&evt) {
                        handle.start(file, TusStartOptions::default());
                    }
                },
            }

            if snap.is_uploading() || snap.is_complete() {
                div {
                    progress {
                        class: "progress progress-primary w-full",
                        value: pct,
                        max: 100,
                    }
                    p { class: "mt-1 text-sm text-base-content/60", "{pct}%" }
                }
            }

            if snap.is_complete() {
                p { class: "text-sm font-medium text-success", "✓ Upload complete" }
            }
            if let Some(error) = &snap.error {
                p { class: "text-sm text-error", "Error: {error}" }
            }
        }
    }
}
