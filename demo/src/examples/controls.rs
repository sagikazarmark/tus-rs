use dioxus::prelude::*;
use dioxus_tus::{TusConfig, TusStartOptions, file_from_event, use_tus_upload};

use crate::endpoint::use_endpoint;
use crate::ui::format_size;

/// A single upload driven by explicit pause / resume / abort controls. Pause
/// and resume act at the next chunk boundary; abort resets to idle.
#[component]
pub fn ControlsExample() -> Element {
    let endpoint = use_endpoint();
    // A small chunk size makes pause/resume visible even on fast connections.
    let (state, handle) = use_tus_upload(TusConfig::new(endpoint).with_chunk_size(64 * 1024));

    let snap = state.read();
    let pct = snap
        .progress_fraction()
        .map(|f| (f * 100.0) as i64)
        .unwrap_or(0);
    let uploaded = snap.bytes_uploaded;
    let total = snap.bytes_total.unwrap_or(0);

    rsx! {
        div { class: "space-y-4",
            input {
                r#type: "file",
                class: "file-input file-input-bordered file-input-sm w-full",
                aria_label: "Choose a file to upload",
                onchange: {
                    let handle = handle.clone();
                    move |evt| {
                        if let Some(file) = file_from_event(&evt) {
                            handle.start(file, TusStartOptions::default());
                        }
                    }
                },
            }

            if snap.bytes_total.is_some() {
                div {
                    progress {
                        class: "progress progress-primary w-full",
                        value: pct,
                        max: 100,
                    }
                    p { class: "mt-1 text-sm text-base-content/60",
                        "{pct}% · {format_size(uploaded)} / {format_size(total)}"
                    }
                }
            }

            div { class: "flex flex-wrap gap-2",
                button {
                    class: "btn btn-sm btn-warning",
                    disabled: !snap.is_uploading(),
                    onclick: {
                        let handle = handle.clone();
                        move |_| handle.pause()
                    },
                    "⏸ Pause"
                }
                button {
                    class: "btn btn-sm btn-success",
                    disabled: !snap.is_paused(),
                    onclick: {
                        let handle = handle.clone();
                        move |_| handle.resume()
                    },
                    "▶ Resume"
                }
                button {
                    class: "btn btn-sm btn-error btn-outline",
                    disabled: !(snap.is_uploading() || snap.is_paused()),
                    onclick: move |_| handle.abort(),
                    "✕ Abort"
                }
            }

            StatusLine { state: state }
        }
    }
}

#[component]
fn StatusLine(state: ReadSignal<dioxus_tus::TusUploadState>) -> Element {
    let snap = state.read();
    let (label, class) = if snap.is_complete() {
        ("Complete", "badge-success")
    } else if snap.is_paused() {
        ("Paused", "badge-warning")
    } else if snap.is_uploading() {
        ("Uploading", "badge-info")
    } else if snap.is_error() {
        ("Error", "badge-error")
    } else {
        ("Idle", "badge-ghost")
    };

    rsx! {
        div { class: "flex items-center gap-2 text-sm",
            span { class: "badge {class} badge-sm", "{label}" }
            if let Some(error) = &snap.error {
                span { class: "text-error", "{error}" }
            }
        }
    }
}
