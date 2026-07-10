use dioxus::prelude::*;
use dioxus_tus::{TusConfig, TusStartOptions, file_from_event, use_tus_upload};

use crate::endpoint::use_endpoint;
use crate::ui::format_size;

/// Two halves of the same story. On the left, a normal upload whose
/// server-issued URL is surfaced from `state.upload_url`, that's the handle
/// you'd persist server-side. On the right, `start_with_url` (equivalently
/// `TusStartOptions::with_existing_url`) points a re-picked file at a known URL:
/// the client `HEAD`s it for the current offset and continues from there
/// instead of creating a new upload.
#[component]
pub fn ExistingUrlExample() -> Element {
    let endpoint = use_endpoint();
    // A small chunk size makes it easy to pause the left-hand upload partway
    // and copy its URL before it finishes.
    let (state, handle) = use_tus_upload(TusConfig::new(endpoint).with_chunk_size(64 * 1024));

    // Prefill the resume field from the live upload's URL as soon as the server
    // hands one back, so the two panels connect without copy-paste. Runs in an
    // effect (not during render). `auto_filled` tracks the value we injected so a
    // second upload's URL replaces the first, while a value the user typed or
    // edited is left untouched.
    let mut resume_url = use_signal(String::new);
    let mut auto_filled = use_signal(String::new);
    use_effect(move || {
        let Some(url) = state.read().upload_url.clone() else {
            return;
        };
        let current = resume_url.peek().clone();
        if current.is_empty() || current == *auto_filled.peek() {
            resume_url.set(url.clone());
            auto_filled.set(url);
        }
    });

    let snap = state.read();
    let pct = snap
        .progress_fraction()
        .map(|f| (f * 100.0) as i64)
        .unwrap_or(0);

    rsx! {
        div { class: "space-y-6",
            // ── Left: start a fresh upload and reveal its URL. ──────────────
            div {
                p { class: "mb-1 text-xs font-semibold uppercase tracking-wider text-base-content/45",
                    "1 · Start an upload"
                }
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
                    div { class: "mt-3",
                        progress {
                            class: "progress progress-primary w-full",
                            value: pct,
                            max: 100,
                        }
                        div { class: "mt-2 flex gap-2",
                            button {
                                class: "btn btn-xs btn-warning",
                                disabled: !snap.is_uploading(),
                                onclick: {
                                    let handle = handle.clone();
                                    move |_| handle.pause()
                                },
                                "⏸ Pause"
                            }
                            button {
                                class: "btn btn-xs btn-success",
                                disabled: !snap.is_paused(),
                                onclick: {
                                    let handle = handle.clone();
                                    move |_| handle.resume()
                                },
                                "▶ Resume"
                            }
                        }
                    }
                }

                // The server-issued upload URL, straight off the state.
                if let Some(url) = &snap.upload_url {
                    div { class: "mt-3 rounded-xl border border-base-300 bg-base-200/60 p-3",
                        p { class: "text-xs font-semibold uppercase tracking-wider text-base-content/45",
                            "state.upload_url"
                        }
                        code { class: "mt-1 block break-all font-mono text-xs text-primary", "{url}" }
                    }
                }
            }

            div { class: "divider my-0 text-xs uppercase tracking-wider text-base-content/40",
                "then, in another session"
            }

            // ── Right: resume against a known URL. ──────────────────────────
            div {
                p { class: "mb-1 text-xs font-semibold uppercase tracking-wider text-base-content/45",
                    "2 · Continue from a URL"
                }
                label { class: "form-control",
                    span { class: "label-text mb-1 text-xs font-medium", "Existing upload URL" }
                    input {
                        r#type: "text",
                        class: "input input-bordered input-sm w-full font-mono text-xs",
                        placeholder: "https://your-tus-server/files/abc123…",
                        value: "{resume_url}",
                        oninput: move |e| resume_url.set(e.value()),
                    }
                }
                input {
                    r#type: "file",
                    class: "file-input file-input-bordered file-input-sm mt-2 w-full",
                    aria_label: "Re-pick the file to continue",
                    onchange: move |evt| {
                        let url = resume_url.read().trim().to_string();
                        if url.is_empty() {
                            return;
                        }
                        if let Some(file) = file_from_event(&evt) {
                            handle.start_with_url(file, url, TusStartOptions::default());
                        }
                    },
                }
                p { class: "mt-1 text-xs text-base-content/50",
                    "Re-pick the same file; the client sends a HEAD request to the URL and resumes from the server's offset."
                }
            }

            if snap.is_complete() {
                p { class: "text-sm font-medium text-success",
                    "✓ Upload complete: {format_size(snap.bytes_uploaded)}"
                }
            }
            if let Some(error) = &snap.error {
                p { class: "text-sm text-error", "Error: {error}" }
            }
        }
    }
}
