use dioxus::prelude::*;
use dioxus_tus::{TusConfig, TusStartOptions, file_from_event, use_tus_upload};

/// Per-upload request shaping. `with_header` adds an arbitrary header to every
/// request for this upload (auth proxies, tenant routing, tracing); `with_filename`
/// and `with_content_type` override the `filename` / `filetype` values the hook
/// otherwise derives from the browser `File`, which land in the TUS
/// `Upload-Metadata` header. All three are set per `start()` call, so the values
/// below apply to the next file you pick.
#[component]
pub fn HeadersExample() -> Element {
    let endpoint = crate::endpoint::use_endpoint();
    let (state, handle) = use_tus_upload(TusConfig::new(endpoint));

    let mut header_name = use_signal(|| "X-Tenant-Id".to_string());
    let mut header_value = use_signal(|| "tenant-a".to_string());
    let mut filename = use_signal(String::new);
    let mut content_type = use_signal(String::new);

    let snap = state.read();
    let pct = snap
        .progress_fraction()
        .map(|f| (f * 100.0) as i64)
        .unwrap_or(0);

    rsx! {
        div { class: "space-y-4",
            div { class: "grid grid-cols-2 gap-2",
                label { class: "form-control",
                    span { class: "label-text mb-1 text-xs font-medium", "Header name" }
                    input {
                        r#type: "text",
                        class: "input input-bordered input-sm w-full font-mono text-xs",
                        value: "{header_name}",
                        oninput: move |e| header_name.set(e.value()),
                    }
                }
                label { class: "form-control",
                    span { class: "label-text mb-1 text-xs font-medium", "Header value" }
                    input {
                        r#type: "text",
                        class: "input input-bordered input-sm w-full font-mono text-xs",
                        value: "{header_value}",
                        oninput: move |e| header_value.set(e.value()),
                    }
                }
            }

            div { class: "grid grid-cols-2 gap-2",
                label { class: "form-control",
                    span { class: "label-text mb-1 text-xs font-medium", "Filename override" }
                    input {
                        r#type: "text",
                        class: "input input-bordered input-sm w-full",
                        placeholder: "keep browser name",
                        value: "{filename}",
                        oninput: move |e| filename.set(e.value()),
                    }
                }
                label { class: "form-control",
                    span { class: "label-text mb-1 text-xs font-medium", "Content-type override" }
                    input {
                        r#type: "text",
                        class: "input input-bordered input-sm w-full",
                        placeholder: "keep browser type",
                        value: "{content_type}",
                        oninput: move |e| content_type.set(e.value()),
                    }
                }
            }

            input {
                r#type: "file",
                class: "file-input file-input-bordered file-input-sm w-full",
                aria_label: "Choose a file to upload",
                onchange: move |evt| {
                    if let Some(file) = file_from_event(&evt) {
                        let mut options = TusStartOptions::default();
                        let (name, value) = (
                            header_name.read().trim().to_string(),
                            header_value.read().trim().to_string(),
                        );
                        if !name.is_empty() {
                            options = options.with_header(name, value);
                        }
                        let fname = filename.read().trim().to_string();
                        if !fname.is_empty() {
                            options = options.with_filename(fname);
                        }
                        let ctype = content_type.read().trim().to_string();
                        if !ctype.is_empty() {
                            options = options.with_content_type(ctype);
                        }
                        handle.start(file, options);
                    }
                },
            }

            p { class: "text-xs text-base-content/50",
                "The header rides on every request; filename & content-type land in `Upload-Metadata`. Empty fields fall back to the browser's own values."
            }

            if snap.bytes_total.is_some() {
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
