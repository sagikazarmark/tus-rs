use dioxus::prelude::*;
use dioxus_tus::{TusConfig, TusStartOptions, file_from_event, use_tus_upload};

use crate::endpoint::use_endpoint;

/// Per-upload options (a bearer token and custom metadata) plus client-level
/// config (chunk size, retries). Config is fixed when the hook is created;
/// `TusStartOptions` vary per `start()` call, so the token and metadata below
/// apply to the next upload you pick.
#[component]
pub fn OptionsExample() -> Element {
    let endpoint = use_endpoint();
    // Client-level config, set once at construction. Chunk size and the retry
    // budget/backoff are fixed here; `creation_with_upload_threshold` decides
    // which small files POST their bytes in the creation request. A config-level
    // `with_bearer_token` would set a default token for every upload; the
    // per-upload `with_bearer_token` below overrides it.
    let (state, handle) = use_tus_upload(
        TusConfig::new(endpoint)
            .with_chunk_size(512 * 1024)
            .with_max_retries(5)
            .with_retry_delay_ms(250)
            .with_creation_with_upload_threshold(256 * 1024),
    );

    let mut token = use_signal(String::new);
    let mut meta_key = use_signal(|| "album".to_string());
    let mut meta_value = use_signal(|| "vacation".to_string());

    let snap = state.read();
    let pct = snap
        .progress_fraction()
        .map(|f| (f * 100.0) as i64)
        .unwrap_or(0);

    rsx! {
        div { class: "space-y-4",
            label { class: "form-control",
                span { class: "label-text mb-1 text-xs font-medium", "Bearer token (optional)" }
                input {
                    r#type: "text",
                    class: "input input-bordered input-sm w-full font-mono text-xs",
                    placeholder: "eyJhbGci…",
                    value: "{token}",
                    oninput: move |e| token.set(e.value()),
                }
            }

            div { class: "grid grid-cols-2 gap-2",
                label { class: "form-control",
                    span { class: "label-text mb-1 text-xs font-medium", "Metadata key" }
                    input {
                        r#type: "text",
                        class: "input input-bordered input-sm w-full",
                        value: "{meta_key}",
                        oninput: move |e| meta_key.set(e.value()),
                    }
                }
                label { class: "form-control",
                    span { class: "label-text mb-1 text-xs font-medium", "Metadata value" }
                    input {
                        r#type: "text",
                        class: "input input-bordered input-sm w-full",
                        value: "{meta_value}",
                        oninput: move |e| meta_value.set(e.value()),
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
                        let tok = token.read().trim().to_string();
                        if !tok.is_empty() {
                            options = options.with_bearer_token(tok);
                        }
                        let (k, v) = (meta_key.read().trim().to_string(), meta_value.read().trim().to_string());
                        if !k.is_empty() {
                            options = options.with_metadata(k, v);
                        }
                        handle.start(file, options);
                    }
                },
            }

            p { class: "text-xs text-base-content/50",
                "Config: 512 KiB chunks · 5 retries · 250 ms base backoff · 256 KiB creation-with-upload threshold. Token and metadata apply per upload."
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
