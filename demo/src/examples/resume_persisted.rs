use dioxus::prelude::*;
use dioxus_tus::{TusConfig, TusStartOptions, file_from_event, use_tus_upload};

use crate::endpoint::use_endpoint;

/// `resume_persisted` is the list-free convenience: instead of surfacing every
/// `scan_resumable()` entry, hand it a file and it resumes the persisted upload
/// that matches by `(endpoint, name, size, last-modified)`. It returns `true`
/// if it found and resumed a match, `false` if there was nothing to continue.
#[component]
pub fn ResumePersistedExample() -> Element {
    let endpoint = use_endpoint();
    let (state, handle) = use_tus_upload(TusConfig::new(endpoint).with_chunk_size(64 * 1024));

    // `None` until the user picks a file; then `Some(matched?)`.
    let mut matched = use_signal(|| None::<bool>);

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
                aria_label: "Re-pick a file to resume it if it was persisted",
                onchange: {
                    let handle = handle.clone();
                    move |evt| {
                        if let Some(file) = file_from_event(&evt) {
                            let did = handle.resume_persisted(file, TusStartOptions::default());
                            matched.set(Some(did));
                        }
                    }
                },
            }

            match matched() {
                Some(true) => rsx! {
                    p { class: "text-sm text-success", "Matched a persisted upload, resuming from the stored offset." }
                },
                Some(false) => rsx! {
                    p { class: "text-sm text-base-content/60",
                        "No matching persisted upload for that file. Start one on the section above, pause, reload, then re-pick it here."
                    }
                },
                None => rsx! {},
            }

            if snap.bytes_total.is_some() {
                div {
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
        }
    }
}
