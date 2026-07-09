use dioxus::prelude::*;
use dioxus_tus::persistence::ResumableEntry;
use dioxus_tus::{TusConfig, TusStartOptions, TusUploadHandle, file_from_event, use_tus_upload};

use crate::endpoint::use_endpoint;
use crate::ui::format_size;

/// Resume an upload after a tab reload. In-flight progress is persisted to
/// `localStorage`; on mount, `scan_resumable()` surfaces any partial upload so
/// the user can re-pick the same file and continue from the server's offset.
#[component]
pub fn ResumeExample() -> Element {
    let endpoint = use_endpoint();
    // A small chunk size + a couple of pauses make it easy to reload mid-upload.
    let (state, handle) = use_tus_upload(TusConfig::new(endpoint).with_chunk_size(64 * 1024));

    // Re-scanned every render (a cheap synchronous localStorage read), so rows
    // disappear as uploads complete or are aborted.
    let resumable = handle.scan_resumable();

    let snap = state.read();
    let pct = snap
        .progress_fraction()
        .map(|f| (f * 100.0) as i64)
        .unwrap_or(0);

    rsx! {
        div { class: "space-y-4",
            if !resumable.is_empty() {
                div { role: "status", class: "rounded-2xl border border-info/40 bg-info/5 p-4",
                    p { class: "text-sm font-medium", "Resume from a prior session?" }
                    p { class: "text-xs text-base-content/60",
                        "Click an entry and re-pick the same file to continue."
                    }
                    ul { class: "mt-2 space-y-1",
                        for entry in resumable.iter() {
                            {resume_row(entry, handle.clone())}
                        }
                    }
                }
            }

            div {
                p { class: "mb-1 text-xs font-semibold uppercase tracking-wider text-base-content/45",
                    "Start a new upload"
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
                    p { class: "mt-2 text-xs text-base-content/50",
                        "Pause, then reload the tab — the entry above lets you continue."
                    }
                }
            }
        }
    }
}

/// One persisted-entry row: a `<label>` wrapping a hidden file input, so
/// clicking it opens a picker scoped to that upload. The re-picked file is
/// validated against the entry's match key before resuming.
fn resume_row(entry: &ResumableEntry, handle: TusUploadHandle) -> Element {
    let entry = entry.clone();
    let label = format!(
        "{} — {} / {}",
        entry.filename,
        format_size(entry.bytes_uploaded),
        format_size(entry.file_size),
    );

    rsx! {
        li {
            label { class: "flex cursor-pointer items-center gap-2 rounded-lg px-2 py-1.5 hover:bg-base-100",
                input {
                    r#type: "file",
                    class: "hidden",
                    onchange: move |evt| {
                        if let Some(file) = file_from_event(&evt) {
                            let _ = handle.resume_entry(&entry, file, TusStartOptions::default());
                        }
                    },
                }
                span { class: "flex-1 truncate text-sm", "{label}" }
                span { class: "text-xs font-medium text-primary", "Pick file →" }
            }
        }
    }
}
