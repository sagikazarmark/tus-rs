use dioxus::prelude::*;
use dioxus_tus::{
    QueueItemStatus, TusConfig, TusQueueHandle, TusQueueItem, TusStartOptions,
    files_from_drag_event, files_from_event, use_tus_upload_queue,
};

use crate::endpoint::use_endpoint;
use crate::ui::{format_bytes_per_sec, format_eta, format_size};

/// A concurrent, drag-and-drop upload queue built on `use_tus_upload_queue`:
/// multiple files upload in parallel with per-file and queue-wide controls.
#[component]
pub fn QueueExample() -> Element {
    let endpoint = use_endpoint();
    let (queue, handle) = use_tus_upload_queue(TusConfig::new(endpoint));
    let mut drag_active = use_signal(|| false);

    let items = queue.read().items.clone();
    let any_active = items
        .iter()
        .any(|i| matches!(i.status, QueueItemStatus::Uploading));
    let any_paused = items
        .iter()
        .any(|i| matches!(i.status, QueueItemStatus::Paused));
    let any_finished = items.iter().any(|i| {
        matches!(
            i.status,
            QueueItemStatus::Complete | QueueItemStatus::Aborted | QueueItemStatus::Error
        )
    });

    rsx! {
        div { class: "space-y-4",
            // Drag-and-drop + multi-file picker zone.
            div {
                class: format!(
                    "rounded-2xl border-2 border-dashed p-6 text-center transition-colors {}",
                    if drag_active() { "border-primary bg-primary/5" } else { "border-base-300" },
                ),
                aria_label: "Drop files or click to browse",
                ondragover: move |evt| {
                    evt.prevent_default();
                    drag_active.set(true);
                },
                ondragleave: move |_| drag_active.set(false),
                ondrop: {
                    let handle = handle.clone();
                    move |evt| {
                        evt.prevent_default();
                        drag_active.set(false);
                        let files = files_from_drag_event(&evt);
                        if !files.is_empty() {
                            handle.add_all(files, TusStartOptions::default());
                        }
                    }
                },
                p { class: "mb-3 text-sm text-base-content/60", "Drop files here, or:" }
                input {
                    r#type: "file",
                    class: "file-input file-input-bordered file-input-sm w-full max-w-xs",
                    aria_label: "Choose files to upload",
                    multiple: true,
                    onchange: {
                        let handle = handle.clone();
                        move |evt| {
                            let files = files_from_event(&evt);
                            if !files.is_empty() {
                                handle.add_all(files, TusStartOptions::default());
                            }
                        }
                    },
                }
            }

            // Queue-wide controls.
            div { class: "flex flex-wrap justify-end gap-2",
                button {
                    class: "btn btn-xs btn-warning",
                    disabled: !any_active,
                    onclick: {
                        let handle = handle.clone();
                        move |_| handle.pause_all()
                    },
                    "Pause all"
                }
                button {
                    class: "btn btn-xs btn-success",
                    disabled: !any_paused,
                    onclick: {
                        let handle = handle.clone();
                        move |_| handle.resume_all()
                    },
                    "Resume all"
                }
                button {
                    class: "btn btn-xs btn-error btn-outline",
                    disabled: !(any_active || any_paused),
                    onclick: {
                        let handle = handle.clone();
                        move |_| handle.abort_all()
                    },
                    "Abort all"
                }
                button {
                    class: "btn btn-xs btn-ghost",
                    disabled: !any_finished,
                    onclick: {
                        let handle = handle.clone();
                        move |_| handle.clear_finished()
                    },
                    "Clear finished"
                }
            }

            // Queue list.
            if items.is_empty() {
                p { class: "py-6 text-center text-sm text-base-content/50",
                    "No uploads yet — drop files above to get started."
                }
            } else {
                div { class: "divide-y divide-base-300 overflow-hidden rounded-2xl border border-base-300",
                    role: "list",
                    for item in items.iter() {
                        {queue_row(item, handle.clone())}
                    }
                }
            }
        }
    }
}

/// One queue row. A plain function rather than a `#[component]` because the
/// item + handle props can't implement `PartialEq` (`web_sys::File` and the
/// handle's interior signals), so the memoised-component path doesn't apply.
fn queue_row(item: &TusQueueItem, handle: TusQueueHandle) -> Element {
    let progress = if item.file_size > 0 {
        item.bytes_uploaded as f64 / item.file_size as f64
    } else if matches!(item.status, QueueItemStatus::Complete) {
        1.0
    } else {
        0.0
    };
    let pct = (progress * 100.0) as i64;
    let id = item.id;

    let is_uploading = matches!(item.status, QueueItemStatus::Uploading);
    let is_paused = matches!(item.status, QueueItemStatus::Paused);
    let can_retry = matches!(
        item.status,
        QueueItemStatus::Error | QueueItemStatus::Aborted
    );

    let now_ms = js_sys::Date::now();
    let detail = match &item.status {
        QueueItemStatus::Queued => "Queued".to_string(),
        QueueItemStatus::Uploading => match item.speed_bytes_per_sec(now_ms) {
            Some(bps) => format!(
                "{pct}% · {} · {}",
                format_bytes_per_sec(bps),
                format_eta(item.eta_seconds(now_ms)),
            ),
            None => format!("{pct}%"),
        },
        QueueItemStatus::Paused => "Paused".to_string(),
        QueueItemStatus::Complete => "Complete".to_string(),
        QueueItemStatus::Error => item
            .error
            .as_ref()
            .map(|e| format!("Error: {e}"))
            .unwrap_or_else(|| "Error".to_string()),
        QueueItemStatus::Aborted => "Aborted".to_string(),
        other => format!("{other:?}"),
    };
    let detail_class = match &item.status {
        QueueItemStatus::Complete => "text-success",
        QueueItemStatus::Error => "text-error",
        QueueItemStatus::Aborted => "text-warning",
        _ => "text-base-content/60",
    };

    let handle_pause = handle.clone();
    let handle_resume = handle.clone();
    let handle_retry = handle.clone();
    let handle_remove = handle.clone();

    rsx! {
        div { class: "flex items-center gap-3 bg-base-100 p-3", role: "listitem",
            div { class: "min-w-0 flex-1",
                div { class: "flex items-baseline justify-between gap-2",
                    span { class: "truncate text-sm font-medium", "{item.file_name}" }
                    span { class: "shrink-0 text-xs text-base-content/40", "{format_size(item.file_size)}" }
                }
                progress {
                    class: "progress progress-primary mt-1 w-full",
                    value: pct,
                    max: 100,
                    aria_label: "Upload progress for {item.file_name}",
                }
                div {
                    class: "mt-1 text-xs {detail_class}",
                    role: "status",
                    "aria-live": "polite",
                    "{detail}"
                }
            }
            div { class: "flex gap-1",
                button {
                    class: "btn btn-ghost btn-xs",
                    disabled: !is_uploading,
                    title: "Pause",
                    onclick: move |_| handle_pause.pause_item(id),
                    "⏸"
                }
                button {
                    class: "btn btn-ghost btn-xs",
                    disabled: !is_paused,
                    title: "Resume",
                    onclick: move |_| handle_resume.resume_item(id),
                    "▶"
                }
                button {
                    class: "btn btn-ghost btn-xs",
                    disabled: !can_retry,
                    title: "Retry from scratch",
                    onclick: move |_| handle_retry.retry_item(id),
                    "↻"
                }
                button {
                    class: "btn btn-ghost btn-xs text-error",
                    title: "Remove",
                    onclick: move |_| handle_remove.remove_item(id),
                    "✕"
                }
            }
        }
    }
}
