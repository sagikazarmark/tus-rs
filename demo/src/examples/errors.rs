use dioxus::prelude::*;
use dioxus_tus::{TusConfig, TusError, TusStartOptions, file_from_event, use_tus_upload};

use crate::endpoint::use_endpoint;

/// `state.error` is a typed [`TusError`], not a string, so the UI can branch on
/// the failure mode and give a specific, actionable message. Pick a file, then
/// use the buttons to drive it into different failure modes and watch the typed
/// breakdown below react.
#[component]
pub fn ErrorsExample() -> Element {
    let endpoint = use_endpoint();
    let (state, handle) = use_tus_upload(TusConfig::new(endpoint.clone()));

    // Hold the chosen file so the scenario buttons can re-run it against
    // different URLs without a fresh picker each time.
    let mut file = use_signal(|| None::<web_sys::File>);
    let has_file = file.read().is_some();

    let snap = state.read();

    rsx! {
        div { class: "space-y-4",
            input {
                r#type: "file",
                class: "file-input file-input-bordered file-input-sm w-full",
                aria_label: "Choose a file",
                onchange: move |evt| {
                    file.set(file_from_event(&evt));
                },
            }

            div { class: "flex flex-wrap gap-2",
                button {
                    class: "btn btn-sm btn-primary",
                    disabled: !has_file,
                    onclick: {
                        let handle = handle.clone();
                        move |_| {
                            if let Some(f) = file.read().clone() {
                                handle.start(f, TusStartOptions::default());
                            }
                        }
                    },
                    "Upload normally"
                }
                button {
                    class: "btn btn-sm btn-outline btn-error",
                    disabled: !has_file,
                    title: "HEAD a non-existent upload URL",
                    onclick: {
                        let handle = handle.clone();
                        let endpoint = endpoint.clone();
                        move |_| {
                            if let Some(f) = file.read().clone() {
                                let url = format!("{}/does-not-exist-0000", endpoint.trim_end_matches('/'));
                                handle.start_with_url(f, url, TusStartOptions::default());
                            }
                        }
                    },
                    "Trigger a 404"
                }
                button {
                    class: "btn btn-sm btn-outline btn-error",
                    disabled: !has_file,
                    title: "HEAD an unreachable host",
                    onclick: {
                        let handle = handle.clone();
                        move |_| {
                            if let Some(f) = file.read().clone() {
                                handle
                                    .start_with_url(
                                        f,
                                        "http://127.0.0.1:9/blocked",
                                        TusStartOptions::default(),
                                    );
                            }
                        }
                    },
                    "Trigger a network failure"
                }
            }

            p { class: "text-xs text-base-content/50",
                "Tip: point the header's endpoint switcher at a server without CORS to see the "
                code { class: "font-mono", "Cors" }
                " variant on a normal upload."
            }

            // Typed error breakdown.
            match &snap.error {
                Some(err) => error_card(err),
                None if snap.is_complete() => rsx! {
                    p { class: "text-sm font-medium text-success", "✓ Upload complete, no errors" }
                },
                None if snap.is_uploading() => rsx! {
                    p { class: "text-sm text-base-content/60", "Uploading…" }
                },
                None => rsx! {},
            }
        }
    }
}

/// Renders one typed [`TusError`] as a labelled card: the variant name, the
/// library's message, and a human hint for what to do about it. A plain
/// function rather than a `#[component]` because `TusError` isn't `PartialEq`,
/// so it can't be a memoised component prop.
fn error_card(err: &TusError) -> Element {
    let (kind, hint) = classify(err);
    rsx! {
        div { role: "alert", class: "rounded-2xl border border-error/40 bg-error/5 p-4",
            div { class: "flex items-center gap-2",
                span { class: "badge badge-error badge-sm font-mono", "{kind}" }
                span { class: "text-sm font-medium text-error", "{err}" }
            }
            p { class: "mt-2 text-xs text-base-content/60", "{hint}" }
        }
    }
}

/// Maps each `TusError` variant to a short kind label and a remediation hint.
/// The `_` arm keeps this compiling as new (`#[non_exhaustive]`) variants land.
fn classify(err: &TusError) -> (&'static str, &'static str) {
    match err {
        TusError::Transport(_) => (
            "Transport",
            "The request never reached the server (DNS, connection refused, TLS, or an abrupt disconnect). Retried automatically before surfacing.",
        ),
        TusError::Server { .. } => (
            "Server",
            "The server returned a 4xx/5xx. Inspect the status and body; a 404 here means the upload URL doesn't exist.",
        ),
        TusError::Cors => (
            "Cors",
            "The browser blocked the request. Configure the server's Access-Control-Allow-* / Expose-Headers (see the README's CORS section).",
        ),
        TusError::MissingHeader(_) => (
            "MissingHeader",
            "A required TUS response header was absent, usually CORS Access-Control-Expose-Headers is not exposing it.",
        ),
        TusError::InvalidHeader { .. } => (
            "InvalidHeader",
            "A required header was present but unparseable (e.g. a non-numeric Upload-Offset), a misbehaving server or proxy.",
        ),
        TusError::BlobRead(_) => (
            "BlobRead",
            "The browser could not read the File/Blob contents; the file may have been moved or revoked since it was picked.",
        ),
        TusError::InvalidUrl(_) => (
            "InvalidUrl",
            "The endpoint or upload URL could not be parsed. Check the value in the endpoint switcher.",
        ),
        TusError::ServerMissingExtension(_) => (
            "ServerMissingExtension",
            "The server's Tus-Extension header doesn't advertise a required extension; enable it server-side.",
        ),
        TusError::FileTooLarge { .. } => (
            "FileTooLarge",
            "The file exceeds the server's Tus-Max-Size, caught before any bytes are sent, so no partial upload is created.",
        ),
        _ => (
            "Other",
            "A newer error variant this demo doesn't classify yet; the Display message above still describes it.",
        ),
    }
}
