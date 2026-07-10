use dioxus::prelude::*;
use dioxus_tus::tus_client::{Result, Transport, TransportRequest, TransportResponse};
use dioxus_tus::transport::GlooNetTransport;
use dioxus_tus::{TusConfig, TusStartOptions, file_from_event, use_tus_upload_with_transport};

use crate::endpoint::use_endpoint;

/// `use_tus_upload_with_transport` lets you swap the HTTP layer for any
/// `tus_client::Transport`. Here we wrap the default browser transport
/// ([`GlooNetTransport`]) to log every request and response to a signal, which
/// the panel below renders live. The same seam is how you'd add auth
/// middleware, a service-worker proxy, or a mock transport for tests.
#[component]
pub fn TransportExample() -> Element {
    let endpoint = use_endpoint();
    let log = use_signal(Vec::<String>::new);

    // A small chunk size produces several PATCHes per upload, so the log shows
    // the real request rhythm (POST, then PATCH per chunk).
    let (state, handle) = use_tus_upload_with_transport(
        TusConfig::new(endpoint).with_chunk_size(64 * 1024),
        LoggingTransport {
            log,
            inner: GlooNetTransport,
        },
    );

    let snap = state.read();
    let pct = snap
        .progress_fraction()
        .map(|f| (f * 100.0) as i64)
        .unwrap_or(0);
    let lines = log.read().clone();

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

            // The transport's live request log.
            div {
                p { class: "mb-2 text-xs font-semibold uppercase tracking-wider text-base-content/45",
                    "Transport log"
                }
                if lines.is_empty() {
                    p { class: "text-sm text-base-content/50",
                        "Pick a file: every HTTP request the transport makes appears here."
                    }
                } else {
                    div { class: "max-h-56 overflow-y-auto rounded-xl border border-base-300 bg-base-200/60 p-3 font-mono text-xs leading-relaxed",
                        for (i , line) in lines.iter().enumerate() {
                            div { key: "{i}", class: "whitespace-pre-wrap break-all", "{line}" }
                        }
                    }
                }
            }

            if let Some(error) = &snap.error {
                p { class: "text-sm text-error", "Error: {error}" }
            }
        }
    }
}

/// A `Transport` decorator: it delegates to `inner` and records a line for each
/// request and its outcome into a Dioxus signal. `Transport` requires `Clone`;
/// `Signal` is `Copy` and `GlooNetTransport` is `Clone`, so a derive suffices.
#[derive(Clone)]
struct LoggingTransport {
    log: Signal<Vec<String>>,
    inner: GlooNetTransport,
}

#[async_trait::async_trait(?Send)]
impl Transport for LoggingTransport {
    async fn send(&self, request: TransportRequest) -> Result<TransportResponse> {
        let method = request.method().clone();
        let uri = request.uri().to_string();

        // `Signal` is `Copy`; a local `mut` binding lets us write from this
        // background task (the same context the hook writes its state from).
        let mut log = self.log;
        log.write().push(format!("→ {method} {uri}"));

        let result = self.inner.send(request).await;
        match &result {
            Ok(resp) => log.write().push(format!("  ↳ {}", resp.status().as_u16())),
            Err(err) => log.write().push(format!("  ✗ {err}")),
        }

        // Keep the on-page log bounded.
        let mut guard = log.write();
        let len = guard.len();
        if len > 40 {
            guard.drain(0..len - 40);
        }

        result
    }
}
