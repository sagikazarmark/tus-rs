use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::app::Route;
use crate::components::{DocsCallout, ExternalAction, PageHeader, snippet_theme};

struct Feature {
    title: &'static str,
    body: &'static str,
    route: Route,
    cta: &'static str,
}

fn features() -> Vec<Feature> {
    vec![
        Feature {
            title: "Minimal by default",
            body: "A file input and one hook: chunked, resumable uploads with typed state.",
            route: Route::Minimal {},
            cta: "See the minimal example",
        },
        Feature {
            title: "Concurrent queue",
            body: "Drag-and-drop many files with per-file speed, ETA, and queue-wide controls.",
            route: Route::Queue {},
            cta: "Open the queue",
        },
        Feature {
            title: "Full control",
            body: "Pause, resume, and abort at chunk boundaries, no fighting the runtime.",
            route: Route::Controls {},
            cta: "Try the controls",
        },
        Feature {
            title: "Resumes across reloads",
            body: "Progress persists to localStorage; re-pick a file to continue where it stopped.",
            route: Route::Resume {},
            cta: "See resume",
        },
        Feature {
            title: "Resume from a server URL",
            body: "Continue a pre-created upload with start_with_url; the URL is always on state.upload_url.",
            route: Route::ExistingUrl {},
            cta: "See URL resume",
        },
        Feature {
            title: "Headers, metadata & config",
            body: "Bearer tokens, custom headers, Upload-Metadata, chunk size, retries and backoff.",
            route: Route::Options {},
            cta: "Configure an upload",
        },
        Feature {
            title: "Typed errors",
            body: "state.error is a TusError enum: branch on CORS, server status, oversize files and more.",
            route: Route::Errors {},
            cta: "Handle errors",
        },
        Feature {
            title: "Bring your own transport",
            body: "Swap the HTTP layer for any tus_uploader::Transport: middleware, proxies, or a test mock.",
            route: Route::Transport {},
            cta: "See custom transport",
        },
    ]
}

#[component]
pub fn Home() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "dioxus-tus",
            title: "Resumable uploads for Dioxus web apps",
            intro: "A headless TUS upload hook: type-safe reactive state, chunked PATCH with retry, pause / resume / abort, and resume-from-existing-URL, no stringly-typed events, no runtime to fight.",
        }

        div { class: "mt-8 flex flex-wrap gap-3",
            Link { to: Route::Minimal {}, class: "btn btn-primary", "Get started" }
            Link { to: Route::Queue {}, class: "btn btn-ghost", "Explore the queue" }
        }

        DocsCallout {
            title: "Private by default",
            "Cloudflare builds default to a Rust TUS service worker. Upload chunks stay inside this browser, file contents are discarded after processing, and only resumable offsets and metadata remain in IndexedDB. Local builds default to the native server; use the endpoint switcher to change modes."
        }

        div { class: "mt-10 grid gap-4 sm:grid-cols-2",
            for feature in features() {
                Link {
                    to: feature.route,
                    class: "group rounded-2xl border border-base-300 bg-base-100 p-5 transition-colors hover:border-primary/50",
                    h3 { class: "font-semibold tracking-tight", "{feature.title}" }
                    p { class: "mt-1.5 text-sm leading-6 text-base-content/65", "{feature.body}" }
                    span { class: "mt-3 inline-block text-sm font-medium text-primary",
                        "{feature.cta} →"
                    }
                }
            }
        }

        section { class: "mt-12",
            h2 { class: "text-xl font-semibold tracking-tight", "Quick start" }
            p { class: "mt-2 max-w-[70ch] text-sm leading-6 text-base-content/65",
                "The whole surface is one hook returning reactive state and a handle. Point it at your TUS endpoint (use the switcher in the header to try a live server):"
            }
            div { class: "mt-4 max-w-2xl",
                div { class: "overflow-x-auto rounded-2xl border border-base-300 bg-base-200/60 p-4 text-sm [&_pre]:!bg-transparent",
                    Code { src: code!("/snippets/quickstart.rs"), theme: snippet_theme() }
                }
            }
        }

        DocsCallout {
            title: "Build on the TUS protocol",
            action: Some(ExternalAction::new("Read the protocol", "https://tus.io/protocols/resumable-upload")),
            "The hook handles resumable upload mechanics while keeping progress, controls, and error presentation in your Dioxus component."
        }
    }
}
