//! Router and the shared shell: header with a live endpoint switcher, grouped
//! sidebar, routed page outlet, and footer.

use dioxus::prelude::*;

use crate::components::{DemoFooter, DemoHeader, Sidebar, SidebarNavLink, SidebarNavSection};
use crate::endpoint::{
    Endpoint, is_browser_endpoint, navigate_to_browser_endpoint, navigate_to_endpoint,
    prepare_browser_endpoint, resolve_endpoint, server_endpoint, use_endpoint,
};
use crate::pages::{
    controls::Controls, errors::Errors, existing_url::ExistingUrl, headers::Headers, home::Home,
    minimal::Minimal, options::Options, queue::Queue, resume::Resume, transport::Transport,
};

const STYLE: Asset = asset!("/build/style.css");

/// Every page hangs off the one [`DemoLayout`], so the header, sidebar, and
/// endpoint context are shared across the gallery.
#[derive(Routable, Clone, PartialEq, Debug)]
pub enum Route {
    #[layout(DemoLayout)]
    #[route("/")]
    Home {},
    #[route("/minimal")]
    Minimal {},
    #[route("/queue")]
    Queue {},
    #[route("/controls")]
    Controls {},
    #[route("/options")]
    Options {},
    #[route("/headers")]
    Headers {},
    #[route("/resume")]
    Resume {},
    #[route("/existing-url")]
    ExistingUrl {},
    #[route("/errors")]
    Errors {},
    #[route("/transport")]
    Transport {},
}

#[component]
pub fn App() -> Element {
    let endpoint = resolve_endpoint();
    let needs_worker = is_browser_endpoint(&endpoint);
    let mut endpoint_status = use_signal(|| {
        if needs_worker {
            EndpointStatus::Starting
        } else {
            EndpointStatus::Ready
        }
    });

    // Resolve the endpoint once and share it with every example.
    use_context_provider({
        let endpoint = endpoint.clone();
        move || Endpoint(endpoint)
    });

    use_effect(move || {
        if needs_worker {
            spawn(async move {
                endpoint_status.set(match prepare_browser_endpoint().await {
                    Ok(()) => EndpointStatus::Ready,
                    Err(error) => EndpointStatus::Failed(error),
                });
            });
        }
    });

    let body = match endpoint_status.read().clone() {
        EndpointStatus::Starting => rsx! {
            EndpointStartup {
                title: "Starting the browser-local TUS endpoint",
                detail: "The upload examples will appear after the Rust service worker is ready.",
            }
        },
        EndpointStatus::Failed(error) => rsx! {
            EndpointStartup {
                title: "The browser-local endpoint could not start",
                detail: "{error}. Serve the demo from localhost or HTTPS, or add ?endpoint=https%3A%2F%2Fyour-server%2Ffiles to use another TUS server.",
            }
        },
        EndpointStatus::Ready => rsx! { Router::<Route> {} },
    };

    rsx! {
        document::Stylesheet { href: STYLE }
        {body}
    }
}

#[derive(Clone)]
enum EndpointStatus {
    Starting,
    Ready,
    Failed(String),
}

#[component]
fn EndpointStartup(title: &'static str, detail: String) -> Element {
    rsx! {
        main { class: "grid min-h-screen place-items-center bg-base-100 px-6 text-base-content",
            div { class: "max-w-lg rounded-3xl border border-base-300 bg-base-200/40 p-8 shadow-sm",
                div { class: "mb-5 grid h-11 w-11 place-items-center rounded-2xl bg-primary font-bold text-primary-content",
                    "tu"
                }
                h1 { class: "text-2xl font-bold tracking-tight", "{title}" }
                p { class: "mt-3 leading-7 text-base-content/65", "{detail}" }
            }
        }
    }
}

/// Shared application shell for every demo route.
#[component]
fn DemoLayout() -> Element {
    rsx! {
        div { class: "min-h-screen bg-base-100 text-base-content",
            DemoHeader {
                home: Route::Home {},
                mark: "tu",
                name: "dioxus-tus",
                github_url: "https://github.com/sagikazarmark/tus-rs",
                actions: Some(rsx! { EndpointSwitcher {} }),
            }
            div { class: "mx-auto w-full max-w-7xl lg:flex lg:gap-8 lg:px-6",
                Sidebar {
                    SidebarNavSection { label: "Basics",
                        SidebarNavLink { route: Route::Home {}, label: "Overview" }
                        SidebarNavLink { route: Route::Minimal {}, label: "Minimal" }
                    }
                    SidebarNavSection { label: "Uploading",
                        SidebarNavLink { route: Route::Queue {}, label: "Upload queue" }
                        SidebarNavLink { route: Route::Controls {}, label: "Pause · resume · abort" }
                    }
                    SidebarNavSection { label: "Configuration",
                        SidebarNavLink { route: Route::Options {}, label: "Tokens, metadata & config" }
                        SidebarNavLink { route: Route::Headers {}, label: "Headers & file naming" }
                    }
                    SidebarNavSection { label: "Resuming",
                        SidebarNavLink { route: Route::Resume {}, label: "Resume across reload" }
                        SidebarNavLink { route: Route::ExistingUrl {}, label: "Resume from a URL" }
                    }
                    SidebarNavSection { label: "Advanced",
                        SidebarNavLink { route: Route::Errors {}, label: "Typed error handling" }
                        SidebarNavLink { route: Route::Transport {}, label: "Custom transport" }
                    }
                }
                main { id: "main-content", class: "min-w-0 flex-1 px-4 py-8 sm:px-6 lg:px-0 lg:py-12",
                    Outlet::<Route> {}
                }
            }
            DemoFooter {
                description: "A docs-by-example gallery for the dioxus-tus library.",
                links: rsx! {
                    a {
                        class: "hover:text-base-content",
                        href: "https://github.com/sagikazarmark/tus-rs",
                        target: "_blank",
                        rel: "noopener noreferrer",
                        "Repository"
                    }
                    a {
                        class: "hover:text-base-content",
                        href: "https://tus.io",
                        target: "_blank",
                        rel: "noopener noreferrer",
                        "TUS protocol"
                    }
                },
            }
        }
    }
}

/// Header control that shows the active TUS endpoint and lets you re-point the
/// whole demo at another server (reloads with a fresh `?endpoint=`).
#[component]
fn EndpointSwitcher() -> Element {
    let current = use_endpoint();
    let browser_local = is_browser_endpoint(&current);
    let mut value = use_signal(|| {
        if browser_local {
            server_endpoint()
        } else {
            current.clone()
        }
    });
    let toggle_browser_local = move |_| {
        if browser_local {
            navigate_to_endpoint(&server_endpoint());
        } else {
            navigate_to_browser_endpoint(&current);
        }
    };

    let submit = move |evt: FormEvent| {
        // Suppress the browser's native form navigation; we do our own
        // full-page navigation via `navigate_to_endpoint`.
        evt.prevent_default();
        let next = value.read().trim().to_string();
        if !next.is_empty() {
            navigate_to_endpoint(&next);
        }
    };

    rsx! {
        label {
            class: "flex cursor-pointer items-center gap-2 whitespace-nowrap rounded-lg px-2 py-1 text-xs font-medium text-base-content/65 hover:bg-base-200 hover:text-base-content",
            title: if browser_local {
                "Switch to the configured TUS server"
            } else {
                "Process uploads in this browser and discard their bytes"
            },
            span { class: "hidden xl:inline", "Browser-local" }
            input {
                class: "toggle toggle-success toggle-sm",
                r#type: "checkbox",
                checked: browser_local,
                aria_label: "Use browser-local TUS endpoint",
                onchange: toggle_browser_local,
            }
        }
        form {
            class: "join hidden sm:flex",
            onsubmit: submit,
            div { class: "join-item grid place-items-center border border-base-300 border-r-0 bg-base-200 px-2 text-[0.65rem] font-semibold uppercase tracking-wider text-base-content/50",
                "Endpoint"
            }
            input {
                class: "input input-sm input-bordered join-item w-64 font-mono text-xs",
                r#type: "text",
                value: "{value}",
                spellcheck: false,
                aria_label: "TUS endpoint URL",
                oninput: move |e| value.set(e.value()),
            }
            button { class: "btn btn-sm btn-primary join-item", r#type: "submit", "Set" }
        }
    }
}
