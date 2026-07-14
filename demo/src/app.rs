//! Router and the shared shell: header with a live endpoint switcher, grouped
//! sidebar, routed page outlet, and footer.

use dioxus::prelude::*;

use crate::components::{DemoFooter, DemoHeader, Sidebar, SidebarNavLink, SidebarNavSection};
use crate::endpoint::{Endpoint, navigate_to_endpoint, resolve_endpoint, use_endpoint};
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
    // Resolve the endpoint once and share it with every example.
    use_context_provider(|| Endpoint(resolve_endpoint()));

    rsx! {
        document::Stylesheet { href: STYLE }
        Router::<Route> {}
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
                main { class: "min-w-0 flex-1 px-4 py-8 sm:px-6 lg:px-0 lg:py-12",
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
    let mut value = use_signal(|| current.clone());

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
