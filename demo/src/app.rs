//! Router and the shared shell: header with a live endpoint switcher, a
//! grouped sidebar, and the routed page outlet.

use dioxus::prelude::*;
use dioxus_free_icons::Icon;
use dioxus_free_icons::icons::fa_brands_icons::FaGithub;

use crate::endpoint::{Endpoint, navigate_to_endpoint, resolve_endpoint, use_endpoint};
use crate::pages::{
    controls::Controls, errors::Errors, existing_url::ExistingUrl, headers::Headers, home::Home,
    minimal::Minimal, options::Options, queue::Queue, resume::Resume, transport::Transport,
};

const STYLE_CSS: Asset = asset!("/assets/tailwind.css");

/// Every page hangs off the one [`Layout`], so the header, sidebar, and
/// endpoint context are shared across the gallery.
#[derive(Routable, Clone, PartialEq, Debug)]
pub enum Route {
    #[layout(Layout)]
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

/// Grouped navigation shared by the desktop sidebar and the mobile strip.
fn nav_groups() -> Vec<(&'static str, Vec<(Route, &'static str)>)> {
    vec![
        (
            "Basics",
            vec![(Route::Home {}, "Overview"), (Route::Minimal {}, "Minimal")],
        ),
        (
            "Uploading",
            vec![
                (Route::Queue {}, "Upload queue"),
                (Route::Controls {}, "Pause · resume · abort"),
            ],
        ),
        (
            "Configuration",
            vec![
                (Route::Options {}, "Tokens, metadata & config"),
                (Route::Headers {}, "Headers & file naming"),
            ],
        ),
        (
            "Resuming",
            vec![
                (Route::Resume {}, "Resume across reload"),
                (Route::ExistingUrl {}, "Resume from a URL"),
            ],
        ),
        (
            "Advanced",
            vec![
                (Route::Errors {}, "Typed error handling"),
                (Route::Transport {}, "Custom transport"),
            ],
        ),
    ]
}

#[component]
pub fn App() -> Element {
    // Resolve the endpoint once and share it with every example.
    use_context_provider(|| Endpoint(resolve_endpoint()));

    rsx! {
        document::Stylesheet { href: STYLE_CSS }
        Router::<Route> {}
    }
}

#[component]
fn Layout() -> Element {
    rsx! {
        div { class: "min-h-screen bg-base-200/60 text-base-content",
            Header {}
            MobileNav {}
            div { class: "mx-auto flex w-full max-w-7xl gap-8 px-4 sm:px-6",
                Sidebar {}
                main { class: "min-w-0 flex-1 py-8 lg:py-12",
                    Outlet::<Route> {}
                    Footer {}
                }
            }
        }
    }
}

#[component]
fn Header() -> Element {
    rsx! {
        header { class: "sticky top-0 z-20 border-b border-base-300 bg-base-100/85 backdrop-blur",
            div { class: "mx-auto flex w-full max-w-7xl flex-wrap items-center gap-x-4 gap-y-3 px-4 py-3 sm:px-6",
                Link {
                    to: Route::Home {},
                    class: "flex items-center gap-2.5 font-semibold",
                    span { class: "grid size-8 place-items-center rounded-lg bg-primary text-primary-content shadow-sm",
                        "↥"
                    }
                    span { class: "text-lg tracking-tight", "dioxus-tus" }
                    span { class: "badge badge-sm badge-ghost", "demo" }
                }
                div { class: "ml-auto flex items-center gap-3",
                    EndpointSwitcher {}
                    a {
                        class: "btn btn-sm btn-ghost btn-circle",
                        href: "https://github.com/sagikazarmark/tus-rs",
                        target: "_blank",
                        rel: "noopener noreferrer",
                        "aria-label": "View tus-rs on GitHub",
                        title: "View on GitHub",
                        Icon { width: 20, height: 20, icon: FaGithub }
                    }
                }
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

#[component]
fn Sidebar() -> Element {
    rsx! {
        aside { class: "hidden w-60 shrink-0 lg:block",
            nav { class: "sticky top-20 space-y-6 py-10",
                for (group , items) in nav_groups() {
                    div {
                        p { class: "px-3 pb-2 text-xs font-semibold uppercase tracking-wider text-base-content/40",
                            "{group}"
                        }
                        ul { class: "menu w-full gap-0.5 p-0",
                            for (route , label) in items {
                                li {
                                    Link {
                                        to: route,
                                        class: "rounded-lg px-3 py-2 text-sm font-medium text-base-content/70 transition-colors hover:bg-base-200",
                                        active_class: "!bg-primary/10 !text-primary",
                                        "{label}"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Horizontal scrollable nav for narrow screens (the sidebar hides below `lg`).
#[component]
fn MobileNav() -> Element {
    rsx! {
        nav { class: "border-b border-base-300 bg-base-100/70 lg:hidden",
            div { class: "mx-auto flex w-full max-w-7xl gap-1 overflow-x-auto px-4 py-2 sm:px-6",
                for (_group , items) in nav_groups() {
                    for (route , label) in items {
                        Link {
                            to: route,
                            class: "whitespace-nowrap rounded-lg px-3 py-1.5 text-sm font-medium text-base-content/70 hover:bg-base-200",
                            active_class: "!bg-primary/10 !text-primary",
                            "{label}"
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn Footer() -> Element {
    rsx! {
        footer { class: "mt-16 border-t border-base-300 pt-6 text-sm text-base-content/50",
            "Built with "
            a {
                class: "link",
                href: "https://dioxuslabs.com",
                target: "_blank",
                rel: "noopener noreferrer",
                "Dioxus"
            }
            " · "
            a {
                class: "link",
                href: "https://tus.io",
                target: "_blank",
                rel: "noopener noreferrer",
                "the TUS protocol"
            }
            " · "
            a {
                class: "link",
                href: "https://daisyui.com",
                target: "_blank",
                rel: "noopener noreferrer",
                "DaisyUI"
            }
        }
    }
}
