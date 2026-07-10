//! Shared presentation helpers. Pure layout chrome; nothing here touches
//! `dioxus-tus`, so the `pages` and `examples` modules stay focused on the
//! library being demonstrated.

use dioxus::prelude::*;
use dioxus_code::Theme;

/// Theme for every on-page snippet, defined once so they all match. The demo is
/// light by default, so a light code theme keeps the panels cohesive.
pub fn snippet_theme() -> Theme {
    Theme::GITHUB_LIGHT
}

/// Consistent page heading: a small colored eyebrow, a title, and a lead
/// paragraph. `intro` is an `Element` so it can carry inline [`InlineCode`] and
/// links rather than rendering literal backticks.
#[component]
pub fn PageHeader(
    #[props(into)] eyebrow: String,
    #[props(into)] title: String,
    intro: Element,
) -> Element {
    rsx! {
        header { class: "max-w-3xl",
            p { class: "text-sm font-semibold uppercase tracking-[0.18em] text-primary", "{eyebrow}" }
            h1 { class: "mt-3 text-4xl font-bold tracking-tight text-balance", "{title}" }
            p { class: "mt-4 text-lg leading-8 text-base-content/70", {intro} }
        }
    }
}

/// Inline monospace styling for an API or type name mentioned in prose (e.g.
/// `InlineCode { "use_tus_upload" }`). Keeps every code reference reading the
/// same, and avoids literal backticks in rendered text.
#[component]
pub fn InlineCode(children: Element) -> Element {
    rsx! {
        code { class: "rounded bg-base-200 px-1.5 py-0.5 font-mono text-[0.85em] text-base-content/80",
            {children}
        }
    }
}

/// A single documented example: heading, short explanation, the live component,
/// and the exact source that produced it in a scrollable code panel.
///
/// `intro` is an `Element` so it can carry inline [`InlineCode`] and links. By
/// default the live demo and its source sit side by side; set `stacked` for
/// wider examples (a multi-panel layout, a request log) whose natural width
/// overflows a half-width column, so the source drops below a full-width live
/// demo instead of fighting it for horizontal space.
#[component]
pub fn ExampleSection(
    #[props(into)] title: String,
    intro: Element,
    demo: Element,
    code: Element,
    #[props(default = false)] stacked: bool,
) -> Element {
    // Stacked lays the demo and source out in one full-width column;
    // side-by-side keeps them in a two-column grid on large screens.
    let layout_class = if stacked {
        "mt-6 grid grid-cols-1 gap-6"
    } else {
        "mt-6 grid gap-6 lg:grid-cols-2"
    };
    rsx! {
        section { class: "mt-10 rounded-[1.5rem] border border-base-300 bg-base-100 p-6 shadow-sm sm:p-8",
            h2 { class: "text-xl font-semibold tracking-tight", "{title}" }
            p { class: "mt-2 max-w-[70ch] text-sm leading-6 text-base-content/65", {intro} }
            div { class: "{layout_class}",
                // Live column.
                div {
                    p { class: "mb-3 text-xs font-semibold uppercase tracking-wider text-base-content/45",
                        "Live"
                    }
                    div { class: "overflow-x-auto rounded-2xl border border-base-300 bg-base-200/40 p-5", {demo} }
                }
                // Source column.
                div {
                    p { class: "mb-3 text-xs font-semibold uppercase tracking-wider text-base-content/45",
                        "Source"
                    }
                    SourcePanel { code }
                }
            }
        }
    }
}

/// Card chrome around a `dioxus_code::Code` block: a scrollable, bordered panel
/// whose own background shows through the theme's `<pre>`.
#[component]
pub fn SourcePanel(code: Element) -> Element {
    rsx! {
        div { class: "source-block overflow-x-auto rounded-2xl border border-base-300 bg-base-200/60 p-4 text-sm [&_pre]:!m-0 [&_pre]:!bg-transparent",
            {code}
        }
    }
}

/// Inline link to external documentation.
#[component]
pub fn DocLink(#[props(into)] href: String, children: Element) -> Element {
    rsx! {
        a {
            class: "link link-primary",
            href: "{href}",
            target: "_blank",
            rel: "noopener noreferrer",
            {children}
        }
    }
}

/// Pretty-prints a bytes-per-second value as `1.2 MB/s`, `350 KB/s`, etc.
pub fn format_bytes_per_sec(bps: f64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    if bps >= MB {
        format!("{:.1} MB/s", bps / MB)
    } else if bps >= KB {
        format!("{:.0} KB/s", bps / KB)
    } else {
        format!("{bps:.0} B/s")
    }
}

/// Pretty-prints a byte count as `4.0 MB`, `512 KB`, etc.
pub fn format_size(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    let b = bytes as f64;
    if b >= MB {
        format!("{:.1} MB", b / MB)
    } else if b >= KB {
        format!("{:.0} KB", b / KB)
    } else {
        format!("{bytes} B")
    }
}

/// Pretty-prints an ETA in seconds as `45s left`, `2m left`, etc.
pub fn format_eta(seconds: Option<f64>) -> String {
    match seconds {
        Some(s) if s.is_finite() && s > 0.0 => {
            if s < 60.0 {
                format!("{}s left", s as u64)
            } else if s < 3600.0 {
                format!("{}m left", (s / 60.0) as u64)
            } else {
                format!("{:.1}h left", s / 3600.0)
            }
        }
        _ => "n/a".to_string(),
    }
}
