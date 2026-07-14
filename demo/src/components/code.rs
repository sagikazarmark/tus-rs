//! Code presentation for docs-by-example applications.

use dioxus::prelude::*;
use dioxus_code::{CodeTheme, Theme};

/// Theme for every on-page code snippet. The demo has one light-only theme, so
/// the code palette stays fixed to match it.
pub fn snippet_theme() -> CodeTheme {
    CodeTheme::fixed(Theme::GITHUB_LIGHT)
}

/// Inline monospace styling for an API name or identifier mentioned in prose.
#[component]
pub fn InlineCode(children: Element) -> Element {
    rsx! {
        code { class: "rounded bg-base-200 px-1.5 py-0.5 font-mono text-[0.85em] text-base-content/80",
            {children}
        }
    }
}
