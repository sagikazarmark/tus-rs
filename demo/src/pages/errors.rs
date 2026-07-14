use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::components::{ExampleSection, PageHeader, snippet_theme};
use crate::examples::errors::ErrorsExample;

#[component]
pub fn Errors() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Advanced",
            title: "Typed error handling",
            intro: "state.error is a TusError enum, not a string. Match on it to distinguish a transient network failure from a server 4xx/5xx, a CORS block, a file that exceeds Tus-Max-Size, and more, then show the user something they can act on.",
        }
        ExampleSection {
            title: "Branching on TusError",
            intro: rsx! {
                "Pick a file, then drive it into different failures with the buttons. Each variant renders with its kind, the library's message, and a remediation hint."
            },
            demo: rsx! {
                ErrorsExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/errors.rs"), theme: snippet_theme() }
            },
        }
    }
}
