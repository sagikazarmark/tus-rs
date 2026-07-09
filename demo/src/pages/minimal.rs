use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::examples::minimal::MinimalExample;
use crate::ui::{ExampleSection, PageHeader, snippet_theme};

#[component]
pub fn Minimal() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Basics",
            title: "Minimal uploader",
            intro: "One hook, one file input. `use_tus_upload` returns reactive state and a handle; call `handle.start(file, …)` and read `state` to render progress.",
        }
        ExampleSection {
            title: "use_tus_upload",
            intro: "The state signal drives the progress bar and the completion / error messages — no manual event wiring.",
            demo: rsx! {
                MinimalExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/minimal.rs"), theme: snippet_theme() }
            },
        }
    }
}
