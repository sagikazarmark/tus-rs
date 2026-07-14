use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::components::{ExampleLayout, ExampleSection, PageHeader, snippet_theme};
use crate::examples::minimal::MinimalExample;

#[component]
pub fn Minimal() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Basics",
            title: "Minimal uploader",
            intro: "One hook, one file input. use_tus_upload returns reactive state and a handle; call handle.start(file, ...) and read state to render progress.",
        }
        ExampleSection {
            title: "use_tus_upload",
            layout: ExampleLayout::Columns,
            intro: rsx! {
                "The state signal drives the progress bar and the completion / error messages; there is no manual event wiring."
            },
            demo: rsx! {
                MinimalExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/minimal.rs"), theme: snippet_theme() }
            },
        }
    }
}
