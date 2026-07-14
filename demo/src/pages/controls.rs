use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::components::{ExampleSection, PageHeader, snippet_theme};
use crate::examples::controls::ControlsExample;

#[component]
pub fn Controls() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Uploading",
            title: "Pause, resume & abort",
            intro: "The handle exposes pause(), resume(), and abort(). Pause and resume take effect at the next chunk boundary; abort resets the upload to idle (the server resource is left intact).",
        }
        ExampleSection {
            title: "Chunk-boundary controls",
            intro: rsx! {
                "A small chunk size is used here so pausing is visible even on a fast connection. Buttons enable and disable from the reactive status."
            },
            demo: rsx! {
                ControlsExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/controls.rs"), theme: snippet_theme() }
            },
        }
    }
}
