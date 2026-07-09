use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::examples::options::OptionsExample;
use crate::ui::{ExampleSection, PageHeader, snippet_theme};

#[component]
pub fn Options() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Advanced",
            title: "Tokens, metadata & config",
            intro: "`TusConfig` tunes client-level behaviour (chunk size, retries, creation-with-upload threshold) once at construction. `TusStartOptions` carries per-upload concerns — a bearer token, extra headers, and custom `Upload-Metadata`.",
        }
        ExampleSection {
            title: "TusConfig + TusStartOptions",
            intro: "The token and metadata below are attached to the next file you pick; the chunk size and retry budget are fixed when the hook is created.",
            demo: rsx! {
                OptionsExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/options.rs"), theme: snippet_theme() }
            },
        }
    }
}
