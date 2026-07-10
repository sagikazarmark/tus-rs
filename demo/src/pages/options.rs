use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::examples::options::OptionsExample;
use crate::ui::{ExampleSection, InlineCode, PageHeader, snippet_theme};

#[component]
pub fn Options() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Advanced",
            title: "Tokens, metadata & config",
            intro: rsx! {
                InlineCode { "TusConfig" }
                " tunes client-level behaviour once at construction: chunk size, retry count, retry backoff, a default bearer token, and the creation-with-upload threshold (small files that POST their bytes in one request). "
                InlineCode { "TusStartOptions" }
                " carries per-upload concerns: a bearer token override and custom "
                InlineCode { "Upload-Metadata" }
                "."
            },
        }
        ExampleSection {
            title: "TusConfig + TusStartOptions",
            intro: rsx! {
                "The token and metadata below are attached to the next file you pick; the chunk size and retry budget are fixed when the hook is created."
            },
            demo: rsx! {
                OptionsExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/options.rs"), theme: snippet_theme() }
            },
        }
    }
}
