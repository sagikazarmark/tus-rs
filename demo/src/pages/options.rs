use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::components::{ExampleSection, PageHeader, snippet_theme};
use crate::examples::options::OptionsExample;

#[component]
pub fn Options() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Configuration",
            title: "Tokens, metadata & config",
            intro: "TusConfig tunes client-level behaviour once at construction: chunk size, retry count, retry backoff, a default bearer token, and the creation-with-upload threshold. TusStartOptions carries per-upload concerns: a bearer token override and custom Upload-Metadata.",
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
