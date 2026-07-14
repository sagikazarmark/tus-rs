use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::components::{ExampleSection, InlineCode, PageHeader, snippet_theme};
use crate::examples::existing_url::ExistingUrlExample;

#[component]
pub fn ExistingUrl() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Resuming",
            title: "Resume from a server URL",
            intro: "When your backend pre-creates the TUS upload (or you stored its URL from a previous run), handle.start_with_url(file, url, ...) skips creation and continues against that URL. The server-issued URL is always available on state.upload_url.",
        }
        ExampleSection {
            title: "start_with_url + state.upload_url",
            intro: rsx! {
                "Start an upload on the left and its URL appears from "
                InlineCode { "state.upload_url" }
                " (prefilled into the field on the right). Re-pick the same file below to continue against that URL; the client sends a HEAD request to read the offset instead of creating a new upload."
            },
            demo: rsx! {
                ExistingUrlExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/existing_url.rs"), theme: snippet_theme() }
            },
        }
    }
}
