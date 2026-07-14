use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::components::{ExampleSection, PageHeader, snippet_theme};
use crate::examples::headers::HeadersExample;

#[component]
pub fn Headers() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Configuration",
            title: "Headers & file naming",
            intro: "TusStartOptions shapes each upload's requests. with_header attaches an arbitrary header to every request; with_filename and with_content_type override the filename and filetype the hook otherwise derives from the browser File.",
        }
        ExampleSection {
            title: "with_header · with_filename · with_content_type",
            intro: rsx! {
                "Set a custom header and optional name / type overrides, then pick a file. Empty override fields fall back to the browser's own filename and MIME type."
            },
            demo: rsx! {
                HeadersExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/headers.rs"), theme: snippet_theme() }
            },
        }
    }
}
