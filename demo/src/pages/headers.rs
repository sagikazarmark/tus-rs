use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::examples::headers::HeadersExample;
use crate::ui::{ExampleSection, InlineCode, PageHeader, snippet_theme};

#[component]
pub fn Headers() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Configuration",
            title: "Headers & file naming",
            intro: rsx! {
                InlineCode { "TusStartOptions" }
                " shapes each upload's requests. "
                InlineCode { "with_header" }
                " attaches an arbitrary header to every request (auth proxies, tenant routing, tracing); "
                InlineCode { "with_filename" }
                " and "
                InlineCode { "with_content_type" }
                " override the "
                InlineCode { "filename" }
                " / "
                InlineCode { "filetype" }
                " the hook otherwise derives from the browser "
                InlineCode { "File" }
                "."
            },
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
