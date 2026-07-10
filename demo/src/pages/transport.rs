use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::examples::transport::TransportExample;
use crate::ui::{ExampleSection, InlineCode, PageHeader, snippet_theme};

#[component]
pub fn Transport() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Advanced",
            title: "Custom transport",
            intro: rsx! {
                InlineCode { "use_tus_upload_with_transport" }
                " accepts any "
                InlineCode { "tus_client::Transport" }
                ", so you own the HTTP layer: auth middleware, a service-worker proxy, request logging, or a mock for tests. This example wraps the default browser transport to log every request."
            },
        }
        ExampleSection {
            title: "use_tus_upload_with_transport",
            stacked: true,
            intro: rsx! {
                "A "
                InlineCode { "LoggingTransport" }
                " decorates "
                InlineCode { "GlooNetTransport" }
                ", pushing each request and response into a signal the panel renders live. Pick a file and watch the POST + PATCH rhythm."
            },
            demo: rsx! {
                TransportExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/transport.rs"), theme: snippet_theme() }
            },
        }
    }
}
