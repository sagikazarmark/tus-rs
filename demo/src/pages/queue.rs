use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::examples::queue::QueueExample;
use crate::ui::{ExampleSection, PageHeader, snippet_theme};

#[component]
pub fn Queue() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Uploading",
            title: "Concurrent upload queue",
            intro: "`use_tus_upload_queue` runs several uploads in parallel and exposes per-file and queue-wide controls. Drop a batch of files and watch them go.",
        }
        ExampleSection {
            title: "use_tus_upload_queue",
            intro: "Each row reports live speed and ETA; the queue schedules files across worker slots and lets you pause, resume, retry, or remove any of them.",
            demo: rsx! {
                QueueExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/queue.rs"), theme: snippet_theme() }
            },
        }
    }
}
