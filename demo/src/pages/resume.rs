use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::examples::resume::ResumeExample;
use crate::ui::{ExampleSection, PageHeader, snippet_theme};

#[component]
pub fn Resume() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Advanced",
            title: "Resume across a reload",
            intro: "In-flight uploads persist to `localStorage`. On mount, `scan_resumable()` lists any partial upload; re-picking the same file resumes it from the server's stored offset instead of starting over.",
        }
        ExampleSection {
            title: "scan_resumable + resume_entry",
            intro: "Start an upload, pause it, then reload the tab — the banner reappears and re-picking the file continues where it stopped. Entries are matched by name, size, and last-modified.",
            demo: rsx! {
                ResumeExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/resume.rs"), theme: snippet_theme() }
            },
        }
    }
}
