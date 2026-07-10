use dioxus::prelude::*;
use dioxus_code::{Code, code};

use crate::examples::resume::ResumeExample;
use crate::examples::resume_persisted::ResumePersistedExample;
use crate::ui::{ExampleSection, InlineCode, PageHeader, snippet_theme};

#[component]
pub fn Resume() -> Element {
    rsx! {
        PageHeader {
            eyebrow: "Resuming",
            title: "Resume across a reload",
            intro: rsx! {
                "In-flight uploads persist to "
                InlineCode { "localStorage" }
                ". On mount, "
                InlineCode { "scan_resumable()" }
                " lists any partial upload; re-picking the same file resumes it from the server's stored offset instead of starting over."
            },
        }
        ExampleSection {
            title: "scan_resumable + resume_entry",
            intro: rsx! {
                "Start an upload, pause it, then reload the tab, the banner reappears and re-picking the file continues where it stopped. Entries are matched by name, size, and last-modified."
            },
            demo: rsx! {
                ResumeExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/resume.rs"), theme: snippet_theme() }
            },
        }
        ExampleSection {
            title: "resume_persisted",
            intro: rsx! {
                "The list-free shortcut: hand a file to "
                InlineCode { "resume_persisted" }
                " and it continues the persisted upload that matches, with no banner or entry list to render. It returns whether a match was found."
            },
            demo: rsx! {
                ResumePersistedExample {}
            },
            code: rsx! {
                Code { src: code!("/src/examples/resume_persisted.rs"), theme: snippet_theme() }
            },
        }
    }
}
