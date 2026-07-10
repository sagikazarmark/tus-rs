//! dioxus-tus demo: a docs-by-example gallery for the `dioxus-tus` hook.
//!
//! Every page mounts a real, working uploader *and* shows the exact source
//! that produced it (via `include_str!`), so the snippet you read is the code
//! that runs. The UI lives in [`app`] (router + shell), [`pages`] (one route
//! each), and [`examples`] (the small, pure components the pages mount and
//! quote). Shared chrome is in [`ui`]; the TUS endpoint plumbing is in
//! [`endpoint`].

mod app;
mod endpoint;
mod examples;
mod pages;
mod ui;

fn main() {
    dioxus::launch(app::App);
}
