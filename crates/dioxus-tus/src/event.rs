//! Helpers for extracting `web_sys::File`s from Dioxus events.
//!
//! The Dioxus 0.7 web-event surface for file inputs is several layers of
//! casts away from the underlying `web_sys::HtmlInputElement::files()`. These
//! helpers collapse the boilerplate so consumers don't have to import
//! `WebEventExt`, `JsCast`, and `HtmlInputElement` themselves.

use dioxus::prelude::*;
use dioxus::web::WebEventExt;
use wasm_bindgen::JsCast;
use web_sys::{File, HtmlInputElement};

/// Extracts the first selected file from an `<input type="file">` change event.
///
/// Returns `None` if the event isn't from an input element, the input has no
/// files selected, or the underlying web event isn't reachable (e.g. server-
/// rendered fallback).
///
/// # Example
/// ```rust,ignore
/// onchange: move |evt| {
///     if let Some(file) = dioxus_tus::file_from_event(&evt) {
///         handle.start(file, TusStartOptions::default());
///     }
/// }
/// ```
pub fn file_from_event(evt: &Event<FormData>) -> Option<File> {
    let web_evt = evt.try_as_web_event()?;
    let target = web_evt.target()?;
    let input = target.dyn_into::<HtmlInputElement>().ok()?;
    input.files()?.get(0)
}

/// Extracts every selected file from an `<input type="file" multiple>` change event.
///
/// Returns an empty vec when no files are selected or the event can't be
/// reached as a web event.
pub fn files_from_event(evt: &Event<FormData>) -> Vec<File> {
    let Some(web_evt) = evt.try_as_web_event() else {
        return Vec::new();
    };
    let Some(target) = web_evt.target() else {
        return Vec::new();
    };
    let Ok(input) = target.dyn_into::<HtmlInputElement>() else {
        return Vec::new();
    };
    let Some(files) = input.files() else {
        return Vec::new();
    };
    let len = files.length();
    (0..len).filter_map(|i| files.get(i)).collect()
}

/// Extracts every dropped file from a drag-and-drop `ondrop` event.
///
/// Companion to [`files_from_event`] for input-element changes. Returns an
/// empty vec when the event isn't a drag event, has no `dataTransfer`, or
/// no files were dropped.
///
/// # Example
/// ```rust,ignore
/// div {
///     ondrop: move |evt| {
///         evt.prevent_default();
///         let files = dioxus_tus::files_from_drag_event(&evt);
///         handle.add_all(files, TusStartOptions::default());
///     },
///     ondragover: move |evt| evt.prevent_default(),
///     "Drop files here"
/// }
/// ```
pub fn files_from_drag_event(evt: &Event<DragData>) -> Vec<File> {
    let Some(web_evt) = evt.try_as_web_event() else {
        return Vec::new();
    };
    let Ok(drag_evt) = web_evt.dyn_into::<web_sys::DragEvent>() else {
        return Vec::new();
    };
    let Some(transfer) = drag_evt.data_transfer() else {
        return Vec::new();
    };
    let Some(files) = transfer.files() else {
        return Vec::new();
    };
    let len = files.length();
    (0..len).filter_map(|i| files.get(i)).collect()
}
