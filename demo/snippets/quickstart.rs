let (state, handle) = use_tus_upload(
    TusConfig::new("https://your-tus-server/files"),
);

rsx! {
    input {
        r#type: "file",
        onchange: move |evt| {
            if let Some(file) = file_from_event(&evt) {
                handle.start(file, TusStartOptions::default());
            }
        }
    }
    if let Some(pct) = state.read().progress_fraction() {
        progress { value: (pct * 100.0) as u32, max: 100 }
    }
}
