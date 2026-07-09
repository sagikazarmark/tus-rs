fn main() {
    // `option_env!("TUS_ENDPOINT")` in `src/endpoint.rs` reads the var at compile
    // time, but cargo doesn't track that dependency on its own. Without this
    // directive, changing (or first setting) TUS_ENDPOINT wouldn't invalidate
    // the cached build, so the baked-in fallback endpoint would go stale.
    println!("cargo:rerun-if-env-changed=TUS_ENDPOINT");
}
