use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};

fn main() {
    // `option_env!("TUS_ENDPOINT")` in `src/endpoint.rs` reads the var at compile
    // time, but cargo doesn't track that dependency on its own. Without this
    // directive, changing (or first setting) TUS_ENDPOINT wouldn't invalidate
    // the cached build, so the baked-in fallback endpoint would go stale.
    println!("cargo:rerun-if-env-changed=TUS_ENDPOINT");
    println!("cargo:rerun-if-env-changed=DEMO_BROWSER_LOCAL");

    // The worker artifacts have stable filenames. Varying the registration URL
    // with their contents forces browsers to run the update algorithm whenever
    // the deployed JavaScript or WASM changes.
    let mut hasher = DefaultHasher::new();
    for path in [
        "public/service-worker.js",
        "public/service-worker/service_worker.js",
        "public/service-worker/service_worker_bg.wasm",
    ] {
        println!("cargo:rerun-if-changed={path}");
        path.hash(&mut hasher);
        if let Ok(contents) = fs::read(path) {
            contents.hash(&mut hasher);
        }
    }
    println!(
        "cargo:rustc-env=DEMO_SERVICE_WORKER_VERSION={:016x}",
        hasher.finish()
    );
}
