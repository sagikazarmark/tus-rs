//! Minimal TUS server using axum and in-memory backends.
//!
//! Run with `cargo run -p tus-axum --example server`. Then a client can drive
//! the server, for example:
//!
//! ```text
//! curl -i http://127.0.0.1:8080/files \
//!     -H "Tus-Resumable: 1.0.0" \
//!     -H "Upload-Length: 11" \
//!     -X POST
//!
//! curl -i http://127.0.0.1:8080/files/<id> \
//!     -H "Tus-Resumable: 1.0.0" \
//!     -H "Upload-Offset: 0" \
//!     -H "Content-Type: application/offset+octet-stream" \
//!     -X PATCH \
//!     --data-binary "hello world"
//! ```

use tus_axum::{RouterOptions, TusState, create_router};
use tus_protocol::{
    Config, NoopHookExecutor, ProtocolHandle, locking::memory::MemoryLocker,
    state::memory::MemoryStateStore, storage::memory::MemoryStorage,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::with_all_extensions().with_base_path("/files");

    let state = TusState::new(ProtocolHandle::new(
        config,
        MemoryStorage::new(),
        MemoryStateStore::new(),
        MemoryLocker::new(),
        NoopHookExecutor::new(),
    ));

    let router = create_router(state, &RouterOptions::default())?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    println!("listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router).await?;

    Ok(())
}
