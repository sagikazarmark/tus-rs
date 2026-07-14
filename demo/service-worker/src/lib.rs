//! Browser-local TUS endpoint for the static demo.
//!
//! The worker runs the real `tus-protocol` state machine but intentionally
//! discards uploaded bytes after counting them. Only protocol state and accepted
//! byte counts are persisted in IndexedDB.

#![cfg(target_arch = "wasm32")]

mod database;
mod http;

use std::cell::RefCell;
use std::rc::Rc;

use futures_util::lock::Mutex;
use js_sys::global;
use rexie::{ObjectStore, Rexie};
use tus_protocol::{Config, Extension, NoopHookExecutor, NoopLocker, ProtocolHandle};
use wasm_bindgen::{JsCast, JsValue, prelude::wasm_bindgen};
use web_sys::{Request, Response, ServiceWorkerGlobalScope, Url};

use database::BrowserDatabase;

const DATABASE_NAME: &str = "dioxus-tus-demo";
const STATE_STORE: &str = "upload-state";
const SIZE_STORE: &str = "accepted-size";
const MAX_UPLOAD_SIZE: u64 = 256 * 1024 * 1024;
const MAX_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

type BrowserProtocol =
    ProtocolHandle<BrowserDatabase, BrowserDatabase, NoopLocker, NoopHookExecutor>;

thread_local! {
    static SERVER: RefCell<Option<Rc<BrowserTusServer>>> = const { RefCell::new(None) };
}

/// Browser-local server state hidden behind the service worker's HTTP seam.
struct BrowserTusServer {
    protocol: BrowserProtocol,
    base_path: String,
    gate: Mutex<()>,
}

impl BrowserTusServer {
    async fn new() -> Result<Self, JsValue> {
        let base_path = service_worker_base_path()?;
        let database = Rexie::builder(DATABASE_NAME)
            .version(1)
            .add_object_store(ObjectStore::new(STATE_STORE))
            .add_object_store(ObjectStore::new(SIZE_STORE))
            .build()
            .await
            .map_err(js_error)?;
        let database = BrowserDatabase::new(database, STATE_STORE, SIZE_STORE);
        let config = Config::default()
            .with_base_path(&base_path)
            .with_max_size(MAX_UPLOAD_SIZE)
            .with_max_chunk_size(MAX_CHUNK_SIZE)
            .with_max_intake_buffer(MAX_CHUNK_SIZE)
            .with_extension(Extension::CreationWithUpload);

        Ok(Self {
            protocol: ProtocolHandle::new(
                config,
                database.clone(),
                database,
                NoopLocker::new(),
                NoopHookExecutor::new(),
            ),
            base_path,
            gate: Mutex::new(()),
        })
    }

    async fn handle(&self, request: Request) -> Result<Response, JsValue> {
        // Service worker events can interleave across browser API awaits. A
        // single gate makes NoopLocker sound while keeping the demo backend
        // deliberately small; client-side queues still interleave by chunk.
        let _guard = self.gate.lock().await;
        http::dispatch(&self.protocol, &self.base_path, request).await
    }
}

/// Opens IndexedDB and constructs the protocol state used by fetch events.
#[wasm_bindgen]
pub async fn initialize() -> Result<(), JsValue> {
    if SERVER.with(|server| server.borrow().is_some()) {
        return Ok(());
    }

    let server = Rc::new(BrowserTusServer::new().await?);
    SERVER.with(|slot| *slot.borrow_mut() = Some(server));
    Ok(())
}

/// Handles one request intercepted by the service worker bootstrap script.
#[wasm_bindgen]
pub async fn handle_fetch(request: Request) -> Result<Response, JsValue> {
    let server = SERVER.with(|slot| slot.borrow().clone()).ok_or_else(|| {
        JsValue::from_str("browser TUS endpoint was used before initialization completed")
    })?;
    server.handle(request).await
}

fn service_worker_base_path() -> Result<String, JsValue> {
    let scope: ServiceWorkerGlobalScope = global().unchecked_into();
    let registration_scope = scope.registration().scope();
    let url = Url::new(&registration_scope)?;
    let mut path = url.pathname();
    if !path.ends_with('/') {
        path.push('/');
    }
    path.push_str("files");
    Ok(path)
}

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}
