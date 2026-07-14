import init, { handle_fetch, initialize } from "./service-worker/service_worker.js";

const wasmUrl = new URL("./service-worker/service_worker_bg.wasm", import.meta.url);
wasmUrl.search = new URL(import.meta.url).search;
const ready = init({ module_or_path: wasmUrl }).then(() => initialize());
const scopeUrl = new URL(self.registration.scope);
const endpointPath = `${scopeUrl.pathname.replace(/\/$/, "")}/files`;

self.addEventListener("install", (event) => {
  event.waitUntil(ready.then(() => self.skipWaiting()));
});

self.addEventListener("activate", (event) => {
  event.waitUntil(ready.then(() => self.clients.claim()));
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (
    url.origin !== scopeUrl.origin ||
    (url.pathname !== endpointPath && !url.pathname.startsWith(`${endpointPath}/`))
  ) {
    return;
  }

  event.respondWith(
    ready
      .then(() => handle_fetch(event.request))
      .catch(() =>
        new Response("Browser-local TUS endpoint failed", {
          status: 500,
          headers: { "tus-resumable": "1.0.0" },
        }),
      ),
  );
});
