#[cfg(unix)]
mod unix_tests {
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    use reqwest::Client;

    struct ServerProcess {
        child: Child,
        socket_path: PathBuf,
        _root: tempfile::TempDir,
    }

    impl Drop for ServerProcess {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    fn server_bin() -> &'static str {
        env!("CARGO_BIN_EXE_tus-server")
    }

    fn spawn_server(extra_args: &[&str]) -> ServerProcess {
        let root = tempfile::tempdir().expect("tempdir must be created");
        let socket_path = root.path().join("tus.sock");
        let state_dir = root.path().join("state");

        let mut args = vec![
            "serve".to_string(),
            "--addr".to_string(),
            format!("unix:{}", socket_path.display()),
            "--storage-uri".to_string(),
            "fs://".to_string(),
            "--state-dir".to_string(),
            state_dir.display().to_string(),
        ];
        args.extend(extra_args.iter().map(|arg| arg.to_string()));

        let child = Command::new(server_bin())
            .args(&args)
            .current_dir(root.path())
            .env_clear()
            .env("TUS_STORAGE_ROOT", "uploads")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("tus-server must start");

        ServerProcess {
            child,
            socket_path,
            _root: root,
        }
    }

    fn unix_client(path: &Path) -> Client {
        Client::builder()
            .unix_socket(path.to_path_buf())
            .build()
            .expect("reqwest unix client must build")
    }

    async fn wait_for_socket(server: &mut ServerProcess) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let client = unix_client(&server.socket_path);
        loop {
            if let Some(status) = server
                .child
                .try_wait()
                .expect("child status must be readable")
            {
                let stderr = server
                    .child
                    .stderr
                    .take()
                    .map(|mut stderr| {
                        let mut bytes = Vec::new();
                        std::io::Read::read_to_end(&mut stderr, &mut bytes)
                            .expect("stderr must be readable");
                        String::from_utf8_lossy(&bytes).into_owned()
                    })
                    .unwrap_or_default();
                panic!("server exited early with {status}: {stderr}");
            }

            if client
                .get("http://localhost/healthz")
                .send()
                .await
                .map(|response| response.status().as_u16() == 200)
                .unwrap_or(false)
            {
                return;
            }

            assert!(Instant::now() < deadline, "server did not become ready");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn serves_health_endpoint_over_unix_socket() {
        let mut server = spawn_server(&[]);
        wait_for_socket(&mut server).await;
        let client = unix_client(&server.socket_path);

        let response = client
            .get("http://localhost/healthz")
            .send()
            .await
            .expect("health request must succeed");

        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(response.text().await.unwrap(), "ok");
    }
}

mod tcp_tests {
    use std::collections::HashMap;
    use std::net::TcpListener as StdTcpListener;
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use reqwest::Client;
    use reqwest::header::AUTHORIZATION;
    use tus_uploader::{Error as ClientError, NewUpload, ParallelUpload};

    /// Test-local facade over `tus_uploader::Client` that keeps the call shapes
    /// in these tests compact.
    #[derive(Debug, Clone)]
    struct TusClient {
        inner: tus_uploader::Client<tus_uploader::ReqwestTransport>,
    }

    #[derive(Debug, Clone)]
    struct TusUpload {
        url: String,
        offset: u64,
        length: Option<u64>,
        metadata: tus_uploader::UploadMetadata,
    }

    impl From<tus_uploader::UploadInfo> for TusUpload {
        fn from(info: tus_uploader::UploadInfo) -> Self {
            Self {
                url: info.url().to_string(),
                offset: info.offset(),
                length: info.length(),
                metadata: info.metadata().clone(),
            }
        }
    }

    impl TusClient {
        fn new(endpoint: impl AsRef<str>) -> Result<Self, ClientError> {
            Ok(Self {
                inner: tus_uploader::Client::new(url::Url::parse(endpoint.as_ref())?),
            })
        }

        fn with_bearer_token(mut self, token: &str) -> Result<Self, ClientError> {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                AUTHORIZATION,
                format!("Bearer {token}")
                    .parse()
                    .expect("bearer token must form a valid header value"),
            );
            self.inner = self.inner.with_headers(headers);
            Ok(self)
        }

        fn with_creation_with_upload_threshold(mut self, threshold: usize) -> Self {
            self.inner = self.inner.with_max_initial_upload_size(threshold);
            self
        }

        fn with_patch_chunk_size(mut self, chunk_size: usize) -> Self {
            self.inner = self.inner.with_max_chunk_size(chunk_size);
            self
        }

        fn with_max_retries(mut self, max_retries: usize) -> Self {
            self.inner = self.inner.with_max_retries(max_retries);
            self
        }

        fn with_checksum(mut self, algorithm: tus_protocol::ChecksumAlgorithm) -> Self {
            self.inner = self.inner.with_checksum(algorithm);
            self
        }

        fn with_checksum_trailer(mut self, algorithm: tus_protocol::ChecksumAlgorithm) -> Self {
            self.inner = self
                .inner
                .with_checksum(tus_uploader::ChecksumMode::Trailer(algorithm));
            self
        }

        async fn create_upload(
            &self,
            length: u64,
            metadata: &HashMap<String, String>,
        ) -> Result<TusUpload, ClientError> {
            let (_upload, info) = self
                .inner
                .create_upload(NewUpload::new(length, metadata))
                .await?;
            Ok(info.into())
        }

        async fn upload_file(
            &self,
            path: impl AsRef<std::path::Path>,
            metadata: &HashMap<String, String>,
        ) -> Result<TusUpload, ClientError> {
            let bytes = tokio::fs::read(path).await?;
            self.inner
                .upload_from(bytes, metadata)
                .await
                .map(Into::into)
        }

        async fn upload_file_parallel(
            &self,
            path: impl AsRef<std::path::Path>,
            metadata: &HashMap<String, String>,
            options: ParallelUpload,
        ) -> Result<TusUpload, ClientError> {
            let bytes = tokio::fs::read(path).await?;
            self.inner
                .upload_parallel(bytes, metadata, options)
                .await
                .map(Into::into)
        }

        async fn head(&self, upload_url: impl AsRef<str>) -> Result<TusUpload, ClientError> {
            self.inner
                .upload_at(upload_url.as_ref())?
                .info()
                .await
                .map(Into::into)
        }

        async fn resume_file(
            &self,
            upload_url: impl AsRef<str>,
            path: impl AsRef<std::path::Path>,
        ) -> Result<TusUpload, ClientError> {
            let bytes = tokio::fs::read(path).await?;
            self.inner
                .upload_at(upload_url.as_ref())?
                .upload(bytes)
                .await
                .map(Into::into)
        }

        async fn delete_upload(&self, upload_url: impl AsRef<str>) -> Result<(), ClientError> {
            self.inner.upload_at(upload_url.as_ref())?.terminate().await
        }
    }

    struct ServerProcess {
        child: Child,
        base_url: String,
        storage_root: PathBuf,
        state_dir: PathBuf,
        _port_token: PortToken,
        _root: Option<tempfile::TempDir>,
    }

    static RESERVED_PORTS: Mutex<Vec<u16>> = Mutex::new(Vec::new());

    struct PortToken {
        port: u16,
    }

    struct ReservedPort {
        listener: Option<StdTcpListener>,
        token: PortToken,
    }

    impl Drop for PortToken {
        fn drop(&mut self) {
            let mut ports = RESERVED_PORTS
                .lock()
                .expect("reserved port registry must not be poisoned");
            if let Some(index) = ports.iter().position(|port| *port == self.port) {
                ports.swap_remove(index);
            }
        }
    }

    impl ReservedPort {
        fn into_token(self) -> PortToken {
            self.token
        }

        fn release_listener(&mut self) {
            self.listener.take();
        }
    }

    impl std::fmt::Display for ReservedPort {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.token.port.fmt(f)
        }
    }

    impl Drop for ServerProcess {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    fn server_bin() -> &'static str {
        env!("CARGO_BIN_EXE_tus-server")
    }

    fn reserve_port() -> ReservedPort {
        loop {
            let listener =
                StdTcpListener::bind("127.0.0.1:0").expect("ephemeral port must be reserved");
            let port = listener
                .local_addr()
                .expect("reserved listener must have a local address")
                .port();
            let mut ports = RESERVED_PORTS
                .lock()
                .expect("reserved port registry must not be poisoned");
            if ports.contains(&port) {
                continue;
            }
            ports.push(port);
            drop(ports);

            return ReservedPort {
                listener: Some(listener),
                token: PortToken { port },
            };
        }
    }

    #[test]
    fn reserved_port_stays_bound_until_released() {
        let mut port = reserve_port();
        let addr = format!("127.0.0.1:{port}");

        assert!(
            StdTcpListener::bind(&addr).is_err(),
            "reserved port should not be reusable before server spawn"
        );
        port.release_listener();
        // Another parallel test binding an ephemeral `127.0.0.1:0` port can be
        // handed this just-freed port by the OS before its own reservation loop
        // rejects it (the port is still in RESERVED_PORTS) and releases it.
        // Retry so that transient contention does not flake the assertion; the
        // port is guaranteed to become rebindable because no other reservation
        // can keep it while this test's token is alive.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let rebound = loop {
            if StdTcpListener::bind(&addr).is_ok() {
                break true;
            }
            if std::time::Instant::now() >= deadline {
                break false;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        assert!(rebound, "reserved port should be reusable after release");
    }

    #[cfg(unix)]
    #[test]
    fn address_in_use_detection_waits_for_delayed_bind_failure() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 0.1; echo 'Error: Address already in use' >&2; exit 1")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("delayed failure child must start");

        assert!(child_exited_with_address_in_use(&mut child));
    }

    fn spawn_server(extra_args: &[&str]) -> ServerProcess {
        spawn_server_with_env(extra_args, &[])
    }

    fn spawn_server_with_env(extra_args: &[&str], envs: &[(&str, &str)]) -> ServerProcess {
        let root = tempfile::tempdir().expect("tempdir must be created");
        let storage_root = root.path().join("uploads");
        let state_dir = root.path().join("state");

        for _ in 0..10 {
            let reserved_port = reserve_port();
            let addr = format!("127.0.0.1:{reserved_port}");
            let args = server_args(&addr, &storage_root, &state_dir, extra_args);
            let port_token = reserved_port.into_token();
            let mut child = spawn_child(&args, envs, root.path());

            if child_exited_with_address_in_use(&mut child) {
                continue;
            }

            return ServerProcess {
                child,
                base_url: format!("http://{addr}"),
                storage_root,
                state_dir,
                _port_token: port_token,
                _root: Some(root),
            };
        }

        panic!("tus-server could not bind an ephemeral test port after retries");
    }

    fn spawn_server_in_root(root: &std::path::Path, extra_args: &[&str]) -> ServerProcess {
        let storage_root = root.join("uploads");
        let state_dir = root.join("state");

        for _ in 0..10 {
            let reserved_port = reserve_port();
            let addr = format!("127.0.0.1:{reserved_port}");
            let args = server_args(&addr, &storage_root, &state_dir, extra_args);
            let port_token = reserved_port.into_token();
            let mut child = spawn_child(&args, &[], root);

            if child_exited_with_address_in_use(&mut child) {
                continue;
            }

            return ServerProcess {
                child,
                base_url: format!("http://{addr}"),
                storage_root,
                state_dir,
                _port_token: port_token,
                _root: None,
            };
        }

        panic!("tus-server could not bind an ephemeral test port after retries");
    }

    fn server_args(
        addr: &str,
        _storage_root: &std::path::Path,
        state_dir: &std::path::Path,
        extra_args: &[&str],
    ) -> Vec<String> {
        let mut args = vec![
            "serve".to_string(),
            "--addr".to_string(),
            addr.to_string(),
            "--storage-uri".to_string(),
            "fs://".to_string(),
            "--state-dir".to_string(),
            state_dir.display().to_string(),
        ];
        args.extend(extra_args.iter().map(|arg| arg.to_string()));
        args
    }

    fn spawn_child(args: &[String], envs: &[(&str, &str)], current_dir: &std::path::Path) -> Child {
        let mut command = Command::new(server_bin());
        command
            .args(args)
            .current_dir(current_dir)
            .env_clear()
            .env("TUS_STORAGE_ROOT", "uploads")
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        for (key, value) in envs {
            command.env(key, value);
        }
        command.spawn().expect("tus-server must start")
    }

    fn child_exited_with_address_in_use(child: &mut Child) -> bool {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if let Some(status) = child.try_wait().expect("child status must be readable") {
                let stderr = read_child_stderr(child);
                if stderr.contains("Address already in use") {
                    return true;
                }

                panic!("server exited early with {status}: {stderr}");
            }

            std::thread::sleep(Duration::from_millis(25));
        }

        false
    }

    fn read_child_stderr(child: &mut Child) -> String {
        child
            .stderr
            .take()
            .map(|mut stderr| {
                let mut bytes = Vec::new();
                std::io::Read::read_to_end(&mut stderr, &mut bytes)
                    .expect("stderr must be readable");
                String::from_utf8_lossy(&bytes).into_owned()
            })
            .unwrap_or_default()
    }

    async fn wait_for_http(server: &mut ServerProcess) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let client = Client::new();
        loop {
            if let Some(status) = server
                .child
                .try_wait()
                .expect("child status must be readable")
            {
                let stderr = read_child_stderr(&mut server.child);
                panic!("server exited early with {status}: {stderr}");
            }

            if client
                .get(format!("{}/healthz", server.base_url))
                .send()
                .await
                .map(|response| response.status().as_u16() == 200)
                .unwrap_or(false)
            {
                return;
            }

            assert!(Instant::now() < deadline, "server did not become ready");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn files_endpoint(server: &ServerProcess) -> String {
        format!("{}/files", server.base_url)
    }

    fn upload_id(upload_url: &str) -> &str {
        upload_url
            .rsplit('/')
            .next()
            .expect("upload URL must include an id")
    }

    async fn create_upload(client: &Client, url: &str, bearer: Option<&str>) -> reqwest::Response {
        let mut request = client
            .post(url)
            .header("tus-resumable", tus_protocol::TUS_RESUMABLE)
            .header("upload-length", "0");
        if let Some(token) = bearer {
            request = request.bearer_auth(token);
        }
        request
            .send()
            .await
            .expect("upload creation request must succeed")
    }

    fn resolve_upload_url(server: &ServerProcess, location: &str) -> String {
        if location.starts_with("http://") || location.starts_with("https://") {
            location.to_string()
        } else {
            format!("{}{}", server.base_url, location)
        }
    }

    #[tokio::test]
    async fn tus_uploader_uploads_and_deletes_over_tcp() {
        let mut server = spawn_server(&[]);
        wait_for_http(&mut server).await;

        let endpoint = files_endpoint(&server);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("upload.bin");
        tokio::fs::write(&path, b"hello world").await.unwrap();

        let mut metadata = HashMap::new();
        metadata.insert("filename".to_string(), "upload.bin".to_string());

        let client = TusClient::new(&endpoint)
            .unwrap()
            .with_creation_with_upload_threshold(0)
            .with_patch_chunk_size(4);
        let upload = client.upload_file(&path, &metadata).await.unwrap();
        let head = client.head(&upload.url).await.unwrap();
        assert_eq!(head.offset, 11);
        assert_eq!(head.length, Some(11));
        assert_eq!(
            head.metadata
                .get("filename")
                .and_then(|value| value.as_str()),
            Some("upload.bin")
        );

        let stored = tokio::fs::read(
            server
                .storage_root
                .join(upload_id(&upload.url))
                .join("data"),
        )
        .await
        .unwrap();
        assert_eq!(stored, b"hello world");

        client.delete_upload(&upload.url).await.unwrap();
        let err = client.head(&upload.url).await.unwrap_err();
        assert!(matches!(
            err,
            ClientError::UnexpectedResponse { status, .. } if status == tus_uploader::http::StatusCode::NOT_FOUND
        ));
    }

    #[tokio::test]
    async fn tus_uploader_resumes_partial_upload_over_tcp() {
        let mut server = spawn_server(&[]);
        wait_for_http(&mut server).await;

        let endpoint = files_endpoint(&server);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resume.bin");
        tokio::fs::write(&path, b"abcdefghij").await.unwrap();

        let client = TusClient::new(&endpoint)
            .unwrap()
            .with_creation_with_upload_threshold(0)
            .with_patch_chunk_size(3)
            .with_max_retries(0);
        let upload = client.create_upload(10, &HashMap::new()).await.unwrap();

        Client::new()
            .patch(&upload.url)
            .header("tus-resumable", tus_protocol::TUS_RESUMABLE)
            .header("upload-offset", "0")
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/offset+octet-stream",
            )
            .body("abcde")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let resumed = client.resume_file(&upload.url, &path).await.unwrap();
        assert_eq!(resumed.offset, 10);
        assert_eq!(resumed.length, Some(10));

        let stored = tokio::fs::read(
            server
                .storage_root
                .join(upload_id(&upload.url))
                .join("data"),
        )
        .await
        .unwrap();
        assert_eq!(stored, b"abcdefghij");
    }

    #[tokio::test]
    async fn tus_uploader_recovers_from_stale_state_after_restart() {
        let root = tempfile::tempdir().unwrap();
        let mut server = spawn_server_in_root(root.path(), &[]);
        wait_for_http(&mut server).await;

        let endpoint = files_endpoint(&server);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restart.bin");
        tokio::fs::write(&path, b"abcdefghij").await.unwrap();

        let client = TusClient::new(&endpoint)
            .unwrap()
            .with_creation_with_upload_threshold(0)
            .with_patch_chunk_size(3)
            .with_max_retries(0);
        let upload = client.create_upload(10, &HashMap::new()).await.unwrap();

        Client::new()
            .patch(&upload.url)
            .header("tus-resumable", tus_protocol::TUS_RESUMABLE)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/offset+octet-stream",
            )
            .header("upload-offset", "0")
            .body("abcde")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let upload_id = upload_id(&upload.url).to_string();
        let state_path = server.state_dir.join(format!("{upload_id}.json"));
        let mut stale_state: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&state_path).await.unwrap()).unwrap();
        stale_state["offset"] = serde_json::json!(0);
        tokio::fs::write(
            &state_path,
            serde_json::to_vec_pretty(&stale_state).unwrap(),
        )
        .await
        .unwrap();

        server.child.kill().unwrap();
        server.child.wait().unwrap();

        let mut restarted = spawn_server_in_root(root.path(), &[]);
        wait_for_http(&mut restarted).await;

        let upload_url = format!("{}/files/{upload_id}", restarted.base_url);
        let restarted_client = TusClient::new(files_endpoint(&restarted))
            .unwrap()
            .with_creation_with_upload_threshold(0)
            .with_patch_chunk_size(3)
            .with_max_retries(0);

        let head = restarted_client.head(&upload_url).await.unwrap();
        assert_eq!(head.offset, 5);

        let resumed = restarted_client
            .resume_file(&upload_url, &path)
            .await
            .unwrap();
        assert_eq!(resumed.offset, 10);
        assert_eq!(resumed.length, Some(10));

        let stored = tokio::fs::read(restarted.storage_root.join(upload_id).join("data"))
            .await
            .unwrap();
        assert_eq!(stored, b"abcdefghij");
    }

    #[tokio::test]
    async fn tus_uploader_parallel_uploads_over_tcp_with_all_extensions() {
        let mut server = spawn_server(&["--all-extensions"]);
        wait_for_http(&mut server).await;

        let endpoint = files_endpoint(&server);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("parallel.bin");
        tokio::fs::write(&path, b"abcdefghijklmnop").await.unwrap();

        let client = TusClient::new(&endpoint)
            .unwrap()
            .with_creation_with_upload_threshold(0)
            .with_patch_chunk_size(4);
        let upload = client
            .upload_file_parallel(&path, &HashMap::new(), ParallelUpload::new(4))
            .await
            .unwrap();

        let head = client.head(&upload.url).await.unwrap();
        assert_eq!(head.offset, 16);
        assert_eq!(head.length, Some(16));

        let stored = tokio::fs::read(
            server
                .storage_root
                .join(upload_id(&upload.url))
                .join("data"),
        )
        .await
        .unwrap();
        assert_eq!(stored, b"abcdefghijklmnop");
    }

    #[tokio::test]
    async fn tus_uploader_uploads_with_checksum_headers_over_tcp() {
        let mut server = spawn_server(&["--all-extensions"]);
        wait_for_http(&mut server).await;

        let endpoint = files_endpoint(&server);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checksum-header.bin");
        tokio::fs::write(&path, b"header-check").await.unwrap();

        let client = TusClient::new(&endpoint)
            .unwrap()
            .with_checksum(tus_protocol::ChecksumAlgorithm::Sha1)
            .with_creation_with_upload_threshold(0)
            .with_patch_chunk_size(4);
        let upload = client.upload_file(&path, &HashMap::new()).await.unwrap();

        let head = client.head(&upload.url).await.unwrap();
        assert_eq!(head.offset, 12);
        assert_eq!(head.length, Some(12));

        let stored = tokio::fs::read(
            server
                .storage_root
                .join(upload_id(&upload.url))
                .join("data"),
        )
        .await
        .unwrap();
        assert_eq!(stored, b"header-check");
    }

    #[tokio::test]
    async fn tus_uploader_uploads_with_checksum_trailers_over_tcp() {
        let mut server = spawn_server(&["--all-extensions"]);
        wait_for_http(&mut server).await;

        let endpoint = files_endpoint(&server);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checksum-trailer.bin");
        tokio::fs::write(&path, b"trailer-check").await.unwrap();

        let client = TusClient::new(&endpoint)
            .unwrap()
            .with_checksum_trailer(tus_protocol::ChecksumAlgorithm::Sha1)
            .with_creation_with_upload_threshold(0)
            .with_patch_chunk_size(5);
        let upload = client.upload_file(&path, &HashMap::new()).await.unwrap();

        let head = client.head(&upload.url).await.unwrap();
        assert_eq!(head.offset, 13);
        assert_eq!(head.length, Some(13));

        let stored = tokio::fs::read(
            server
                .storage_root
                .join(upload_id(&upload.url))
                .join("data"),
        )
        .await
        .unwrap();
        assert_eq!(stored, b"trailer-check");
    }

    #[tokio::test]
    async fn completed_uploads_can_be_downloaded_over_tcp() {
        let mut server = spawn_server(&[]);
        wait_for_http(&mut server).await;

        let endpoint = files_endpoint(&server);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("download.bin");
        tokio::fs::write(&path, b"download-me").await.unwrap();

        let client = TusClient::new(&endpoint)
            .unwrap()
            .with_creation_with_upload_threshold(0)
            .with_patch_chunk_size(4);
        let upload = client.upload_file(&path, &HashMap::new()).await.unwrap();

        let response = Client::new().get(&upload.url).send().await.unwrap();
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/octet-stream")
        );
        assert_eq!(
            response
                .headers()
                .get("content-length")
                .and_then(|value| value.to_str().ok()),
            Some("11")
        );
        assert_eq!(response.bytes().await.unwrap().as_ref(), b"download-me");
    }

    #[tokio::test]
    async fn completed_uploads_support_single_byte_ranges_over_tcp() {
        let mut server = spawn_server(&[]);
        wait_for_http(&mut server).await;

        let endpoint = files_endpoint(&server);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range.bin");
        tokio::fs::write(&path, b"hello world").await.unwrap();

        let client = TusClient::new(&endpoint)
            .unwrap()
            .with_creation_with_upload_threshold(0)
            .with_patch_chunk_size(4);
        let upload = client.upload_file(&path, &HashMap::new()).await.unwrap();

        let response = Client::new()
            .get(&upload.url)
            .header(reqwest::header::RANGE, "bytes=6-10")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 206);
        assert_eq!(
            response
                .headers()
                .get("content-range")
                .and_then(|value| value.to_str().ok()),
            Some("bytes 6-10/11")
        );
        assert_eq!(
            response
                .headers()
                .get("content-length")
                .and_then(|value| value.to_str().ok()),
            Some("5")
        );
        assert_eq!(response.bytes().await.unwrap().as_ref(), b"world");
    }

    #[tokio::test]
    async fn downloads_can_be_disabled_over_tcp() {
        let mut server = spawn_server(&["--disable-download"]);
        wait_for_http(&mut server).await;

        let endpoint = files_endpoint(&server);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-download.bin");
        tokio::fs::write(&path, b"do-not-serve").await.unwrap();

        let client = TusClient::new(&endpoint)
            .unwrap()
            .with_creation_with_upload_threshold(0)
            .with_patch_chunk_size(4);
        let upload = client.upload_file(&path, &HashMap::new()).await.unwrap();

        let response = Client::new().get(&upload.url).send().await.unwrap();
        assert_eq!(response.status().as_u16(), 405);
    }

    #[tokio::test]
    async fn expired_uploads_are_deleted_by_background_cleanup() {
        // No opt-in flag: setting --expiration must enable in-process
        // reclamation by default.
        let mut server = spawn_server(&["--expiration", "1s", "--expiration-scan-interval", "1s"]);
        wait_for_http(&mut server).await;

        let client = Client::new();
        let created = client
            .post(files_endpoint(&server))
            .header("tus-resumable", tus_protocol::TUS_RESUMABLE)
            .header("upload-length", "5")
            .send()
            .await
            .unwrap();
        assert_eq!(created.status().as_u16(), 201);

        let location = created
            .headers()
            .get("location")
            .and_then(|value| value.to_str().ok())
            .expect("created upload must include a location header");
        let upload_url = resolve_upload_url(&server, location);
        let patched = client
            .patch(&upload_url)
            .header("tus-resumable", tus_protocol::TUS_RESUMABLE)
            .header("upload-offset", "0")
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/offset+octet-stream",
            )
            .body("h")
            .send()
            .await
            .unwrap();
        assert_eq!(patched.status().as_u16(), 204);

        let upload_id = upload_id(&upload_url).to_string();
        let upload_path = server.storage_root.join(&upload_id);
        let staged_part_path = server
            .storage_root
            .join(format!("{upload_id}/parts/0000000001"));
        let state_path = server.state_dir.join(format!("{upload_id}.json"));
        let tus_uploader = TusClient::new(files_endpoint(&server)).unwrap();

        assert!(
            state_path.exists(),
            "state file should exist before cleanup"
        );
        assert!(
            upload_path.exists() || staged_part_path.exists(),
            "upload data should exist before cleanup"
        );

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let state_gone = !state_path.exists();
            let upload_gone = !upload_path.exists() && !staged_part_path.exists();
            let status = match tus_uploader.head(&upload_url).await {
                Ok(_) => 200,
                Err(ClientError::UnexpectedResponse { status, .. }) => status.as_u16(),
                Err(error) => panic!("unexpected HEAD failure: {error:?}"),
            };

            if state_gone && upload_gone && status == 404 {
                break;
            }

            assert!(
                Instant::now() < deadline,
                "expired upload was not cleaned up: state_gone={state_gone} upload_gone={upload_gone} status={}",
                status
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[tokio::test]
    async fn cleanup_command_removes_expired_uploads_once() {
        let root = tempfile::tempdir().unwrap();
        let mut server = spawn_server_in_root(root.path(), &["--expiration", "1s"]);
        wait_for_http(&mut server).await;

        let client = Client::new();
        let created = client
            .post(files_endpoint(&server))
            .header("tus-resumable", tus_protocol::TUS_RESUMABLE)
            .header("upload-length", "5")
            .send()
            .await
            .unwrap();
        assert_eq!(created.status().as_u16(), 201);

        let location = created
            .headers()
            .get("location")
            .and_then(|value| value.to_str().ok())
            .expect("created upload must include a location header");
        let upload_url = resolve_upload_url(&server, location);
        let patched = client
            .patch(&upload_url)
            .header("tus-resumable", tus_protocol::TUS_RESUMABLE)
            .header("upload-offset", "0")
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/offset+octet-stream",
            )
            .body("h")
            .send()
            .await
            .unwrap();
        assert_eq!(patched.status().as_u16(), 204);

        let upload_id = upload_id(&upload_url).to_string();
        let upload_path = server.storage_root.join(&upload_id);
        let staged_part_path = server
            .storage_root
            .join(format!("{upload_id}/parts/0000000001"));

        tokio::time::sleep(Duration::from_secs(2)).await;

        server.child.kill().unwrap();
        server.child.wait().unwrap();

        // The server was killed above, which is exactly the safe usage
        // --force acknowledges: cleanup's memory locker cannot see a
        // live server's locks.
        let output = Command::new(server_bin())
            .arg("cleanup")
            .arg("--storage-uri")
            .arg("fs://")
            .arg("--state-dir")
            .arg(&server.state_dir)
            .arg("--force")
            .current_dir(root.path())
            .env_clear()
            .env("TUS_STORAGE_ROOT", "uploads")
            .output()
            .expect("tus-server cleanup must run");
        assert!(
            output.status.success(),
            "cleanup failed with {:?}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );

        assert!(!server.state_dir.join(format!("{upload_id}.json")).exists());
        assert!(!upload_path.exists());
        assert!(!staged_part_path.exists());
    }

    #[tokio::test]
    async fn cleanup_command_refuses_to_run_without_force() {
        let root = tempfile::tempdir().unwrap();
        let state_dir = root.path().join("state");
        std::fs::create_dir_all(&state_dir).unwrap();

        let output = Command::new(server_bin())
            .arg("cleanup")
            .arg("--storage-uri")
            .arg("fs://")
            .arg("--state-dir")
            .arg(&state_dir)
            .current_dir(root.path())
            .env_clear()
            .env("TUS_STORAGE_ROOT", "uploads")
            .output()
            .expect("tus-server cleanup must run");

        assert!(
            !output.status.success(),
            "cleanup must refuse to run without --force"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("--force"),
            "error must point at the --force flag, got: {stderr}"
        );
    }

    #[tokio::test]
    async fn tus_uploader_authenticates_with_bearer_token_over_tcp() {
        let mut server = spawn_server(&["--auth-token", "secret-token"]);
        wait_for_http(&mut server).await;

        let endpoint = files_endpoint(&server);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.bin");
        tokio::fs::write(&path, b"authorized").await.unwrap();

        let unauthorized = TusClient::new(&endpoint)
            .unwrap()
            .upload_file(&path, &HashMap::new())
            .await
            .unwrap_err();
        assert!(matches!(
            unauthorized,
            ClientError::UnexpectedResponse { status, .. } if status == tus_uploader::http::StatusCode::UNAUTHORIZED
        ));

        let client = TusClient::new(&endpoint)
            .unwrap()
            .with_bearer_token("secret-token")
            .unwrap()
            .with_creation_with_upload_threshold(0)
            .with_patch_chunk_size(4);
        let upload = client.upload_file(&path, &HashMap::new()).await.unwrap();

        let head = client.head(&upload.url).await.unwrap();
        assert_eq!(head.offset, 10);
        assert_eq!(head.length, Some(10));

        let stored = tokio::fs::read(
            server
                .storage_root
                .join(upload_id(&upload.url))
                .join("data"),
        )
        .await
        .unwrap();
        assert_eq!(stored, b"authorized");
    }

    #[tokio::test]
    async fn toml_config_file_sets_representative_server_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("server.toml");
        tokio::fs::write(
            &config_path,
            r#"
base_path = "/from-file"
"#,
        )
        .await
        .unwrap();

        let mut server = spawn_server(&["--config", config_path.to_str().unwrap()]);
        wait_for_http(&mut server).await;

        let client = Client::new();
        let created = create_upload(&client, &format!("{}/from-file", server.base_url), None).await;
        assert_eq!(created.status().as_u16(), 201);
        let location = created
            .headers()
            .get("location")
            .and_then(|value| value.to_str().ok())
            .expect("created upload must include a location header");
        assert!(
            location.contains("/from-file/"),
            "expected file-configured base path in location header, got {location}"
        );
    }

    #[tokio::test]
    async fn env_vars_override_yaml_config_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("server.yaml");
        tokio::fs::write(
            &config_path,
            r#"
base_path: /from-file
auth_token:
  - file-token
"#,
        )
        .await
        .unwrap();

        let mut server = spawn_server_with_env(
            &["--config", config_path.to_str().unwrap()],
            &[
                ("TUS_BASE_PATH", "/from-env"),
                ("TUS_AUTH_TOKEN", "env-token"),
            ],
        );
        wait_for_http(&mut server).await;

        let client = Client::new();
        let env_path = create_upload(
            &client,
            &format!("{}/from-env", server.base_url),
            Some("env-token"),
        )
        .await;
        assert_eq!(env_path.status().as_u16(), 201);

        let file_path = create_upload(
            &client,
            &format!("{}/from-file", server.base_url),
            Some("env-token"),
        )
        .await;
        assert_eq!(file_path.status().as_u16(), 404);

        let file_token = create_upload(
            &client,
            &format!("{}/from-env", server.base_url),
            Some("file-token"),
        )
        .await;
        assert_eq!(file_token.status().as_u16(), 401);
    }

    #[tokio::test]
    async fn cli_flags_override_env_and_config_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("server.toml");
        tokio::fs::write(
            &config_path,
            r#"
base_path = "/from-file"
auth_token = ["file-token"]
"#,
        )
        .await
        .unwrap();

        let mut server = spawn_server_with_env(
            &[
                "--config",
                config_path.to_str().unwrap(),
                "--base-path",
                "/from-cli",
                "--auth-token",
                "cli-token",
            ],
            &[
                ("TUS_BASE_PATH", "/from-env"),
                ("TUS_AUTH_TOKEN", "env-token"),
            ],
        );
        wait_for_http(&mut server).await;

        let client = Client::new();
        let cli_path = create_upload(
            &client,
            &format!("{}/from-cli", server.base_url),
            Some("cli-token"),
        )
        .await;
        assert_eq!(cli_path.status().as_u16(), 201);

        let env_path = create_upload(
            &client,
            &format!("{}/from-env", server.base_url),
            Some("cli-token"),
        )
        .await;
        assert_eq!(env_path.status().as_u16(), 404);

        let env_token = create_upload(
            &client,
            &format!("{}/from-cli", server.base_url),
            Some("env-token"),
        )
        .await;
        assert_eq!(env_token.status().as_u16(), 401);

        let file_token = create_upload(
            &client,
            &format!("{}/from-cli", server.base_url),
            Some("file-token"),
        )
        .await;
        assert_eq!(file_token.status().as_u16(), 401);
    }
}
