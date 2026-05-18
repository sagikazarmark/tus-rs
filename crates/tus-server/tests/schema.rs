//! CLI and config-schema freeze snapshot tests.
//!
//! These tests pin the `--help` output and the set of accepted config
//! file keys so that a 1.0 breaking change cannot land silently. When
//! a flag or key is added, run with `UPDATE_SNAPSHOTS=1` to refresh
//! the goldens and commit them alongside the code change.

use std::path::{Path, PathBuf};
use std::process::Command;

fn server_bin() -> &'static str {
    env!("CARGO_BIN_EXE_tus-server")
}

fn snapshot_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("snapshots")
}

#[test]
fn help_output_matches_snapshot() {
    // env_clear() makes clap's `[env: TUS_*=]` rendering deterministic
    // regardless of what the caller's shell has set.
    let output = Command::new(server_bin())
        .arg("--help")
        .env_clear()
        .output()
        .expect("tus-server --help must run");
    assert!(
        output.status.success(),
        "tus-server --help exited with {:?}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );
    let actual = String::from_utf8(output.stdout).expect("help must be utf-8");

    let snapshot_path = snapshot_dir().join("help.txt");
    if std::env::var_os("UPDATE_SNAPSHOTS").is_some() {
        std::fs::write(&snapshot_path, &actual).expect("write snapshot");
        return;
    }

    let expected = std::fs::read_to_string(&snapshot_path).expect("help snapshot must exist");
    if expected != actual {
        panic!(
            "tus-server --help drifted from snapshot.\n\
             Run with UPDATE_SNAPSHOTS=1 to refresh, then review the diff.\n\
             --- expected ---\n{expected}\n--- actual ---\n{actual}"
        );
    }
}

#[cfg(unix)]
mod config_schema {
    use super::{server_bin, snapshot_dir};
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

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

    fn spawn_with_config(config_path: &std::path::Path, socket_name: &str) -> ServerProcess {
        let root = tempfile::tempdir().expect("tempdir must be created");
        let socket_path = root.path().join(socket_name);
        let state_dir = root.path().join("state");

        // CLI flags override the config file so we can redirect
        // addr/paths to a per-test tempdir while still loading the
        // kitchen-sink from disk.
        let child = Command::new(server_bin())
            .arg("serve")
            .current_dir(root.path())
            .env_clear()
            .arg("--config")
            .arg(config_path)
            .arg("--addr")
            .arg(format!("unix:{}", socket_path.display()))
            .arg("--storage-uri")
            .arg("fs://")
            .arg("--state-dir")
            .arg(&state_dir)
            .env("TUS_STORAGE_ROOT", "uploads")
            // The kitchen sink sets --all-extensions; keep that effective.
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

    async fn wait_for_ready(server: &mut ServerProcess) {
        let deadline = Instant::now() + Duration::from_secs(15);
        let client = reqwest::Client::builder()
            .unix_socket(server.socket_path.clone())
            .build()
            .expect("reqwest unix client must build");

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

            assert!(
                Instant::now() < deadline,
                "server did not become ready within the deadline"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn kitchen_sink_toml_parses_and_starts_server() {
        let mut server = spawn_with_config(
            &snapshot_dir().join("kitchen_sink.toml"),
            "tus-kitchen-toml.sock",
        );
        wait_for_ready(&mut server).await;
    }

    #[tokio::test]
    async fn kitchen_sink_yaml_parses_and_starts_server() {
        let mut server = spawn_with_config(
            &snapshot_dir().join("kitchen_sink.yaml"),
            "tus-kitchen-yaml.sock",
        );
        wait_for_ready(&mut server).await;
    }

    /// Settings resolve CLI > env > config > default. When all three sources
    /// set the same key, the CLI flag must win. We use `--addr` as the probe
    /// because the socket the process listens on is observable end-to-end.
    #[tokio::test]
    async fn cli_flag_wins_over_env_and_config_file() {
        let root = tempfile::tempdir().expect("tempdir must be created");
        let state_dir = root.path().join("state");

        let cli_socket = root.path().join("precedence-cli.sock");
        let env_socket = root.path().join("precedence-env.sock");
        let config_socket = root.path().join("precedence-config.sock");

        let config_path = root.path().join("precedence.toml");
        std::fs::write(
            &config_path,
            format!(
                "addr = \"unix:{cfg}\"\nstate_dir = \"{st}\"\n\n[storage]\nuri = \"fs://\"\nroot = \"uploads\"\n",
                cfg = config_socket.display(),
                st = state_dir.display(),
            ),
        )
        .expect("write precedence config");

        let child = Command::new(server_bin())
            .arg("serve")
            .current_dir(root.path())
            .env_clear()
            .env("TUS_ADDR", format!("unix:{}", env_socket.display()))
            .arg("--config")
            .arg(&config_path)
            .arg("--addr")
            .arg(format!("unix:{}", cli_socket.display()))
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("tus-server must start");
        let mut server = ServerProcess {
            child,
            socket_path: cli_socket.clone(),
            _root: root,
        };

        wait_for_ready(&mut server).await;

        // Only the CLI-chosen socket should exist; env and config values
        // should have been shadowed.
        assert!(cli_socket.exists(), "CLI socket must exist");
        assert!(
            !env_socket.exists(),
            "env socket must not exist when CLI flag is set"
        );
        assert!(
            !config_socket.exists(),
            "config socket must not exist when CLI flag is set"
        );
    }

    /// Without the CLI override, the env var must win over the config file.
    #[tokio::test]
    async fn env_var_wins_over_config_file() {
        let root = tempfile::tempdir().expect("tempdir must be created");
        let state_dir = root.path().join("state");

        let env_socket = root.path().join("precedence-env-only.sock");
        let config_socket = root.path().join("precedence-config-only.sock");

        let config_path = root.path().join("precedence-env.toml");
        std::fs::write(
            &config_path,
            format!(
                "addr = \"unix:{cfg}\"\nstate_dir = \"{st}\"\n\n[storage]\nuri = \"fs://\"\nroot = \"uploads\"\n",
                cfg = config_socket.display(),
                st = state_dir.display(),
            ),
        )
        .expect("write precedence config");

        let child = Command::new(server_bin())
            .arg("serve")
            .current_dir(root.path())
            .env_clear()
            .env("TUS_ADDR", format!("unix:{}", env_socket.display()))
            .arg("--config")
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("tus-server must start");
        let mut server = ServerProcess {
            child,
            socket_path: env_socket.clone(),
            _root: root,
        };

        wait_for_ready(&mut server).await;

        assert!(env_socket.exists(), "env socket must exist");
        assert!(
            !config_socket.exists(),
            "config socket must not exist when env var is set"
        );
    }
}
