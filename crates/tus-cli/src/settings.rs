use anyhow::{Context, Result};
use clap::{Args, ValueHint};
use figment::{
    Figment,
    providers::{Env, Format, Json, Serialized, Toml, Yaml},
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use url::Url;

use crate::Cli;

#[derive(Debug)]
pub(super) struct Settings {
    pub(super) endpoint: Option<Url>,
    pub(super) bearer_token: Option<String>,
}

impl From<Config> for Settings {
    fn from(config: Config) -> Self {
        Self {
            endpoint: config.endpoint,
            bearer_token: config.bearer_token,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct UploadConfig {
    pub(super) chunk_size: Option<usize>,
}

#[derive(Args, Clone, Debug, Default, Deserialize, Serialize)]
pub(super) struct Config {
    /// Default TUS collection endpoint used for new uploads and relative upload URLs.
    #[arg(
        long,
        env = "TUS_ENDPOINT",
        global = true,
        hide_env_values = true,
        value_name = "URL",
        value_hint = ValueHint::Url
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) endpoint: Option<Url>,

    /// Bearer token sent as `Authorization: Bearer ...`.
    #[arg(
        long,
        env = "TUS_BEARER_TOKEN",
        global = true,
        hide_env_values = true,
        value_name = "TOKEN"
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) bearer_token: Option<String>,
}

pub(super) fn resolve_settings(cli: &Cli) -> Result<Settings> {
    let figment = config_file_figment(cli)?
        .merge(Env::prefixed("TUS_").global())
        .merge(Serialized::globals(cli.config.clone()));
    let config: Config = figment.extract().context("failed to resolve settings")?;

    Ok(Settings::from(config))
}

pub(super) fn resolve_upload_config(cli: &Cli) -> Result<UploadConfig> {
    config_file_figment(cli)?
        .extract()
        .context("failed to resolve upload settings")
}

fn config_file_figment(cli: &Cli) -> Result<Figment> {
    match cli.config_file.as_deref() {
        Some(path) => merge_config_file(Figment::new(), path),
        None => Ok(Figment::new()),
    }
}

fn merge_config_file(figment: Figment, path: &Path) -> Result<Figment> {
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .context("config file must end in .toml, .yaml, .yml, or .json")?;
    let path = path.to_string_lossy();

    match extension {
        "toml" => Ok(figment.merge(Toml::file(path.as_ref()))),
        "yaml" | "yml" => Ok(figment.merge(Yaml::file(path.as_ref()))),
        "json" => Ok(figment.merge(Json::file(path.as_ref()))),
        other => {
            anyhow::bail!(
                "unsupported config extension '.{other}': use .toml, .yaml, .yml, or .json"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, OutputFormat};

    #[test]
    fn config_endpoint_must_be_a_valid_url() {
        let file = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
        std::fs::write(file.path(), "endpoint = \"not a url\"\n").unwrap();
        let cli = Cli {
            config_file: Some(file.path().to_path_buf()),
            config: Config::default(),
            command: Command::Info {
                upload_url: "http://example.com/uploads/1".to_string(),
                output: OutputFormat::Human,
            },
        };

        resolve_settings(&cli).unwrap_err();
    }
}
