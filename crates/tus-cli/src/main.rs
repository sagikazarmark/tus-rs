#![forbid(unsafe_code)]

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum, ValueHint};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap},
    io::IsTerminal,
    path::{Path, PathBuf},
};
use tus_uploader::{Client, FileSource, NewUpload, UploadSource};
use url::Url;

mod progress;
mod settings;
use progress::Progress;
use settings::{Config, Settings, resolve_settings, resolve_upload_config};

#[derive(Parser, Debug)]
#[command(name = "tus")]
#[command(version, about = "Command-line client for TUS uploads")]
struct Cli {
    /// Path to a TOML, YAML, or JSON config file.
    #[arg(
        long = "config",
        env = "TUS_CONFIG",
        global = true,
        hide_env_values = true,
        value_name = "PATH",
        value_hint = ValueHint::FilePath
    )]
    config_file: Option<PathBuf>,

    #[command(flatten)]
    config: Config,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Create a new upload URL without uploading file contents.
    Create {
        /// File whose length will be used for the upload.
        #[arg(
            value_name = "FILE",
            value_hint = ValueHint::FilePath,
            conflicts_with = "length"
        )]
        file: Option<PathBuf>,

        /// Upload length in bytes or a size like 123KiB or 321KB.
        #[arg(
            long,
            value_name = "SIZE",
            value_parser = parse_byte_size,
            required_unless_present = "file",
            conflicts_with = "file"
        )]
        length: Option<u64>,

        /// Upload metadata in KEY=VALUE form. Repeat for multiple metadata entries.
        #[arg(long = "metadata", value_name = "KEY=VALUE", value_parser = parse_metadata)]
        metadata: Vec<(String, String)>,

        /// Output format.
        #[arg(
            short = 'o',
            long = "output",
            value_name = "FORMAT",
            value_enum,
            default_value = "url"
        )]
        output: CreateOutputFormat,
    },
    /// Create a new upload or upload to an existing upload URL.
    Upload {
        /// File whose contents will be uploaded.
        #[arg(value_name = "FILE", value_hint = ValueHint::FilePath)]
        file: PathBuf,

        /// Existing upload URL or relative reference. Omit to create a new upload.
        #[arg(
            value_name = "UPLOAD_URL",
            value_hint = ValueHint::Url,
            conflicts_with = "metadata"
        )]
        upload_url: Option<String>,

        /// Upload metadata in KEY=VALUE form. Repeat for multiple metadata entries.
        #[arg(long = "metadata", value_name = "KEY=VALUE", value_parser = parse_metadata)]
        metadata: Vec<(String, String)>,

        /// Disable upload progress on stderr.
        #[arg(long)]
        no_progress: bool,

        /// Maximum bytes per PATCH request. Smaller values force more PATCH
        /// boundaries, which is the unit of resume, useful for testing
        /// resumability under network faults. Defaults to the tus-uploader
        /// default (8 MiB) when unset.
        #[arg(
            long,
            env = "TUS_CHUNK_SIZE",
            hide_env_values = true,
            value_name = "BYTES"
        )]
        chunk_size: Option<usize>,

        /// Output format.
        #[arg(
            short = 'o',
            long = "output",
            value_name = "FORMAT",
            value_enum,
            default_value = "human"
        )]
        output: UploadOutputFormat,
    },
    /// Print the current offset, length, and metadata for an upload.
    Info {
        /// Upload URL or relative reference to inspect.
        #[arg(value_name = "UPLOAD_URL", value_hint = ValueHint::Url)]
        upload_url: String,

        /// Output format.
        #[arg(
            short = 'o',
            long = "output",
            value_name = "FORMAT",
            value_enum,
            default_value = "human"
        )]
        output: OutputFormat,
    },
    /// Terminate an upload.
    Terminate {
        /// Upload URL or relative reference to terminate.
        #[arg(value_name = "UPLOAD_URL", value_hint = ValueHint::Url)]
        upload_url: String,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputFormat {
    Human,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CreateOutputFormat {
    Human,
    Url,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum UploadOutputFormat {
    Human,
    Url,
}

#[derive(Clone, Copy, Debug)]
struct UploadOptions {
    progress: bool,
    chunk_size: Option<usize>,
}

impl UploadOptions {
    fn new(no_progress: bool, chunk_size: Option<usize>) -> Self {
        Self {
            progress: !no_progress && std::io::stderr().is_terminal(),
            chunk_size,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let settings = resolve_settings(&cli)?;
    let upload_config = match &cli.command {
        Command::Upload { .. } => resolve_upload_config(&cli)?,
        _ => Default::default(),
    };

    match cli.command {
        Command::Create {
            file,
            length,
            metadata,
            output,
        } => {
            let upload = create_upload(file, length, metadata, &settings).await?;
            print_create_result(upload, output)?;
        }
        Command::Upload {
            file,
            upload_url,
            metadata,
            no_progress,
            chunk_size,
            output,
        } => {
            let options = UploadOptions::new(no_progress, chunk_size.or(upload_config.chunk_size));
            upload_file(file, upload_url, metadata, output, options, &settings).await?;
        }
        Command::Info { upload_url, output } => {
            let client = build_upload_client(&upload_url, &settings)?;
            let upload = client.upload_at(&upload_url)?.info().await?;
            print_upload_info(upload, output)?;
        }
        Command::Terminate { upload_url } => {
            let client = build_upload_client(&upload_url, &settings)?;
            let upload = client.upload_at(&upload_url)?;
            upload.terminate().await?;
            eprintln!("Upload terminated");
        }
    }

    Ok(())
}

fn resolve_collection_endpoint(settings: &Settings) -> Result<Url> {
    settings
        .endpoint
        .clone()
        .context("upload collection URL required; pass --endpoint or configure `endpoint`")
}

fn build_collection_client(endpoint: Url, settings: &Settings) -> Result<Client> {
    let client = Client::new(endpoint);
    apply_client_settings(client, settings)
}

fn build_upload_client(upload_url: &str, settings: &Settings) -> Result<Client> {
    let endpoint = match &settings.endpoint {
        Some(endpoint) => endpoint.clone(),
        None => match Url::parse(upload_url) {
            Ok(url) => collection_endpoint(url)?,
            Err(url::ParseError::RelativeUrlWithoutBase) => anyhow::bail!(
                "endpoint required for relative upload URL; pass --endpoint or configure `endpoint`"
            ),
            Err(err) => return Err(err).context("invalid upload URL"),
        },
    };
    let client = Client::new(endpoint);
    apply_client_settings(client, settings)
}

fn apply_client_settings(client: Client, settings: &Settings) -> Result<Client> {
    let client = match settings.bearer_token.as_deref() {
        Some(token) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}"))
                    .context("invalid bearer token")?,
            );
            client.with_headers(headers)
        }
        None => client,
    };
    Ok(client)
}

fn apply_upload_options(client: Client, options: UploadOptions) -> Client {
    match options.chunk_size {
        Some(size) => client.with_max_chunk_size(size),
        None => client,
    }
}

fn print_upload_info(upload: tus_uploader::UploadInfo, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Human => {
            print_upload_info_human(upload);
            Ok(())
        }
        OutputFormat::Json => print_upload_info_json(upload),
    }
}

fn print_upload_result(upload: tus_uploader::UploadInfo, output: UploadOutputFormat) {
    match output {
        UploadOutputFormat::Human => eprintln!("Upload complete: {}", upload.url()),
        UploadOutputFormat::Url => println!("{}", upload.url()),
    }
}

fn print_create_result(upload: tus_uploader::UploadInfo, output: CreateOutputFormat) -> Result<()> {
    match output {
        CreateOutputFormat::Human => {
            eprintln!("Upload created: {}", upload.url());
            Ok(())
        }
        CreateOutputFormat::Url => {
            println!("{}", upload.url());
            Ok(())
        }
        CreateOutputFormat::Json => print_upload_info_json(upload),
    }
}

fn print_upload_info_human(upload: tus_uploader::UploadInfo) {
    println!("url: {}", upload.url());
    println!("offset: {}", upload.offset());
    match upload.length() {
        Some(length) => println!("length: {}", length),
        None => println!("length: deferred"),
    }
    println!("metadata:");
    for (key, value) in metadata_to_sorted_strings(upload.metadata()) {
        println!("{}={}", key, value);
    }
}

fn print_upload_info_json(upload: tus_uploader::UploadInfo) -> Result<()> {
    let output = UploadInfoJson {
        url: upload.url().to_string(),
        offset: upload.offset(),
        length: upload.length(),
        metadata: metadata_to_sorted_strings(upload.metadata()),
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn metadata_to_sorted_strings(metadata: &tus_uploader::UploadMetadata) -> BTreeMap<String, String> {
    metadata
        .iter()
        .map(|(key, value)| (key.clone(), value.to_string_lossy().into_owned()))
        .collect()
}

#[derive(Serialize)]
struct UploadInfoJson {
    url: String,
    offset: u64,
    length: Option<u64>,
    metadata: BTreeMap<String, String>,
}

async fn create_upload(
    file: Option<PathBuf>,
    length: Option<u64>,
    metadata: Vec<(String, String)>,
    settings: &Settings,
) -> Result<tus_uploader::UploadInfo> {
    let endpoint = resolve_collection_endpoint(settings)?;
    let client = build_collection_client(endpoint, settings)?;
    let length = resolve_create_length(file, length).await?;
    let (_upload, info) = client
        .create_upload(NewUpload::new(length, to_metadata_map(metadata)))
        .await?;

    Ok(info)
}

async fn resolve_create_length(file: Option<PathBuf>, length: Option<u64>) -> Result<u64> {
    match (file, length) {
        (_, Some(length)) => Ok(length),
        (Some(file), None) => Ok(tokio::fs::metadata(&file)
            .await
            .with_context(|| format!("failed to read metadata for {}", file.display()))?
            .len()),
        (None, None) => anyhow::bail!("upload length required; pass FILE or --length"),
    }
}

async fn upload_file(
    file: PathBuf,
    upload_url: Option<String>,
    metadata: Vec<(String, String)>,
    output: UploadOutputFormat,
    options: UploadOptions,
    settings: &Settings,
) -> Result<()> {
    match upload_url {
        Some(upload_url) => {
            upload_existing_file(file, &upload_url, output, options, settings).await
        }
        None => create_upload_file(file, metadata, output, options, settings).await,
    }
}

async fn create_upload_file(
    file: PathBuf,
    metadata: Vec<(String, String)>,
    output: UploadOutputFormat,
    options: UploadOptions,
    settings: &Settings,
) -> Result<()> {
    let endpoint = resolve_collection_endpoint(settings)?;
    let client = apply_upload_options(build_collection_client(endpoint, settings)?, options);
    let metadata = to_metadata_map(metadata);
    let source = open_upload_file(&file).await?;
    let (upload, _info) = client
        .create_upload(NewUpload::new(source.len(), metadata))
        .await?;

    // Print the upload URL *before* transferring any bytes: if the upload
    // fails or is interrupted mid-transfer, the user still has the URL and
    // can resume with `tus upload FILE URL`.
    match output {
        UploadOutputFormat::Human => eprintln!("Upload created: {}", upload.url()),
        UploadOutputFormat::Url => println!("{}", upload.url()),
    }

    let info = drive_upload(&upload, source, output, options).await?;
    if output == UploadOutputFormat::Human {
        eprintln!("Upload complete: {}", info.url());
    }

    Ok(())
}

async fn upload_existing_file(
    file: PathBuf,
    upload_url: &str,
    output: UploadOutputFormat,
    options: UploadOptions,
    settings: &Settings,
) -> Result<()> {
    let client = apply_upload_options(build_upload_client(upload_url, settings)?, options);
    let upload = client.upload_at(upload_url)?;
    let source = open_upload_file(&file).await?;
    if output == UploadOutputFormat::Human {
        eprintln!("Uploading to {}", upload.url());
    }

    let info = drive_upload(&upload, source, output, options).await?;
    print_upload_result(info, output);

    Ok(())
}

async fn drive_upload(
    upload: &tus_uploader::Upload<tus_uploader::ReqwestTransport>,
    source: FileSource,
    output: UploadOutputFormat,
    options: UploadOptions,
) -> Result<tus_uploader::UploadInfo> {
    let info = if should_show_progress(output, options) {
        let total = source.len();
        let mut progress = Progress::new(total);
        let info = upload.upload_with_progress(source, &mut progress).await?;
        progress.finish(info.offset());
        info
    } else {
        upload.upload(source).await?
    };

    Ok(info)
}

fn should_show_progress(output: UploadOutputFormat, options: UploadOptions) -> bool {
    output == UploadOutputFormat::Human && options.progress
}

async fn open_upload_file(path: &Path) -> Result<FileSource> {
    FileSource::open(path)
        .await
        .with_context(|| format!("failed to open upload source for {}", path.display()))
}

fn collection_endpoint(mut url: Url) -> Result<Url> {
    let mut segments = url
        .path_segments()
        .context("upload URL must include a path")?
        .collect::<Vec<_>>();
    // A trailing slash produces an empty final segment; strip empties so
    // popping removes the upload id instead of returning the upload URL
    // itself as its own collection endpoint.
    while segments.last() == Some(&"") {
        segments.pop();
    }
    if segments.is_empty() {
        anyhow::bail!("upload URL must include an upload id path segment");
    }
    segments.pop();
    url.set_path(&segments.join("/"));
    Ok(url)
}

fn parse_metadata(input: &str) -> Result<(String, String), String> {
    let (key, value) = input
        .split_once('=')
        .ok_or_else(|| "metadata must be KEY=VALUE".to_string())?;
    if key.is_empty() {
        return Err("metadata key must not be empty".to_string());
    }
    Ok((key.to_string(), value.to_string()))
}

fn parse_byte_size(input: &str) -> Result<u64, String> {
    parse_size::parse_size(input).map_err(|err| format!("invalid size: {err}"))
}

fn to_metadata_map(entries: Vec<(String, String)>) -> HashMap<String, String> {
    entries.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn endpoint_option_must_be_a_valid_url() {
        Cli::try_parse_from([
            "tus",
            "--endpoint",
            "not a url",
            "info",
            "http://example.com/uploads/1",
        ])
        .unwrap_err();
    }

    #[test]
    fn cli_configuration_options_are_flattened_into_config() {
        let cli = Cli::try_parse_from([
            "tus",
            "--endpoint",
            "http://example.com/files",
            "--bearer-token",
            "secret",
            "info",
            "http://example.com/files/1",
        ])
        .unwrap();

        assert_eq!(
            cli.config.endpoint.unwrap().as_str(),
            "http://example.com/files"
        );
        assert_eq!(cli.config.bearer_token.as_deref(), Some("secret"));
    }

    #[test]
    fn upload_options_are_parsed_on_upload_command() {
        let cli = Cli::try_parse_from([
            "tus",
            "upload",
            "--no-progress",
            "--chunk-size",
            "1024",
            "file.txt",
        ])
        .unwrap();

        match cli.command {
            Command::Upload {
                no_progress,
                chunk_size,
                ..
            } => {
                assert!(no_progress);
                assert_eq!(chunk_size, Some(1024));
            }
            command => panic!("expected upload command, got {command:?}"),
        }
    }

    #[test]
    fn upload_options_use_config_file_chunk_size_when_command_option_is_unset() {
        let file = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
        std::fs::write(file.path(), "chunk_size = 1024\n").unwrap();
        let cli = Cli::try_parse_from([
            "tus",
            "--config",
            file.path().to_str().unwrap(),
            "upload",
            "file.txt",
        ])
        .unwrap();
        let upload_config = settings::resolve_upload_config(&cli).unwrap();

        match cli.command {
            Command::Upload {
                no_progress,
                chunk_size,
                ..
            } => {
                let options =
                    UploadOptions::new(no_progress, chunk_size.or(upload_config.chunk_size));

                assert_eq!(options.chunk_size, Some(1024));
            }
            command => panic!("expected upload command, got {command:?}"),
        }
    }

    #[test]
    fn byte_size_parser_accepts_plain_bytes_and_standard_suffixes() {
        assert_eq!(parse_byte_size("123").unwrap(), 123);
        assert_eq!(parse_byte_size("12.5KB").unwrap(), 12_500);
        assert_eq!(parse_byte_size("2ki").unwrap(), 2_048);
        assert_eq!(parse_byte_size("321KB").unwrap(), 321_000);
        assert_eq!(parse_byte_size("123KiB").unwrap(), 125_952);
        assert_eq!(parse_byte_size("2MB").unwrap(), 2_000_000);
        assert_eq!(parse_byte_size("2MiB").unwrap(), 2_097_152);
    }

    #[test]
    fn byte_size_parser_rejects_invalid_sizes() {
        assert!(parse_byte_size("").is_err());
        assert!(parse_byte_size("12XB").is_err());
        assert!(parse_byte_size("18446744073709551616").is_err());
    }

    #[test]
    fn create_options_accept_file_or_length() {
        let cli = Cli::try_parse_from([
            "tus",
            "create",
            "--length",
            "123KiB",
            "--metadata",
            "filename=upload.bin",
            "--output",
            "json",
        ])
        .unwrap();

        match cli.command {
            Command::Create {
                file,
                length,
                metadata,
                output,
            } => {
                assert_eq!(file, None);
                assert_eq!(length, Some(125_952));
                assert_eq!(
                    metadata,
                    vec![("filename".to_string(), "upload.bin".to_string())]
                );
                assert_eq!(output, CreateOutputFormat::Json);
            }
            command => panic!("expected create command, got {command:?}"),
        }

        let cli = Cli::try_parse_from(["tus", "create", "upload.bin"]).unwrap();
        match cli.command {
            Command::Create { file, length, .. } => {
                assert_eq!(file, Some(PathBuf::from("upload.bin")));
                assert_eq!(length, None);
            }
            command => panic!("expected create command, got {command:?}"),
        }
    }

    #[test]
    fn create_requires_exactly_one_length_source() {
        let err = Cli::try_parse_from(["tus", "create"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);

        let err =
            Cli::try_parse_from(["tus", "create", "upload.bin", "--length", "123"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn root_help_documents_commands_and_global_options() {
        let help = Cli::command().render_help().to_string();

        assert!(
            help.contains("Command-line client for TUS uploads"),
            "{help}"
        );
        assert!(help.contains("--config <PATH>"), "{help}");
        assert!(help.contains("--endpoint <URL>"), "{help}");
        assert!(help.contains("--bearer-token <TOKEN>"), "{help}");
        assert!(!help.contains("--chunk-size <BYTES>"), "{help}");
        assert!(!help.contains("--no-progress"), "{help}");
        assert!(help.contains("--version"), "{help}");
        assert!(help.contains("create"), "{help}");
        assert!(help.contains("upload"), "{help}");
        assert!(help.contains("info"), "{help}");
        assert!(help.contains("terminate"), "{help}");
    }

    #[test]
    fn upload_help_documents_arguments_and_options() {
        let mut command = Cli::command();
        let upload = command.find_subcommand_mut("upload").unwrap();
        let help = upload.render_help().to_string();

        assert!(help.contains("<FILE>"), "{help}");
        assert!(help.contains("[UPLOAD_URL]"), "{help}");
        assert!(help.contains("--metadata <KEY=VALUE>"), "{help}");
        assert!(help.contains("--no-progress"), "{help}");
        assert!(help.contains("--chunk-size <BYTES>"), "{help}");
        assert!(help.contains("--output <FORMAT>"), "{help}");
    }

    #[test]
    fn create_help_documents_arguments_and_options() {
        let mut command = Cli::command();
        let create = command.find_subcommand_mut("create").unwrap();
        let help = create.render_help().to_string();

        assert!(help.contains("[FILE]"), "{help}");
        assert!(help.contains("--length <SIZE>"), "{help}");
        assert!(help.contains("--metadata <KEY=VALUE>"), "{help}");
        assert!(help.contains("--output <FORMAT>"), "{help}");
    }

    #[test]
    fn upload_options_are_not_accepted_by_other_commands() {
        let err = Cli::try_parse_from([
            "tus",
            "info",
            "--chunk-size",
            "1024",
            "http://example.com/files/1",
        ])
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);

        let err =
            Cli::try_parse_from(["tus", "info", "--no-progress", "http://example.com/files/1"])
                .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn info_and_terminate_help_document_arguments_and_options() {
        let mut command = Cli::command();
        let info = command.find_subcommand_mut("info").unwrap();
        let help = info.render_help().to_string();

        assert!(help.contains("<UPLOAD_URL>"), "{help}");
        assert!(help.contains("--output <FORMAT>"), "{help}");

        let terminate = command.find_subcommand_mut("terminate").unwrap();
        let help = terminate.render_help().to_string();

        assert!(help.contains("<UPLOAD_URL>"), "{help}");
    }

    #[test]
    fn upload_conflicts_are_validated_by_clap() {
        let err = Cli::try_parse_from([
            "tus",
            "upload",
            "file.txt",
            "http://example.com/files/1",
            "--metadata",
            "filename=file.txt",
        ])
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn upload_resume_option_is_not_accepted() {
        let err = Cli::try_parse_from([
            "tus",
            "upload",
            "--resume",
            "file.txt",
            "http://example.com/files/1",
        ])
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn put_subcommand_is_not_accepted() {
        let err = Cli::try_parse_from(["tus", "put", "file.txt", "http://example.com/files"])
            .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }

    #[test]
    fn cat_subcommand_is_not_accepted() {
        let err = Cli::try_parse_from([
            "tus",
            "cat",
            "http://example.com/files/1",
            "http://example.com/files/2",
        ])
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }

    #[test]
    fn collection_endpoint_pops_the_upload_id_segment() {
        let cases = [
            (
                "http://example.com/files/upload-1",
                "http://example.com/files",
            ),
            // Trailing slashes must not make the upload URL its own
            // collection endpoint.
            (
                "http://example.com/files/upload-1/",
                "http://example.com/files",
            ),
            (
                "http://example.com/files/upload-1//",
                "http://example.com/files",
            ),
        ];

        for (upload_url, expected) in cases {
            let endpoint = collection_endpoint(Url::parse(upload_url).unwrap()).unwrap();

            assert_eq!(endpoint.as_str(), expected, "for {upload_url}");
        }
    }

    #[test]
    fn collection_endpoint_rejects_urls_without_an_upload_id_segment() {
        for upload_url in [
            "http://example.com",
            "http://example.com/",
            "http://example.com//",
        ] {
            let result = collection_endpoint(Url::parse(upload_url).unwrap());

            assert!(result.is_err(), "expected error for {upload_url}");
        }
    }

    #[test]
    fn upload_client_prefers_configured_endpoint_for_relative_references() {
        let settings = Settings {
            endpoint: Some(Url::parse("http://example.com/files").unwrap()),
            bearer_token: None,
        };

        let client = build_upload_client("http://other.example/uploads/part-1", &settings).unwrap();

        assert_eq!(
            client.upload_at("part-2").unwrap().url().as_str(),
            "http://example.com/files/part-2"
        );
    }
}
