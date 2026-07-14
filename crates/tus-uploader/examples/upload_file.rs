#[cfg(not(target_arch = "wasm32"))]
mod platform {
    use std::{collections::HashMap, env, error::Error, path::PathBuf};

    use tus_uploader::{Client, FileSource};
    use url::Url;

    pub type MainResult = Result<(), Box<dyn Error>>;

    pub fn main() -> MainResult {
        let mut args = env::args();
        let program = args.next().unwrap_or_else(|| "upload_file".to_string());
        let Some(endpoint) = args.next() else {
            eprintln!("usage: {program} <tus-endpoint> <file>");
            std::process::exit(2);
        };
        let Some(path) = args.next().map(PathBuf::from) else {
            eprintln!("usage: {program} <tus-endpoint> <file>");
            std::process::exit(2);
        };

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async move {
                let source = FileSource::open(path.clone()).await?;
                let mut metadata = HashMap::new();
                if let Some(filename) = path.file_name().and_then(|name| name.to_str()) {
                    metadata.insert("filename".to_string(), filename.to_string());
                }

                let upload = Client::new(Url::parse(&endpoint)?)
                    .upload_from(source, &metadata)
                    .await?;
                println!("{}", upload.url());
                Ok(())
            })
    }
}

#[cfg(target_arch = "wasm32")]
mod platform {
    pub type MainResult = ();

    pub fn main() {}
}

fn main() -> platform::MainResult {
    platform::main()
}
