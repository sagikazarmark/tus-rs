#[cfg(not(target_arch = "wasm32"))]
mod platform {
    use std::{collections::HashMap, env, error::Error, path::PathBuf};

    use async_trait::async_trait;
    use tokio::{
        fs::File,
        io::{AsyncReadExt, AsyncSeekExt},
    };
    use tus_client::{Client, UploadSource};
    use url::Url;

    pub type MainResult = Result<(), Box<dyn Error>>;

    #[derive(Clone)]
    struct FileUploadSource {
        path: PathBuf,
        len: u64,
    }

    impl FileUploadSource {
        async fn open(path: PathBuf) -> tus_client::Result<Self> {
            let len = tokio::fs::metadata(&path).await?.len();
            Ok(Self { path, len })
        }
    }

    #[cfg_attr(not(feature = "local-futures"), async_trait)]
    #[cfg_attr(feature = "local-futures", async_trait(?Send))]
    impl UploadSource for FileUploadSource {
        fn len(&self) -> u64 {
            self.len
        }

        async fn read_chunk(&mut self, offset: u64, max_len: usize) -> tus_client::Result<Vec<u8>> {
            let mut file = File::open(&self.path).await?;
            file.seek(std::io::SeekFrom::Start(offset)).await?;

            let mut buffer = vec![0; max_len];
            let read = file.read(&mut buffer).await?;
            buffer.truncate(read);
            Ok(buffer)
        }
    }

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
                let source = FileUploadSource::open(path.clone()).await?;
                let mut metadata = HashMap::new();
                if let Some(filename) = path.file_name().and_then(|name| name.to_str()) {
                    metadata.insert("filename".to_string(), filename.to_string());
                }

                let upload = Client::new(Url::parse(&endpoint)?)
                    .upload_from(source, &metadata)
                    .await?;
                println!("{}", upload.url);
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
