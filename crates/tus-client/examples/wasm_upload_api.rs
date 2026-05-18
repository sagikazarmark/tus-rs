#![allow(dead_code)]

#[cfg(target_arch = "wasm32")]
mod wasm {
    #[derive(Clone)]
    struct CustomUploadSource(Vec<u8>);

    #[async_trait::async_trait(?Send)]
    impl tus_client::UploadSource for CustomUploadSource {
        fn len(&self) -> u64 {
            self.0.len() as u64
        }

        async fn read_chunk(&mut self, offset: u64, max_len: usize) -> tus_client::Result<Vec<u8>> {
            let offset = offset as usize;
            let Some(bytes) = self.0.get(offset..) else {
                return Ok(Vec::new());
            };
            let len = bytes.len().min(max_len);
            Ok(bytes[..len].to_vec())
        }
    }

    async fn compile_wasm_upload_api<T>(
        client: tus_client::Client<T>,
        upload_url: url::Url,
    ) -> tus_client::Result<()>
    where
        T: tus_client::Transport,
    {
        let upload = client.upload(upload_url)?;
        let custom_source = CustomUploadSource(Vec::new());

        upload.upload(Vec::new()).await?;
        upload.upload(custom_source.clone()).await?;
        upload
            .upload_with_progress(Vec::new(), &mut |_uploaded, _total| {})
            .await?;
        client
            .upload_from(Vec::new(), tus_client::UploadMetadata::new())
            .await?;
        client
            .upload_from(custom_source, tus_client::UploadMetadata::new())
            .await?;
        client
            .upload_from_with_progress(
                Vec::new(),
                tus_client::UploadMetadata::new(),
                &mut |_uploaded, _total| {},
            )
            .await?;

        Ok(())
    }
}

fn main() {}
