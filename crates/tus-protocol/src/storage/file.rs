//! File-based storage implementation.
//!
//! This storage backend persists upload bytes as files in a directory. It is a
//! small native default backend; provider-backed object storage lives in
//! separate integration crates.

use std::io;
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio_util::io::ReaderStream;

use crate::error::{Error, Result};
use crate::storage::{
    AppendRequest, ByteStream, ChunkStream, ConcatRequest, Storage, StorageHandle,
};

/// File-based storage backend.
///
/// Stores each upload in one file under the configured root directory.
pub struct FileStorage {
    root: PathBuf,
}

impl FileStorage {
    /// Creates a new file storage backend.
    ///
    /// # Errors
    /// Returns an error if the root directory cannot be created.
    pub async fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).await.map_err(Error::Io)?;
        Ok(Self { root })
    }

    /// Creates a new file storage backend synchronously.
    ///
    /// Use this when you need to create storage outside an async context.
    pub fn new_sync(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root).map_err(Error::Io)?;
        Ok(Self { root })
    }

    /// Returns the storage root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn new_key() -> String {
        format!("{}.upload", uuid::Uuid::new_v4().simple())
    }

    fn path_for_key(&self, key: &str) -> Result<PathBuf> {
        let path = Path::new(key);
        let mut components = path.components();
        let valid = matches!(components.next(), Some(Component::Normal(_)))
            && components.next().is_none()
            && Self::is_generated_key(key);

        if !valid {
            return Err(Error::storage(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid file storage key: {key}"),
            )));
        }

        Ok(self.root.join(path))
    }

    fn is_generated_key(key: &str) -> bool {
        let Some(stem) = key.strip_suffix(".upload") else {
            return false;
        };

        stem.len() == 32 && uuid::Uuid::parse_str(stem).is_ok()
    }

    fn path_for_handle(&self, handle: &StorageHandle) -> Result<PathBuf> {
        self.path_for_key(handle.key())
    }
}

impl std::fmt::Debug for FileStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStorage")
            .field("root", &self.root)
            .finish()
    }
}

#[async_trait]
impl Storage for FileStorage {
    fn name(&self) -> &'static str {
        "file"
    }

    async fn create(&self, _upload_id: &str) -> Result<StorageHandle> {
        let key = Self::new_key();
        let path = self.path_for_key(&key)?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .map_err(Error::Io)?;
        file.sync_all().await.map_err(Error::Io)?;

        Ok(StorageHandle::new(key))
    }

    async fn append(&self, request: AppendRequest) -> Result<StorageHandle> {
        let AppendRequest {
            handle,
            expected_offset,
            data,
            completes_upload: _,
        } = request;
        let path = self.path_for_handle(&handle)?;
        let current_size = match fs::metadata(&path).await {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(Error::NotFound(handle.key().to_string()));
            }
            Err(error) => return Err(Error::Io(error)),
        };

        if current_size != expected_offset {
            return Err(Error::Internal(format!(
                "file storage size {current_size} does not match expected offset {expected_offset} for key {}",
                handle.key()
            )));
        }

        let bytes = collect_chunk_stream(data).await?;
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .map_err(Error::Io)?;
        file.write_all(&bytes).await.map_err(Error::Io)?;
        file.sync_data().await.map_err(Error::Io)?;

        Ok(handle)
    }

    async fn get_stream(&self, handle: &StorageHandle) -> Result<ByteStream> {
        let path = self.path_for_handle(handle)?;
        let file = File::open(path).await.map_err(Error::Io)?;
        Ok(Box::pin(ReaderStream::new(file)))
    }

    async fn concat(&self, request: ConcatRequest) -> Result<StorageHandle> {
        let ConcatRequest { target, parts } = request;
        let target_path = self.path_for_handle(&target)?;
        let temp_path = unique_concat_temp_path(&target_path);
        let mut writer = File::create(&temp_path).await.map_err(Error::Io)?;

        for part in &parts {
            let part_path = self.path_for_handle(part)?;
            let mut reader = File::open(part_path).await.map_err(Error::Io)?;
            tokio::io::copy(&mut reader, &mut writer)
                .await
                .map_err(Error::Io)?;
        }

        writer.sync_all().await.map_err(Error::Io)?;
        drop(writer);
        replace_file(&temp_path, &target_path).await?;

        Ok(target)
    }

    async fn delete(&self, handle: &StorageHandle) -> Result<()> {
        let path = self.path_for_handle(handle)?;

        match fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::Io(error)),
        }
    }

    async fn size(&self, handle: &StorageHandle) -> Result<Option<u64>> {
        let path = self.path_for_handle(handle)?;

        match fs::metadata(path).await {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(Error::Io(error)),
        }
    }

    async fn get_range(
        &self,
        handle: &StorageHandle,
        start: u64,
        end: Option<u64>,
    ) -> Result<ByteStream> {
        let path = self.path_for_handle(handle)?;
        let size = fs::metadata(&path).await.map_err(Error::Io)?.len();
        let start = start.min(size);
        let end = end.unwrap_or(size).min(size);

        if start >= end {
            return Ok(Box::pin(futures::stream::once(async { Ok(Bytes::new()) })));
        }

        let mut file = File::open(path).await.map_err(Error::Io)?;
        file.seek(SeekFrom::Start(start)).await.map_err(Error::Io)?;
        let reader = file.take(end - start);
        Ok(Box::pin(ReaderStream::new(reader)))
    }
}

async fn replace_file(source: &Path, target: &Path) -> Result<()> {
    match fs::rename(source, target).await {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::AlreadyExists | io::ErrorKind::PermissionDenied
            ) =>
        {
            match fs::remove_file(target).await {
                Ok(()) => {}
                Err(remove_error) if remove_error.kind() == io::ErrorKind::NotFound => {}
                Err(remove_error) => return Err(Error::Io(remove_error)),
            }
            fs::rename(source, target).await.map_err(Error::Io)
        }
        Err(error) => Err(Error::Io(error)),
    }
}

fn unique_concat_temp_path(target_path: &Path) -> PathBuf {
    let file_name = target_path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "upload".into());
    target_path.with_file_name(format!(
        "{file_name}.{}.concat.tmp",
        uuid::Uuid::new_v4().simple()
    ))
}

async fn collect_chunk_stream(data: ChunkStream) -> Result<Bytes> {
    match data {
        ChunkStream::Buffered(bytes) => Ok(bytes),
        ChunkStream::Stream(mut stream) => {
            let mut out = Vec::new();
            while let Some(chunk) = stream.next().await {
                out.extend_from_slice(&chunk.map_err(Error::Io)?);
            }
            Ok(Bytes::from(out))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    #[tokio::test]
    async fn storage_conformance() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path()).await.unwrap();

        crate::storage::conformance::assert_full_semantics(&storage).await;
    }

    async fn collect_stream(mut stream: ByteStream) -> Vec<u8> {
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            data.extend_from_slice(&chunk.unwrap());
        }
        data
    }

    #[tokio::test]
    async fn appends_and_streams_upload_data() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path()).await.unwrap();

        let handle = storage.create("upload-1").await.unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), Some(0));

        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("hello ")),
                completes_upload: false,
            })
            .await
            .unwrap();

        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 6,
                data: ChunkStream::from_bytes(Bytes::from("world")),
                completes_upload: true,
            })
            .await
            .unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), Some(11));

        let body = collect_stream(storage.get_stream(&handle).await.unwrap()).await;
        assert_eq!(body, b"hello world");
    }

    #[tokio::test]
    async fn reads_byte_ranges_without_buffering_contract_changes() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path()).await.unwrap();

        let handle = storage.create("upload-2").await.unwrap();
        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("alpha beta gamma")),
                completes_upload: true,
            })
            .await
            .unwrap();

        let range = collect_stream(storage.get_range(&handle, 6, Some(10)).await.unwrap()).await;
        assert_eq!(range, b"beta");

        let suffix = collect_stream(storage.get_range(&handle, 11, None).await.unwrap()).await;
        assert_eq!(suffix, b"gamma");
    }

    #[tokio::test]
    async fn concatenates_parts_into_target() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path()).await.unwrap();

        let part1 = storage.create("part-1").await.unwrap();
        let part1 = storage
            .append(AppendRequest {
                handle: part1,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("left")),
                completes_upload: true,
            })
            .await
            .unwrap();

        let part2 = storage.create("part-2").await.unwrap();
        let part2 = storage
            .append(AppendRequest {
                handle: part2,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("right")),
                completes_upload: true,
            })
            .await
            .unwrap();

        let target = storage.create("target").await.unwrap();
        let target = storage
            .concat(ConcatRequest {
                target,
                parts: vec![part1, part2],
            })
            .await
            .unwrap();

        let body = collect_stream(storage.get_stream(&target).await.unwrap()).await;
        assert_eq!(body, b"leftright");
    }

    #[tokio::test]
    async fn delete_removes_upload_data() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path()).await.unwrap();

        let handle = storage.create("upload-3").await.unwrap();
        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("data")),
                completes_upload: true,
            })
            .await
            .unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), Some(4));

        storage.delete(&handle).await.unwrap();
        assert_eq!(storage.size(&handle).await.unwrap(), None);
    }

    #[tokio::test]
    async fn append_preserves_existing_handle_internals() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path()).await.unwrap();
        let mut handle = storage.create("upload-internals").await.unwrap();
        handle.set_internal("adapter_fact", "keep-me");

        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("data")),
                completes_upload: true,
            })
            .await
            .unwrap();

        assert_eq!(handle.get_internal("adapter_fact"), Some("keep-me"));
    }

    #[tokio::test]
    async fn concat_preserves_existing_target_handle_internals() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path()).await.unwrap();
        let part = storage.create("part-internals").await.unwrap();
        let part = storage
            .append(AppendRequest {
                handle: part,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("part")),
                completes_upload: true,
            })
            .await
            .unwrap();
        let mut target = storage.create("target-internals").await.unwrap();
        target.set_internal("target_fact", "keep-me");

        let target = storage
            .concat(ConcatRequest {
                target,
                parts: vec![part],
            })
            .await
            .unwrap();

        assert_eq!(target.get_internal("target_fact"), Some("keep-me"));
    }

    #[tokio::test]
    async fn rejects_stale_offset_before_reading_body() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path()).await.unwrap();

        let handle = storage.create("upload-4").await.unwrap();
        let handle = storage
            .append(AppendRequest {
                handle,
                expected_offset: 0,
                data: ChunkStream::from_bytes(Bytes::from("seed")),
                completes_upload: false,
            })
            .await
            .unwrap();

        let stream: ByteStream = Box::pin(futures::stream::once(async {
            panic!("body stream should not be read when storage offset is stale");
            #[allow(unreachable_code)]
            Ok(Bytes::from("must not be consumed"))
        }));
        let error = storage
            .append(AppendRequest {
                handle: handle.clone(),
                expected_offset: 0,
                data: ChunkStream::from_stream(stream),
                completes_upload: false,
            })
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Internal(_)));
        assert_eq!(storage.size(&handle).await.unwrap(), Some(4));
    }

    #[test]
    fn path_for_key_rejects_non_local_keys() {
        let storage = FileStorage {
            root: PathBuf::from("/tmp/uploads"),
        };

        assert!(storage.path_for_key("../escape").is_err());
        assert!(storage.path_for_key("nested/key").is_err());
        assert!(storage.path_for_key("key").is_err());
        assert!(
            storage
                .path_for_key("0123456789abcdef0123456789abcdef.upload")
                .is_ok()
        );
    }
}
