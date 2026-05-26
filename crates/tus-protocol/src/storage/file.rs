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
use crate::state::UploadState;
use crate::storage::{ByteStream, ChunkStream, Storage};

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

    fn path_for_state(&self, state: &UploadState) -> Result<PathBuf> {
        let key = state.storage_key().ok_or(Error::StorageKeyMissing)?;
        self.path_for_key(key)
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

    async fn create(&self, state: &mut UploadState) -> Result<String> {
        let key = Self::new_key();
        let path = self.path_for_key(&key)?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .map_err(Error::Io)?;
        file.sync_all().await.map_err(Error::Io)?;

        state.set_storage_key(key.clone());
        Ok(key)
    }

    async fn append(&self, state: &mut UploadState, data: ChunkStream) -> Result<u64> {
        let path = self.path_for_state(state)?;
        let current_size = match fs::metadata(&path).await {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let key = state.storage_key().ok_or(Error::StorageKeyMissing)?;
                return Err(Error::NotFound(key.to_string()));
            }
            Err(error) => return Err(Error::Io(error)),
        };

        if current_size != state.offset() {
            return Err(Error::Internal(format!(
                "file storage size {current_size} does not match recorded offset {} for upload {}",
                state.offset(),
                state.id()
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

        let new_offset = current_size.saturating_add(bytes.len() as u64);
        Ok(new_offset)
    }

    async fn get_stream(&self, state: &UploadState) -> Result<ByteStream> {
        let path = self.path_for_state(state)?;
        let file = File::open(path).await.map_err(Error::Io)?;
        Ok(Box::pin(ReaderStream::new(file)))
    }

    async fn concat(&self, target: &mut UploadState, parts: Vec<UploadState>) -> Result<()> {
        let target_path = self.path_for_state(target)?;
        let temp_path = unique_concat_temp_path(&target_path);
        let mut writer = File::create(&temp_path).await.map_err(Error::Io)?;

        for part in &parts {
            let part_path = self.path_for_state(part)?;
            let mut reader = File::open(part_path).await.map_err(Error::Io)?;
            tokio::io::copy(&mut reader, &mut writer)
                .await
                .map_err(Error::Io)?;
        }

        writer.sync_all().await.map_err(Error::Io)?;
        drop(writer);
        replace_file(&temp_path, &target_path).await?;

        Ok(())
    }

    async fn delete(&self, state: &UploadState) -> Result<()> {
        let Some(key) = state.storage_key() else {
            return Ok(());
        };
        let path = self.path_for_key(key)?;

        match fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::Io(error)),
        }
    }

    async fn size(&self, state: &UploadState) -> Result<Option<u64>> {
        let Some(key) = state.storage_key() else {
            return Ok(None);
        };
        let path = self.path_for_key(key)?;

        match fs::metadata(path).await {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(Error::Io(error)),
        }
    }

    async fn get_range(
        &self,
        state: &UploadState,
        start: u64,
        end: Option<u64>,
    ) -> Result<ByteStream> {
        let path = self.path_for_state(state)?;
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
        let mut state = UploadState::new("upload-1");

        let key = storage.create(&mut state).await.unwrap();
        assert_eq!(state.storage_key(), Some(key.as_str()));
        assert_eq!(storage.size(&state).await.unwrap(), Some(0));

        let offset = storage
            .append(&mut state, ChunkStream::from_bytes(Bytes::from("hello ")))
            .await
            .unwrap();
        assert_eq!(offset, 6);
        assert_eq!(state.offset(), 0);
        state.set_offset(offset);

        let offset = storage
            .append(&mut state, ChunkStream::from_bytes(Bytes::from("world")))
            .await
            .unwrap();
        assert_eq!(offset, 11);
        assert_eq!(storage.size(&state).await.unwrap(), Some(11));

        let body = collect_stream(storage.get_stream(&state).await.unwrap()).await;
        assert_eq!(body, b"hello world");
    }

    #[tokio::test]
    async fn reads_byte_ranges_without_buffering_contract_changes() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path()).await.unwrap();
        let mut state = UploadState::new("upload-2");

        storage.create(&mut state).await.unwrap();
        let offset = storage
            .append(
                &mut state,
                ChunkStream::from_bytes(Bytes::from("alpha beta gamma")),
            )
            .await
            .unwrap();
        state.set_offset(offset);

        let range = collect_stream(storage.get_range(&state, 6, Some(10)).await.unwrap()).await;
        assert_eq!(range, b"beta");

        let suffix = collect_stream(storage.get_range(&state, 11, None).await.unwrap()).await;
        assert_eq!(suffix, b"gamma");
    }

    #[tokio::test]
    async fn concatenates_parts_into_target() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path()).await.unwrap();

        let mut part1 = UploadState::new("part-1");
        storage.create(&mut part1).await.unwrap();
        let offset = storage
            .append(&mut part1, ChunkStream::from_bytes(Bytes::from("left")))
            .await
            .unwrap();
        part1.set_offset(offset);

        let mut part2 = UploadState::new("part-2");
        storage.create(&mut part2).await.unwrap();
        let offset = storage
            .append(&mut part2, ChunkStream::from_bytes(Bytes::from("right")))
            .await
            .unwrap();
        part2.set_offset(offset);

        let mut target = UploadState::new("target");
        storage.create(&mut target).await.unwrap();
        storage
            .concat(&mut target, vec![part1, part2])
            .await
            .unwrap();

        let body = collect_stream(storage.get_stream(&target).await.unwrap()).await;
        assert_eq!(body, b"leftright");
    }

    #[tokio::test]
    async fn delete_removes_upload_data() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path()).await.unwrap();
        let mut state = UploadState::new("upload-3");

        storage.create(&mut state).await.unwrap();
        storage
            .append(&mut state, ChunkStream::from_bytes(Bytes::from("data")))
            .await
            .unwrap();
        assert_eq!(storage.size(&state).await.unwrap(), Some(4));

        storage.delete(&state).await.unwrap();
        assert_eq!(storage.size(&state).await.unwrap(), None);
    }

    #[tokio::test]
    async fn rejects_stale_offset_before_reading_body() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path()).await.unwrap();
        let mut state = UploadState::new("upload-4");

        storage.create(&mut state).await.unwrap();
        storage
            .append(&mut state, ChunkStream::from_bytes(Bytes::from("seed")))
            .await
            .unwrap();
        state.set_offset(0);

        let stream: ByteStream = Box::pin(futures::stream::once(async {
            panic!("body stream should not be read when storage offset is stale");
            #[allow(unreachable_code)]
            Ok(Bytes::from("must not be consumed"))
        }));
        let error = storage
            .append(&mut state, ChunkStream::from_stream(stream))
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Internal(_)));
        assert_eq!(storage.size(&state).await.unwrap(), Some(4));
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
