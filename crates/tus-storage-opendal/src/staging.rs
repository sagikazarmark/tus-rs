use futures::StreamExt;
use opendal::Operator;

use tus_protocol::{ChunkStream, Error, Result, StorageHandle};

pub(crate) struct UploadObjects<'a> {
    operator: &'a Operator,
    key: &'a str,
}

/// Where the next PATCH body goes and how many bytes precede it.
struct AppendPosition {
    part_number: u64,
    staged_size: u64,
    offset: u64,
}

impl<'a> UploadObjects<'a> {
    pub(crate) fn new(operator: &'a Operator, key: &'a str) -> Self {
        Self { operator, key }
    }

    pub(crate) fn initialize_handle(handle: &mut StorageHandle) {
        handle.set_internal(INTERNAL_NEXT_PART, "1");
        handle.set_internal(INTERNAL_STAGED_SIZE, "0");
    }

    /// Validates the expected offset and stages the PATCH body as the next
    /// part object.
    ///
    /// The body is streamed into the part writer chunk by chunk; nothing is
    /// buffered beyond the caller-provided chunks. On any mid-stream error the
    /// staged part is discarded (writer abort, falling back to best-effort
    /// delete) so a failed PATCH leaves no partial part behind, mirroring the
    /// per-PATCH atomicity of the file backend.
    pub(crate) async fn append_part(
        &self,
        handle: &mut StorageHandle,
        expected_offset: u64,
        data: ChunkStream,
    ) -> Result<()> {
        let position = self.append_position(handle).await?;
        if position.offset != expected_offset {
            return Err(Error::Internal(format!(
                "opendal storage size {} does not match expected offset {expected_offset} for key {}",
                position.offset, self.key
            )));
        }

        let part_key = self.part_key(position.part_number);
        let written = self.write_part(&part_key, data).await?;

        handle.set_internal(INTERNAL_NEXT_PART, (position.part_number + 1).to_string());
        handle.set_internal(
            INTERNAL_STAGED_SIZE,
            position.staged_size.saturating_add(written).to_string(),
        );

        Ok(())
    }

    pub(crate) async fn stored_size(&self) -> Result<Option<u64>> {
        let staged_size = self.staged_size().await?;

        // If both main and staged objects exist, use the larger size. This is
        // safe for recovery from old direct-to-main finalize failures (partial
        // main object plus complete staged parts) and from crashes after temp
        // promotion but before all staged parts were cleaned up.
        match self.operator.stat(self.key).await {
            Ok(stat) => Ok(Some(match staged_size {
                Some(staged_size) => staged_size.max(stat.content_length()),
                None => stat.content_length(),
            })),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(staged_size),
            Err(e) => Err(Error::storage(e)),
        }
    }

    pub(crate) async fn finalize(&self) -> Result<()> {
        let part_keys = self.list_parts().await?;
        self.materialize("finalize", &part_keys).await?;

        for part_key in &part_keys {
            let _ = self.operator.delete(part_key).await;
        }
        let _ = self.operator.delete(&self.parts_prefix()).await;
        let _ = self.operator.delete(&self.temp_prefix()).await;

        Ok(())
    }

    pub(crate) async fn concat(&self, source_keys: &[String]) -> Result<()> {
        self.materialize("concat", source_keys).await
    }

    pub(crate) async fn delete_all(&self) -> Result<()> {
        self.delete_staging().await;
        self.delete_temp_objects().await;

        match self.operator.delete(self.key).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::storage(e)),
        }
    }

    /// Determines the append position with one stat in the common case.
    ///
    /// The handle carries a part cursor and an accumulated staged size,
    /// persisted together after every append. When the cursor is fresh (its
    /// part object does not exist yet) the persisted size is trusted, so an
    /// append costs a single stat instead of listing and stat-ing every
    /// staged part. The listing-based recovery runs only when the internals
    /// are stale (the cursor's part already exists, meaning the process
    /// crashed after a PUT but before persisting the handle) or missing
    /// (handles persisted before the size counter existed).
    async fn append_position(&self, handle: &StorageHandle) -> Result<AppendPosition> {
        let candidate = next_part(handle);
        let candidate_key = self.part_key(candidate);

        match self.operator.stat(&candidate_key).await {
            Ok(_) => self.recovered_position(candidate).await,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => match staged_size_fact(handle) {
                Some(staged_size) => Ok(AppendPosition {
                    part_number: candidate,
                    staged_size,
                    offset: staged_size,
                }),
                None => self.recovered_position(candidate).await,
            },
            Err(e) => Err(Error::storage(e)),
        }
    }

    /// Rebuilds the append position from staged objects when the handle
    /// internals cannot be trusted.
    async fn recovered_position(&self, candidate: u64) -> Result<AppendPosition> {
        let part_keys = self.list_parts().await?;

        let next_existing = part_keys
            .iter()
            .filter_map(|part_key| part_number(part_key))
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let staged_size = self.sum_part_sizes(&part_keys).await?;

        // A partial main object (from an old direct-to-main finalize failure)
        // must not shrink the reported offset below the staged bytes.
        let offset = match self.operator.stat(self.key).await {
            Ok(stat) => staged_size.max(stat.content_length()),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => staged_size,
            Err(e) => return Err(Error::storage(e)),
        };

        Ok(AppendPosition {
            part_number: candidate.max(next_existing),
            staged_size,
            offset,
        })
    }

    /// Writes a PATCH body to the part key, streaming chunk by chunk.
    ///
    /// Returns the number of bytes written. On failure the partially written
    /// part is discarded before the error is returned.
    async fn write_part(&self, part_key: &str, data: ChunkStream) -> Result<u64> {
        match data {
            // A buffered body is a single atomic PUT.
            ChunkStream::Buffered(bytes) => {
                let len = bytes.len() as u64;
                self.operator
                    .write(part_key, bytes)
                    .await
                    .map_err(Error::storage)?;
                Ok(len)
            }
            ChunkStream::Stream(mut stream) => {
                let mut writer = self
                    .operator
                    .writer(part_key)
                    .await
                    .map_err(Error::storage)?;
                let mut written = 0_u64;

                while let Some(chunk) = stream.next().await {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(error) => {
                            self.discard_partial_part(&mut writer, part_key).await;
                            return Err(Error::Io(error));
                        }
                    };

                    written = written.saturating_add(chunk.len() as u64);
                    if let Err(error) = writer.write(chunk).await {
                        self.discard_partial_part(&mut writer, part_key).await;
                        return Err(Error::storage(error));
                    }
                }

                if let Err(error) = writer.close().await {
                    self.discard_partial_part(&mut writer, part_key).await;
                    return Err(Error::storage(error));
                }

                Ok(written)
            }
        }
    }

    /// Ensures a failed streamed part write leaves no object behind.
    ///
    /// `Writer::abort` discards in-flight multipart uploads. Backends without
    /// abort support (for example `fs` without an atomic write dir) may have
    /// already materialized partial bytes at the part key, so fall back to a
    /// best-effort delete.
    async fn discard_partial_part(&self, writer: &mut opendal::Writer, part_key: &str) {
        if writer.abort().await.is_err() {
            let _ = self.operator.delete(part_key).await;
        }
    }

    async fn staged_size(&self) -> Result<Option<u64>> {
        let part_keys = self.list_parts().await?;
        if part_keys.is_empty() {
            return Ok(None);
        }

        Ok(Some(self.sum_part_sizes(&part_keys).await?))
    }

    async fn sum_part_sizes(&self, part_keys: &[String]) -> Result<u64> {
        let mut total = 0_u64;
        for part_key in part_keys {
            let stat = self.operator.stat(part_key).await.map_err(Error::storage)?;
            total = total.saturating_add(stat.content_length());
        }

        Ok(total)
    }

    async fn list_parts(&self) -> Result<Vec<String>> {
        let prefix = self.parts_prefix();
        let entries = match self.operator.list(&prefix).await {
            Ok(e) => e,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::storage(e)),
        };
        let mut part_keys: Vec<String> = entries
            .into_iter()
            .filter(|e| e.metadata().is_file())
            .map(|e| e.path().to_string())
            .collect();
        // Zero-padded part numbers mean lex sort == numeric sort.
        part_keys.sort();
        Ok(part_keys)
    }

    async fn list_temp_objects(&self) -> Result<Vec<String>> {
        let prefix = self.temp_prefix();
        let entries = match self.operator.list(&prefix).await {
            Ok(e) => e,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::storage(e)),
        };
        Ok(entries
            .into_iter()
            .filter(|e| e.metadata().is_file())
            .map(|e| e.path().to_string())
            .collect())
    }

    async fn delete_staging(&self) {
        if let Ok(parts) = self.list_parts().await {
            for part_key in parts {
                let _ = self.operator.delete(&part_key).await;
            }
            let _ = self.operator.delete(&self.parts_prefix()).await;
        }
    }

    async fn delete_temp_objects(&self) {
        if let Ok(temp_objects) = self.list_temp_objects().await {
            for temp_key in temp_objects {
                let _ = self.operator.delete(&temp_key).await;
            }
            let _ = self.operator.delete(&self.temp_prefix()).await;
        }
    }

    async fn materialize(&self, purpose: &str, source_keys: &[String]) -> Result<()> {
        let temp_key = self.temp_key(purpose);
        self.materialize_with_temp(&temp_key, source_keys).await
    }

    async fn materialize_with_temp(&self, temp_key: &str, source_keys: &[String]) -> Result<()> {
        if let Err(error) = self.write_objects_to_key(temp_key, source_keys).await {
            let _ = self.operator.delete(temp_key).await;
            return Err(error);
        }

        if let Err(error) = self.promote_temp(temp_key).await {
            let _ = self.operator.delete(temp_key).await;
            return Err(error);
        }

        Ok(())
    }

    async fn write_objects_to_key(&self, target_key: &str, source_keys: &[String]) -> Result<()> {
        let mut writer = self
            .operator
            .writer(target_key)
            .await
            .map_err(Error::storage)?;
        for source_key in source_keys {
            self.copy_object_into_writer(source_key, &mut writer)
                .await?;
        }
        writer.close().await.map(|_| ()).map_err(Error::storage)
    }

    async fn copy_object_into_writer(
        &self,
        source_key: &str,
        writer: &mut opendal::Writer,
    ) -> Result<()> {
        let reader = self
            .operator
            .reader(source_key)
            .await
            .map_err(Error::storage)?;
        let mut stream = reader
            .into_bytes_stream(0..)
            .await
            .map_err(Error::storage)?;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(Error::storage)?;
            writer.write(chunk).await.map_err(Error::storage)?;
        }
        Ok(())
    }

    async fn promote_temp(&self, temp_key: &str) -> Result<()> {
        let capability = self.operator.info().full_capability();

        if capability.rename {
            return self
                .operator
                .rename(temp_key, self.key)
                .await
                .map_err(Error::storage);
        }

        if capability.copy {
            self.operator
                .copy(temp_key, self.key)
                .await
                .map_err(Error::storage)?;
            let _ = self.operator.delete(temp_key).await;
            return Ok(());
        }

        Err(Error::storage(
            opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "OpenDAL service must support rename or copy to promote materialized uploads",
            )
            .with_operation("tus_storage_opendal::staging::UploadObjects::promote_temp")
            .with_context("service", self.operator.info().scheme()),
        ))
    }

    fn parts_prefix(&self) -> String {
        format!("{}.parts/", self.key)
    }

    fn part_key(&self, part_number: u64) -> String {
        format!("{}.parts/{:010}", self.key, part_number)
    }

    fn temp_prefix(&self) -> String {
        format!("{}.tmp/", self.key)
    }

    fn temp_key(&self, purpose: &str) -> String {
        format!(
            "{}{}-{}",
            self.temp_prefix(),
            purpose,
            uuid::Uuid::new_v4().simple()
        )
    }
}

pub(crate) const INTERNAL_NEXT_PART: &str = "opendal_next_part";
pub(crate) const INTERNAL_STAGED_SIZE: &str = "opendal_staged_size";

fn next_part(handle: &StorageHandle) -> u64 {
    handle
        .internal(INTERNAL_NEXT_PART)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1)
}

fn staged_size_fact(handle: &StorageHandle) -> Option<u64> {
    handle
        .internal(INTERNAL_STAGED_SIZE)
        .and_then(|s| s.parse::<u64>().ok())
}

fn part_number(part_key: &str) -> Option<u64> {
    part_key.rsplit('/').next()?.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use opendal::services::Fs;

    struct TestOperator {
        operator: Operator,
        _tempdir: tempfile::TempDir,
    }

    fn create_test_operator() -> TestOperator {
        let tempdir = tempfile::tempdir().unwrap();
        let operator = Operator::new(Fs::default().root(tempdir.path().to_str().unwrap()))
            .unwrap()
            .finish();

        TestOperator {
            operator,
            _tempdir: tempdir,
        }
    }

    #[tokio::test]
    async fn finalize_removes_staged_parts_and_temporary_objects() {
        let test_operator = create_test_operator();
        let upload = UploadObjects::new(&test_operator.operator, "finalize-cleanup");
        let mut handle = StorageHandle::new("finalize-cleanup");
        UploadObjects::initialize_handle(&mut handle);

        upload
            .append_part(
                &mut handle,
                0,
                ChunkStream::from_bytes(Bytes::from_static(b"hello")),
            )
            .await
            .unwrap();
        assert_eq!(upload.staged_size().await.unwrap(), Some(5));

        upload.finalize().await.unwrap();

        assert_eq!(upload.stored_size().await.unwrap(), Some(5));
        assert!(upload.list_parts().await.unwrap().is_empty());
        assert!(upload.list_temp_objects().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn materialization_error_removes_temporary_object() {
        let test_operator = create_test_operator();
        let upload = UploadObjects::new(&test_operator.operator, "failed-materialize-cleanup");
        let temp_key = upload.temp_key("concat");
        test_operator
            .operator
            .write(&temp_key, Bytes::from_static(b"leftover temp"))
            .await
            .unwrap();

        let result = upload
            .materialize_with_temp(&temp_key, &["missing-source".to_string()])
            .await;

        assert!(result.is_err());
        assert!(upload.list_temp_objects().await.unwrap().is_empty());
        assert_eq!(upload.stored_size().await.unwrap(), None);
    }
}
