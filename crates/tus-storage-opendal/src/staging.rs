use bytes::Bytes;
use futures::StreamExt;
use opendal::Operator;

use tus_protocol::{Error, Result, StorageHandle};

pub(crate) struct UploadObjects<'a> {
    operator: &'a Operator,
    key: &'a str,
}

impl<'a> UploadObjects<'a> {
    pub(crate) fn new(operator: &'a Operator, key: &'a str) -> Self {
        Self { operator, key }
    }

    pub(crate) fn initialize_handle(handle: &mut StorageHandle) {
        handle.set_internal(INTERNAL_NEXT_PART, "1");
    }

    pub(crate) async fn append_part(&self, handle: &mut StorageHandle, bytes: Bytes) -> Result<()> {
        let part_number = self.next_part_for_append(handle).await?;
        let part_key = self.part_key(part_number);

        self.operator
            .write(&part_key, bytes)
            .await
            .map_err(Error::storage)?;

        handle.set_internal(INTERNAL_NEXT_PART, (part_number + 1).to_string());

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

    async fn next_part_for_append(&self, handle: &StorageHandle) -> Result<u64> {
        let candidate = next_part(handle);
        let candidate_key = self.part_key(candidate);

        match self.operator.stat(&candidate_key).await {
            Ok(_) => {
                let next_existing = self
                    .list_parts()
                    .await?
                    .iter()
                    .filter_map(|part_key| part_number(part_key))
                    .max()
                    .unwrap_or(0)
                    .saturating_add(1);

                Ok(candidate.max(next_existing))
            }
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(candidate),
            Err(e) => Err(Error::storage(e)),
        }
    }

    async fn staged_size(&self) -> Result<Option<u64>> {
        let part_keys = self.list_parts().await?;
        if part_keys.is_empty() {
            return Ok(None);
        }

        let mut total = 0_u64;
        for part_key in part_keys {
            let stat = self
                .operator
                .stat(&part_key)
                .await
                .map_err(Error::storage)?;
            total = total.saturating_add(stat.content_length());
        }

        Ok(Some(total))
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

fn next_part(handle: &StorageHandle) -> u64 {
    handle
        .get_internal(INTERNAL_NEXT_PART)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1)
}

fn part_number(part_key: &str) -> Option<u64> {
    part_key.rsplit('/').next()?.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
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
            .append_part(&mut handle, Bytes::from_static(b"hello"))
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
