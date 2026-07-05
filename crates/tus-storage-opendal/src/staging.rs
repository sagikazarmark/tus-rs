use futures_util::StreamExt;
use opendal::Operator;

use tus_protocol::{ChunkStream, Error, Result, StorageHandle};

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
        handle.set_internal(INTERNAL_STAGED_SIZE, "0");
    }

    /// Removes leftover staging state before a key is reused for a new upload.
    ///
    /// A previous upload at the same key may have left staged parts, temporary
    /// materialization objects, or a completion marker behind (crashes, failed
    /// cleanup). Inheriting them would splice stale bytes into the new upload,
    /// so creation fails if the leftovers cannot be removed.
    pub(crate) async fn prepare_for_new_upload(&self) -> Result<()> {
        // Same ordering rationale as `delete_all`; the main object is removed
        // too so a reused key cannot report the previous upload's size.
        self.delete_object(&self.completion_marker_key()).await?;
        self.delete_object(self.key).await?;
        self.delete_staged_parts().await?;
        self.delete_temp_objects().await
    }

    /// Validates the expected offset and stages the PATCH body as the next
    /// part object.
    ///
    /// The body is streamed into the part writer chunk by chunk; nothing is
    /// buffered beyond the caller-provided chunks. On any mid-stream error the
    /// staged part is discarded (writer abort, falling back to best-effort
    /// delete) so a failed PATCH leaves no partial part behind, mirroring the
    /// per-PATCH atomicity of the file backend.
    ///
    /// When `completes_upload` is set, a completion marker recording the part
    /// number of this final part is written durably *before* the body is
    /// staged. The final part is staged through a temporary object and promoted
    /// atomically, so after a crash the marked part either exists in full (the
    /// upload is fully staged and [`ensure_materialized`] can repair it) or does
    /// not exist at all (the upload is still incomplete and the client resumes
    /// normally); a torn write can never leave a truncated part behind the
    /// marker.
    ///
    /// [`ensure_materialized`]: Self::ensure_materialized
    pub(crate) async fn append_part(
        &self,
        handle: &mut StorageHandle,
        expected_offset: u64,
        data: ChunkStream,
        completes_upload: bool,
    ) -> Result<()> {
        let position = self.append_position(handle).await?;
        if position.offset != expected_offset {
            // Divergence between persisted upload state and stored bytes
            // (e.g. after a failed completing append) is an offset conflict
            // the client can recover from via HEAD, not an internal error.
            return Err(Error::OffsetMismatch {
                expected: position.offset,
                actual: expected_offset,
            });
        }

        let part_key = self.part_key(position.part_number);
        let written = if completes_upload {
            self.write_completion_marker(position.part_number).await?;
            // The marker makes this part authoritative for repair, so it must
            // never be observable half-written: a crash mid-write on a backend
            // without atomic object writes (e.g. `fs`) could otherwise leave a
            // truncated part that `ensure_materialized` would promote as the
            // whole upload. Stage it to a temp object and promote it, so the
            // part key holds either the full final body or nothing at all.
            match self.stage_final_part(&part_key, data).await {
                Ok(written) => written,
                Err(error) => {
                    // The completing body was not staged; remove the marker so
                    // a later part written at this number (for example after
                    // the client re-declares a different deferred length)
                    // cannot be mistaken for a completed upload. Best effort:
                    // a leaked marker only matters together with that rare
                    // sequence, and the next completing append rewrites it.
                    let _ = self.operator.delete(&self.completion_marker_key()).await;
                    return Err(error);
                }
            }
        } else {
            // A torn non-completing part is resume-safe: offsets are derived
            // from the bytes actually staged, so the client just continues from
            // there. Only the marked final part needs atomic staging.
            self.write_part(&part_key, data).await?
        };

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

    /// Materializes staged parts into the main object and cleans staging up.
    ///
    /// Once materialization succeeded the upload is durable at the main key,
    /// so cleanup failures no longer fail the upload: leftover staging
    /// objects are garbage, not data loss. They are logged loudly instead and
    /// removed by a later [`delete_all`](Self::delete_all).
    pub(crate) async fn finalize(&self) -> Result<()> {
        let parts = self.list_parts().await?;
        let part_keys: Vec<String> = parts.into_iter().map(|part| part.key).collect();
        self.materialize("finalize", &part_keys).await?;

        if let Err(error) = self.delete_staged_parts().await {
            tracing::warn!(
                key = self.key,
                error = %error,
                "completed upload left staged part objects behind"
            );
        }
        if let Err(error) = self.delete_temp_objects().await {
            tracing::warn!(
                key = self.key,
                error = %error,
                "completed upload left temporary objects behind"
            );
        }
        if let Err(error) = self.delete_object(&self.completion_marker_key()).await {
            tracing::warn!(
                key = self.key,
                error = %error,
                "completed upload left its completion marker behind"
            );
        }

        Ok(())
    }

    /// Ensures the main object exists, repairing an interrupted finalize.
    ///
    /// Returns `true` when the main object exists (possibly after repair) and
    /// `false` when there is nothing readable at the main key: the upload is
    /// missing entirely or staged but not yet complete.
    ///
    /// Repair eligibility is decided by the durable completion marker written
    /// by the completing [`append_part`](Self::append_part): if the marker
    /// exists and the part it names was staged (the completing part is promoted
    /// atomically, so its existence implies the full final body was staged),
    /// the upload completed but its finalize never materialized the main
    /// object. In that
    /// case the staged parts are re-driven through the same
    /// temp-object-then-promote path as finalize.
    ///
    /// Concurrency: repair never deletes staged parts or the marker, so two
    /// readers repairing the same upload both stream the same parts into
    /// distinct temporary objects and promote byte-identical content;
    /// double-repair is idempotent last-writer-wins. If materialization fails
    /// because a concurrent repair (or finalize retry) already promoted the
    /// main object, the freshly promoted object is accepted. Staging garbage
    /// left behind by a repaired upload is removed on delete/termination.
    pub(crate) async fn ensure_materialized(&self) -> Result<bool> {
        match self.operator.stat(self.key).await {
            Ok(_) => return Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::storage(e)),
        }

        let Some(completing_part) = self.read_completion_marker().await? else {
            return Ok(false);
        };
        match self.operator.stat(&self.part_key(completing_part)).await {
            Ok(_) => {}
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(Error::storage(e)),
        }

        // The marker plus its named part prove the final PATCH was staged, but
        // a torn delete can have removed earlier parts. Repairing from a gapped
        // sequence would promote truncated content, so require every part
        // 1..=completing_part to be present and materialize exactly those, in
        // order.
        let staged: std::collections::HashSet<u64> = self
            .list_parts()
            .await?
            .into_iter()
            .filter_map(|part| part_number(&part.key))
            .collect();
        if completing_part == 0 || (1..=completing_part).any(|n| !staged.contains(&n)) {
            return Ok(false);
        }
        let part_keys: Vec<String> = (1..=completing_part).map(|n| self.part_key(n)).collect();

        tracing::warn!(
            key = self.key,
            "repairing interrupted finalize by re-materializing staged parts"
        );
        match self.materialize("repair", &part_keys).await {
            Ok(()) => Ok(true),
            Err(error) => match self.operator.stat(self.key).await {
                // A concurrent repair or finalize won the promotion race with
                // byte-identical content; serve the promoted object.
                Ok(_) => Ok(true),
                Err(_) => Err(error),
            },
        }
    }

    pub(crate) async fn concat(&self, source_keys: &[String]) -> Result<()> {
        self.materialize("concat", source_keys).await
    }

    /// Deletes the main object and all staging/temporary objects.
    ///
    /// Missing objects are treated as success so retried termination stays
    /// idempotent, but real deletion failures are propagated: returning
    /// success while staged bytes remain would orphan them forever, whereas an
    /// error lets the client retry the DELETE.
    pub(crate) async fn delete_all(&self) -> Result<()> {
        // Marker first, then the main object: once the marker is gone a torn
        // delete can no longer leave the upload repair-eligible from a gapped
        // part sequence, and once main is gone nothing is servable. The
        // remaining parts and temp objects are inert garbage that a retried
        // DELETE removes.
        self.delete_object(&self.completion_marker_key()).await?;
        self.delete_object(self.key).await?;
        self.delete_staged_parts().await?;
        self.delete_temp_objects().await
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
        let parts = self.list_parts().await?;

        let next_existing = parts
            .iter()
            .filter_map(|part| part_number(&part.key))
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let staged_size = self.sum_part_sizes(&parts).await?;

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

    /// Stages the completing part so it appears atomically at the part key.
    ///
    /// The body is written to a temporary object and promoted with the same
    /// rename-or-copy path as finalize, so the part key only ever holds the
    /// fully written final body. That is what lets the completion marker be
    /// trusted during repair: a crash mid-write leaves the temp object behind
    /// (inert garbage a later cleanup removes), never a truncated part.
    async fn stage_final_part(&self, part_key: &str, data: ChunkStream) -> Result<u64> {
        let temp_key = self.temp_key("part");
        let written = match self.write_part(&temp_key, data).await {
            Ok(written) => written,
            Err(error) => {
                let _ = self.operator.delete(&temp_key).await;
                return Err(error);
            }
        };
        self.promote_object(&temp_key, part_key).await?;
        Ok(written)
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
        let parts = self.list_parts().await?;
        if parts.is_empty() {
            return Ok(None);
        }

        Ok(Some(self.sum_part_sizes(&parts).await?))
    }

    /// Sums part sizes from listing metadata, stat-ing only entries whose
    /// listing did not include a size.
    ///
    /// This keeps `size()` (the HEAD hot path) at one listing for backends
    /// whose listings carry content lengths, instead of one stat per part.
    async fn sum_part_sizes(&self, parts: &[PartObject]) -> Result<u64> {
        let mut total = 0_u64;
        for part in parts {
            let size = match part.size {
                Some(size) => size,
                None => self
                    .operator
                    .stat(&part.key)
                    .await
                    .map_err(Error::storage)?
                    .content_length(),
            };
            total = total.saturating_add(size);
        }

        Ok(total)
    }

    async fn list_parts(&self) -> Result<Vec<PartObject>> {
        let prefix = self.parts_prefix();
        let entries = match self.operator.list(&prefix).await {
            Ok(e) => e,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::storage(e)),
        };
        let mut parts: Vec<PartObject> = entries
            .into_iter()
            .filter(|e| e.metadata().is_file())
            .map(|e| PartObject {
                key: e.path().to_string(),
                // A zero content length is indistinguishable from a listing
                // that omits sizes, so treat it as unknown; the stat fallback
                // resolves it (staged parts are almost never empty).
                size: match e.metadata().content_length() {
                    0 => None,
                    len => Some(len),
                },
            })
            .collect();
        // Zero-padded part numbers mean lex sort == numeric sort.
        parts.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(parts)
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

    async fn delete_staged_parts(&self) -> Result<()> {
        for part in self.list_parts().await? {
            self.delete_object(&part.key).await?;
        }
        self.delete_object(&self.parts_prefix()).await
    }

    async fn delete_temp_objects(&self) -> Result<()> {
        for temp_key in self.list_temp_objects().await? {
            self.delete_object(&temp_key).await?;
        }
        self.delete_object(&self.temp_prefix()).await
    }

    /// Deletes one object, treating a missing object as success.
    async fn delete_object(&self, key: &str) -> Result<()> {
        match self.operator.delete(key).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::storage(e)),
        }
    }

    async fn write_completion_marker(&self, part_number: u64) -> Result<()> {
        self.operator
            .write(&self.completion_marker_key(), part_number.to_string())
            .await
            .map(|_| ())
            .map_err(Error::storage)
    }

    /// Reads the completion marker, returning the completing part number.
    async fn read_completion_marker(&self) -> Result<Option<u64>> {
        let buffer = match self.operator.read(&self.completion_marker_key()).await {
            Ok(buffer) => buffer,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::storage(e)),
        };

        let contents = String::from_utf8_lossy(&buffer.to_bytes()).into_owned();
        match contents.trim().parse::<u64>() {
            Ok(part_number) => Ok(Some(part_number)),
            Err(_) => {
                tracing::warn!(key = self.key, "ignoring unparseable completion marker");
                Ok(None)
            }
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
        self.promote_object(temp_key, self.key).await
    }

    /// Atomically moves a temporary object onto a target key.
    ///
    /// Prefers a native rename; falls back to copy-then-delete on backends
    /// without rename. Used both to promote a materialized main object and to
    /// stage the completing part atomically.
    async fn promote_object(&self, from_key: &str, to_key: &str) -> Result<()> {
        let capability = self.operator.info().full_capability();

        if capability.rename {
            return self
                .operator
                .rename(from_key, to_key)
                .await
                .map_err(Error::storage);
        }

        if capability.copy {
            self.operator
                .copy(from_key, to_key)
                .await
                .map_err(Error::storage)?;
            let _ = self.operator.delete(from_key).await;
            return Ok(());
        }

        Err(Error::storage(
            opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "OpenDAL service must support rename or copy to promote materialized uploads",
            )
            .with_operation("tus_storage_opendal::staging::UploadObjects::promote_object")
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

    fn completion_marker_key(&self) -> String {
        format!("{}.complete", self.key)
    }
}

/// Where the next PATCH body goes and how many bytes precede it.
struct AppendPosition {
    part_number: u64,
    staged_size: u64,
    offset: u64,
}

/// A staged part object as observed in a listing.
struct PartObject {
    key: String,
    /// Size from the listing, when the backend includes one.
    size: Option<u64>,
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
                true,
            )
            .await
            .unwrap();
        assert_eq!(upload.staged_size().await.unwrap(), Some(5));

        upload.finalize().await.unwrap();

        assert_eq!(upload.stored_size().await.unwrap(), Some(5));
        assert!(upload.list_parts().await.unwrap().is_empty());
        assert!(upload.list_temp_objects().await.unwrap().is_empty());
        assert_eq!(upload.read_completion_marker().await.unwrap(), None);
    }

    #[tokio::test]
    async fn completing_append_stages_final_part_atomically() {
        let test_operator = create_test_operator();
        let upload = UploadObjects::new(&test_operator.operator, "atomic-final");
        let mut handle = StorageHandle::new("atomic-final");
        UploadObjects::initialize_handle(&mut handle);

        upload
            .append_part(
                &mut handle,
                0,
                ChunkStream::from_bytes(Bytes::from_static(b"hello")),
                true,
            )
            .await
            .unwrap();

        // The promoted part holds the full body and leaves no temp object
        // behind, so the completion marker can be trusted during repair.
        let part = test_operator
            .operator
            .read(&upload.part_key(1))
            .await
            .unwrap();
        assert_eq!(part.to_bytes().as_ref(), b"hello");
        assert!(upload.list_temp_objects().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn finalize_removes_orphaned_temp_objects_from_earlier_attempts() {
        let test_operator = create_test_operator();
        let upload = UploadObjects::new(&test_operator.operator, "finalize-orphan-cleanup");
        let mut handle = StorageHandle::new("finalize-orphan-cleanup");
        UploadObjects::initialize_handle(&mut handle);

        // A temp object left behind by a crashed earlier finalize attempt.
        test_operator
            .operator
            .write(
                "finalize-orphan-cleanup.tmp/finalize-deadbeef",
                Bytes::from_static(b"orphan"),
            )
            .await
            .unwrap();

        upload
            .append_part(
                &mut handle,
                0,
                ChunkStream::from_bytes(Bytes::from_static(b"hello")),
                true,
            )
            .await
            .unwrap();
        upload.finalize().await.unwrap();

        assert!(upload.list_temp_objects().await.unwrap().is_empty());
        assert_eq!(upload.stored_size().await.unwrap(), Some(5));
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

    #[tokio::test]
    async fn ensure_materialized_repairs_completed_but_unfinalized_upload() {
        let test_operator = create_test_operator();
        let upload = UploadObjects::new(&test_operator.operator, "repairable");
        let mut handle = StorageHandle::new("repairable");
        UploadObjects::initialize_handle(&mut handle);

        // Crash state: the completing append staged its part and marker, but
        // finalize never ran.
        upload
            .append_part(
                &mut handle,
                0,
                ChunkStream::from_bytes(Bytes::from_static(b"hello ")),
                false,
            )
            .await
            .unwrap();
        upload
            .append_part(
                &mut handle,
                6,
                ChunkStream::from_bytes(Bytes::from_static(b"world")),
                true,
            )
            .await
            .unwrap();

        assert!(upload.ensure_materialized().await.unwrap());

        let body = test_operator.operator.read("repairable").await.unwrap();
        assert_eq!(body.to_bytes().as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn ensure_materialized_leaves_incomplete_upload_alone() {
        let test_operator = create_test_operator();
        let upload = UploadObjects::new(&test_operator.operator, "incomplete");
        let mut handle = StorageHandle::new("incomplete");
        UploadObjects::initialize_handle(&mut handle);

        upload
            .append_part(
                &mut handle,
                0,
                ChunkStream::from_bytes(Bytes::from_static(b"hello")),
                false,
            )
            .await
            .unwrap();

        assert!(!upload.ensure_materialized().await.unwrap());
        assert!(matches!(
            test_operator.operator.stat("incomplete").await,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound
        ));
        // The staged bytes are untouched.
        assert_eq!(upload.staged_size().await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn ensure_materialized_refuses_gapped_part_sequence() {
        let test_operator = create_test_operator();
        let upload = UploadObjects::new(&test_operator.operator, "torn-delete");
        let mut handle = StorageHandle::new("torn-delete");
        UploadObjects::initialize_handle(&mut handle);

        upload
            .append_part(
                &mut handle,
                0,
                ChunkStream::from_bytes(Bytes::from_static(b"hello ")),
                false,
            )
            .await
            .unwrap();
        upload
            .append_part(
                &mut handle,
                6,
                ChunkStream::from_bytes(Bytes::from_static(b"world")),
                true,
            )
            .await
            .unwrap();

        // Torn delete: part 1 is gone but the marker and part 2 survive.
        // Repairing from the gapped sequence would serve truncated content.
        test_operator
            .operator
            .delete(&upload.part_key(1))
            .await
            .unwrap();

        assert!(!upload.ensure_materialized().await.unwrap());
        assert!(matches!(
            test_operator.operator.stat("torn-delete").await,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound
        ));
    }

    #[tokio::test]
    async fn concurrent_repairs_are_idempotent() {
        let test_operator = create_test_operator();
        let upload_a = UploadObjects::new(&test_operator.operator, "double-repair");
        let upload_b = UploadObjects::new(&test_operator.operator, "double-repair");
        let mut handle = StorageHandle::new("double-repair");
        UploadObjects::initialize_handle(&mut handle);

        upload_a
            .append_part(
                &mut handle,
                0,
                ChunkStream::from_bytes(Bytes::from_static(b"same bytes")),
                true,
            )
            .await
            .unwrap();

        let (a, b) = tokio::join!(
            upload_a.ensure_materialized(),
            upload_b.ensure_materialized()
        );
        assert!(a.unwrap());
        assert!(b.unwrap());

        let body = test_operator.operator.read("double-repair").await.unwrap();
        assert_eq!(body.to_bytes().as_ref(), b"same bytes");
    }
}
