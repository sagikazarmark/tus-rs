use std::rc::Rc;

use futures_util::StreamExt;
use rexie::{Rexie, TransactionMode, TransactionResult};
use thiserror::Error as ThisError;
use tus_protocol::{
    AppendRequest, ChunkStream, ConcatRequest, Error, StateStore, Storage, StorageHandle, UploadId,
    UploadState, WriteMode,
    async_trait::async_trait,
    chrono::{DateTime, Utc},
};
use wasm_bindgen::JsValue;

#[derive(Clone, Debug)]
pub(crate) struct BrowserDatabase {
    database: Rc<Rexie>,
    state_store: &'static str,
    size_store: &'static str,
}

impl BrowserDatabase {
    pub(crate) fn new(
        database: Rexie,
        state_store: &'static str,
        size_store: &'static str,
    ) -> Self {
        Self {
            database: Rc::new(database),
            state_store,
            size_store,
        }
    }

    async fn get_string(
        &self,
        store_name: &str,
        key: &str,
    ) -> Result<Option<String>, DatabaseError> {
        let transaction = self
            .database
            .transaction(&[store_name], TransactionMode::ReadOnly)?;
        let store = transaction.store(store_name)?;
        let value = store.get(JsValue::from_str(key)).await?;
        ensure_committed(transaction.done().await?)?;
        value.map(js_string).transpose()
    }

    async fn add_string(
        &self,
        store_name: &str,
        key: &str,
        value: &str,
    ) -> Result<(), DatabaseError> {
        let transaction = self
            .database
            .transaction(&[store_name], TransactionMode::ReadWrite)?;
        let store = transaction.store(store_name)?;
        let key = JsValue::from_str(key);
        store.add(&JsValue::from_str(value), Some(&key)).await?;
        ensure_committed(transaction.done().await?)
    }

    async fn put_string(
        &self,
        store_name: &str,
        key: &str,
        value: &str,
    ) -> Result<(), DatabaseError> {
        let transaction = self
            .database
            .transaction(&[store_name], TransactionMode::ReadWrite)?;
        let store = transaction.store(store_name)?;
        let key = JsValue::from_str(key);
        store.put(&JsValue::from_str(value), Some(&key)).await?;
        ensure_committed(transaction.done().await?)
    }

    async fn delete_key(&self, store_name: &str, key: &str) -> Result<(), DatabaseError> {
        let transaction = self
            .database
            .transaction(&[store_name], TransactionMode::ReadWrite)?;
        transaction
            .store(store_name)?
            .delete(JsValue::from_str(key))
            .await?;
        ensure_committed(transaction.done().await?)
    }

    async fn all_strings(&self, store_name: &str) -> Result<Vec<String>, DatabaseError> {
        let transaction = self
            .database
            .transaction(&[store_name], TransactionMode::ReadOnly)?;
        let values = transaction.store(store_name)?.get_all(None, None).await?;
        ensure_committed(transaction.done().await?)?;
        values.into_iter().map(js_string).collect()
    }

    async fn accepted_size(&self, key: &str) -> Result<Option<u64>, DatabaseError> {
        self.get_string(self.size_store, key)
            .await?
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| DatabaseError::InvalidRecord("accepted size is not a u64"))
            })
            .transpose()
    }

    async fn compare_and_set_size(
        &self,
        key: &str,
        expected: u64,
        next: u64,
    ) -> Result<(), DatabaseError> {
        let transaction = self
            .database
            .transaction(&[self.size_store], TransactionMode::ReadWrite)?;
        let store = transaction.store(self.size_store)?;
        let js_key = JsValue::from_str(key);
        let actual = store
            .get(js_key.clone())
            .await?
            .ok_or(DatabaseError::MissingRecord)
            .and_then(js_string)?
            .parse::<u64>()
            .map_err(|_| DatabaseError::InvalidRecord("accepted size is not a u64"))?;
        if actual != expected {
            return Err(DatabaseError::StaleSize { expected, actual });
        }
        store
            .put(&JsValue::from_str(&next.to_string()), Some(&js_key))
            .await?;
        ensure_committed(transaction.done().await?)
    }
}

#[async_trait(?Send)]
impl Storage for BrowserDatabase {
    fn name(&self) -> &'static str {
        "browser-discard"
    }

    async fn create(&self, upload_id: &str) -> tus_protocol::Result<StorageHandle> {
        let handle = StorageHandle::new(upload_id);
        if self
            .accepted_size(handle.key())
            .await
            .map_err(Error::storage)?
            .is_some()
        {
            return Err(Error::AlreadyExists(upload_id.to_string()));
        }
        self.add_string(self.size_store, handle.key(), "0")
            .await
            .map_err(Error::storage)?;
        Ok(handle)
    }

    async fn append(&self, request: AppendRequest) -> tus_protocol::Result<StorageHandle> {
        let AppendRequest {
            handle,
            expected_offset,
            data,
            ..
        } = request;
        let key = handle.key();
        let actual = self
            .accepted_size(key)
            .await
            .map_err(Error::storage)?
            .ok_or_else(|| Error::NotFound(key.to_string()))?;
        if actual != expected_offset {
            return Err(Error::Internal(format!(
                "discard storage size {actual} does not match expected offset {expected_offset}"
            )));
        }

        let appended = match data {
            ChunkStream::Buffered(bytes) => bytes.len() as u64,
            ChunkStream::Stream(mut stream) => {
                let mut length = 0_u64;
                while let Some(chunk) = stream.next().await {
                    length = length
                        .checked_add(chunk?.len() as u64)
                        .ok_or_else(|| Error::Internal("chunk length overflow".to_string()))?;
                }
                length
            }
        };
        let next = expected_offset
            .checked_add(appended)
            .ok_or_else(|| Error::Internal("upload offset overflow".to_string()))?;
        self.compare_and_set_size(key, expected_offset, next)
            .await
            .map_err(Error::storage)?;
        Ok(handle)
    }

    async fn concat(&self, _request: ConcatRequest) -> tus_protocol::Result<StorageHandle> {
        Err(Error::ExtensionNotSupported("concatenation".to_string()))
    }

    async fn delete(&self, handle: &StorageHandle) -> tus_protocol::Result<()> {
        self.delete_key(self.size_store, handle.key())
            .await
            .map_err(Error::storage)
    }

    async fn size(&self, handle: &StorageHandle) -> tus_protocol::Result<Option<u64>> {
        self.accepted_size(handle.key())
            .await
            .map_err(Error::storage)
    }
}

#[async_trait(?Send)]
impl StateStore for BrowserDatabase {
    fn name(&self) -> &'static str {
        "indexeddb"
    }

    async fn set(&self, state: &UploadState, mode: WriteMode) -> tus_protocol::Result<()> {
        validate_upload_id(state.id())?;
        let value = serde_json::to_string(state).map_err(Error::state_store)?;
        match mode {
            WriteMode::CreateNew => {
                if self.get(state.id()).await?.is_some() {
                    return Err(Error::AlreadyExists(state.id().to_string()));
                }
                self.add_string(self.state_store, state.id(), &value)
                    .await
                    .map_err(Error::state_store)
            }
            WriteMode::Update => self
                .put_string(self.state_store, state.id(), &value)
                .await
                .map_err(Error::state_store),
            _ => Err(Error::Internal(
                "unsupported browser state write mode".to_string(),
            )),
        }
    }

    async fn get(&self, id: &str) -> tus_protocol::Result<Option<UploadState>> {
        validate_upload_id(id)?;
        self.get_string(self.state_store, id)
            .await
            .map_err(Error::state_store)?
            .map(|value| serde_json::from_str(&value).map_err(Error::state_store))
            .transpose()
    }

    async fn delete(&self, id: &str) -> tus_protocol::Result<()> {
        validate_upload_id(id)?;
        self.delete_key(self.state_store, id)
            .await
            .map_err(Error::state_store)
    }

    async fn list_expired(&self, before: DateTime<Utc>) -> tus_protocol::Result<Vec<String>> {
        let values = self
            .all_strings(self.state_store)
            .await
            .map_err(Error::state_store)?;
        values
            .into_iter()
            .map(|value| serde_json::from_str::<UploadState>(&value).map_err(Error::state_store))
            .filter_map(|state| match state {
                Ok(state) if state.expires_before(before) => Some(Ok(state.id().to_string())),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }
}

fn validate_upload_id(id: &str) -> tus_protocol::Result<()> {
    id.parse::<UploadId>()?;
    Ok(())
}

fn ensure_committed(result: TransactionResult) -> Result<(), DatabaseError> {
    if result.is_committed() {
        Ok(())
    } else {
        Err(DatabaseError::TransactionAborted)
    }
}

fn js_string(value: JsValue) -> Result<String, DatabaseError> {
    value
        .as_string()
        .ok_or(DatabaseError::InvalidRecord("value is not a string"))
}

#[derive(Debug, ThisError)]
enum DatabaseError {
    #[error("IndexedDB operation failed")]
    Rexie(#[from] rexie::Error),
    #[error("IndexedDB transaction aborted")]
    TransactionAborted,
    #[error("IndexedDB record is missing")]
    MissingRecord,
    #[error("invalid IndexedDB record: {0}")]
    InvalidRecord(&'static str),
    #[error("accepted size changed from {expected} to {actual}")]
    StaleSize { expected: u64, actual: u64 },
}
