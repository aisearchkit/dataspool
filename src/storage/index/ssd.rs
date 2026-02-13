// SPDX-License-Identifier: AGPL-3.0-or-later

//! L2: SSD layer using Parquet files

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use arrow::array::Array;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::error::{DatspoolError, Result};
use crate::storage::KvStore;

pub struct SsdStore<S: KvStore> {
    base_dir: PathBuf,
    capacity_bytes: u64,
    #[allow(dead_code)]
    metadata_store: Arc<S>,
    current_size: Arc<RwLock<u64>>,
}

impl<S: KvStore> SsdStore<S> {
    pub fn new(base_dir: PathBuf, capacity_bytes: u64, metadata_store: Arc<S>) -> Self {
        Self {
            base_dir,
            capacity_bytes,
            metadata_store,
            current_size: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn get(&self, task_id: &str, chunk_id: u32) -> Result<Option<Vec<String>>> {
        let path = self.chunk_path(task_id, chunk_id);

        if !path.exists() {
            return Ok(None);
        }

        let file = std::fs::File::open(&path).map_err(|e| {
            DatspoolError::Storage(crate::error::StorageError::ReadFailed(e.to_string()))
        })?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| {
                DatspoolError::Storage(crate::error::StorageError::ReadFailed(e.to_string()))
            })?
            .build()
            .map_err(|e| {
                DatspoolError::Storage(crate::error::StorageError::ReadFailed(e.to_string()))
            })?;

        let mut doc_ids = Vec::new();

        for batch in reader {
            let batch = batch.map_err(|e| {
                DatspoolError::Storage(crate::error::StorageError::ReadFailed(e.to_string()))
            })?;

            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..col.len() {
                if !col.is_null(i) {
                    doc_ids.push(col.value(i).to_string());
                }
            }
        }

        Ok(Some(doc_ids))
    }

    pub async fn put(&self, task_id: String, chunk_id: u32, doc_ids: Vec<String>) -> Result<()> {
        let path = self.chunk_path(&task_id, chunk_id);

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                DatspoolError::Storage(crate::error::StorageError::WriteFailed(e.to_string()))
            })?;
        }

        let schema = Schema::new(vec![Field::new("doc_id", DataType::Utf8, false)]);
        let doc_id_array = StringArray::from(doc_ids.clone());
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(doc_id_array)])
            .map_err(|e| {
                DatspoolError::Storage(crate::error::StorageError::WriteFailed(e.to_string()))
            })?;

        let file = std::fs::File::create(&path).map_err(|e| {
            DatspoolError::Storage(crate::error::StorageError::WriteFailed(e.to_string()))
        })?;

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer =
            ArrowWriter::try_new(file, Arc::new(schema), Some(props)).map_err(|e| {
                DatspoolError::Storage(crate::error::StorageError::WriteFailed(e.to_string()))
            })?;

        writer.write(&batch).map_err(|e| {
            DatspoolError::Storage(crate::error::StorageError::WriteFailed(e.to_string()))
        })?;

        writer.close().map_err(|e| {
            DatspoolError::Storage(crate::error::StorageError::WriteFailed(e.to_string()))
        })?;

        let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        *self.current_size.write().await += file_size;

        info!("Wrote {} doc IDs ({} bytes)", doc_ids.len(), file_size);
        Ok(())
    }

    fn chunk_path(&self, task_id: &str, chunk_id: u32) -> PathBuf {
        self.base_dir
            .join(task_id)
            .join(format!("chunk-{:08}.parquet", chunk_id))
    }

    pub async fn current_size_bytes(&self) -> u64 {
        *self.current_size.read().await
    }

    pub async fn is_over_capacity(&self) -> bool {
        self.current_size_bytes().await > self.capacity_bytes
    }

    #[allow(dead_code)]
    pub async fn evict_oldest(&self) -> Result<Option<PathBuf>> {
        warn!("evict_oldest not implemented yet");
        Ok(None)
    }
}
