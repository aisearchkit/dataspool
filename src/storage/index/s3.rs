// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! L3: S3 object storage layer using Parquet
//!
//! This layer stores cold index data in S3-compatible object storage:
//! - Path: s3://{bucket}/indexes/{task_id}/chunk-{chunk_id}.parquet
//! - Same format as L2 (Parquet with Snappy compression)
//! - On-demand download to temporary directory
//! - No local caching (use L1/L2 for hot data)

use std::path::Path;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tracing::{debug, info, warn};

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::error::{DatspoolError, Result};

/// S3-based Parquet storage
pub struct S3Store {
    bucket: String,
    region: String,
    endpoint: Option<String>,
    client: OnceCell<S3Client>,
}

impl S3Store {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            region: "us-east-1".to_string(),
            endpoint: None,
            client: OnceCell::new(),
        }
    }

    pub fn with_config(bucket: String, region: String, endpoint: Option<String>) -> Self {
        Self {
            bucket,
            region,
            endpoint,
            client: OnceCell::new(),
        }
    }

    /// Get S3 client (lazy initialization)
    async fn client(&self) -> Result<&S3Client> {
        self.client
            .get_or_try_init(|| async {
                let config = if let Some(endpoint) = &self.endpoint {
                    // MinIO or custom S3-compatible endpoint
                    aws_config::from_env()
                        .region(aws_config::Region::new(self.region.clone()))
                        .endpoint_url(endpoint)
                        .load()
                        .await
                } else {
                    // AWS S3
                    aws_config::from_env()
                        .region(aws_config::Region::new(self.region.clone()))
                        .load()
                        .await
                };

                Ok(S3Client::new(&config))
            })
            .await
    }

    /// Get document IDs from S3 Parquet file
    pub async fn get(&self, task_id: &str, chunk_id: u32) -> Result<Option<Vec<String>>> {
        let s3_key = self.s3_key(task_id, chunk_id);
        let client = self.client().await?;

        // Download from S3
        debug!("Downloading s3://{}/{}", self.bucket, s3_key);

        let response = client
            .get_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .send()
            .await;

        match response {
            Ok(output) => {
                // Write to temporary file
                let temp_file = tempfile::NamedTempFile::new().map_err(|e| {
                    DatspoolError::Storage(crate::error::StorageError::ReadFailed(e.to_string()))
                })?;

                let body = output.body.collect().await.map_err(|e| {
                    DatspoolError::Storage(crate::error::StorageError::ReadFailed(e.to_string()))
                })?;

                std::fs::write(temp_file.path(), body.into_bytes()).map_err(|e| {
                    DatspoolError::Storage(crate::error::StorageError::ReadFailed(e.to_string()))
                })?;

                // Parse Parquet
                let file = std::fs::File::open(temp_file.path()).map_err(|e| {
                    DatspoolError::Storage(crate::error::StorageError::ReadFailed(e.to_string()))
                })?;

                let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|e| {
                        DatspoolError::Storage(crate::error::StorageError::ReadFailed(
                            e.to_string(),
                        ))
                    })?
                    .build()
                    .map_err(|e| {
                        DatspoolError::Storage(crate::error::StorageError::ReadFailed(
                            e.to_string(),
                        ))
                    })?;

                let mut doc_ids = Vec::new();
                for batch in reader {
                    let batch = batch.map_err(|e| {
                        DatspoolError::Storage(crate::error::StorageError::ReadFailed(
                            e.to_string(),
                        ))
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

                info!("Downloaded {} doc IDs from S3", doc_ids.len());
                Ok(Some(doc_ids))
            }
            Err(e) if e.to_string().contains("NoSuchKey") => {
                debug!("S3 key not found: {}", s3_key);
                Ok(None)
            }
            Err(e) => Err(DatspoolError::Storage(
                crate::error::StorageError::ReadFailed(format!("S3 error: {}", e)),
            )),
        }
    }

    /// Upload document IDs to S3 as Parquet
    pub async fn put(&self, task_id: String, chunk_id: u32, doc_ids: Vec<String>) -> Result<()> {
        // Build Parquet file in memory
        let temp_file = tempfile::NamedTempFile::new().map_err(|e| {
            DatspoolError::Storage(crate::error::StorageError::WriteFailed(e.to_string()))
        })?;

        let schema = Schema::new(vec![Field::new("doc_id", DataType::Utf8, false)]);
        let doc_id_array = StringArray::from(doc_ids.clone());
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(doc_id_array)])
            .map_err(|e| {
                DatspoolError::Storage(crate::error::StorageError::WriteFailed(e.to_string()))
            })?;

        let file = std::fs::File::create(temp_file.path()).map_err(|e| {
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

        // Upload to S3
        self.upload_file(temp_file.path(), &task_id, chunk_id)
            .await?;

        Ok(())
    }

    /// Upload existing Parquet file to S3
    pub async fn upload_file(&self, local_path: &Path, task_id: &str, chunk_id: u32) -> Result<()> {
        let s3_key = self.s3_key(task_id, chunk_id);
        let client = self.client().await?;

        let body = ByteStream::from_path(local_path).await.map_err(|e| {
            DatspoolError::Storage(crate::error::StorageError::WriteFailed(e.to_string()))
        })?;

        info!("Uploading to s3://{}/{}", self.bucket, s3_key);

        client
            .put_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .body(body)
            .content_type("application/octet-stream")
            .send()
            .await
            .map_err(|e| {
                DatspoolError::Storage(crate::error::StorageError::WriteFailed(format!(
                    "S3 upload error: {}",
                    e
                )))
            })?;

        info!(
            "Uploaded {} bytes to S3",
            std::fs::metadata(local_path).unwrap().len()
        );
        Ok(())
    }

    /// Generate S3 key for a chunk
    fn s3_key(&self, task_id: &str, chunk_id: u32) -> String {
        format!("indexes/{}/chunk-{:08}.parquet", task_id, chunk_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_key_format() {
        let store = S3Store::new("test-bucket".to_string());
        assert_eq!(
            store.s3_key("task-123", 5),
            "indexes/task-123/chunk-00000005.parquet"
        );
    }

    // Note: Real S3 tests require AWS credentials and are expensive
    // Run them manually with: cargo test s3 -- --ignored
}
