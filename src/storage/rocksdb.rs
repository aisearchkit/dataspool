// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

//! RocksDB storage implementation

use super::kv::{ColumnFamily, KvStore, WriteBatch, cf};
use crate::error::{Result, StorageError};
use async_trait::async_trait;
use bytes::Bytes;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use std::path::Path;
use std::sync::Arc;
use tokio::task;

/// RocksDB-based storage implementation
#[derive(Clone)]
pub struct RocksDbStore {
    db: Arc<DB>,
}

impl RocksDbStore {
    /// Open or create RocksDB with default column families
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Configure for optimal write performance
        opts.set_max_background_jobs(4);
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        opts.set_max_write_buffer_number(3);
        opts.set_min_write_buffer_number_to_merge(2);

        // Enable compression
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        // Create column family descriptors
        let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf::all()
            .iter()
            .map(|&name| {
                let mut cf_opts = Options::default();

                // Optimize for different CF workloads
                match name {
                    cf::RAFT => {
                        // Raft CF: sequential writes, range scans
                        cf_opts
                            .set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(8));
                    }
                    cf::OFFSET => {
                        // Offset CF: frequent updates
                        cf_opts.set_max_write_buffer_number(2);
                    }
                    _ => {
                        // Default settings for other CFs
                    }
                }

                ColumnFamilyDescriptor::new(name, cf_opts)
            })
            .collect();

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Get column family handle
    fn get_cf_handle(&self, cf: ColumnFamily) -> Result<&rocksdb::ColumnFamily> {
        self.db.cf_handle(cf).ok_or_else(|| {
            StorageError::DatabaseError(format!("Column family not found: {}", cf)).into()
        })
    }
}

#[async_trait]
impl KvStore for RocksDbStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let db = self.db.clone();
        let key = key.to_vec();

        let result = task::spawn_blocking(move || {
            db.get(&key)
                .map_err(|e| StorageError::ReadFailed(e.to_string()))
                .map(|opt| opt.map(Bytes::from))
        })
        .await
        .map_err(|e| StorageError::DatabaseError(format!("Task join error: {}", e)))?;

        result.map_err(Into::into)
    }

    async fn get_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<Bytes>> {
        let db = self.db.clone();
        let cf_name = cf.to_string();
        let key = key.to_vec();

        let result = task::spawn_blocking(move || {
            let cf_handle = db.cf_handle(&cf_name).ok_or_else(|| {
                StorageError::DatabaseError(format!("Column family not found: {}", cf_name))
            })?;

            db.get_cf(cf_handle, &key)
                .map_err(|e| StorageError::ReadFailed(e.to_string()))
                .map(|opt| opt.map(Bytes::from))
        })
        .await
        .map_err(|e| StorageError::DatabaseError(format!("Task join error: {}", e)))?;

        result.map_err(Into::into)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let db = self.db.clone();
        let key = key.to_vec();
        let value = value.to_vec();

        task::spawn_blocking(move || {
            db.put(&key, &value)
                .map_err(|e| StorageError::WriteFailed(e.to_string()).into())
        })
        .await
        .map_err(|e| StorageError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn put_cf(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<()> {
        let db = self.db.clone();
        let cf_name = cf.to_string();
        let key = key.to_vec();
        let value = value.to_vec();

        task::spawn_blocking(move || {
            let cf_handle = db.cf_handle(&cf_name).ok_or_else(|| {
                StorageError::DatabaseError(format!("Column family not found: {}", cf_name))
            })?;

            db.put_cf(cf_handle, &key, &value)
                .map_err(|e| StorageError::WriteFailed(e.to_string()).into())
        })
        .await
        .map_err(|e| StorageError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let db = self.db.clone();
        let key = key.to_vec();

        task::spawn_blocking(move || {
            db.delete(&key)
                .map_err(|e| StorageError::WriteFailed(e.to_string()).into())
        })
        .await
        .map_err(|e| StorageError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn delete_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<()> {
        let db = self.db.clone();
        let cf_name = cf.to_string();
        let key = key.to_vec();

        task::spawn_blocking(move || {
            let cf_handle = db.cf_handle(&cf_name).ok_or_else(|| {
                StorageError::DatabaseError(format!("Column family not found: {}", cf_name))
            })?;

            db.delete_cf(cf_handle, &key)
                .map_err(|e| StorageError::WriteFailed(e.to_string()).into())
        })
        .await
        .map_err(|e| StorageError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let db = self.db.clone();
        let mut rocksdb_batch = rocksdb::WriteBatch::default();

        // Pre-collect CF handles we need
        let _cf_names: Vec<_> = batch
            .operations()
            .iter()
            .filter_map(|op| match op {
                super::kv::BatchOperation::Put { cf: Some(cf), .. } => Some(*cf),
                super::kv::BatchOperation::Delete { cf: Some(cf), .. } => Some(*cf),
                _ => None,
            })
            .collect();

        // Build batch with references
        for op in batch.operations() {
            match op {
                super::kv::BatchOperation::Put { cf, key, value } => {
                    if let Some(cf_name) = cf {
                        let cf_handle = self.get_cf_handle(cf_name)?;
                        rocksdb_batch.put_cf(cf_handle, key, value);
                    } else {
                        rocksdb_batch.put(key, value);
                    }
                }
                super::kv::BatchOperation::Delete { cf, key } => {
                    if let Some(cf_name) = cf {
                        let cf_handle = self.get_cf_handle(cf_name)?;
                        rocksdb_batch.delete_cf(cf_handle, key);
                    } else {
                        rocksdb_batch.delete(key);
                    }
                }
            }
        }

        task::spawn_blocking(move || {
            db.write(rocksdb_batch)
                .map_err(|e| StorageError::WriteFailed(e.to_string()).into())
        })
        .await
        .map_err(|e| StorageError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let db = self.db.clone();
        let prefix = prefix.to_vec();

        task::spawn_blocking(move || {
            let mut result = Vec::new();
            let iter = db.iterator(rocksdb::IteratorMode::From(
                &prefix,
                rocksdb::Direction::Forward,
            ));

            for item in iter {
                let (key, value) = item.map_err(|e| StorageError::ReadFailed(e.to_string()))?;
                if !key.starts_with(&prefix) {
                    break;
                }
                result.push((Bytes::from(key.to_vec()), Bytes::from(value.to_vec())));
            }

            Ok(result)
        })
        .await
        .map_err(|e| StorageError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn prefix_scan_cf(&self, cf: ColumnFamily, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let db = self.db.clone();
        let cf_name = cf.to_string();
        let prefix = prefix.to_vec();

        task::spawn_blocking(move || {
            let cf_handle = db.cf_handle(&cf_name).ok_or_else(|| {
                StorageError::DatabaseError(format!("Column family not found: {}", cf_name))
            })?;

            let mut result = Vec::new();
            let iter = db.iterator_cf(
                cf_handle,
                rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            );

            for item in iter {
                let (key, value) = item.map_err(|e| StorageError::ReadFailed(e.to_string()))?;
                if !key.starts_with(&prefix) {
                    break;
                }
                result.push((Bytes::from(key.to_vec()), Bytes::from(value.to_vec())));
            }

            Ok(result)
        })
        .await
        .map_err(|e| StorageError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn prefix_scan_page_cf(
        &self,
        cf: ColumnFamily,
        prefix: &[u8],
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let db = self.db.clone();
        let cf_name = cf.to_string();
        let prefix = prefix.to_vec();
        task::spawn_blocking(move || {
            let cf_handle = db.cf_handle(&cf_name).ok_or_else(|| {
                StorageError::DatabaseError(format!("Column family not found: {cf_name}"))
            })?;
            let mut result = Vec::with_capacity(limit);
            let iter = db.iterator_cf(
                cf_handle,
                rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            );
            let mut skipped = 0usize;
            for item in iter {
                let (key, value) = item.map_err(|e| StorageError::ReadFailed(e.to_string()))?;
                if !key.starts_with(&prefix) {
                    break;
                }
                if skipped < offset {
                    skipped += 1;
                    continue;
                }
                result.push((Bytes::from(key.to_vec()), Bytes::from(value.to_vec())));
                if result.len() >= limit {
                    break;
                }
            }
            Ok(result)
        })
        .await
        .map_err(|e| StorageError::DatabaseError(format!("Task join error: {e}")))?
    }

    async fn count_prefix_cf(&self, cf: ColumnFamily, prefix: &[u8]) -> Result<usize> {
        let db = self.db.clone();
        let cf_name = cf.to_string();
        let prefix = prefix.to_vec();
        task::spawn_blocking(move || {
            let cf_handle = db.cf_handle(&cf_name).ok_or_else(|| {
                StorageError::DatabaseError(format!("Column family not found: {cf_name}"))
            })?;
            let mut count = 0usize;
            let iter = db.iterator_cf(
                cf_handle,
                rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            );
            for item in iter {
                let (key, _) = item.map_err(|e| StorageError::ReadFailed(e.to_string()))?;
                if !key.starts_with(&prefix) {
                    break;
                }
                count = count.saturating_add(1);
            }
            Ok(count)
        })
        .await
        .map_err(|e| StorageError::DatabaseError(format!("Task join error: {e}")))?
    }

    async fn exists(&self, key: &[u8]) -> Result<bool> {
        self.get(key).await.map(|opt| opt.is_some())
    }

    async fn exists_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<bool> {
        self.get_cf(cf, key).await.map(|opt| opt.is_some())
    }
}

// Make BatchOperation public for use in RocksDB implementation

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_store() -> (RocksDbStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = RocksDbStore::open(temp_dir.path()).unwrap();
        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_basic_operations() {
        let (store, _temp_dir) = create_test_store().await;

        // Put and get
        store.put(b"key1", b"value1").await.unwrap();
        let value = store.get(b"key1").await.unwrap();
        assert_eq!(value.unwrap().as_ref(), b"value1");

        // Delete
        store.delete(b"key1").await.unwrap();
        let value = store.get(b"key1").await.unwrap();
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_column_family_operations() {
        let (store, _temp_dir) = create_test_store().await;

        // Put in META column family
        store
            .put_cf(cf::META, b"task:123", b"task_data")
            .await
            .unwrap();
        let value = store.get_cf(cf::META, b"task:123").await.unwrap();
        assert_eq!(value.unwrap().as_ref(), b"task_data");

        // Verify it doesn't exist in default CF
        let value = store.get(b"task:123").await.unwrap();
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_batch_write() {
        let (store, _temp_dir) = create_test_store().await;

        let mut batch = WriteBatch::new();
        batch.put("key1", "value1");
        batch.put_cf(cf::META, "key2", "value2");
        batch.delete("key3");

        store.write_batch(batch).await.unwrap();

        assert_eq!(
            store.get(b"key1").await.unwrap().unwrap().as_ref(),
            b"value1"
        );
        assert_eq!(
            store
                .get_cf(cf::META, b"key2")
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"value2"
        );
    }

    #[tokio::test]
    async fn test_prefix_scan() {
        let (store, _temp_dir) = create_test_store().await;

        // Insert multiple keys with same prefix
        store.put(b"task:001", b"data1").await.unwrap();
        store.put(b"task:002", b"data2").await.unwrap();
        store.put(b"task:003", b"data3").await.unwrap();
        store.put(b"other:001", b"other").await.unwrap();

        let results = store.prefix_scan(b"task:").await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_exists() {
        let (store, _temp_dir) = create_test_store().await;

        assert!(!store.exists(b"key1").await.unwrap());

        store.put(b"key1", b"value1").await.unwrap();
        assert!(store.exists(b"key1").await.unwrap());

        store.delete(b"key1").await.unwrap();
        assert!(!store.exists(b"key1").await.unwrap());
    }
}
