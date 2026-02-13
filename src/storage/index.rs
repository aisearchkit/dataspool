// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Dataspool Contributors

//! Index storage layer for migration indexes
//!
//! This module provides high-performance index storage for large-scale
//! migration data using eloqstore. Suitable for storing:
//! - Document ID mappings
//! - Primary key indexes
//! - Temporary migration metadata
//!
//! Unlike RocksDB (for system metadata), eloqstore supports:
//! - Local disk + S3 hybrid storage
//! - Automatic tiering (hot/warm/cold)
//! - Higher throughput for bulk operations

use crate::error::Result;
use async_trait::async_trait;

/// Index table identifier
#[derive(Debug, Clone)]
pub struct IndexTable {
    /// Table name (e.g., "es_doc_ids", "mysql_pk_index")
    pub name: String,
    /// Partition ID (for parallel writes)
    pub partition: u32,
}

impl IndexTable {
    pub fn new(name: impl Into<String>, partition: u32) -> Self {
        Self {
            name: name.into(),
            partition,
        }
    }
}

/// Index storage operations
#[async_trait]
pub trait IndexStore: Send + Sync {
    /// Start the index store
    async fn start(&mut self) -> Result<()>;

    /// Stop the index store gracefully
    async fn stop(&mut self) -> Result<()>;

    /// Put a key-value pair into the index table
    async fn put(&self, table: &IndexTable, key: &[u8], value: &[u8]) -> Result<()>;

    /// Put with sequence number (for versioning)
    async fn put_with_seq(
        &self,
        table: &IndexTable,
        key: &[u8],
        value: &[u8],
        seq: u64,
    ) -> Result<()>;

    /// Get value by key
    async fn get(&self, table: &IndexTable, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Delete a key
    async fn delete(&self, table: &IndexTable, key: &[u8]) -> Result<()>;

    /// Scan a range of keys
    async fn scan(
        &self,
        table: &IndexTable,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Flush pending writes to disk
    async fn flush(&self, table: &IndexTable) -> Result<()>;
}

/// Mock implementation for testing (without eloqstore dependency)
#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    pub struct MockIndexStore {
        tables: Arc<RwLock<HashMap<String, HashMap<Vec<u8>, Vec<u8>>>>>,
    }

    impl MockIndexStore {
        pub fn new() -> Self {
            Self {
                tables: Arc::new(RwLock::new(HashMap::new())),
            }
        }

        fn table_key(table: &IndexTable) -> String {
            format!("{}:{}", table.name, table.partition)
        }
    }

    #[async_trait]
    impl IndexStore for MockIndexStore {
        async fn start(&mut self) -> Result<()> {
            Ok(())
        }

        async fn stop(&mut self) -> Result<()> {
            Ok(())
        }

        async fn put(&self, table: &IndexTable, key: &[u8], value: &[u8]) -> Result<()> {
            let table_key = Self::table_key(table);
            let mut tables = self.tables.write().await;
            tables
                .entry(table_key)
                .or_insert_with(HashMap::new)
                .insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        async fn put_with_seq(
            &self,
            table: &IndexTable,
            key: &[u8],
            value: &[u8],
            _seq: u64,
        ) -> Result<()> {
            self.put(table, key, value).await
        }

        async fn get(&self, table: &IndexTable, key: &[u8]) -> Result<Option<Vec<u8>>> {
            let table_key = Self::table_key(table);
            let tables = self.tables.read().await;
            Ok(tables.get(&table_key).and_then(|t| t.get(key).cloned()))
        }

        async fn delete(&self, table: &IndexTable, key: &[u8]) -> Result<()> {
            let table_key = Self::table_key(table);
            let mut tables = self.tables.write().await;
            if let Some(table_map) = tables.get_mut(&table_key) {
                table_map.remove(key);
            }
            Ok(())
        }

        async fn scan(
            &self,
            table: &IndexTable,
            start_key: &[u8],
            end_key: &[u8],
            limit: usize,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let table_key = Self::table_key(table);
            let tables = self.tables.read().await;

            if let Some(table_map) = tables.get(&table_key) {
                let mut results: Vec<_> = table_map
                    .iter()
                    .filter(|(k, _)| k.as_slice() >= start_key && k.as_slice() < end_key)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                results.sort_by(|a, b| a.0.cmp(&b.0));
                results.truncate(limit);
                Ok(results)
            } else {
                Ok(vec![])
            }
        }

        async fn flush(&self, _table: &IndexTable) -> Result<()> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::mock::MockIndexStore;
    use super::*;

    #[tokio::test]
    async fn test_mock_index_store() {
        let mut store = MockIndexStore::new();
        store.start().await.unwrap();

        let table = IndexTable::new("test_table", 0);

        // Test put and get
        store.put(&table, b"key1", b"value1").await.unwrap();
        let value = store.get(&table, b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Test put_with_seq
        store
            .put_with_seq(&table, b"key2", b"value2", 1)
            .await
            .unwrap();
        let value = store.get(&table, b"key2").await.unwrap();
        assert_eq!(value, Some(b"value2".to_vec()));

        // Test delete
        store.delete(&table, b"key1").await.unwrap();
        let value = store.get(&table, b"key1").await.unwrap();
        assert_eq!(value, None);

        // Test scan
        store.put(&table, b"a", b"val_a").await.unwrap();
        store.put(&table, b"b", b"val_b").await.unwrap();
        store.put(&table, b"c", b"val_c").await.unwrap();

        let results = store.scan(&table, b"a", b"c", 10).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"a");
        assert_eq!(results[1].0, b"b");

        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_tables() {
        let mut store = MockIndexStore::new();
        store.start().await.unwrap();

        let table1 = IndexTable::new("table1", 0);
        let table2 = IndexTable::new("table2", 0);

        store.put(&table1, b"key", b"value1").await.unwrap();
        store.put(&table2, b"key", b"value2").await.unwrap();

        assert_eq!(
            store.get(&table1, b"key").await.unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            store.get(&table2, b"key").await.unwrap(),
            Some(b"value2".to_vec())
        );

        store.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_partitioned_table() {
        let mut store = MockIndexStore::new();
        store.start().await.unwrap();

        let partition0 = IndexTable::new("test", 0);
        let partition1 = IndexTable::new("test", 1);

        store.put(&partition0, b"key", b"p0_value").await.unwrap();
        store.put(&partition1, b"key", b"p1_value").await.unwrap();

        assert_eq!(
            store.get(&partition0, b"key").await.unwrap(),
            Some(b"p0_value".to_vec())
        );
        assert_eq!(
            store.get(&partition1, b"key").await.unwrap(),
            Some(b"p1_value".to_vec())
        );

        store.stop().await.unwrap();
    }
}
