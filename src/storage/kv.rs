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

//! Key-Value storage abstraction layer

use crate::error::{Result, StorageError};
use async_trait::async_trait;
use bytes::Bytes;

/// Column Family name type
pub type ColumnFamily = &'static str;

/// Key-Value storage trait
///
/// Provides async interface for persistent storage operations.
/// Implementations include RocksDB and potentially Sled.
#[async_trait]
pub trait KvStore: Send + Sync {
    /// Get value by key from default column family
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Get value by key from specific column family
    async fn get_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<Bytes>>;

    /// Put key-value pair in default column family
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Put key-value pair in specific column family
    async fn put_cf(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete key from default column family
    async fn delete(&self, key: &[u8]) -> Result<()>;

    /// Delete key from specific column family
    async fn delete_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<()>;

    /// Atomic batch write
    async fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Create iterator over all keys with given prefix
    async fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>>;

    /// Create iterator over specific column family with prefix
    async fn prefix_scan_cf(
        &self,
        cf: ColumnFamily,
        prefix: &[u8],
    ) -> Result<Vec<(Bytes, Bytes)>>;

    /// Check if key exists
    async fn exists(&self, key: &[u8]) -> Result<bool>;

    /// Check if key exists in column family
    async fn exists_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<bool>;
}

/// Batch write operations
#[derive(Debug, Default)]
pub struct WriteBatch {
    operations: Vec<BatchOperation>,
}

#[derive(Debug)]
pub enum BatchOperation {
    Put {
        cf: Option<ColumnFamily>,
        key: Bytes,
        value: Bytes,
    },
    Delete {
        cf: Option<ColumnFamily>,
        key: Bytes,
    },
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add put operation to batch
    pub fn put(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        self.operations.push(BatchOperation::Put {
            cf: None,
            key: key.into(),
            value: value.into(),
        });
    }

    /// Add put operation to specific column family
    pub fn put_cf(
        &mut self,
        cf: ColumnFamily,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) {
        self.operations.push(BatchOperation::Put {
            cf: Some(cf),
            key: key.into(),
            value: value.into(),
        });
    }

    /// Add delete operation to batch
    pub fn delete(&mut self, key: impl Into<Bytes>) {
        self.operations.push(BatchOperation::Delete {
            cf: None,
            key: key.into(),
        });
    }

    /// Add delete operation to specific column family
    pub fn delete_cf(&mut self, cf: ColumnFamily, key: impl Into<Bytes>) {
        self.operations.push(BatchOperation::Delete {
            cf: Some(cf),
            key: key.into(),
        });
    }

    /// Get number of operations in batch
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Get operations
    pub(crate) fn operations(&self) -> &[BatchOperation] {
        &self.operations
    }
}

/// Column Family definitions
pub mod cf {
    use super::ColumnFamily;

    /// Raft consensus data (logs, hard state, snapshots)
    pub const RAFT: ColumnFamily = "raft";

    /// Metadata (tasks, shards, nodes, templates, mappings)
    pub const META: ColumnFamily = "meta";

    /// Offset tracking for shard checkpoints
    pub const OFFSET: ColumnFamily = "offset";

    /// Authentication data (users, API keys, roles)
    pub const AUTH: ColumnFamily = "auth";

    /// Alert rules and history
    pub const ALERT: ColumnFamily = "alert";

    /// Get all column family names
    pub fn all() -> &'static [ColumnFamily] {
        &[RAFT, META, OFFSET, AUTH, ALERT]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_batch() {
        let mut batch = WriteBatch::new();
        assert!(batch.is_empty());

        batch.put("key1", "value1");
        batch.put_cf(cf::META, "key2", "value2");
        batch.delete("key3");

        assert_eq!(batch.len(), 3);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_column_families() {
        let cfs = cf::all();
        assert_eq!(cfs.len(), 5);
        assert!(cfs.contains(&cf::RAFT));
        assert!(cfs.contains(&cf::META));
        assert!(cfs.contains(&cf::OFFSET));
    }
}
