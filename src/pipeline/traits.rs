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

//! Data source, transformation, and sink traits for pipeline

use super::record::{Record, RecordBatch};
use crate::error::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Offset position for resuming reads
///
/// Different sources track progress differently:
/// - ES: document count or scroll ID
/// - MySQL: primary key value or binlog position
/// - Files: byte offset
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum OffsetValue {
    /// Document count (for ES Scroll API)
    DocumentCount(u64),

    /// Primary key value (for MySQL, PostgreSQL)
    PrimaryKey(serde_json::Value),

    /// Byte offset in file
    ByteOffset(u64),

    /// MySQL Binlog position
    BinlogPosition { file: String, position: u64 },

    /// MongoDB resume token
    ResumeToken(String),

    /// Custom offset (driver-specific)
    Custom(serde_json::Value),
}

impl OffsetValue {
    /// Create initial offset (start from beginning)
    pub fn initial() -> Self {
        Self::DocumentCount(0)
    }

    /// Check if this is the initial offset
    pub fn is_initial(&self) -> bool {
        matches!(self, Self::DocumentCount(0) | Self::ByteOffset(0))
    }
}

/// Data source trait
///
/// Implementations read data from various sources (ES, MySQL, files, etc.)
/// and convert them into universal Record format.
#[async_trait]
pub trait Source: Send + Sync {
    /// Initialize connection to the data source
    async fn connect(&mut self) -> Result<()>;

    /// Read a batch of records starting from the given offset
    ///
    /// Returns the batch and the new offset position.
    /// Empty batch indicates end of data for finite sources. Continuous sources
    /// may return an empty batch when no new events are currently available.
    async fn read_batch(
        &mut self,
        offset: &OffsetValue,
        batch_size: usize,
    ) -> Result<(RecordBatch, OffsetValue)>;

    /// Whether this source remains active after its current backlog is drained.
    /// Continuous sources (for example, CDC feeds) are polled until execution is stopped.
    fn is_continuous(&self) -> bool {
        false
    }

    /// Estimate total number of records (for progress calculation)
    ///
    /// Returns None if count is unknown or expensive to calculate.
    async fn estimate_count(&self) -> Result<Option<u64>>;

    /// Close the connection
    async fn close(&mut self) -> Result<()>;

    /// Get source type identifier
    fn source_type(&self) -> &str;
}

/// Data sink trait
///
/// Implementations write records to various destinations (ES, MySQL, files, etc.)
#[async_trait]
pub trait Sink: Send + Sync {
    /// Initialize connection to the data sink
    async fn connect(&mut self) -> Result<()>;

    /// Write a batch of records
    ///
    /// Returns statistics about the write operation.
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<WriteResult>;

    /// Flush any buffered data
    async fn flush(&mut self) -> Result<()>;

    /// Close the connection
    async fn close(&mut self) -> Result<()>;

    /// Get sink type identifier
    fn sink_type(&self) -> &str;
}

/// Result of a write operation
#[derive(Debug, Clone, Default)]
pub struct WriteResult {
    /// Permanent per-document failures that have enough detail for durable DLQ storage.
    pub failed_records: Vec<FailedRecord>,

    /// Number of successfully written records
    pub success_count: usize,

    /// Number of failed records
    pub failed_count: usize,

    /// Number of transient cluster rejections still failing after retries.
    pub rejected_count: usize,

    /// Number of documents re-submitted after a transient rejection.
    pub retried_count: usize,

    /// Total bytes written
    pub bytes_written: u64,

    /// Errors encountered (if any)
    pub errors: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FailedRecord {
    pub record: super::record::Record,
    pub status: u16,
    pub error_type: String,
    pub reason: String,
}

impl WriteResult {
    /// Create successful write result
    pub fn success(count: usize, bytes: u64) -> Self {
        Self {
            failed_records: Vec::new(),
            success_count: count,
            failed_count: 0,
            rejected_count: 0,
            retried_count: 0,
            bytes_written: bytes,
            errors: Vec::new(),
        }
    }

    /// Check if write was completely successful
    pub fn is_success(&self) -> bool {
        self.failed_count == 0 && self.errors.is_empty()
    }

    /// Add error
    pub fn add_error(&mut self, error: String) {
        self.errors.push(error);
    }
}

/// Data transformation trait
///
/// Transformations can:
/// - Modify record fields (rename, cast types)
/// - Filter records (return empty vec to drop)
/// - Split records (return multiple records)
/// - Enrich records (add computed fields)
pub trait Transform: Send + Sync {
    /// Transform a single record
    ///
    /// Returns:
    /// - Vec with one record: 1:1 transformation
    /// - Empty vec: record filtered out
    /// - Vec with multiple records: 1:N split
    fn transform(&self, record: Record) -> Result<Vec<Record>>;

    /// Get transform type identifier
    fn transform_type(&self) -> &str;
}

/// Simple pass-through transform (no-op)
pub struct NoOpTransform;

impl Transform for NoOpTransform {
    fn transform(&self, record: Record) -> Result<Vec<Record>> {
        Ok(vec![record])
    }

    fn transform_type(&self) -> &str {
        "noop"
    }
}

/// Field renaming transform
pub struct FieldRenameTransform {
    mappings: Vec<(String, String)>, // (from, to) pairs
}

impl FieldRenameTransform {
    pub fn new(mappings: Vec<(String, String)>) -> Self {
        Self { mappings }
    }
}

impl Transform for FieldRenameTransform {
    fn transform(&self, mut record: Record) -> Result<Vec<Record>> {
        if let serde_json::Value::Object(ref mut map) = record.payload {
            for (from, to) in &self.mappings {
                if let Some(value) = map.remove(from) {
                    map.insert(to.clone(), value);
                }
            }
        }
        Ok(vec![record])
    }

    fn transform_type(&self) -> &str {
        "field_rename"
    }
}

/// Filter transform (drop records not matching predicate)
pub struct FilterTransform<F>
where
    F: Fn(&Record) -> bool + Send + Sync,
{
    predicate: F,
}

impl<F> FilterTransform<F>
where
    F: Fn(&Record) -> bool + Send + Sync,
{
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

impl<F> Transform for FilterTransform<F>
where
    F: Fn(&Record) -> bool + Send + Sync,
{
    fn transform(&self, record: Record) -> Result<Vec<Record>> {
        if (self.predicate)(&record) {
            Ok(vec![record])
        } else {
            Ok(vec![]) // Filter out
        }
    }

    fn transform_type(&self) -> &str {
        "filter"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_offset_value() {
        let offset = OffsetValue::DocumentCount(100);
        assert!(!offset.is_initial());

        let initial = OffsetValue::initial();
        assert!(initial.is_initial());
    }

    #[test]
    fn test_noop_transform() {
        let transform = NoOpTransform;
        let record = Record::new("test", json!({"key": "value"}));

        let result = transform.transform(record.clone()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], record);
    }

    #[test]
    fn test_field_rename_transform() {
        let transform = FieldRenameTransform::new(vec![
            ("old_name".to_string(), "new_name".to_string()),
            ("foo".to_string(), "bar".to_string()),
        ]);

        let record = Record::new(
            "test",
            json!({
                "old_name": "value1",
                "foo": "value2",
                "unchanged": "value3"
            }),
        );

        let result = transform.transform(record).unwrap();
        assert_eq!(result.len(), 1);

        let transformed = &result[0];
        assert_eq!(transformed.payload["new_name"], "value1");
        assert_eq!(transformed.payload["bar"], "value2");
        assert_eq!(transformed.payload["unchanged"], "value3");
        assert!(transformed.payload.get("old_name").is_none());
    }

    #[test]
    fn test_filter_transform() {
        let transform = FilterTransform::new(|record: &Record| {
            record
                .payload
                .get("keep")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        });

        let keep_record = Record::new("test", json!({"keep": true}));
        let drop_record = Record::new("test", json!({"keep": false}));

        let result1 = transform.transform(keep_record).unwrap();
        assert_eq!(result1.len(), 1);

        let result2 = transform.transform(drop_record).unwrap();
        assert_eq!(result2.len(), 0);
    }

    #[test]
    fn test_write_result() {
        let mut result = WriteResult::success(100, 1024);
        assert!(result.is_success());
        assert_eq!(result.success_count, 100);

        result.add_error("test error".to_string());
        assert!(!result.is_success());
    }
}
