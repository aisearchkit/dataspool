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

//! Universal record structure for data pipeline

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single record flowing through the pipeline
///
/// Records are the universal data structure that flows from Source -> Transform -> Sink.
/// They encapsulate data from any source (ES, MySQL, files, etc.) in a common format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Record {
    /// Optional document ID (for ES, MongoDB, etc.)
    pub id: Option<String>,

    /// Source type identifier (e.g., "elasticsearch", "mysql", "file")
    pub source_type: String,

    /// Main payload as JSON value
    pub payload: serde_json::Value,

    /// Additional metadata fields
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl Record {
    /// Create a new record with given source type and payload
    pub fn new(source_type: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            id: None,
            source_type: source_type.into(),
            payload,
            metadata: HashMap::new(),
        }
    }

    /// Create a new record with ID
    pub fn with_id(
        id: impl Into<String>,
        source_type: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: Some(id.into()),
            source_type: source_type.into(),
            payload,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata field
    pub fn add_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into());
    }

    /// Get metadata field
    pub fn get_metadata(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).map(|s| s.as_str())
    }

    /// Set payload
    pub fn set_payload(&mut self, payload: serde_json::Value) {
        self.payload = payload;
    }

    /// Get payload as typed value
    pub fn get_payload_as<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.payload.clone())
    }

    /// Convert to bytes (JSON serialization)
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Parse from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Batch of records for efficient processing
#[derive(Debug, Default)]
pub struct RecordBatch {
    records: Vec<Record>,
}

impl RecordBatch {
    /// Create new empty batch
    pub fn new() -> Self {
        Self::default()
    }

    /// Create batch with capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            records: Vec::with_capacity(capacity),
        }
    }

    /// Add record to batch
    pub fn push(&mut self, record: Record) {
        self.records.push(record);
    }

    /// Get number of records in batch
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get records as slice
    pub fn records(&self) -> &[Record] {
        &self.records
    }

    /// Consume batch and return records
    pub fn into_records(self) -> Vec<Record> {
        self.records
    }

    /// Clear all records
    pub fn clear(&mut self) {
        self.records.clear();
    }
}

impl From<Vec<Record>> for RecordBatch {
    fn from(records: Vec<Record>) -> Self {
        Self { records }
    }
}

impl IntoIterator for RecordBatch {
    type Item = Record;
    type IntoIter = std::vec::IntoIter<Record>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_record_creation() {
        let payload = json!({"field": "value", "number": 42});
        let record = Record::new("test", payload.clone());

        assert_eq!(record.source_type, "test");
        assert_eq!(record.payload, payload);
        assert!(record.id.is_none());
    }

    #[test]
    fn test_record_with_id() {
        let record = Record::with_id("doc_123", "elasticsearch", json!({"data": "test"}));

        assert_eq!(record.id.unwrap(), "doc_123");
        assert_eq!(record.source_type, "elasticsearch");
    }

    #[test]
    fn test_record_metadata() {
        let mut record = Record::new("test", json!({}));

        record.add_metadata("index", "logs-2024");
        record.add_metadata("_routing", "user-123");

        assert_eq!(record.get_metadata("index"), Some("logs-2024"));
        assert_eq!(record.get_metadata("_routing"), Some("user-123"));
        assert_eq!(record.get_metadata("nonexistent"), None);
    }

    #[test]
    fn test_record_serialization() {
        let record = Record::with_id("123", "test", json!({"key": "value"}));

        let bytes = record.to_bytes().unwrap();
        let deserialized = Record::from_bytes(&bytes).unwrap();

        assert_eq!(record, deserialized);
    }

    #[test]
    fn test_record_batch() {
        let mut batch = RecordBatch::with_capacity(3);

        assert!(batch.is_empty());

        batch.push(Record::new("test", json!({"a": 1})));
        batch.push(Record::new("test", json!({"b": 2})));

        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());

        let records = batch.into_records();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_get_payload_as() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct TestData {
            field: String,
            count: u32,
        }

        let record = Record::new(
            "test",
            json!({
                "field": "value",
                "count": 10
            }),
        );

        let data: TestData = record.get_payload_as().unwrap();
        assert_eq!(data.field, "value");
        assert_eq!(data.count, 10);
    }
}
