// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Driver factory for creating Source and Sink instances from configuration

use serde::{Deserialize, Serialize};

use crate::error::{DatspoolError, Result};
use crate::pipeline::{Sink, Source};

/// Source driver type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    /// Document store (Elasticsearch/OpenSearch)
    Document {
        #[serde(flatten)]
        config: super::document::DocumentConfig,
    },
    // Future drivers:
    // Mysql { config: MysqlConfig },
    // MongoDB { config: MongoConfig },
    // Snapshot { config: SnapshotConfig },
}

/// Sink driver type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkConfig {
    /// Document store (Elasticsearch/OpenSearch)
    Document {
        #[serde(flatten)]
        config: super::document::DocumentConfig,
    },
    // Future drivers:
    // Mysql { config: MysqlConfig },
    // MongoDB { config: MongoConfig },
}

/// Create a Source instance from configuration
pub fn create_source(config: &SourceConfig) -> Result<Box<dyn Source>> {
    match config {
        SourceConfig::Document { config } => {
            let source = super::document::DocumentSource::new(config.clone());
            Ok(Box::new(source))
        }
        // Future implementations:
        // SourceConfig::Mysql { config } => Ok(Box::new(MysqlSource::new(config.clone()))),
        // SourceConfig::MongoDB { config } => Ok(Box::new(MongoSource::new(config.clone()))),
    }
}

/// Create a Sink instance from configuration
pub fn create_sink(config: &SinkConfig) -> Result<Box<dyn Sink>> {
    match config {
        SinkConfig::Document { config } => {
            // Use the index from config as target_index
            let sink = super::document::DocumentSink::new(config.clone(), config.index.clone());
            Ok(Box::new(sink))
        }
        // Future implementations:
        // SinkConfig::Mysql { config } => Ok(Box::new(MysqlSink::new(config.clone()))),
        // SinkConfig::MongoDB { config } => Ok(Box::new(MongoSink::new(config.clone()))),
    }
}

/// Complete task configuration including source and sink
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    pub source: SourceConfig,
    pub sink: SinkConfig,
    
    /// Optional transformations to apply
    #[serde(default)]
    pub transforms: Vec<TransformConfig>,
}

/// Transform configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransformConfig {
    /// Rename fields
    FieldRename {
        mappings: std::collections::HashMap<String, String>,
    },
    
    /// Filter records
    Filter {
        condition: String, // JSON path expression
    },
    
    /// No-op (pass-through)
    NoOp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_source_config() {
        let config = SourceConfig::Document {
            config: crate::drivers::document::DocumentConfig::local("test-index"),
        };
        
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains(r#""type":"document"#));
        assert!(json.contains("test-index"));
    }

    #[test]
    fn test_deserialize_source_config() {
        let json = r#"{
            "type": "document",
            "urls": ["http://localhost:9200"],
            "index": "test-index"
        }"#;
        
        let config: SourceConfig = serde_json::from_str(json).unwrap();
        match config {
            SourceConfig::Document { config } => {
                assert_eq!(config.urls, vec!["http://localhost:9200"]);
                assert_eq!(config.index, "test-index");
            }
        }
    }

    #[test]
    fn test_create_source() {
        let config = SourceConfig::Document {
            config: crate::drivers::document::DocumentConfig::local("test-index"),
        };
        
        let source = create_source(&config).unwrap();
        assert_eq!(source.source_type(), "document");
    }

    #[test]
    fn test_task_config_serialization() {
        let task_config = TaskConfig {
            source: SourceConfig::Document {
                config: crate::drivers::document::DocumentConfig::local("source-index"),
            },
            sink: SinkConfig::Document {
                config: crate::drivers::document::DocumentConfig::local("dest-index"),
            },
            transforms: vec![],
        };
        
        let json = serde_json::to_string_pretty(&task_config).unwrap();
        println!("Task config JSON:\n{}", json);
        
        // Verify we can deserialize it back
        let _parsed: TaskConfig = serde_json::from_str(&json).unwrap();
    }
}
