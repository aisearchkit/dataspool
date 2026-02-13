// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Dataspool Contributors

//! Shard metadata and generation logic

use crate::pipeline::OffsetValue;
use crate::types::{NodeId, ShardId, ShardState, TaskId};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Sharding strategies for different data sources
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShardingStrategy {
    /// Single shard (no sharding)
    None,

    /// Shard by count (number of documents per shard)
    ByCount { shard_size: u64 },

    /// Shard by time range (for time-series data)
    ByTimeRange {
        field: String,
        interval: String, // e.g., "1d", "1h"
    },

    /// Shard by ID range (for sequential IDs)
    ByIdRange {
        field: String,
        ranges: Vec<(String, String)>, // start, end pairs
    },

    /// Shard by index pattern (for Elasticsearch)
    ByIndexPattern { pattern: String },

    /// Shard by hash (consistent hashing)
    ByHash { field: String, num_shards: u32 },
}

/// Shard metadata representing a unit of work
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMeta {
    /// Unique shard identifier
    pub shard_id: ShardId,

    /// Parent task identifier
    pub task_id: TaskId,

    /// Shard index within the task
    pub shard_index: u32,

    /// Current shard state (includes node assignment and timestamps)
    pub state: ShardState,

    /// Shard-specific configuration (JSON string)
    pub config: String,

    /// Current offset (for resumable reads)
    pub offset: OffsetValue,

    /// Number of records processed
    pub records_processed: u64,

    /// Number of bytes processed
    pub bytes_processed: u64,

    /// Shard creation timestamp
    pub created_at: SystemTime,

    /// Last update timestamp
    pub updated_at: SystemTime,
}

impl ShardMeta {
    /// Create a new shard metadata
    pub fn new(task_id: TaskId, shard_index: u32, config: String) -> Self {
        let now = SystemTime::now();
        Self {
            shard_id: ShardId::new(),
            task_id,
            shard_index,
            state: ShardState::Pending,
            config,
            offset: OffsetValue::initial(),
            records_processed: 0,
            bytes_processed: 0,
            created_at: now,
            updated_at: now,
        }
    }

    /// Update shard state
    pub fn set_state(&mut self, new_state: ShardState) {
        self.state = new_state;
        self.updated_at = SystemTime::now();
    }

    /// Assign shard to a node and start running
    pub fn start(&mut self, node_id: NodeId) {
        self.state = ShardState::Running {
            node_id,
            started_at: chrono::Utc::now(),
        };
        self.updated_at = SystemTime::now();
    }

    /// Update progress
    pub fn update_progress(&mut self, records: u64, bytes: u64, offset: OffsetValue) {
        self.records_processed += records;
        self.bytes_processed += bytes;
        self.offset = offset;
        self.updated_at = SystemTime::now();
    }

    /// Mark shard as completed
    pub fn complete(&mut self, docs_processed: u64) {
        self.state = ShardState::Completed {
            finished_at: chrono::Utc::now(),
            docs_processed,
        };
        self.updated_at = SystemTime::now();
    }

    /// Mark shard as failed
    pub fn fail(&mut self, error: String, attempts: u32) {
        self.state = ShardState::Failed { error, attempts };
        self.updated_at = SystemTime::now();
    }

    /// Check if shard is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            ShardState::Completed { .. } | ShardState::Failed { .. }
        )
    }

    /// Check if shard is running
    pub fn is_running(&self) -> bool {
        matches!(self.state, ShardState::Running { .. })
    }

    /// Get assigned node ID if running
    pub fn assigned_node(&self) -> Option<&NodeId> {
        match &self.state {
            ShardState::Running { node_id, .. } => Some(node_id),
            _ => None,
        }
    }
}

/// Shard generator for creating shards based on strategy
pub struct ShardGenerator;

impl ShardGenerator {
    /// Generate shards based on the sharding strategy
    pub fn generate(
        task_id: TaskId,
        strategy: &ShardingStrategy,
        source_config: &str,
    ) -> Vec<ShardMeta> {
        match strategy {
            ShardingStrategy::None => {
                vec![ShardMeta::new(task_id, 0, source_config.to_string())]
            }

            ShardingStrategy::ByCount { shard_size: _ } => {
                // This requires knowing total count, simplified for now
                vec![ShardMeta::new(task_id, 0, source_config.to_string())]
            }

            ShardingStrategy::ByIdRange { field, ranges } => {
                ranges
                    .iter()
                    .enumerate()
                    .map(|(i, (start, end))| {
                        let config = serde_json::json!({
                            "base": serde_json::from_str::<serde_json::Value>(source_config).unwrap_or_default(),
                            "shard": {
                                "field": field,
                                "start": start,
                                "end": end,
                            }
                        });
                        ShardMeta::new(task_id, i as u32, config.to_string())
                    })
                    .collect()
            }

            ShardingStrategy::ByIndexPattern { pattern: _ } => {
                // Placeholder - would expand pattern to actual indices
                vec![ShardMeta::new(task_id, 0, source_config.to_string())]
            }

            ShardingStrategy::ByHash { num_shards, .. } => {
                (0..*num_shards)
                    .map(|i| {
                        let config = serde_json::json!({
                            "base": serde_json::from_str::<serde_json::Value>(source_config).unwrap_or_default(),
                            "shard": {
                                "index": i,
                                "total": num_shards,
                            }
                        });
                        ShardMeta::new(task_id, i, config.to_string())
                    })
                    .collect()
            }

            ShardingStrategy::ByTimeRange { .. } => {
                // Placeholder - would generate time-based shards
                vec![ShardMeta::new(task_id, 0, source_config.to_string())]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TaskId;

    #[test]
    fn test_shard_meta_new() {
        let task_id = TaskId::new();
        let shard = ShardMeta::new(task_id, 0, "{}".to_string());

        assert_eq!(shard.task_id, task_id);
        assert_eq!(shard.shard_index, 0);
        assert_eq!(shard.state, ShardState::Pending);
        assert_eq!(shard.records_processed, 0);
        assert!(!shard.is_terminal());
    }

    #[test]
    fn test_shard_start() {
        let task_id = TaskId::new();
        let mut shard = ShardMeta::new(task_id, 0, "{}".to_string());

        let node_id = NodeId::new("node-1");
        shard.start(node_id.clone());
        assert!(shard.is_running());
        assert_eq!(shard.assigned_node(), Some(&node_id));
    }

    #[test]
    fn test_shard_completion() {
        let task_id = TaskId::new();
        let mut shard = ShardMeta::new(task_id, 0, "{}".to_string());

        let node_id = NodeId::new("node-1");
        shard.start(node_id);

        shard.complete(1000);
        assert!(shard.is_terminal());
        assert!(!shard.is_running());

        match &shard.state {
            ShardState::Completed { docs_processed, .. } => {
                assert_eq!(*docs_processed, 1000);
            }
            _ => panic!("Expected Completed state"),
        }
    }

    #[test]
    fn test_shard_failure() {
        let task_id = TaskId::new();
        let mut shard = ShardMeta::new(task_id, 0, "{}".to_string());

        shard.fail("network error".to_string(), 3);
        assert!(shard.is_terminal());

        match &shard.state {
            ShardState::Failed { error, attempts } => {
                assert_eq!(error, "network error");
                assert_eq!(*attempts, 3);
            }
            _ => panic!("Expected Failed state"),
        }
    }

    #[test]
    fn test_shard_progress_tracking() {
        let task_id = TaskId::new();
        let mut shard = ShardMeta::new(task_id, 0, "{}".to_string());

        shard.update_progress(100, 1024, OffsetValue::DocumentCount(100));
        assert_eq!(shard.records_processed, 100);
        assert_eq!(shard.bytes_processed, 1024);

        shard.update_progress(50, 512, OffsetValue::DocumentCount(150));
        assert_eq!(shard.records_processed, 150);
        assert_eq!(shard.bytes_processed, 1536);
    }

    #[test]
    fn test_shard_generator_none() {
        let task_id = TaskId::new();
        let shards = ShardGenerator::generate(task_id, &ShardingStrategy::None, "{}");

        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_index, 0);
    }

    #[test]
    fn test_shard_generator_by_hash() {
        let task_id = TaskId::new();
        let strategy = ShardingStrategy::ByHash {
            field: "id".to_string(),
            num_shards: 5,
        };

        let shards = ShardGenerator::generate(task_id, &strategy, "{}");

        assert_eq!(shards.len(), 5);
        for (i, shard) in shards.iter().enumerate() {
            assert_eq!(shard.shard_index, i as u32);
        }
    }

    #[test]
    fn test_shard_generator_by_id_range() {
        let task_id = TaskId::new();
        let strategy = ShardingStrategy::ByIdRange {
            field: "id".to_string(),
            ranges: vec![
                ("0".to_string(), "1000".to_string()),
                ("1000".to_string(), "2000".to_string()),
                ("2000".to_string(), "3000".to_string()),
            ],
        };

        let shards = ShardGenerator::generate(task_id, &strategy, r#"{"type":"mysql"}"#);

        assert_eq!(shards.len(), 3);
        assert_eq!(shards[0].shard_index, 0);
        assert_eq!(shards[1].shard_index, 1);
        assert_eq!(shards[2].shard_index, 2);
    }
}
