// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Dataspool Contributors

//! Shard metadata and generation logic

use crate::pipeline::OffsetValue;
use crate::types::{NodeId, ShardId, ShardState, TaskId};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Start ES external versions in a high range so existing internally-versioned
/// documents are normally below the first Dataspool lease generation.
pub const FENCING_TOKEN_BASE: u64 = 1_u64 << 62;

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

    /// Monotonically increasing ownership generation. Never reset on requeue.
    #[serde(default)]
    pub fencing_token: u64,

    /// Lease deadline for the current owner. None means the shard is not safely reclaimable.
    #[serde(default)]
    pub lease_expires_at: Option<chrono::DateTime<chrono::Utc>>,

    /// Idempotency key for the current lease acquisition.
    #[serde(default)]
    pub lease_id: Option<String>,

    /// Last node that owned this shard, retained for historical per-node aggregation.
    #[serde(default)]
    pub last_owner_node_id: Option<NodeId>,

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
            fencing_token: 0,
            lease_expires_at: None,
            lease_id: None,
            last_owner_node_id: None,
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
        self.set_state_at(new_state, chrono::Utc::now());
    }

    pub fn set_state_at(&mut self, new_state: ShardState, at: chrono::DateTime<chrono::Utc>) {
        if !matches!(new_state, ShardState::Running { .. }) {
            self.lease_expires_at = None;
            self.lease_id = None;
        }
        self.state = new_state;
        self.updated_at = system_time_from_utc(at);
    }

    /// Start a new ownership generation with a bounded lease.
    pub fn start_with_lease(
        &mut self,
        node_id: NodeId,
        started_at: chrono::DateTime<chrono::Utc>,
        lease_expires_at: chrono::DateTime<chrono::Utc>,
        lease_id: String,
    ) -> Option<u64> {
        self.fencing_token = self.fencing_token.max(FENCING_TOKEN_BASE).checked_add(1)?;
        self.lease_expires_at = Some(lease_expires_at);
        self.lease_id = Some(lease_id);
        self.last_owner_node_id = Some(node_id.clone());
        self.state = ShardState::Running {
            node_id,
            started_at,
        };
        self.updated_at = system_time_from_utc(started_at);
        Some(self.fencing_token)
    }

    /// Extend a lease only for its current owner and fencing generation.
    pub fn renew_lease(
        &mut self,
        node_id: &NodeId,
        fencing_token: u64,
        lease_expires_at: chrono::DateTime<chrono::Utc>,
    ) -> bool {
        self.renew_lease_at(node_id, fencing_token, lease_expires_at, chrono::Utc::now())
    }

    pub fn renew_lease_at(
        &mut self,
        node_id: &NodeId,
        fencing_token: u64,
        lease_expires_at: chrono::DateTime<chrono::Utc>,
        at: chrono::DateTime<chrono::Utc>,
    ) -> bool {
        let owns_shard = matches!(
            &self.state,
            ShardState::Running { node_id: owner, .. } if owner == node_id
        );
        if !owns_shard || self.fencing_token != fencing_token {
            return false;
        }
        if self
            .lease_expires_at
            .as_ref()
            .is_none_or(|current| lease_expires_at <= *current)
        {
            return false;
        }
        self.lease_expires_at = Some(lease_expires_at);
        self.updated_at = system_time_from_utc(at);
        true
    }

    /// Assign shard to a node and start running
    pub fn start(&mut self, node_id: NodeId) {
        self.fencing_token = self.fencing_token.max(FENCING_TOKEN_BASE).saturating_add(1);
        self.lease_expires_at = None;
        self.lease_id = None;
        self.last_owner_node_id = Some(node_id.clone());
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

    /// Set an absolute progress checkpoint. This is idempotent when a committed
    /// Raft entry is replayed after a crash between application data and log metadata.
    pub fn set_progress_at(
        &mut self,
        records: u64,
        bytes: u64,
        offset: OffsetValue,
        at: chrono::DateTime<chrono::Utc>,
    ) {
        self.records_processed = records;
        self.bytes_processed = bytes;
        self.offset = offset;
        self.updated_at = system_time_from_utc(at);
    }

    /// Mark shard as completed
    pub fn complete(&mut self, docs_processed: u64) {
        self.complete_at(docs_processed, chrono::Utc::now());
    }

    pub fn complete_at(&mut self, docs_processed: u64, at: chrono::DateTime<chrono::Utc>) {
        self.lease_expires_at = None;
        self.lease_id = None;
        self.state = ShardState::Completed {
            finished_at: at,
            docs_processed,
        };
        self.updated_at = system_time_from_utc(at);
    }

    /// Mark shard as failed
    pub fn fail(&mut self, error: String, attempts: u32) {
        self.fail_at(error, attempts, chrono::Utc::now());
    }

    pub fn fail_at(&mut self, error: String, attempts: u32, at: chrono::DateTime<chrono::Utc>) {
        self.lease_expires_at = None;
        self.lease_id = None;
        self.state = ShardState::Failed { error, attempts };
        self.updated_at = system_time_from_utc(at);
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

fn system_time_from_utc(value: chrono::DateTime<chrono::Utc>) -> SystemTime {
    SystemTime::UNIX_EPOCH
        + std::time::Duration::from_millis(value.timestamp_millis().max(0) as u64)
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
