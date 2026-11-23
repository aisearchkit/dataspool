// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Dataspool Contributors

//! Task metadata definitions

use crate::pipeline::OffsetValue;
use crate::types::{ShardId, ShardState, TaskId, TaskState, TaskStats};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// Task priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum TaskPriority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Per-node contribution to a task's globally aggregated execution state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeTaskAggregate {
    pub pending_shards: u32,
    pub running_shards: u32,
    pub completed_shards: u32,
    pub failed_shards: u32,
    pub docs_processed: u64,
    pub bytes_processed: u64,
}

/// Last committed checkpoint for one shard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardCheckpoint {
    pub shard_id: ShardId,
    pub node_id: Option<String>,
    pub fencing_token: u64,
    pub offset: OffsetValue,
    pub records_processed: u64,
    pub bytes_processed: u64,
    pub updated_at_ms: u64,
}

/// Rebuildable, cluster-wide task view derived exclusively from committed shard metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TaskAggregate {
    pub total_shards: u32,
    pub pending_shards: u32,
    pub running_shards: u32,
    pub completed_shards: u32,
    pub failed_shards: u32,
    pub docs_processed: u64,
    pub bytes_processed: u64,
    pub elapsed_ms: u64,
    pub throughput_docs_per_sec: f64,
    pub throughput_bytes_per_sec: f64,
    pub nodes: HashMap<String, NodeTaskAggregate>,
    pub checkpoints: Vec<ShardCheckpoint>,
}

/// Task metadata containing all information about a data migration task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMeta {
    /// Unique task identifier
    pub task_id: TaskId,

    /// Human-readable task name
    pub name: String,

    /// Task description (optional)
    pub description: Option<String>,

    /// Current task state (includes timestamps and stats)
    pub state: TaskState,

    /// Task priority
    pub priority: TaskPriority,

    /// Source configuration (JSON string)
    pub source_config: String,

    /// Sink configuration (JSON string)
    pub sink_config: String,

    /// Transform configurations (optional, JSON array string)
    pub transform_configs: Option<String>,

    /// Sharding strategy and parameters (optional)
    pub sharding_config: Option<String>,

    /// Total number of shards
    pub shard_count: u32,

    /// Number of completed shards
    pub completed_shards: u32,

    /// Number of failed shards
    pub failed_shards: u32,

    /// Rebuildable cluster-wide view derived from committed shard records.
    #[serde(default)]
    pub aggregate: TaskAggregate,

    /// Additional metadata (key-value pairs)
    pub metadata: HashMap<String, String>,

    /// Task creation timestamp
    pub created_at: SystemTime,

    /// Task execution start time, retained after completion for stable duration metrics.
    #[serde(default)]
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,

    /// Task last update timestamp
    pub updated_at: SystemTime,
}

impl TaskMeta {
    /// Create a new task metadata
    pub fn new(name: String, source_config: String, sink_config: String) -> Self {
        let now = SystemTime::now();
        Self {
            task_id: TaskId::new(),
            name,
            description: None,
            state: TaskState::Pending,
            priority: TaskPriority::default(),
            source_config,
            sink_config,
            transform_configs: None,
            sharding_config: None,
            shard_count: 0,
            completed_shards: 0,
            failed_shards: 0,
            aggregate: TaskAggregate::default(),
            metadata: HashMap::new(),
            created_at: now,
            started_at: None,
            updated_at: now,
        }
    }

    /// Update task state
    pub fn set_state(&mut self, new_state: TaskState) {
        self.state = new_state;
        self.updated_at = SystemTime::now();
    }

    /// Mark a shard as completed
    pub fn mark_shard_completed(&mut self) {
        self.completed_shards += 1;
        self.updated_at = SystemTime::now();

        // Check if all shards are done
        if self.completed_shards == self.shard_count {
            let stats = TaskStats {
                docs_processed: 0, // Would be aggregated from shards
                bytes_processed: 0,
                duration_ms: 0,
            };
            self.state = TaskState::Completed {
                finished_at: chrono::Utc::now(),
                stats,
            };
        }
    }

    /// Mark a shard as failed
    pub fn mark_shard_failed(&mut self) {
        self.failed_shards += 1;
        self.updated_at = SystemTime::now();
    }

    /// Set error and mark task as failed
    pub fn mark_failed(&mut self, error: String) {
        self.state = TaskState::Failed {
            error,
            failed_at: chrono::Utc::now(),
        };
        self.updated_at = SystemTime::now();
    }

    /// Start the task
    pub fn start(&mut self, shard_count: u32) {
        self.shard_count = shard_count;
        let started_at = chrono::Utc::now();
        self.started_at = Some(started_at);
        self.state = TaskState::Running {
            started_at,
            shard_count,
        };
        self.updated_at = SystemTime::now();
    }

    /// Rebuild task counters, terminal state, throughput and checkpoints from shard truth.
    pub fn refresh_from_shards(
        &mut self,
        shards: &[crate::task::ShardMeta],
        at: chrono::DateTime<chrono::Utc>,
    ) {
        let mut aggregate = TaskAggregate {
            total_shards: shards.len() as u32,
            ..TaskAggregate::default()
        };
        let mut earliest_start: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut latest_finish: Option<chrono::DateTime<chrono::Utc>> = None;

        for shard in shards {
            aggregate.docs_processed = aggregate
                .docs_processed
                .saturating_add(shard.records_processed);
            aggregate.bytes_processed = aggregate
                .bytes_processed
                .saturating_add(shard.bytes_processed);
            let state = match &shard.state {
                ShardState::Pending => {
                    aggregate.pending_shards += 1;
                    0_u8
                }
                ShardState::Running { started_at, .. } => {
                    aggregate.running_shards += 1;
                    earliest_start =
                        Some(earliest_start.map_or(*started_at, |v| v.min(*started_at)));
                    1_u8
                }
                ShardState::Completed { finished_at, .. } => {
                    aggregate.completed_shards += 1;
                    latest_finish =
                        Some(latest_finish.map_or(*finished_at, |v| v.max(*finished_at)));
                    2_u8
                }
                ShardState::Failed { .. } => {
                    aggregate.failed_shards += 1;
                    let failed_at = chrono::DateTime::<chrono::Utc>::from(shard.updated_at);
                    latest_finish = Some(latest_finish.map_or(failed_at, |v| v.max(failed_at)));
                    3_u8
                }
            };
            let owner = shard.last_owner_node_id.as_ref().map(ToString::to_string);
            if let Some(owner) = owner.as_ref() {
                let node = aggregate.nodes.entry(owner.clone()).or_default();
                node.pending_shards += u32::from(state == 0);
                node.running_shards += u32::from(state == 1);
                node.completed_shards += u32::from(state == 2);
                node.failed_shards += u32::from(state == 3);
                node.docs_processed = node.docs_processed.saturating_add(shard.records_processed);
                node.bytes_processed = node.bytes_processed.saturating_add(shard.bytes_processed);
            }
            aggregate.checkpoints.push(ShardCheckpoint {
                shard_id: shard.shard_id,
                node_id: owner,
                fencing_token: shard.fencing_token,
                offset: shard.offset.clone(),
                records_processed: shard.records_processed,
                bytes_processed: shard.bytes_processed,
                updated_at_ms: shard
                    .updated_at
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
            });
        }

        aggregate
            .checkpoints
            .sort_by_key(|checkpoint| checkpoint.shard_id.to_string());
        self.shard_count = aggregate.total_shards;
        self.completed_shards = aggregate.completed_shards;
        self.failed_shards = aggregate.failed_shards;

        let started_at = self.started_at.or_else(|| match &self.state {
            TaskState::Running { started_at, .. } => Some(*started_at),
            _ => earliest_start,
        });
        if self.started_at.is_none() {
            self.started_at = started_at;
        }
        let terminal_count = aggregate.completed_shards + aggregate.failed_shards;
        if aggregate.total_shards > 0 && terminal_count == aggregate.total_shards {
            let finished_at = latest_finish.unwrap_or(at);
            let duration_ms = started_at
                .map(|start| {
                    finished_at
                        .signed_duration_since(start)
                        .num_milliseconds()
                        .max(0) as u64
                })
                .unwrap_or_default();
            aggregate.elapsed_ms = duration_ms;
            if aggregate.failed_shards > 0 {
                self.state = TaskState::Failed {
                    error: format!(
                        "{} of {} shards failed",
                        aggregate.failed_shards, aggregate.total_shards
                    ),
                    failed_at: finished_at,
                };
            } else {
                self.state = TaskState::Completed {
                    finished_at,
                    stats: TaskStats {
                        docs_processed: aggregate.docs_processed,
                        bytes_processed: aggregate.bytes_processed,
                        duration_ms,
                    },
                };
            }
        } else if !matches!(
            self.state,
            TaskState::Pausing | TaskState::Paused | TaskState::Canceling
        ) {
            if aggregate.running_shards > 0
                || aggregate.completed_shards > 0
                || aggregate.failed_shards > 0
            {
                let started_at = started_at.unwrap_or_else(|| self.created_at.into());
                self.state = TaskState::Running {
                    started_at,
                    shard_count: aggregate.total_shards,
                };
            } else {
                self.state = TaskState::Pending;
            }
        }
        if aggregate.total_shards > 0 {
            let elapsed = if self.is_terminal() {
                match &self.state {
                    TaskState::Completed { stats, .. } => stats.duration_ms,
                    TaskState::Failed { failed_at, .. } => started_at
                        .map(|start| {
                            failed_at
                                .signed_duration_since(start)
                                .num_milliseconds()
                                .max(0) as u64
                        })
                        .unwrap_or_default(),
                    _ => 0,
                }
            } else {
                let start = started_at.unwrap_or_else(|| self.created_at.into());
                at.signed_duration_since(start).num_milliseconds().max(0) as u64
            };
            aggregate.elapsed_ms = elapsed;
            aggregate.throughput_docs_per_sec = if elapsed > 0 {
                aggregate.docs_processed as f64 * 1_000.0 / elapsed as f64
            } else {
                0.0
            };
            aggregate.throughput_bytes_per_sec = if elapsed > 0 {
                aggregate.bytes_processed as f64 * 1_000.0 / elapsed as f64
            } else {
                0.0
            };
        }
        self.aggregate = aggregate;
        self.updated_at = std::time::UNIX_EPOCH
            + std::time::Duration::from_millis(at.timestamp_millis().max(0) as u64);
    }

    /// Calculate task progress (0.0 to 1.0)
    pub fn progress(&self) -> f64 {
        if self.shard_count == 0 {
            return 0.0;
        }
        self.completed_shards as f64 / self.shard_count as f64
    }

    /// Check if task is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            TaskState::Completed { .. } | TaskState::Failed { .. }
        )
    }

    /// Check if task is running
    pub fn is_running(&self) -> bool {
        matches!(self.state, TaskState::Running { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_meta_new() {
        let task = TaskMeta::new("test-task".to_string(), "{}".to_string(), "{}".to_string());

        assert_eq!(task.name, "test-task");
        assert_eq!(task.state, TaskState::Pending);
        assert_eq!(task.priority, TaskPriority::Normal);
        assert_eq!(task.shard_count, 0);
        assert_eq!(task.completed_shards, 0);
        assert!(!task.is_terminal());
    }

    #[test]
    fn test_task_start() {
        let mut task = TaskMeta::new("test".to_string(), "{}".to_string(), "{}".to_string());

        task.start(10);
        assert!(task.is_running());
        assert_eq!(task.shard_count, 10);
    }

    #[test]
    fn test_shard_tracking() {
        let mut task = TaskMeta::new("test".to_string(), "{}".to_string(), "{}".to_string());
        task.start(10);

        assert_eq!(task.progress(), 0.0);

        for _ in 0..5 {
            task.mark_shard_completed();
        }
        assert_eq!(task.completed_shards, 5);
        assert_eq!(task.progress(), 0.5);

        task.mark_shard_failed();
        assert_eq!(task.failed_shards, 1);

        // Complete remaining shards
        for _ in 0..4 {
            task.mark_shard_completed();
        }
        assert_eq!(task.completed_shards, 9);

        task.mark_shard_completed();
        assert!(task.is_terminal());
        assert_eq!(task.progress(), 1.0);
    }

    #[test]
    fn test_task_failure() {
        let mut task = TaskMeta::new("test".to_string(), "{}".to_string(), "{}".to_string());

        task.mark_failed("connection error".to_string());
        assert!(task.is_terminal());

        match &task.state {
            TaskState::Failed { error, .. } => {
                assert_eq!(error, "connection error");
            }
            _ => panic!("Expected Failed state"),
        }
    }

    #[test]
    fn test_priority_ordering() {
        assert!(TaskPriority::Critical > TaskPriority::High);
        assert!(TaskPriority::High > TaskPriority::Normal);
        assert!(TaskPriority::Normal > TaskPriority::Low);
    }
}
