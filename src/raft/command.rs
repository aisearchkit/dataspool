// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Raft command definitions - all cluster state mutations

use crate::task::{ShardMeta, TaskMeta};
use serde::{Deserialize, Serialize};

/// Commands that can be proposed to the Raft log
///
/// All state changes go through Raft consensus. Only the Leader
/// can propose commands. Followers will forward writes to the Leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    // ==================== Task Management ====================
    /// Create a new task
    CreateTask {
        task: Box<TaskMeta>,
        shards: Vec<ShardMeta>,
    },

    /// Update task state
    UpdateTaskState {
        task_id: String,
        state: String, // JSON-serialized TaskState
    },

    /// Delete a task (soft delete - mark as archived)
    DeleteTask { task_id: String },

    // ==================== Shard Management ====================
    /// Claim a shard (CAS operation)
    ClaimShard {
        shard_id: String,
        node_id: String,
        expected_state: String, // Must be "Pending"
    },

    /// Update shard progress
    UpdateShardProgress {
        shard_id: String,
        processed_records: u64,
        offset: String, // JSON-serialized OffsetValue
    },

    /// Mark shard as completed
    CompleteShard {
        shard_id: String,
        total_records: u64,
    },

    /// Mark shard as failed
    FailShard {
        shard_id: String,
        error_message: String,
    },

    // ==================== Node Management ====================
    /// Register a new node
    RegisterNode {
        node_id: String,
        address: String,
        tags: std::collections::HashMap<String, String>,
    },

    /// Update node heartbeat
    UpdateNodeHeartbeat {
        node_id: String,
        timestamp: i64, // Unix timestamp
    },

    /// Mark node as offline
    MarkNodeOffline { node_id: String },

    // ==================== Configuration ====================
    /// Update cluster configuration
    UpdateConfig {
        key: String,
        value: String, // JSON-serialized value
    },
}

impl Command {
    /// Get a human-readable description of the command
    pub fn description(&self) -> String {
        match self {
            Command::CreateTask { task, .. } => {
                format!("CreateTask({})", task.task_id)
            }
            Command::UpdateTaskState { task_id, .. } => {
                format!("UpdateTaskState({})", task_id)
            }
            Command::DeleteTask { task_id } => {
                format!("DeleteTask({})", task_id)
            }
            Command::ClaimShard {
                shard_id, node_id, ..
            } => {
                format!("ClaimShard({}, {})", shard_id, node_id)
            }
            Command::UpdateShardProgress { shard_id, .. } => {
                format!("UpdateShardProgress({})", shard_id)
            }
            Command::CompleteShard { shard_id, .. } => {
                format!("CompleteShard({})", shard_id)
            }
            Command::FailShard { shard_id, .. } => {
                format!("FailShard({})", shard_id)
            }
            Command::RegisterNode { node_id, .. } => {
                format!("RegisterNode({})", node_id)
            }
            Command::UpdateNodeHeartbeat { node_id, .. } => {
                format!("UpdateNodeHeartbeat({})", node_id)
            }
            Command::MarkNodeOffline { node_id } => {
                format!("MarkNodeOffline({})", node_id)
            }
            Command::UpdateConfig { key, .. } => {
                format!("UpdateConfig({})", key)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_serialization() {
        let cmd = Command::ClaimShard {
            shard_id: "shard-1".to_string(),
            node_id: "node-1".to_string(),
            expected_state: "Pending".to_string(),
        };

        let json = serde_json::to_string(&cmd).unwrap();
        let decoded: Command = serde_json::from_str(&json).unwrap();

        match decoded {
            Command::ClaimShard {
                shard_id, node_id, ..
            } => {
                assert_eq!(shard_id, "shard-1");
                assert_eq!(node_id, "node-1");
            }
            _ => panic!("Wrong command type"),
        }
    }

    #[test]
    fn test_command_description() {
        let cmd = Command::CreateTask {
            task: Box::new(TaskMeta::new(
                "Test Task".to_string(),
                "{}".to_string(), // source_config
                "{}".to_string(), // sink_config
            )),
            shards: vec![],
        };

        // task_id is generated, so we check the prefix
        assert!(cmd.description().starts_with("CreateTask("));
    }
}
