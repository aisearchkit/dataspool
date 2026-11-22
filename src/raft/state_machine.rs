// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Raft state machine - applies commands to storage

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::Command;
use crate::error::{DatspoolError, Result};
use crate::storage::KvStore;
use crate::task::TaskStore;

/// Response from applying a command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
    pub success: bool,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

/// Raft state machine that applies commands to RocksDB
pub struct MetaStateMachine<S: KvStore> {
    task_store: TaskStore<S>,
    #[allow(dead_code)]
    last_applied_log: u64,
}

impl<S: KvStore + Clone> MetaStateMachine<S> {
    pub fn new(store: S) -> Self {
        Self {
            task_store: TaskStore::new(store),
            last_applied_log: 0,
        }
    }

    /// Apply a single command to the state machine
    pub async fn apply_command(&mut self, cmd: Command) -> Result<CommandResponse> {
        info!("Applying command: {}", cmd.description());

        match cmd {
            Command::CreateTask { task, shards } => self.apply_create_task(task, shards).await,
            Command::ClaimShard {
                shard_id,
                node_id,
                expected_state,
            } => {
                self.apply_claim_shard(shard_id, node_id, expected_state)
                    .await
            }
            // TODO: Implement other commands
            _ => {
                warn!("Command not implemented yet: {}", cmd.description());
                Ok(CommandResponse {
                    success: false,
                    message: "Not implemented yet".to_string(),
                    data: None,
                })
            }
        }
    }

    async fn apply_create_task(
        &mut self,
        task: Box<crate::task::TaskMeta>,
        shards: Vec<crate::task::ShardMeta>,
    ) -> Result<CommandResponse> {
        // This would use TaskStore to persist task and shards
        // For now, just a stub
        Ok(CommandResponse {
            success: true,
            message: format!("Task {} created with {} shards", task.task_id, shards.len()),
            data: Some(serde_json::json!({ "task_id": task.task_id })),
        })
    }

    async fn apply_claim_shard(
        &mut self,
        shard_id: String,
        node_id: String,
        expected_state: String,
    ) -> Result<CommandResponse> {
        use crate::types::{NodeId, ShardId, ShardState};

        // Parse expected state
        let expected_state: ShardState = serde_json::from_str(&expected_state)
            .map_err(|e| DatspoolError::Serialization(e.to_string()))?;

        // Parse shard ID
        let shard_id: ShardId = shard_id
            .parse()
            .map_err(|e: uuid::Error| DatspoolError::Serialization(e.to_string()))?;

        // Get current shard
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(s) => s,
            None => {
                return Ok(CommandResponse {
                    success: false,
                    message: format!("Shard {} not found", shard_id),
                    data: None,
                });
            }
        };

        // Check if expected state matches (CAS semantics)
        if shard.state != expected_state {
            return Ok(CommandResponse {
                success: false,
                message: format!(
                    "Shard {} state is {:?}, expected {:?}",
                    shard_id, shard.state, expected_state
                ),
                data: None,
            });
        }

        // Transition to Running using built-in method
        shard.start(NodeId(node_id.clone()));

        // Persist change
        self.task_store.save_shard(&shard).await?;

        info!("Shard {} claimed by {} (CAS succeeded)", shard_id, node_id);

        Ok(CommandResponse {
            success: true,
            message: format!("Shard {} claimed by {}", shard_id, node_id),
            data: Some(serde_json::json!({
                "shard_id": shard_id.to_string(),
                "node_id": node_id,
                "state": "Running"
            })),
        })
    }
}

// NOTE: Full openraft::RaftStateMachine implementation would go here
// This is a placeholder showing the structure
