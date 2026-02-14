// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Shard claimer - acquires pending shards via Raft CAS

use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::error::{DatspoolError, Result};
use crate::storage::kv::KvStore;
use crate::task::store::TaskStore;
use crate::types::{NodeId, ShardId, ShardState};

/// Shard claimer that uses Raft CAS to acquire pending shards
pub struct ShardClaimer<S: KvStore> {
    node_id: NodeId,
    task_store: Arc<TaskStore<S>>,
    max_attempts: usize,
}

impl<S: KvStore + Clone + 'static> Clone for ShardClaimer<S> {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            task_store: self.task_store.clone(),
            max_attempts: self.max_attempts,
        }
    }
}

impl<S: KvStore + Clone + 'static> ShardClaimer<S> {
    pub fn new(node_id: String, task_store: Arc<TaskStore<S>>) -> Self {
        Self {
            node_id: NodeId::new(node_id),
            task_store,
            max_attempts: 10, // Try up to 10 shards before giving up
        }
    }

    /// Try to claim an available shard
    ///
    /// Returns Some(shard_id) if successful, None if no shards available
    pub async fn try_claim(&self) -> Result<Option<String>> {
        info!("Attempting to claim shard for node {}", self.node_id);

        // Query all tasks and find pending shards
        let tasks = self.task_store.list_tasks().await?;
        
        if tasks.is_empty() {
            debug!("No tasks available");
            return Ok(None);
        }

        debug!("Checking {} tasks for pending shards", tasks.len());

        // Collect all pending shards from all tasks
        let mut pending_shards = Vec::new();
        for task in &tasks {
            let shards = self.task_store
                .list_shards_by_state(&task.task_id, |s| matches!(s, ShardState::Pending))
                .await?;
            pending_shards.extend(shards);
        }

        if pending_shards.is_empty() {
            debug!("No pending shards available");
            return Ok(None);
        }

        debug!("Found {} pending shards", pending_shards.len());

        // Try to claim shards one by one
        let attempts = pending_shards.len().min(self.max_attempts);
        for shard in pending_shards.iter().take(attempts) {
            match self.try_claim_shard(&shard.shard_id).await {
                Ok(true) => {
                    info!(
                        "Successfully claimed shard {} for node {}",
                        shard.shard_id, self.node_id
                    );
                    return Ok(Some(shard.shard_id.to_string()));
                }
                Ok(false) => {
                    debug!(
                        "Failed to claim shard {} (already claimed by another node)",
                        shard.shard_id
                    );
                    // Try next shard
                    continue;
                }
                Err(e) => {
                    warn!(
                        "Error attempting to claim shard {}: {}",
                        shard.shard_id, e
                    );
                    // Try next shard
                    continue;
                }
            }
        }

        debug!(
            "Failed to claim any shard after {} attempts",
            attempts
        );
        Ok(None)
    }

    /// Try to claim a specific shard using CAS (Compare-And-Swap)
    ///
    /// Returns Ok(true) if successfully claimed, Ok(false) if already claimed
    async fn try_claim_shard(&self, shard_id: &ShardId) -> Result<bool> {
        // TODO: Integrate with Raft for atomic CAS operation
        // For now, we'll use a simple state transition in TaskStore
        // In production, this should be:
        // 1. Propose ClaimShard command to Raft
        // 2. Wait for commit
        // 3. Apply state machine update
        // 4. Return success/failure

        // Load current shard state
        let shard = self.task_store.get_shard(shard_id).await?
            .ok_or_else(|| DatspoolError::Internal(format!("Shard not found: {}", shard_id)))?;

        // Check if still pending
        if !matches!(shard.state, ShardState::Pending) {
            debug!("Shard {} is not pending: {:?}", shard_id, shard.state);
            return Ok(false);
        }

        // Try to transition to Running
        // In real implementation, this would be an atomic Raft proposal
        let now = chrono::Utc::now();
        let new_state = ShardState::Running {
            node_id: self.node_id.clone(),
            started_at: now,
        };

        // Update shard state (this is NOT atomic without Raft!)
        // TODO: Replace with Raft CAS operation
        let mut updated_shard = shard.clone();
        updated_shard.state = new_state;
        updated_shard.updated_at = std::time::SystemTime::now();

        // Save updated shard
        self.task_store.save_shard(&updated_shard).await?;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::rocksdb::RocksDbStore;
    use crate::types::TaskId;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_claim_no_shards() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let task_store = Arc::new(TaskStore::new(db));
        
        let claimer = ShardClaimer::new("node-1".to_string(), task_store);
        let result = claimer.try_claim().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_claim_pending_shard() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let task_store = Arc::new(TaskStore::new(db));

        // Create a task first
        let task_id = TaskId::new();
        let task = crate::task::meta::TaskMeta::new(
            "test-task".to_string(),
            "test-source".to_string(),
            "test-sink".to_string(),
        );
        let mut task_with_id = task;
        task_with_id.task_id = task_id.clone();
        task_store.save_task(&task_with_id).await.unwrap();

        // Create a pending shard
        let shard = crate::task::shard::ShardMeta::new(
            task_id,
            0,
            "{}".to_string(),
        );
        task_store.save_shard(&shard).await.unwrap();

        // Try to claim
        let claimer = ShardClaimer::new("node-1".to_string(), task_store.clone());
        let result = claimer.try_claim().await.unwrap();
        
        assert!(result.is_some());
        assert_eq!(result.unwrap(), shard.shard_id.to_string());

        // Verify shard is now Running
        let claimed = task_store.get_shard(&shard.shard_id).await.unwrap().unwrap();
        assert!(matches!(claimed.state, ShardState::Running { .. }));
    }

    #[tokio::test]
    async fn test_claim_conflict() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let task_store = Arc::new(TaskStore::new(db));

        // Create a task first
        let task_id = TaskId::new();
        let task = crate::task::meta::TaskMeta::new(
            "test-task".to_string(),
            "test-source".to_string(),
            "test-sink".to_string(),
        );
        let mut task_with_id = task;
        task_with_id.task_id = task_id.clone();
        task_store.save_task(&task_with_id).await.unwrap();

        // Create a pending shard
        let shard = crate::task::shard::ShardMeta::new(
            task_id,
            0,
            "{}".to_string(),
        );
        task_store.save_shard(&shard).await.unwrap();

        // First claimer claims it
        let claimer1 = ShardClaimer::new("node-1".to_string(), task_store.clone());
        let result1 = claimer1.try_claim().await.unwrap();
        assert!(result1.is_some());

        // Second claimer should get None (no pending shards)
        let claimer2 = ShardClaimer::new("node-2".to_string(), task_store);
        let result2 = claimer2.try_claim().await.unwrap();
        assert!(result2.is_none());
    }
}
