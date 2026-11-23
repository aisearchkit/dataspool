// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Dataspool Contributors

//! Task and shard metadata storage

use crate::error::Result;
use crate::storage::{KvStore, cf};
use crate::task::{ShardMeta, TaskMeta};
use crate::types::{ShardId, ShardState, TaskId};

/// Task metadata storage
pub struct TaskStore<S: KvStore> {
    store: S,
}

impl<S: KvStore> TaskStore<S> {
    /// Create a new task store
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// Borrow the underlying KV store for shard-scoped durable pipeline queues.
    pub fn kv_store(&self) -> &S {
        &self.store
    }

    /// Save task metadata
    pub async fn save_task(&self, task: &TaskMeta) -> Result<()> {
        let key = format!("task:{}", task.task_id);
        let value = serde_json::to_vec(task)?;
        self.store.put_cf(cf::META, key.as_bytes(), &value).await?;
        Ok(())
    }

    /// Get task metadata by ID
    pub async fn get_task(&self, task_id: &TaskId) -> Result<Option<TaskMeta>> {
        let key = format!("task:{}", task_id);
        if let Some(value) = self.store.get_cf(cf::META, key.as_bytes()).await? {
            let task: TaskMeta = serde_json::from_slice(&value)?;
            Ok(Some(task))
        } else {
            Ok(None)
        }
    }

    /// List all tasks
    pub async fn list_tasks(&self) -> Result<Vec<TaskMeta>> {
        let prefix = b"task:";
        let pairs = self.store.prefix_scan_cf(cf::META, prefix).await?;

        let mut tasks = Vec::new();
        for (_key, value) in pairs {
            let task: TaskMeta = serde_json::from_slice(&value)?;
            tasks.push(task);
        }

        Ok(tasks)
    }

    /// Delete task metadata
    pub async fn delete_task(&self, task_id: &TaskId) -> Result<()> {
        let key = format!("task:{}", task_id);
        self.store.delete_cf(cf::META, key.as_bytes()).await?;
        Ok(())
    }

    /// Save shard metadata
    pub async fn save_shard(&self, shard: &ShardMeta) -> Result<()> {
        let key = format!("shard:{}", shard.shard_id);
        let value = serde_json::to_vec(shard)?;
        self.store.put_cf(cf::META, key.as_bytes(), &value).await?;

        // Also index by task_id for efficient lookup
        let task_index_key = format!("task_shard:{}:{}", shard.task_id, shard.shard_id);
        self.store
            .put_cf(cf::META, task_index_key.as_bytes(), b"")
            .await?;

        Ok(())
    }

    /// Get shard metadata by ID
    pub async fn get_shard(&self, shard_id: &ShardId) -> Result<Option<ShardMeta>> {
        let key = format!("shard:{}", shard_id);
        if let Some(value) = self.store.get_cf(cf::META, key.as_bytes()).await? {
            let shard: ShardMeta = serde_json::from_slice(&value)?;
            Ok(Some(shard))
        } else {
            Ok(None)
        }
    }

    /// List all shards for a task
    pub async fn list_task_shards(&self, task_id: &TaskId) -> Result<Vec<ShardMeta>> {
        let prefix = format!("task_shard:{}:", task_id);
        let pairs = self
            .store
            .prefix_scan_cf(cf::META, prefix.as_bytes())
            .await?;

        let mut shards = Vec::new();
        for (key, _value) in pairs {
            // Extract shard_id from "task_shard:TASK_ID:SHARD_ID"
            let key_str = String::from_utf8_lossy(&key);
            if let Some(shard_id_str) = key_str.split(':').nth(2) {
                let shard_key = format!("shard:{}", shard_id_str);
                if let Some(value) = self.store.get_cf(cf::META, shard_key.as_bytes()).await? {
                    let shard: ShardMeta = serde_json::from_slice(&value)?;
                    shards.push(shard);
                }
            }
        }

        Ok(shards)
    }

    /// List shards by state
    pub async fn list_shards_by_state(
        &self,
        task_id: &TaskId,
        state_matcher: impl Fn(&ShardState) -> bool,
    ) -> Result<Vec<ShardMeta>> {
        let all_shards = self.list_task_shards(task_id).await?;
        Ok(all_shards
            .into_iter()
            .filter(|s| state_matcher(&s.state))
            .collect())
    }

    /// Return shards left Running by this node to Pending during process startup.
    /// Only the local node's leases are recovered; another node may still own
    /// and actively execute its shards.
    pub async fn recover_interrupted_shards(&self, node_id: &str) -> Result<usize> {
        let tasks = self.list_tasks().await?;
        let mut recovered = 0;
        for task in tasks {
            let shards = self
                .list_shards_by_state(&task.task_id, |state| {
                    matches!(state, ShardState::Running { node_id: owner, .. } if owner.as_str() == node_id)
                })
                .await?;
            for mut shard in shards {
                shard.set_state(ShardState::Pending);
                self.save_shard(&shard).await?;
                recovered += 1;
            }
        }
        Ok(recovered)
    }

    /// Delete shard metadata
    pub async fn delete_shard(&self, shard: &ShardMeta) -> Result<()> {
        let key = format!("shard:{}", shard.shard_id);
        self.store.delete_cf(cf::META, key.as_bytes()).await?;

        // Also delete task index
        let task_index_key = format!("task_shard:{}:{}", shard.task_id, shard.shard_id);
        self.store
            .delete_cf(cf::META, task_index_key.as_bytes())
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RocksDbStore;
    use crate::types::{NodeId, TaskState};
    use tempfile::TempDir;

    async fn create_test_store() -> (TaskStore<RocksDbStore>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db = RocksDbStore::open(temp_dir.path()).unwrap();
        (TaskStore::new(db), temp_dir)
    }

    #[tokio::test]
    async fn test_save_and_get_task() {
        let (store, _temp) = create_test_store().await;

        let task = TaskMeta::new("test-task".to_string(), "{}".to_string(), "{}".to_string());
        let task_id = task.task_id;

        store.save_task(&task).await.unwrap();

        let retrieved = store.get_task(&task_id).await.unwrap();
        assert!(retrieved.is_some());
        let retrieved_task = retrieved.unwrap();
        assert_eq!(retrieved_task.name, "test-task");
        assert_eq!(retrieved_task.task_id, task_id);
    }

    #[tokio::test]
    async fn test_list_tasks() {
        let (store, _temp) = create_test_store().await;

        let mut task1 = TaskMeta::new("task1".to_string(), "{}".to_string(), "{}".to_string());
        task1.start(5);

        let task2 = TaskMeta::new("task2".to_string(), "{}".to_string(), "{}".to_string());

        store.save_task(&task1).await.unwrap();
        store.save_task(&task2).await.unwrap();

        let all_tasks = store.list_tasks().await.unwrap();
        assert_eq!(all_tasks.len(), 2);
    }

    #[tokio::test]
    async fn test_save_and_get_shard() {
        let (store, _temp) = create_test_store().await;

        let task_id = TaskId::new();
        let shard = ShardMeta::new(task_id, 0, "{}".to_string());
        let shard_id = shard.shard_id;

        store.save_shard(&shard).await.unwrap();

        let retrieved = store.get_shard(&shard_id).await.unwrap();
        assert!(retrieved.is_some());
        let retrieved_shard = retrieved.unwrap();
        assert_eq!(retrieved_shard.shard_id, shard_id);
        assert_eq!(retrieved_shard.task_id, task_id);
    }

    #[tokio::test]
    async fn test_list_task_shards() {
        let (store, _temp) = create_test_store().await;

        let task_id = TaskId::new();

        // Create multiple shards for the same task
        for i in 0..5 {
            let shard = ShardMeta::new(task_id, i, "{}".to_string());
            store.save_shard(&shard).await.unwrap();
        }

        let shards = store.list_task_shards(&task_id).await.unwrap();
        assert_eq!(shards.len(), 5);
    }

    #[tokio::test]
    async fn test_list_shards_by_state() {
        let (store, _temp) = create_test_store().await;

        let task_id = TaskId::new();

        // Create shards with different states
        let mut shard1 = ShardMeta::new(task_id, 0, "{}".to_string());
        shard1.start(NodeId::new("node-1"));

        let shard2 = ShardMeta::new(task_id, 1, "{}".to_string());

        let mut shard3 = ShardMeta::new(task_id, 2, "{}".to_string());
        shard3.complete(1000);

        store.save_shard(&shard1).await.unwrap();
        store.save_shard(&shard2).await.unwrap();
        store.save_shard(&shard3).await.unwrap();

        let running_shards = store
            .list_shards_by_state(&task_id, |s| matches!(s, ShardState::Running { .. }))
            .await
            .unwrap();
        assert_eq!(running_shards.len(), 1);

        let pending_shards = store
            .list_shards_by_state(&task_id, |s| matches!(s, ShardState::Pending))
            .await
            .unwrap();
        assert_eq!(pending_shards.len(), 1);

        let completed_shards = store
            .list_shards_by_state(&task_id, |s| matches!(s, ShardState::Completed { .. }))
            .await
            .unwrap();
        assert_eq!(completed_shards.len(), 1);
    }

    #[tokio::test]
    async fn test_recover_only_shards_owned_by_restarting_node() {
        let (store, _temp) = create_test_store().await;
        let task = TaskMeta::new("recovery-test".to_string(), "{}".to_string(), "{}".to_string());
        let task_id = task.task_id;
        store.save_task(&task).await.unwrap();

        let mut local = ShardMeta::new(task_id, 0, "{}".to_string());
        local.start(NodeId::new("node-local"));
        let local_id = local.shard_id;
        store.save_shard(&local).await.unwrap();

        let mut remote = ShardMeta::new(task_id, 1, "{}".to_string());
        remote.start(NodeId::new("node-remote"));
        let remote_id = remote.shard_id;
        store.save_shard(&remote).await.unwrap();

        assert_eq!(store.recover_interrupted_shards("node-local").await.unwrap(), 1);
        assert!(matches!(store.get_shard(&local_id).await.unwrap().unwrap().state, ShardState::Pending));
        assert!(matches!(store.get_shard(&remote_id).await.unwrap().unwrap().state, ShardState::Running { .. }));
    }

    #[tokio::test]
    async fn test_task_state_persistence() {
        let (store, _temp) = create_test_store().await;

        let mut task = TaskMeta::new("test".to_string(), "{}".to_string(), "{}".to_string());
        let task_id = task.task_id;

        task.start(10);
        store.save_task(&task).await.unwrap();

        let loaded = store.get_task(&task_id).await.unwrap().unwrap();
        assert!(matches!(loaded.state, TaskState::Running { .. }));
        assert_eq!(loaded.shard_count, 10);
    }
}
