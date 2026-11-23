// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Dataspool Contributors

//! Task and shard metadata storage

use crate::error::Result;
use crate::storage::{KvStore, cf};
use crate::task::{ShardMeta, TaskMeta};
use crate::types::{ShardId, ShardState, TaskId};

/// Replicated application column-family contents included in Raft snapshots.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskStoreSnapshot {
    pub metadata: Vec<(Vec<u8>, Vec<u8>)>,
    pub offsets: Vec<(Vec<u8>, Vec<u8>)>,
    pub cache: Vec<(Vec<u8>, Vec<u8>)>,
    pub dead_letters: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Task metadata storage
pub struct TaskStore<S: KvStore> {
    store: S,
}

impl<S: KvStore> std::fmt::Debug for TaskStore<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("TaskStore").finish_non_exhaustive()
    }
}

impl<S: KvStore + Clone> Clone for TaskStore<S> {
    fn clone(&self) -> Self {
        Self::new(self.store.clone())
    }
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
        let task_index_key = format!("task_shard:{}:{}", shard.task_id, shard.shard_id);
        let mut batch = crate::storage::WriteBatch::new();
        batch.put_cf(cf::META, key, value);
        batch.put_cf(cf::META, task_index_key, bytes::Bytes::new());
        self.store.write_batch(batch).await?;
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

    /// Persist a shard batch under its deterministic shard-scoped key.
    pub(crate) async fn save_cache_batch(
        &self,
        shard_id: &str,
        batch_id: &str,
        entry: &crate::pipeline::cache::CacheEntry,
    ) -> Result<()> {
        let key = crate::pipeline::cache::LocalBatchCache::<S>::key_for_shard(shard_id, batch_id);
        let value = serde_json::to_vec(entry)?;
        self.store.put_cf(crate::storage::cf::CACHE, &key, &value).await?;
        Ok(())
    }

    pub(crate) async fn cache_batch_count(&self, shard_id: &str) -> Result<usize> {
        let prefix = crate::pipeline::cache::LocalBatchCache::<S>::prefix_for_shard(shard_id);
        Ok(self.store.prefix_scan_cf(crate::storage::cf::CACHE, &prefix).await?.len())
    }

    /// Return the oldest unacknowledged replicated batch key for FIFO enforcement.
    pub(crate) async fn first_cache_batch_key(&self, shard_id: &str) -> Result<Option<Vec<u8>>> {
        let prefix = crate::pipeline::cache::LocalBatchCache::<S>::prefix_for_shard(shard_id);
        Ok(self.store.prefix_scan_cf(crate::storage::cf::CACHE, &prefix).await?
            .into_iter().next().map(|(key, _)| key.to_vec()))
    }

    /// Highest acknowledged Raft batch ID for a shard. Batch IDs are fixed-width
    /// hexadecimal fencing-generation/sequence pairs, so lexical order is numeric.
    pub(crate) async fn cache_ack_watermark(&self, shard_id: &str) -> Result<Option<String>> {
        let key = format!("shard_cache_ack:{shard_id}");
        self.store.get_cf(cf::META, key.as_bytes()).await?
            .map(|value| String::from_utf8(value.to_vec())
                .map_err(|e| crate::error::DatspoolError::Serialization(e.to_string())))
            .transpose()
    }

    /// Atomically remove a replicated batch and advance its per-shard ack watermark.
    pub(crate) async fn ack_cache_batch(&self, shard_id: &str, batch_id: &str, key: &[u8]) -> Result<()> {
        let watermark_key = format!("shard_cache_ack:{shard_id}");
        let mut batch = crate::storage::WriteBatch::new();
        batch.delete_cf(crate::storage::cf::CACHE, key.to_vec());
        batch.put_cf(cf::META, watermark_key, bytes::Bytes::copy_from_slice(batch_id.as_bytes()));
        self.store.write_batch(batch).await?;
        Ok(())
    }

    /// Clear pre-Raft application state on a brand-new non-bootstrap member before
    /// it joins the cluster. The bootstrap member is the sole source of truth.
    pub(crate) async fn clear_unreplicated_state(&self) -> Result<usize> {
        let mut batch = crate::storage::WriteBatch::new();
        let mut removed = 0;
        for column_family in [cf::META, cf::OFFSET, cf::CACHE, cf::DEAD_LETTER] {
            for (key, _) in self.store.prefix_scan_cf(column_family, b"").await? {
                batch.delete_cf(column_family, key);
                removed += 1;
            }
        }
        if removed > 0 {
            self.store.write_batch(batch).await?;
            self.store.flush_durable().await?;
        }
        Ok(removed)
    }

    /// Whether the bootstrap member has imported legacy state and opened the
    /// replicated scheduler for workers on every cluster member.
    pub(crate) async fn bootstrap_complete(&self) -> Result<bool> {
        Ok(self.store.get_cf(cf::META, b"system:bootstrap_complete").await?.is_some())
    }

    pub(crate) async fn mark_bootstrap_complete(&self) -> Result<()> {
        self.store.put_cf(cf::META, b"system:bootstrap_complete", b"1").await?;
        Ok(())
    }

    /// Remove the old node-local queue after its entries have been imported
    /// into the replicated Raft cache.
    pub(crate) async fn clear_legacy_cache(&self) -> Result<usize> {
        let mut batch = crate::storage::WriteBatch::new();
        let entries = self.store.prefix_scan_cf(crate::storage::cf::CACHE, b"migration-batch/").await?;
        let count = entries.len();
        for (key, _) in entries {
            batch.delete_cf(crate::storage::cf::CACHE, key);
        }
        if count > 0 {
            self.store.write_batch(batch).await?;
            self.store.flush_durable().await?;
        }
        Ok(count)
    }

    /// Read the state-machine applied marker stored atomically with application data.
    pub(crate) async fn raft_applied_state(&self) -> Result<Option<Vec<u8>>> {
        Ok(self.store.get_cf(cf::META, b"system:raft_applied_state").await?
            .map(|value| value.to_vec()))
    }

    /// Persist the applied marker in the same RocksDB as replicated application data.
    pub(crate) async fn set_raft_applied_state(&self, value: &[u8]) -> Result<()> {
        self.store.put_cf(cf::META, b"system:raft_applied_state", value).await?;
        Ok(())
    }

    /// Export all shard/task state and durable migration queues for a Raft snapshot.
    pub async fn export_snapshot(&self) -> Result<TaskStoreSnapshot> {
        let metadata = self.store.prefix_scan_cf(crate::storage::cf::META, b"").await?
            .into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        let offsets = self.store.prefix_scan_cf(crate::storage::cf::OFFSET, b"").await?
            .into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        let cache = self.store.prefix_scan_cf(crate::storage::cf::CACHE, b"").await?
            .into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        let dead_letters = self.store.prefix_scan_cf(crate::storage::cf::DEAD_LETTER, b"").await?
            .into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        Ok(TaskStoreSnapshot { metadata, offsets, cache, dead_letters })
    }

    /// Atomically replace replicated application data from an installed Raft snapshot.
    pub async fn replace_snapshot(&self, snapshot: TaskStoreSnapshot) -> Result<()> {
        let mut batch = crate::storage::WriteBatch::new();
        for column_family in [crate::storage::cf::META, crate::storage::cf::OFFSET, crate::storage::cf::CACHE, crate::storage::cf::DEAD_LETTER] {
            for (key, _) in self.store.prefix_scan_cf(column_family, b"").await? {
                batch.delete_cf(column_family, key);
            }
        }
        for (key, value) in snapshot.metadata { batch.put_cf(crate::storage::cf::META, key, value); }
        for (key, value) in snapshot.offsets { batch.put_cf(crate::storage::cf::OFFSET, key, value); }
        for (key, value) in snapshot.cache { batch.put_cf(crate::storage::cf::CACHE, key, value); }
        for (key, value) in snapshot.dead_letters { batch.put_cf(crate::storage::cf::DEAD_LETTER, key, value); }
        self.store.write_batch(batch).await?;
        self.store.flush_durable().await?;
        Ok(())
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
    async fn raft_applied_marker_is_included_in_snapshots() {
        let (store, _temp) = create_test_store().await;
        let marker = serde_json::to_vec(&serde_json::json!({"last_applied": 42})).unwrap();
        store.set_raft_applied_state(&marker).await.unwrap();
        store.mark_bootstrap_complete().await.unwrap();
        store.kv_store().flush_durable().await.unwrap();

        let snapshot = store.export_snapshot().await.unwrap();
        assert!(snapshot.metadata.iter().any(|(key, value)| {
            key == b"system:raft_applied_state" && value == &marker
        }));

        let (restored, _restored_temp) = create_test_store().await;
        restored.replace_snapshot(snapshot).await.unwrap();
        assert_eq!(restored.raft_applied_state().await.unwrap(), Some(marker));
        assert!(restored.bootstrap_complete().await.unwrap());
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
