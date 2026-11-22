// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Worker pool for concurrent shard execution

use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use tracing::{info, warn};

use super::{ShardClaimer, ShardExecutor};
use crate::error::{DatspoolError, Result};
use crate::storage::index::TieredIndexStore;
use crate::storage::kv::KvStore;
use crate::task::store::TaskStore;

/// Resource limits for the worker pool
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    pub max_concurrent_tasks: usize,
    #[allow(dead_code)]
    pub memory_limit_mb: u64,
}

/// Worker pool that manages concurrent shard execution
pub struct WorkerPool<S: KvStore> {
    node_id: String,
    limits: ResourceLimits,
    semaphore: Arc<Semaphore>,
    active_shards: Arc<RwLock<Vec<String>>>,
    claimer: ShardClaimer<S>,
    executor: ShardExecutor<S>,
}

impl<S: KvStore + Clone + 'static> WorkerPool<S> {
    pub fn new(
        node_id: String,
        max_concurrent_tasks: usize,
        task_store: Arc<TaskStore<S>>,
        index_store: Arc<RwLock<TieredIndexStore<S>>>,
    ) -> Result<Self> {
        Ok(Self {
            node_id: node_id.clone(),
            limits: ResourceLimits {
                max_concurrent_tasks,
                memory_limit_mb: 4096, // Default 4GB
            },
            semaphore: Arc::new(Semaphore::new(max_concurrent_tasks)),
            active_shards: Arc::new(RwLock::new(Vec::new())),
            claimer: ShardClaimer::new(node_id.clone(), task_store.clone()),
            executor: ShardExecutor::new(task_store, index_store),
        })
    }

    /// Start the worker pool main loop
    pub async fn run(&mut self) -> Result<()> {
        info!("Starting worker pool on node {}", self.node_id);

        loop {
            // Try to claim a shard if we have capacity
            let permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| DatspoolError::Internal(format!("Semaphore error: {}", e)))?;

            match self.claimer.try_claim().await {
                Ok(Some(shard_id)) => {
                    info!("Claimed shard: {}", shard_id);

                    // Track active shard
                    self.active_shards.write().await.push(shard_id.clone());

                    // Spawn shard execution task
                    let executor = self.executor.clone();
                    let active_shards = self.active_shards.clone();
                    let shard_id_clone = shard_id.clone();

                    tokio::spawn(async move {
                        match executor.execute(&shard_id).await {
                            Ok(stats) => {
                                info!(
                                    "Shard {} completed: {} docs read, {} docs written in {:?}",
                                    shard_id,
                                    stats.docs_read,
                                    stats.docs_written,
                                    stats.duration()
                                );
                            }
                            Err(e) => warn!("Shard {} failed: {}", shard_id, e),
                        }

                        // Remove from active shards
                        active_shards
                            .write()
                            .await
                            .retain(|id| id != &shard_id_clone);
                        drop(permit); // Release semaphore
                    });
                }
                Ok(None) => {
                    // No shards available, wait before retrying
                    drop(permit);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
                Err(e) => {
                    warn!("Failed to claim shard: {}", e);
                    drop(permit);
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                }
            }
        }
    }

    /// Get current resource usage
    pub async fn get_usage(&self) -> (usize, usize) {
        let active = self.active_shards.read().await.len();
        (active, self.limits.max_concurrent_tasks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::index::TieredConfig;
    use crate::storage::rocksdb::RocksDbStore;
    use tempfile::tempdir;

    #[test]
    fn test_worker_pool_creation() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let task_store = Arc::new(TaskStore::new(db.clone()));

        let index_config = TieredConfig::default();
        let index_store = Arc::new(RwLock::new(TieredIndexStore::new(
            index_config,
            Arc::new(db),
        )));

        let pool = WorkerPool::new("node-1".to_string(), 4, task_store, index_store).unwrap();
        assert_eq!(pool.node_id, "node-1");
        assert_eq!(pool.limits.max_concurrent_tasks, 4);
    }
}
