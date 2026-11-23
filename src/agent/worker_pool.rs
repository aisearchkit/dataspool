// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Worker pool for concurrent shard execution

use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{ShardClaimer, ShardExecutor};
use crate::error::{DatspoolError, Result};
use crate::raft::RaftNode;
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
        raft: Arc<RaftNode>,
    ) -> Result<Self> {
        let executor_raft = raft.clone();
        Ok(Self {
            node_id: node_id.clone(),
            limits: ResourceLimits {
                max_concurrent_tasks,
                memory_limit_mb: 4096, // Default 4GB
            },
            semaphore: Arc::new(Semaphore::new(max_concurrent_tasks)),
            active_shards: Arc::new(RwLock::new(Vec::new())),
            claimer: ShardClaimer::new(node_id.clone(), task_store.clone(), raft),
            executor: ShardExecutor::new_with_raft(task_store, index_store, executor_raft),
        })
    }

    /// Start the worker pool main loop. Use run_until_cancelled when the
    /// embedding service needs coordinated shutdown.
    pub async fn run(&mut self) -> Result<()> {
        self.run_until_cancelled(CancellationToken::new()).await
    }

    /// Stop claiming work when cancelled, signal active shard executors, and
    /// wait for their in-flight writes/checkpoints and connection cleanup.
    pub async fn run_until_cancelled(&mut self, shutdown: CancellationToken) -> Result<()> {
        info!("Starting worker pool on node {}", self.node_id);
        let mut workers = JoinSet::new();

        loop {
            while let Some(joined) = workers.try_join_next() {
                if let Err(error) = joined {
                    warn!("Shard worker task panicked: {}", error);
                }
            }

            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Worker pool shutdown requested on node {}", self.node_id);
                    break;
                }
                permit = self.semaphore.clone().acquire_owned() => {
                    let permit = permit.map_err(|e| {
                        DatspoolError::Internal(format!("Semaphore error: {}", e))
                    })?;
                    if shutdown.is_cancelled() {
                        drop(permit);
                        break;
                    }

                    match self.claimer.try_claim().await {
                        Ok(Some(lease)) => {
                            let shard_id = lease.shard_id.to_string();
                            info!(shard_id, fencing_token = lease.fencing_token, expires_at = %lease.expires_at, "Claimed shard with Raft lease");
                            self.active_shards.write().await.push(shard_id.clone());

                            let executor = self.executor.clone();
                            let active_shards = self.active_shards.clone();
                            let shard_id_clone = shard_id.clone();
                            let claimer = self.claimer.clone();
                            let worker_shutdown = shutdown.child_token();

                            workers.spawn(async move {
                                let mut lease = lease;
                                let execution_lease = lease.clone();
                                let worker_cancel = worker_shutdown.clone();
                                let mut execution = tokio::spawn(async move {
                                    executor.execute_with_lease(&shard_id, execution_lease, worker_cancel).await
                                });
                                let renew_interval = std::time::Duration::from_secs(8);
                                loop {
                                    tokio::select! {
                                        result = &mut execution => {
                                            match result {
                                                Err(error) => warn!(%error, "Shard executor task panicked"),
                                                Ok(Err(error)) => warn!(%shard_id, %error, "Shard execution failed"),
                                                Ok(Ok(_)) => {}
                                            }
                                            break;
                                        }
                                        _ = worker_shutdown.cancelled() => {
                                            break;
                                        }
                                        _ = tokio::time::sleep(renew_interval) => {
                                            match claimer.renew(&mut lease).await {
                                                Ok(true) => {}
                                                Ok(false) => {
                                                    warn!(%shard_id, fencing_token = lease.fencing_token, "Lease lost; cancelling stale shard worker");
                                                    worker_shutdown.cancel();
                                                    break;
                                                }
                                                Err(error) => {
                                                    warn!(%shard_id, %error, "Lease renewal failed; cancelling shard worker");
                                                    worker_shutdown.cancel();
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                                worker_shutdown.cancel();
                                if !execution.is_finished() {
                                    let _ = execution.await;
                                }
                                active_shards.write().await.retain(|id| id != &shard_id_clone);
                                drop(permit);
                            });
                        }
                        Ok(None) => {
                            drop(permit);
                            tokio::select! {
                                _ = shutdown.cancelled() => break,
                                _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {}
                            }
                        }
                        Err(error) => {
                            warn!("Failed to claim shard: {}", error);
                            drop(permit);
                            tokio::select! {
                                _ = shutdown.cancelled() => break,
                                _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {}
                            }
                        }
                    }
                }
            }
        }

        while let Some(joined) = workers.join_next().await {
            if let Err(error) = joined {
                warn!("Shard worker task failed during shutdown: {}", error);
            }
        }

        info!("Worker pool stopped on node {}", self.node_id);
        Ok(())
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

    #[tokio::test]
    async fn worker_pool_exits_when_shutdown_is_cancelled() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let task_store = Arc::new(TaskStore::new(db.clone()));
        let raft = crate::raft::RaftNode::new(
            1,
            dir.path().join("raft"),
            (*task_store).clone(),
            crate::raft::default_raft_config(),
            "test-raft-auth-token-0123456789abcdef".to_string(),
        ).await.unwrap();
        raft.initialize(std::collections::BTreeMap::from([(1, openraft::BasicNode::new("127.0.0.1:0"))])).await.unwrap();
        let index_store = Arc::new(RwLock::new(TieredIndexStore::new(TieredConfig::default(), Arc::new(db))));
        let mut pool = WorkerPool::new("node-shutdown-test".to_string(), 1, task_store, index_store, Arc::new(raft)).unwrap();
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        pool.run_until_cancelled(shutdown).await.unwrap();
        assert_eq!(pool.get_usage().await.0, 0);
    }


}
