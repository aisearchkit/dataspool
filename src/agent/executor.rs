// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Shard executor - runs data migration pipeline for a shard

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::drivers::factory::{SinkConfig, SourceConfig, create_sink, create_source};
use crate::error::{DatspoolError, Result};
use crate::pipeline::{DeadLetterQueue, LocalBatchCache, Sink, Source};
use crate::storage::index::TieredIndexStore;
use crate::storage::kv::KvStore;
use crate::task::shard::ShardMeta;
use crate::task::store::TaskStore;

/// Shard execution statistics
#[derive(Debug, Clone, Default)]
pub struct ExecutionStats {
    pub docs_read: u64,
    pub docs_written: u64,
    pub docs_indexed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub start_time: Option<std::time::Instant>,
    pub end_time: Option<std::time::Instant>,
}

impl ExecutionStats {
    pub fn duration(&self) -> Option<std::time::Duration> {
        match (self.start_time, self.end_time) {
            (Some(start), Some(end)) => Some(end.duration_since(start)),
            _ => None,
        }
    }
}

/// Executes a shard using the data pipeline
pub struct ShardExecutor<S: KvStore> {
    task_store: Arc<TaskStore<S>>,
    index_store: Arc<RwLock<TieredIndexStore<S>>>,
    batch_size: usize,
}

impl<S: KvStore + Clone + 'static> Clone for ShardExecutor<S> {
    fn clone(&self) -> Self {
        Self {
            task_store: self.task_store.clone(),
            index_store: self.index_store.clone(),
            batch_size: self.batch_size,
        }
    }
}

impl<S: KvStore + Clone + 'static> ShardExecutor<S> {
    pub fn new(
        task_store: Arc<TaskStore<S>>,
        index_store: Arc<RwLock<TieredIndexStore<S>>>,
    ) -> Self {
        Self {
            task_store,
            index_store,
            batch_size: 1000, // Default batch size
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Load source/sink configurations from the owning task and execute a shard.
    pub async fn execute(&self, shard_id: &str) -> Result<ExecutionStats> {
        use std::str::FromStr;

        info!("Executing shard: {}", shard_id);
        let shard_key = crate::types::ShardId::from_str(shard_id).map_err(|e| {
            DatspoolError::Configuration(format!("Invalid shard id '{shard_id}': {e}"))
        })?;
        let shard = self
            .task_store
            .get_shard(&shard_key)
            .await?
            .ok_or_else(|| DatspoolError::Internal(format!("Shard {shard_id} not found")))?;
        let task = self
            .task_store
            .get_task(&shard.task_id)
            .await?
            .ok_or_else(|| DatspoolError::Internal(format!("Task {} not found", shard.task_id)))?;

        let source_config: SourceConfig = serde_json::from_str(&task.source_config)
            .map_err(|e| DatspoolError::Configuration(format!("Invalid source config: {e}")))?;
        let sink_config: SinkConfig = serde_json::from_str(&task.sink_config)
            .map_err(|e| DatspoolError::Configuration(format!("Invalid sink config: {e}")))?;
        let mut source = create_source(&source_config)?;
        let mut sink = create_sink(&sink_config)?;

        let stats = self
            .execute_pipeline_with_instances(&shard, source.as_mut(), sink.as_mut())
            .await?;
        let mut completed = self
            .task_store
            .get_shard(&shard_key)
            .await?
            .ok_or_else(|| {
                DatspoolError::Internal(format!("Shard {shard_id} disappeared during execution"))
            })?;
        completed.complete(stats.docs_written);
        self.task_store.save_shard(&completed).await?;

        info!(
            shard_id,
            docs_read = stats.docs_read,
            docs_written = stats.docs_written,
            "Shard execution completed"
        );
        Ok(stats)
    }

    /// Execute pipeline for a shard (will be used when Source/Sink factory is ready)
    ///
    /// Example usage:
    /// ```ignore
    /// let source = create_source_from_config(&shard.source_config)?;
    /// let sink = create_sink_from_config(&shard.sink_config)?;
    /// let stats = executor.execute_pipeline_with_instances(&shard, source, sink).await?;
    /// ```
    #[allow(dead_code)]
    pub async fn execute_pipeline_with_instances(
        &self,
        shard: &ShardMeta,
        source: &mut dyn Source,
        sink: &mut dyn Sink,
    ) -> Result<ExecutionStats> {
        let mut stats = ExecutionStats {
            start_time: Some(std::time::Instant::now()),
            ..Default::default()
        };

        // Connect source and sink
        source.connect().await?;
        sink.connect().await?;

        // Use the latest durable shard checkpoint, not a possibly stale caller copy.
        let current_shard = self.task_store.get_shard(&shard.shard_id).await?;
        let mut offset = current_shard
            .as_ref()
            .map(|stored| stored.offset.clone())
            .unwrap_or_else(|| shard.offset.clone());
        let cache = LocalBatchCache::new_for_shard(
            Arc::new(self.task_store.kv_store().clone()),
            128,
            &shard.shard_id.to_string(),
        );

        // Replay durable batches before reading new source data after a restart.
        while cache.pending().await? > 0 {
            let task_store = self.task_store.clone();
            let shard_id = shard.shard_id;
            let sink_ref = &mut *sink;
            let delivered = cache
                .deliver_one_with_checkpoint(|batch, next_offset, source_count| async move {
                    let write = sink_ref.write_batch(batch).await?;
                    if write.rejected_count > 0 || write.failed_count > write.failed_records.len() {
                        return Err(crate::error::DatspoolError::Internal(format!(
                            "cached batch delivery still has retryable/unclassified failures: {} failed, {} rejected: {}",
                            write.failed_count, write.rejected_count, write.errors.join("; ")
                        )));
                    }
                    if !write.failed_records.is_empty() {
                        let dlq = DeadLetterQueue::new(Arc::new(task_store.kv_store().clone()), &shard_id.to_string());
                        dlq.enqueue(write.failed_records).await?;
                    }
                    if let Some(next_offset) = next_offset {
                        let mut stored =
                            task_store.get_shard(&shard_id).await?.ok_or_else(|| {
                                crate::error::DatspoolError::Internal(format!(
                                    "Shard {} not found",
                                    shard_id
                                ))
                            })?;
                        // Checkpoint writes are idempotent if a crash occurred after save but before cache ACK.
                        if stored.offset != next_offset {
                            stored.update_progress(source_count, 0, next_offset);
                            task_store.save_shard(&stored).await?;
                        }
                    }
                    Ok(())
                })
                .await?;
            if !delivered {
                break;
            }
        }
        if let Some(stored) = self.task_store.get_shard(&shard.shard_id).await? {
            offset = stored.offset;
        }

        loop {
            // Read batch from source. The source receives the committed cursor.
            let (batch, new_offset) = source.read_batch(&offset, self.batch_size).await?;
            let batch_size = batch.len();
            if batch_size == 0 {
                info!("No more data to read for shard {}", shard.shard_id);
                break;
            }

            // Persist payload + next cursor before attempting destination delivery.
            cache
                .enqueue_with_checkpoint(&batch, Some(new_offset.clone()), batch_size as u64)
                .await?;
            let task_store = self.task_store.clone();
            let shard_id = shard.shard_id;
            let sink_ref = &mut *sink;
            cache
                .deliver_one_with_checkpoint(|cached_batch, next_offset, source_count| async move {
                    let write = sink_ref.write_batch(cached_batch).await?;
                    if write.rejected_count > 0 || write.failed_count > write.failed_records.len() {
                        return Err(crate::error::DatspoolError::Internal(format!(
                            "batch delivery failed for shard {}: {} failed, {} rejected: {}",
                            shard_id,
                            write.failed_count,
                            write.rejected_count,
                            write.errors.join("; ")
                        )));
                    }
                    if !write.failed_records.is_empty() {
                        let dlq = DeadLetterQueue::new(
                            Arc::new(task_store.kv_store().clone()),
                            &shard_id.to_string(),
                        );
                        dlq.enqueue(write.failed_records).await?;
                    }
                    if let Some(next_offset) = next_offset {
                        let mut stored =
                            task_store.get_shard(&shard_id).await?.ok_or_else(|| {
                                crate::error::DatspoolError::Internal(format!(
                                    "Shard {} not found",
                                    shard_id
                                ))
                            })?;
                        if stored.offset != next_offset {
                            stored.update_progress(source_count, 0, next_offset);
                            task_store.save_shard(&stored).await?;
                        }
                    }
                    Ok(())
                })
                .await?;

            stats.docs_read += batch_size as u64;
            stats.docs_written += batch_size as u64;
            offset = new_offset;

            if stats
                .docs_read
                .is_multiple_of((self.batch_size as u64 * 10).max(1))
            {
                info!(
                    "Shard {} progress: {} docs read, {} written",
                    shard.shard_id, stats.docs_read, stats.docs_written
                );
            }
        }

        // Close connections
        source.close().await?;
        sink.close().await?;

        stats.end_time = Some(std::time::Instant::now());
        Ok(stats)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::index::TieredConfig;
    use crate::storage::rocksdb::RocksDbStore;
    use tempfile::tempdir;

    #[test]
    fn test_executor_creation() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let task_store = Arc::new(TaskStore::new(db.clone()));

        let index_config = TieredConfig::default();
        let index_store = Arc::new(RwLock::new(TieredIndexStore::new(
            index_config,
            Arc::new(db),
        )));

        let executor = ShardExecutor::new(task_store, index_store);
        assert_eq!(executor.batch_size, 1000);
    }

    #[tokio::test]
    async fn test_executor_with_batch_size() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let task_store = Arc::new(TaskStore::new(db.clone()));

        let index_config = TieredConfig::default();
        let index_store = Arc::new(RwLock::new(TieredIndexStore::new(
            index_config,
            Arc::new(db),
        )));

        let executor = ShardExecutor::new(task_store, index_store).with_batch_size(5000);
        assert_eq!(executor.batch_size, 5000);
    }
}
