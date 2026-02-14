// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Shard executor - runs data migration pipeline for a shard

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::error::Result;
use crate::pipeline::{RecordBatch, Source, Sink};
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
    pub fn new(task_store: Arc<TaskStore<S>>, index_store: Arc<RwLock<TieredIndexStore<S>>>) -> Self {
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

    /// Execute a shard migration
    pub async fn execute(&self, shard_id: &str) -> Result<ExecutionStats> {
        info!("Executing shard: {}", shard_id);
        
        // TODO: Implement full shard execution
        // Current limitation: We need a way to deserialize Source/Sink from shard config
        // This requires a factory pattern or enum-based dispatch
        
        // For now, return placeholder stats
        let stats = ExecutionStats {
            start_time: Some(std::time::Instant::now()),
            end_time: Some(std::time::Instant::now()),
            ..Default::default()
        };
        
        info!("Shard {} execution completed (placeholder)", shard_id);
        
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

        // Resume from last offset if available
        let mut offset = shard.offset.clone();

        loop {
            // Read batch from source
            let (batch, new_offset) = source.read_batch(&offset, self.batch_size).await?;
            let batch_size = batch.len();

            if batch_size == 0 {
                info!("No more data to read for shard {}", shard.shard_id);
                break;
            }

            stats.docs_read += batch_size as u64;

            // Index document IDs to tiered storage
            if let Err(e) = self.index_documents(&batch).await {
                warn!("Failed to index documents: {}", e);
                // Continue execution even if indexing fails (non-critical)
            } else {
                stats.docs_indexed += batch_size as u64;
            }

            // Write batch to sink
            sink.write_batch(batch).await?;
            stats.docs_written += batch_size as u64;

            // Update offset
            offset = new_offset.clone();

            // Report progress periodically (every 10 batches)
            if stats.docs_read % (self.batch_size as u64 * 10) == 0 {
                info!(
                    "Shard {} progress: {} docs read, {} written, {} indexed",
                    shard.shard_id, stats.docs_read, stats.docs_written, stats.docs_indexed
                );
            }

            // TODO: Checkpoint offset to TaskStore periodically
        }

        // Close connections
        source.close().await?;
        sink.close().await?;

        stats.end_time = Some(std::time::Instant::now());
        Ok(stats)
    }

    /// Index document IDs to tiered storage
    async fn index_documents(&self, _batch: &RecordBatch) -> Result<()> {
        // For now, we don't index individual documents
        // TODO: Implement chunking and batch indexing when needed
        // This would require task_id and chunk_id from shard metadata
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::rocksdb::RocksDbStore;
    use crate::storage::index::TieredConfig;
    use tempfile::tempdir;

    #[test]
    fn test_executor_creation() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let task_store = Arc::new(TaskStore::new(db.clone()));
        
        let index_config = TieredConfig::default();
        let index_store = Arc::new(RwLock::new(TieredIndexStore::new(index_config, Arc::new(db))));
        
        let executor = ShardExecutor::new(task_store, index_store);
        assert_eq!(executor.batch_size, 1000);
    }

    #[tokio::test]
    async fn test_executor_with_batch_size() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let task_store = Arc::new(TaskStore::new(db.clone()));
        
        let index_config = TieredConfig::default();
        let index_store = Arc::new(RwLock::new(TieredIndexStore::new(index_config, Arc::new(db))));
        
        let executor = ShardExecutor::new(task_store, index_store).with_batch_size(5000);
        assert_eq!(executor.batch_size, 5000);
    }
}
