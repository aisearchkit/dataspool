// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Shard executor - runs data migration pipeline for a shard

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::ShardLease;
use crate::drivers::factory::{SinkConfig, SourceConfig, create_sink, create_source};
use crate::error::{DatspoolError, Result};
use crate::pipeline::{DeadLetterQueue, LocalBatchCache, Sink, Source};
use crate::raft::{Command, RaftNode};
use crate::storage::index::TieredIndexStore;
use crate::storage::kv::KvStore;
use crate::task::shard::ShardMeta;
use crate::task::store::TaskStore;

/// Shard execution statistics
#[derive(Debug, Clone, Default)]
pub struct ExecutionStats {
    /// True when the source is a continuous stream and the shard must remain active.
    pub is_continuous: bool,
    /// True when execution stopped because its cancellation token was signalled.
    pub cancelled: bool,
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
    raft: Option<Arc<RaftNode>>,
    #[cfg(test)]
    crash_after_sink_before_delivery: Arc<std::sync::atomic::AtomicBool>,
}

impl<S: KvStore + Clone + 'static> Clone for ShardExecutor<S> {
    fn clone(&self) -> Self {
        Self {
            task_store: self.task_store.clone(),
            index_store: self.index_store.clone(),
            batch_size: self.batch_size,
            raft: self.raft.clone(),
            #[cfg(test)]
            crash_after_sink_before_delivery: self.crash_after_sink_before_delivery.clone(),
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
            raft: None,
            #[cfg(test)]
            crash_after_sink_before_delivery: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn new_with_raft(
        task_store: Arc<TaskStore<S>>,
        index_store: Arc<RwLock<TieredIndexStore<S>>>,
        raft: Arc<RaftNode>,
    ) -> Self {
        let mut executor = Self::new(task_store, index_store);
        executor.raft = Some(raft);
        executor
    }

    #[cfg(test)]
    fn inject_crash_after_sink_before_delivery_once(&self) {
        self.crash_after_sink_before_delivery
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn maybe_inject_crash_after_sink_before_delivery(&self) -> Result<()> {
        #[cfg(test)]
        if self
            .crash_after_sink_before_delivery
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(DatspoolError::Internal(
                "injected worker crash after destination commit and before Raft delivery record"
                    .into(),
            ));
        }
        Ok(())
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Load source/sink configurations from the owning task and execute a shard.
    pub async fn execute(&self, shard_id: &str) -> Result<ExecutionStats> {
        self.execute_with_cancellation(shard_id, CancellationToken::new())
            .await
    }

    /// Execute a shard until completion or until the supplied token is cancelled.
    /// Cancellation finishes any in-flight destination write, persists its checkpoint,
    /// closes both connections, and returns the shard to Pending for another worker.
    pub async fn execute_with_cancellation(
        &self,
        shard_id: &str,
        cancellation: CancellationToken,
    ) -> Result<ExecutionStats> {
        self.execute_internal(shard_id, None, cancellation).await
    }

    pub async fn execute_with_lease(
        &self,
        shard_id: &str,
        lease: ShardLease,
        cancellation: CancellationToken,
    ) -> Result<ExecutionStats> {
        self.execute_internal(shard_id, Some(lease), cancellation)
            .await
    }

    async fn execute_internal(
        &self,
        shard_id: &str,
        lease: Option<ShardLease>,
        cancellation: CancellationToken,
    ) -> Result<ExecutionStats> {
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
        if cancellation.is_cancelled() {
            self.release_lease(lease.as_ref()).await;
            return Ok(ExecutionStats {
                cancelled: true,
                ..Default::default()
            });
        }
        if let Some(lease) = lease.as_ref() {
            self.ensure_lease(lease).await?;
            if shard.fencing_token != lease.fencing_token {
                return Err(DatspoolError::Internal(
                    "Shard fencing token does not match claimed lease".into(),
                ));
            }
        }
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
        let compensation = match (
            task.metadata.get("_dataspool_federation_migration_id"),
            task.metadata.get("_dataspool_federation_generation"),
        ) {
            (Some(migration_id), Some(generation)) => {
                let generation = generation.parse::<u64>().map_err(|error| {
                    DatspoolError::Configuration(format!(
                        "Invalid federated migration generation: {error}"
                    ))
                })?;
                if migration_id.trim().is_empty() {
                    return Err(DatspoolError::Configuration(
                        "Invalid federated migration compensation identity".into(),
                    ));
                }
                Some((migration_id.clone(), generation))
            }
            (None, None) => None,
            _ => {
                return Err(DatspoolError::Configuration(
                    "Incomplete federated migration compensation metadata".into(),
                ));
            }
        };

        let execution = self
            .execute_pipeline_with_lease(
                &shard,
                source.as_mut(),
                sink.as_mut(),
                lease.clone(),
                cancellation.clone(),
                compensation,
            )
            .await;
        let control_stop = self
            .task_store
            .get_task(&shard.task_id)
            .await?
            .is_some_and(|task| {
                matches!(
                    task.state,
                    crate::types::TaskState::Pausing | crate::types::TaskState::Cancelled { .. }
                )
            });
        let should_requeue = matches!(&execution, Ok(stats) if stats.cancelled)
            || cancellation.is_cancelled()
            || control_stop;
        if should_requeue {
            self.release_lease(lease.as_ref()).await;
            return match execution {
                Ok(stats) => Ok(stats),
                Err(_) => Ok(ExecutionStats {
                    cancelled: true,
                    ..Default::default()
                }),
            };
        }
        let stats = execution?;
        if !stats.is_continuous {
            if let Some(lease) = lease.as_ref() {
                self.ensure_lease(lease).await?;
                let completed = self
                    .task_store
                    .get_shard(&shard_key)
                    .await?
                    .ok_or_else(|| {
                        DatspoolError::Internal(format!(
                            "Shard {shard_id} disappeared before completion"
                        ))
                    })?;
                let response = self
                    .raft
                    .as_ref()
                    .ok_or_else(|| {
                        DatspoolError::Internal("Raft is required for leased execution".into())
                    })?
                    .propose(Command::CompleteShard {
                        shard_id: shard_id.to_string(),
                        node_id: lease.node_id.to_string(),
                        fencing_token: lease.fencing_token,
                        issued_at_ms: chrono::Utc::now().timestamp_millis(),
                        total_records: completed.records_processed,
                    })
                    .await?;
                if !response.success {
                    return Err(DatspoolError::Internal(format!(
                        "Shard completion rejected: {}",
                        response.message
                    )));
                }
            } else {
                let mut completed =
                    self.task_store
                        .get_shard(&shard_key)
                        .await?
                        .ok_or_else(|| {
                            DatspoolError::Internal(format!(
                                "Shard {shard_id} disappeared during execution"
                            ))
                        })?;
                completed.complete(stats.docs_written);
                self.task_store.save_shard(&completed).await?;
            }
        }

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
        self.execute_pipeline_with_lease(shard, source, sink, None, CancellationToken::new(), None)
            .await
    }

    /// Execute with cooperative cancellation. A cancellation never interrupts
    /// an in-flight sink write or the checkpoint/ack sequence that follows it.
    pub async fn execute_pipeline_with_cancellation(
        &self,
        shard: &ShardMeta,
        source: &mut dyn Source,
        sink: &mut dyn Sink,
        cancellation: CancellationToken,
    ) -> Result<ExecutionStats> {
        self.execute_pipeline_with_lease(shard, source, sink, None, cancellation, None)
            .await
    }

    async fn execute_pipeline_with_lease(
        &self,
        shard: &ShardMeta,
        source: &mut dyn Source,
        sink: &mut dyn Sink,
        lease: Option<ShardLease>,
        cancellation: CancellationToken,
        compensation: Option<(String, u64)>,
    ) -> Result<ExecutionStats> {
        let mut stats = ExecutionStats {
            is_continuous: source.is_continuous(),
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
        let cache_store = Arc::new(self.task_store.kv_store().clone());
        let cache = if lease.is_some() {
            LocalBatchCache::new_replicated_for_shard(cache_store, 128, &shard.shard_id.to_string())
        } else {
            LocalBatchCache::new_for_shard(cache_store, 128, &shard.shard_id.to_string())
        };

        // Replay durable batches before reading new source data after a restart.
        while cache.pending().await? > 0 {
            if cancellation.is_cancelled() {
                stats.cancelled = true;
                break;
            }
            let task_store = self.task_store.clone();
            let executor = self.clone();
            let lease_for_delivery = lease.clone();
            let lease_for_ack = lease.clone();
            let compensation_for_delivery = compensation.clone();
            let executor_for_ack = self.clone();
            let shard_id = shard.shard_id;
            let shard_id_for_ack = shard.shard_id;
            let sink_ref = &mut *sink;
            let delivered = cache
                .deliver_one_with_checkpoint_and_ack(|batch, next_offset, source_count, batch_id| async move {
                    let batch_bytes = batch.records().iter().map(|record| record.to_bytes().map(|bytes| bytes.len() as u64))
                        .collect::<std::result::Result<Vec<_>, _>>()?.into_iter().sum();
                    executor.ensure_optional_lease(lease_for_delivery.as_ref()).await?;
                    if let Some(lease) = lease_for_delivery.as_ref() {
                        let manifest_key = crate::pipeline::cache::BatchManifest::key(&shard_id.to_string(), &batch_id);
                        if task_store.kv_store().get_cf(crate::storage::cf::META, &manifest_key).await?.is_none() {
                            let response = executor.raft.as_ref().ok_or_else(|| DatspoolError::Internal("Raft is required to backfill a legacy batch manifest".into()))?
                                .propose(Command::CacheBatch {
                                    shard_id: shard_id.to_string(), node_id: lease.node_id.to_string(),
                                    fencing_token: lease.fencing_token, issued_at_ms: chrono::Utc::now().timestamp_millis(),
                                    batch_id: batch_id.clone(), records: batch.records().to_vec(),
                                    next_offset: next_offset.clone(), source_count,
                                }).await?;
                            if !response.success { return Err(DatspoolError::Internal(format!("Legacy batch manifest backfill rejected: {}", response.message))); }
                        }
                    }
                    let write = if let Some((migration_id, generation)) = compensation_for_delivery.as_ref() {
                        sink_ref.write_batch_compensatable(batch, migration_id, *generation).await?
                    } else {
                        match lease_for_delivery.as_ref() {
                            Some(lease) => sink_ref.write_batch_fenced(
                                batch, lease.next_sink_version()?, &shard_id.to_string(), &batch_id,
                            ).await?,
                            None => sink_ref.write_batch(batch).await?,
                        }
                    };
                    executor.maybe_inject_crash_after_sink_before_delivery()?;
                    if write.rejected_count > 0 || write.failed_count > write.failed_records.len() {
                        return Err(crate::error::DatspoolError::Internal(format!(
                            "cached batch delivery still has retryable/unclassified failures: {} failed, {} rejected: {}",
                            write.failed_count, write.rejected_count, write.errors.join("; ")
                        )));
                    }
                    if !write.failed_records.is_empty() {
                        if let Some(lease) = lease_for_delivery.as_ref() {
                            let response = executor.raft.as_ref()
                                .ok_or_else(|| DatspoolError::Internal("Raft is required to persist dead letters".into()))?
                                .propose(Command::PersistDeadLetters {
                                    shard_id: shard_id.to_string(),
                                    node_id: lease.node_id.to_string(),
                                    fencing_token: lease.fencing_token,
                                    issued_at_ms: chrono::Utc::now().timestamp_millis(),
                                    failures: write.failed_records,
                                }).await?;
                            if !response.success {
                                return Err(DatspoolError::Internal(format!("Fenced DLQ persistence rejected: {}", response.message)));
                            }
                        } else {
                            let dlq = DeadLetterQueue::new(Arc::new(task_store.kv_store().clone()), &shard_id.to_string());
                            dlq.enqueue(write.failed_records).await?;
                        }
                    }
                    if let Some(lease) = lease_for_delivery.as_ref() {
                        let response = executor.raft.as_ref().ok_or_else(|| DatspoolError::Internal("Raft is required to record batch delivery".into()))?
                            .propose(Command::RecordBatchDelivery {
                                shard_id: shard_id.to_string(), node_id: lease.node_id.to_string(),
                                fencing_token: lease.fencing_token, issued_at_ms: chrono::Utc::now().timestamp_millis(),
                                batch_id: batch_id.clone(), success_count: source_count.saturating_sub(write.failed_count as u64),
                                failed_count: write.failed_count as u64, bytes_written: write.bytes_written,
                            }).await?;
                        if !response.success { return Err(DatspoolError::Internal(format!("Batch delivery manifest rejected: {}", response.message))); }
                    }
                    if let Some(next_offset) = next_offset {
                        executor.persist_progress(lease_for_delivery.as_ref(), &shard_id, source_count, batch_bytes, next_offset, Some(batch_id)).await?;
                    }
                    Ok(())
                }, move |cache_key| async move {
                    if let Some(lease) = lease_for_ack.as_ref() {
                        executor_for_ack.ensure_lease(lease).await?;
                        let response = executor_for_ack.raft.as_ref()
                            .ok_or_else(|| DatspoolError::Internal("Raft is required to acknowledge replicated cache".into()))?
                            .propose(Command::AckCacheBatch {
                                shard_id: shard_id_for_ack.to_string(),
                                node_id: lease.node_id.to_string(),
                                fencing_token: lease.fencing_token,
                                issued_at_ms: chrono::Utc::now().timestamp_millis(),
                                cache_key,
                            }).await?;
                        if !response.success {
                            return Err(DatspoolError::Internal(format!("Replicated cache acknowledgement rejected: {}", response.message)));
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
            if cancellation.is_cancelled() {
                stats.cancelled = true;
                break;
            }
            // Read batch from source. Cancellation may interrupt a source read,
            // but never interrupts the sink write/checkpoint/ack critical section.
            let read = tokio::select! {
                _ = cancellation.cancelled() => {
                    stats.cancelled = true;
                    break;
                },
                result = source.read_batch(&offset, self.batch_size) => result?,
            };
            let (batch, new_offset) = read;
            let batch_size = batch.len();
            if batch_size == 0 {
                if source.is_continuous() {
                    // Continuous composite sources can advance phases (snapshot -> CDC)
                    // without emitting a record. Persist that transition so a restart
                    // does not reopen the snapshot or recapture a later CDC watermark.
                    if new_offset != offset {
                        self.persist_progress(
                            lease.as_ref(),
                            &shard.shard_id,
                            0,
                            0,
                            new_offset.clone(),
                            None,
                        )
                        .await?;
                        offset = new_offset;
                    }
                    // A drained CDC feed is idle, not complete. Keep the source
                    // connected and poll again so writes arriving later are copied.
                    tokio::select! {
                        _ = cancellation.cancelled() => {
                            stats.cancelled = true;
                            break;
                        },
                        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                    }
                    continue;
                }
                info!("No more data to read for shard {}", shard.shard_id);
                break;
            }

            // Replicate payload + next cursor to the Raft state machine before
            // attempting any destination write. A new owner can then replay it.
            if let Some(lease) = lease.as_ref() {
                self.ensure_lease(lease).await?;
                cache.ensure_capacity().await?;
                let batch_id = lease.next_batch_id()?;
                let expected_batch_id = batch_id.clone();
                let response = self
                    .raft
                    .as_ref()
                    .ok_or_else(|| {
                        DatspoolError::Internal(
                            "Raft is required to replicate source batches".into(),
                        )
                    })?
                    .propose(Command::CacheBatch {
                        shard_id: shard.shard_id.to_string(),
                        node_id: lease.node_id.to_string(),
                        fencing_token: lease.fencing_token,
                        issued_at_ms: chrono::Utc::now().timestamp_millis(),
                        batch_id,
                        records: batch.records().to_vec(),
                        next_offset: Some(new_offset.clone()),
                        source_count: batch_size as u64,
                    })
                    .await?;
                if !response.success {
                    return Err(DatspoolError::Internal(format!(
                        "Raft rejected source batch replication: {}",
                        response.message
                    )));
                }
                cache
                    .wait_for_batch(&expected_batch_id, std::time::Duration::from_secs(15))
                    .await?;
            } else {
                cache
                    .enqueue_with_checkpoint(&batch, Some(new_offset.clone()), batch_size as u64)
                    .await?;
            }
            let task_store = self.task_store.clone();
            let executor = self.clone();
            let lease_for_delivery = lease.clone();
            let lease_for_ack = lease.clone();
            let compensation_for_delivery = compensation.clone();
            let executor_for_ack = self.clone();
            let shard_id = shard.shard_id;
            let shard_id_for_ack = shard.shard_id;
            let sink_ref = &mut *sink;
            cache
                .deliver_one_with_checkpoint_and_ack(
                    |cached_batch, next_offset, source_count, batch_id| async move {
                        let batch_bytes = cached_batch
                            .records()
                            .iter()
                            .map(|record| record.to_bytes().map(|bytes| bytes.len() as u64))
                            .collect::<std::result::Result<Vec<_>, _>>()?
                            .into_iter()
                            .sum::<u64>();
                        executor
                            .ensure_optional_lease(lease_for_delivery.as_ref())
                            .await?;
                        let write = if let Some((migration_id, generation)) =
                            compensation_for_delivery.as_ref()
                        {
                            sink_ref
                                .write_batch_compensatable(cached_batch, migration_id, *generation)
                                .await?
                        } else {
                            match lease_for_delivery.as_ref() {
                                Some(lease) => {
                                    sink_ref
                                        .write_batch_fenced(
                                            cached_batch,
                                            lease.next_sink_version()?,
                                            &shard_id.to_string(),
                                            &batch_id,
                                        )
                                        .await?
                                }
                                None => sink_ref.write_batch(cached_batch).await?,
                            }
                        };
                        executor.maybe_inject_crash_after_sink_before_delivery()?;
                        if write.rejected_count > 0
                            || write.failed_count > write.failed_records.len()
                        {
                            return Err(crate::error::DatspoolError::Internal(format!(
                                "batch delivery failed for shard {}: {} failed, {} rejected: {}",
                                shard_id,
                                write.failed_count,
                                write.rejected_count,
                                write.errors.join("; ")
                            )));
                        }
                        if !write.failed_records.is_empty() {
                            if let Some(lease) = lease_for_delivery.as_ref() {
                                let response = executor
                                    .raft
                                    .as_ref()
                                    .ok_or_else(|| {
                                        DatspoolError::Internal(
                                            "Raft is required to persist dead letters".into(),
                                        )
                                    })?
                                    .propose(Command::PersistDeadLetters {
                                        shard_id: shard_id.to_string(),
                                        node_id: lease.node_id.to_string(),
                                        fencing_token: lease.fencing_token,
                                        issued_at_ms: chrono::Utc::now().timestamp_millis(),
                                        failures: write.failed_records,
                                    })
                                    .await?;
                                if !response.success {
                                    return Err(DatspoolError::Internal(format!(
                                        "Fenced DLQ persistence rejected: {}",
                                        response.message
                                    )));
                                }
                            } else {
                                let dlq = DeadLetterQueue::new(
                                    Arc::new(task_store.kv_store().clone()),
                                    &shard_id.to_string(),
                                );
                                dlq.enqueue(write.failed_records).await?;
                            }
                        }
                        if let Some(lease) = lease_for_delivery.as_ref() {
                            let response = executor
                                .raft
                                .as_ref()
                                .ok_or_else(|| {
                                    DatspoolError::Internal(
                                        "Raft is required to record batch delivery".into(),
                                    )
                                })?
                                .propose(Command::RecordBatchDelivery {
                                    shard_id: shard_id.to_string(),
                                    node_id: lease.node_id.to_string(),
                                    fencing_token: lease.fencing_token,
                                    issued_at_ms: chrono::Utc::now().timestamp_millis(),
                                    batch_id: batch_id.clone(),
                                    success_count: source_count
                                        .saturating_sub(write.failed_count as u64),
                                    failed_count: write.failed_count as u64,
                                    bytes_written: write.bytes_written,
                                })
                                .await?;
                            if !response.success {
                                return Err(DatspoolError::Internal(format!(
                                    "Batch delivery manifest rejected: {}",
                                    response.message
                                )));
                            }
                        }
                        if let Some(next_offset) = next_offset {
                            executor
                                .persist_progress(
                                    lease_for_delivery.as_ref(),
                                    &shard_id,
                                    source_count,
                                    batch_bytes,
                                    next_offset,
                                    Some(batch_id),
                                )
                                .await?;
                        }
                        Ok(())
                    },
                    move |cache_key| async move {
                        if let Some(lease) = lease_for_ack.as_ref() {
                            executor_for_ack.ensure_lease(lease).await?;
                            let response = executor_for_ack
                                .raft
                                .as_ref()
                                .ok_or_else(|| {
                                    DatspoolError::Internal(
                                        "Raft is required to acknowledge replicated cache".into(),
                                    )
                                })?
                                .propose(Command::AckCacheBatch {
                                    shard_id: shard_id_for_ack.to_string(),
                                    node_id: lease.node_id.to_string(),
                                    fencing_token: lease.fencing_token,
                                    issued_at_ms: chrono::Utc::now().timestamp_millis(),
                                    cache_key,
                                })
                                .await?;
                            if !response.success {
                                return Err(DatspoolError::Internal(format!(
                                    "Replicated cache acknowledgement rejected: {}",
                                    response.message
                                )));
                            }
                        }
                        Ok(())
                    },
                )
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

        // Attempt both closes even if one driver reports a cleanup error.
        let source_close = source.close().await;
        let sink_close = sink.close().await;
        source_close?;
        sink_close?;

        stats.end_time = Some(std::time::Instant::now());
        Ok(stats)
    }

    async fn ensure_optional_lease(&self, lease: Option<&ShardLease>) -> Result<()> {
        if let Some(lease) = lease {
            self.ensure_lease(lease).await?;
        }
        Ok(())
    }

    async fn ensure_lease(&self, lease: &ShardLease) -> Result<()> {
        if self.raft.is_none() {
            return Err(DatspoolError::Internal(
                "Leased execution requires an OpenRaft node".into(),
            ));
        }
        let shard = self
            .task_store
            .get_shard(&lease.shard_id)
            .await?
            .ok_or_else(|| {
                DatspoolError::Internal(format!("Shard {} not found", lease.shard_id))
            })?;
        let task = self
            .task_store
            .get_task(&shard.task_id)
            .await?
            .ok_or_else(|| {
                DatspoolError::Internal(format!(
                    "Task {} not found for leased shard",
                    shard.task_id
                ))
            })?;
        if matches!(
            task.state,
            crate::types::TaskState::Cancelled { .. } | crate::types::TaskState::Failed { .. }
        ) {
            return Err(DatspoolError::Internal(format!(
                "Task {} is terminal; stopping shard execution",
                shard.task_id
            )));
        }
        let owner_matches = matches!(&shard.state, crate::types::ShardState::Running { node_id, .. } if node_id == &lease.node_id);
        if !owner_matches
            || shard.fencing_token != lease.fencing_token
            || shard
                .lease_expires_at
                .as_ref()
                .is_none_or(|expires| expires <= &chrono::Utc::now())
        {
            return Err(DatspoolError::Internal(format!(
                "Shard {} lease is no longer valid (fencing token {})",
                lease.shard_id, lease.fencing_token
            )));
        }
        Ok(())
    }

    async fn persist_progress(
        &self,
        lease: Option<&ShardLease>,
        shard_id: &crate::types::ShardId,
        batch_records: u64,
        batch_bytes: u64,
        offset: crate::pipeline::OffsetValue,
        batch_id: Option<String>,
    ) -> Result<()> {
        if let Some(lease) = lease {
            self.ensure_lease(lease).await?;
            let current =
                self.task_store.get_shard(shard_id).await?.ok_or_else(|| {
                    DatspoolError::Internal(format!("Shard {shard_id} not found"))
                })?;
            let offset_unchanged = current.offset == offset;
            let checkpoint_already_applied = if let Some(batch_id) = batch_id.as_ref() {
                let key =
                    crate::pipeline::cache::BatchManifest::key(&shard_id.to_string(), batch_id);
                self.task_store
                    .kv_store()
                    .get_cf(crate::storage::cf::META, &key)
                    .await?
                    .map(|bytes| {
                        serde_json::from_slice::<crate::pipeline::cache::BatchManifest>(&bytes)
                    })
                    .transpose()
                    .map_err(|error| DatspoolError::Serialization(error.to_string()))?
                    .is_some_and(|manifest| {
                        matches!(
                            manifest.status,
                            crate::pipeline::cache::BatchManifestStatus::Checkpointed
                        )
                    })
            } else {
                offset_unchanged
            };
            if checkpoint_already_applied {
                return self
                    .persist_progress_checkpoint_replay(lease, shard_id, offset, batch_id, &current)
                    .await;
            }
            if offset_unchanged && batch_id.is_none() {
                return Ok(());
            }
            let response = self
                .raft
                .as_ref()
                .ok_or_else(|| {
                    DatspoolError::Internal("Raft is required for lease checkpoint".into())
                })?
                .propose(Command::UpdateShardProgress {
                    shard_id: shard_id.to_string(),
                    node_id: lease.node_id.to_string(),
                    fencing_token: lease.fencing_token,
                    issued_at_ms: chrono::Utc::now().timestamp_millis(),
                    processed_records: current.records_processed.saturating_add(batch_records),
                    processed_bytes: current.bytes_processed.saturating_add(batch_bytes),
                    offset: serde_json::to_string(&offset)
                        .map_err(|e| DatspoolError::Serialization(e.to_string()))?,
                    batch_id,
                })
                .await?;
            if !response.success {
                return Err(DatspoolError::Internal(format!(
                    "Fenced checkpoint rejected: {}",
                    response.message
                )));
            }
        } else {
            let mut current =
                self.task_store.get_shard(shard_id).await?.ok_or_else(|| {
                    DatspoolError::Internal(format!("Shard {shard_id} not found"))
                })?;
            if current.offset != offset {
                current.update_progress(batch_records, batch_bytes, offset);
                self.task_store.save_shard(&current).await?;
            }
        }
        Ok(())
    }

    async fn persist_progress_checkpoint_replay(
        &self,
        lease: &ShardLease,
        shard_id: &crate::types::ShardId,
        offset: crate::pipeline::OffsetValue,
        batch_id: Option<String>,
        current: &crate::task::ShardMeta,
    ) -> Result<()> {
        let response = self
            .raft
            .as_ref()
            .ok_or_else(|| DatspoolError::Internal("Raft is required for lease checkpoint".into()))?
            .propose(Command::UpdateShardProgress {
                shard_id: shard_id.to_string(),
                node_id: lease.node_id.to_string(),
                fencing_token: lease.fencing_token,
                issued_at_ms: chrono::Utc::now().timestamp_millis(),
                processed_records: current.records_processed,
                processed_bytes: current.bytes_processed,
                offset: serde_json::to_string(&offset)
                    .map_err(|error| DatspoolError::Serialization(error.to_string()))?,
                batch_id,
            })
            .await?;
        if !response.success {
            return Err(DatspoolError::Internal(format!(
                "Fenced checkpoint replay rejected: {}",
                response.message
            )));
        }
        Ok(())
    }

    async fn release_lease(&self, lease: Option<&ShardLease>) {
        let Some(lease) = lease else {
            return;
        };
        let Some(raft) = self.raft.as_ref() else {
            return;
        };
        let result = raft
            .propose(Command::ReleaseShardLease {
                shard_id: lease.shard_id.to_string(),
                node_id: lease.node_id.to_string(),
                fencing_token: lease.fencing_token,
                issued_at_ms: chrono::Utc::now().timestamp_millis(),
            })
            .await;
        if let Err(error) = result {
            tracing::warn!(shard_id = %lease.shard_id, %error, "Unable to release shard lease; it will expire naturally");
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{OffsetValue, RecordBatch, WriteResult};
    use crate::storage::index::TieredConfig;
    use crate::storage::rocksdb::RocksDbStore;
    use crate::task::shard::ShardMeta;
    use crate::types::TaskId;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::tempdir;

    struct IdleContinuousSource {
        closed: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Source for IdleContinuousSource {
        async fn connect(&mut self) -> Result<()> {
            Ok(())
        }

        async fn read_batch(
            &mut self,
            offset: &OffsetValue,
            _batch_size: usize,
        ) -> Result<(RecordBatch, OffsetValue)> {
            Ok((RecordBatch::new(), offset.clone()))
        }

        fn is_continuous(&self) -> bool {
            true
        }

        async fn estimate_count(&self) -> Result<Option<u64>> {
            Ok(None)
        }

        async fn close(&mut self) -> Result<()> {
            self.closed.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn source_type(&self) -> &str {
            "idle-test"
        }
    }

    struct CloseTrackingSink {
        closed: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Sink for CloseTrackingSink {
        async fn connect(&mut self) -> Result<()> {
            Ok(())
        }

        async fn write_batch(&mut self, batch: RecordBatch) -> Result<WriteResult> {
            Ok(WriteResult::success(batch.len(), 0))
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            self.closed.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn sink_type(&self) -> &str {
            "close-tracking-test"
        }
    }

    struct OneBatchSource {
        emitted: bool,
    }

    #[async_trait::async_trait]
    impl Source for OneBatchSource {
        async fn connect(&mut self) -> Result<()> {
            Ok(())
        }
        async fn read_batch(
            &mut self,
            _offset: &OffsetValue,
            _batch_size: usize,
        ) -> Result<(RecordBatch, OffsetValue)> {
            if self.emitted {
                return Ok((RecordBatch::new(), OffsetValue::DocumentCount(1)));
            }
            self.emitted = true;
            let mut batch = RecordBatch::new();
            batch.push(crate::pipeline::Record::with_id(
                "doc-1",
                "crash-test",
                serde_json::json!({"v": 1}),
            ));
            Ok((batch, OffsetValue::DocumentCount(1)))
        }
        async fn estimate_count(&self) -> Result<Option<u64>> {
            Ok(Some(1))
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn source_type(&self) -> &str {
            "crash-test"
        }
    }

    #[derive(Clone, Default)]
    struct DurableTarget(
        Arc<std::sync::Mutex<std::collections::HashMap<String, serde_json::Value>>>,
    );

    #[async_trait::async_trait]
    impl Sink for DurableTarget {
        async fn connect(&mut self) -> Result<()> {
            Ok(())
        }
        async fn write_batch(&mut self, batch: RecordBatch) -> Result<WriteResult> {
            let mut docs = self.0.lock().unwrap();
            for record in batch.into_records() {
                docs.entry(record.id.unwrap()).or_insert(record.payload);
            }
            let count = docs.len();
            Ok(WriteResult::success(count, 16))
        }
        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn sink_type(&self) -> &str {
            "durable-target-test"
        }
    }

    #[tokio::test]
    async fn shard_executor_recovers_after_destination_commit_before_raft_delivery_ack() {
        use crate::agent::ShardClaimer;
        use crate::raft::{Command, RaftNode, default_raft_config};
        use crate::task::{ShardMeta, TaskMeta};
        use chrono::Utc;
        use openraft::BasicNode;
        use std::collections::BTreeMap;

        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path().join("app")).unwrap();
        let store = Arc::new(TaskStore::new(db.clone()));
        let token = "executor-crash-test-token-0123456789".to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let raft_path = dir.path().join("raft");
        let raft = RaftNode::new(
            1,
            &raft_path,
            (*store).clone(),
            default_raft_config(),
            token.clone(),
        )
        .await
        .unwrap();
        let router = crate::raft_router(raft.handle(), token.clone());
        let rpc = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        raft.initialize(BTreeMap::from([(1, BasicNode::new(address.to_string()))]))
            .await
            .unwrap();
        raft.wait_for_leader(std::time::Duration::from_secs(10))
            .await
            .unwrap();

        let task = TaskMeta::new("executor-crash-recovery".into(), "{}".into(), "{}".into());
        let shard = ShardMeta::new(task.task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        let created = raft
            .propose(Command::CreateTask {
                request_id: format!("create:{}", task.task_id),
                actor: "test".into(),
                task: Box::new(task),
                shards: vec![shard.clone()],
            })
            .await
            .unwrap();
        assert!(created.success, "{}", created.message);

        let claimer = ShardClaimer::new("worker-a".into(), store.clone(), Arc::new(raft.clone()))
            .with_lease_seconds(3)
            .unwrap();
        let first_lease = claimer.try_claim().await.unwrap().unwrap();
        let index_store = Arc::new(RwLock::new(TieredIndexStore::new(
            TieredConfig::default(),
            Arc::new(db.clone()),
        )));
        let first_executor = ShardExecutor::new_with_raft(
            store.clone(),
            index_store.clone(),
            Arc::new(raft.clone()),
        );
        first_executor.inject_crash_after_sink_before_delivery_once();
        let target = DurableTarget::default();
        let mut source = OneBatchSource { emitted: false };
        let mut sink = target.clone();
        let first_result = first_executor
            .execute_pipeline_with_lease(
                &shard,
                &mut source,
                &mut sink,
                Some(first_lease.clone()),
                CancellationToken::new(),
                None,
            )
            .await;
        assert!(
            first_result.is_err(),
            "fault injection must stop the worker before RecordBatchDelivery"
        );
        assert_eq!(
            target.0.lock().unwrap().len(),
            1,
            "destination commit must survive worker failure"
        );
        let before = store.get_shard(&shard_id).await.unwrap().unwrap();
        assert_eq!(
            before.records_processed, 0,
            "checkpoint must not advance before Raft delivery record"
        );
        let batch_id = format!("{:016x}-{:06x}", first_lease.fencing_token, 0);
        let manifest_key =
            crate::pipeline::cache::BatchManifest::key(&shard_id.to_string(), &batch_id);
        let manifest_bytes = store
            .kv_store()
            .get_cf(crate::storage::cf::META, &manifest_key)
            .await
            .unwrap()
            .unwrap();
        let manifest: crate::pipeline::cache::BatchManifest =
            serde_json::from_slice(&manifest_bytes).unwrap();
        assert_eq!(
            manifest.status,
            crate::pipeline::cache::BatchManifestStatus::Prepared
        );

        // Restart the Raft node and construct a fresh executor, preserving only durable storage + target data.
        drop(first_executor);
        rpc.abort();
        let _ = rpc.await;
        raft.shutdown().await.unwrap();
        drop(raft);
        let wait = (first_lease.expires_at - Utc::now())
            .to_std()
            .unwrap_or_default()
            + std::time::Duration::from_millis(100);
        tokio::time::sleep(wait).await;
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let restarted = RaftNode::new(
            1,
            &raft_path,
            (*store).clone(),
            default_raft_config(),
            token.clone(),
        )
        .await
        .unwrap();
        let router = crate::raft_router(restarted.handle(), token);
        let restarted_rpc = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        restarted
            .wait_for_leader(std::time::Duration::from_secs(10))
            .await
            .unwrap();
        let next_claimer = ShardClaimer::new(
            "worker-b".into(),
            store.clone(),
            Arc::new(restarted.clone()),
        )
        .with_lease_seconds(3)
        .unwrap();
        let next_lease = next_claimer.try_claim().await.unwrap().unwrap();
        assert!(next_lease.fencing_token > first_lease.fencing_token);

        let recovered_executor =
            ShardExecutor::new_with_raft(store.clone(), index_store, Arc::new(restarted.clone()));
        let mut resumed_source = OneBatchSource { emitted: true };
        let mut resumed_sink = target.clone();
        recovered_executor
            .execute_pipeline_with_lease(
                &store.get_shard(&shard_id).await.unwrap().unwrap(),
                &mut resumed_source,
                &mut resumed_sink,
                Some(next_lease),
                CancellationToken::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            target.0.lock().unwrap().len(),
            1,
            "replaying the committed batch must be idempotent"
        );
        let after = store.get_shard(&shard_id).await.unwrap().unwrap();
        assert_eq!(after.records_processed, 1);
        assert_eq!(after.offset, OffsetValue::DocumentCount(1));
        let manifest_bytes = store
            .kv_store()
            .get_cf(crate::storage::cf::META, &manifest_key)
            .await
            .unwrap()
            .unwrap();
        let manifest: crate::pipeline::cache::BatchManifest =
            serde_json::from_slice(&manifest_bytes).unwrap();
        assert_eq!(
            manifest.status,
            crate::pipeline::cache::BatchManifestStatus::Acknowledged
        );

        restarted_rpc.abort();
        let _ = restarted_rpc.await;
        restarted.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_closes_continuous_source_and_sink() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let task_store = Arc::new(TaskStore::new(db.clone()));
        let index_store = Arc::new(RwLock::new(TieredIndexStore::new(
            TieredConfig::default(),
            Arc::new(db),
        )));
        let executor = ShardExecutor::new(task_store, index_store);
        let source_closed = Arc::new(AtomicBool::new(false));
        let sink_closed = Arc::new(AtomicBool::new(false));
        let mut source = IdleContinuousSource {
            closed: source_closed.clone(),
        };
        let mut sink = CloseTrackingSink {
            closed: sink_closed.clone(),
        };
        let shard = ShardMeta::new(TaskId::new(), 0, "{}".into());
        let cancellation = CancellationToken::new();
        let cancel = cancellation.clone();

        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            cancel.cancel();
        });

        let stats = executor
            .execute_pipeline_with_cancellation(&shard, &mut source, &mut sink, cancellation)
            .await
            .unwrap();
        cancel_task.await.unwrap();

        assert!(stats.is_continuous);
        assert!(stats.cancelled);
        assert!(stats.end_time.is_some());
        assert!(source_closed.load(Ordering::SeqCst));
        assert!(sink_closed.load(Ordering::SeqCst));
    }

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
