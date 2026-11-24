// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Dataspool Contributors

//! Task and shard metadata storage

use crate::error::Result;
use crate::storage::{KvStore, cf};
use crate::task::{ShardMeta, TaskMeta};
use crate::types::{ShardId, ShardState, TaskId};

/// Immutable audit record written by the committed Raft state machine.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AuditEvent {
    pub log_index: u64,
    pub actor: String,
    pub request_id: String,
    pub action: String,
    pub resource_id: String,
    pub from_state: String,
    pub to_state: String,
    pub occurred_at_ms: i64,
}

/// Current task-level SLA status. Alerts are derived, never independently persisted.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SlaAlert {
    pub task_id: String,
    pub task_name: String,
    pub state: String,
    pub breached: bool,
    pub elapsed_seconds: u64,
    pub retries: u64,
    pub reasons: Vec<String>,
}

/// Cluster-wide operational counters derived from replicated task and node metadata.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct NodeLoadMetric {
    pub node_id: String,
    pub active_shards: u64,
    pub reserved_memory_mb: u64,
    pub capacity_shards: u64,
    pub memory_capacity_mb: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct OperationalMetrics {
    pub sla_tasks_at_risk: u64,
    pub sla_tasks_breached: u64,
    pub tasks_total: u64,
    pub tasks_pending: u64,
    pub tasks_running: u64,
    pub tasks_paused: u64,
    pub tasks_cancelling: u64,
    pub tasks_cancelled: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub shards_pending: u64,
    pub shards_running: u64,
    pub shards_completed: u64,
    pub shards_failed: u64,
    pub nodes_healthy: u64,
    pub nodes_degraded: u64,
    pub nodes_paused: u64,
    pub nodes_offline: u64,
    pub retries_total: u64,
    pub oldest_pending_seconds: u64,
    pub node_load: Vec<NodeLoadMetric>,
}

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

    /// Persist a replicated cluster node record.
    pub async fn save_node(&self, node: &crate::types::NodeMeta) -> Result<()> {
        let key = format!("node:{}", node.node_id);
        self.store
            .put_cf(cf::META, key.as_bytes(), &serde_json::to_vec(node)?)
            .await?;
        Ok(())
    }

    /// Read a replicated cluster node record.
    pub async fn get_node(
        &self,
        node_id: &crate::types::NodeId,
    ) -> Result<Option<crate::types::NodeMeta>> {
        let key = format!("node:{node_id}");
        self.store
            .get_cf(cf::META, key.as_bytes())
            .await?
            .map(|value| serde_json::from_slice(&value).map_err(Into::into))
            .transpose()
    }

    /// List all replicated cluster node records.
    pub async fn list_nodes(&self) -> Result<Vec<crate::types::NodeMeta>> {
        self.store
            .prefix_scan_cf(cf::META, b"node:")
            .await?
            .into_iter()
            .map(|(_, value)| serde_json::from_slice(&value).map_err(Into::into))
            .collect()
    }

    /// Evaluate task-level SLA policies from replicated task metadata.
    /// Supported metadata keys: sla.deadline_at (RFC3339),
    /// sla.max_duration_seconds, and sla.max_retries.
    pub async fn sla_alerts(&self) -> Result<Vec<SlaAlert>> {
        let now = chrono::Utc::now();
        let mut alerts = Vec::new();
        for task in self.list_tasks().await? {
            if matches!(
                task.state,
                crate::types::TaskState::Completed { .. }
                    | crate::types::TaskState::Cancelled { .. }
            ) {
                continue;
            }
            let elapsed = task
                .started_at
                .map(|started| (now - started).num_seconds().max(0) as u64)
                .unwrap_or(0);
            let retries: u64 = self
                .list_task_shards(&task.task_id)
                .await?
                .iter()
                .map(|shard| shard.retry_count)
                .sum();
            let mut reasons = Vec::new();
            let mut breached = false;
            if let Some(deadline) = task.metadata.get("sla.deadline_at")
                && let Ok(deadline) = chrono::DateTime::parse_from_rfc3339(deadline)
            {
                if now >= deadline.with_timezone(&chrono::Utc) {
                    reasons.push("deadline_exceeded".to_string());
                    breached = true;
                } else if (deadline.with_timezone(&chrono::Utc) - now).num_minutes() <= 15 {
                    reasons.push("deadline_at_risk".to_string());
                }
            }
            if let Some(limit) = task
                .metadata
                .get("sla.max_duration_seconds")
                .and_then(|v| v.parse::<u64>().ok())
            {
                if elapsed >= limit && limit > 0 {
                    reasons.push("duration_exceeded".to_string());
                    breached = true;
                } else if limit > elapsed && limit - elapsed <= 900 {
                    reasons.push("duration_at_risk".to_string());
                }
            }
            if let Some(limit) = task
                .metadata
                .get("sla.max_retries")
                .and_then(|v| v.parse::<u64>().ok())
            {
                if retries > limit {
                    reasons.push("retry_budget_exceeded".to_string());
                    breached = true;
                } else if retries == limit && limit > 0 {
                    reasons.push("retry_budget_exhausted".to_string());
                }
            }
            if !reasons.is_empty() {
                alerts.push(SlaAlert {
                    task_id: task.task_id.to_string(),
                    task_name: task.name,
                    state: format!("{:?}", task.state),
                    breached,
                    elapsed_seconds: elapsed,
                    retries,
                    reasons,
                });
            }
        }
        alerts.sort_by(|a, b| {
            b.breached
                .cmp(&a.breached)
                .then_with(|| a.task_id.cmp(&b.task_id))
        });
        Ok(alerts)
    }

    /// Read the newest audit events, newest first.
    pub async fn list_audit_events(&self, limit: usize) -> Result<Vec<AuditEvent>> {
        let mut events: Vec<AuditEvent> = self
            .store
            .prefix_scan_cf(cf::META, b"audit:")
            .await?
            .into_iter()
            .map(|(_, value)| serde_json::from_slice(&value))
            .collect::<std::result::Result<_, _>>()?;
        events.sort_by_key(|event| std::cmp::Reverse(event.log_index));
        events.truncate(limit.clamp(1, 1000));
        Ok(events)
    }

    /// Compute bounded-cardinality operational metrics from the replicated metadata.
    pub async fn operational_metrics(&self) -> Result<OperationalMetrics> {
        let now = chrono::Utc::now();
        let tasks = self.list_tasks().await?;
        let mut metrics = OperationalMetrics {
            tasks_total: tasks.len() as u64,
            ..Default::default()
        };
        let mut active_by_node = std::collections::HashMap::<String, u64>::new();
        let mut active_memory_by_node = std::collections::HashMap::<String, u64>::new();
        for task in tasks {
            match task.state {
                crate::types::TaskState::Pending => metrics.tasks_pending += 1,
                crate::types::TaskState::Running { .. } | crate::types::TaskState::Pausing => {
                    metrics.tasks_running += 1
                }
                crate::types::TaskState::Paused => metrics.tasks_paused += 1,
                crate::types::TaskState::Canceling => metrics.tasks_cancelling += 1,
                crate::types::TaskState::Cancelled { .. } => metrics.tasks_cancelled += 1,
                crate::types::TaskState::Completed { .. } => metrics.tasks_completed += 1,
                crate::types::TaskState::Failed { .. } => metrics.tasks_failed += 1,
            }
            for shard in self.list_task_shards(&task.task_id).await? {
                metrics.retries_total = metrics.retries_total.saturating_add(shard.retry_count);
                if let ShardState::Running { node_id, .. } = &shard.state
                    && shard
                        .lease_expires_at
                        .as_ref()
                        .is_some_and(|expiry| expiry > &now)
                {
                    *active_by_node.entry(node_id.to_string()).or_default() += 1;
                    let reserved_memory = task
                        .metadata
                        .get("requires.memory_mb")
                        .and_then(|value| value.parse::<u64>().ok())
                        .unwrap_or_default();
                    let active_memory = active_memory_by_node
                        .entry(node_id.to_string())
                        .or_default();
                    *active_memory = active_memory.saturating_add(reserved_memory);
                }
                match shard.state {
                    ShardState::Pending => {
                        metrics.shards_pending += 1;
                        let age = now
                            .timestamp_millis()
                            .saturating_sub(
                                chrono::DateTime::<chrono::Utc>::from(shard.created_at)
                                    .timestamp_millis(),
                            )
                            .max(0) as u64
                            / 1000;
                        metrics.oldest_pending_seconds = metrics.oldest_pending_seconds.max(age);
                    }
                    ShardState::Running { .. } => metrics.shards_running += 1,
                    ShardState::Completed { .. } => metrics.shards_completed += 1,
                    ShardState::Failed { .. } => metrics.shards_failed += 1,
                }
            }
        }
        for node in self.list_nodes().await? {
            let capacity_shards = node
                .tags
                .get("capacity.max_concurrent")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_default();
            let memory_capacity_mb = node
                .tags
                .get("capacity.memory_mb")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_default();
            metrics.node_load.push(NodeLoadMetric {
                node_id: node.node_id.to_string(),
                active_shards: active_by_node
                    .remove(node.node_id.as_str())
                    .unwrap_or_default(),
                reserved_memory_mb: active_memory_by_node
                    .remove(node.node_id.as_str())
                    .unwrap_or_default(),
                capacity_shards,
                memory_capacity_mb,
            });
            match node.status {
                crate::types::NodeStatus::Healthy => metrics.nodes_healthy += 1,
                crate::types::NodeStatus::Degraded => metrics.nodes_degraded += 1,
                crate::types::NodeStatus::Paused => metrics.nodes_paused += 1,
                crate::types::NodeStatus::Offline => metrics.nodes_offline += 1,
            }
        }
        Ok(metrics)
    }

    /// Read the committed payload associated with a control-plane idempotency key.
    pub async fn get_control_request(&self, request_id: &str) -> Result<Option<String>> {
        let key = format!("control_request:{request_id}");
        self.store
            .get_cf(cf::META, key.as_bytes())
            .await?
            .map(|value| {
                String::from_utf8(value.to_vec())
                    .map_err(|error| crate::error::DatspoolError::Serialization(error.to_string()))
            })
            .transpose()
    }

    /// Atomically commit a control request, its state mutations, and optional audit event.
    /// This prevents a replay from observing an idempotency key without its audit record.
    pub async fn save_control_batch(
        &self,
        request_id: &str,
        payload: &str,
        records: Vec<(String, Vec<u8>)>,
        audit: Option<AuditEvent>,
    ) -> Result<()> {
        let mut batch = crate::storage::WriteBatch::new();
        batch.put_cf(
            cf::META,
            format!("control_request:{request_id}"),
            payload.as_bytes().to_vec(),
        );
        for (key, value) in records {
            batch.put_cf(cf::META, key, value);
        }
        if let Some(event) = audit {
            batch.put_cf(
                cf::META,
                format!("audit:{:020}", event.log_index),
                serde_json::to_vec(&event)?,
            );
        }
        self.store.write_batch(batch).await?;
        Ok(())
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

    /// Rebuild every persisted task aggregate from the committed shard records.
    pub async fn rebuild_task_aggregates(&self) -> Result<usize> {
        let mut rebuilt = Vec::new();
        for mut task in self.list_tasks().await? {
            let shards = self.list_task_shards(&task.task_id).await?;
            let at = shards
                .iter()
                .map(|shard| shard.updated_at)
                .max()
                .map(chrono::DateTime::<chrono::Utc>::from)
                .unwrap_or_else(|| chrono::DateTime::<chrono::Utc>::from(task.updated_at));
            task.refresh_from_shards(&shards, at);
            rebuilt.push(task);
        }
        if rebuilt.is_empty() {
            return Ok(0);
        }
        let mut batch = crate::storage::WriteBatch::new();
        for task in &rebuilt {
            batch.put_cf(
                cf::META,
                format!("task:{}", task.task_id),
                serde_json::to_vec(task)?,
            );
        }
        self.store.write_batch(batch).await?;
        Ok(rebuilt.len())
    }

    /// Read a task and the exact shard set used to compute its live aggregate.
    /// Returning both from one shard scan keeps the API aggregate and shard rows aligned.
    pub async fn get_task_with_aggregate_and_shards(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<(TaskMeta, Vec<ShardMeta>)>> {
        let Some(mut task) = self.get_task(task_id).await? else {
            return Ok(None);
        };
        let shards = self.list_task_shards(task_id).await?;
        task.refresh_from_shards(&shards, chrono::Utc::now());
        Ok(Some((task, shards)))
    }

    /// Read a task together with a freshly computed aggregate, without trusting a cached view.
    pub async fn get_task_with_aggregate(&self, task_id: &TaskId) -> Result<Option<TaskMeta>> {
        Ok(self
            .get_task_with_aggregate_and_shards(task_id)
            .await?
            .map(|(task, _)| task))
    }

    /// Delete task metadata
    pub async fn delete_task(&self, task_id: &TaskId) -> Result<()> {
        let key = format!("task:{}", task_id);
        self.store.delete_cf(cf::META, key.as_bytes()).await?;
        Ok(())
    }

    /// Atomically persist a shard mutation, task aggregate, and optional lifecycle audit event.
    pub(crate) async fn save_shard_and_task_with_audit(
        &self,
        shard: &ShardMeta,
        task: &TaskMeta,
        audit: Option<AuditEvent>,
    ) -> Result<()> {
        let shard_key = format!("shard:{}", shard.shard_id);
        let task_shard_key = format!("task_shard:{}:{}", shard.task_id, shard.shard_id);
        let task_key = format!("task:{}", task.task_id);
        let mut batch = crate::storage::WriteBatch::new();
        batch.put_cf(cf::META, shard_key, serde_json::to_vec(shard)?);
        batch.put_cf(cf::META, task_shard_key, bytes::Bytes::new());
        batch.put_cf(cf::META, task_key, serde_json::to_vec(task)?);
        if let Some(event) = audit {
            batch.put_cf(
                cf::META,
                format!("audit:{:020}", event.log_index),
                serde_json::to_vec(&event)?,
            );
        }
        self.store.write_batch(batch).await?;
        Ok(())
    }

    /// Atomically persist a node transition, affected shards, and rebuilt task aggregates.
    pub(crate) async fn save_node_shards_tasks(
        &self,
        node: &crate::types::NodeMeta,
        shards: &[ShardMeta],
        tasks: &[TaskMeta],
    ) -> Result<()> {
        let mut batch = crate::storage::WriteBatch::new();
        batch.put_cf(
            cf::META,
            format!("node:{}", node.node_id),
            serde_json::to_vec(node)?,
        );
        for shard in shards {
            batch.put_cf(
                cf::META,
                format!("shard:{}", shard.shard_id),
                serde_json::to_vec(shard)?,
            );
            batch.put_cf(
                cf::META,
                format!("task_shard:{}:{}", shard.task_id, shard.shard_id),
                bytes::Bytes::new(),
            );
        }
        for task in tasks {
            batch.put_cf(
                cf::META,
                format!("task:{}", task.task_id),
                serde_json::to_vec(task)?,
            );
        }
        self.store.write_batch(batch).await?;
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
        self.store
            .put_cf(crate::storage::cf::CACHE, &key, &value)
            .await?;
        Ok(())
    }

    pub(crate) async fn cache_batch_count(&self, shard_id: &str) -> Result<usize> {
        let prefix = crate::pipeline::cache::LocalBatchCache::<S>::prefix_for_shard(shard_id);
        Ok(self
            .store
            .prefix_scan_cf(crate::storage::cf::CACHE, &prefix)
            .await?
            .len())
    }

    /// Return the oldest unacknowledged replicated batch key for FIFO enforcement.
    pub(crate) async fn first_cache_batch_key(&self, shard_id: &str) -> Result<Option<Vec<u8>>> {
        let prefix = crate::pipeline::cache::LocalBatchCache::<S>::prefix_for_shard(shard_id);
        Ok(self
            .store
            .prefix_scan_cf(crate::storage::cf::CACHE, &prefix)
            .await?
            .into_iter()
            .next()
            .map(|(key, _)| key.to_vec()))
    }

    /// Highest acknowledged Raft batch ID for a shard. Batch IDs are fixed-width
    /// hexadecimal fencing-generation/sequence pairs, so lexical order is numeric.
    pub(crate) async fn cache_ack_watermark(&self, shard_id: &str) -> Result<Option<String>> {
        let key = format!("shard_cache_ack:{shard_id}");
        self.store
            .get_cf(cf::META, key.as_bytes())
            .await?
            .map(|value| {
                String::from_utf8(value.to_vec())
                    .map_err(|e| crate::error::DatspoolError::Serialization(e.to_string()))
            })
            .transpose()
    }

    /// Atomically remove a replicated batch and advance its per-shard ack watermark.
    pub(crate) async fn ack_cache_batch(
        &self,
        shard_id: &str,
        batch_id: &str,
        key: &[u8],
    ) -> Result<()> {
        let watermark_key = format!("shard_cache_ack:{shard_id}");
        let mut batch = crate::storage::WriteBatch::new();
        batch.delete_cf(crate::storage::cf::CACHE, key.to_vec());
        batch.put_cf(
            cf::META,
            watermark_key,
            bytes::Bytes::copy_from_slice(batch_id.as_bytes()),
        );
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
        Ok(self
            .store
            .get_cf(cf::META, b"system:bootstrap_complete")
            .await?
            .is_some())
    }

    pub(crate) async fn mark_bootstrap_complete(&self) -> Result<()> {
        self.store
            .put_cf(cf::META, b"system:bootstrap_complete", b"1")
            .await?;
        Ok(())
    }

    /// Remove the old node-local queue after its entries have been imported
    /// into the replicated Raft cache.
    pub(crate) async fn clear_legacy_cache(&self) -> Result<usize> {
        let mut batch = crate::storage::WriteBatch::new();
        let entries = self
            .store
            .prefix_scan_cf(crate::storage::cf::CACHE, b"migration-batch/")
            .await?;
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
        Ok(self
            .store
            .get_cf(cf::META, b"system:raft_applied_state")
            .await?
            .map(|value| value.to_vec()))
    }

    /// Persist the applied marker in the same RocksDB as replicated application data.
    pub(crate) async fn set_raft_applied_state(&self, value: &[u8]) -> Result<()> {
        self.store
            .put_cf(cf::META, b"system:raft_applied_state", value)
            .await?;
        Ok(())
    }

    /// Export all shard/task state and durable migration queues for a Raft snapshot.
    pub async fn export_snapshot(&self) -> Result<TaskStoreSnapshot> {
        let metadata = self
            .store
            .prefix_scan_cf(crate::storage::cf::META, b"")
            .await?
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        let offsets = self
            .store
            .prefix_scan_cf(crate::storage::cf::OFFSET, b"")
            .await?
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        let cache = self
            .store
            .prefix_scan_cf(crate::storage::cf::CACHE, b"")
            .await?
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        let dead_letters = self
            .store
            .prefix_scan_cf(crate::storage::cf::DEAD_LETTER, b"")
            .await?
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        Ok(TaskStoreSnapshot {
            metadata,
            offsets,
            cache,
            dead_letters,
        })
    }

    /// Atomically replace replicated application data from an installed Raft snapshot.
    pub async fn replace_snapshot(&self, snapshot: TaskStoreSnapshot) -> Result<()> {
        let mut batch = crate::storage::WriteBatch::new();
        for column_family in [
            crate::storage::cf::META,
            crate::storage::cf::OFFSET,
            crate::storage::cf::CACHE,
            crate::storage::cf::DEAD_LETTER,
        ] {
            for (key, _) in self.store.prefix_scan_cf(column_family, b"").await? {
                batch.delete_cf(column_family, key);
            }
        }
        for (key, value) in snapshot.metadata {
            batch.put_cf(crate::storage::cf::META, key, value);
        }
        for (key, value) in snapshot.offsets {
            batch.put_cf(crate::storage::cf::OFFSET, key, value);
        }
        for (key, value) in snapshot.cache {
            batch.put_cf(crate::storage::cf::CACHE, key, value);
        }
        for (key, value) in snapshot.dead_letters {
            batch.put_cf(crate::storage::cf::DEAD_LETTER, key, value);
        }
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
    async fn snapshot_restore_rebuilds_task_aggregate_from_committed_shards() {
        let (source, _source_temp) = create_test_store().await;
        let task = TaskMeta::new("aggregate-restore".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        source.save_task(&task).await.unwrap();

        let mut shard = ShardMeta::new(task_id, 0, "{}".into());
        shard.last_owner_node_id = Some(crate::types::NodeId::new("worker-recovered"));
        shard.records_processed = 17;
        shard.bytes_processed = 4096;
        shard.offset = crate::pipeline::OffsetValue::DocumentCount(17);
        shard.complete_at(17, chrono::Utc::now());
        let shard_id = shard.shard_id;
        source.save_shard(&shard).await.unwrap();

        // Simulate an older/incomplete cached aggregate in a snapshot. Shard
        // records remain authoritative and the restored member must rebuild it.
        let snapshot = source.export_snapshot().await.unwrap();
        let (restored, _restored_temp) = create_test_store().await;
        restored.replace_snapshot(snapshot).await.unwrap();
        assert_eq!(restored.rebuild_task_aggregates().await.unwrap(), 1);

        let task = restored
            .get_task_with_aggregate(&task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.aggregate.total_shards, 1);
        assert_eq!(task.aggregate.completed_shards, 1);
        assert_eq!(task.aggregate.docs_processed, 17);
        assert_eq!(task.aggregate.bytes_processed, 4096);
        assert_eq!(task.aggregate.checkpoints[0].shard_id, shard_id);
        assert_eq!(task.aggregate.nodes["worker-recovered"].completed_shards, 1);
        assert!(matches!(
            task.state,
            crate::types::TaskState::Completed { .. }
        ));
    }

    #[tokio::test]
    async fn raft_applied_marker_is_included_in_snapshots() {
        let (store, _temp) = create_test_store().await;
        let marker = serde_json::to_vec(&serde_json::json!({"last_applied": 42})).unwrap();
        store.set_raft_applied_state(&marker).await.unwrap();
        store.mark_bootstrap_complete().await.unwrap();
        store.kv_store().flush_durable().await.unwrap();

        let snapshot = store.export_snapshot().await.unwrap();
        assert!(
            snapshot
                .metadata
                .iter()
                .any(|(key, value)| { key == b"system:raft_applied_state" && value == &marker })
        );

        let (restored, _restored_temp) = create_test_store().await;
        restored.replace_snapshot(snapshot).await.unwrap();
        assert_eq!(restored.raft_applied_state().await.unwrap(), Some(marker));
        assert!(restored.bootstrap_complete().await.unwrap());
    }

    #[tokio::test]
    async fn sla_alerts_report_deadline_duration_and_retry_breaches() {
        let (store, _temp) = create_test_store().await;
        let mut task = TaskMeta::new("sla-task".into(), "{}".into(), "{}".into());
        task.start(1);
        task.metadata.insert(
            "sla.deadline_at".into(),
            (chrono::Utc::now() - chrono::Duration::minutes(1)).to_rfc3339(),
        );
        task.metadata
            .insert("sla.max_duration_seconds".into(), "1".into());
        task.metadata.insert("sla.max_retries".into(), "0".into());
        let task_id = task.task_id;
        store.save_task(&task).await.unwrap();
        let mut shard = ShardMeta::new(task_id, 0, "{}".into());
        shard.retry_count = 1;
        store.save_shard(&shard).await.unwrap();

        let alerts = store.sla_alerts().await.unwrap();
        assert_eq!(alerts.len(), 1);
        assert!(alerts[0].breached);
        assert!(alerts[0].reasons.contains(&"deadline_exceeded".to_string()));
        assert!(alerts[0].reasons.contains(&"duration_exceeded".to_string()));
        assert!(
            alerts[0]
                .reasons
                .contains(&"retry_budget_exceeded".to_string())
        );
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
