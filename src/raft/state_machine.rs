// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Deterministic application of committed Raft commands.

use serde::{Deserialize, Serialize};
use tracing::info;

use super::Command;
use crate::error::{DatspoolError, Result};
use crate::storage::KvStore;
use crate::task::{ShardMeta, TaskStore};
use crate::types::{NodeId, ShardId, ShardState};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommandResponse {
    pub success: bool,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

pub struct MetaStateMachine<S: KvStore> {
    task_store: TaskStore<S>,
}

struct CacheBatchRequest {
    shard_id: String,
    node_id: String,
    fencing_token: u64,
    issued_at_ms: i64,
    batch_id: String,
    records: Vec<crate::pipeline::Record>,
    next_offset: Option<crate::pipeline::OffsetValue>,
    source_count: u64,
}

struct ShardProgressRequest {
    shard_id: String,
    node_id: String,
    fencing_token: u64,
    issued_at_ms: i64,
    processed_records: u64,
    processed_bytes: u64,
    offset: String,
}

impl<S: KvStore + Clone> MetaStateMachine<S> {
    pub fn new(store: S) -> Self {
        Self {
            task_store: TaskStore::new(store),
        }
    }

    pub async fn apply_command(&mut self, cmd: Command) -> Result<CommandResponse> {
        info!("Applying committed command: {}", cmd.description());
        match cmd {
            Command::CreateTask { task, shards } => self.apply_create_task(*task, shards).await,
            Command::ClaimShard {
                shard_id,
                node_id,
                claim_id,
                issued_at_ms,
                lease_expires_at_ms,
            } => {
                self.apply_claim_shard(
                    shard_id,
                    node_id,
                    claim_id,
                    issued_at_ms,
                    lease_expires_at_ms,
                )
                .await
            }
            Command::ImportLegacyCache {
                shard_id,
                batch_id,
                records,
                next_offset,
                source_count,
            } => {
                self.apply_import_legacy_cache(
                    shard_id,
                    batch_id,
                    records,
                    next_offset,
                    source_count,
                )
                .await
            }
            Command::BootstrapComplete => {
                self.task_store.mark_bootstrap_complete().await?;
                Ok(accepted(
                    "Raft bootstrap completed",
                    serde_json::json!({"ready": true}),
                ))
            }
            Command::ReleaseShardLease {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
            } => {
                self.apply_release_lease(shard_id, node_id, fencing_token, issued_at_ms)
                    .await
            }
            Command::CacheBatch {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                batch_id,
                records,
                next_offset,
                source_count,
            } => {
                self.apply_cache_batch(CacheBatchRequest {
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    batch_id,
                    records,
                    next_offset,
                    source_count,
                })
                .await
            }
            Command::PersistDeadLetters {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                failures,
            } => {
                self.apply_dead_letters(shard_id, node_id, fencing_token, issued_at_ms, failures)
                    .await
            }
            Command::AckCacheBatch {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                cache_key,
            } => {
                self.apply_ack_cache_batch(
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    cache_key,
                )
                .await
            }
            Command::RenewShardLease {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                lease_expires_at_ms,
            } => {
                self.apply_renew_lease(
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    lease_expires_at_ms,
                )
                .await
            }
            Command::UpdateShardProgress {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                processed_records,
                processed_bytes,
                offset,
            } => {
                self.apply_progress(ShardProgressRequest {
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    processed_records,
                    processed_bytes,
                    offset,
                })
                .await
            }
            Command::CompleteShard {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                total_records,
            } => {
                self.apply_complete(
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    total_records,
                )
                .await
            }
            Command::FailShard {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                error_message,
            } => {
                self.apply_fail(
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    error_message,
                )
                .await
            }
            Command::RegisterNode {
                node_id,
                address,
                tags,
                issued_at_ms,
            } => {
                self.apply_register_node(node_id, address, tags, issued_at_ms)
                    .await
            }
            Command::UpdateNodeHeartbeat {
                node_id,
                issued_at_ms,
            } => self.apply_node_heartbeat(node_id, issued_at_ms).await,
            Command::MarkNodeOffline {
                node_id,
                issued_at_ms,
                heartbeat_timeout_ms,
            } => {
                self.apply_mark_node_offline(node_id, issued_at_ms, heartbeat_timeout_ms)
                    .await
            }
            Command::UpdateTaskState { .. }
            | Command::DeleteTask { .. }
            | Command::UpdateConfig { .. } => Ok(rejected("Command is not implemented")),
        }
    }

    async fn apply_register_node(
        &self,
        node_id: String,
        address: String,
        tags: std::collections::HashMap<String, String>,
        issued_at_ms: i64,
    ) -> Result<CommandResponse> {
        if node_id.trim().is_empty() || address.trim().is_empty() {
            return Ok(rejected("Node ID and advertised address are required"));
        }
        let existing = self
            .task_store
            .get_node(&NodeId::new(node_id.clone()))
            .await?;
        let registered_at_ms = existing
            .as_ref()
            .map(|node| node.registered_at_ms)
            .unwrap_or(issued_at_ms);
        let node = crate::types::NodeMeta {
            node_id: NodeId::new(node_id.clone()),
            address,
            tags,
            registered_at_ms,
            last_heartbeat_ms: issued_at_ms,
            status: crate::types::NodeStatus::Healthy,
        };
        self.task_store.save_node(&node).await?;
        Ok(accepted(
            "Node registered",
            serde_json::json!({"node_id": node_id, "status": "healthy"}),
        ))
    }

    async fn apply_node_heartbeat(
        &self,
        node_id: String,
        issued_at_ms: i64,
    ) -> Result<CommandResponse> {
        let id = NodeId::new(node_id.clone());
        let Some(mut node) = self.task_store.get_node(&id).await? else {
            return Ok(rejected("Node must register before sending heartbeats"));
        };
        if issued_at_ms < node.last_heartbeat_ms {
            return Ok(rejected("Heartbeat timestamp cannot move backwards"));
        }
        node.last_heartbeat_ms = issued_at_ms;
        node.status = crate::types::NodeStatus::Healthy;
        self.task_store.save_node(&node).await?;
        Ok(accepted(
            "Node heartbeat recorded",
            serde_json::json!({"node_id": node_id, "last_heartbeat_ms": issued_at_ms}),
        ))
    }

    async fn apply_mark_node_offline(
        &self,
        node_id: String,
        issued_at_ms: i64,
        heartbeat_timeout_ms: i64,
    ) -> Result<CommandResponse> {
        if heartbeat_timeout_ms < 1_000 {
            return Ok(rejected("Heartbeat timeout must be at least one second"));
        }
        let id = NodeId::new(node_id.clone());
        let Some(mut node) = self.task_store.get_node(&id).await? else {
            return Ok(rejected("Node not found"));
        };
        if node.status == crate::types::NodeStatus::Offline {
            return Ok(accepted(
                "Node already offline",
                serde_json::json!({"node_id": node_id, "requeued_shards": 0}),
            ));
        }
        let cutoff = issued_at_ms.saturating_sub(heartbeat_timeout_ms);
        if node.last_heartbeat_ms > cutoff {
            return Ok(rejected(
                "Node heartbeat is still within the failure-detection window",
            ));
        }
        node.status = crate::types::NodeStatus::Offline;
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid node-offline timestamp".into()))?;
        let mut requeued_shards = Vec::new();
        let mut affected_tasks = Vec::new();
        for mut task in self.task_store.list_tasks().await? {
            let mut shards = self.task_store.list_task_shards(&task.task_id).await?;
            let mut changed = false;
            for shard in &mut shards {
                let owned = matches!(&shard.state, ShardState::Running { node_id: owner, .. } if owner == &id);
                if owned {
                    shard.set_state_at(ShardState::Pending, at);
                    requeued_shards.push(shard.clone());
                    changed = true;
                }
            }
            if changed {
                task.refresh_from_shards(&shards, at);
                affected_tasks.push(task);
            }
        }
        let requeued = requeued_shards.len();
        self.task_store
            .save_node_shards_tasks(&node, &requeued_shards, &affected_tasks)
            .await?;
        Ok(accepted(
            "Node marked offline; owned shards atomically returned to pending",
            serde_json::json!({"node_id": node_id, "requeued_shards": requeued}),
        ))
    }

    async fn persist_shard_and_aggregate(&self, shard: &ShardMeta) -> Result<()> {
        let mut task = self
            .task_store
            .get_task(&shard.task_id)
            .await?
            .ok_or_else(|| {
                DatspoolError::Internal(format!(
                    "Task {} missing for shard {}",
                    shard.task_id, shard.shard_id
                ))
            })?;
        let mut shards = self.task_store.list_task_shards(&shard.task_id).await?;
        if let Some(existing) = shards
            .iter_mut()
            .find(|existing| existing.shard_id == shard.shard_id)
        {
            *existing = shard.clone();
        } else {
            shards.push(shard.clone());
        }
        task.refresh_from_shards(
            &shards,
            chrono::DateTime::from_timestamp_millis(
                shard
                    .updated_at
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(i64::MAX as u128) as i64,
            )
            .unwrap_or_else(chrono::Utc::now),
        );
        self.task_store.save_shard_and_task(shard, &task).await
    }

    async fn apply_create_task(
        &self,
        mut task: crate::task::TaskMeta,
        shards: Vec<ShardMeta>,
    ) -> Result<CommandResponse> {
        let task_id = task.task_id;
        for shard in &shards {
            if shard.task_id != task_id {
                return Ok(rejected("Shard belongs to a different task"));
            }
        }
        // Re-applying a committed entry after a crash must be safe.
        if self.task_store.get_task(&task_id).await?.is_none() {
            self.task_store.save_task(&task).await?;
        }
        for shard in &shards {
            match self.task_store.get_shard(&shard.shard_id).await? {
                None => self.task_store.save_shard(shard).await?,
                Some(existing)
                    if matches!(&existing.state, ShardState::Running { .. })
                        && existing.lease_expires_at.is_none()
                        && existing.lease_id.is_none()
                        && matches!(&shard.state, ShardState::Pending) =>
                {
                    self.task_store.save_shard(shard).await?;
                }
                Some(_) => {}
            }
        }
        let committed_shards = self.task_store.list_task_shards(&task_id).await?;
        let aggregate_at = chrono::DateTime::<chrono::Utc>::from(task.updated_at);
        task.refresh_from_shards(&committed_shards, aggregate_at);
        self.task_store.save_task(&task).await?;
        Ok(accepted(
            "Task and shards created",
            serde_json::json!({"task_id": task_id, "shards": shards.len()}),
        ))
    }

    async fn apply_claim_shard(
        &self,
        shard_id: String,
        node_id: String,
        claim_id: String,
        issued_at_ms: i64,
        lease_expires_at_ms: i64,
    ) -> Result<CommandResponse> {
        let shard_id = parse_shard_id(&shard_id)?;
        if self
            .task_store
            .get_node(&NodeId::new(node_id.clone()))
            .await?
            .is_some_and(|node| node.status == crate::types::NodeStatus::Offline)
        {
            return Ok(rejected(
                "Offline nodes cannot claim shards until they re-register",
            ));
        }
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        let previous_expiry = shard
            .lease_expires_at
            .as_ref()
            .filter(|expires| expires.timestamp_millis() > issued_at_ms)
            .copied();
        let same_claim = matches!(&shard.state, ShardState::Running { node_id: owner, .. } if owner.as_str() == node_id)
            && shard.lease_id.as_deref() == Some(claim_id.as_str());
        if let Some(previous_expiry) = previous_expiry.filter(|_| same_claim) {
            return Ok(accepted(
                "Shard claim already committed",
                serde_json::json!({
                    "shard_id": shard_id.to_string(), "node_id": node_id,
                    "claim_id": claim_id, "fencing_token": shard.fencing_token,
                    "lease_expires_at_ms": previous_expiry.timestamp_millis()
                }),
            ));
        }
        let reclaimable = match &shard.state {
            ShardState::Pending => true,
            ShardState::Running { started_at, .. } => shard
                .lease_expires_at
                .as_ref()
                .map(|expires| expires.timestamp_millis() <= issued_at_ms)
                .unwrap_or_else(|| {
                    started_at.timestamp_millis().saturating_add(30_000) <= issued_at_ms
                }),
            _ => false,
        };
        if !reclaimable {
            return Ok(rejected("Shard is not pending or its lease is still valid"));
        }
        if lease_expires_at_ms <= issued_at_ms {
            return Ok(rejected("Lease expiration must be later than claim time"));
        }
        let expires = chrono::DateTime::from_timestamp_millis(lease_expires_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid lease expiration".into()))?;
        let issued_at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid claim timestamp".into()))?;
        let fencing_token = match shard.start_with_lease(
            NodeId::new(node_id.clone()),
            issued_at,
            expires,
            claim_id.clone(),
        ) {
            Some(token) => token,
            None => return Ok(rejected("Shard fencing token exhausted")),
        };
        self.persist_shard_and_aggregate(&shard).await?;
        Ok(accepted(
            "Shard claimed",
            serde_json::json!({
                "shard_id": shard_id.to_string(), "node_id": node_id, "claim_id": claim_id,
                "fencing_token": fencing_token, "lease_expires_at_ms": lease_expires_at_ms
            }),
        ))
    }

    async fn apply_release_lease(
        &self,
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
    ) -> Result<CommandResponse> {
        let shard_id = parse_shard_id(&shard_id)?;
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid release timestamp".into()))?;
        if matches!(&shard.state, ShardState::Pending) && shard.fencing_token == fencing_token {
            return Ok(accepted(
                "Shard lease already released",
                serde_json::json!({"fencing_token": fencing_token}),
            ));
        }
        if !lease_matches(&shard, &node_id, fencing_token, at) {
            return Ok(rejected("Lease release rejected by fencing token"));
        }
        shard.set_state_at(ShardState::Pending, at);
        self.persist_shard_and_aggregate(&shard).await?;
        Ok(accepted(
            "Shard lease released",
            serde_json::json!({"fencing_token": fencing_token}),
        ))
    }

    async fn apply_import_legacy_cache(
        &self,
        shard_id: String,
        batch_id: String,
        records: Vec<crate::pipeline::Record>,
        next_offset: Option<crate::pipeline::OffsetValue>,
        source_count: u64,
    ) -> Result<CommandResponse> {
        if self.task_store.bootstrap_complete().await? {
            return Ok(rejected("Legacy cache import is closed after bootstrap"));
        }
        let parsed_shard_id = parse_shard_id(&shard_id)?;
        let shard = match self.task_store.get_shard(&parsed_shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        if !matches!(&shard.state, ShardState::Pending)
            || shard.lease_expires_at.is_some()
            || shard.lease_id.is_some()
        {
            return Ok(rejected(
                "Legacy cache can only be imported before the shard is first claimed",
            ));
        }
        if !valid_batch_id(&batch_id) || !batch_id.starts_with("0000000000000000-") {
            return Ok(rejected("Invalid legacy cache import ID"));
        }
        let key = crate::pipeline::cache::LocalBatchCache::<S>::key_for_shard(&shard_id, &batch_id);
        let entry = crate::pipeline::cache::CacheEntry {
            records,
            next_offset,
            source_count,
        };
        if let Some(existing) = self
            .task_store
            .kv_store()
            .get_cf(crate::storage::cf::CACHE, &key)
            .await?
        {
            let existing: crate::pipeline::cache::CacheEntry = serde_json::from_slice(&existing)?;
            return if existing == entry {
                Ok(accepted(
                    "Legacy cache entry already imported",
                    serde_json::json!({"batch_id": batch_id}),
                ))
            } else {
                Ok(rejected(
                    "Legacy cache import ID was reused with a different payload",
                ))
            };
        }
        if self.task_store.cache_batch_count(&shard_id).await? >= 128 {
            return Ok(rejected("Replicated shard cache is full during bootstrap"));
        }
        self.task_store
            .save_cache_batch(&shard_id, &batch_id, &entry)
            .await?;
        Ok(accepted(
            "Legacy cache entry imported",
            serde_json::json!({"batch_id": batch_id}),
        ))
    }

    async fn apply_cache_batch(&self, request: CacheBatchRequest) -> Result<CommandResponse> {
        let CacheBatchRequest {
            shard_id,
            node_id,
            fencing_token,
            issued_at_ms,
            batch_id,
            records,
            next_offset,
            source_count,
        } = request;
        let parsed_shard_id = parse_shard_id(&shard_id)?;
        let shard = match self.task_store.get_shard(&parsed_shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid cache batch timestamp".into()))?;
        if !lease_matches(&shard, &node_id, fencing_token, at) {
            return Ok(rejected("Cache batch rejected by fencing token"));
        }
        if !valid_batch_id(&batch_id) {
            return Ok(rejected(
                "Invalid cache batch ID; expected a 16-hex generation and 6-hex sequence",
            ));
        }
        if self
            .task_store
            .cache_ack_watermark(&shard_id)
            .await?
            .as_deref()
            .is_some_and(|watermark| batch_id.as_str() <= watermark)
        {
            return Ok(accepted(
                "Source batch was already acknowledged",
                serde_json::json!({"batch_id": batch_id}),
            ));
        }
        let key = crate::pipeline::cache::LocalBatchCache::<S>::key_for_shard(&shard_id, &batch_id);
        let entry = crate::pipeline::cache::CacheEntry {
            records,
            next_offset,
            source_count,
        };
        if let Some(existing) = self
            .task_store
            .kv_store()
            .get_cf(crate::storage::cf::CACHE, &key)
            .await?
        {
            let existing: crate::pipeline::cache::CacheEntry = serde_json::from_slice(&existing)?;
            if existing == entry {
                return Ok(accepted(
                    "Source batch already replicated",
                    serde_json::json!({"batch_id": batch_id}),
                ));
            }
            return Ok(rejected(
                "Cache batch ID was reused with a different payload",
            ));
        }
        if self.task_store.cache_batch_count(&shard_id).await? >= 128 {
            return Ok(rejected("Replicated shard cache is full"));
        }
        self.task_store
            .save_cache_batch(&shard_id, &batch_id, &entry)
            .await?;
        Ok(accepted(
            "Source batch replicated",
            serde_json::json!({"batch_id": batch_id}),
        ))
    }

    async fn apply_dead_letters(
        &self,
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        failures: Vec<crate::pipeline::FailedRecord>,
    ) -> Result<CommandResponse> {
        let parsed_shard_id = parse_shard_id(&shard_id)?;
        let shard = match self.task_store.get_shard(&parsed_shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid dead-letter timestamp".into()))?;
        if !lease_matches(&shard, &node_id, fencing_token, at) {
            return Ok(rejected(
                "Dead-letter persistence rejected by fencing token",
            ));
        }
        let count = failures.len();
        let queue = crate::pipeline::DeadLetterQueue::new(
            std::sync::Arc::new(self.task_store.kv_store().clone()),
            &shard_id,
        );
        queue
            .enqueue_at(failures, issued_at_ms.max(0) as u64)
            .await?;
        Ok(accepted(
            "Dead-letter records replicated",
            serde_json::json!({"count": count}),
        ))
    }

    async fn apply_ack_cache_batch(
        &self,
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        cache_key: String,
    ) -> Result<CommandResponse> {
        let parsed_shard_id = parse_shard_id(&shard_id)?;
        let expected_prefix =
            crate::pipeline::cache::LocalBatchCache::<S>::prefix_for_shard(&shard_id);
        let batch_id = match cache_key
            .as_bytes()
            .strip_prefix(expected_prefix.as_slice())
        {
            Some(value) => match std::str::from_utf8(value) {
                Ok(value) if valid_batch_id(value) => value.to_string(),
                _ => return Ok(rejected("Invalid replicated cache key")),
            },
            None => return Ok(rejected("Cache key does not belong to this shard")),
        };
        if self
            .task_store
            .cache_ack_watermark(&shard_id)
            .await?
            .as_deref()
            .is_some_and(|watermark| batch_id.as_str() <= watermark)
        {
            return Ok(accepted(
                "Replicated cache entry already acknowledged",
                serde_json::json!({"batch_id": batch_id}),
            ));
        }
        let shard = match self.task_store.get_shard(&parsed_shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms).ok_or_else(|| {
            DatspoolError::Serialization("Invalid cache acknowledgement timestamp".into())
        })?;
        if !lease_matches(&shard, &node_id, fencing_token, at) {
            return Ok(rejected("Cache acknowledgement rejected by fencing token"));
        }
        match self.task_store.first_cache_batch_key(&shard_id).await? {
            Some(first_key) if first_key == cache_key.as_bytes() => {}
            Some(_) => return Ok(rejected("Cache batches must be acknowledged in FIFO order")),
            None => return Ok(rejected("Replicated cache batch does not exist")),
        }
        self.task_store
            .ack_cache_batch(&shard_id, &batch_id, cache_key.as_bytes())
            .await?;
        Ok(accepted(
            "Replicated cache entry acknowledged",
            serde_json::json!({"cache_key": cache_key}),
        ))
    }

    async fn apply_renew_lease(
        &self,
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        lease_expires_at_ms: i64,
    ) -> Result<CommandResponse> {
        let shard_id = parse_shard_id(&shard_id)?;
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        let owner_matches = matches!(&shard.state, ShardState::Running { node_id: owner, .. } if owner.as_str() == node_id);
        let lease_valid_at_issue = shard
            .lease_expires_at
            .as_ref()
            .is_some_and(|expires| expires.timestamp_millis() > issued_at_ms);
        let new_expiry = chrono::DateTime::from_timestamp_millis(lease_expires_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid lease expiration".into()))?;
        if owner_matches
            && shard.fencing_token == fencing_token
            && shard.lease_expires_at.as_ref() == Some(&new_expiry)
        {
            return Ok(accepted(
                "Lease renewal already committed",
                serde_json::json!({"fencing_token": fencing_token, "lease_expires_at_ms": lease_expires_at_ms}),
            ));
        }
        let issued_at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid renewal timestamp".into()))?;
        if !owner_matches
            || !lease_valid_at_issue
            || !shard.renew_lease_at(&NodeId::new(node_id), fencing_token, new_expiry, issued_at)
        {
            return Ok(rejected(
                "Lease renewal rejected: owner, fencing token, or lease deadline is stale",
            ));
        }
        self.task_store.save_shard(&shard).await?;
        Ok(accepted(
            "Lease renewed",
            serde_json::json!({"fencing_token": fencing_token, "lease_expires_at_ms": lease_expires_at_ms}),
        ))
    }

    async fn apply_progress(&self, request: ShardProgressRequest) -> Result<CommandResponse> {
        let ShardProgressRequest {
            shard_id,
            node_id,
            fencing_token,
            issued_at_ms,
            processed_records,
            processed_bytes,
            offset,
        } = request;
        let shard_id = parse_shard_id(&shard_id)?;
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        if !lease_matches(
            &shard,
            &node_id,
            fencing_token,
            chrono::DateTime::from_timestamp_millis(issued_at_ms)
                .ok_or_else(|| DatspoolError::Serialization("Invalid command timestamp".into()))?,
        ) {
            return Ok(rejected("Progress rejected by fencing token"));
        }
        let offset = serde_json::from_str(&offset)
            .map_err(|e| DatspoolError::Serialization(e.to_string()))?;
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid progress timestamp".into()))?;
        shard.set_progress_at(processed_records, processed_bytes, offset, at);
        self.persist_shard_and_aggregate(&shard).await?;
        Ok(accepted(
            "Shard progress updated",
            serde_json::json!({"fencing_token": fencing_token}),
        ))
    }

    async fn apply_complete(
        &self,
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        total_records: u64,
    ) -> Result<CommandResponse> {
        let shard_id = parse_shard_id(&shard_id)?;
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        if matches!(&shard.state, ShardState::Completed { .. })
            && shard.fencing_token == fencing_token
        {
            return Ok(accepted(
                "Shard completion already committed",
                serde_json::json!({"fencing_token": fencing_token}),
            ));
        }
        if !lease_matches(
            &shard,
            &node_id,
            fencing_token,
            chrono::DateTime::from_timestamp_millis(issued_at_ms)
                .ok_or_else(|| DatspoolError::Serialization("Invalid command timestamp".into()))?,
        ) {
            return Ok(rejected("Completion rejected by fencing token"));
        }
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid completion timestamp".into()))?;
        shard.complete_at(total_records, at);
        self.persist_shard_and_aggregate(&shard).await?;
        Ok(accepted(
            "Shard completed",
            serde_json::json!({"fencing_token": fencing_token}),
        ))
    }

    async fn apply_fail(
        &self,
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        error_message: String,
    ) -> Result<CommandResponse> {
        let shard_id = parse_shard_id(&shard_id)?;
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        if matches!(&shard.state, ShardState::Failed { .. }) && shard.fencing_token == fencing_token
        {
            return Ok(accepted(
                "Shard failure already committed",
                serde_json::json!({"fencing_token": fencing_token}),
            ));
        }
        if !lease_matches(
            &shard,
            &node_id,
            fencing_token,
            chrono::DateTime::from_timestamp_millis(issued_at_ms)
                .ok_or_else(|| DatspoolError::Serialization("Invalid command timestamp".into()))?,
        ) {
            return Ok(rejected("Failure update rejected by fencing token"));
        }
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid failure timestamp".into()))?;
        shard.fail_at(error_message, 1, at);
        self.persist_shard_and_aggregate(&shard).await?;
        Ok(accepted(
            "Shard marked failed",
            serde_json::json!({"fencing_token": fencing_token}),
        ))
    }

    pub async fn validate_lease(
        &self,
        shard_id: &ShardId,
        node_id: &str,
        fencing_token: u64,
    ) -> Result<bool> {
        let Some(shard) = self.task_store.get_shard(shard_id).await? else {
            return Ok(false);
        };
        Ok(lease_matches(
            &shard,
            node_id,
            fencing_token,
            chrono::Utc::now(),
        ))
    }
}

fn lease_matches(
    shard: &ShardMeta,
    node_id: &str,
    fencing_token: u64,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    shard.fencing_token == fencing_token
        && shard
            .lease_expires_at
            .as_ref()
            .is_some_and(|expires| expires > &now)
        && matches!(&shard.state, ShardState::Running { node_id: owner, .. } if owner.as_str() == node_id)
}

fn valid_batch_id(value: &str) -> bool {
    let Some((generation, sequence)) = value.split_once('-') else {
        return false;
    };
    generation.len() == 16
        && sequence.len() == 6
        && generation.bytes().all(|byte| byte.is_ascii_hexdigit())
        && sequence.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn parse_shard_id(value: &str) -> Result<ShardId> {
    value
        .parse()
        .map_err(|e: uuid::Error| DatspoolError::Serialization(e.to_string()))
}

fn accepted(message: &str, data: serde_json::Value) -> CommandResponse {
    CommandResponse {
        success: true,
        message: message.to_string(),
        data: Some(data),
    }
}

fn rejected(message: &str) -> CommandResponse {
    CommandResponse {
        success: false,
        message: message.to_string(),
        data: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RocksDbStore;
    use crate::task::{ShardMeta, TaskMeta};
    use tempfile::tempdir;

    #[tokio::test]
    async fn legacy_cache_import_is_idempotent_and_closed_after_bootstrap() {
        use crate::pipeline::{OffsetValue, Record};
        use serde_json::json;

        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let task = TaskMeta::new("bootstrap-import".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let created = sm
            .apply_command(Command::CreateTask {
                task: Box::new(task),
                shards: vec![shard],
            })
            .await
            .unwrap();
        assert!(created.success);

        let import = Command::ImportLegacyCache {
            shard_id: shard_id.to_string(),
            batch_id: "0000000000000000-000000".into(),
            records: vec![Record::with_id(
                "legacy-1",
                "elasticsearch",
                json!({"v": 1}),
            )],
            next_offset: Some(OffsetValue::DocumentCount(1)),
            source_count: 1,
        };
        assert!(sm.apply_command(import.clone()).await.unwrap().success);
        assert!(sm.apply_command(import.clone()).await.unwrap().success);
        assert_eq!(
            store
                .cache_batch_count(&shard_id.to_string())
                .await
                .unwrap(),
            1
        );

        assert!(
            sm.apply_command(Command::BootstrapComplete)
                .await
                .unwrap()
                .success
        );
        let mut late_import = import;
        if let Command::ImportLegacyCache { batch_id, .. } = &mut late_import {
            *batch_id = "0000000000000000-000001".into();
        }
        assert!(!sm.apply_command(late_import).await.unwrap().success);
        assert_eq!(
            store
                .cache_batch_count(&shard_id.to_string())
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn node_heartbeat_failure_atomically_requeues_owned_shards_and_fences_old_owner() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let task = TaskMeta::new("node-failure".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        store.save_task(&task).await.unwrap();
        store.save_shard(&shard).await.unwrap();
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let now = chrono::Utc::now().timestamp_millis();

        assert!(
            sm.apply_command(Command::RegisterNode {
                node_id: "worker-a".into(),
                address: "127.0.0.1:9300".into(),
                tags: std::collections::HashMap::new(),
                issued_at_ms: now,
            })
            .await
            .unwrap()
            .success
        );
        let claim = sm
            .apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                claim_id: "claim-a".into(),
                issued_at_ms: now,
                lease_expires_at_ms: now + 60_000,
            })
            .await
            .unwrap();
        assert!(claim.success);

        let heartbeat = sm
            .apply_command(Command::UpdateNodeHeartbeat {
                node_id: "worker-a".into(),
                issued_at_ms: now + 15_000,
            })
            .await
            .unwrap();
        assert!(heartbeat.success);
        let too_early = sm
            .apply_command(Command::MarkNodeOffline {
                node_id: "worker-a".into(),
                issued_at_ms: now + 20_000,
                heartbeat_timeout_ms: 10_000,
            })
            .await
            .unwrap();
        assert!(!too_early.success);

        let offline = sm
            .apply_command(Command::MarkNodeOffline {
                node_id: "worker-a".into(),
                issued_at_ms: now + 26_000,
                heartbeat_timeout_ms: 10_000,
            })
            .await
            .unwrap();
        assert!(offline.success);
        assert_eq!(offline.data.unwrap()["requeued_shards"], 1);
        assert_eq!(
            store
                .get_node(&NodeId::new("worker-a"))
                .await
                .unwrap()
                .unwrap()
                .status,
            crate::types::NodeStatus::Offline
        );
        let requeued = store.get_shard(&shard_id).await.unwrap().unwrap();
        assert!(matches!(requeued.state, ShardState::Pending));
        assert_eq!(
            requeued.fencing_token,
            crate::task::shard::FENCING_TOKEN_BASE + 1
        );

        let offline_claim = sm
            .apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                claim_id: "stale-worker-claim".into(),
                issued_at_ms: now + 27_000,
                lease_expires_at_ms: now + 57_000,
            })
            .await
            .unwrap();
        assert!(!offline_claim.success);

        let reassigned = sm
            .apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-b".into(),
                claim_id: "claim-b".into(),
                issued_at_ms: now + 27_000,
                lease_expires_at_ms: now + 57_000,
            })
            .await
            .unwrap();
        assert!(reassigned.success);
        assert_eq!(
            reassigned.data.unwrap()["fencing_token"],
            crate::task::shard::FENCING_TOKEN_BASE + 2
        );

        let stale_renewal = sm
            .apply_command(Command::RenewShardLease {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token: crate::task::shard::FENCING_TOKEN_BASE + 1,
                issued_at_ms: now + 28_000,
                lease_expires_at_ms: now + 58_000,
            })
            .await
            .unwrap();
        assert!(!stale_renewal.success);
    }

    #[tokio::test]
    async fn task_aggregate_tracks_progress_completion_failure_and_node_contributions() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let task = TaskMeta::new("distributed-aggregate".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let shard_a = ShardMeta::new(task_id, 0, "{}".into());
        let shard_b = ShardMeta::new(task_id, 1, "{}".into());
        let shard_a_id = shard_a.shard_id;
        let shard_b_id = shard_b.shard_id;
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let now = chrono::Utc::now().timestamp_millis();

        let created = sm
            .apply_command(Command::CreateTask {
                task: Box::new(task),
                shards: vec![shard_a, shard_b],
            })
            .await
            .unwrap();
        assert!(created.success);

        for (shard_id, node_id) in [(&shard_a_id, "worker-a"), (&shard_b_id, "worker-b")] {
            let claim = sm
                .apply_command(Command::ClaimShard {
                    shard_id: shard_id.to_string(),
                    node_id: node_id.into(),
                    claim_id: format!("claim-{node_id}"),
                    issued_at_ms: now,
                    lease_expires_at_ms: now + 60_000,
                })
                .await
                .unwrap();
            assert!(claim.success, "{}", claim.message);
        }

        let progress = sm
            .apply_command(Command::UpdateShardProgress {
                shard_id: shard_a_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token: crate::task::shard::FENCING_TOKEN_BASE + 1,
                issued_at_ms: now + 1,
                processed_records: 10,
                processed_bytes: 2048,
                offset: serde_json::to_string(&crate::pipeline::OffsetValue::DocumentCount(10))
                    .unwrap(),
            })
            .await
            .unwrap();
        assert!(progress.success);

        let completed = sm
            .apply_command(Command::CompleteShard {
                shard_id: shard_a_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token: crate::task::shard::FENCING_TOKEN_BASE + 1,
                issued_at_ms: now + 2,
                total_records: 10,
            })
            .await
            .unwrap();
        assert!(completed.success);

        let failed_command = Command::FailShard {
            shard_id: shard_b_id.to_string(),
            node_id: "worker-b".into(),
            fencing_token: crate::task::shard::FENCING_TOKEN_BASE + 1,
            issued_at_ms: now + 3,
            error_message: "destination rejected records".into(),
        };
        let failed = sm.apply_command(failed_command.clone()).await.unwrap();
        assert!(failed.success);
        assert!(sm.apply_command(failed_command).await.unwrap().success);

        let aggregate = store
            .get_task_with_aggregate(&task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(aggregate.aggregate.total_shards, 2);
        assert_eq!(aggregate.aggregate.pending_shards, 0);
        assert_eq!(aggregate.aggregate.running_shards, 0);
        assert_eq!(aggregate.aggregate.completed_shards, 1);
        assert_eq!(aggregate.aggregate.failed_shards, 1);
        assert_eq!(aggregate.completed_shards, 1);
        assert_eq!(aggregate.failed_shards, 1);
        assert_eq!(aggregate.aggregate.docs_processed, 10);
        assert_eq!(aggregate.aggregate.bytes_processed, 2048);
        assert!(aggregate.aggregate.throughput_docs_per_sec > 0.0);
        assert!(aggregate.aggregate.throughput_bytes_per_sec > 0.0);
        assert!(aggregate.aggregate.elapsed_ms > 0);
        let duration_ms = aggregate.aggregate.elapsed_ms;
        let rebuilt = store
            .get_task_with_aggregate(&task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rebuilt.aggregate.elapsed_ms, duration_ms);
        assert_eq!(aggregate.aggregate.nodes["worker-a"].completed_shards, 1);
        assert_eq!(aggregate.aggregate.nodes["worker-a"].completed_shards, 1);
        assert_eq!(aggregate.aggregate.nodes["worker-b"].failed_shards, 1);
        assert_eq!(aggregate.aggregate.checkpoints.len(), 2);
        assert!(matches!(
            aggregate.state,
            crate::types::TaskState::Failed { .. }
        ));
    }

    #[tokio::test]
    async fn stale_fencing_token_cannot_renew_or_complete_reclaimed_shard() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let task = TaskMeta::new("fence-test".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        store.save_task(&task).await.unwrap();
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        store.save_shard(&shard).await.unwrap();
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let now = chrono::Utc::now().timestamp_millis();

        let first = sm
            .apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "node-a".into(),
                claim_id: "claim-a".into(),
                issued_at_ms: now,
                lease_expires_at_ms: now + 30_000,
            })
            .await
            .unwrap();
        assert!(first.success);
        assert_eq!(
            first.data.as_ref().unwrap()["fencing_token"],
            crate::task::shard::FENCING_TOKEN_BASE + 1
        );

        let reclaimed = sm
            .apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "node-b".into(),
                claim_id: "claim-b".into(),
                issued_at_ms: now + 31_000,
                lease_expires_at_ms: now + 61_000,
            })
            .await
            .unwrap();
        assert!(reclaimed.success);
        assert_eq!(
            reclaimed.data.as_ref().unwrap()["fencing_token"],
            crate::task::shard::FENCING_TOKEN_BASE + 2
        );

        let stale = sm
            .apply_command(Command::CompleteShard {
                shard_id: shard_id.to_string(),
                node_id: "node-a".into(),
                fencing_token: crate::task::shard::FENCING_TOKEN_BASE + 1,
                issued_at_ms: now + 32_000,
                total_records: 100,
            })
            .await
            .unwrap();
        assert!(!stale.success);

        let current = store.get_shard(&shard_id).await.unwrap().unwrap();
        assert_eq!(
            current.fencing_token,
            crate::task::shard::FENCING_TOKEN_BASE + 2
        );
        assert!(
            matches!(current.state, ShardState::Running { ref node_id, .. } if node_id.as_str() == "node-b")
        );
    }
}
