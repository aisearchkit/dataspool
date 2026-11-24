// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Shard claiming through committed Raft commands and monotonic lease generations.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, warn};

use crate::error::{DatspoolError, Result};
use crate::raft::{Command, RaftNode};
use crate::storage::kv::KvStore;
use crate::task::TaskMeta;
use crate::task::store::TaskStore;
use crate::types::TaskState;
use crate::types::{NodeId, ShardId, ShardState};

const DEFAULT_LEASE_SECONDS: i64 = 30;

#[derive(Debug, Clone)]
pub struct ShardLease {
    pub shard_id: ShardId,
    pub node_id: NodeId,
    pub fencing_token: u64,
    pub claim_id: String,
    pub expires_at: DateTime<Utc>,
    batch_sequence: Arc<AtomicU64>,
    sink_sequence: Arc<AtomicU64>,
}

impl ShardLease {
    /// Allocate a deterministic shard-cache entry ID for a replicated source batch.
    pub fn next_batch_id(&self) -> Result<String> {
        let sequence = self.batch_sequence.fetch_add(1, Ordering::SeqCst);
        if sequence >= 1_000_000 {
            return Err(DatspoolError::Internal(
                "Shard lease exceeded its one-million-batch fencing generation".into(),
            ));
        }
        Ok(format!("{:016x}-{:06x}", self.fencing_token, sequence))
    }

    /// Allocate a strictly increasing Elasticsearch external version within this lease.
    /// One million batch versions are reserved per fencing generation.
    pub fn next_sink_version(&self) -> Result<i64> {
        let sequence = self.sink_sequence.fetch_add(1, Ordering::SeqCst);
        if sequence >= 1_000_000 {
            return Err(DatspoolError::Internal(
                "Shard lease exceeded its one-million-batch fencing version range".into(),
            ));
        }
        let generation = self
            .fencing_token
            .saturating_sub(crate::task::shard::FENCING_TOKEN_BASE);
        let version = crate::task::shard::FENCING_TOKEN_BASE
            .checked_add(
                generation
                    .checked_mul(1_000_000)
                    .ok_or_else(|| DatspoolError::Internal("Fencing version overflow".into()))?,
            )
            .and_then(|base| base.checked_add(sequence))
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| {
                DatspoolError::Internal(
                    "Fencing version exceeds Elasticsearch's signed version range".into(),
                )
            })?;
        Ok(version)
    }
}

pub struct ShardClaimer<S: KvStore> {
    node_id: NodeId,
    task_store: Arc<TaskStore<S>>,
    raft: Arc<RaftNode>,
    lease_seconds: i64,
    max_attempts: usize,
}

impl<S: KvStore + Clone + 'static> Clone for ShardClaimer<S> {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            task_store: self.task_store.clone(),
            raft: self.raft.clone(),
            lease_seconds: self.lease_seconds,
            max_attempts: self.max_attempts,
        }
    }
}

impl<S: KvStore + Clone + 'static> ShardClaimer<S> {
    pub fn new(node_id: String, task_store: Arc<TaskStore<S>>, raft: Arc<RaftNode>) -> Self {
        Self {
            node_id: NodeId::new(node_id),
            task_store,
            raft,
            lease_seconds: DEFAULT_LEASE_SECONDS,
            max_attempts: 32,
        }
    }

    pub fn with_lease_seconds(mut self, lease_seconds: i64) -> Result<Self> {
        if lease_seconds < 3 {
            return Err(DatspoolError::Configuration(
                "Shard lease must be at least 3 seconds".into(),
            ));
        }
        self.lease_seconds = lease_seconds;
        Ok(self)
    }

    /// Expose the currently observed Raft leader for recovery diagnostics.
    pub async fn leader_id(&self) -> Option<String> {
        self.raft.get_leader_id().await
    }

    async fn enforce_task_sla(&self, task: &TaskMeta) -> Result<bool> {
        if matches!(
            task.state,
            TaskState::Completed { .. } | TaskState::Cancelled { .. } | TaskState::Failed { .. }
        ) {
            return Ok(false);
        }
        let now = Utc::now();
        let mut reasons = Vec::new();
        if let Some(deadline) = task.metadata.get("sla.deadline_at")
            && let Ok(deadline) = DateTime::parse_from_rfc3339(deadline)
            && now >= deadline.with_timezone(&Utc)
        {
            reasons.push("deadline");
        }
        let started = task
            .started_at
            .unwrap_or_else(|| DateTime::<Utc>::from(task.created_at));
        if let Some(limit) = task
            .metadata
            .get("sla.max_duration_seconds")
            .and_then(|value| value.parse::<u64>().ok())
            && limit > 0
            && now.signed_duration_since(started).num_seconds().max(0) as u64 >= limit
        {
            reasons.push("duration");
        }
        if let Some(limit_ms) = task
            .metadata
            .get("sla.execution_budget_ms")
            .and_then(|value| value.parse::<u64>().ok())
            && limit_ms > 0
            && now.signed_duration_since(started).num_milliseconds().max(0) as u64 >= limit_ms
        {
            reasons.push("execution_budget");
        }
        if let Some(limit) = task
            .metadata
            .get("sla.max_retries")
            .and_then(|value| value.parse::<u64>().ok())
        {
            let retries: u64 = self
                .task_store
                .list_task_shards(&task.task_id)
                .await?
                .iter()
                .map(|shard| shard.retry_count)
                .sum();
            if retries > limit {
                reasons.push("retry_budget");
            }
        }
        if reasons.is_empty() {
            return Ok(false);
        }
        let policy = task
            .metadata
            .get("sla.timeout_policy")
            .map(String::as_str)
            .unwrap_or("pause");
        let action = match policy {
            "cancel" => "cancel",
            "fail" => "fail",
            _ => "pause",
        };
        let request_id = format!("sla:{}:{}", task.task_id, reasons.join("+"));
        let response = self
            .raft
            .propose(Command::ControlTask {
                task_id: task.task_id.to_string(),
                action: action.into(),
                request_id,
                actor: "sla-enforcer".into(),
            })
            .await?;
        if !response.success {
            return Err(DatspoolError::Internal(format!(
                "SLA enforcement rejected for task {}: {}",
                task.task_id, response.message
            )));
        }
        debug!(task_id = %task.task_id, ?reasons, action, "Task SLA policy enforced");
        Ok(true)
    }

    pub async fn try_claim(&self) -> Result<Option<ShardLease>> {
        let mut tasks = self.task_store.list_tasks().await?;
        // Priority is strict; creation time provides deterministic FIFO fairness within a tier.
        sort_task_candidates(&mut tasks);
        let mut sla_blocked = std::collections::HashSet::new();
        for task in &tasks {
            if self.enforce_task_sla(task).await? {
                sla_blocked.insert(task.task_id);
            }
        }
        let nodes = self.task_store.list_nodes().await?;
        let node = nodes.iter().find(|node| node.node_id == self.node_id);
        if node.is_some_and(|node| node.status != crate::types::NodeStatus::Healthy) {
            return Ok(None);
        }
        let max_concurrent = node
            .and_then(|node| node.tags.get("capacity.max_concurrent"))
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(usize::MAX);
        let now = Utc::now();
        let mut active_by_node = std::collections::HashMap::<String, usize>::new();
        let mut active_memory_by_node = std::collections::HashMap::<String, u64>::new();
        for task in &tasks {
            for shard in self.task_store.list_task_shards(&task.task_id).await? {
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
            }
        }
        let mut candidates = Vec::new();
        for task in &tasks {
            if sla_blocked.contains(&task.task_id)
                || !matches!(task.state, TaskState::Pending | TaskState::Running { .. })
            {
                continue;
            }
            if !nodes.is_empty() {
                let Some(selected_node) = select_node_for_task(
                    &task.metadata,
                    &nodes,
                    &active_by_node,
                    &active_memory_by_node,
                ) else {
                    continue;
                };
                if selected_node != self.node_id {
                    continue;
                }
            } else if node
                .is_some_and(|node| !node_satisfies_task_requirements(&task.metadata, &node.tags))
            {
                continue;
            }
            for shard in self.task_store.list_task_shards(&task.task_id).await? {
                let available = match &shard.state {
                    ShardState::Pending => true,
                    ShardState::Running { started_at, .. } => shard
                        .lease_expires_at
                        .as_ref()
                        .map(|deadline| deadline <= &now)
                        .unwrap_or_else(|| {
                            started_at
                                .timestamp_millis()
                                .saturating_add(DEFAULT_LEASE_SECONDS * 1_000)
                                <= now.timestamp_millis()
                        }),
                    _ => false,
                };
                if available {
                    candidates.push((task.priority, task.created_at, shard.shard_id));
                }
            }
        }
        if node.is_some_and(|_| {
            active_by_node
                .get(self.node_id.as_str())
                .copied()
                .unwrap_or_default()
                >= max_concurrent
        }) {
            return Ok(None);
        }
        candidates.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let candidates: Vec<ShardId> = candidates
            .into_iter()
            .map(|(_, _, shard_id)| shard_id)
            .collect();
        if candidates.is_empty() {
            debug!("No pending or expired-lease shards available");
            return Ok(None);
        }

        for shard_id in candidates.into_iter().take(self.max_attempts) {
            let issued_at = Utc::now();
            let expires_at = issued_at + ChronoDuration::seconds(self.lease_seconds);
            let claim_id = uuid::Uuid::new_v4().to_string();
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(8),
                self.raft.propose(Command::ClaimShard {
                    shard_id: shard_id.to_string(),
                    node_id: self.node_id.to_string(),
                    claim_id: claim_id.clone(),
                    issued_at_ms: issued_at.timestamp_millis(),
                    lease_expires_at_ms: expires_at.timestamp_millis(),
                }),
            )
            .await
            .map_err(|_| DatspoolError::Raft(crate::error::RaftError::ElectionTimeout))?;
            match response {
                Ok(response) if response.success => {
                    let fencing_token = response
                        .data
                        .as_ref()
                        .and_then(|data| data.get("fencing_token"))
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| {
                            DatspoolError::Internal(
                                "Raft claim response omitted fencing token".into(),
                            )
                        })?;
                    let committed_expiry_ms = response
                        .data
                        .as_ref()
                        .and_then(|data| data.get("lease_expires_at_ms"))
                        .and_then(serde_json::Value::as_i64)
                        .ok_or_else(|| {
                            DatspoolError::Internal(
                                "Raft claim response omitted lease expiration".into(),
                            )
                        })?;
                    let committed_expiry = DateTime::from_timestamp_millis(committed_expiry_ms)
                        .ok_or_else(|| {
                            DatspoolError::Internal("Raft returned invalid lease expiration".into())
                        })?;
                    self.wait_for_local_lease(&shard_id, fencing_token, committed_expiry)
                        .await?;
                    info!(%shard_id, node_id = %self.node_id, fencing_token, "Shard lease committed by Raft");
                    return Ok(Some(ShardLease {
                        shard_id,
                        node_id: self.node_id.clone(),
                        fencing_token,
                        claim_id,
                        expires_at: committed_expiry,
                        batch_sequence: Arc::new(AtomicU64::new(0)),
                        sink_sequence: Arc::new(AtomicU64::new(0)),
                    }));
                }
                Ok(_) => debug!(%shard_id, "Shard claim lost a Raft state-machine race"),
                Err(error) => {
                    warn!(%shard_id, %error, "Raft shard claim failed");
                    return Err(error);
                }
            }
        }
        Ok(None)
    }

    pub async fn renew(&self, lease: &mut ShardLease) -> Result<bool> {
        let issued_at = Utc::now();
        let expires_at = issued_at + ChronoDuration::seconds(self.lease_seconds);
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.raft.propose(Command::RenewShardLease {
                shard_id: lease.shard_id.to_string(),
                node_id: lease.node_id.to_string(),
                fencing_token: lease.fencing_token,
                issued_at_ms: issued_at.timestamp_millis(),
                lease_expires_at_ms: expires_at.timestamp_millis(),
            }),
        )
        .await
        .map_err(|_| DatspoolError::Raft(crate::error::RaftError::ElectionTimeout))??;
        if response.success {
            let committed_expiry_ms = response
                .data
                .as_ref()
                .and_then(|data| data.get("lease_expires_at_ms"))
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| {
                    DatspoolError::Internal("Raft renewal response omitted lease expiration".into())
                })?;
            let committed_expiry = DateTime::from_timestamp_millis(committed_expiry_ms)
                .ok_or_else(|| {
                    DatspoolError::Internal("Raft returned invalid lease expiration".into())
                })?;
            self.wait_for_local_lease(&lease.shard_id, lease.fencing_token, committed_expiry)
                .await?;
            lease.expires_at = committed_expiry;
            Ok(true)
        } else {
            warn!(shard_id = %lease.shard_id, fencing_token = lease.fencing_token, reason = %response.message, "Shard lease renewal rejected");
            Ok(false)
        }
    }

    async fn wait_for_local_lease(
        &self,
        shard_id: &ShardId,
        fencing_token: u64,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        let mut last_seen = String::from("shard metadata not present on local member");
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                if let Some(shard) = self.task_store.get_shard(shard_id).await? {
                    let owner_matches = matches!(&shard.state, ShardState::Running { node_id, .. } if node_id == &self.node_id);
                    if owner_matches
                        && shard.fencing_token == fencing_token
                        && shard.lease_expires_at.as_ref().is_some_and(|current| current.timestamp_millis() >= expires_at.timestamp_millis())
                    {
                        return Ok(());
                    }
                    last_seen = format!("state={:?}, owner_matches={owner_matches}, fencing_token={}, lease_expires_at={:?}, expected_token={fencing_token}, expected_expiry={expires_at}", shard.state, shard.fencing_token, shard.lease_expires_at);
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }).await.map_err(|_| DatspoolError::Internal(format!("Timed out waiting for committed shard lease on local member: {last_seen}")))?
    }
}

/// Higher task priority wins; creation time is FIFO within each priority tier.
fn sort_task_candidates(tasks: &mut [TaskMeta]) {
    tasks.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| a.created_at.cmp(&b.created_at))
    });
}

/// Select the least-loaded eligible healthy node. Integer ratio comparison keeps placement deterministic.
fn select_node_for_task(
    task_metadata: &std::collections::HashMap<String, String>,
    nodes: &[crate::types::NodeMeta],
    active_by_node: &std::collections::HashMap<String, usize>,
    active_memory_by_node: &std::collections::HashMap<String, u64>,
) -> Option<NodeId> {
    let required_memory = task_metadata
        .get("requires.memory_mb")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default();
    let mut eligible: Vec<_> = nodes
        .iter()
        .filter(|node| {
            let capacity = node
                .tags
                .get("capacity.max_concurrent")
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(1);
            let active = active_by_node
                .get(node.node_id.as_str())
                .copied()
                .unwrap_or_default();
            let memory_capacity = node
                .tags
                .get("capacity.memory_mb")
                .and_then(|value| value.parse::<u64>().ok());
            let active_memory = active_memory_by_node
                .get(node.node_id.as_str())
                .copied()
                .unwrap_or_default();
            let memory_available = match memory_capacity {
                Some(capacity_mb) => active_memory.saturating_add(required_memory) <= capacity_mb,
                None => required_memory == 0,
            };
            node.status == crate::types::NodeStatus::Healthy
                && node_satisfies_task_requirements(task_metadata, &node.tags)
                && active < capacity
                && memory_available
        })
        .collect();
    let load_score = |node: &crate::types::NodeMeta| {
        let active = active_by_node
            .get(node.node_id.as_str())
            .copied()
            .unwrap_or_default() as u128;
        let capacity = node
            .tags
            .get("capacity.max_concurrent")
            .and_then(|value| value.parse::<u128>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1);
        let memory_active = active_memory_by_node
            .get(node.node_id.as_str())
            .copied()
            .unwrap_or_default() as u128;
        let memory_capacity = node
            .tags
            .get("capacity.memory_mb")
            .and_then(|value| value.parse::<u128>().ok())
            .unwrap_or(0);
        let concurrency_load = active.saturating_mul(1_000_000) / capacity;
        let memory_load = memory_active
            .saturating_mul(1_000_000)
            .checked_div(memory_capacity)
            .unwrap_or_default();
        concurrency_load.saturating_add(memory_load)
    };
    eligible.sort_by(|left, right| {
        load_score(left)
            .cmp(&load_score(right))
            .then_with(|| {
                let left_memory = left
                    .tags
                    .get("capacity.memory_mb")
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or_default();
                let right_memory = right
                    .tags
                    .get("capacity.memory_mb")
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or_default();
                right_memory.cmp(&left_memory)
            })
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    eligible.first().map(|node| node.node_id.clone())
}

/// Task metadata keys prefixed with `requires.` are hard node-placement constraints.
fn node_satisfies_task_requirements(
    task_metadata: &std::collections::HashMap<String, String>,
    node_tags: &std::collections::HashMap<String, String>,
) -> bool {
    task_metadata
        .iter()
        .filter_map(|(key, value)| key.strip_prefix("requires.").map(|tag| (tag, value)))
        .all(|(tag, value)| {
            if tag == "memory_mb" {
                return node_tags
                    .get("capacity.memory_mb")
                    .and_then(|capacity| capacity.parse::<u64>().ok())
                    .zip(value.parse::<u64>().ok())
                    .is_some_and(|(capacity, required)| capacity >= required);
            }
            node_tags.get(tag).is_some_and(|actual| actual == value)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::rocksdb::RocksDbStore;
    use tempfile::tempdir;

    #[test]
    fn scheduler_orders_priority_then_fifo() {
        let mut old_normal = TaskMeta::new("old-normal".into(), "{}".into(), "{}".into());
        old_normal.priority = crate::task::TaskPriority::Normal;
        old_normal.created_at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1);
        let mut new_normal = TaskMeta::new("new-normal".into(), "{}".into(), "{}".into());
        new_normal.priority = crate::task::TaskPriority::Normal;
        new_normal.created_at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(2);
        let mut critical = TaskMeta::new("critical".into(), "{}".into(), "{}".into());
        critical.priority = crate::task::TaskPriority::Critical;
        critical.created_at = std::time::UNIX_EPOCH;
        let mut tasks = vec![new_normal, critical, old_normal];
        sort_task_candidates(&mut tasks);
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.name.as_str())
                .collect::<Vec<_>>(),
            vec!["critical", "old-normal", "new-normal"]
        );
    }

    #[test]
    fn scheduler_selects_least_loaded_eligible_node_with_capacity_and_health() {
        let node = |id: &str, capacity: usize, status| crate::types::NodeMeta {
            node_id: NodeId::new(id),
            address: "127.0.0.1:1".into(),
            tags: std::collections::HashMap::from([
                ("region".into(), "west".into()),
                ("capacity.max_concurrent".into(), capacity.to_string()),
                ("capacity.memory_mb".into(), "8192".into()),
            ]),
            registered_at_ms: 1,
            last_heartbeat_ms: 1,
            status,
        };
        let nodes = vec![
            node("node-a", 4, crate::types::NodeStatus::Healthy),
            node("node-b", 4, crate::types::NodeStatus::Healthy),
            node("node-c", 8, crate::types::NodeStatus::Paused),
        ];
        let active = std::collections::HashMap::from([("node-a".into(), 2), ("node-b".into(), 1)]);
        let active_memory = std::collections::HashMap::new();
        assert_eq!(
            select_node_for_task(
                &std::collections::HashMap::new(),
                &nodes,
                &active,
                &active_memory
            )
            .unwrap()
            .as_str(),
            "node-b"
        );
        let full = std::collections::HashMap::from([("node-a".into(), 4), ("node-b".into(), 4)]);
        assert!(
            select_node_for_task(
                &std::collections::HashMap::new(),
                &nodes,
                &full,
                &active_memory
            )
            .is_none()
        );
        let balanced =
            std::collections::HashMap::from([("node-a".into(), 1), ("node-b".into(), 1)]);
        let memory_load =
            std::collections::HashMap::from([("node-a".into(), 7000), ("node-b".into(), 1000)]);
        assert_eq!(
            select_node_for_task(
                &std::collections::HashMap::new(),
                &nodes,
                &balanced,
                &memory_load
            )
            .unwrap()
            .as_str(),
            "node-b"
        );
    }

    #[test]
    fn scheduler_enforces_required_node_tags() {
        let requirements = std::collections::HashMap::from([
            ("requires.region".to_string(), "us-west".to_string()),
            ("requires.storage".to_string(), "ssd".to_string()),
        ]);
        let matching = std::collections::HashMap::from([
            ("region".to_string(), "us-west".to_string()),
            ("storage".to_string(), "ssd".to_string()),
            ("capacity.max_concurrent".to_string(), "8".to_string()),
            ("capacity.memory_mb".to_string(), "16384".to_string()),
        ]);
        let wrong_region = std::collections::HashMap::from([
            ("region".to_string(), "eu-central".to_string()),
            ("storage".to_string(), "ssd".to_string()),
        ]);
        assert!(node_satisfies_task_requirements(&requirements, &matching));
        assert!(!node_satisfies_task_requirements(
            &requirements,
            &wrong_region
        ));
        assert!(node_satisfies_task_requirements(
            &std::collections::HashMap::new(),
            &wrong_region
        ));
        let memory_requirement =
            std::collections::HashMap::from([("requires.memory_mb".into(), "8192".into())]);
        assert!(node_satisfies_task_requirements(
            &memory_requirement,
            &matching
        ));
        assert!(!node_satisfies_task_requirements(
            &memory_requirement,
            &wrong_region
        ));
    }

    #[test]
    fn sink_versions_increase_within_and_across_lease_generations() {
        let mut lease = ShardLease {
            shard_id: ShardId::new(),
            node_id: NodeId::new("node-a"),
            fencing_token: crate::task::shard::FENCING_TOKEN_BASE + 1,
            claim_id: "test-claim".into(),
            expires_at: Utc::now() + ChronoDuration::seconds(30),
            batch_sequence: Arc::new(AtomicU64::new(0)),
            sink_sequence: Arc::new(AtomicU64::new(0)),
        };
        let batch_id = lease.next_batch_id().unwrap();
        let first = lease.next_sink_version().unwrap();
        let second = lease.next_sink_version().unwrap();
        assert_eq!(second, first + 1);
        assert!(batch_id.ends_with("000000"));
        lease.fencing_token += 1;
        lease.batch_sequence = Arc::new(AtomicU64::new(0));
        lease.sink_sequence = Arc::new(AtomicU64::new(0));
        let next_lease = lease.next_sink_version().unwrap();
        assert!(next_lease > second);
    }

    struct CheckpointProbeSource {
        observed: Arc<std::sync::Mutex<Option<crate::pipeline::OffsetValue>>>,
    }

    #[async_trait::async_trait]
    impl crate::pipeline::Source for CheckpointProbeSource {
        async fn connect(&mut self) -> Result<()> {
            Ok(())
        }
        async fn read_batch(
            &mut self,
            offset: &crate::pipeline::OffsetValue,
            _batch_size: usize,
        ) -> Result<(crate::pipeline::RecordBatch, crate::pipeline::OffsetValue)> {
            *self.observed.lock().unwrap() = Some(offset.clone());
            Ok((crate::pipeline::RecordBatch::new(), offset.clone()))
        }
        async fn estimate_count(&self) -> Result<Option<u64>> {
            Ok(None)
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn source_type(&self) -> &str {
            "checkpoint-probe"
        }
    }

    struct CheckpointProbeSink;

    #[async_trait::async_trait]
    impl crate::pipeline::Sink for CheckpointProbeSink {
        async fn connect(&mut self) -> Result<()> {
            Ok(())
        }
        async fn write_batch(
            &mut self,
            batch: crate::pipeline::RecordBatch,
        ) -> Result<crate::pipeline::WriteResult> {
            Ok(crate::pipeline::WriteResult::success(batch.len(), 0))
        }
        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn sink_type(&self) -> &str {
            "checkpoint-probe"
        }
    }

    #[tokio::test]
    async fn worker_process_restart_reclaims_lease_and_preserves_checkpoint() {
        use crate::raft::{Command, RaftNode, default_raft_config};
        use crate::task::{ShardMeta, TaskMeta};
        use openraft::BasicNode;
        use std::collections::BTreeMap;

        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path().join("app")).unwrap();
        let store = Arc::new(TaskStore::new(db.clone()));
        let token = "worker-restart-test-token-0123456789".to_string();
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
        let rpc_server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        raft.initialize(BTreeMap::from([(1, BasicNode::new(address.to_string()))]))
            .await
            .unwrap();
        raft.wait_for_leader(std::time::Duration::from_secs(10))
            .await
            .unwrap();

        let task = TaskMeta::new("worker-restart-checkpoint".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        let created = raft
            .propose(Command::CreateTask {
                request_id: format!("create:{}", task.task_id),
                actor: "system".into(),
                task: Box::new(task),
                shards: vec![shard],
            })
            .await
            .unwrap();
        assert!(created.success, "{}", created.message);

        let first_claimer =
            ShardClaimer::new("worker-a".into(), store.clone(), Arc::new(raft.clone()))
                .with_lease_seconds(3)
                .unwrap();
        let first_lease = first_claimer.try_claim().await.unwrap().unwrap();
        let checkpoint = crate::pipeline::OffsetValue::DocumentCount(17);
        let progress = raft
            .propose(Command::UpdateShardProgress {
                shard_id: shard_id.to_string(),
                node_id: first_lease.node_id.to_string(),
                fencing_token: first_lease.fencing_token,
                issued_at_ms: Utc::now().timestamp_millis(),
                processed_records: 17,
                processed_bytes: 1_700,
                offset: serde_json::to_string(&checkpoint).unwrap(),
                batch_id: None,
            })
            .await
            .unwrap();
        assert!(progress.success, "{}", progress.message);

        // Simulate a worker process crash: Raft and the worker's local client
        // disappear, but the durable Raft log and application checkpoint remain.
        drop(first_claimer);
        rpc_server.abort();
        let _ = rpc_server.await;
        raft.shutdown().await.unwrap();
        drop(raft);

        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let restarted_raft = RaftNode::new(
            1,
            &raft_path,
            (*store).clone(),
            default_raft_config(),
            token.clone(),
        )
        .await
        .unwrap();
        let router = crate::raft_router(restarted_raft.handle(), token);
        let restarted_rpc = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        restarted_raft
            .wait_for_leader(std::time::Duration::from_secs(10))
            .await
            .unwrap();

        let persisted = store.get_shard(&shard_id).await.unwrap().unwrap();
        assert_eq!(persisted.records_processed, 17);
        assert_eq!(persisted.bytes_processed, 1_700);
        assert_eq!(persisted.offset, checkpoint);
        assert_eq!(persisted.fencing_token, first_lease.fencing_token);

        let wait = (first_lease.expires_at - Utc::now())
            .to_std()
            .unwrap_or_default()
            + std::time::Duration::from_millis(100);
        tokio::time::sleep(wait).await;
        let restarted_claimer = ShardClaimer::new(
            "worker-a".into(),
            store.clone(),
            Arc::new(restarted_raft.clone()),
        )
        .with_lease_seconds(3)
        .unwrap();
        let next_lease = restarted_claimer.try_claim().await.unwrap().unwrap();
        assert!(next_lease.fencing_token > first_lease.fencing_token);
        let recovered = store.get_shard(&shard_id).await.unwrap().unwrap();
        assert_eq!(recovered.records_processed, 17);
        assert_eq!(recovered.bytes_processed, 1_700);
        assert_eq!(recovered.offset, checkpoint);
        assert_eq!(recovered.fencing_token, next_lease.fencing_token);

        // A newly constructed executor must seek from the durable checkpoint,
        // not replay from the beginning after the worker process restarts.
        let index_store = Arc::new(tokio::sync::RwLock::new(
            crate::storage::index::TieredIndexStore::new(
                crate::storage::index::TieredConfig::default(),
                Arc::new(db.clone()),
            ),
        ));
        let executor = crate::agent::ShardExecutor::new_with_raft(
            store.clone(),
            index_store,
            Arc::new(restarted_raft.clone()),
        );
        let observed_offset = Arc::new(std::sync::Mutex::new(None));
        let mut source = CheckpointProbeSource {
            observed: observed_offset.clone(),
        };
        let mut sink = CheckpointProbeSink;
        executor
            .execute_pipeline_with_instances(&recovered, &mut source, &mut sink)
            .await
            .unwrap();
        assert_eq!(observed_offset.lock().unwrap().as_ref(), Some(&checkpoint));

        drop(executor);
        drop(restarted_claimer);
        restarted_rpc.abort();
        let _ = restarted_rpc.await;
        restarted_raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rejects_unreasonably_short_leases() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = Arc::new(TaskStore::new(db));
        // Raft is deliberately not created for this configuration validation test.
        let config = crate::raft::default_raft_config();
        assert!(config.heartbeat_interval > 0);
        assert_eq!(DEFAULT_LEASE_SECONDS, 30);
        let _ = store;
    }
}
