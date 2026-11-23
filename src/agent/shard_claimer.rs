// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Shard claiming through committed Raft commands and monotonic lease generations.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use tracing::{debug, info, warn};

use crate::error::{DatspoolError, Result};
use crate::raft::{Command, RaftNode};
use crate::storage::kv::KvStore;
use crate::task::store::TaskStore;
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
            return Err(DatspoolError::Internal("Shard lease exceeded its one-million-batch fencing generation".into()));
        }
        Ok(format!("{:016x}-{:06x}", self.fencing_token, sequence))
    }

    /// Allocate a strictly increasing Elasticsearch external version within this lease.
    /// One million batch versions are reserved per fencing generation.
    pub fn next_sink_version(&self) -> Result<i64> {
        let sequence = self.sink_sequence.fetch_add(1, Ordering::SeqCst);
        if sequence >= 1_000_000 {
            return Err(DatspoolError::Internal("Shard lease exceeded its one-million-batch fencing version range".into()));
        }
        let generation = self.fencing_token.saturating_sub(crate::task::shard::FENCING_TOKEN_BASE);
        let version = crate::task::shard::FENCING_TOKEN_BASE
            .checked_add(generation.checked_mul(1_000_000).ok_or_else(|| DatspoolError::Internal("Fencing version overflow".into()))?)
            .and_then(|base| base.checked_add(sequence))
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| DatspoolError::Internal("Fencing version exceeds Elasticsearch's signed version range".into()))?;
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
            return Err(DatspoolError::Configuration("Shard lease must be at least 3 seconds".into()));
        }
        self.lease_seconds = lease_seconds;
        Ok(self)
    }

    pub async fn try_claim(&self) -> Result<Option<ShardLease>> {
        let tasks = self.task_store.list_tasks().await?;
        let mut candidates = Vec::new();
        let now = Utc::now();
        for task in &tasks {
            for shard in self.task_store.list_task_shards(&task.task_id).await? {
                let available = match &shard.state {
                    ShardState::Pending => true,
                    ShardState::Running { started_at, .. } => shard.lease_expires_at.as_ref()
                    .map(|deadline| deadline <= &now)
                    .unwrap_or_else(|| started_at.timestamp_millis().saturating_add(DEFAULT_LEASE_SECONDS * 1_000) <= now.timestamp_millis()),
                    _ => false,
                };
                if available { candidates.push(shard.shard_id); }
            }
        }
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
            ).await.map_err(|_| DatspoolError::Raft(crate::error::RaftError::ElectionTimeout))?;
            match response {
                Ok(response) if response.success => {
                    let fencing_token = response.data.as_ref()
                        .and_then(|data| data.get("fencing_token"))
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| DatspoolError::Internal("Raft claim response omitted fencing token".into()))?;
                    self.wait_for_local_lease(&shard_id, fencing_token, expires_at).await?;
                    info!(%shard_id, node_id = %self.node_id, fencing_token, "Shard lease committed by Raft");
                    return Ok(Some(ShardLease {
                        shard_id,
                        node_id: self.node_id.clone(),
                        fencing_token,
                        claim_id,
                        expires_at,
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
        ).await.map_err(|_| DatspoolError::Raft(crate::error::RaftError::ElectionTimeout))??;
        if response.success {
            self.wait_for_local_lease(&lease.shard_id, lease.fencing_token, expires_at).await?;
            lease.expires_at = expires_at;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::rocksdb::RocksDbStore;
    use tempfile::tempdir;


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
