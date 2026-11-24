// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! OpenRaft node lifecycle and linearizable command proposals.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use openraft::{BasicNode, Config};
use tracing::{info, warn};

use super::Command;
use super::network::NetworkFactory;
use super::storage::{self, RocksNodeId, TypeConfig};
use crate::error::{DatspoolError, Result};
use crate::storage::RocksDbStore;
use crate::task::TaskStore;

pub type Raft = openraft::Raft<TypeConfig>;

/// Rewrite client-supplied wall-clock timestamps at the Raft leader. Lease
/// ownership must be judged against one authoritative clock, not a worker's
/// potentially skewed clock. Claim/renew durations are preserved, while their
/// absolute deadlines are rebased onto the leader's current time.
pub(crate) fn normalize_command_timestamp(command: &mut Command) -> Result<()> {
    const MIN_LEASE_MILLIS: i64 = 3_000;
    const MAX_LEASE_MILLIS: i64 = 24 * 60 * 60 * 1_000;

    let now = chrono::Utc::now().timestamp_millis();
    match command {
        Command::ClaimShard {
            issued_at_ms,
            lease_expires_at_ms,
            ..
        }
        | Command::RenewShardLease {
            issued_at_ms,
            lease_expires_at_ms,
            ..
        } => {
            let duration = lease_expires_at_ms.saturating_sub(*issued_at_ms);
            if !(MIN_LEASE_MILLIS..=MAX_LEASE_MILLIS).contains(&duration) {
                return Err(DatspoolError::Configuration(format!(
                    "Shard lease duration must be between {MIN_LEASE_MILLIS}ms and {MAX_LEASE_MILLIS}ms"
                )));
            }
            *issued_at_ms = now;
            *lease_expires_at_ms = now.saturating_add(duration);
        }
        Command::RegisterNode { issued_at_ms, .. }
        | Command::UpdateNodeHeartbeat { issued_at_ms, .. }
        | Command::MarkNodeOffline { issued_at_ms, .. }
        | Command::ReleaseShardLease { issued_at_ms, .. }
        | Command::CacheBatch { issued_at_ms, .. }
        | Command::PersistDeadLetters { issued_at_ms, .. }
        | Command::AckCacheBatch { issued_at_ms, .. }
        | Command::UpdateShardProgress { issued_at_ms, .. }
        | Command::CompleteShard { issued_at_ms, .. }
        | Command::FailShard { issued_at_ms, .. } => *issued_at_ms = now,
        Command::UpdateFederatedMigrationProgress { at_ms, .. } => *at_ms = now,
        Command::RecordFederatedRemoteOperation { operation, .. } => operation.updated_at_ms = now,
        _ => {}
    }
    Ok(())
}

#[derive(Clone)]
pub struct RaftNode {
    node_id: RocksNodeId,
    raft: Arc<Raft>,
    raft_token: String,
    #[cfg(test)]
    network: NetworkFactory,
}

impl RaftNode {
    pub async fn new<P: AsRef<Path>>(
        node_id: RocksNodeId,
        raft_data_dir: P,
        task_store: TaskStore<RocksDbStore>,
        config: Config,
        raft_token: String,
    ) -> Result<Self> {
        let config = Arc::new(
            config
                .validate()
                .map_err(|e| DatspoolError::Configuration(e.to_string()))?,
        );
        let (log_store, state_machine) = storage::new(raft_data_dir, task_store)
            .await
            .map_err(|e| DatspoolError::Raft(crate::error::RaftError::Storage(e)))?;
        let network = NetworkFactory::new(raft_token.clone());
        let raft = Raft::new(node_id, config, network.clone(), log_store, state_machine)
            .await
            .map_err(|e| DatspoolError::Raft(crate::error::RaftError::Storage(e.to_string())))?;
        Ok(Self {
            node_id,
            raft: Arc::new(raft),
            raft_token,
            #[cfg(test)]
            network,
        })
    }

    pub fn handle(&self) -> Arc<Raft> {
        self.raft.clone()
    }

    pub fn node_id(&self) -> RocksNodeId {
        self.node_id
    }

    #[cfg(test)]
    pub async fn set_peer_blocked(&self, node_id: RocksNodeId, blocked: bool) {
        self.network.set_peer_blocked(node_id, blocked).await;
    }

    pub async fn is_initialized(&self) -> Result<bool> {
        self.raft
            .is_initialized()
            .await
            .map_err(|e| DatspoolError::Raft(crate::error::RaftError::Storage(e.to_string())))
    }

    /// Prepare a brand-new non-bootstrap member to receive the authoritative
    /// state machine from the bootstrap node. Existing Raft members and nodes
    /// with an applied marker are never cleared.
    pub async fn prepare_join(&self, task_store: &TaskStore<RocksDbStore>) -> Result<usize> {
        if task_store.raft_applied_state().await?.is_some() {
            return Ok(0);
        }
        task_store.clear_unreplicated_state().await
    }

    pub async fn initialize(&self, members: BTreeMap<RocksNodeId, BasicNode>) -> Result<()> {
        if self
            .raft
            .is_initialized()
            .await
            .map_err(|e| DatspoolError::Raft(crate::error::RaftError::Storage(e.to_string())))?
        {
            return Ok(());
        }
        match self.raft.initialize(members).await {
            Ok(()) => {
                info!(
                    node_id = self.node_id,
                    "Initialized OpenRaft cluster membership"
                );
                Ok(())
            }
            Err(error) => {
                // Concurrent startup with the same initial membership is safe in OpenRaft.
                if self.raft.is_initialized().await.unwrap_or(false) {
                    warn!(node_id = self.node_id, %error, "Raft cluster was initialized concurrently");
                    Ok(())
                } else {
                    Err(DatspoolError::Raft(
                        crate::error::RaftError::ProposalFailed(error.to_string()),
                    ))
                }
            }
        }
    }

    /// Confirm this node is the current leader and that its state machine is
    /// applied through a quorum-confirmed read index before serving a read.
    pub async fn ensure_linearizable(&self) -> Result<()> {
        self.raft
            .ensure_linearizable()
            .await
            .map(|_| ())
            .map_err(|error| {
                DatspoolError::Raft(crate::error::RaftError::ProposalFailed(error.to_string()))
            })
    }

    pub async fn current_leader(&self) -> Option<RocksNodeId> {
        self.raft.current_leader().await
    }

    pub async fn wait_for_leader(&self, timeout: std::time::Duration) -> Result<RocksNodeId> {
        tokio::time::timeout(timeout, async {
            loop {
                if let Some(leader) = self.raft.current_leader().await {
                    return leader;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| DatspoolError::Raft(crate::error::RaftError::ElectionTimeout))
    }

    /// Replicate pre-Raft tasks and node-local pending batches from the
    /// deterministic bootstrap member, then publish a cluster-wide readiness barrier.
    pub async fn bootstrap_local_state(
        &self,
        task_store: &TaskStore<RocksDbStore>,
    ) -> Result<usize> {
        if task_store.bootstrap_complete().await? {
            task_store.clear_legacy_cache().await?;
            return Ok(0);
        }

        let mut replicated_tasks = 0;
        for task in task_store.list_tasks().await? {
            let mut shards = task_store.list_task_shards(&task.task_id).await?;
            for shard in &mut shards {
                if matches!(&shard.state, crate::types::ShardState::Running { .. })
                    && shard.lease_expires_at.is_none()
                    && shard.lease_id.is_none()
                {
                    shard.set_state_at(crate::types::ShardState::Pending, chrono::Utc::now());
                }
            }
            let response = self
                .propose(Command::CreateTask {
                    request_id: format!("create:{}", task.task_id),
                    actor: "system".into(),
                    task: Box::new(task),
                    shards: shards.clone(),
                })
                .await?;
            if !response.success {
                return Err(DatspoolError::Raft(
                    crate::error::RaftError::ProposalFailed(response.message),
                ));
            }
            replicated_tasks += 1;

            for shard in shards {
                let legacy_cache = crate::pipeline::LocalBatchCache::new_for_shard(
                    Arc::new(task_store.kv_store().clone()),
                    128,
                    &shard.shard_id.to_string(),
                );
                for (sequence, (_legacy_key, entry)) in
                    legacy_cache.entries().await?.into_iter().enumerate()
                {
                    if sequence >= 1_000_000 {
                        return Err(DatspoolError::Internal(format!(
                            "Too many legacy cache entries for shard {}",
                            shard.shard_id
                        )));
                    }
                    // Generation zero sorts before every lease-backed batch. Stable
                    // sequence IDs make a partially completed bootstrap resumable.
                    let batch_id = format!("{:016x}-{:06x}", 0, sequence);
                    let response = self
                        .propose(Command::ImportLegacyCache {
                            shard_id: shard.shard_id.to_string(),
                            batch_id,
                            records: entry.records,
                            next_offset: entry.next_offset,
                            source_count: entry.source_count,
                        })
                        .await?;
                    if !response.success {
                        return Err(DatspoolError::Raft(
                            crate::error::RaftError::ProposalFailed(response.message),
                        ));
                    }
                }
            }
        }

        let response = self.propose(Command::BootstrapComplete).await?;
        if !response.success {
            return Err(DatspoolError::Raft(
                crate::error::RaftError::ProposalFailed(response.message),
            ));
        }
        self.wait_for_bootstrap_complete(task_store, std::time::Duration::from_secs(60))
            .await?;
        task_store.clear_legacy_cache().await?;
        Ok(replicated_tasks)
    }

    /// Followers do not start workers until the bootstrap member has replicated
    /// its legacy tasks and pending local cache entries to the whole cluster.
    pub async fn wait_for_bootstrap_complete(
        &self,
        task_store: &TaskStore<RocksDbStore>,
        timeout: std::time::Duration,
    ) -> Result<()> {
        tokio::time::timeout(timeout, async {
            loop {
                if task_store.bootstrap_complete().await? {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| DatspoolError::Raft(crate::error::RaftError::ElectionTimeout))?
    }

    pub async fn propose(&self, command: Command) -> Result<super::CommandResponse> {
        let mut last_error = None;
        // Followers may learn the elected leader slightly later than the leader
        // itself. Do not block in client_write while the leader is unknown: that
        // prevents the forwarding/retry path from ever running.
        for attempt in 0..20 {
            match self.raft.current_leader().await {
                Some(leader_id) if leader_id == self.node_id => {
                    let mut proposal = command.clone();
                    normalize_command_timestamp(&mut proposal)?;
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        self.raft.client_write(proposal),
                    )
                    .await
                    {
                        Ok(Ok(response)) => return Ok(response.data),
                        Ok(Err(error)) => last_error = Some(error.to_string()),
                        Err(_) => last_error = Some("Raft proposal timed out".to_string()),
                    }
                }
                Some(_) => match self.forward_proposal(command.clone()).await {
                    Ok(response) => return Ok(response),
                    Err(error) => last_error = Some(error.to_string()),
                },
                None => last_error = Some("Raft leader is not known yet".to_string()),
            }
            if attempt < 19 {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }
        Err(DatspoolError::Raft(
            crate::error::RaftError::ProposalFailed(
                last_error.unwrap_or_else(|| "Raft proposal failed".to_string()),
            ),
        ))
    }

    async fn forward_proposal(&self, command: Command) -> Result<super::CommandResponse> {
        let metrics = self.raft.metrics();
        let (leader_id, leader_addr) = {
            let current = metrics.borrow();
            let leader_id = current.current_leader;
            let address = leader_id.and_then(|id| {
                current
                    .membership_config
                    .membership()
                    .get_node(&id)
                    .map(|node| node.addr.clone())
            });
            (leader_id, address)
        };
        let leader_id = leader_id.ok_or(DatspoolError::Raft(
            crate::error::RaftError::NotLeader(None),
        ))?;
        let address = leader_addr.ok_or_else(|| {
            DatspoolError::Raft(crate::error::RaftError::NotLeader(Some(
                leader_id.to_string(),
            )))
        })?;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .map_err(|e| DatspoolError::Raft(crate::error::RaftError::Network(e.to_string())))?;
        let response = client
            .post(format!("http://{address}/raft/propose"))
            .header("x-dataspool-raft-token", &self.raft_token)
            .json(&command)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| DatspoolError::Raft(crate::error::RaftError::Network(e.to_string())))?;
        response
            .json::<super::CommandResponse>()
            .await
            .map_err(|e| DatspoolError::Raft(crate::error::RaftError::Network(e.to_string())))
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.ensure_linearizable().await.is_ok()
    }

    pub async fn get_leader_id(&self) -> Option<String> {
        self.raft.current_leader().await.map(|id| id.to_string())
    }

    /// Force a state-machine snapshot and purge the covered log prefix.
    pub async fn trigger_snapshot_and_purge(&self) -> Result<()> {
        let previous_snapshot = self.raft.metrics().borrow().snapshot;
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| DatspoolError::Raft(crate::error::RaftError::Storage(e.to_string())))?;
        let snapshot_index = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(snapshot) = self.raft.metrics().borrow().snapshot
                    && Some(snapshot) != previous_snapshot
                {
                    return snapshot.index;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| {
            DatspoolError::Raft(crate::error::RaftError::Storage(
                "Timed out waiting for Raft snapshot creation".into(),
            ))
        })?;
        self.raft
            .trigger()
            .purge_log(snapshot_index)
            .await
            .map_err(|e| DatspoolError::Raft(crate::error::RaftError::Storage(e.to_string())))?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| DatspoolError::Raft(crate::error::RaftError::Storage(e.to_string())))
    }
}

pub fn stable_node_id(value: &str) -> RocksNodeId {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(value.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(bytes).max(1)
}

#[cfg(test)]
mod cluster_tests {
    use super::*;
    use crate::agent::ShardClaimer;
    use crate::pipeline::{LocalBatchCache, OffsetValue, Record};
    use crate::task::{ShardMeta, TaskMeta, TaskStore};
    use crate::types::ShardState;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn proposal_timestamps_use_the_leader_clock_and_preserve_lease_duration() {
        let before = chrono::Utc::now().timestamp_millis();
        let mut command = Command::ClaimShard {
            shard_id: "shard-1".into(),
            node_id: "worker-a".into(),
            claim_id: "clock-skew-test".into(),
            issued_at_ms: 1,
            lease_expires_at_ms: 3_001,
        };
        normalize_command_timestamp(&mut command).unwrap();
        let after = chrono::Utc::now().timestamp_millis();
        let Command::ClaimShard {
            issued_at_ms,
            lease_expires_at_ms,
            ..
        } = command
        else {
            unreachable!();
        };
        assert!((before..=after).contains(&issued_at_ms));
        assert_eq!(lease_expires_at_ms - issued_at_ms, 3_000);
    }

    #[tokio::test]
    async fn three_node_raft_cluster_replicates_claims_and_rejects_stale_fencing_tokens() {
        let token = "cluster-test-token-0123456789abcdef".to_string();
        let mut directories = Vec::new();
        let mut stores = Vec::new();
        let mut listeners = Vec::new();
        let mut addresses = Vec::new();

        for _ in 0..3 {
            let directory = tempdir().unwrap();
            let store = RocksDbStore::open(directory.path().join("app")).unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.push(listener.local_addr().unwrap().to_string());
            directories.push(directory);
            stores.push(store);
            listeners.push(listener);
        }

        let ids = [11_u64, 22_u64, 33_u64];
        let members = ids
            .iter()
            .zip(addresses.iter())
            .map(|(id, address)| (*id, BasicNode::new(address.clone())))
            .collect::<BTreeMap<_, _>>();
        let config =
            crate::raft::raft_config(Duration::from_millis(150), Duration::from_millis(600));
        let mut nodes = Vec::new();
        let mut server_tasks = Vec::new();

        for index in 0..3 {
            let task_store = TaskStore::new(stores[index].clone());
            let node = RaftNode::new(
                ids[index],
                directories[index].path().join("raft"),
                task_store,
                config.clone(),
                token.clone(),
            )
            .await
            .unwrap();
            let router = super::super::network::router(node.handle(), token.clone());
            let listener = listeners.remove(0);
            server_tasks.push(tokio::spawn(async move {
                let _ = axum::serve(listener, router).await;
            }));
            nodes.push(node);
        }

        nodes[0].initialize(members.clone()).await.unwrap();

        let leader_index = tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                for (index, node) in nodes.iter().enumerate() {
                    if node.is_leader().await {
                        return index;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("Raft cluster did not elect a leader");
        let leader = &nodes[leader_index];
        leader.ensure_linearizable().await.unwrap();
        let follower_index = (leader_index + 1) % nodes.len();
        assert!(nodes[follower_index].ensure_linearizable().await.is_err());

        let task = TaskMeta::new("raft-fencing-integration".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        let created = leader
            .propose(Command::CreateTask {
                request_id: format!("create:{}", task.task_id),
                actor: "system".into(),
                task: Box::new(task),
                shards: vec![shard],
            })
            .await
            .unwrap();
        assert!(created.success, "{}", created.message);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut replicated = true;
                for store in &stores {
                    if TaskStore::new(store.clone())
                        .get_shard(&shard_id)
                        .await
                        .unwrap()
                        .is_none()
                    {
                        replicated = false;
                        break;
                    }
                }
                if replicated {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("task/shard metadata did not replicate to all members");

        // A follower must be able to claim and renew through the elected leader;
        // shard execution is distributed across all members, not leader-only.
        let worker_index = (leader_index + 1) % nodes.len();
        let worker_node = nodes[worker_index].clone();
        let known_leader = worker_node
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(known_leader, leader.node_id());
        let worker_store = Arc::new(TaskStore::new(stores[worker_index].clone()));
        let claimer = ShardClaimer::new(
            "worker-a".into(),
            worker_store.clone(),
            Arc::new(worker_node.clone()),
        )
        .with_lease_seconds(3)
        .unwrap();
        let mut lease = claimer
            .try_claim()
            .await
            .unwrap()
            .expect("follower should claim pending shard");
        let first_token = lease.fencing_token;
        assert!(
            claimer.renew(&mut lease).await.unwrap(),
            "follower should renew its committed lease"
        );

        let batch_id = lease.next_batch_id().unwrap();
        let cache_key = crate::pipeline::cache::LocalBatchCache::<RocksDbStore>::key_for_shard(
            &shard_id.to_string(),
            &batch_id,
        );
        let batch_command = Command::CacheBatch {
            shard_id: shard_id.to_string(),
            node_id: "worker-a".into(),
            fencing_token: first_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            batch_id: batch_id.clone(),
            records: vec![Record::with_id(
                "doc-1",
                "elasticsearch",
                json!({"value": 1}),
            )],
            next_offset: Some(OffsetValue::DocumentCount(1)),
            source_count: 1,
        };
        let cached = worker_node.propose(batch_command.clone()).await.unwrap();
        assert!(cached.success, "{}", cached.message);
        let duplicate = worker_node.propose(batch_command.clone()).await.unwrap();
        assert!(
            duplicate.success,
            "same cache batch retry must be idempotent: {}",
            duplicate.message
        );
        let mut conflicting = batch_command.clone();
        if let Command::CacheBatch { records, .. } = &mut conflicting {
            records[0].payload = json!({"value": 2});
        }
        let conflict = worker_node.propose(conflicting).await.unwrap();
        assert!(
            !conflict.success,
            "a cache batch ID must not be reusable with a different payload"
        );

        let local_cache = LocalBatchCache::new_replicated_for_shard(
            Arc::new(stores[worker_index].clone()),
            128,
            &shard_id.to_string(),
        );
        local_cache
            .wait_for_batch(&batch_id, Duration::from_secs(5))
            .await
            .unwrap();
        for store in &stores {
            assert_eq!(
                TaskStore::new(store.clone())
                    .cache_batch_count(&shard_id.to_string())
                    .await
                    .unwrap(),
                1
            );
        }

        let now = chrono::Utc::now().timestamp_millis();
        let delivered = worker_node
            .propose(Command::RecordBatchDelivery {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token: first_token,
                issued_at_ms: now,
                batch_id: batch_id.clone(),
                success_count: 1,
                failed_count: 0,
                bytes_written: 128,
            })
            .await
            .unwrap();
        assert!(delivered.success, "{}", delivered.message);
        let progress = worker_node
            .propose(Command::UpdateShardProgress {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token: first_token,
                issued_at_ms: now,
                processed_records: 1,
                processed_bytes: 128,
                offset: serde_json::to_string(&OffsetValue::DocumentCount(1)).unwrap(),
                batch_id: Some(batch_id.clone()),
            })
            .await
            .unwrap();
        assert!(progress.success, "{}", progress.message);
        // Every Raft member must expose the same aggregate, checkpoint, byte count,
        // and per-node contribution after the committed progress entry.
        for store in &stores {
            let task = TaskStore::new(store.clone())
                .get_task_with_aggregate(&task_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(task.aggregate.total_shards, 1);
            assert_eq!(task.aggregate.running_shards, 1);
            assert_eq!(task.aggregate.docs_processed, 1);
            assert_eq!(task.aggregate.bytes_processed, 128);
            assert_eq!(task.aggregate.checkpoints.len(), 1);
            assert_eq!(
                task.aggregate.checkpoints[0].offset,
                OffsetValue::DocumentCount(1)
            );
            let worker = task.aggregate.nodes.get("worker-a").unwrap();
            assert_eq!(worker.running_shards, 1);
        }

        let ack_command = Command::AckCacheBatch {
            shard_id: shard_id.to_string(),
            node_id: "worker-a".into(),
            fencing_token: first_token,
            issued_at_ms: now + 1,
            cache_key: String::from_utf8(cache_key).unwrap(),
        };
        let ack = worker_node.propose(ack_command.clone()).await.unwrap();
        assert!(ack.success, "{}", ack.message);
        for store in &stores {
            assert_eq!(
                TaskStore::new(store.clone())
                    .cache_batch_count(&shard_id.to_string())
                    .await
                    .unwrap(),
                0
            );
        }
        // A delayed retry of an already acknowledged log command must not
        // resurrect the batch after the FIFO cache watermark has advanced.
        let late_retry = worker_node.propose(batch_command.clone()).await.unwrap();
        assert!(
            late_retry.success,
            "late cache retry should be idempotent: {}",
            late_retry.message
        );
        for store in &stores {
            assert_eq!(
                TaskStore::new(store.clone())
                    .cache_batch_count(&shard_id.to_string())
                    .await
                    .unwrap(),
                0
            );
        }

        let remaining_ms =
            lease.expires_at.timestamp_millis() - chrono::Utc::now().timestamp_millis();
        if remaining_ms > 0 {
            tokio::time::sleep(Duration::from_millis(remaining_ms as u64 + 25)).await;
        }
        let takeover_at = chrono::Utc::now().timestamp_millis();
        let second = worker_node
            .propose(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-b".into(),
                claim_id: "claim-worker-b".into(),
                issued_at_ms: takeover_at,
                lease_expires_at_ms: takeover_at + 30_000,
            })
            .await
            .unwrap();
        assert!(second.success, "{}", second.message);
        let second_token = second.data.unwrap()["fencing_token"].as_u64().unwrap();
        assert!(second_token > first_token);
        let complete_command = Command::CompleteShard {
            shard_id: shard_id.to_string(),
            node_id: "worker-b".into(),
            fencing_token: second_token,
            issued_at_ms: takeover_at + 2_000,
            total_records: 1,
        };
        let completed = worker_node.propose(complete_command.clone()).await.unwrap();
        assert!(completed.success, "{}", completed.message);
        let duplicate_completion = worker_node.propose(complete_command.clone()).await.unwrap();
        assert!(
            duplicate_completion.success,
            "duplicate completion must be idempotent"
        );
        for store in &stores {
            let task = TaskStore::new(store.clone())
                .get_task_with_aggregate(&task_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(task.aggregate.completed_shards, 1);
            assert_eq!(task.aggregate.failed_shards, 0);
            assert_eq!(task.completed_shards, 1);
            assert_eq!(task.aggregate.docs_processed, 1);
            assert!(matches!(
                task.state,
                crate::types::TaskState::Completed { .. }
            ));
            let worker_b = task.aggregate.nodes.get("worker-b").unwrap();
            assert_eq!(worker_b.completed_shards, 1);
            assert_eq!(worker_b.docs_processed, 1);
            assert_eq!(worker_b.bytes_processed, 128);
            assert!(worker_b.throughput_docs_per_sec > 0.0);
        }

        let late_ack = worker_node.propose(ack_command).await.unwrap();
        assert!(
            late_ack.success,
            "an already committed ack remains idempotent after lease handoff"
        );
        let stale_batch = worker_node.propose(batch_command).await.unwrap();
        assert!(
            !stale_batch.success,
            "an old owner cannot append a batch after lease handoff"
        );
        for store in &stores {
            assert_eq!(
                TaskStore::new(store.clone())
                    .cache_batch_count(&shard_id.to_string())
                    .await
                    .unwrap(),
                0
            );
        }

        let stale = worker_node
            .propose(Command::CompleteShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token: first_token,
                issued_at_ms: takeover_at + 1_000,
                total_records: 99,
            })
            .await
            .unwrap();
        assert!(
            !stale.success,
            "stale worker must not complete a reclaimed shard"
        );

        for store in &stores {
            let current = TaskStore::new(store.clone())
                .get_shard(&shard_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(current.fencing_token, second_token);
            assert!(matches!(current.state, ShardState::Completed { .. }));
            assert_eq!(
                current
                    .last_owner_node_id
                    .as_ref()
                    .map(|node| node.as_str()),
                Some("worker-b")
            );
        }

        // Create a bidirectional network partition: block every outbound RPC
        // from the isolated follower and every inbound RPC to it. The remaining
        // two members retain quorum and continue committing state.
        let isolated_index = (leader_index + 2) % nodes.len();
        let isolated_id = ids[isolated_index];
        for (index, node) in nodes.iter().enumerate() {
            if index == isolated_index {
                for peer_id in ids.iter().copied().filter(|peer| *peer != isolated_id) {
                    node.set_peer_blocked(peer_id, true).await;
                }
            } else {
                node.set_peer_blocked(isolated_id, true).await;
            }
        }
        let recovery_task = TaskMeta::new("snapshot-recovery".into(), "{}".into(), "{}".into());
        let recovery_task_id = recovery_task.task_id;
        let recovery_shard = ShardMeta::new(recovery_task_id, 0, "{}".into());
        let recovery_created = worker_node
            .propose(Command::CreateTask {
                request_id: format!("create:{}", recovery_task.task_id),
                actor: "system".into(),
                task: Box::new(recovery_task),
                shards: vec![recovery_shard],
            })
            .await
            .unwrap();
        assert!(recovery_created.success, "{}", recovery_created.message);
        nodes[leader_index]
            .trigger_snapshot_and_purge()
            .await
            .unwrap();

        // Heal both directions; the returning member must install the snapshot
        // after its compacted log prefix has been purged.
        for node in &nodes {
            for peer_id in ids {
                node.set_peer_blocked(peer_id, false).await;
            }
        }
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if TaskStore::new(stores[isolated_index].clone())
                    .get_task_with_aggregate(&recovery_task_id)
                    .await
                    .unwrap()
                    .is_some_and(|task| task.aggregate.total_shards == 1)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("partitioned follower did not recover task state from snapshot");

        // Crash the leader. The remaining quorum must elect a new leader and
        // accept writes through a follower client without losing committed state.
        server_tasks[leader_index].abort();
        nodes[leader_index].shutdown().await.unwrap();
        let new_leader_index = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                for (index, node) in nodes.iter().enumerate() {
                    if index != leader_index && node.is_leader().await {
                        return index;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("cluster did not elect a replacement leader after leader shutdown");
        assert_ne!(new_leader_index, leader_index);

        let after_failover =
            TaskMeta::new("leader-failover-write".into(), "{}".into(), "{}".into());
        let after_failover_id = after_failover.task_id;
        let after_failover_shard = ShardMeta::new(after_failover_id, 0, "{}".into());
        let failover_created = nodes[new_leader_index]
            .propose(Command::CreateTask {
                request_id: format!("create:{}", after_failover.task_id),
                actor: "system".into(),
                task: Box::new(after_failover),
                shards: vec![after_failover_shard],
            })
            .await
            .unwrap();
        assert!(failover_created.success, "{}", failover_created.message);
        for (index, store) in stores.iter().enumerate() {
            if index == leader_index {
                continue;
            }
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if TaskStore::new(store.clone())
                        .get_task(&after_failover_id)
                        .await
                        .unwrap()
                        .is_some()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("post-failover task did not replicate to surviving members");
        }

        // Restart the first failed member from its durable RocksDB state, then
        // crash the replacement leader. The remaining quorum must elect a second
        // replacement without losing the committed migration/task state.
        let restarted = RaftNode::new(
            ids[leader_index],
            directories[leader_index].path().join("raft"),
            TaskStore::new(stores[leader_index].clone()),
            config.clone(),
            token.clone(),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind(&addresses[leader_index])
            .await
            .unwrap();
        let router = super::super::network::router(restarted.handle(), token.clone());
        server_tasks[leader_index] = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        nodes[leader_index] = restarted;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if TaskStore::new(stores[leader_index].clone())
                    .get_task(&after_failover_id)
                    .await
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("restarted member did not recover committed task state");

        server_tasks[new_leader_index].abort();
        nodes[new_leader_index].shutdown().await.unwrap();
        let second_leader_index = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                for (index, node) in nodes.iter().enumerate() {
                    if index != new_leader_index && node.is_leader().await {
                        return index;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("cluster did not elect a second replacement leader");
        assert_ne!(second_leader_index, new_leader_index);

        let after_second_failover = TaskMeta::new(
            "second-leader-failover-write".into(),
            "{}".into(),
            "{}".into(),
        );
        let second_failover_id = after_second_failover.task_id;
        let second_failover_shard = ShardMeta::new(second_failover_id, 0, "{}".into());
        let second_failover_created = nodes[second_leader_index]
            .propose(Command::CreateTask {
                request_id: format!("create:{}", after_second_failover.task_id),
                actor: "system".into(),
                task: Box::new(after_second_failover),
                shards: vec![second_failover_shard],
            })
            .await
            .unwrap();
        assert!(
            second_failover_created.success,
            "{}",
            second_failover_created.message
        );
        for (index, store) in stores.iter().enumerate() {
            if index == new_leader_index {
                continue;
            }
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if TaskStore::new(store.clone())
                        .get_task(&second_failover_id)
                        .await
                        .unwrap()
                        .is_some()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("second post-failover task did not replicate to surviving members");
        }

        for (index, node) in nodes.iter().enumerate() {
            if index != new_leader_index {
                node.shutdown().await.unwrap();
            }
        }
        for server in server_tasks {
            server.abort();
        }
    }
}
