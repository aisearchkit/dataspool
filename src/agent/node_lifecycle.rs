// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Raft-backed node registration, heartbeat, and failure detection.

use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    error::Result,
    raft::{Command, RaftNode},
    storage::kv::KvStore,
    task::store::TaskStore,
    types::NodeStatus,
};

pub struct NodeLifecycle<S: KvStore> {
    node_id: String,
    address: String,
    tags: HashMap<String, String>,
    task_store: Arc<TaskStore<S>>,
    raft: Arc<RaftNode>,
    heartbeat_interval: Duration,
    failure_timeout: Duration,
}

impl<S: KvStore + Clone + 'static> NodeLifecycle<S> {
    pub fn new(
        node_id: String,
        address: String,
        tags: HashMap<String, String>,
        task_store: Arc<TaskStore<S>>,
        raft: Arc<RaftNode>,
        heartbeat_interval: Duration,
        failure_timeout: Duration,
    ) -> Self {
        Self {
            node_id,
            address,
            tags,
            task_store,
            raft,
            heartbeat_interval: heartbeat_interval.max(Duration::from_secs(1)),
            failure_timeout: failure_timeout.max(Duration::from_secs(5)),
        }
    }

    pub async fn register(&self) -> Result<()> {
        let response = self
            .raft
            .propose(Command::RegisterNode {
                node_id: self.node_id.clone(),
                address: self.address.clone(),
                tags: self.tags.clone(),
                issued_at_ms: chrono::Utc::now().timestamp_millis(),
            })
            .await?;
        if !response.success {
            return Err(crate::error::DatspoolError::Internal(format!(
                "Raft rejected node registration: {}",
                response.message
            )));
        }
        info!(node_id = %self.node_id, address = %self.address, "Node registered in Raft cluster");
        Ok(())
    }

    pub async fn run_until_cancelled(&self, shutdown: CancellationToken) -> Result<()> {
        let mut interval = tokio::time::interval(self.heartbeat_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(error) = self.heartbeat().await {
                        warn!(node_id = %self.node_id, %error, "Raft node heartbeat failed; retrying");
                    }
                    if self.raft.is_leader().await {
                        if let Err(error) = self.detect_failed_nodes().await {
                            warn!(node_id = %self.node_id, %error, "Node failure detection pass failed");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn heartbeat(&self) -> Result<()> {
        let response = self
            .raft
            .propose(Command::UpdateNodeHeartbeat {
                node_id: self.node_id.clone(),
                issued_at_ms: chrono::Utc::now().timestamp_millis(),
            })
            .await?;
        if !response.success {
            // Re-register after state restoration or an administrative record removal.
            let registered = self
                .raft
                .propose(Command::RegisterNode {
                    node_id: self.node_id.clone(),
                    address: self.address.clone(),
                    tags: self.tags.clone(),
                    issued_at_ms: chrono::Utc::now().timestamp_millis(),
                })
                .await?;
            if !registered.success {
                return Err(crate::error::DatspoolError::Internal(format!(
                    "Node heartbeat and re-registration rejected: {}",
                    response.message
                )));
            }
        }
        Ok(())
    }

    async fn detect_failed_nodes(&self) -> Result<()> {
        let now = chrono::Utc::now().timestamp_millis();
        let timeout_ms = self.failure_timeout.as_millis().min(i64::MAX as u128) as i64;
        for node in self.task_store.list_nodes().await? {
            if node.node_id.as_str() == self.node_id || node.status == NodeStatus::Offline {
                continue;
            }
            if now.saturating_sub(node.last_heartbeat_ms) < timeout_ms {
                continue;
            }
            let response = self
                .raft
                .propose(Command::MarkNodeOffline {
                    node_id: node.node_id.to_string(),
                    issued_at_ms: now,
                    heartbeat_timeout_ms: timeout_ms,
                })
                .await?;
            if response.success {
                let requeued = response
                    .data
                    .as_ref()
                    .and_then(|data| data.get("requeued_shards"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
                info!(failed_node = %node.node_id, requeued_shards = requeued, "Marked unresponsive node offline");
            }
        }
        Ok(())
    }
}
