// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use dataspool::{
    CancellationToken, DatspoolConfig, DatspoolError, Result, TaskStore, WorkerPool,
};
use openraft::BasicNode;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let config_path = std::env::var("DATASPOOL_CONFIG").ok();
    let config = config_path.as_deref().map(DatspoolConfig::from_file).transpose()?;
    let admin_token = std::env::var("DATASPOOL_ADMIN_TOKEN").map_err(|_| {
        DatspoolError::Configuration("DATASPOOL_ADMIN_TOKEN must be set (minimum 32 bytes)".into())
    })?;
    let db_path = std::env::var("DATASPOOL_DB_PATH")
        .ok()
        .or_else(|| config.as_ref().map(|value| value.storage.data_dir.display().to_string()))
        .unwrap_or_else(|| "./data/dataspool".into());
    let bind = std::env::var("DATASPOOL_HTTP_BIND")
        .ok()
        .or_else(|| config.as_ref().map(|value| value.http_addr().to_string()))
        .unwrap_or_else(|| "127.0.0.1:8080".into());

    if let Some(config) = config.as_ref() {
        if config.storage.engine != dataspool::config::StorageEngine::RocksDb {
            return Err(DatspoolError::Configuration("Only RocksDB storage is currently supported".into()));
        }
        if config.server.tls.enabled {
            return Err(DatspoolError::Configuration("Terminate TLS at a trusted reverse proxy; native TLS is not enabled".into()));
        }
    }

    let store = dataspool::RocksDbStore::open(&db_path)?;
    let tasks = Arc::new(TaskStore::new(store.clone()));
    let api = dataspool::DlqApi::new(tasks.clone(), admin_token)?;
    let listener = tokio::net::TcpListener::bind(&bind).await
        .map_err(|e| DatspoolError::Internal(format!("HTTP bind {bind} failed: {e}")))?;

    let shutdown = CancellationToken::new();
    let mut raft_node = None;
    let mut raft_server_task = None;
    let worker = if let Some(config) = config.as_ref().filter(|value| value.agent.enabled) {
        let node_name = config.node_id().to_string();
        let node_id = dataspool::raft::stable_node_id(&node_name);
        let raft_token = std::env::var("DATASPOOL_RAFT_TOKEN").map_err(|_| {
            DatspoolError::Configuration("DATASPOOL_RAFT_TOKEN must be set for Agent/Raft mode (minimum 32 bytes, shared by all cluster members)".into())
        })?;
        if raft_token.len() < 32 {
            return Err(DatspoolError::Configuration("DATASPOOL_RAFT_TOKEN must contain at least 32 bytes".into()));
        }

        let raft_advertise = std::env::var("DATASPOOL_RAFT_ADVERTISE_ADDR").ok()
            .unwrap_or_else(|| format!("{}:{}", config.server.bind_addr, config.server.grpc_port));
        if !config.cluster.peers.is_empty() && (raft_advertise.starts_with("0.0.0.0:") || raft_advertise.starts_with("[::]:")) {
            return Err(DatspoolError::Configuration("Set DATASPOOL_RAFT_ADVERTISE_ADDR to a routable address when running a multi-node cluster".into()));
        }
        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new(raft_advertise));
        for peer in &config.cluster.peers {
            let (peer_id, address) = peer.split_once('@').unwrap_or((peer.as_str(), peer.as_str()));
            if address.is_empty() || peer_id.is_empty() {
                return Err(DatspoolError::Configuration(format!("Invalid cluster peer '{peer}'; expected node-id@host:port")));
            }
            let id = dataspool::raft::stable_node_id(peer_id);
            if members.insert(id, BasicNode::new(address.to_string())).is_some() {
                return Err(DatspoolError::Configuration(format!("Duplicate Raft node identity '{peer_id}'")));
            }
        }

        let raft_config = dataspool::raft::raft_config(config.cluster.heartbeat_interval, config.cluster.election_timeout);
        let raft = dataspool::RaftNode::new(
            node_id,
            std::path::Path::new(&db_path).join("raft"),
            (*tasks).clone(),
            raft_config,
            raft_token.clone(),
        ).await?;
        // Clear a brand-new follower's unreplicated legacy state before exposing
        // its RPC listener, so AppendEntries cannot race with local cleanup.
        let bootstrap_node = *members.keys().next().ok_or_else(|| DatspoolError::Configuration("Raft membership cannot be empty".into()))?;
        if node_id != bootstrap_node {
            let removed = raft.prepare_join(&tasks).await?;
            if removed > 0 {
                info!(removed_records = removed, "Cleared unreplicated local state before joining the Raft cluster");
            }
        }
        let raft_bind = std::env::var("DATASPOOL_RAFT_BIND")
            .unwrap_or_else(|_| config.grpc_addr().to_string());
        let raft_listener = tokio::net::TcpListener::bind(&raft_bind).await
            .map_err(|e| DatspoolError::Internal(format!("Raft RPC bind {raft_bind} failed: {e}")))?;
        let raft_router = dataspool::raft_router(raft.handle(), raft_token);
        let raft_shutdown = shutdown.clone();
        raft_server_task = Some(tokio::spawn(async move {
            axum::serve(raft_listener, raft_router)
                .with_graceful_shutdown(raft_shutdown.cancelled_owned())
                .await
                .map_err(|e| DatspoolError::Internal(format!("Raft RPC server failed: {e}")))
        }));

        if !raft.is_initialized().await? && node_id == bootstrap_node {
            // Avoid re-initializing a cluster if this node's local Raft directory
            // was lost but an existing leader is already reachable.
            if raft.wait_for_leader(std::time::Duration::from_secs(3)).await.is_err() {
                raft.initialize(members.clone()).await?;
            }
        }
        let elected_leader = raft.wait_for_leader(std::time::Duration::from_secs(60)).await?;
        if node_id == bootstrap_node {
            let replicated = raft.bootstrap_local_state(&tasks).await?;
            info!(replicated_tasks = replicated, leader_id = elected_leader, "Replicated bootstrap tasks and legacy cache through Raft");
        } else {
            raft.wait_for_bootstrap_complete(&tasks, std::time::Duration::from_secs(300)).await?;
        }

        let index_store = Arc::new(RwLock::new(dataspool::storage::index::TieredIndexStore::new(
            dataspool::storage::index::TieredConfig::default(), Arc::new(store.clone()),
        )));
        let pool = WorkerPool::new(
            node_name.clone(),
            config.agent.resources.max_concurrent_tasks,
            tasks.clone(),
            index_store,
            Arc::new(raft.clone()),
        )?;
        let worker_shutdown = shutdown.clone();
        info!(node_id = %node_name, raft_node_id = node_id, cluster_members = members.len(), "Starting Raft-backed Agent worker pool");
        raft_node = Some(raft);
        Some(tokio::spawn(async move {
            let mut pool = pool;
            pool.run_until_cancelled(worker_shutdown).await
        }))
    } else {
        info!("Agent worker pool disabled; running admin API only");
        None
    };

    info!(%bind, %db_path, "Starting Dataspool DLQ admin API");
    if !bind.starts_with("127.0.0.1:") && !bind.starts_with("[::1]:") {
        warn!("DLQ admin API is bound to a non-loopback address; terminate TLS and restrict network access at a trusted reverse proxy");
    }

    let server = axum::serve(listener, api.router())
        .with_graceful_shutdown(wait_for_shutdown(shutdown.clone()));
    let server_result = server.await
        .map_err(|e| DatspoolError::Internal(format!("HTTP server failed: {e}")));
    shutdown.cancel();

    if let Some(worker) = worker {
        worker.await.map_err(|e| DatspoolError::Internal(format!("Agent worker task failed: {e}")))??;
    }
    if let Some(raft_server) = raft_server_task {
        raft_server.await.map_err(|e| DatspoolError::Internal(format!("Raft RPC task failed: {e}")))??;
    }
    if let Some(raft) = raft_node {
        raft.shutdown().await?;
    }
    server_result
}

async fn wait_for_shutdown(shutdown: CancellationToken) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    warn!(%error, "Failed to listen for Ctrl-C");
                }
            }
            _ = terminate.recv() => {
                info!("Received SIGTERM");
            }
        }
    }
    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        warn!(%error, "Failed to listen for Ctrl-C");
    }
    shutdown.cancel();
    info!("Shutdown signal received");
}
