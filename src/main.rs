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
    let config = config_path
        .as_deref()
        .map(DatspoolConfig::from_file)
        .transpose()?;

    let admin_token = std::env::var("DATASPOOL_ADMIN_TOKEN").map_err(|_| {
        DatspoolError::Configuration("DATASPOOL_ADMIN_TOKEN must be set (minimum 32 bytes)".into())
    })?;
    if let Some(config) = config.as_ref() {
        if config.storage.engine != dataspool::config::StorageEngine::RocksDb {
            return Err(DatspoolError::Configuration(
                "This binary currently supports only the RocksDB storage engine".into(),
            ));
        }
        if config.server.tls.enabled {
            return Err(DatspoolError::Configuration(
                "HTTP TLS is not terminated by Dataspool; configure TLS at a trusted reverse proxy and set server.tls.enabled to false".into(),
            ));
        }
    }

    let db_path = std::env::var("DATASPOOL_DB_PATH")
        .ok()
        .or_else(|| config.as_ref().map(|value| value.storage.data_dir.display().to_string()))
        .unwrap_or_else(|| "./data/dataspool".into());
    let bind = std::env::var("DATASPOOL_HTTP_BIND")
        .ok()
        .or_else(|| config.as_ref().map(|value| value.http_addr().to_string()))
        .unwrap_or_else(|| "127.0.0.1:8080".into());

    let store = dataspool::RocksDbStore::open(&db_path)?;
    let tasks = Arc::new(TaskStore::new(store.clone()));
    let api = dataspool::DlqApi::new(tasks.clone(), admin_token)?;
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| DatspoolError::Internal(format!("HTTP bind {bind} failed: {e}")))?;

    let shutdown = CancellationToken::new();
    let worker = if let Some(config) = config.as_ref().filter(|value| value.agent.enabled) {
        if config.cluster.peers.len() > 1 {
            return Err(DatspoolError::Configuration(
                "Agent execution is currently single-node only: multi-node shard claiming must be connected to Raft before enabling agents".into(),
            ));
        }
        if config.agent.resources.max_concurrent_tasks == 0 {
            return Err(DatspoolError::Configuration(
                "agent.resources.max_concurrent_tasks must be greater than zero".into(),
            ));
        }

        let node_id = config.node_id().to_string();
        let recovered = tasks.recover_interrupted_shards(&node_id).await?;
        if recovered > 0 {
            warn!(node_id = %node_id, recovered, "Recovered interrupted shards from previous process");
        }

        let index_store = Arc::new(RwLock::new(dataspool::storage::index::TieredIndexStore::new(
            dataspool::storage::index::TieredConfig::default(),
            Arc::new(store),
        )));
        let mut pool = WorkerPool::new(
            node_id.clone(),
            config.agent.resources.max_concurrent_tasks,
            tasks.clone(),
            index_store,
        )?;
        let worker_shutdown = shutdown.clone();
        info!(
            node_id = %node_id,
            max_concurrent_tasks = config.agent.resources.max_concurrent_tasks,
            recovered_shards = recovered,
            "Starting Dataspool agent worker pool"
        );
        Some(tokio::spawn(async move {
            pool.run_until_cancelled(worker_shutdown).await
        }))
    } else {
        info!("Agent worker pool disabled; running admin API only");
        None
    };

    info!(%bind, %db_path, "Starting Dataspool DLQ admin API");
    if !bind.starts_with("127.0.0.1:") && !bind.starts_with("[::1]:") {
        warn!(
            "DLQ admin API is bound to a non-loopback address; terminate TLS and restrict network access at a trusted reverse proxy"
        );
    }

    let server = axum::serve(listener, api.router())
        .with_graceful_shutdown(wait_for_shutdown(shutdown.clone()));
    let server_result = server
        .await
        .map_err(|e| DatspoolError::Internal(format!("HTTP server failed: {e}")));

    shutdown.cancel();
    if let Some(worker) = worker {
        worker
            .await
            .map_err(|e| DatspoolError::Internal(format!("Agent worker task failed: {e}")))??;
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
