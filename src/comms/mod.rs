// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Communication layer - gRPC server and client for cluster communication

mod client;
mod generated;
mod server;

pub use client::GrpcClient;
pub use generated::*;
pub use server::GrpcServer;

use crate::error::Result;
use crate::raft::RaftNode;
use crate::storage::RocksDbStore;
use crate::task::TaskStore;
use std::sync::Arc;

/// Initialize the gRPC communication layer
pub async fn init_comms(
    bind_addr: &str,
    task_store: Arc<TaskStore<RocksDbStore>>,
    raft_node: Option<Arc<RaftNode>>,
) -> Result<GrpcServer> {
    GrpcServer::new(bind_addr, task_store, raft_node).await
}
