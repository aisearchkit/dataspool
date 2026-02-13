// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Raft network layer - gRPC-based RPC implementation

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Network layer for Raft communication
///
/// Uses gRPC to send AppendEntries, RequestVote, etc. between nodes
pub struct RaftNetwork {
    #[allow(dead_code)]
    node_id: String,
    peers: Arc<RwLock<HashMap<String, String>>>,  // node_id -> address
}

impl RaftNetwork {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a peer node
    pub async fn add_peer(&self, node_id: String, address: String) {
        info!("Adding peer: {} @ {}", node_id, address);
        self.peers.write().await.insert(node_id, address);
    }

    /// Remove a peer node
    pub async fn remove_peer(&self, node_id: &str) {
        info!("Removing peer: {}", node_id);
        self.peers.write().await.remove(node_id);
    }

    /// Get all peer addresses
    pub async fn get_peers(&self) -> HashMap<String, String> {
        self.peers.read().await.clone()
    }
}

// NOTE: Full openraft::RaftNetwork implementation would go here
// This would use GrpcClient to send RPCs to peers
