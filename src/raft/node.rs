// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Raft node - high-level interface for consensus operations

use tracing::info;
use crate::error::Result;
use super::{Command, MetaStateMachine, RaftNetwork};
use crate::storage::KvStore;

/// Raft node representing this instance in the cluster
pub struct RaftNode<S: KvStore> {
    #[allow(dead_code)]
    node_id: String,
    state_machine: MetaStateMachine<S>,
    #[allow(dead_code)]
    network: RaftNetwork,
}

impl<S: KvStore> RaftNode<S> {
    pub fn new(node_id: String, store: S) -> Self {
        Self {
            node_id: node_id.clone(),
            state_machine: MetaStateMachine::new(store),
            network: RaftNetwork::new(node_id),
        }
    }

    /// Propose a command (only Leader can propose)
    pub async fn propose(&mut self, cmd: Command) -> Result<()> {
        info!("Proposing command: {}", cmd.description());
        // TODO: Integrate with openraft::Raft
        // For now, directly apply to state machine (single-node mode)
        let response = self.state_machine.apply_command(cmd).await?;
        info!("Command applied: {}", response.message);
        Ok(())
    }

    /// Check if this node is the Leader
    pub async fn is_leader(&self) -> bool {
        // TODO: Query openraft::Raft
        true  // Single-node always leader
    }

    /// Get current Leader ID
    pub async fn get_leader_id(&self) -> Option<String> {
        // TODO: Query openraft::Raft
        Some(self.node_id.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RocksDbStore;

    #[tokio::test]
    async fn test_raft_node_creation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = RocksDbStore::open(temp_dir.path()).unwrap();
        
        let node = RaftNode::new("node-1".to_string(), store);
        assert_eq!(node.node_id, "node-1");
        assert!(node.is_leader().await);
    }
}
