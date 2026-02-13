// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Raft consensus layer - distributed metadata synchronization

mod command;
mod state_machine;
mod network;
mod node;

pub use command::Command;
pub use state_machine::MetaStateMachine;
pub use network::RaftNetwork;
pub use node::RaftNode;

use openraft::Config;

/// Initialize Raft configuration with defaults
pub fn default_raft_config() -> Config {
    Config {
        cluster_name: "dataspool".to_string(),
        heartbeat_interval: 500,  // 500ms
        election_timeout_min: 1500,  // 1.5s
        election_timeout_max: 3000,  // 3s
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = default_raft_config();
        assert_eq!(config.cluster_name, "dataspool");
        assert_eq!(config.heartbeat_interval, 500);
    }
}
