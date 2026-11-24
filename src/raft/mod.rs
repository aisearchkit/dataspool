// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! OpenRaft consensus and replicated shard ownership state.

mod command;
mod network;
mod node;
mod state_machine;
mod storage;

pub use command::Command;
pub use network::router as raft_router;
#[cfg(test)]
pub(crate) use network::test_router_with_shared_raft;
pub use node::{Raft, RaftNode, stable_node_id};
pub use state_machine::{CommandResponse, MetaStateMachine};
pub use storage::{RocksNodeId, TypeConfig};

use openraft::Config;
use std::time::Duration;

pub fn default_raft_config() -> Config {
    raft_config(Duration::from_millis(500), Duration::from_millis(1500))
}

pub fn raft_config(heartbeat: Duration, election_timeout: Duration) -> Config {
    let heartbeat_ms = heartbeat.as_millis().clamp(100, u64::MAX as u128) as u64;
    let election_ms = election_timeout
        .as_millis()
        .clamp((heartbeat_ms.saturating_mul(3)) as u128, u64::MAX as u128)
        as u64;
    Config {
        cluster_name: "dataspool".to_string(),
        heartbeat_interval: heartbeat_ms,
        election_timeout_min: election_ms,
        election_timeout_max: election_ms.saturating_mul(2),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_timing_respects_configured_durations() {
        let config = raft_config(Duration::from_millis(250), Duration::from_secs(2));
        assert_eq!(config.heartbeat_interval, 250);
        assert_eq!(config.election_timeout_min, 2000);
        assert_eq!(config.election_timeout_max, 4000);
    }

    #[test]
    fn node_ids_are_stable_and_nonzero() {
        assert_eq!(stable_node_id("node-a"), stable_node_id("node-a"));
        assert_ne!(stable_node_id("node-a"), stable_node_id("node-b"));
        assert_ne!(stable_node_id("node-a"), 0);
    }
}
