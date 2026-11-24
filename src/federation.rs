// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 Dataspool Contributors

//! Federated cluster registry and cross-cluster migration control-plane records.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FederatedClusterStatus {
    Healthy,
    Degraded,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederatedCluster {
    pub cluster_id: String,
    pub endpoint: String,
    /// SHA-256 fingerprint of the remote control-plane identity certificate.
    pub identity_fingerprint: String,
    pub version: String,
    pub capabilities: BTreeSet<String>,
    pub status: FederatedClusterStatus,
    pub last_seen_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MigrationState {
    Planned,
    Preparing,
    Running,
    Committing,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederatedMigration {
    pub migration_id: String,
    pub source_cluster: String,
    pub target_cluster: String,
    pub task_id: String,
    #[serde(default)]
    pub required_capabilities: BTreeSet<String>,
    pub state: MigrationState,
    /// Incremented on every explicit retry; used to fence stale stage reports.
    pub generation: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub last_error: Option<String>,
}

impl FederatedMigration {
    pub fn transition(
        &mut self,
        next: MigrationState,
        expected_generation: u64,
        at_ms: i64,
        error: Option<String>,
    ) -> Result<(), String> {
        if self.generation != expected_generation {
            return Err(format!(
                "stale migration generation: expected {}, current {}",
                expected_generation, self.generation
            ));
        }
        let allowed = matches!(
            (&self.state, &next),
            (
                MigrationState::Planned,
                MigrationState::Preparing | MigrationState::Cancelled
            ) | (
                MigrationState::Preparing,
                MigrationState::Running | MigrationState::Failed | MigrationState::Cancelled
            ) | (
                MigrationState::Running,
                MigrationState::Committing | MigrationState::Failed | MigrationState::Cancelled
            ) | (
                MigrationState::Committing,
                MigrationState::Completed | MigrationState::Failed
            ) | (
                MigrationState::Failed,
                MigrationState::Preparing | MigrationState::Cancelled
            )
        );
        if !allowed {
            return Err(format!(
                "invalid migration transition: {:?} -> {:?}",
                self.state, next
            ));
        }
        if self.state == MigrationState::Failed && next == MigrationState::Preparing {
            self.generation = self.generation.saturating_add(1);
        }
        self.state = next;
        self.updated_at_ms = at_ms;
        self.last_error = error;
        Ok(())
    }
}

pub fn validate_cluster(cluster: &FederatedCluster) -> Result<(), String> {
    if cluster.cluster_id.trim().is_empty() || cluster.cluster_id.len() > 128 {
        return Err("cluster_id must contain 1-128 characters".into());
    }
    if cluster.version.trim().is_empty() || cluster.version.len() > 64 {
        return Err("version must contain 1-64 characters".into());
    }
    let parsed = url::Url::parse(&cluster.endpoint)
        .map_err(|error| format!("invalid cluster endpoint: {error}"))?;
    let local = parsed
        .host_str()
        .is_some_and(|host| host == "localhost" || host == "127.0.0.1" || host == "::1");
    if !(parsed.scheme() == "https" || (parsed.scheme() == "http" && local)) {
        return Err(
            "federated cluster endpoint must use HTTPS (HTTP is allowed only for loopback)".into(),
        );
    }
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(
            "cluster endpoint must be an origin URL without credentials, query, or fragment".into(),
        );
    }
    if cluster.identity_fingerprint.len() != 64
        || !cluster
            .identity_fingerprint
            .bytes()
            .all(|b| b.is_ascii_hexdigit())
    {
        return Err("identity_fingerprint must be a 64-character SHA-256 hex digest".into());
    }
    if cluster
        .capabilities
        .iter()
        .any(|cap| cap.trim().is_empty() || cap.len() > 128)
    {
        return Err("cluster capabilities must contain 1-128 characters".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_endpoint_requires_secure_origin_and_valid_fingerprint() {
        let mut cluster = FederatedCluster {
            cluster_id: "east".into(),
            endpoint: "https://east.example.com:9443".into(),
            identity_fingerprint: "a".repeat(64),
            version: "0.1.0".into(),
            capabilities: BTreeSet::new(),
            status: FederatedClusterStatus::Healthy,
            last_seen_at_ms: 1,
            updated_at_ms: 1,
        };
        assert!(validate_cluster(&cluster).is_ok());
        cluster.endpoint = "http://east.example.com:9443".into();
        assert!(validate_cluster(&cluster).is_err());
        cluster.endpoint = "http://localhost:9443".into();
        assert!(validate_cluster(&cluster).is_ok());
        cluster.endpoint = "https://user:secret@east.example.com".into();
        assert!(validate_cluster(&cluster).is_err());
        cluster.endpoint = "https://east.example.com".into();
        cluster.identity_fingerprint = "not-a-fingerprint".into();
        assert!(validate_cluster(&cluster).is_err());
    }

    #[test]
    fn migration_transition_graph_is_terminal_and_generation_fenced() {
        let mut migration = FederatedMigration {
            migration_id: "m1".into(),
            source_cluster: "a".into(),
            target_cluster: "b".into(),
            task_id: "t1".into(),
            required_capabilities: BTreeSet::new(),
            state: MigrationState::Planned,
            generation: 0,
            created_at_ms: 1,
            updated_at_ms: 1,
            last_error: None,
        };
        assert!(
            migration
                .transition(MigrationState::Preparing, 0, 2, None)
                .is_ok()
        );
        assert!(
            migration
                .transition(MigrationState::Running, 0, 3, None)
                .is_err()
        );
        assert!(
            migration
                .transition(MigrationState::Running, 0, 3, None)
                .is_ok()
        );
        assert!(
            migration
                .transition(MigrationState::Committing, 0, 4, None)
                .is_ok()
        );
        assert!(
            migration
                .transition(MigrationState::Completed, 0, 5, None)
                .is_ok()
        );
        assert!(
            migration
                .transition(MigrationState::Failed, 0, 6, None)
                .is_err()
        );
    }
}
