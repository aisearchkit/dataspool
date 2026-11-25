// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 Dataspool Contributors

//! Federated cluster registry and cross-cluster migration control-plane records.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

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
    /// Previous identity remains trusted only until this deadline.
    #[serde(default)]
    pub previous_identity_fingerprint: Option<String>,
    #[serde(default)]
    pub previous_identity_expires_at_ms: Option<i64>,
    /// Candidate identity accepted only by the proof RPC, never ordinary RPCs.
    #[serde(default)]
    pub pending_identity_fingerprint: Option<String>,
    #[serde(default)]
    pub rotation_started_at_ms: Option<i64>,
    #[serde(default)]
    pub pending_rotation_grace_period_ms: Option<i64>,
    /// Active PEM trust bundle used to validate peer client certificates.
    /// Public CA material is replicated through Raft; private keys are never stored here.
    #[serde(default)]
    pub trust_bundle_pem: Option<String>,
    #[serde(default)]
    pub trust_bundle_fingerprint: Option<String>,
    /// Candidate CA bundle is staged but is not trusted until activation.
    #[serde(default)]
    pub pending_trust_bundle_pem: Option<String>,
    #[serde(default)]
    pub pending_trust_bundle_fingerprint: Option<String>,
    #[serde(default)]
    pub trust_bundle_staged_at_ms: Option<i64>,
    /// Previous CA bundle remains trusted only until this committed deadline.
    #[serde(default)]
    pub previous_trust_bundle_pem: Option<String>,
    #[serde(default)]
    pub previous_trust_bundle_fingerprint: Option<String>,
    #[serde(default)]
    pub previous_trust_bundle_expires_at_ms: Option<i64>,
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
    Paused,
    Committing,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FederatedRemoteOperationState {
    Prepared,
    /// Durable fence written before any destructive compensation side effect.
    Compensating,
    Committed,
    Compensated,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederatedRemoteOperation {
    pub migration_id: String,
    pub source_cluster: String,
    pub generation: u64,
    pub task_id: String,
    pub state: FederatedRemoteOperationState,
    pub checkpoint_digest: Option<String>,
    pub updated_at_ms: i64,
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
    /// Incremented for failed-attempt retries; pause/resume keeps the same attempt generation.
    pub generation: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub last_error: Option<String>,
    #[serde(default)]
    pub task_name: String,
    #[serde(default)]
    pub source_config: String,
    #[serde(default)]
    pub sink_config: String,
    #[serde(default)]
    pub transform_config: Option<String>,
    #[serde(default)]
    pub sharding_strategy: Option<String>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    #[serde(default)]
    pub remote_task_id: Option<String>,
    #[serde(default)]
    pub remote_task_state: Option<String>,
    #[serde(default)]
    pub remote_checkpoint: Option<serde_json::Value>,
    #[serde(default)]
    pub last_reconciled_at_ms: Option<i64>,
    #[serde(default)]
    pub deadline_at_ms: Option<i64>,
    #[serde(default)]
    pub execution_budget_ms: Option<u64>,
    #[serde(default)]
    pub retry_budget: Option<u32>,
    #[serde(default)]
    pub timeout_policy: MigrationTimeoutPolicy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum MigrationTimeoutPolicy {
    #[default]
    Pause,
    Cancel,
    Fail,
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
                MigrationState::Preparing
                    | MigrationState::Paused
                    | MigrationState::Failed
                    | MigrationState::Cancelled
            ) | (
                MigrationState::Preparing,
                MigrationState::Running
                    | MigrationState::Paused
                    | MigrationState::Failed
                    | MigrationState::Cancelled
            ) | (
                MigrationState::Running,
                MigrationState::Paused
                    | MigrationState::Committing
                    | MigrationState::Failed
                    | MigrationState::Cancelled
            ) | (
                MigrationState::Paused,
                MigrationState::Preparing
                    | MigrationState::Committing
                    | MigrationState::Cancelled
                    | MigrationState::Failed
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
            self.remote_task_id = None;
            self.remote_task_state = None;
            self.remote_checkpoint = None;
            self.last_reconciled_at_ms = None;
        }
        self.state = next;
        self.updated_at_ms = at_ms;
        self.last_error = error;
        Ok(())
    }
}

impl FederatedCluster {
    /// Return the CA material that should be trusted at this instant. The old bundle
    /// is included only before its Raft-committed expiry deadline.
    pub fn effective_trust_bundle(&self, now_ms: i64) -> Option<String> {
        let active = self.trust_bundle_pem.as_deref()?;
        let previous = self
            .previous_trust_bundle_expires_at_ms
            .filter(|expires| now_ms < *expires)
            .and(self.previous_trust_bundle_pem.as_deref());
        Some(match previous {
            Some(previous) => format!("{}\n{}", active.trim_end(), previous.trim_start()),
            None => active.to_string(),
        })
    }

    pub fn accepts_fingerprint(&self, fingerprint: &str, now_ms: i64) -> bool {
        self.identity_fingerprint.eq_ignore_ascii_case(fingerprint)
            || (self
                .previous_identity_expires_at_ms
                .is_some_and(|expires| now_ms < expires)
                && self
                    .previous_identity_fingerprint
                    .as_deref()
                    .is_some_and(|previous| previous.eq_ignore_ascii_case(fingerprint)))
    }
}

/// Validate a PEM CA bundle and return its stable SHA-256 fingerprint.
/// Fingerprints are calculated over the concatenated DER certificates, not PEM whitespace.
pub fn validate_trust_bundle(pem: &str) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    use std::io::Cursor;

    const MAX_BUNDLE_BYTES: usize = 1024 * 1024;
    const MAX_CERTIFICATES: usize = 32;
    if pem.trim().is_empty() || pem.len() > MAX_BUNDLE_BYTES {
        return Err("CA trust bundle must contain 1 byte to 1 MiB of PEM data".into());
    }
    let mut reader = Cursor::new(pem.as_bytes());
    let items = rustls_pemfile::read_all(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| format!("invalid CA bundle PEM: {error}"))?;
    let mut certificates = Vec::with_capacity(items.len());
    for item in items {
        match item {
            rustls_pemfile::Item::X509Certificate(certificate) => certificates.push(certificate),
            _ => return Err("CA trust bundle may contain only CERTIFICATE PEM blocks; private keys and other PEM objects are forbidden".into()),
        }
    }
    if certificates.is_empty() || certificates.len() > MAX_CERTIFICATES {
        return Err("CA trust bundle must contain between 1 and 32 certificates".into());
    }
    let mut digest = Sha256::new();
    let mut roots = rustls::RootCertStore::empty();
    for certificate in certificates {
        if certificate.is_empty() {
            return Err("CA trust bundle contains an empty certificate".into());
        }
        roots
            .add(certificate.clone())
            .map_err(|error| format!("invalid CA certificate in trust bundle: {error}"))?;
        digest.update(certificate.as_ref());
    }
    Ok(format!("{:x}", digest.finalize()))
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
        || parsed.path() != "/"
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
    fn legacy_registry_records_deserialize_without_rotation_fields() {
        let cluster: FederatedCluster = serde_json::from_value(serde_json::json!({
            "cluster_id": "legacy-peer",
            "endpoint": "https://legacy.example.com",
            "identity_fingerprint": "a".repeat(64),
            "version": "0.1.0",
            "capabilities": [],
            "status": "healthy",
            "last_seen_at_ms": 1,
            "updated_at_ms": 1
        }))
        .unwrap();
        assert!(cluster.previous_identity_fingerprint.is_none());
        assert!(cluster.previous_identity_expires_at_ms.is_none());
        assert!(cluster.pending_identity_fingerprint.is_none());
        assert!(cluster.rotation_started_at_ms.is_none());
        assert!(cluster.pending_rotation_grace_period_ms.is_none());
    }

    #[test]
    fn cluster_endpoint_requires_secure_origin_and_valid_fingerprint() {
        let mut cluster = FederatedCluster {
            cluster_id: "east".into(),
            endpoint: "https://east.example.com:9443".into(),
            identity_fingerprint: "a".repeat(64),
            previous_identity_fingerprint: None,
            previous_identity_expires_at_ms: None,
            pending_identity_fingerprint: None,
            rotation_started_at_ms: None,
            pending_rotation_grace_period_ms: None,
            trust_bundle_pem: None,
            trust_bundle_fingerprint: None,
            pending_trust_bundle_pem: None,
            pending_trust_bundle_fingerprint: None,
            trust_bundle_staged_at_ms: None,
            previous_trust_bundle_pem: None,
            previous_trust_bundle_fingerprint: None,
            previous_trust_bundle_expires_at_ms: None,
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
    fn paused_migration_resume_preserves_generation_and_remote_progress() {
        let mut migration = FederatedMigration {
            migration_id: "m2".into(),
            source_cluster: "a".into(),
            target_cluster: "b".into(),
            task_id: "t2".into(),
            required_capabilities: BTreeSet::new(),
            state: MigrationState::Planned,
            generation: 0,
            created_at_ms: 1,
            updated_at_ms: 1,
            last_error: None,
            task_name: String::new(),
            source_config: String::new(),
            sink_config: String::new(),
            transform_config: None,
            sharding_strategy: None,
            priority: 1,
            tags: HashMap::new(),
            remote_task_id: Some("remote-0".into()),
            remote_task_state: Some("paused".into()),
            remote_checkpoint: Some(serde_json::json!({"offset": 10})),
            last_reconciled_at_ms: Some(2),
            deadline_at_ms: None,
            execution_budget_ms: None,
            retry_budget: None,
            timeout_policy: Default::default(),
        };
        migration.state = MigrationState::Paused;
        assert!(
            migration
                .transition(MigrationState::Preparing, 0, 3, None)
                .is_ok()
        );
        assert_eq!(migration.generation, 0);
        assert_eq!(migration.remote_task_id.as_deref(), Some("remote-0"));
        assert_eq!(migration.remote_task_state.as_deref(), Some("paused"));
        assert_eq!(migration.remote_checkpoint.as_ref().unwrap()["offset"], 10);
    }

    #[test]
    fn failed_migration_retry_advances_generation_and_discards_old_remote_progress() {
        let mut migration = FederatedMigration {
            migration_id: "m-failed".into(),
            source_cluster: "a".into(),
            target_cluster: "b".into(),
            task_id: "t-failed".into(),
            required_capabilities: BTreeSet::new(),
            state: MigrationState::Failed,
            generation: 4,
            created_at_ms: 1,
            updated_at_ms: 2,
            last_error: Some("injected failure".into()),
            task_name: "failed-retry".into(),
            source_config: "{}".into(),
            sink_config: "{}".into(),
            transform_config: None,
            sharding_strategy: None,
            priority: 1,
            tags: HashMap::new(),
            remote_task_id: Some("remote-old".into()),
            remote_task_state: Some("failed".into()),
            remote_checkpoint: Some(serde_json::json!({"offset": 10})),
            last_reconciled_at_ms: Some(2),
            deadline_at_ms: None,
            execution_budget_ms: None,
            retry_budget: None,
            timeout_policy: Default::default(),
        };
        migration
            .transition(MigrationState::Preparing, 4, 3, None)
            .unwrap();
        assert_eq!(migration.generation, 5);
        assert!(migration.remote_task_id.is_none());
        assert!(migration.remote_task_state.is_none());
        assert!(migration.remote_checkpoint.is_none());
        assert!(migration.last_reconciled_at_ms.is_none());
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
            task_name: String::new(),
            source_config: String::new(),
            sink_config: String::new(),
            transform_config: None,
            sharding_strategy: None,
            priority: 1,
            tags: Default::default(),
            remote_task_id: None,
            remote_task_state: None,
            remote_checkpoint: None,
            last_reconciled_at_ms: None,
            deadline_at_ms: None,
            execution_budget_ms: None,
            retry_budget: None,
            timeout_policy: Default::default(),
        };
        assert!(
            migration
                .transition(MigrationState::Preparing, 0, 2, None)
                .is_ok()
        );
        assert!(
            migration
                .transition(MigrationState::Running, 1, 3, None)
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

#[path = "federation/orchestrator.rs"]
pub mod orchestrator;
