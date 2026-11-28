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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FederatedTrustRotationPhase {
    Staging,
    AwaitingDeployment,
    ActivationBarrier,
    Activating,
    Overlap,
    Retiring,
    Completed,
    RollingBack,
    RolledBack,
    RecoveryRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederatedTrustRotation {
    pub rotation_id: String,
    pub peer_cluster_id: String,
    /// Current peer CA pin observed when this rotation was created; prevents stale starts.
    pub current_peer_ca_fingerprint: Option<String>,
    /// The CA bundle this cluster must install on the peer to trust our new leaves.
    pub local_ca_bundle_pem: String,
    pub local_ca_bundle_fingerprint: String,
    /// The CA bundle this cluster installs locally to trust the peer's new leaves.
    pub peer_ca_bundle_pem: String,
    pub peer_ca_bundle_fingerprint: String,
    pub local_leaf_fingerprint: String,
    pub peer_leaf_fingerprint: String,
    pub overlap_ms: i64,
    pub phase: FederatedTrustRotationPhase,
    pub local_staged: bool,
    pub peer_staged: bool,
    pub local_deployment_ready: bool,
    pub peer_deployment_ready: bool,
    pub activation_barrier_committed: bool,
    pub local_activation_expires_at_ms: Option<i64>,
    pub peer_activation_expires_at_ms: Option<i64>,
    pub local_retired: bool,
    pub peer_retired: bool,
    pub rollback_reason: Option<String>,
    /// True only after the peer has durably entered the same pre-activation rollback.
    #[serde(default)]
    pub rollback_peer_confirmed: bool,
    pub last_error: Option<String>,
    pub revision: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Linearizable remote view used to decide whether pre-activation rollback is safe.
/// Rotation events and peer registry writes are separate Raft commands, so callers
/// inspect the actual trust pin as well as the workflow record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederatedTrustRotationSnapshot {
    pub rotation: FederatedTrustRotation,
    pub peer_active_ca_fingerprint: Option<String>,
    pub peer_pending_ca_fingerprint: Option<String>,
    pub peer_previous_ca_fingerprint: Option<String>,
    pub peer_previous_ca_expires_at_ms: Option<i64>,
    pub peer_active_identity_fingerprint: String,
    pub peer_pending_identity_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "event", content = "data")]
pub enum FederatedTrustRotationEvent {
    LocalStaged { fingerprint: String },
    PeerStaged { fingerprint: String },
    LocalDeploymentReady { leaf_fingerprint: String },
    PeerDeploymentReady { leaf_fingerprint: String },
    AuthorizeActivation,
    LocalActivated { previous_expires_at_ms: i64 },
    PeerActivated { previous_expires_at_ms: i64 },
    LocalRetired,
    PeerRetired,
    BeginRollback { reason: String },
    ConfirmRollback,
    LocalRolledBack,
    PeerRolledBack,
    MarkRecoveryRequired { reason: String },
}

impl FederatedTrustRotation {
    pub fn apply_event(
        &mut self,
        event: FederatedTrustRotationEvent,
        expected_revision: u64,
        at_ms: i64,
    ) -> Result<(), String> {
        if self.revision != expected_revision {
            return Err(format!(
                "stale trust rotation revision: expected {}, current {}",
                expected_revision, self.revision
            ));
        }
        match event {
            FederatedTrustRotationEvent::LocalStaged { fingerprint } => {
                if self.phase != FederatedTrustRotationPhase::Staging
                    || !fingerprint.eq_ignore_ascii_case(&self.peer_ca_bundle_fingerprint)
                {
                    return Err("local staged CA fingerprint or phase is invalid".into());
                }
                self.local_staged = true;
                if self.peer_staged {
                    self.phase = FederatedTrustRotationPhase::AwaitingDeployment;
                }
            }
            FederatedTrustRotationEvent::PeerStaged { fingerprint } => {
                if self.phase != FederatedTrustRotationPhase::Staging
                    || !fingerprint.eq_ignore_ascii_case(&self.local_ca_bundle_fingerprint)
                {
                    return Err("peer staged CA fingerprint or phase is invalid".into());
                }
                self.peer_staged = true;
                if self.local_staged {
                    self.phase = FederatedTrustRotationPhase::AwaitingDeployment;
                }
            }
            FederatedTrustRotationEvent::LocalDeploymentReady { leaf_fingerprint } => {
                if self.phase != FederatedTrustRotationPhase::AwaitingDeployment
                    || !self.local_staged
                    || !self.peer_staged
                    || !leaf_fingerprint.eq_ignore_ascii_case(&self.local_leaf_fingerprint)
                {
                    return Err("local deployment readiness requires both staged bundles and the expected leaf".into());
                }
                self.local_deployment_ready = true;
                if self.peer_deployment_ready {
                    self.phase = FederatedTrustRotationPhase::ActivationBarrier;
                }
            }
            FederatedTrustRotationEvent::PeerDeploymentReady { leaf_fingerprint } => {
                if self.phase != FederatedTrustRotationPhase::AwaitingDeployment
                    || !self.local_staged
                    || !self.peer_staged
                    || !leaf_fingerprint.eq_ignore_ascii_case(&self.peer_leaf_fingerprint)
                {
                    return Err("peer deployment readiness requires both staged bundles and the expected leaf".into());
                }
                self.peer_deployment_ready = true;
                if self.local_deployment_ready {
                    self.phase = FederatedTrustRotationPhase::ActivationBarrier;
                }
            }
            FederatedTrustRotationEvent::AuthorizeActivation => {
                if !self.local_deployment_ready || !self.peer_deployment_ready {
                    return Err(
                        "activation barrier requires deployment readiness on both sides".into(),
                    );
                }
                if self.activation_barrier_committed {
                    if !matches!(
                        self.phase,
                        FederatedTrustRotationPhase::Activating
                            | FederatedTrustRotationPhase::RecoveryRequired
                            | FederatedTrustRotationPhase::Overlap
                            | FederatedTrustRotationPhase::Retiring
                            | FederatedTrustRotationPhase::Completed
                    ) {
                        return Err(
                            "committed activation barrier has an invalid workflow phase".into()
                        );
                    }
                    if self.phase == FederatedTrustRotationPhase::RecoveryRequired {
                        self.phase = if self.local_activation_expires_at_ms.is_some()
                            && self.peer_activation_expires_at_ms.is_some()
                        {
                            FederatedTrustRotationPhase::Overlap
                        } else {
                            FederatedTrustRotationPhase::Activating
                        };
                    }
                } else {
                    if !matches!(
                        self.phase,
                        FederatedTrustRotationPhase::ActivationBarrier
                            | FederatedTrustRotationPhase::RecoveryRequired
                    ) {
                        return Err(
                            "activation barrier requires both deployment-ready acknowledgements"
                                .into(),
                        );
                    }
                    self.activation_barrier_committed = true;
                    self.phase = FederatedTrustRotationPhase::Activating;
                }
            }
            FederatedTrustRotationEvent::LocalActivated {
                previous_expires_at_ms,
            } => {
                self.apply_activation(true, previous_expires_at_ms, at_ms)?;
            }
            FederatedTrustRotationEvent::PeerActivated {
                previous_expires_at_ms,
            } => {
                self.apply_activation(false, previous_expires_at_ms, at_ms)?;
            }
            FederatedTrustRotationEvent::LocalRetired => {
                self.apply_retirement(true, at_ms)?;
            }
            FederatedTrustRotationEvent::PeerRetired => {
                self.apply_retirement(false, at_ms)?;
            }
            FederatedTrustRotationEvent::BeginRollback { reason } => {
                if self.local_activation_expires_at_ms.is_some()
                    || self.peer_activation_expires_at_ms.is_some()
                    || matches!(
                        self.phase,
                        FederatedTrustRotationPhase::Completed
                            | FederatedTrustRotationPhase::RolledBack
                            | FederatedTrustRotationPhase::RecoveryRequired
                    )
                {
                    return Err("rollback is forbidden after either side activates; forward recovery is required".into());
                }
                if reason.trim().is_empty() || reason.len() > 1024 {
                    return Err("rollback reason must contain 1-1024 characters".into());
                }
                if self.phase == FederatedTrustRotationPhase::RollingBack {
                    if self.rollback_reason.as_deref() != Some(reason.as_str()) {
                        return Err(
                            "rollback reason cannot change after rollback has started".into()
                        );
                    }
                } else {
                    self.rollback_reason = Some(reason);
                    self.rollback_peer_confirmed = false;
                    self.phase = FederatedTrustRotationPhase::RollingBack;
                }
            }
            FederatedTrustRotationEvent::ConfirmRollback => {
                if self.phase != FederatedTrustRotationPhase::RollingBack
                    || self.rollback_reason.is_none()
                    || self.local_activation_expires_at_ms.is_some()
                    || self.peer_activation_expires_at_ms.is_some()
                {
                    return Err(
                        "peer rollback confirmation requires a pre-activation rolling_back state"
                            .into(),
                    );
                }
                self.rollback_peer_confirmed = true;
            }
            FederatedTrustRotationEvent::LocalRolledBack => {
                if self.phase != FederatedTrustRotationPhase::RollingBack
                    || !self.rollback_peer_confirmed
                    || self.local_activation_expires_at_ms.is_some()
                    || self.peer_activation_expires_at_ms.is_some()
                {
                    return Err("local rollback acknowledgement requires bilateral pre-activation rollback confirmation".into());
                }
                self.local_staged = false;
                if !self.peer_staged {
                    self.phase = FederatedTrustRotationPhase::RolledBack;
                }
            }
            FederatedTrustRotationEvent::PeerRolledBack => {
                if self.phase != FederatedTrustRotationPhase::RollingBack
                    || !self.rollback_peer_confirmed
                    || self.local_activation_expires_at_ms.is_some()
                    || self.peer_activation_expires_at_ms.is_some()
                {
                    return Err("peer rollback acknowledgement requires bilateral pre-activation rollback confirmation".into());
                }
                self.peer_staged = false;
                if !self.local_staged {
                    self.phase = FederatedTrustRotationPhase::RolledBack;
                }
            }
            FederatedTrustRotationEvent::MarkRecoveryRequired { reason } => {
                if (!self.activation_barrier_committed
                    && self.phase != FederatedTrustRotationPhase::ActivationBarrier)
                    || matches!(
                        self.phase,
                        FederatedTrustRotationPhase::Completed
                            | FederatedTrustRotationPhase::RolledBack
                            | FederatedTrustRotationPhase::RollingBack
                    )
                {
                    return Err(
                        "recovery-required is valid only after the activation barrier".into(),
                    );
                }
                if reason.trim().is_empty() || reason.len() > 1024 {
                    return Err("recovery reason must contain 1-1024 characters".into());
                }
                self.last_error = Some(reason);
                self.phase = FederatedTrustRotationPhase::RecoveryRequired;
            }
        }
        self.revision = self.revision.saturating_add(1);
        self.updated_at_ms = at_ms;
        if !matches!(self.phase, FederatedTrustRotationPhase::RecoveryRequired) {
            self.last_error = None;
        }
        Ok(())
    }

    fn apply_activation(
        &mut self,
        local: bool,
        previous_expires_at_ms: i64,
        at_ms: i64,
    ) -> Result<(), String> {
        if !self.activation_barrier_committed
            || !matches!(
                self.phase,
                FederatedTrustRotationPhase::Activating
                    | FederatedTrustRotationPhase::RecoveryRequired
            )
            || previous_expires_at_ms != at_ms.saturating_add(self.overlap_ms)
        {
            return Err(
                "activation requires the committed barrier and the exact overlap deadline".into(),
            );
        }
        let deadline = if local {
            &mut self.local_activation_expires_at_ms
        } else {
            &mut self.peer_activation_expires_at_ms
        };
        match deadline {
            Some(existing) if *existing == previous_expires_at_ms => {}
            Some(_) => return Err("replayed activation cannot change the original expiry".into()),
            None => *deadline = Some(previous_expires_at_ms),
        }
        if self.local_activation_expires_at_ms.is_some()
            && self.peer_activation_expires_at_ms.is_some()
        {
            self.phase = FederatedTrustRotationPhase::Overlap;
        }
        Ok(())
    }

    fn apply_retirement(&mut self, local: bool, at_ms: i64) -> Result<(), String> {
        if self.local_activation_expires_at_ms.is_none()
            || self.peer_activation_expires_at_ms.is_none()
            || !matches!(
                self.phase,
                FederatedTrustRotationPhase::Overlap | FederatedTrustRotationPhase::Retiring
            )
        {
            return Err("retirement requires both sides to activate".into());
        }
        let deadline = self
            .local_activation_expires_at_ms
            .unwrap()
            .max(self.peer_activation_expires_at_ms.unwrap());
        if at_ms < deadline {
            return Err("both CA overlap deadlines must expire before retirement".into());
        }
        self.phase = FederatedTrustRotationPhase::Retiring;
        if local {
            self.local_retired = true;
        } else {
            self.peer_retired = true;
        }
        if self.local_retired && self.peer_retired {
            self.phase = FederatedTrustRotationPhase::Completed;
        }
        Ok(())
    }
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

    /// Return all roots usable during a TLS handshake, including a staged candidate.
    /// Staging expands trust for pre-activation deployment checks without changing the
    /// active CA pin recorded by the federation state machine.
    pub fn handshake_trust_bundle(&self, now_ms: i64) -> Option<String> {
        let mut bundles = Vec::new();
        if let Some(active) = self.trust_bundle_pem.as_deref() {
            bundles.push(active.trim().to_string());
        }
        if let Some(pending) = self.pending_trust_bundle_pem.as_deref() {
            bundles.push(pending.trim().to_string());
        }
        if let Some(previous) = self
            .previous_trust_bundle_expires_at_ms
            .filter(|expires| now_ms < *expires)
            .and(self.previous_trust_bundle_pem.as_deref())
        {
            bundles.push(previous.trim().to_string());
        }
        bundles.sort();
        bundles.dedup();
        (!bundles.is_empty()).then(|| bundles.join("\n"))
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

    fn trust_rotation_fixture() -> FederatedTrustRotation {
        FederatedTrustRotation {
            rotation_id: "rotate-peer-1".into(),
            peer_cluster_id: "peer-1".into(),
            current_peer_ca_fingerprint: Some("a".repeat(64)),
            local_ca_bundle_pem: "local-ca-pem".into(),
            local_ca_bundle_fingerprint: "b".repeat(64),
            peer_ca_bundle_pem: "peer-ca-pem".into(),
            peer_ca_bundle_fingerprint: "c".repeat(64),
            local_leaf_fingerprint: "d".repeat(64),
            peer_leaf_fingerprint: "e".repeat(64),
            overlap_ms: 60_000,
            phase: FederatedTrustRotationPhase::Staging,
            local_staged: false,
            peer_staged: false,
            local_deployment_ready: false,
            peer_deployment_ready: false,
            activation_barrier_committed: false,
            local_activation_expires_at_ms: None,
            peer_activation_expires_at_ms: None,
            local_retired: false,
            peer_retired: false,
            rollback_reason: None,
            rollback_peer_confirmed: false,
            last_error: None,
            revision: 0,
            created_at_ms: 1,
            updated_at_ms: 1,
        }
    }

    #[test]
    fn bilateral_trust_rotation_requires_both_stages_and_both_deployments_before_activation() {
        let mut rotation = trust_rotation_fixture();
        assert!(
            rotation
                .apply_event(FederatedTrustRotationEvent::AuthorizeActivation, 0, 10,)
                .is_err()
        );
        rotation
            .apply_event(
                FederatedTrustRotationEvent::LocalStaged {
                    fingerprint: "c".repeat(64),
                },
                0,
                11,
            )
            .unwrap();
        assert_eq!(rotation.phase, FederatedTrustRotationPhase::Staging);
        rotation
            .apply_event(
                FederatedTrustRotationEvent::PeerStaged {
                    fingerprint: "b".repeat(64),
                },
                1,
                12,
            )
            .unwrap();
        assert_eq!(
            rotation.phase,
            FederatedTrustRotationPhase::AwaitingDeployment
        );
        rotation
            .apply_event(
                FederatedTrustRotationEvent::LocalDeploymentReady {
                    leaf_fingerprint: "d".repeat(64),
                },
                2,
                13,
            )
            .unwrap();
        assert_eq!(
            rotation.phase,
            FederatedTrustRotationPhase::AwaitingDeployment
        );
        assert!(
            rotation
                .apply_event(FederatedTrustRotationEvent::AuthorizeActivation, 3, 14,)
                .is_err()
        );
        rotation
            .apply_event(
                FederatedTrustRotationEvent::PeerDeploymentReady {
                    leaf_fingerprint: "e".repeat(64),
                },
                3,
                15,
            )
            .unwrap();
        assert_eq!(
            rotation.phase,
            FederatedTrustRotationPhase::ActivationBarrier
        );
        rotation
            .apply_event(FederatedTrustRotationEvent::AuthorizeActivation, 4, 16)
            .unwrap();
        assert!(rotation.activation_barrier_committed);
        assert_eq!(rotation.phase, FederatedTrustRotationPhase::Activating);
    }

    #[test]
    fn trust_rotation_expiry_is_immutable_and_retirement_waits_for_both_sides() {
        let mut rotation = trust_rotation_fixture();
        for (event, revision, at) in [
            (
                FederatedTrustRotationEvent::LocalStaged {
                    fingerprint: "c".repeat(64),
                },
                0,
                10,
            ),
            (
                FederatedTrustRotationEvent::PeerStaged {
                    fingerprint: "b".repeat(64),
                },
                1,
                11,
            ),
            (
                FederatedTrustRotationEvent::LocalDeploymentReady {
                    leaf_fingerprint: "d".repeat(64),
                },
                2,
                12,
            ),
            (
                FederatedTrustRotationEvent::PeerDeploymentReady {
                    leaf_fingerprint: "e".repeat(64),
                },
                3,
                13,
            ),
            (FederatedTrustRotationEvent::AuthorizeActivation, 4, 14),
        ] {
            rotation.apply_event(event, revision, at).unwrap();
        }
        rotation
            .apply_event(
                FederatedTrustRotationEvent::LocalActivated {
                    previous_expires_at_ms: 60_015,
                },
                5,
                15,
            )
            .unwrap();
        assert!(
            rotation
                .apply_event(
                    FederatedTrustRotationEvent::LocalActivated {
                        previous_expires_at_ms: 90_015
                    },
                    6,
                    15,
                )
                .is_err()
        );
        rotation
            .apply_event(
                FederatedTrustRotationEvent::PeerActivated {
                    previous_expires_at_ms: 60_020,
                },
                6,
                20,
            )
            .unwrap();
        assert_eq!(rotation.phase, FederatedTrustRotationPhase::Overlap);
        assert!(
            rotation
                .apply_event(FederatedTrustRotationEvent::LocalRetired, 7, 60_019)
                .is_err()
        );
        rotation
            .apply_event(FederatedTrustRotationEvent::LocalRetired, 7, 60_020)
            .unwrap();
        assert_eq!(rotation.phase, FederatedTrustRotationPhase::Retiring);
        rotation
            .apply_event(FederatedTrustRotationEvent::PeerRetired, 8, 60_020)
            .unwrap();
        assert_eq!(rotation.phase, FederatedTrustRotationPhase::Completed);
    }

    #[test]
    fn trust_rotation_rollback_is_allowed_only_before_any_side_activates() {
        let mut rotation = trust_rotation_fixture();
        rotation
            .apply_event(
                FederatedTrustRotationEvent::LocalStaged {
                    fingerprint: "c".repeat(64),
                },
                0,
                10,
            )
            .unwrap();
        rotation
            .apply_event(
                FederatedTrustRotationEvent::PeerStaged {
                    fingerprint: "b".repeat(64),
                },
                1,
                11,
            )
            .unwrap();
        rotation
            .apply_event(
                FederatedTrustRotationEvent::BeginRollback {
                    reason: "peer deployment failed".into(),
                },
                2,
                12,
            )
            .unwrap();
        assert!(
            rotation
                .apply_event(FederatedTrustRotationEvent::LocalRolledBack, 3, 13)
                .is_err(),
            "a local-only BeginRollback must not authorize material removal"
        );
        assert!(
            rotation
                .apply_event(FederatedTrustRotationEvent::PeerRolledBack, 3, 13)
                .is_err(),
            "peer material removal must also wait for bilateral confirmation"
        );
        assert!(
            rotation
                .apply_event(
                    FederatedTrustRotationEvent::BeginRollback {
                        reason: "different reason".into(),
                    },
                    3,
                    13,
                )
                .is_err(),
            "rollback reason is immutable after the rollback fence is committed"
        );
        assert!(
            rotation
                .apply_event(
                    FederatedTrustRotationEvent::MarkRecoveryRequired {
                        reason: "must not supersede rollback".into(),
                    },
                    3,
                    13,
                )
                .is_err(),
            "recovery-required must not reopen the forward path after rollback starts"
        );
        rotation
            .apply_event(FederatedTrustRotationEvent::ConfirmRollback, 3, 13)
            .unwrap();
        assert!(rotation.rollback_peer_confirmed);
        rotation
            .apply_event(FederatedTrustRotationEvent::LocalRolledBack, 4, 14)
            .unwrap();
        assert_eq!(rotation.phase, FederatedTrustRotationPhase::RollingBack);
        rotation
            .apply_event(FederatedTrustRotationEvent::PeerRolledBack, 5, 15)
            .unwrap();
        assert_eq!(rotation.phase, FederatedTrustRotationPhase::RolledBack);

        let mut active = trust_rotation_fixture();
        for (event, revision, at) in [
            (
                FederatedTrustRotationEvent::LocalStaged {
                    fingerprint: "c".repeat(64),
                },
                0,
                10,
            ),
            (
                FederatedTrustRotationEvent::PeerStaged {
                    fingerprint: "b".repeat(64),
                },
                1,
                11,
            ),
            (
                FederatedTrustRotationEvent::LocalDeploymentReady {
                    leaf_fingerprint: "d".repeat(64),
                },
                2,
                12,
            ),
            (
                FederatedTrustRotationEvent::PeerDeploymentReady {
                    leaf_fingerprint: "e".repeat(64),
                },
                3,
                13,
            ),
            (FederatedTrustRotationEvent::AuthorizeActivation, 4, 14),
            (
                FederatedTrustRotationEvent::LocalActivated {
                    previous_expires_at_ms: 60_015,
                },
                5,
                15,
            ),
        ] {
            active.apply_event(event, revision, at).unwrap();
        }
        assert!(
            active
                .apply_event(
                    FederatedTrustRotationEvent::BeginRollback {
                        reason: "unsafe rollback".into()
                    },
                    6,
                    16,
                )
                .is_err()
        );
        active
            .apply_event(
                FederatedTrustRotationEvent::MarkRecoveryRequired {
                    reason: "peer activation response lost".into(),
                },
                6,
                16,
            )
            .unwrap();
        assert_eq!(active.phase, FederatedTrustRotationPhase::RecoveryRequired);
    }

    #[test]
    fn staged_ca_is_handshake_trusted_without_becoming_the_active_pin() {
        let active = include_str!("../tests/fixtures/federation-trust-bundle/old-ca.pem");
        let pending = include_str!("../tests/fixtures/federation-trust-bundle/new-ca.pem");
        let mut cluster = FederatedCluster {
            cluster_id: "staged-peer".into(),
            endpoint: "https://127.0.0.1:9443".into(),
            identity_fingerprint: "a".repeat(64),
            previous_identity_fingerprint: None,
            previous_identity_expires_at_ms: None,
            pending_identity_fingerprint: None,
            rotation_started_at_ms: None,
            pending_rotation_grace_period_ms: None,
            trust_bundle_pem: Some(active.to_string()),
            trust_bundle_fingerprint: Some(validate_trust_bundle(active).unwrap()),
            pending_trust_bundle_pem: Some(pending.to_string()),
            pending_trust_bundle_fingerprint: Some(validate_trust_bundle(pending).unwrap()),
            trust_bundle_staged_at_ms: Some(10),
            previous_trust_bundle_pem: None,
            previous_trust_bundle_fingerprint: None,
            previous_trust_bundle_expires_at_ms: None,
            version: "0.1.0".into(),
            capabilities: BTreeSet::new(),
            status: FederatedClusterStatus::Healthy,
            last_seen_at_ms: 10,
            updated_at_ms: 10,
        };
        assert_eq!(
            cluster.trust_bundle_fingerprint.as_deref(),
            Some(validate_trust_bundle(active).unwrap().as_str())
        );
        assert_eq!(
            cluster
                .effective_trust_bundle(11)
                .unwrap()
                .matches("BEGIN CERTIFICATE")
                .count(),
            1
        );
        assert_eq!(
            cluster
                .handshake_trust_bundle(11)
                .unwrap()
                .matches("BEGIN CERTIFICATE")
                .count(),
            2
        );
        cluster.trust_bundle_pem = Some(pending.to_string());
        cluster.trust_bundle_fingerprint = cluster.pending_trust_bundle_fingerprint.take();
        cluster.previous_trust_bundle_pem = Some(active.to_string());
        cluster.previous_trust_bundle_expires_at_ms = Some(100);
        cluster.pending_trust_bundle_pem = None;
        assert_eq!(
            cluster
                .handshake_trust_bundle(99)
                .unwrap()
                .matches("BEGIN CERTIFICATE")
                .count(),
            2
        );
        assert_eq!(
            cluster
                .handshake_trust_bundle(100)
                .unwrap()
                .matches("BEGIN CERTIFICATE")
                .count(),
            1
        );
    }

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

#[path = "federation/trust_rotation.rs"]
pub mod trust_rotation;
