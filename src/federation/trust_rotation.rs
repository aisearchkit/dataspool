// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 Dataspool Contributors

//! Durable coordinator for bilateral CA trust and leaf-certificate lifecycle changes.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::comms::GrpcClient;
use crate::federation::{
    FederatedCluster, FederatedTrustRotation, FederatedTrustRotationEvent,
    FederatedTrustRotationPhase, FederatedTrustRotationSnapshot, validate_trust_bundle,
};
use crate::raft::{Command, RaftNode};
use crate::storage::RocksDbStore;
use crate::task::TaskStore;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FederatedTrustRotationRecoveryAction {
    ContinueStaging,
    ResumeRollback,
    ForwardRecovery,
    RetireBoth,
    WaitForOverlap,
    AwaitOperator,
    NoAction,
    ManualInterventionRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederatedTrustRotationSideDiagnostics {
    pub cluster_id: String,
    pub phase: FederatedTrustRotationPhase,
    pub revision: u64,
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
    pub rollback_peer_confirmed: bool,
    pub local_ca_bundle_fingerprint: String,
    pub peer_ca_bundle_fingerprint: String,
    pub local_leaf_fingerprint: String,
    pub peer_leaf_fingerprint: String,
    pub peer_active_ca_fingerprint: Option<String>,
    pub peer_pending_ca_fingerprint: Option<String>,
    pub peer_previous_ca_fingerprint: Option<String>,
    pub peer_previous_ca_expires_at_ms: Option<i64>,
    pub peer_active_identity_fingerprint: String,
    pub peer_pending_identity_fingerprint: Option<String>,
    pub last_error: Option<String>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederatedTrustRotationDiagnostics {
    pub rotation_id: String,
    pub local: FederatedTrustRotationSideDiagnostics,
    pub peer: FederatedTrustRotationSideDiagnostics,
    pub state_aligned: bool,
    pub differences: Vec<String>,
    pub safety_violations: Vec<String>,
    pub recommended_action: FederatedTrustRotationRecoveryAction,
    pub automated_recovery_safe: bool,
    pub stalled: bool,
    pub stalled_for_ms: i64,
    pub observed_at_ms: i64,
}

fn side_diagnostics(
    cluster_id: &str,
    snapshot: &FederatedTrustRotationSnapshot,
) -> FederatedTrustRotationSideDiagnostics {
    let rotation = &snapshot.rotation;
    FederatedTrustRotationSideDiagnostics {
        cluster_id: cluster_id.to_string(),
        phase: rotation.phase,
        revision: rotation.revision,
        local_staged: rotation.local_staged,
        peer_staged: rotation.peer_staged,
        local_deployment_ready: rotation.local_deployment_ready,
        peer_deployment_ready: rotation.peer_deployment_ready,
        activation_barrier_committed: rotation.activation_barrier_committed,
        local_activation_expires_at_ms: rotation.local_activation_expires_at_ms,
        peer_activation_expires_at_ms: rotation.peer_activation_expires_at_ms,
        local_retired: rotation.local_retired,
        peer_retired: rotation.peer_retired,
        rollback_reason: rotation.rollback_reason.clone(),
        rollback_peer_confirmed: rotation.rollback_peer_confirmed,
        local_ca_bundle_fingerprint: rotation.local_ca_bundle_fingerprint.clone(),
        peer_ca_bundle_fingerprint: rotation.peer_ca_bundle_fingerprint.clone(),
        local_leaf_fingerprint: rotation.local_leaf_fingerprint.clone(),
        peer_leaf_fingerprint: rotation.peer_leaf_fingerprint.clone(),
        peer_active_ca_fingerprint: snapshot.peer_active_ca_fingerprint.clone(),
        peer_pending_ca_fingerprint: snapshot.peer_pending_ca_fingerprint.clone(),
        peer_previous_ca_fingerprint: snapshot.peer_previous_ca_fingerprint.clone(),
        peer_previous_ca_expires_at_ms: snapshot.peer_previous_ca_expires_at_ms,
        peer_active_identity_fingerprint: snapshot.peer_active_identity_fingerprint.clone(),
        peer_pending_identity_fingerprint: snapshot.peer_pending_identity_fingerprint.clone(),
        last_error: rotation.last_error.clone(),
        updated_at_ms: rotation.updated_at_ms,
    }
}

fn build_rotation_diagnostics(
    rotation_id: &str,
    local_cluster_id: &str,
    peer_cluster_id: &str,
    local: FederatedTrustRotationSnapshot,
    peer: FederatedTrustRotationSnapshot,
    now_ms: i64,
) -> FederatedTrustRotationDiagnostics {
    let local_status = side_diagnostics(local_cluster_id, &local);
    let peer_status = side_diagnostics(peer_cluster_id, &peer);
    let mut differences = Vec::new();
    let mut safety_violations = Vec::new();
    let left = &local.rotation;
    let right = &peer.rotation;

    macro_rules! difference {
        ($condition:expr, $message:expr) => {
            if !$condition {
                differences.push($message.to_string());
            }
        };
    }
    difference!(left.phase == right.phase, "phase differs between clusters");
    difference!(
        left.revision == right.revision,
        "Raft rotation revisions differ"
    );
    difference!(
        left.local_staged == right.peer_staged,
        "local/peer staged acknowledgements differ"
    );
    difference!(
        left.peer_staged == right.local_staged,
        "peer/local staged acknowledgements differ"
    );
    difference!(
        left.local_deployment_ready == right.peer_deployment_ready,
        "local/peer deployment readiness differs"
    );
    difference!(
        left.peer_deployment_ready == right.local_deployment_ready,
        "peer/local deployment readiness differs"
    );
    difference!(
        left.activation_barrier_committed == right.activation_barrier_committed,
        "activation barrier commits differ"
    );
    difference!(
        left.local_activation_expires_at_ms == right.peer_activation_expires_at_ms,
        "local activation expiry differs from peer record"
    );
    difference!(
        left.peer_activation_expires_at_ms == right.local_activation_expires_at_ms,
        "peer activation expiry differs from local record"
    );
    difference!(
        left.local_retired == right.peer_retired,
        "local/peer retirement acknowledgements differ"
    );
    difference!(
        left.peer_retired == right.local_retired,
        "peer/local retirement acknowledgements differ"
    );
    difference!(
        left.rollback_peer_confirmed == right.rollback_peer_confirmed,
        "bilateral rollback confirmations differ"
    );
    difference!(
        left.rollback_reason == right.rollback_reason,
        "rollback reasons differ between clusters"
    );

    if left.rotation_id != rotation_id
        || right.rotation_id != rotation_id
        || left.peer_cluster_id != peer_cluster_id
        || right.peer_cluster_id != local_cluster_id
    {
        safety_violations.push("rotation identity or peer mapping is inconsistent".into());
    }
    if left.overlap_ms != right.overlap_ms {
        safety_violations.push("overlap durations differ between clusters".into());
    }
    if (left.phase == FederatedTrustRotationPhase::Completed
        && right.phase == FederatedTrustRotationPhase::RolledBack)
        || (left.phase == FederatedTrustRotationPhase::RolledBack
            && right.phase == FederatedTrustRotationPhase::Completed)
    {
        safety_violations
            .push("clusters report conflicting terminal outcomes for the same rotation".into());
    }
    if !left
        .local_ca_bundle_fingerprint
        .eq_ignore_ascii_case(&right.peer_ca_bundle_fingerprint)
        || !left
            .peer_ca_bundle_fingerprint
            .eq_ignore_ascii_case(&right.local_ca_bundle_fingerprint)
        || !left
            .local_leaf_fingerprint
            .eq_ignore_ascii_case(&right.peer_leaf_fingerprint)
        || !left
            .peer_leaf_fingerprint
            .eq_ignore_ascii_case(&right.local_leaf_fingerprint)
    {
        safety_violations
            .push("candidate CA or leaf fingerprints do not map across clusters".into());
    }
    if left.rollback_reason.is_some()
        && right.rollback_reason.is_some()
        && left.rollback_reason != right.rollback_reason
    {
        safety_violations.push("rollback reason was changed on one side".into());
    }
    if left.local_activation_expires_at_ms.is_some()
        && right.peer_activation_expires_at_ms.is_some()
        && left.local_activation_expires_at_ms != right.peer_activation_expires_at_ms
        || left.peer_activation_expires_at_ms.is_some()
            && right.local_activation_expires_at_ms.is_some()
            && left.peer_activation_expires_at_ms != right.local_activation_expires_at_ms
    {
        safety_violations.push("committed activation expiry values differ across clusters".into());
    }
    let any_rollback = left.phase == FederatedTrustRotationPhase::RollingBack
        || right.phase == FederatedTrustRotationPhase::RollingBack;
    let any_activation_barrier =
        left.activation_barrier_committed || right.activation_barrier_committed;
    let any_actual_activation = left.local_activation_expires_at_ms.is_some()
        || left.peer_activation_expires_at_ms.is_some()
        || right.local_activation_expires_at_ms.is_some()
        || right.peer_activation_expires_at_ms.is_some();
    if any_rollback
        && any_actual_activation
        && (left.rollback_peer_confirmed || right.rollback_peer_confirmed)
    {
        safety_violations
            .push("bilaterally confirmed rollback conflicts with a committed CA activation".into());
    }
    if any_rollback
        && any_activation_barrier
        && (left.rollback_peer_confirmed || right.rollback_peer_confirmed)
    {
        safety_violations.push(
            "bilaterally confirmed rollback conflicts with a committed activation barrier".into(),
        );
    }
    for (snapshot, rotation) in [(&local, left), (&peer, right)] {
        let candidate_is_active = snapshot
            .peer_active_ca_fingerprint
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint));
        let candidate_is_previous = snapshot
            .peer_previous_ca_fingerprint
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint));
        if rotation.phase == FederatedTrustRotationPhase::RolledBack
            && (candidate_is_active
                || candidate_is_previous
                || snapshot
                    .peer_pending_ca_fingerprint
                    .as_deref()
                    .is_some_and(|value| {
                        value.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
                    })
                || snapshot
                    .peer_pending_identity_fingerprint
                    .as_deref()
                    .is_some_and(|value| {
                        value.eq_ignore_ascii_case(&rotation.peer_leaf_fingerprint)
                    }))
        {
            safety_violations.push(format!("{} reports RolledBack while candidate trust or identity material remains installed", rotation.rotation_id));
        }
        if rotation.phase == FederatedTrustRotationPhase::Completed
            && !(rotation.local_retired && rotation.peer_retired)
        {
            safety_violations.push(format!(
                "{} reports Completed without both retirement acknowledgements",
                rotation.rotation_id
            ));
        }
        if rotation.local_activation_expires_at_ms.is_some()
            && !snapshot
                .peer_active_ca_fingerprint
                .as_deref()
                .is_some_and(|value| {
                    value.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
                })
        {
            safety_violations.push(format!(
                "{} peer active CA does not match the locally committed candidate",
                rotation.rotation_id
            ));
        }
        if rotation.local_activation_expires_at_ms.is_some()
            && !rotation.local_retired
            && (snapshot.peer_previous_ca_expires_at_ms != rotation.local_activation_expires_at_ms
                || snapshot.peer_previous_ca_fingerprint != rotation.current_peer_ca_fingerprint)
        {
            safety_violations.push(format!(
                "{} peer previous-CA expiry does not match its committed local activation expiry",
                rotation.rotation_id
            ));
        }
        if rotation.local_retired && snapshot.peer_previous_ca_expires_at_ms.is_some() {
            safety_violations.push(format!(
                "{} previous CA remains present after local retirement was acknowledged",
                rotation.rotation_id
            ));
        }
    }

    let deadline = [
        left.local_activation_expires_at_ms,
        left.peer_activation_expires_at_ms,
        right.local_activation_expires_at_ms,
        right.peer_activation_expires_at_ms,
    ]
    .into_iter()
    .flatten()
    .max();
    let recommended_action = if !safety_violations.is_empty() {
        FederatedTrustRotationRecoveryAction::ManualInterventionRequired
    } else if any_activation_barrier
        || any_actual_activation
        || matches!(
            left.phase,
            FederatedTrustRotationPhase::Activating | FederatedTrustRotationPhase::RecoveryRequired
        )
        || matches!(
            right.phase,
            FederatedTrustRotationPhase::Activating | FederatedTrustRotationPhase::RecoveryRequired
        )
    {
        FederatedTrustRotationRecoveryAction::ForwardRecovery
    } else if any_rollback {
        FederatedTrustRotationRecoveryAction::ResumeRollback
    } else if matches!(
        left.phase,
        FederatedTrustRotationPhase::Overlap | FederatedTrustRotationPhase::Retiring
    ) || matches!(
        right.phase,
        FederatedTrustRotationPhase::Overlap | FederatedTrustRotationPhase::Retiring
    ) {
        if deadline.is_some_and(|value| now_ms >= value) {
            FederatedTrustRotationRecoveryAction::RetireBoth
        } else {
            FederatedTrustRotationRecoveryAction::WaitForOverlap
        }
    } else if left.phase == FederatedTrustRotationPhase::Staging
        || right.phase == FederatedTrustRotationPhase::Staging
    {
        FederatedTrustRotationRecoveryAction::ContinueStaging
    } else if matches!(
        left.phase,
        FederatedTrustRotationPhase::Completed | FederatedTrustRotationPhase::RolledBack
    ) && matches!(
        right.phase,
        FederatedTrustRotationPhase::Completed | FederatedTrustRotationPhase::RolledBack
    ) {
        FederatedTrustRotationRecoveryAction::NoAction
    } else {
        FederatedTrustRotationRecoveryAction::AwaitOperator
    };
    let updated_at = left.updated_at_ms.min(right.updated_at_ms);
    let stalled_for_ms = now_ms.saturating_sub(updated_at).max(0);
    let stalled = stalled_for_ms >= 30_000
        && matches!(
            recommended_action,
            FederatedTrustRotationRecoveryAction::ContinueStaging
                | FederatedTrustRotationRecoveryAction::ResumeRollback
                | FederatedTrustRotationRecoveryAction::ForwardRecovery
                | FederatedTrustRotationRecoveryAction::RetireBoth
        );
    let automated_recovery_safe = matches!(
        recommended_action,
        FederatedTrustRotationRecoveryAction::ContinueStaging
            | FederatedTrustRotationRecoveryAction::ResumeRollback
            | FederatedTrustRotationRecoveryAction::ForwardRecovery
            | FederatedTrustRotationRecoveryAction::RetireBoth
    );
    FederatedTrustRotationDiagnostics {
        rotation_id: rotation_id.to_string(),
        local: local_status,
        peer: peer_status,
        state_aligned: differences.is_empty() && safety_violations.is_empty(),
        differences,
        safety_violations,
        recommended_action,
        automated_recovery_safe,
        stalled,
        stalled_for_ms,
        observed_at_ms: now_ms,
    }
}

async fn connect_mtls_with_retry(
    endpoint: &str,
    ca_file: &std::path::Path,
    certificate_file: &std::path::Path,
    private_key_file: &std::path::Path,
) -> crate::Result<GrpcClient> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match GrpcClient::connect_mtls(endpoint, ca_file, certificate_file, private_key_file)
                .await
            {
                Ok(client) => return Ok(client),
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .map_err(|_| {
        crate::DatspoolError::Internal(format!(
            "timed out connecting to federation gRPC endpoint {endpoint}"
        ))
    })?
}

/// Stateless process-level coordinator that drives a bilateral activation
/// through the two clusters' authenticated gRPC control planes. Durable progress
/// remains in each cluster's Raft log, so this coordinator can be SIGKILLed and
/// restarted without sharing a Raft member process or local database.
pub struct FederatedTrustRotationProcessCoordinator {
    local_client: GrpcClient,
    peer_client: GrpcClient,
    local_cluster_id: String,
    peer_cluster_id: String,
    rotation_id: String,
    request_prefix: String,
}

impl FederatedTrustRotationProcessCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        local_cluster_id: impl Into<String>,
        peer_cluster_id: impl Into<String>,
        rotation_id: impl Into<String>,
        request_prefix: impl Into<String>,
        local_endpoint: &str,
        peer_endpoint: &str,
        local_ca_file: &std::path::Path,
        peer_ca_file: &std::path::Path,
        local_certificate_file: &std::path::Path,
        local_private_key_file: &std::path::Path,
        peer_certificate_file: &std::path::Path,
        peer_private_key_file: &std::path::Path,
    ) -> crate::Result<Self> {
        let local_cluster_id = local_cluster_id.into();
        let peer_cluster_id = peer_cluster_id.into();
        let rotation_id = rotation_id.into();
        let request_prefix = request_prefix.into();
        if local_cluster_id.trim().is_empty()
            || peer_cluster_id.trim().is_empty()
            || rotation_id.trim().is_empty()
            || request_prefix.trim().is_empty()
            || request_prefix.len() > 80
        {
            return Err(crate::DatspoolError::Configuration(
                "local/peer cluster IDs, rotation ID and request prefix are required (prefix <= 80 bytes)".into(),
            ));
        }
        // The local cluster authenticates the peer's certificate; the peer
        // cluster authenticates this coordinator's local-cluster certificate.
        let local_client = connect_mtls_with_retry(
            local_endpoint,
            local_ca_file,
            peer_certificate_file,
            peer_private_key_file,
        )
        .await?;
        let peer_client = connect_mtls_with_retry(
            peer_endpoint,
            peer_ca_file,
            local_certificate_file,
            local_private_key_file,
        )
        .await?;
        Ok(Self {
            local_client,
            peer_client,
            local_cluster_id,
            peer_cluster_id,
            rotation_id,
            request_prefix,
        })
    }

    /// Resume bilateral activation using stable request IDs. The first cluster
    /// activates its peer trust, then the peer records that acknowledgement and
    /// activates its own peer trust, then the first cluster records peer activation.
    /// Repeating this method after process death replays the same committed commands.
    pub async fn activate_both(
        &mut self,
    ) -> crate::Result<(
        FederatedTrustRotationSnapshot,
        FederatedTrustRotationSnapshot,
    )> {
        self.activate_both_with_expiry(None).await
    }

    /// Activate using a caller-supplied absolute deadline. This allows an
    /// external scheduler to persist the intended deadline before issuing RPCs.
    pub async fn activate_both_at(
        &mut self,
        expiry_ms: i64,
    ) -> crate::Result<(
        FederatedTrustRotationSnapshot,
        FederatedTrustRotationSnapshot,
    )> {
        self.activate_both_with_expiry(Some(expiry_ms)).await
    }

    /// Resume a bilateral pre-activation rollback using only the two durable
    /// Raft endpoints. Every event and request ID is stable across coordinator
    /// restarts; the peer RPC also performs the corresponding local leaf/CA abort.
    pub async fn rollback_before_activation(
        &mut self,
        reason: &str,
    ) -> crate::Result<(
        FederatedTrustRotationSnapshot,
        FederatedTrustRotationSnapshot,
    )> {
        if reason.trim().is_empty() || reason.len() > 1024 {
            return Err(crate::DatspoolError::Configuration(
                "rollback reason must contain 1-1024 bytes".into(),
            ));
        }
        let (local_before, peer_before) = self.read_snapshots().await?;
        let activation_uncertain = [&local_before, &peer_before].iter().any(|snapshot| {
            let rotation = &snapshot.rotation;
            let candidate_is_active =
                snapshot
                    .peer_active_ca_fingerprint
                    .as_deref()
                    .is_some_and(|fingerprint| {
                        fingerprint.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
                    });
            let candidate_was_active = snapshot
                .peer_previous_ca_fingerprint
                .as_deref()
                .is_some_and(|fingerprint| {
                    fingerprint.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
                });
            rotation.activation_barrier_committed
                || rotation.local_activation_expires_at_ms.is_some()
                || rotation.peer_activation_expires_at_ms.is_some()
                || candidate_is_active
                || candidate_was_active
                || matches!(
                    rotation.phase,
                    FederatedTrustRotationPhase::Activating
                        | FederatedTrustRotationPhase::RecoveryRequired
                        | FederatedTrustRotationPhase::Overlap
                        | FederatedTrustRotationPhase::Retiring
                        | FederatedTrustRotationPhase::Completed
                )
        });
        if activation_uncertain {
            if local_before.rotation.phase == FederatedTrustRotationPhase::RollingBack
                && !local_before.rotation.rollback_peer_confirmed
            {
                Self::ensure_remote_success(
                    self.local_client
                        .advance_remote_trust_rotation(
                            &self.peer_cluster_id,
                            &self.rotation_id,
                            &format!("{}:promote-local-forward-recovery", self.request_prefix),
                            &FederatedTrustRotationEvent::MarkRecoveryRequired {
                                reason: "peer activation committed while rollback was not yet bilaterally confirmed".into(),
                            },
                        )
                        .await?,
                )?;
            }
            if peer_before.rotation.phase == FederatedTrustRotationPhase::RollingBack
                && !peer_before.rotation.rollback_peer_confirmed
            {
                Self::ensure_remote_success(
                    self.peer_client
                        .advance_remote_trust_rotation(
                            &self.local_cluster_id,
                            &self.rotation_id,
                            &format!("{}:promote-peer-forward-recovery", self.request_prefix),
                            &FederatedTrustRotationEvent::MarkRecoveryRequired {
                                reason: "peer activation committed while rollback was not yet bilaterally confirmed".into(),
                            },
                        )
                        .await?,
                )?;
            }
            return Err(crate::DatspoolError::Configuration(
                "rollback is forbidden after either side activates or activation is uncertain; forward recovery is required".into(),
            ));
        }
        for snapshot in [&local_before, &peer_before] {
            if snapshot
                .rotation
                .rollback_reason
                .as_deref()
                .is_some_and(|existing| existing != reason)
            {
                return Err(crate::DatspoolError::Configuration(
                    "rollback reason cannot change after either side has begun rollback".into(),
                ));
            }
            let rotation = &snapshot.rotation;
            if !matches!(
                rotation.phase,
                FederatedTrustRotationPhase::Staging
                    | FederatedTrustRotationPhase::AwaitingDeployment
                    | FederatedTrustRotationPhase::ActivationBarrier
                    | FederatedTrustRotationPhase::RollingBack
                    | FederatedTrustRotationPhase::RolledBack
            ) {
                return Err(crate::DatspoolError::Configuration(format!(
                    "rotation {} cannot roll back from phase {:?}",
                    rotation.rotation_id, rotation.phase
                )));
            }
        }
        if local_before.rotation.overlap_ms != peer_before.rotation.overlap_ms {
            return Err(crate::DatspoolError::Configuration(
                "bilateral trust rotation overlap durations do not match".into(),
            ));
        }

        let begin = FederatedTrustRotationEvent::BeginRollback {
            reason: reason.to_string(),
        };
        if !matches!(
            local_before.rotation.phase,
            FederatedTrustRotationPhase::RollingBack | FederatedTrustRotationPhase::RolledBack
        ) {
            Self::ensure_remote_success(
                self.local_client
                    .advance_remote_trust_rotation(
                        &self.peer_cluster_id,
                        &self.rotation_id,
                        &format!("{}:begin-local-rollback", self.request_prefix),
                        &begin,
                    )
                    .await?,
            )?;
        }
        if !matches!(
            peer_before.rotation.phase,
            FederatedTrustRotationPhase::RollingBack | FederatedTrustRotationPhase::RolledBack
        ) {
            Self::ensure_remote_success(
                self.peer_client
                    .advance_remote_trust_rotation(
                        &self.local_cluster_id,
                        &self.rotation_id,
                        &format!("{}:begin-peer-rollback", self.request_prefix),
                        &begin,
                    )
                    .await?,
            )?;
        }

        // Both BeginRollback events must be durable before either side is
        // authorized to remove staged material. Persist the peer confirmation
        // independently on both Raft groups so recovery can resume this barrier.
        let (local_rolling, peer_rolling) = self.read_snapshots().await?;
        if local_rolling.rotation.phase != FederatedTrustRotationPhase::RolledBack
            && !local_rolling.rotation.rollback_peer_confirmed
        {
            Self::ensure_remote_success(
                self.local_client
                    .advance_remote_trust_rotation(
                        &self.peer_cluster_id,
                        &self.rotation_id,
                        &format!("{}:confirm-local-rollback", self.request_prefix),
                        &FederatedTrustRotationEvent::ConfirmRollback,
                    )
                    .await?,
            )?;
        }
        if peer_rolling.rotation.phase != FederatedTrustRotationPhase::RolledBack
            && !peer_rolling.rotation.rollback_peer_confirmed
        {
            Self::ensure_remote_success(
                self.peer_client
                    .advance_remote_trust_rotation(
                        &self.local_cluster_id,
                        &self.rotation_id,
                        &format!("{}:confirm-peer-rollback", self.request_prefix),
                        &FederatedTrustRotationEvent::ConfirmRollback,
                    )
                    .await?,
            )?;
        }
        let (local_rolling, peer_rolling) = self.read_snapshots().await?;
        if peer_rolling.rotation.phase == FederatedTrustRotationPhase::RollingBack {
            if peer_rolling.rotation.local_staged {
                Self::ensure_remote_success(
                    self.peer_client
                        .advance_remote_trust_rotation(
                            &self.local_cluster_id,
                            &self.rotation_id,
                            &format!("{}:rollback-peer-local-material", self.request_prefix),
                            &FederatedTrustRotationEvent::PeerRolledBack,
                        )
                        .await?,
                )?;
            }
            if peer_rolling.rotation.peer_staged {
                Self::ensure_remote_success(
                    self.peer_client
                        .advance_remote_trust_rotation(
                            &self.local_cluster_id,
                            &self.rotation_id,
                            &format!("{}:rollback-peer-peer-ack", self.request_prefix),
                            &FederatedTrustRotationEvent::LocalRolledBack,
                        )
                        .await?,
                )?;
            }
        }
        if local_rolling.rotation.phase == FederatedTrustRotationPhase::RollingBack {
            if local_rolling.rotation.local_staged {
                Self::ensure_remote_success(
                    self.local_client
                        .advance_remote_trust_rotation(
                            &self.peer_cluster_id,
                            &self.rotation_id,
                            &format!("{}:rollback-local-local-material", self.request_prefix),
                            &FederatedTrustRotationEvent::PeerRolledBack,
                        )
                        .await?,
                )?;
            }
            if local_rolling.rotation.peer_staged {
                Self::ensure_remote_success(
                    self.local_client
                        .advance_remote_trust_rotation(
                            &self.peer_cluster_id,
                            &self.rotation_id,
                            &format!("{}:rollback-local-peer-ack", self.request_prefix),
                            &FederatedTrustRotationEvent::LocalRolledBack,
                        )
                        .await?,
                )?;
            }
        }

        let (local_after, peer_after) = self.read_snapshots().await?;
        for snapshot in [&local_after, &peer_after] {
            if snapshot.rotation.phase != FederatedTrustRotationPhase::RolledBack
                || snapshot.rotation.local_staged
                || snapshot.rotation.peer_staged
                || snapshot.rotation.local_activation_expires_at_ms.is_some()
                || snapshot.rotation.peer_activation_expires_at_ms.is_some()
                || snapshot
                    .peer_active_ca_fingerprint
                    .as_deref()
                    .is_some_and(|fingerprint| {
                        fingerprint
                            .eq_ignore_ascii_case(&snapshot.rotation.peer_ca_bundle_fingerprint)
                    })
                || snapshot
                    .peer_pending_ca_fingerprint
                    .as_deref()
                    .is_some_and(|fingerprint| {
                        fingerprint
                            .eq_ignore_ascii_case(&snapshot.rotation.peer_ca_bundle_fingerprint)
                    })
                || snapshot
                    .peer_previous_ca_fingerprint
                    .as_deref()
                    .is_some_and(|fingerprint| {
                        fingerprint
                            .eq_ignore_ascii_case(&snapshot.rotation.peer_ca_bundle_fingerprint)
                    })
                || snapshot
                    .peer_pending_identity_fingerprint
                    .as_deref()
                    .is_some_and(|fingerprint| {
                        fingerprint.eq_ignore_ascii_case(&snapshot.rotation.peer_leaf_fingerprint)
                    })
                || snapshot
                    .peer_active_identity_fingerprint
                    .eq_ignore_ascii_case(&snapshot.rotation.peer_leaf_fingerprint)
            {
                return Err(crate::DatspoolError::Internal(
                    "bilateral rollback did not converge to a clean RolledBack state".into(),
                ));
            }
        }
        Ok((local_after, peer_after))
    }

    async fn activate_both_with_expiry(
        &mut self,
        requested_expiry: Option<i64>,
    ) -> crate::Result<(
        FederatedTrustRotationSnapshot,
        FederatedTrustRotationSnapshot,
    )> {
        let (local_before, peer_before) = self.read_snapshots().await?;
        if !local_before.rotation.activation_barrier_committed
            || !peer_before.rotation.activation_barrier_committed
        {
            return Err(crate::DatspoolError::Configuration(
                "both clusters must commit the activation barrier before process-level activation"
                    .into(),
            ));
        }
        for rotation in [&local_before.rotation, &peer_before.rotation] {
            if !matches!(
                rotation.phase,
                FederatedTrustRotationPhase::Activating
                    | FederatedTrustRotationPhase::RecoveryRequired
                    | FederatedTrustRotationPhase::Overlap
            ) {
                return Err(crate::DatspoolError::Configuration(format!(
                    "rotation {} cannot activate from phase {:?}",
                    rotation.rotation_id, rotation.phase
                )));
            }
        }
        if local_before.rotation.overlap_ms != peer_before.rotation.overlap_ms {
            return Err(crate::DatspoolError::Configuration(
                "bilateral trust rotation overlap durations do not match".into(),
            ));
        }
        let overlap_ms = local_before.rotation.overlap_ms;
        let committed_expiries = [
            local_before.rotation.local_activation_expires_at_ms,
            local_before.rotation.peer_activation_expires_at_ms,
            peer_before.rotation.local_activation_expires_at_ms,
            peer_before.rotation.peer_activation_expires_at_ms,
        ]
        .into_iter()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>();
        if committed_expiries.len() > 1 {
            return Err(crate::DatspoolError::Internal(
                "bilateral activation already contains conflicting overlap expiries".into(),
            ));
        }
        let committed_expiry = committed_expiries.first().copied();
        if requested_expiry.is_some()
            && committed_expiry.is_some()
            && requested_expiry != committed_expiry
        {
            return Err(crate::DatspoolError::Configuration(
                "requested activation deadline differs from a deadline already committed in Raft"
                    .into(),
            ));
        }
        let expiry = committed_expiry.or(requested_expiry).unwrap_or_else(|| {
            chrono::Utc::now()
                .timestamp_millis()
                .saturating_add(overlap_ms)
        });

        Self::ensure_remote_success(
            self.local_client
                .advance_remote_trust_rotation(
                    &self.peer_cluster_id,
                    &self.rotation_id,
                    &format!("{}:activate-local", self.request_prefix),
                    &FederatedTrustRotationEvent::PeerActivated {
                        previous_expires_at_ms: expiry,
                    },
                )
                .await?,
        )?;
        Self::ensure_remote_success(
            self.peer_client
                .advance_remote_trust_rotation(
                    &self.local_cluster_id,
                    &self.rotation_id,
                    &format!("{}:peer-records-local-activation", self.request_prefix),
                    &FederatedTrustRotationEvent::LocalActivated {
                        previous_expires_at_ms: expiry,
                    },
                )
                .await?,
        )?;
        Self::ensure_remote_success(
            self.peer_client
                .advance_remote_trust_rotation(
                    &self.local_cluster_id,
                    &self.rotation_id,
                    &format!("{}:activate-peer", self.request_prefix),
                    &FederatedTrustRotationEvent::PeerActivated {
                        previous_expires_at_ms: expiry,
                    },
                )
                .await?,
        )?;
        Self::ensure_remote_success(
            self.local_client
                .advance_remote_trust_rotation(
                    &self.peer_cluster_id,
                    &self.rotation_id,
                    &format!("{}:event-peer-activated", self.request_prefix),
                    &FederatedTrustRotationEvent::LocalActivated {
                        previous_expires_at_ms: expiry,
                    },
                )
                .await?,
        )?;

        let (local_after, peer_after) = self.read_snapshots().await?;
        for snapshot in [&local_after, &peer_after] {
            if snapshot.rotation.phase != FederatedTrustRotationPhase::Overlap
                || snapshot.rotation.local_activation_expires_at_ms != Some(expiry)
                || snapshot.rotation.peer_activation_expires_at_ms != Some(expiry)
                || snapshot.peer_previous_ca_expires_at_ms != Some(expiry)
            {
                return Err(crate::DatspoolError::Internal(
                    "bilateral activation did not converge to the same durable overlap deadline"
                        .into(),
                ));
            }
        }
        Ok((local_after, peer_after))
    }

    async fn read_snapshots(
        &mut self,
    ) -> crate::Result<(
        FederatedTrustRotationSnapshot,
        FederatedTrustRotationSnapshot,
    )> {
        let local = self
            .local_client
            .get_remote_trust_rotation(&self.peer_cluster_id, &self.rotation_id)
            .await?;
        let peer = self
            .peer_client
            .get_remote_trust_rotation(&self.local_cluster_id, &self.rotation_id)
            .await?;
        if !local.accepted || !peer.accepted {
            return Err(crate::DatspoolError::Internal(
                "one federation cluster rejected a durable trust-rotation read".into(),
            ));
        }
        let local: FederatedTrustRotationSnapshot = serde_json::from_str(&local.state_json)
            .map_err(|error| crate::DatspoolError::Serialization(error.to_string()))?;
        let peer: FederatedTrustRotationSnapshot = serde_json::from_str(&peer.state_json)
            .map_err(|error| crate::DatspoolError::Serialization(error.to_string()))?;
        if local.rotation.peer_cluster_id != self.peer_cluster_id
            || peer.rotation.peer_cluster_id != self.local_cluster_id
            || local.rotation.rotation_id != self.rotation_id
            || peer.rotation.rotation_id != self.rotation_id
        {
            return Err(crate::DatspoolError::Configuration(
                "the two gRPC endpoints returned mismatched federation rotation identities".into(),
            ));
        }
        Ok((local, peer))
    }

    fn ensure_remote_success(
        response: crate::comms::task::FederatedTrustRotationResponse,
    ) -> crate::Result<()> {
        if response.accepted {
            Ok(())
        } else {
            Err(crate::DatspoolError::Internal(response.message))
        }
    }
}

#[derive(Clone)]
pub struct FederatedTrustRotationCoordinator {
    store: Arc<TaskStore<RocksDbStore>>,
    raft: Arc<RaftNode>,
    local_cluster_id: String,
    ca_file: PathBuf,
    certificate_file: PathBuf,
    private_key_file: PathBuf,
}

impl FederatedTrustRotationCoordinator {
    pub fn new(
        store: Arc<TaskStore<RocksDbStore>>,
        raft: Arc<RaftNode>,
        local_cluster_id: impl Into<String>,
        ca_file: PathBuf,
        certificate_file: PathBuf,
        private_key_file: PathBuf,
    ) -> Self {
        Self {
            store,
            raft,
            local_cluster_id: local_cluster_id.into(),
            ca_file,
            certificate_file,
            private_key_file,
        }
    }

    /// Create both Raft records and stage each side's candidate CA. All writes use
    /// stable request IDs, so a new leader can safely resume this method.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_rotation(
        &self,
        rotation_id: &str,
        peer_cluster_id: &str,
        local_ca_bundle_pem: String,
        peer_ca_bundle_pem: String,
        local_leaf_fingerprint: &str,
        peer_leaf_fingerprint: &str,
        overlap_ms: i64,
        request_id: &str,
        actor: &str,
    ) -> crate::Result<FederatedTrustRotation> {
        if rotation_id.trim().is_empty()
            || rotation_id.len() > 64
            || !rotation_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
            || peer_cluster_id.trim().is_empty()
            || peer_cluster_id.len() > 128
            || request_id.trim().is_empty()
            || request_id.len() > 64
            || !(60_000..=604_800_000).contains(&overlap_ms)
        {
            return Err(crate::DatspoolError::Configuration(
                "rotation_id (1-64 safe characters), peer_cluster_id, request_id (1-64 bytes), and a 1-minute to 7-day overlap are required".into(),
            ));
        }
        for fingerprint in [local_leaf_fingerprint, peer_leaf_fingerprint] {
            if fingerprint.len() != 64 || !fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(crate::DatspoolError::Configuration(
                    "leaf certificate fingerprints must be 64-character SHA-256 hex digests".into(),
                ));
            }
        }
        let local_ca_fingerprint = validate_trust_bundle(&local_ca_bundle_pem)
            .map_err(crate::DatspoolError::Configuration)?;
        let peer_ca_fingerprint = validate_trust_bundle(&peer_ca_bundle_pem)
            .map_err(crate::DatspoolError::Configuration)?;
        let registered_peers = self.store.list_federated_clusters().await?;
        if registered_peers
            .iter()
            .any(|registered| registered.trust_bundle_pem.is_none())
        {
            return Err(crate::DatspoolError::Configuration(
                "all registered federation peers must have Raft-managed CA bundles before rotation; otherwise the static CA fallback could continue trusting a retired root".into(),
            ));
        }
        let peer = self.peer_record(peer_cluster_id).await?;
        if peer.trust_bundle_fingerprint.is_none() {
            return Err(crate::DatspoolError::Configuration(
                "bilateral CA rotation requires an already bootstrapped peer trust bundle".into(),
            ));
        }
        let now = chrono::Utc::now().timestamp_millis();
        let rotation = FederatedTrustRotation {
            rotation_id: rotation_id.to_string(),
            peer_cluster_id: peer_cluster_id.to_string(),
            current_peer_ca_fingerprint: peer.trust_bundle_fingerprint.clone(),
            local_ca_bundle_pem: local_ca_bundle_pem.clone(),
            local_ca_bundle_fingerprint: local_ca_fingerprint.clone(),
            peer_ca_bundle_pem: peer_ca_bundle_pem.clone(),
            peer_ca_bundle_fingerprint: peer_ca_fingerprint.clone(),
            local_leaf_fingerprint: local_leaf_fingerprint.to_string(),
            peer_leaf_fingerprint: peer_leaf_fingerprint.to_string(),
            overlap_ms,
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
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.ensure_success(
            self.raft
                .propose(Command::CreateFederatedTrustRotation {
                    rotation: Box::new(rotation),
                    at_ms: now,
                    request_id: format!("federation-trust:{rotation_id}:create-local"),
                    actor: format!("{actor} request:{request_id}"),
                })
                .await?,
        )?;

        let mut peer_client = self.connect_peer(&peer).await?;
        let remote_created = peer_client
            .create_remote_trust_rotation(
                &self.local_cluster_id,
                rotation_id,
                &format!("federation-trust:{rotation_id}:create-peer"),
                &local_ca_bundle_pem,
                &peer_ca_bundle_pem,
                local_leaf_fingerprint,
                peer_leaf_fingerprint,
                overlap_ms,
            )
            .await?;
        if !remote_created.accepted {
            return Err(crate::DatspoolError::Internal(format!(
                "Peer trust rotation creation failed: {}",
                remote_created.message
            )));
        }

        let peer_after_create = self.peer_record(peer_cluster_id).await?;
        if !peer_after_create
            .pending_identity_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| fingerprint.eq_ignore_ascii_case(peer_leaf_fingerprint))
            && !peer_after_create
                .identity_fingerprint
                .eq_ignore_ascii_case(peer_leaf_fingerprint)
        {
            self.ensure_success(
                self.raft
                    .propose(Command::StageFederatedCertificateRotation {
                        cluster_id: peer_cluster_id.to_string(),
                        current_fingerprint: peer_after_create.identity_fingerprint,
                        new_fingerprint: peer_leaf_fingerprint.to_string(),
                        at_ms: chrono::Utc::now().timestamp_millis(),
                        grace_period_ms: overlap_ms,
                        request_id: format!("federation-trust:{rotation_id}:stage-peer-leaf-local"),
                        actor: actor.to_string(),
                    })
                    .await?,
            )?;
        }

        self.advance_local(
            rotation_id,
            FederatedTrustRotationEvent::PeerStaged {
                fingerprint: local_ca_fingerprint.clone(),
            },
            &format!("federation-trust:{rotation_id}:peer-staged"),
            chrono::Utc::now().timestamp_millis(),
            actor,
        )
        .await?;

        let current_peer_fingerprint = peer.trust_bundle_fingerprint.unwrap_or_default();
        self.ensure_success(
            self.raft
                .propose(Command::StageFederatedTrustBundle {
                    cluster_id: peer_cluster_id.to_string(),
                    current_fingerprint: current_peer_fingerprint,
                    bundle_pem: peer_ca_bundle_pem,
                    bundle_fingerprint: peer_ca_fingerprint.clone(),
                    at_ms: chrono::Utc::now().timestamp_millis(),
                    request_id: format!("federation-trust:{rotation_id}:stage-local"),
                    actor: actor.to_string(),
                })
                .await?,
        )?;
        self.advance_local(
            rotation_id,
            FederatedTrustRotationEvent::LocalStaged {
                fingerprint: peer_ca_fingerprint.clone(),
            },
            &format!("federation-trust:{rotation_id}:local-staged"),
            chrono::Utc::now().timestamp_millis(),
            actor,
        )
        .await?;

        let mut peer_client = self.connect_peer_by_id(peer_cluster_id).await?;
        self.ensure_remote_success(
            peer_client
                .advance_remote_trust_rotation(
                    &self.local_cluster_id,
                    rotation_id,
                    &format!("federation-trust:{rotation_id}:peer-records-local-stage"),
                    &FederatedTrustRotationEvent::LocalStaged {
                        fingerprint: peer_ca_fingerprint,
                    },
                )
                .await?,
        )?;
        self.get_rotation(rotation_id).await
    }

    /// Record that the new local leaf has been deployed and mirror the readiness
    /// assertion into the peer's Raft record. The peer does the symmetric action.
    pub async fn mark_local_deployment_ready(
        &self,
        rotation_id: &str,
        request_id: &str,
        actor: &str,
    ) -> crate::Result<FederatedTrustRotation> {
        if request_id.trim().is_empty() || request_id.len() > 64 {
            return Err(crate::DatspoolError::Configuration(
                "request_id must contain 1-64 bytes".into(),
            ));
        }
        let rotation = self.get_rotation(rotation_id).await?;
        let event = FederatedTrustRotationEvent::LocalDeploymentReady {
            leaf_fingerprint: rotation.local_leaf_fingerprint.clone(),
        };
        self.advance_local(
            rotation_id,
            event.clone(),
            request_id,
            chrono::Utc::now().timestamp_millis(),
            actor,
        )
        .await?;
        let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
        self.ensure_remote_success(
            peer.advance_remote_trust_rotation(
                &self.local_cluster_id,
                rotation_id,
                &format!("{request_id}:peer"),
                &event,
            )
            .await?,
        )?;
        self.get_rotation(rotation_id).await
    }

    /// Commit the activation barrier on both independent Raft clusters. Neither
    /// side is allowed to activate until both deployment-ready assertions exist.
    /// The first committed barrier is the point of no return for rollback: if a
    /// concurrent rollback has begun on the other side, it is promoted to forward
    /// recovery before activation can proceed.
    pub async fn authorize_activation(
        &self,
        rotation_id: &str,
        request_id: &str,
        actor: &str,
    ) -> crate::Result<FederatedTrustRotation> {
        if request_id.trim().is_empty() || request_id.len() > 64 {
            return Err(crate::DatspoolError::Configuration(
                "request_id must contain 1-64 bytes".into(),
            ));
        }
        let mut rotation = self.get_rotation(rotation_id).await?;
        let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
        let remote_state = peer
            .get_remote_trust_rotation(&self.local_cluster_id, rotation_id)
            .await?;
        if !remote_state.accepted {
            return Err(crate::DatspoolError::Internal(
                "peer rejected the durable trust-rotation read before activation authorization"
                    .into(),
            ));
        }
        let mut remote_snapshot: FederatedTrustRotationSnapshot =
            serde_json::from_str(&remote_state.state_json)
                .map_err(|error| crate::DatspoolError::Serialization(error.to_string()))?;

        if rotation.phase == FederatedTrustRotationPhase::RolledBack
            || remote_snapshot.rotation.phase == FederatedTrustRotationPhase::RolledBack
        {
            return Err(crate::DatspoolError::Configuration(
                "activation is forbidden because at least one side has completed rollback".into(),
            ));
        }

        if rotation.phase == FederatedTrustRotationPhase::RollingBack {
            if rotation.rollback_peer_confirmed {
                return Err(crate::DatspoolError::Internal(
                    "activation conflicts with a bilaterally confirmed rollback; manual intervention is required".into(),
                ));
            }
            let peer_has_committed_activation =
                remote_snapshot.rotation.activation_barrier_committed
                    || remote_snapshot
                        .rotation
                        .local_activation_expires_at_ms
                        .is_some()
                    || remote_snapshot
                        .rotation
                        .peer_activation_expires_at_ms
                        .is_some()
                    || matches!(
                        remote_snapshot.rotation.phase,
                        FederatedTrustRotationPhase::Activating
                            | FederatedTrustRotationPhase::RecoveryRequired
                            | FederatedTrustRotationPhase::Overlap
                            | FederatedTrustRotationPhase::Retiring
                            | FederatedTrustRotationPhase::Completed
                    );
            if !peer_has_committed_activation {
                return Err(crate::DatspoolError::Configuration(
                    "activation cannot proceed while the peer is still attempting pre-activation rollback".into(),
                ));
            }
            self.mark_recovery_required(
                rotation_id,
                "peer activation barrier committed while local rollback was not bilaterally confirmed",
                &format!("{actor} request:{request_id}"),
            )
            .await?;
            rotation = self.get_rotation(rotation_id).await?;
        }

        if remote_snapshot.rotation.phase == FederatedTrustRotationPhase::RollingBack {
            if remote_snapshot.rotation.rollback_peer_confirmed {
                return Err(crate::DatspoolError::Internal(
                    "activation conflicts with a bilaterally confirmed peer rollback; manual intervention is required".into(),
                ));
            }
            if !rotation.activation_barrier_committed {
                return Err(crate::DatspoolError::Configuration(
                    "activation cannot override peer rollback before this cluster has committed its activation barrier".into(),
                ));
            }
            self.ensure_remote_success(
                peer.advance_remote_trust_rotation(
                    &self.local_cluster_id,
                    rotation_id,
                    &format!("{request_id}:promote-peer-forward-recovery"),
                    &FederatedTrustRotationEvent::MarkRecoveryRequired {
                        reason: "local activation barrier committed while peer rollback was not bilaterally confirmed".into(),
                    },
                )
                .await?,
            )?;
            let state = peer
                .get_remote_trust_rotation(&self.local_cluster_id, rotation_id)
                .await?;
            remote_snapshot = serde_json::from_str(&state.state_json)
                .map_err(|error| crate::DatspoolError::Serialization(error.to_string()))?;
        }

        if !matches!(
            remote_snapshot.rotation.phase,
            FederatedTrustRotationPhase::ActivationBarrier
                | FederatedTrustRotationPhase::Activating
                | FederatedTrustRotationPhase::RecoveryRequired
                | FederatedTrustRotationPhase::Overlap
                | FederatedTrustRotationPhase::Retiring
                | FederatedTrustRotationPhase::Completed
        ) {
            return Err(crate::DatspoolError::Configuration(
                "peer is not at a phase that permits activation authorization".into(),
            ));
        }
        self.ensure_remote_success(
            peer.advance_remote_trust_rotation(
                &self.local_cluster_id,
                rotation_id,
                &format!("{request_id}:peer"),
                &FederatedTrustRotationEvent::AuthorizeActivation,
            )
            .await?,
        )?;
        self.advance_local(
            rotation_id,
            FederatedTrustRotationEvent::AuthorizeActivation,
            request_id,
            chrono::Utc::now().timestamp_millis(),
            actor,
        )
        .await?;
        self.get_rotation(rotation_id).await
    }

    /// Activate the local peer CA and ask the peer to activate our CA. If one side
    /// succeeds and the other is unavailable, the durable state enters forward
    /// recovery; it is never rolled back after the first activation.
    pub async fn activate_both(
        &self,
        rotation_id: &str,
        request_id: &str,
        actor: &str,
    ) -> crate::Result<FederatedTrustRotation> {
        if request_id.trim().is_empty() || request_id.len() > 64 {
            return Err(crate::DatspoolError::Configuration(
                "request_id must contain 1-64 bytes".into(),
            ));
        }
        let rotation = self.get_rotation(rotation_id).await?;
        let audit_actor = format!("{actor} request:{request_id}");
        if !rotation.activation_barrier_committed {
            return Err(crate::DatspoolError::Configuration(
                "bilateral activation barrier has not been committed".into(),
            ));
        }
        let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
        let remote_state = peer
            .get_remote_trust_rotation(&self.local_cluster_id, rotation_id)
            .await?;
        if !remote_state.accepted {
            return Err(crate::DatspoolError::Internal(
                "peer rejected the durable trust-rotation read before CA activation".into(),
            ));
        }
        let remote_snapshot: FederatedTrustRotationSnapshot =
            serde_json::from_str(&remote_state.state_json)
                .map_err(|error| crate::DatspoolError::Serialization(error.to_string()))?;
        if !remote_snapshot.rotation.activation_barrier_committed
            || matches!(
                remote_snapshot.rotation.phase,
                FederatedTrustRotationPhase::RollingBack | FederatedTrustRotationPhase::RolledBack
            )
        {
            return Err(crate::DatspoolError::Configuration(
                "CA activation requires both committed activation barriers and no active rollback"
                    .into(),
            ));
        }
        let local_expiry = if let Some(expiry) = rotation.local_activation_expires_at_ms {
            expiry
        } else {
            let now = chrono::Utc::now().timestamp_millis();
            self.ensure_success(
                self.raft
                    .propose(Command::ActivateFederatedTrustBundle {
                        cluster_id: rotation.peer_cluster_id.clone(),
                        bundle_fingerprint: rotation.peer_ca_bundle_fingerprint.clone(),
                        at_ms: now,
                        overlap_ms: rotation.overlap_ms,
                        request_id: format!("federation-trust:{rotation_id}:activate-local"),
                        actor: audit_actor.clone(),
                    })
                    .await?,
            )?;
            let activated = self.peer_record(&rotation.peer_cluster_id).await?;
            if !activated
                .trust_bundle_fingerprint
                .as_deref()
                .is_some_and(|fingerprint| {
                    fingerprint.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
                })
            {
                return Err(crate::DatspoolError::Internal(
                    "local CA activation did not become the active peer trust bundle".into(),
                ));
            }
            activated
                .previous_trust_bundle_expires_at_ms
                .ok_or_else(|| {
                    crate::DatspoolError::Internal(
                        "activated peer trust bundle did not persist an overlap expiry".into(),
                    )
                })?
        };
        self.advance_local(
            rotation_id,
            FederatedTrustRotationEvent::LocalActivated {
                previous_expires_at_ms: local_expiry,
            },
            &format!("federation-trust:{rotation_id}:event-local-activated"),
            local_expiry.saturating_sub(rotation.overlap_ms),
            &audit_actor,
        )
        .await?;

        let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
        self.ensure_remote_success(
            peer.advance_remote_trust_rotation(
                &self.local_cluster_id,
                rotation_id,
                &format!("federation-trust:{rotation_id}:peer-records-local-activation"),
                &FederatedTrustRotationEvent::LocalActivated {
                    previous_expires_at_ms: local_expiry,
                },
            )
            .await?,
        )?;
        let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
        let remote_activation = peer
            .advance_remote_trust_rotation(
                &self.local_cluster_id,
                rotation_id,
                &format!("federation-trust:{rotation_id}:activate-peer"),
                &FederatedTrustRotationEvent::PeerActivated {
                    previous_expires_at_ms: local_expiry,
                },
            )
            .await;
        match remote_activation {
            Ok(response) if response.accepted => {}
            Ok(response) => {
                self.mark_recovery_required(rotation_id, &response.message, &audit_actor)
                    .await?;
                return Err(crate::DatspoolError::Internal(format!(
                    "Peer activation requires forward recovery: {}",
                    response.message
                )));
            }
            Err(error) => {
                self.mark_recovery_required(rotation_id, &error.to_string(), &audit_actor)
                    .await?;
                return Err(error);
            }
        }
        self.advance_local(
            rotation_id,
            FederatedTrustRotationEvent::PeerActivated {
                previous_expires_at_ms: local_expiry,
            },
            &format!("federation-trust:{rotation_id}:event-peer-activated"),
            local_expiry.saturating_sub(rotation.overlap_ms),
            &audit_actor,
        )
        .await?;
        self.get_rotation(rotation_id).await
    }

    /// Retire both old CA bundles only after the later of the two committed
    /// overlap deadlines. Partial retirement remains retryable and forward-only.
    pub async fn retire_both(
        &self,
        rotation_id: &str,
        request_id: &str,
        actor: &str,
    ) -> crate::Result<FederatedTrustRotation> {
        if request_id.trim().is_empty() || request_id.len() > 64 {
            return Err(crate::DatspoolError::Configuration(
                "request_id must contain 1-64 bytes".into(),
            ));
        }
        let rotation = self.get_rotation(rotation_id).await?;
        let audit_actor = format!("{actor} request:{request_id}");
        let deadline = rotation
            .local_activation_expires_at_ms
            .zip(rotation.peer_activation_expires_at_ms)
            .map(|(local, peer)| local.max(peer))
            .ok_or_else(|| {
                crate::DatspoolError::Configuration(
                    "both sides must activate before retirement".into(),
                )
            })?;
        let now = chrono::Utc::now().timestamp_millis();
        if now < deadline {
            return Err(crate::DatspoolError::Configuration(format!(
                "CA overlap remains active until {deadline}"
            )));
        }
        if !rotation.local_retired {
            self.ensure_success(
                self.raft
                    .propose(Command::RetireFederatedTrustBundle {
                        cluster_id: rotation.peer_cluster_id.clone(),
                        at_ms: now,
                        request_id: format!("federation-trust:{rotation_id}:retire-local"),
                        actor: audit_actor.clone(),
                    })
                    .await?,
            )?;
            self.advance_local(
                rotation_id,
                FederatedTrustRotationEvent::LocalRetired,
                &format!("federation-trust:{rotation_id}:event-local-retired"),
                now,
                &audit_actor,
            )
            .await?;
        }
        let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
        self.ensure_remote_success(
            peer.advance_remote_trust_rotation(
                &self.local_cluster_id,
                rotation_id,
                &format!("federation-trust:{rotation_id}:peer-records-local-retirement"),
                &FederatedTrustRotationEvent::LocalRetired,
            )
            .await?,
        )?;
        let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
        self.ensure_remote_success(
            peer.advance_remote_trust_rotation(
                &self.local_cluster_id,
                rotation_id,
                &format!("federation-trust:{rotation_id}:retire-peer"),
                &FederatedTrustRotationEvent::PeerRetired,
            )
            .await?,
        )?;
        self.advance_local(
            rotation_id,
            FederatedTrustRotationEvent::PeerRetired,
            &format!("federation-trust:{rotation_id}:event-peer-retired"),
            now,
            &audit_actor,
        )
        .await?;
        self.get_rotation(rotation_id).await
    }

    /// Roll back staged roots on both sides. The state machine refuses this path
    /// after either Activate has committed; such failures must use forward recovery.
    pub async fn rollback_before_activation(
        &self,
        rotation_id: &str,
        reason: &str,
        request_id: &str,
        actor: &str,
    ) -> crate::Result<FederatedTrustRotation> {
        if request_id.trim().is_empty() || request_id.len() > 64 {
            return Err(crate::DatspoolError::Configuration(
                "request_id must contain 1-64 bytes".into(),
            ));
        }
        let rotation = self.get_rotation(rotation_id).await?;
        let audit_actor = format!("{actor} request:{request_id}");
        let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
        let remote_state = peer
            .get_remote_trust_rotation(&self.local_cluster_id, rotation_id)
            .await?;
        let remote_snapshot: FederatedTrustRotationSnapshot =
            serde_json::from_str(&remote_state.state_json)
                .map_err(|error| crate::DatspoolError::Serialization(error.to_string()))?;
        let mut remote_rotation = remote_snapshot.rotation;
        for existing_reason in [
            rotation.rollback_reason.as_deref(),
            remote_rotation.rollback_reason.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if existing_reason != reason {
                return Err(crate::DatspoolError::Configuration(
                    "rollback reason cannot change after either side has begun rollback".into(),
                ));
            }
        }
        let local_peer_record = self.peer_record(&rotation.peer_cluster_id).await?;
        let local_ca_already_active = local_peer_record
            .trust_bundle_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| {
                fingerprint.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
            });
        let local_ca_was_active = local_peer_record
            .previous_trust_bundle_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| {
                fingerprint.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
            });
        let remote_ca_already_active = remote_snapshot
            .peer_active_ca_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| {
                fingerprint.eq_ignore_ascii_case(&rotation.local_ca_bundle_fingerprint)
            });
        let remote_ca_was_active = remote_snapshot
            .peer_previous_ca_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| {
                fingerprint.eq_ignore_ascii_case(&rotation.local_ca_bundle_fingerprint)
            });
        let activation_uncertain = rotation.activation_barrier_committed
            || remote_rotation.activation_barrier_committed
            || rotation.phase == FederatedTrustRotationPhase::RecoveryRequired
            || remote_rotation.phase == FederatedTrustRotationPhase::RecoveryRequired
            || rotation.local_activation_expires_at_ms.is_some()
            || rotation.peer_activation_expires_at_ms.is_some()
            || remote_rotation.local_activation_expires_at_ms.is_some()
            || remote_rotation.peer_activation_expires_at_ms.is_some()
            || local_ca_already_active
            || local_ca_was_active
            || remote_ca_already_active
            || remote_ca_was_active;
        if activation_uncertain {
            if (rotation.phase == FederatedTrustRotationPhase::RollingBack
                && rotation.rollback_peer_confirmed)
                || (remote_rotation.phase == FederatedTrustRotationPhase::RollingBack
                    && remote_rotation.rollback_peer_confirmed)
            {
                return Err(crate::DatspoolError::Internal(
                    "activation evidence conflicts with a bilaterally confirmed rollback; manual intervention is required".into(),
                ));
            }
            self.mark_recovery_required(
                rotation_id,
                "a CA activation is committed or uncertain; forward recovery is required",
                &audit_actor,
            )
            .await?;
            return Err(crate::DatspoolError::Configuration(
                "rollback is forbidden because at least one federation side has activated or activation is uncertain; forward recovery is required".into(),
            ));
        }
        let begin = FederatedTrustRotationEvent::BeginRollback {
            reason: reason.to_string(),
        };
        if !matches!(
            rotation.phase,
            FederatedTrustRotationPhase::RollingBack | FederatedTrustRotationPhase::RolledBack
        ) {
            self.advance_local(
                rotation_id,
                begin.clone(),
                request_id,
                chrono::Utc::now().timestamp_millis(),
                &audit_actor,
            )
            .await?;
        }
        if !matches!(
            remote_rotation.phase,
            FederatedTrustRotationPhase::RollingBack | FederatedTrustRotationPhase::RolledBack
        ) {
            let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
            self.ensure_remote_success(
                peer.advance_remote_trust_rotation(
                    &self.local_cluster_id,
                    rotation_id,
                    &format!("{request_id}:peer-begin"),
                    &begin,
                )
                .await?,
            )?;
        }

        // The local coordinator has observed both durable BeginRollback commits.
        // Record that bilateral barrier on each side before allowing material
        // cleanup; each confirmation has its own replay-stable request identity.
        let current_rotation = self.get_rotation(rotation_id).await?;
        if current_rotation.phase == FederatedTrustRotationPhase::RollingBack
            && !current_rotation.rollback_peer_confirmed
        {
            self.advance_local(
                rotation_id,
                FederatedTrustRotationEvent::ConfirmRollback,
                &format!("{request_id}:confirm-local"),
                chrono::Utc::now().timestamp_millis(),
                &audit_actor,
            )
            .await?;
        }
        let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
        let remote_state = peer
            .get_remote_trust_rotation(&self.local_cluster_id, rotation_id)
            .await?;
        remote_rotation =
            serde_json::from_str::<FederatedTrustRotationSnapshot>(&remote_state.state_json)
                .map_err(|error| crate::DatspoolError::Serialization(error.to_string()))?
                .rotation;
        if remote_rotation.phase == FederatedTrustRotationPhase::RollingBack
            && !remote_rotation.rollback_peer_confirmed
        {
            self.ensure_remote_success(
                peer.advance_remote_trust_rotation(
                    &self.local_cluster_id,
                    rotation_id,
                    &format!("{request_id}:confirm-peer"),
                    &FederatedTrustRotationEvent::ConfirmRollback,
                )
                .await?,
            )?;
            let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
            let state = peer
                .get_remote_trust_rotation(&self.local_cluster_id, rotation_id)
                .await?;
            remote_rotation =
                serde_json::from_str::<FederatedTrustRotationSnapshot>(&state.state_json)
                    .map_err(|error| crate::DatspoolError::Serialization(error.to_string()))?
                    .rotation;
        }

        // Resume each acknowledgement independently. A crash may happen after one
        // side cleared one staged flag but before the second acknowledgement.
        if remote_rotation.phase == FederatedTrustRotationPhase::RollingBack
            && remote_rotation.peer_staged
        {
            let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
            self.ensure_remote_success(
                peer.advance_remote_trust_rotation(
                    &self.local_cluster_id,
                    rotation_id,
                    &format!("{request_id}:peer-observes-local-rollback"),
                    &FederatedTrustRotationEvent::LocalRolledBack,
                )
                .await?,
            )?;
            let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
            let state = peer
                .get_remote_trust_rotation(&self.local_cluster_id, rotation_id)
                .await?;
            remote_rotation =
                serde_json::from_str::<FederatedTrustRotationSnapshot>(&state.state_json)
                    .map_err(|error| crate::DatspoolError::Serialization(error.to_string()))?
                    .rotation;
        }
        if remote_rotation.phase == FederatedTrustRotationPhase::RollingBack
            && remote_rotation.local_staged
        {
            let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
            self.ensure_remote_success(
                peer.advance_remote_trust_rotation(
                    &self.local_cluster_id,
                    rotation_id,
                    &format!("{request_id}:abort-peer"),
                    &FederatedTrustRotationEvent::PeerRolledBack,
                )
                .await?,
            )?;
        }

        let current_rotation = self.get_rotation(rotation_id).await?;
        if current_rotation.phase != FederatedTrustRotationPhase::RolledBack {
            let current_peer = self.peer_record(&rotation.peer_cluster_id).await?;
            if current_peer
                .pending_identity_fingerprint
                .as_deref()
                .is_some_and(|fingerprint| {
                    fingerprint.eq_ignore_ascii_case(&rotation.peer_leaf_fingerprint)
                })
            {
                self.ensure_success(
                    self.raft
                        .propose(Command::AbortFederatedCertificateRotation {
                            cluster_id: rotation.peer_cluster_id.clone(),
                            new_fingerprint: rotation.peer_leaf_fingerprint.clone(),
                            at_ms: chrono::Utc::now().timestamp_millis(),
                            request_id: format!("federation-trust:{rotation_id}:abort-local-leaf"),
                            actor: audit_actor.clone(),
                        })
                        .await?,
                )?;
            } else if current_peer
                .identity_fingerprint
                .eq_ignore_ascii_case(&rotation.peer_leaf_fingerprint)
            {
                self.ensure_success(
                    self.raft
                        .propose(Command::RollbackFederatedCertificateRotation {
                            cluster_id: rotation.peer_cluster_id.clone(),
                            new_fingerprint: rotation.peer_leaf_fingerprint.clone(),
                            at_ms: chrono::Utc::now().timestamp_millis(),
                            request_id: format!(
                                "federation-trust:{rotation_id}:rollback-local-leaf"
                            ),
                            actor: audit_actor.clone(),
                        })
                        .await?,
                )?;
            }
            if current_peer
                .pending_trust_bundle_fingerprint
                .as_deref()
                .is_some_and(|fingerprint| {
                    fingerprint.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
                })
            {
                self.ensure_success(
                    self.raft
                        .propose(Command::AbortFederatedTrustBundle {
                            cluster_id: rotation.peer_cluster_id.clone(),
                            bundle_fingerprint: rotation.peer_ca_bundle_fingerprint.clone(),
                            at_ms: chrono::Utc::now().timestamp_millis(),
                            request_id: format!("federation-trust:{rotation_id}:abort-local"),
                            actor: audit_actor.clone(),
                        })
                        .await?,
                )?;
            }
            let current_rotation = self.get_rotation(rotation_id).await?;
            if current_rotation.phase == FederatedTrustRotationPhase::RollingBack
                && current_rotation.local_staged
            {
                self.advance_local(
                    rotation_id,
                    FederatedTrustRotationEvent::LocalRolledBack,
                    &format!("{request_id}:event-local-rolled-back"),
                    chrono::Utc::now().timestamp_millis(),
                    &audit_actor,
                )
                .await?;
            }
            let current_rotation = self.get_rotation(rotation_id).await?;
            if current_rotation.phase == FederatedTrustRotationPhase::RollingBack
                && current_rotation.peer_staged
            {
                self.advance_local(
                    rotation_id,
                    FederatedTrustRotationEvent::PeerRolledBack,
                    &format!("{request_id}:event-peer-rolled-back"),
                    chrono::Utc::now().timestamp_millis(),
                    &audit_actor,
                )
                .await?;
            }
        }
        self.get_rotation(rotation_id).await
    }

    /// Reconcile durable workflow state after leader/process restart. Staging is
    /// replayed idempotently; activation only proceeds after the peer has committed
    /// the operator-authorized barrier; overlap retirement is deadline-gated.
    pub async fn reconcile_once(&self) -> crate::Result<()> {
        let rotations = self.store.list_federated_trust_rotations().await?;
        for rotation in rotations {
            let digest = Sha256::digest(rotation.rotation_id.as_bytes());
            let request_id = format!(
                "reconcile-{}",
                digest[..12]
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
            let result = async {
                match rotation.phase {
                    FederatedTrustRotationPhase::Staging => self
                        .start_rotation(
                            &rotation.rotation_id,
                            &rotation.peer_cluster_id,
                            rotation.local_ca_bundle_pem.clone(),
                            rotation.peer_ca_bundle_pem.clone(),
                            &rotation.local_leaf_fingerprint,
                            &rotation.peer_leaf_fingerprint,
                            rotation.overlap_ms,
                            &request_id,
                            "trust-rotation-reconciler",
                        )
                        .await
                        .map(|_| ()),
                    FederatedTrustRotationPhase::ActivationBarrier => {
                        let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
                        let remote = peer
                            .get_remote_trust_rotation(
                                &self.local_cluster_id,
                                &rotation.rotation_id,
                            )
                            .await?;
                        let snapshot: FederatedTrustRotationSnapshot =
                            serde_json::from_str(&remote.state_json).map_err(|error| {
                                crate::DatspoolError::Serialization(error.to_string())
                            })?;
                        if snapshot.rotation.activation_barrier_committed {
                            self.authorize_activation(
                                &rotation.rotation_id,
                                &request_id,
                                "trust-rotation-reconciler",
                            )
                            .await
                            .map(|_| ())
                        } else {
                            Ok(())
                        }
                    }
                    FederatedTrustRotationPhase::Activating
                    | FederatedTrustRotationPhase::RecoveryRequired => {
                        self.authorize_activation(
                            &rotation.rotation_id,
                            &request_id,
                            "trust-rotation-reconciler",
                        )
                        .await?;
                        self.activate_both(
                            &rotation.rotation_id,
                            &request_id,
                            "trust-rotation-reconciler",
                        )
                        .await
                        .map(|_| ())
                    }
                    FederatedTrustRotationPhase::Overlap
                    | FederatedTrustRotationPhase::Retiring => {
                        let deadline = rotation
                            .local_activation_expires_at_ms
                            .zip(rotation.peer_activation_expires_at_ms)
                            .map(|(local, peer)| local.max(peer));
                        if deadline.is_some_and(|deadline| {
                            chrono::Utc::now().timestamp_millis() >= deadline
                        }) {
                            self.retire_both(
                                &rotation.rotation_id,
                                &request_id,
                                "trust-rotation-reconciler",
                            )
                            .await
                            .map(|_| ())
                        } else {
                            Ok(())
                        }
                    }
                    FederatedTrustRotationPhase::RollingBack => self
                        .rollback_before_activation(
                            &rotation.rotation_id,
                            rotation
                                .rollback_reason
                                .as_deref()
                                .unwrap_or("resuming interrupted rollback"),
                            &request_id,
                            "trust-rotation-reconciler",
                        )
                        .await
                        .map(|_| ()),
                    FederatedTrustRotationPhase::AwaitingDeployment
                    | FederatedTrustRotationPhase::Completed
                    | FederatedTrustRotationPhase::RolledBack => Ok(()),
                }
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(rotation_id = %rotation.rotation_id, phase = ?rotation.phase, %error, "Federated trust rotation reconciliation will retry");
            }
        }
        Ok(())
    }

    pub async fn run_until_cancelled(&self, shutdown: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(error) = self.reconcile_once().await {
                        tracing::warn!(%error, "Federated trust rotation reconciliation pass failed");
                    }
                }
            }
        }
    }

    pub async fn get_rotation(&self, rotation_id: &str) -> crate::Result<FederatedTrustRotation> {
        self.store
            .get_federated_trust_rotation(rotation_id)
            .await?
            .ok_or_else(|| {
                crate::DatspoolError::Configuration(format!(
                    "Federated trust rotation '{rotation_id}' not found"
                ))
            })
    }

    /// Read both durable Raft views and return a sanitized operational diagnosis.
    /// CA bundle PEM and private-key material are intentionally never included.
    pub async fn inspect_bilateral(
        &self,
        rotation_id: &str,
    ) -> crate::Result<FederatedTrustRotationDiagnostics> {
        let rotation = self.get_rotation(rotation_id).await?;
        let peer_record = self.peer_record(&rotation.peer_cluster_id).await?;
        let mut peer_client = self.connect_peer(&peer_record).await?;
        let response = peer_client
            .get_remote_trust_rotation(&self.local_cluster_id, rotation_id)
            .await?;
        if !response.accepted {
            return Err(crate::DatspoolError::Internal(
                "peer rejected the bilateral trust-rotation diagnostic read".into(),
            ));
        }
        let peer_snapshot: FederatedTrustRotationSnapshot =
            serde_json::from_str(&response.state_json)
                .map_err(|error| crate::DatspoolError::Serialization(error.to_string()))?;
        if rotation.peer_cluster_id != peer_record.cluster_id
            || peer_snapshot.rotation.rotation_id != rotation_id
            || peer_snapshot.rotation.peer_cluster_id != self.local_cluster_id
        {
            return Err(crate::DatspoolError::Configuration(
                "bilateral diagnostic read returned a mismatched rotation or cluster identity"
                    .into(),
            ));
        }
        let local_snapshot = FederatedTrustRotationSnapshot {
            rotation,
            peer_active_ca_fingerprint: peer_record.trust_bundle_fingerprint,
            peer_pending_ca_fingerprint: peer_record.pending_trust_bundle_fingerprint,
            peer_previous_ca_fingerprint: peer_record.previous_trust_bundle_fingerprint,
            peer_previous_ca_expires_at_ms: peer_record.previous_trust_bundle_expires_at_ms,
            peer_active_identity_fingerprint: peer_record.identity_fingerprint,
            peer_pending_identity_fingerprint: peer_record.pending_identity_fingerprint,
        };
        Ok(build_rotation_diagnostics(
            rotation_id,
            &self.local_cluster_id,
            &peer_record.cluster_id,
            local_snapshot,
            peer_snapshot,
            chrono::Utc::now().timestamp_millis(),
        ))
    }

    /// Safely resume one rotation using only its persisted lifecycle phase. This
    /// method never bypasses deployment readiness, the activation barrier, the
    /// overlap deadline, or the bilateral rollback confirmation fence.
    pub async fn recover_rotation(
        &self,
        rotation_id: &str,
        expected_action: &FederatedTrustRotationRecoveryAction,
        request_id: &str,
        actor: &str,
    ) -> crate::Result<FederatedTrustRotation> {
        if request_id.trim().is_empty() || request_id.len() > 64 {
            return Err(crate::DatspoolError::Configuration(
                "request_id must contain 1-64 bytes".into(),
            ));
        }
        let rotation = self.get_rotation(rotation_id).await?;
        let diagnostics = self.inspect_bilateral(rotation_id).await?;
        if !diagnostics.safety_violations.is_empty()
            || !diagnostics.automated_recovery_safe
            || diagnostics.recommended_action != expected_action.clone()
        {
            return Err(crate::DatspoolError::Configuration(format!(
                "recovery precondition changed: expected action {:?}, current action {:?}, safety violations: {}",
                expected_action,
                diagnostics.recommended_action,
                diagnostics.safety_violations.join("; ")
            )));
        }
        if diagnostics.peer.phase == FederatedTrustRotationPhase::RollingBack
            && matches!(
                rotation.phase,
                FederatedTrustRotationPhase::Staging
                    | FederatedTrustRotationPhase::AwaitingDeployment
                    | FederatedTrustRotationPhase::ActivationBarrier
                    | FederatedTrustRotationPhase::RolledBack
            )
            && !diagnostics.local.activation_barrier_committed
            && !diagnostics.peer.activation_barrier_committed
        {
            let reason = diagnostics.peer.rollback_reason.as_deref().ok_or_else(|| {
                crate::DatspoolError::Internal(
                    "peer RollingBack state has no persisted rollback reason".into(),
                )
            })?;
            return self
                .rollback_before_activation(rotation_id, reason, request_id, actor)
                .await;
        }
        match rotation.phase {
            FederatedTrustRotationPhase::Staging => {
                self.start_rotation(
                    &rotation.rotation_id,
                    &rotation.peer_cluster_id,
                    rotation.local_ca_bundle_pem.clone(),
                    rotation.peer_ca_bundle_pem.clone(),
                    &rotation.local_leaf_fingerprint,
                    &rotation.peer_leaf_fingerprint,
                    rotation.overlap_ms,
                    request_id,
                    actor,
                )
                .await
            }
            FederatedTrustRotationPhase::ActivationBarrier => {
                let peer = self.peer_record(&rotation.peer_cluster_id).await?;
                let mut client = self.connect_peer(&peer).await?;
                let response = client
                    .get_remote_trust_rotation(&self.local_cluster_id, rotation_id)
                    .await?;
                if !response.accepted {
                    return Err(crate::DatspoolError::Internal(
                        "peer rejected the activation-barrier recovery read".into(),
                    ));
                }
                let remote: FederatedTrustRotationSnapshot =
                    serde_json::from_str(&response.state_json)
                        .map_err(|error| crate::DatspoolError::Serialization(error.to_string()))?;
                if !rotation.activation_barrier_committed
                    && !remote.rotation.activation_barrier_committed
                {
                    return Ok(rotation);
                }
                self.authorize_activation(rotation_id, request_id, actor)
                    .await?;
                self.activate_both(rotation_id, request_id, actor).await
            }
            FederatedTrustRotationPhase::Activating
            | FederatedTrustRotationPhase::RecoveryRequired => {
                self.authorize_activation(rotation_id, request_id, actor)
                    .await?;
                self.activate_both(rotation_id, request_id, actor).await
            }
            FederatedTrustRotationPhase::RollingBack => {
                let reason = rotation.rollback_reason.as_deref().ok_or_else(|| {
                    crate::DatspoolError::Internal(
                        "rolling-back rotation has no persisted rollback reason".into(),
                    )
                })?;
                match self
                    .rollback_before_activation(rotation_id, reason, request_id, actor)
                    .await
                {
                    Ok(rotation) => Ok(rotation),
                    Err(error) => {
                        let current = self.get_rotation(rotation_id).await?;
                        if current.phase != FederatedTrustRotationPhase::RecoveryRequired {
                            return Err(error);
                        }
                        self.authorize_activation(rotation_id, request_id, actor)
                            .await?;
                        self.activate_both(rotation_id, request_id, actor).await
                    }
                }
            }
            FederatedTrustRotationPhase::Overlap | FederatedTrustRotationPhase::Retiring => {
                let deadline = rotation
                    .local_activation_expires_at_ms
                    .zip(rotation.peer_activation_expires_at_ms)
                    .map(|(local, peer)| local.max(peer))
                    .ok_or_else(|| {
                        crate::DatspoolError::Internal(
                            "overlap or retiring rotation is missing a committed expiry".into(),
                        )
                    })?;
                if chrono::Utc::now().timestamp_millis() < deadline {
                    return Ok(rotation);
                }
                self.retire_both(rotation_id, request_id, actor).await
            }
            FederatedTrustRotationPhase::Completed => {
                if matches!(
                    diagnostics.peer.phase,
                    FederatedTrustRotationPhase::Overlap | FederatedTrustRotationPhase::Retiring
                ) {
                    let deadline = rotation
                        .local_activation_expires_at_ms
                        .zip(rotation.peer_activation_expires_at_ms)
                        .map(|(local, peer)| local.max(peer));
                    if deadline.is_some_and(|value| chrono::Utc::now().timestamp_millis() >= value)
                    {
                        return self.retire_both(rotation_id, request_id, actor).await;
                    }
                }
                Ok(rotation)
            }
            FederatedTrustRotationPhase::AwaitingDeployment
            | FederatedTrustRotationPhase::RolledBack => Ok(rotation),
        }
    }

    async fn advance_local(
        &self,
        rotation_id: &str,
        event: FederatedTrustRotationEvent,
        request_id: &str,
        at_ms: i64,
        actor: &str,
    ) -> crate::Result<()> {
        let rotation = self.get_rotation(rotation_id).await?;
        self.ensure_success(
            self.raft
                .propose(Command::AdvanceFederatedTrustRotation {
                    rotation_id: rotation_id.to_string(),
                    expected_revision: rotation.revision,
                    event,
                    at_ms,
                    request_id: request_id.to_string(),
                    actor: actor.to_string(),
                })
                .await?,
        )
    }

    async fn mark_recovery_required(
        &self,
        rotation_id: &str,
        reason: &str,
        actor: &str,
    ) -> crate::Result<()> {
        let rotation = self.get_rotation(rotation_id).await?;
        if rotation.phase == FederatedTrustRotationPhase::RecoveryRequired {
            return Ok(());
        }
        let reason = reason.chars().take(1024).collect::<String>();
        let digest = Sha256::digest(reason.as_bytes());
        let reason_key = digest[..8]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        self.advance_local(
            rotation_id,
            FederatedTrustRotationEvent::MarkRecoveryRequired { reason },
            &format!("federation-trust:{rotation_id}:recovery-{reason_key}"),
            chrono::Utc::now().timestamp_millis(),
            actor,
        )
        .await
    }

    async fn peer_record(&self, peer_cluster_id: &str) -> crate::Result<FederatedCluster> {
        self.store
            .get_federated_cluster(peer_cluster_id)
            .await?
            .filter(|peer| peer.status != crate::federation::FederatedClusterStatus::Offline)
            .ok_or_else(|| {
                crate::DatspoolError::Configuration(format!(
                    "Federated peer '{peer_cluster_id}' is not registered or is offline"
                ))
            })
    }

    async fn connect_peer_by_id(&self, peer_cluster_id: &str) -> crate::Result<GrpcClient> {
        let peer = self.peer_record(peer_cluster_id).await?;
        self.connect_peer(&peer).await
    }

    async fn connect_peer(&self, peer: &FederatedCluster) -> crate::Result<GrpcClient> {
        if let Some(bundle) = peer.handshake_trust_bundle(chrono::Utc::now().timestamp_millis()) {
            GrpcClient::connect_mtls_with_ca_pem(
                &peer.endpoint,
                bundle.as_bytes(),
                &self.certificate_file,
                &self.private_key_file,
            )
            .await
        } else {
            GrpcClient::connect_mtls(
                &peer.endpoint,
                &self.ca_file,
                &self.certificate_file,
                &self.private_key_file,
            )
            .await
        }
    }

    fn ensure_success(&self, response: crate::raft::CommandResponse) -> crate::Result<()> {
        if response.success {
            Ok(())
        } else {
            Err(crate::DatspoolError::Internal(response.message))
        }
    }

    fn ensure_remote_success(
        &self,
        response: crate::comms::task::FederatedTrustRotationResponse,
    ) -> crate::Result<()> {
        if response.accepted {
            Ok(())
        } else {
            Err(crate::DatspoolError::Internal(response.message))
        }
    }
}

#[cfg(test)]
mod diagnostics_tests {
    use super::*;

    fn snapshots(
        local_phase: FederatedTrustRotationPhase,
        peer_phase: FederatedTrustRotationPhase,
    ) -> (
        FederatedTrustRotationSnapshot,
        FederatedTrustRotationSnapshot,
    ) {
        let local = FederatedTrustRotation {
            rotation_id: "diagnostic-rotation".into(),
            peer_cluster_id: "target".into(),
            current_peer_ca_fingerprint: Some("0".repeat(64)),
            local_ca_bundle_pem: "local-ca-pem".into(),
            local_ca_bundle_fingerprint: "a".repeat(64),
            peer_ca_bundle_pem: "peer-ca-pem".into(),
            peer_ca_bundle_fingerprint: "b".repeat(64),
            local_leaf_fingerprint: "c".repeat(64),
            peer_leaf_fingerprint: "d".repeat(64),
            overlap_ms: 60_000,
            phase: local_phase,
            local_staged: true,
            peer_staged: true,
            local_deployment_ready: true,
            peer_deployment_ready: true,
            activation_barrier_committed: false,
            local_activation_expires_at_ms: None,
            peer_activation_expires_at_ms: None,
            local_retired: false,
            peer_retired: false,
            rollback_reason: Some("operator requested rollback".into()),
            rollback_peer_confirmed: false,
            last_error: None,
            revision: 5,
            created_at_ms: 1,
            updated_at_ms: 99_000,
        };
        let peer = FederatedTrustRotation {
            rotation_id: "diagnostic-rotation".into(),
            peer_cluster_id: "source".into(),
            current_peer_ca_fingerprint: Some("0".repeat(64)),
            local_ca_bundle_pem: "peer-ca-pem".into(),
            local_ca_bundle_fingerprint: "b".repeat(64),
            peer_ca_bundle_pem: "local-ca-pem".into(),
            peer_ca_bundle_fingerprint: "a".repeat(64),
            local_leaf_fingerprint: "d".repeat(64),
            peer_leaf_fingerprint: "c".repeat(64),
            overlap_ms: 60_000,
            phase: peer_phase,
            local_staged: true,
            peer_staged: true,
            local_deployment_ready: true,
            peer_deployment_ready: true,
            activation_barrier_committed: false,
            local_activation_expires_at_ms: None,
            peer_activation_expires_at_ms: None,
            local_retired: false,
            peer_retired: false,
            rollback_reason: None,
            rollback_peer_confirmed: false,
            last_error: None,
            revision: 4,
            created_at_ms: 1,
            updated_at_ms: 99_500,
        };
        let snapshot = |rotation: FederatedTrustRotation| FederatedTrustRotationSnapshot {
            rotation,
            peer_active_ca_fingerprint: Some("0".repeat(64)),
            peer_pending_ca_fingerprint: None,
            peer_previous_ca_fingerprint: None,
            peer_previous_ca_expires_at_ms: None,
            peer_active_identity_fingerprint: "e".repeat(64),
            peer_pending_identity_fingerprint: None,
        };
        (snapshot(local), snapshot(peer))
    }

    #[test]
    fn diagnostics_recommend_resume_for_pre_activation_rollback_drift() {
        let (local, peer) = snapshots(
            FederatedTrustRotationPhase::RollingBack,
            FederatedTrustRotationPhase::ActivationBarrier,
        );
        let diagnostics = build_rotation_diagnostics(
            "diagnostic-rotation",
            "source",
            "target",
            local,
            peer,
            100_000,
        );
        assert!(!diagnostics.state_aligned);
        assert!(diagnostics.safety_violations.is_empty());
        assert_eq!(
            diagnostics.recommended_action,
            FederatedTrustRotationRecoveryAction::ResumeRollback
        );
        assert!(diagnostics.automated_recovery_safe);
        let serialized = serde_json::to_string(&diagnostics).unwrap();
        assert!(!serialized.contains("local-ca-pem"));
        assert!(!serialized.contains("peer-ca-pem"));
    }

    #[test]
    fn diagnostics_promote_unconfirmed_rollback_when_peer_barrier_committed() {
        let (local, mut peer) = snapshots(
            FederatedTrustRotationPhase::RollingBack,
            FederatedTrustRotationPhase::Activating,
        );
        peer.rotation.activation_barrier_committed = true;
        let diagnostics = build_rotation_diagnostics(
            "diagnostic-rotation",
            "source",
            "target",
            local,
            peer,
            100_000,
        );
        assert!(diagnostics.safety_violations.is_empty());
        assert_eq!(
            diagnostics.recommended_action,
            FederatedTrustRotationRecoveryAction::ForwardRecovery
        );
        assert!(diagnostics.automated_recovery_safe);
    }

    #[test]
    fn diagnostics_choose_forward_recovery_after_peer_activation_commit() {
        let (local, mut peer) = snapshots(
            FederatedTrustRotationPhase::RollingBack,
            FederatedTrustRotationPhase::Activating,
        );
        peer.rotation.local_activation_expires_at_ms = Some(160_000);
        peer.peer_active_ca_fingerprint = Some(peer.rotation.peer_ca_bundle_fingerprint.clone());
        peer.peer_previous_ca_fingerprint = Some("0".repeat(64));
        peer.peer_previous_ca_expires_at_ms = Some(160_000);
        let diagnostics = build_rotation_diagnostics(
            "diagnostic-rotation",
            "source",
            "target",
            local,
            peer,
            100_000,
        );
        assert!(diagnostics.safety_violations.is_empty());
        assert_eq!(
            diagnostics.recommended_action,
            FederatedTrustRotationRecoveryAction::ForwardRecovery
        );
        assert!(diagnostics.automated_recovery_safe);
    }

    #[test]
    fn diagnostics_require_manual_intervention_for_confirmed_rollback_conflict() {
        let (mut local, mut peer) = snapshots(
            FederatedTrustRotationPhase::RollingBack,
            FederatedTrustRotationPhase::Activating,
        );
        peer.rotation.activation_barrier_committed = true;
        local.rotation.rollback_peer_confirmed = true;
        let diagnostics = build_rotation_diagnostics(
            "diagnostic-rotation",
            "source",
            "target",
            local,
            peer,
            100_000,
        );
        assert!(!diagnostics.safety_violations.is_empty());
        assert_eq!(
            diagnostics.recommended_action,
            FederatedTrustRotationRecoveryAction::ManualInterventionRequired
        );
        assert!(!diagnostics.automated_recovery_safe);
    }
}
