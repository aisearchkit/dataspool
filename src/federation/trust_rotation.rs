// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 Dataspool Contributors

//! Durable coordinator for bilateral CA trust and leaf-certificate lifecycle changes.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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
        let rotation = self.get_rotation(rotation_id).await?;
        let mut peer = self.connect_peer_by_id(&rotation.peer_cluster_id).await?;
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
        if rotation.phase == FederatedTrustRotationPhase::RecoveryRequired
            || remote_rotation.phase == FederatedTrustRotationPhase::RecoveryRequired
            || rotation.local_activation_expires_at_ms.is_some()
            || rotation.peer_activation_expires_at_ms.is_some()
            || remote_rotation.local_activation_expires_at_ms.is_some()
            || remote_rotation.peer_activation_expires_at_ms.is_some()
            || local_ca_already_active
            || local_ca_was_active
            || remote_ca_already_active
            || remote_ca_was_active
        {
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
