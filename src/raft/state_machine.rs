// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Deterministic application of committed Raft commands.

use serde::{Deserialize, Serialize};
use sha2::Digest;
use tracing::info;

use super::Command;
use crate::error::{DatspoolError, Result};
use crate::storage::KvStore;
use crate::task::{ShardMeta, TaskStore};
use crate::types::{NodeId, ShardId, ShardState};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommandResponse {
    pub success: bool,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

pub struct MetaStateMachine<S: KvStore> {
    task_store: TaskStore<S>,
    applied_log_index: Option<u64>,
}

struct CacheBatchRequest {
    shard_id: String,
    node_id: String,
    fencing_token: u64,
    issued_at_ms: i64,
    batch_id: String,
    records: Vec<crate::pipeline::Record>,
    next_offset: Option<crate::pipeline::OffsetValue>,
    source_count: u64,
}

struct BatchDeliveryRequest {
    shard_id: String,
    node_id: String,
    fencing_token: u64,
    issued_at_ms: i64,
    batch_id: String,
    success_count: u64,
    failed_count: u64,
    bytes_written: u64,
}

struct CdcPartitionedCutoverRequest {
    task_id: String,
    alias: String,
    old_index: Option<String>,
    new_index: String,
    final_watermarks: std::collections::BTreeMap<u32, u64>,
    applied_watermarks: std::collections::BTreeMap<u32, u64>,
    fencing_shard_id: String,
    node_id: String,
    fencing_token: u64,
    issued_at_ms: i64,
}

struct CdcCutoverRequest {
    task_id: String,
    alias: String,
    old_index: Option<String>,
    new_index: String,
    final_watermark: u64,
    applied_watermark: u64,
    fencing_shard_id: String,
    node_id: String,
    fencing_token: u64,
    issued_at_ms: i64,
}

struct ShardProgressRequest {
    shard_id: String,
    node_id: String,
    fencing_token: u64,
    issued_at_ms: i64,
    processed_records: u64,
    processed_bytes: u64,
    offset: String,
    batch_id: Option<String>,
}

impl<S: KvStore + Clone> MetaStateMachine<S> {
    pub fn new(store: S) -> Self {
        Self {
            task_store: TaskStore::new(store),
            applied_log_index: None,
        }
    }

    pub async fn apply_command_at(
        &mut self,
        cmd: Command,
        log_index: u64,
    ) -> Result<CommandResponse> {
        let previous_index = self.applied_log_index.replace(log_index);
        let result = self.apply_command_inner(cmd).await;
        self.applied_log_index = previous_index;
        result
    }

    pub async fn apply_command(&mut self, cmd: Command) -> Result<CommandResponse> {
        let previous_index = self.applied_log_index.take();
        let result = self.apply_command_inner(cmd).await;
        self.applied_log_index = previous_index;
        result
    }

    async fn apply_command_inner(&mut self, cmd: Command) -> Result<CommandResponse> {
        info!("Applying committed command: {}", cmd.description());
        match cmd {
            Command::CreateTask {
                task,
                shards,
                request_id,
                actor,
            } => {
                self.apply_create_task(*task, shards, request_id, actor)
                    .await
            }
            Command::ClaimShard {
                shard_id,
                node_id,
                claim_id,
                issued_at_ms,
                lease_expires_at_ms,
            } => {
                self.apply_claim_shard(
                    shard_id,
                    node_id,
                    claim_id,
                    issued_at_ms,
                    lease_expires_at_ms,
                )
                .await
            }
            Command::ImportLegacyCache {
                shard_id,
                batch_id,
                records,
                next_offset,
                source_count,
            } => {
                self.apply_import_legacy_cache(
                    shard_id,
                    batch_id,
                    records,
                    next_offset,
                    source_count,
                )
                .await
            }
            Command::BootstrapComplete => {
                self.task_store.mark_bootstrap_complete().await?;
                Ok(accepted(
                    "Raft bootstrap completed",
                    serde_json::json!({"ready": true}),
                ))
            }
            Command::ReleaseShardLease {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
            } => {
                self.apply_release_lease(shard_id, node_id, fencing_token, issued_at_ms)
                    .await
            }
            Command::CacheBatch {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                batch_id,
                records,
                next_offset,
                source_count,
            } => {
                self.apply_cache_batch(CacheBatchRequest {
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    batch_id,
                    records,
                    next_offset,
                    source_count,
                })
                .await
            }
            Command::RecordBatchDelivery {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                batch_id,
                success_count,
                failed_count,
                bytes_written,
            } => {
                self.apply_record_batch_delivery(BatchDeliveryRequest {
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    batch_id,
                    success_count,
                    failed_count,
                    bytes_written,
                })
                .await
            }
            Command::PersistDeadLetters {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                failures,
            } => {
                self.apply_dead_letters(shard_id, node_id, fencing_token, issued_at_ms, failures)
                    .await
            }
            Command::AckCacheBatch {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                cache_key,
            } => {
                self.apply_ack_cache_batch(
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    cache_key,
                )
                .await
            }
            Command::RenewShardLease {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                lease_expires_at_ms,
            } => {
                self.apply_renew_lease(
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    lease_expires_at_ms,
                )
                .await
            }
            Command::UpdateShardProgress {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                processed_records,
                processed_bytes,
                offset,
                batch_id,
            } => {
                self.apply_progress(ShardProgressRequest {
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    processed_records,
                    processed_bytes,
                    offset,
                    batch_id,
                })
                .await
            }
            Command::RecordCdcCutover {
                task_id,
                alias,
                old_index,
                new_index,
                final_watermark,
                applied_watermark,
                fencing_shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
            } => {
                self.apply_record_cdc_cutover(CdcCutoverRequest {
                    task_id,
                    alias,
                    old_index,
                    new_index,
                    final_watermark,
                    applied_watermark,
                    fencing_shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                })
                .await
            }
            Command::RecordCdcPartitionedCutover {
                task_id,
                alias,
                old_index,
                new_index,
                final_watermarks,
                applied_watermarks,
                fencing_shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
            } => {
                self.apply_record_cdc_partitioned_cutover(CdcPartitionedCutoverRequest {
                    task_id,
                    alias,
                    old_index,
                    new_index,
                    final_watermarks,
                    applied_watermarks,
                    fencing_shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                })
                .await
            }
            Command::CompleteShard {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                total_records,
            } => {
                self.apply_complete(
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    total_records,
                )
                .await
            }
            Command::FailShard {
                shard_id,
                node_id,
                fencing_token,
                issued_at_ms,
                error_message,
            } => {
                self.apply_fail(
                    shard_id,
                    node_id,
                    fencing_token,
                    issued_at_ms,
                    error_message,
                )
                .await
            }
            Command::RegisterNode {
                node_id,
                address,
                tags,
                issued_at_ms,
            } => {
                self.apply_register_node(node_id, address, tags, issued_at_ms)
                    .await
            }
            Command::UpdateNodeHeartbeat {
                node_id,
                issued_at_ms,
            } => self.apply_node_heartbeat(node_id, issued_at_ms).await,
            Command::MarkNodeOffline {
                node_id,
                issued_at_ms,
                heartbeat_timeout_ms,
            } => {
                self.apply_mark_node_offline(node_id, issued_at_ms, heartbeat_timeout_ms)
                    .await
            }
            Command::ControlTask {
                task_id,
                action,
                request_id,
                actor,
            } => {
                self.apply_control_task(task_id, action, request_id, actor)
                    .await
            }
            Command::RetryTask {
                task_id,
                request_id,
                actor,
            } => self.apply_retry_task(task_id, request_id, actor).await,
            Command::ControlNodeExecution {
                node_id,
                paused,
                request_id,
                actor,
            } => {
                self.apply_control_node(node_id, paused, request_id, actor)
                    .await
            }
            Command::UpsertFederatedCluster {
                cluster,
                request_id,
                actor,
            } => {
                self.apply_federated_cluster(cluster, request_id, actor)
                    .await
            }
            Command::CreateFederatedMigration {
                migration,
                request_id,
                actor,
            } => {
                self.apply_federated_migration(*migration, request_id, actor)
                    .await
            }
            Command::HeartbeatFederatedCluster {
                cluster_id,
                identity_fingerprint,
                status,
                last_seen_at_ms,
                request_id,
                actor,
            } => {
                self.apply_federated_heartbeat(
                    cluster_id,
                    identity_fingerprint,
                    status,
                    last_seen_at_ms,
                    request_id,
                    actor,
                )
                .await
            }
            Command::StageFederatedCertificateRotation {
                cluster_id,
                current_fingerprint,
                new_fingerprint,
                at_ms,
                grace_period_ms,
                request_id,
                actor,
            } => {
                self.apply_stage_federated_certificate_rotation(
                    cluster_id,
                    current_fingerprint,
                    new_fingerprint,
                    at_ms,
                    grace_period_ms,
                    request_id,
                    actor,
                )
                .await
            }
            Command::CompleteFederatedCertificateRotation {
                cluster_id,
                new_fingerprint,
                at_ms,
                request_id,
                actor,
            } => {
                self.apply_complete_federated_certificate_rotation(
                    cluster_id,
                    new_fingerprint,
                    at_ms,
                    request_id,
                    actor,
                )
                .await
            }
            Command::AbortFederatedCertificateRotation {
                cluster_id,
                new_fingerprint,
                at_ms,
                request_id,
                actor,
            } => {
                self.apply_abort_federated_certificate_rotation(
                    cluster_id,
                    new_fingerprint,
                    at_ms,
                    request_id,
                    actor,
                )
                .await
            }
            Command::RollbackFederatedCertificateRotation {
                cluster_id,
                new_fingerprint,
                at_ms,
                request_id,
                actor,
            } => {
                self.apply_rollback_federated_certificate_rotation(
                    cluster_id,
                    new_fingerprint,
                    at_ms,
                    request_id,
                    actor,
                )
                .await
            }
            Command::StageFederatedTrustBundle {
                cluster_id,
                current_fingerprint,
                bundle_pem,
                bundle_fingerprint,
                at_ms,
                request_id,
                actor,
            } => {
                self.apply_stage_federated_trust_bundle(
                    cluster_id,
                    current_fingerprint,
                    bundle_pem,
                    bundle_fingerprint,
                    at_ms,
                    request_id,
                    actor,
                )
                .await
            }
            Command::ActivateFederatedTrustBundle {
                cluster_id,
                bundle_fingerprint,
                at_ms,
                overlap_ms,
                request_id,
                actor,
            } => {
                self.apply_activate_federated_trust_bundle(
                    cluster_id,
                    bundle_fingerprint,
                    at_ms,
                    overlap_ms,
                    request_id,
                    actor,
                )
                .await
            }
            Command::RetireFederatedTrustBundle {
                cluster_id,
                at_ms,
                request_id,
                actor,
            } => {
                self.apply_retire_federated_trust_bundle(cluster_id, at_ms, request_id, actor)
                    .await
            }
            Command::AbortFederatedTrustBundle {
                cluster_id,
                bundle_fingerprint,
                at_ms,
                request_id,
                actor,
            } => {
                self.apply_abort_federated_trust_bundle(
                    cluster_id,
                    bundle_fingerprint,
                    at_ms,
                    request_id,
                    actor,
                )
                .await
            }
            Command::CreateFederatedTrustRotation {
                rotation,
                at_ms,
                request_id,
                actor,
            } => {
                self.apply_create_federated_trust_rotation(*rotation, at_ms, request_id, actor)
                    .await
            }
            Command::AdvanceFederatedTrustRotation {
                rotation_id,
                expected_revision,
                event,
                at_ms,
                request_id,
                actor,
            } => {
                self.apply_advance_federated_trust_rotation(
                    rotation_id,
                    expected_revision,
                    event,
                    at_ms,
                    request_id,
                    actor,
                )
                .await
            }
            Command::TransitionFederatedMigration {
                migration_id,
                next_state,
                expected_generation,
                at_ms,
                error,
                request_id,
                actor,
            } => {
                self.apply_federated_transition(
                    migration_id,
                    next_state,
                    expected_generation,
                    at_ms,
                    error,
                    request_id,
                    actor,
                )
                .await
            }
            Command::UpdateFederatedMigrationProgress {
                migration_id,
                expected_generation,
                remote_task_id,
                remote_task_state,
                remote_checkpoint,
                at_ms,
                request_id,
                actor,
            } => {
                self.apply_federated_progress(
                    migration_id,
                    expected_generation,
                    remote_task_id,
                    remote_task_state,
                    remote_checkpoint,
                    at_ms,
                    request_id,
                    actor,
                )
                .await
            }
            Command::RecordFederatedRemoteOperation {
                operation,
                request_id,
                actor,
            } => {
                self.apply_federated_remote_operation(*operation, request_id, actor)
                    .await
            }
            Command::UpdateTaskState { .. }
            | Command::DeleteTask { .. }
            | Command::UpdateConfig { .. } => Ok(rejected("Command is not implemented")),
        }
    }

    async fn apply_federated_cluster(
        &self,
        mut cluster: crate::federation::FederatedCluster,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        use crate::federation::validate_cluster;
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        if let Err(message) = validate_cluster(&cluster) {
            return Ok(rejected(&message));
        }
        let payload = format!(
            "federated-cluster:{}",
            serde_json::json!({"cluster_id": cluster.cluster_id, "endpoint": cluster.endpoint, "identity_fingerprint": cluster.identity_fingerprint, "version": cluster.version, "capabilities": cluster.capabilities})
        );
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "Federated cluster registration already applied",
                    serde_json::json!({"cluster_id": cluster.cluster_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        if let Some(existing) = self
            .task_store
            .get_federated_cluster(&cluster.cluster_id)
            .await?
        {
            if existing.identity_fingerprint != cluster.identity_fingerprint {
                return Ok(rejected(
                    "cluster identity fingerprint is immutable; use the certificate rotation protocol",
                ));
            }
            cluster.previous_identity_fingerprint = existing.previous_identity_fingerprint;
            cluster.previous_identity_expires_at_ms = existing.previous_identity_expires_at_ms;
            cluster.pending_identity_fingerprint = existing.pending_identity_fingerprint;
            cluster.rotation_started_at_ms = existing.rotation_started_at_ms;
            cluster.pending_rotation_grace_period_ms = existing.pending_rotation_grace_period_ms;
            cluster.trust_bundle_pem = existing.trust_bundle_pem;
            cluster.trust_bundle_fingerprint = existing.trust_bundle_fingerprint;
            cluster.pending_trust_bundle_pem = existing.pending_trust_bundle_pem;
            cluster.pending_trust_bundle_fingerprint = existing.pending_trust_bundle_fingerprint;
            cluster.trust_bundle_staged_at_ms = existing.trust_bundle_staged_at_ms;
            cluster.previous_trust_bundle_pem = existing.previous_trust_bundle_pem;
            cluster.previous_trust_bundle_fingerprint = existing.previous_trust_bundle_fingerprint;
            cluster.previous_trust_bundle_expires_at_ms =
                existing.previous_trust_bundle_expires_at_ms;
        }
        let key = format!("federated_cluster:{}", cluster.cluster_id);
        let data = serde_json::to_value(&cluster)?;
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_cluster.upsert",
            &cluster.cluster_id,
            "registered",
            "registered",
        );
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                key,
                serde_json::to_vec(&cluster)?,
                audit,
            )
            .await?;
        Ok(accepted("Federated cluster registered", data))
    }

    async fn apply_federated_heartbeat(
        &self,
        cluster_id: String,
        identity_fingerprint: String,
        status: crate::federation::FederatedClusterStatus,
        last_seen_at_ms: i64,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let mut cluster = match self.task_store.get_federated_cluster(&cluster_id).await? {
            Some(value) => value,
            None => return Ok(rejected("federated cluster not found")),
        };
        if !cluster.accepts_fingerprint(&identity_fingerprint, last_seen_at_ms) {
            return Ok(rejected("federated cluster identity fingerprint mismatch"));
        }
        let payload = format!("federated-heartbeat:{cluster_id}:{identity_fingerprint}:{status:?}");
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "Federated cluster heartbeat already applied",
                    serde_json::json!({"cluster_id": cluster_id, "status": cluster.status}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        if last_seen_at_ms < cluster.last_seen_at_ms {
            return Ok(rejected("stale federated cluster heartbeat"));
        }
        let from = format!("{:?}", cluster.status).to_lowercase();
        cluster.status = status;
        cluster.last_seen_at_ms = last_seen_at_ms;
        cluster.updated_at_ms = last_seen_at_ms;
        let to = format!("{:?}", cluster.status).to_lowercase();
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_cluster.heartbeat",
            &cluster_id,
            &from,
            &to,
        );
        let data = serde_json::to_value(&cluster)?;
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_cluster:{cluster_id}"),
                serde_json::to_vec(&cluster)?,
                audit,
            )
            .await?;
        Ok(accepted("Federated cluster heartbeat accepted", data))
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_stage_federated_certificate_rotation(
        &self,
        cluster_id: String,
        current_fingerprint: String,
        new_fingerprint: String,
        at_ms: i64,
        grace_period_ms: i64,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let valid = |value: &str| value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit());
        if !(60_000..=604_800_000).contains(&grace_period_ms) {
            return Ok(rejected(
                "certificate overlap grace period must be between 1 minute and 7 days",
            ));
        }
        if !valid(&new_fingerprint) {
            return Ok(rejected(
                "new fingerprint must be a 64-character SHA-256 hex digest",
            ));
        }
        let mut cluster = match self.task_store.get_federated_cluster(&cluster_id).await? {
            Some(value) => value,
            None => return Ok(rejected("federated cluster not found")),
        };
        if !cluster
            .identity_fingerprint
            .eq_ignore_ascii_case(&current_fingerprint)
        {
            return Ok(rejected(
                "rotation must be staged by the currently trusted certificate",
            ));
        }
        if cluster
            .identity_fingerprint
            .eq_ignore_ascii_case(&new_fingerprint)
        {
            return Ok(rejected("new fingerprint is already the active identity"));
        }
        if cluster
            .previous_identity_expires_at_ms
            .is_some_and(|expires| at_ms < expires)
        {
            return Ok(rejected(
                "previous certificate trust window is still active",
            ));
        }
        if cluster
            .pending_identity_fingerprint
            .as_deref()
            .is_some_and(|pending| !pending.eq_ignore_ascii_case(&new_fingerprint))
            && cluster
                .rotation_started_at_ms
                .is_some_and(|started| at_ms.saturating_sub(started) < 3_600_000)
        {
            return Ok(rejected("another certificate rotation is already pending"));
        }
        let payload = format!(
            "federated-cert-stage:{cluster_id}:{current_fingerprint}:{new_fingerprint}:{grace_period_ms}"
        );
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "Certificate rotation already staged",
                    serde_json::json!({"cluster_id": cluster_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        for rotation in self.task_store.list_federated_trust_rotations().await? {
            if rotation.peer_cluster_id == cluster_id
                && !matches!(
                    rotation.phase,
                    crate::federation::FederatedTrustRotationPhase::Completed
                        | crate::federation::FederatedTrustRotationPhase::RolledBack
                )
                && (!rotation
                    .peer_leaf_fingerprint
                    .eq_ignore_ascii_case(&new_fingerprint)
                    || rotation.phase
                        == crate::federation::FederatedTrustRotationPhase::RollingBack)
            {
                return Ok(rejected(
                    "leaf staging conflicts with the active bilateral trust rotation",
                ));
            }
        }
        cluster.pending_identity_fingerprint = Some(new_fingerprint.clone());
        cluster.rotation_started_at_ms = Some(at_ms);
        cluster.pending_rotation_grace_period_ms = Some(grace_period_ms);
        cluster.updated_at_ms = at_ms;
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_cluster.certificate_rotation_staged",
            &cluster_id,
            "active",
            "pending",
        );
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_cluster:{cluster_id}"),
                serde_json::to_vec(&cluster)?,
                audit,
            )
            .await?;
        Ok(accepted(
            "Certificate rotation staged; new certificate proof required",
            serde_json::json!({"cluster_id": cluster_id, "pending_fingerprint": new_fingerprint}),
        ))
    }

    async fn apply_complete_federated_certificate_rotation(
        &self,
        cluster_id: String,
        new_fingerprint: String,
        at_ms: i64,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let mut cluster = match self.task_store.get_federated_cluster(&cluster_id).await? {
            Some(value) => value,
            None => return Ok(rejected("federated cluster not found")),
        };
        let payload = format!("federated-cert-complete:{cluster_id}:{new_fingerprint}");
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "Certificate rotation already completed",
                    serde_json::json!({"cluster_id": cluster_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        if !cluster
            .pending_identity_fingerprint
            .as_deref()
            .is_some_and(|pending| pending.eq_ignore_ascii_case(&new_fingerprint))
        {
            return Ok(rejected(
                "new certificate has not been staged for this cluster",
            ));
        }
        for rotation in self.task_store.list_federated_trust_rotations().await? {
            if rotation.peer_cluster_id == cluster_id
                && rotation
                    .peer_leaf_fingerprint
                    .eq_ignore_ascii_case(&new_fingerprint)
                && !matches!(
                    rotation.phase,
                    crate::federation::FederatedTrustRotationPhase::AwaitingDeployment
                        | crate::federation::FederatedTrustRotationPhase::ActivationBarrier
                        | crate::federation::FederatedTrustRotationPhase::Activating
                        | crate::federation::FederatedTrustRotationPhase::RecoveryRequired
                        | crate::federation::FederatedTrustRotationPhase::Overlap
                        | crate::federation::FederatedTrustRotationPhase::Retiring
                        | crate::federation::FederatedTrustRotationPhase::Completed
                )
            {
                return Ok(rejected(
                    "bilateral leaf proof is not valid in the current trust rotation phase",
                ));
            }
        }
        let grace_period_ms = cluster.pending_rotation_grace_period_ms.unwrap_or_default();
        if !(60_000..=604_800_000).contains(&grace_period_ms) {
            return Ok(rejected(
                "staged certificate rotation has no valid overlap grace period",
            ));
        }
        if cluster
            .rotation_started_at_ms
            .is_none_or(|started| at_ms.saturating_sub(started) > 3_600_000)
        {
            return Ok(rejected("staged certificate rotation has expired"));
        }
        let old_fingerprint =
            std::mem::replace(&mut cluster.identity_fingerprint, new_fingerprint.clone());
        cluster.previous_identity_fingerprint = Some(old_fingerprint);
        cluster.previous_identity_expires_at_ms = Some(at_ms.saturating_add(grace_period_ms));
        cluster.pending_identity_fingerprint = None;
        cluster.rotation_started_at_ms = None;
        cluster.pending_rotation_grace_period_ms = None;
        cluster.updated_at_ms = at_ms;
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_cluster.certificate_rotation_completed",
            &cluster_id,
            "pending",
            "active_with_overlap",
        );
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_cluster:{cluster_id}"),
                serde_json::to_vec(&cluster)?,
                audit,
            )
            .await?;
        Ok(accepted(
            "Certificate rotation completed",
            serde_json::json!({"cluster_id": cluster_id, "active_fingerprint": new_fingerprint, "previous_fingerprint_expires_at_ms": cluster.previous_identity_expires_at_ms}),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_stage_federated_trust_bundle(
        &self,
        cluster_id: String,
        current_fingerprint: String,
        bundle_pem: String,
        bundle_fingerprint: String,
        at_ms: i64,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let calculated = match crate::federation::validate_trust_bundle(&bundle_pem) {
            Ok(value) => value,
            Err(error) => return Ok(rejected(&error)),
        };
        if !calculated.eq_ignore_ascii_case(&bundle_fingerprint) {
            return Ok(rejected(
                "CA bundle fingerprint does not match its PEM certificates",
            ));
        }
        let mut cluster = match self.task_store.get_federated_cluster(&cluster_id).await? {
            Some(value) => value,
            None => return Ok(rejected("federated cluster not found")),
        };
        let payload =
            format!("federated-ca-stage:{cluster_id}:{current_fingerprint}:{bundle_fingerprint}");
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "CA bundle stage already committed",
                    serde_json::json!({"cluster_id": cluster_id, "fingerprint": bundle_fingerprint}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        for rotation in self.task_store.list_federated_trust_rotations().await? {
            if rotation.peer_cluster_id == cluster_id
                && !matches!(
                    rotation.phase,
                    crate::federation::FederatedTrustRotationPhase::Completed
                        | crate::federation::FederatedTrustRotationPhase::RolledBack
                )
                && (!rotation
                    .peer_ca_bundle_fingerprint
                    .eq_ignore_ascii_case(&bundle_fingerprint)
                    || rotation.phase
                        == crate::federation::FederatedTrustRotationPhase::RollingBack)
            {
                return Ok(rejected(
                    "CA staging conflicts with an active bilateral trust rotation",
                ));
            }
        }
        match cluster.trust_bundle_fingerprint.as_deref() {
            Some(active) if !active.eq_ignore_ascii_case(&current_fingerprint) => {
                return Ok(rejected(
                    "CA bundle must be staged by the currently active trust bundle",
                ));
            }
            None if !current_fingerprint.is_empty() => {
                return Ok(rejected(
                    "initial CA bundle bootstrap requires an empty current fingerprint",
                ));
            }
            _ => {}
        }
        if cluster
            .previous_trust_bundle_expires_at_ms
            .is_some_and(|expiry| at_ms < expiry)
        {
            return Ok(rejected("previous CA trust overlap is still active"));
        }
        if cluster
            .pending_trust_bundle_fingerprint
            .as_deref()
            .is_some_and(|pending| !pending.eq_ignore_ascii_case(&bundle_fingerprint))
        {
            return Ok(rejected("another CA bundle rotation is already staged"));
        }
        cluster.pending_trust_bundle_pem = Some(bundle_pem);
        cluster.pending_trust_bundle_fingerprint = Some(bundle_fingerprint.clone());
        cluster.trust_bundle_staged_at_ms = Some(at_ms);
        cluster.updated_at_ms = at_ms;
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_cluster.ca_bundle_staged",
            &cluster_id,
            "active",
            "staged",
        );
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_cluster:{cluster_id}"),
                serde_json::to_vec(&cluster)?,
                audit,
            )
            .await?;
        Ok(accepted(
            "CA trust bundle staged; active trust is unchanged",
            serde_json::json!({"cluster_id": cluster_id, "fingerprint": bundle_fingerprint}),
        ))
    }

    async fn apply_activate_federated_trust_bundle(
        &self,
        cluster_id: String,
        bundle_fingerprint: String,
        at_ms: i64,
        overlap_ms: i64,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        if !(60_000..=604_800_000).contains(&overlap_ms) {
            return Ok(rejected("CA overlap must be between 1 minute and 7 days"));
        }
        let mut cluster = match self.task_store.get_federated_cluster(&cluster_id).await? {
            Some(value) => value,
            None => return Ok(rejected("federated cluster not found")),
        };
        let payload =
            format!("federated-ca-activate:{cluster_id}:{bundle_fingerprint}:{overlap_ms}");
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "CA bundle activation already committed",
                    serde_json::json!({"cluster_id": cluster_id, "fingerprint": bundle_fingerprint, "previous_expires_at_ms": cluster.previous_trust_bundle_expires_at_ms}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        for rotation in self.task_store.list_federated_trust_rotations().await? {
            if rotation.peer_cluster_id == cluster_id
                && rotation
                    .peer_ca_bundle_fingerprint
                    .eq_ignore_ascii_case(&bundle_fingerprint)
                && !matches!(
                    rotation.phase,
                    crate::federation::FederatedTrustRotationPhase::Completed
                        | crate::federation::FederatedTrustRotationPhase::RolledBack
                )
                && (!rotation.activation_barrier_committed
                    || !matches!(
                        rotation.phase,
                        crate::federation::FederatedTrustRotationPhase::Activating
                            | crate::federation::FederatedTrustRotationPhase::RecoveryRequired
                            | crate::federation::FederatedTrustRotationPhase::Overlap
                            | crate::federation::FederatedTrustRotationPhase::Retiring
                    ))
            {
                return Ok(rejected(
                    "bilateral CA activation requires the committed activation barrier and must not race with rollback",
                ));
            }
        }
        if !cluster
            .pending_trust_bundle_fingerprint
            .as_deref()
            .is_some_and(|pending| pending.eq_ignore_ascii_case(&bundle_fingerprint))
            || cluster.pending_trust_bundle_pem.is_none()
        {
            return Ok(rejected("CA bundle has not been staged"));
        }
        if cluster
            .previous_trust_bundle_expires_at_ms
            .is_some_and(|expiry| at_ms < expiry)
        {
            return Ok(rejected("previous CA trust overlap is still active"));
        }
        cluster.previous_trust_bundle_pem = cluster.trust_bundle_pem.take();
        cluster.previous_trust_bundle_fingerprint = cluster.trust_bundle_fingerprint.take();
        cluster.previous_trust_bundle_expires_at_ms = if cluster.previous_trust_bundle_pem.is_some()
        {
            Some(at_ms.saturating_add(overlap_ms))
        } else {
            None
        };
        cluster.trust_bundle_pem = cluster.pending_trust_bundle_pem.take();
        cluster.trust_bundle_fingerprint = cluster.pending_trust_bundle_fingerprint.take();
        cluster.trust_bundle_staged_at_ms = None;
        cluster.updated_at_ms = at_ms;
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_cluster.ca_bundle_activated",
            &cluster_id,
            "staged",
            "active_with_overlap",
        );
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_cluster:{cluster_id}"),
                serde_json::to_vec(&cluster)?,
                audit,
            )
            .await?;
        Ok(accepted(
            "CA bundle activated with a committed overlap deadline",
            serde_json::json!({"cluster_id": cluster_id, "active_fingerprint": cluster.trust_bundle_fingerprint, "previous_expires_at_ms": cluster.previous_trust_bundle_expires_at_ms}),
        ))
    }

    async fn apply_retire_federated_trust_bundle(
        &self,
        cluster_id: String,
        at_ms: i64,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let mut cluster = match self.task_store.get_federated_cluster(&cluster_id).await? {
            Some(value) => value,
            None => return Ok(rejected("federated cluster not found")),
        };
        let payload = format!("federated-ca-retire:{cluster_id}");
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "previous CA bundle retirement already committed",
                    serde_json::json!({"cluster_id": cluster_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        for rotation in self.task_store.list_federated_trust_rotations().await? {
            if rotation.peer_cluster_id == cluster_id
                && !matches!(
                    rotation.phase,
                    crate::federation::FederatedTrustRotationPhase::Completed
                        | crate::federation::FederatedTrustRotationPhase::RolledBack
                )
            {
                if !cluster
                    .trust_bundle_fingerprint
                    .as_deref()
                    .is_some_and(|fingerprint| {
                        fingerprint.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
                    })
                {
                    return Ok(rejected(
                        "active peer CA does not match the bilateral rotation candidate",
                    ));
                }
                if !matches!(
                    rotation.phase,
                    crate::federation::FederatedTrustRotationPhase::Overlap
                        | crate::federation::FederatedTrustRotationPhase::Retiring
                ) {
                    return Ok(rejected(
                        "bilateral trust rotation is not ready for CA retirement",
                    ));
                }
                let previous_pin_matches = match (
                    cluster.previous_trust_bundle_fingerprint.as_deref(),
                    rotation.current_peer_ca_fingerprint.as_deref(),
                ) {
                    (Some(previous), Some(expected)) => previous.eq_ignore_ascii_case(expected),
                    (None, None) => true,
                    _ => false,
                };
                if !previous_pin_matches {
                    return Ok(rejected(
                        "previous CA pin does not match the bilateral rotation's recorded starting trust",
                    ));
                }
                let bilateral_deadline = rotation
                    .local_activation_expires_at_ms
                    .zip(rotation.peer_activation_expires_at_ms)
                    .map(|(local, peer)| local.max(peer));
                if bilateral_deadline.is_none_or(|deadline| at_ms < deadline) {
                    return Ok(rejected(
                        "both federation CA overlap deadlines must expire before retirement",
                    ));
                }
            }
        }
        let expiry = match cluster.previous_trust_bundle_expires_at_ms {
            Some(value) => value,
            None => return Ok(rejected("there is no previous CA bundle to retire")),
        };
        if at_ms < expiry {
            return Ok(rejected("previous CA bundle overlap has not expired"));
        }
        cluster.previous_trust_bundle_pem = None;
        cluster.previous_trust_bundle_fingerprint = None;
        cluster.previous_trust_bundle_expires_at_ms = None;
        cluster.updated_at_ms = at_ms;
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_cluster.ca_bundle_retired",
            &cluster_id,
            "overlap",
            "active_only",
        );
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_cluster:{cluster_id}"),
                serde_json::to_vec(&cluster)?,
                audit,
            )
            .await?;
        Ok(accepted(
            "previous CA bundle retired",
            serde_json::json!({"cluster_id": cluster_id, "active_fingerprint": cluster.trust_bundle_fingerprint}),
        ))
    }

    async fn apply_abort_federated_certificate_rotation(
        &self,
        cluster_id: String,
        new_fingerprint: String,
        at_ms: i64,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let mut cluster = match self.task_store.get_federated_cluster(&cluster_id).await? {
            Some(value) => value,
            None => return Ok(rejected("federated cluster not found")),
        };
        let payload = format!("federated-leaf-abort:{cluster_id}:{new_fingerprint}");
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "staged leaf identity abort already committed",
                    serde_json::json!({"cluster_id": cluster_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        if !cluster
            .pending_identity_fingerprint
            .as_deref()
            .is_some_and(|pending| pending.eq_ignore_ascii_case(&new_fingerprint))
        {
            return Ok(rejected("requested leaf identity is not pending"));
        }
        for rotation in self.task_store.list_federated_trust_rotations().await? {
            if rotation.peer_cluster_id == cluster_id
                && rotation
                    .peer_leaf_fingerprint
                    .eq_ignore_ascii_case(&new_fingerprint)
                && (rotation.phase != crate::federation::FederatedTrustRotationPhase::RollingBack
                    || !rotation.rollback_peer_confirmed
                    || rotation.local_activation_expires_at_ms.is_some()
                    || rotation.peer_activation_expires_at_ms.is_some())
            {
                return Ok(rejected(
                    "bilateral leaf abort requires a committed pre-activation RollingBack state",
                ));
            }
        }
        cluster.pending_identity_fingerprint = None;
        cluster.rotation_started_at_ms = None;
        cluster.pending_rotation_grace_period_ms = None;
        cluster.updated_at_ms = at_ms;
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_cluster.leaf_rotation_aborted",
            &cluster_id,
            "pending",
            "active_unchanged",
        );
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_cluster:{cluster_id}"),
                serde_json::to_vec(&cluster)?,
                audit,
            )
            .await?;
        Ok(accepted(
            "staged leaf identity aborted",
            serde_json::json!({"cluster_id": cluster_id, "identity_fingerprint": cluster.identity_fingerprint}),
        ))
    }

    async fn apply_rollback_federated_certificate_rotation(
        &self,
        cluster_id: String,
        new_fingerprint: String,
        at_ms: i64,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let mut cluster = match self.task_store.get_federated_cluster(&cluster_id).await? {
            Some(value) => value,
            None => return Ok(rejected("federated cluster not found")),
        };
        let payload = format!("federated-leaf-rollback:{cluster_id}:{new_fingerprint}");
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "leaf identity rollback already committed",
                    serde_json::json!({"cluster_id": cluster_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        if !cluster
            .identity_fingerprint
            .eq_ignore_ascii_case(&new_fingerprint)
        {
            return Ok(rejected(
                "active leaf identity does not match the requested rollback target",
            ));
        }
        let Some(previous) = cluster.previous_identity_fingerprint.clone() else {
            return Ok(rejected(
                "no previous leaf identity is available for rollback",
            ));
        };
        if !cluster
            .previous_identity_expires_at_ms
            .is_some_and(|expires| at_ms < expires)
        {
            return Ok(rejected("previous leaf identity grace period has expired"));
        }
        for rotation in self.task_store.list_federated_trust_rotations().await? {
            if rotation.peer_cluster_id == cluster_id
                && rotation
                    .peer_leaf_fingerprint
                    .eq_ignore_ascii_case(&new_fingerprint)
                && (rotation.phase != crate::federation::FederatedTrustRotationPhase::RollingBack
                    || !rotation.rollback_peer_confirmed
                    || rotation.local_activation_expires_at_ms.is_some()
                    || rotation.peer_activation_expires_at_ms.is_some())
            {
                return Ok(rejected(
                    "bilateral leaf rollback requires a committed pre-activation RollingBack state",
                ));
            }
        }
        cluster.identity_fingerprint = previous;
        cluster.previous_identity_fingerprint = None;
        cluster.previous_identity_expires_at_ms = None;
        cluster.pending_identity_fingerprint = None;
        cluster.rotation_started_at_ms = None;
        cluster.pending_rotation_grace_period_ms = None;
        cluster.updated_at_ms = at_ms;
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_cluster.leaf_rotation_rolled_back",
            &cluster_id,
            "new_identity",
            "previous_identity",
        );
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_cluster:{cluster_id}"),
                serde_json::to_vec(&cluster)?,
                audit,
            )
            .await?;
        Ok(accepted(
            "previous leaf identity restored",
            serde_json::json!({"cluster_id": cluster_id, "identity_fingerprint": cluster.identity_fingerprint}),
        ))
    }

    async fn apply_abort_federated_trust_bundle(
        &self,
        cluster_id: String,
        bundle_fingerprint: String,
        at_ms: i64,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let mut cluster = match self.task_store.get_federated_cluster(&cluster_id).await? {
            Some(value) => value,
            None => return Ok(rejected("federated cluster not found")),
        };
        let payload = format!("federated-ca-abort:{cluster_id}:{bundle_fingerprint}");
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "staged CA bundle abort already committed",
                    serde_json::json!({"cluster_id": cluster_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        if cluster
            .trust_bundle_fingerprint
            .as_deref()
            .is_some_and(|active| active.eq_ignore_ascii_case(&bundle_fingerprint))
            || cluster
                .previous_trust_bundle_fingerprint
                .as_deref()
                .is_some_and(|previous| previous.eq_ignore_ascii_case(&bundle_fingerprint))
        {
            return Ok(rejected(
                "an activated CA bundle cannot be rolled back; forward recovery is required",
            ));
        }
        if !cluster
            .pending_trust_bundle_fingerprint
            .as_deref()
            .is_some_and(|pending| pending.eq_ignore_ascii_case(&bundle_fingerprint))
        {
            return Ok(rejected("the requested CA bundle is not staged"));
        }
        for rotation in self.task_store.list_federated_trust_rotations().await? {
            if rotation.peer_cluster_id == cluster_id
                && rotation
                    .peer_ca_bundle_fingerprint
                    .eq_ignore_ascii_case(&bundle_fingerprint)
                && (rotation.phase != crate::federation::FederatedTrustRotationPhase::RollingBack
                    || !rotation.rollback_peer_confirmed
                    || rotation.local_activation_expires_at_ms.is_some()
                    || rotation.peer_activation_expires_at_ms.is_some())
            {
                return Ok(rejected(
                    "bilateral CA abort requires a committed pre-activation RollingBack state",
                ));
            }
        }
        cluster.pending_trust_bundle_pem = None;
        cluster.pending_trust_bundle_fingerprint = None;
        cluster.trust_bundle_staged_at_ms = None;
        cluster.updated_at_ms = at_ms;
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_cluster.ca_bundle_aborted",
            &cluster_id,
            "staged",
            "active_unchanged",
        );
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_cluster:{cluster_id}"),
                serde_json::to_vec(&cluster)?,
                audit,
            )
            .await?;
        Ok(accepted(
            "staged CA bundle aborted without changing active trust",
            serde_json::json!({"cluster_id": cluster_id, "active_fingerprint": cluster.trust_bundle_fingerprint}),
        ))
    }

    async fn apply_create_federated_trust_rotation(
        &self,
        mut rotation: crate::federation::FederatedTrustRotation,
        at_ms: i64,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty()
            || request_id.len() > 128
            || rotation.rotation_id.trim().is_empty()
            || rotation.rotation_id.len() > 96
            || rotation.peer_cluster_id.trim().is_empty()
            || rotation.peer_cluster_id.len() > 128
            || !rotation
                .rotation_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
            || rotation.overlap_ms < 60_000
            || rotation.overlap_ms > 604_800_000
        {
            return Ok(rejected(
                "rotation_id, peer_cluster_id, request_id and a 1-minute to 7-day overlap are required",
            ));
        }
        let local_ca = match crate::federation::validate_trust_bundle(&rotation.local_ca_bundle_pem)
        {
            Ok(value) => value,
            Err(error) => return Ok(rejected(&format!("invalid local CA bundle: {error}"))),
        };
        let peer_ca = match crate::federation::validate_trust_bundle(&rotation.peer_ca_bundle_pem) {
            Ok(value) => value,
            Err(error) => return Ok(rejected(&format!("invalid peer CA bundle: {error}"))),
        };
        if !local_ca.eq_ignore_ascii_case(&rotation.local_ca_bundle_fingerprint)
            || !peer_ca.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
        {
            return Ok(rejected(
                "trust rotation CA fingerprints do not match their PEM bundles",
            ));
        }
        if rotation
            .current_peer_ca_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| {
                fingerprint.len() != 64 || !fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        {
            return Ok(rejected(
                "current peer CA fingerprint must be a 64-character SHA-256 hex digest",
            ));
        }
        for fingerprint in [
            &rotation.local_leaf_fingerprint,
            &rotation.peer_leaf_fingerprint,
        ] {
            if fingerprint.len() != 64 || !fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Ok(rejected(
                    "local and peer leaf fingerprints must be 64-character SHA-256 hex digests",
                ));
            }
        }
        if rotation.phase != crate::federation::FederatedTrustRotationPhase::Staging
            || rotation.revision != 0
            || rotation.local_staged
            || rotation.peer_staged
            || rotation.local_deployment_ready
            || rotation.peer_deployment_ready
            || rotation.activation_barrier_committed
            || rotation.local_activation_expires_at_ms.is_some()
            || rotation.peer_activation_expires_at_ms.is_some()
            || rotation.local_retired
            || rotation.peer_retired
        {
            return Ok(rejected(
                "new trust rotations must start in the pristine staging state",
            ));
        }
        let payload = format!(
            "federated-trust-rotation-create:{}:{}:{}:{}:{}:{}:{}:{}",
            rotation.rotation_id,
            rotation.peer_cluster_id,
            rotation
                .current_peer_ca_fingerprint
                .as_deref()
                .unwrap_or("<bootstrap>"),
            rotation.local_ca_bundle_fingerprint,
            rotation.peer_ca_bundle_fingerprint,
            rotation.local_leaf_fingerprint,
            rotation.peer_leaf_fingerprint,
            rotation.overlap_ms
        );
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "Federated trust rotation already created",
                    serde_json::json!({"rotation_id": rotation.rotation_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        let peer = match self
            .task_store
            .get_federated_cluster(&rotation.peer_cluster_id)
            .await?
        {
            Some(peer) if peer.status != crate::federation::FederatedClusterStatus::Offline => peer,
            _ => {
                return Ok(rejected(
                    "trust rotation peer is not registered or is offline",
                ));
            }
        };
        let current_pin_matches = match (
            peer.trust_bundle_fingerprint.as_deref(),
            rotation.current_peer_ca_fingerprint.as_deref(),
        ) {
            (None, None) => true,
            (Some(current), Some(expected)) => current.eq_ignore_ascii_case(expected),
            _ => false,
        };
        if !current_pin_matches {
            return Ok(rejected(
                "peer CA pin changed since the trust rotation was planned",
            ));
        }
        if peer
            .trust_bundle_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| {
                fingerprint.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
            })
        {
            return Ok(rejected("candidate peer CA bundle is already active"));
        }
        if peer.pending_identity_fingerprint.is_some()
            || peer.pending_trust_bundle_fingerprint.is_some()
        {
            return Ok(rejected(
                "peer already has staged certificate or CA material outside this rotation",
            ));
        }
        if self
            .task_store
            .get_federated_trust_rotation(&rotation.rotation_id)
            .await?
            .is_some()
        {
            return Ok(rejected("trust rotation ID already exists"));
        }
        for existing in self.task_store.list_federated_trust_rotations().await? {
            if existing.peer_cluster_id == rotation.peer_cluster_id
                && !matches!(
                    existing.phase,
                    crate::federation::FederatedTrustRotationPhase::Completed
                        | crate::federation::FederatedTrustRotationPhase::RolledBack
                )
            {
                return Ok(rejected(
                    "another trust rotation for this peer is still active",
                ));
            }
        }
        rotation.created_at_ms = at_ms;
        rotation.updated_at_ms = at_ms;
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federation.trust_rotation.create",
            &rotation.rotation_id,
            "none",
            "staging",
        );
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_trust_rotation:{}", rotation.rotation_id),
                serde_json::to_vec(&rotation)?,
                audit,
            )
            .await?;
        Ok(accepted(
            "Federated trust rotation created",
            serde_json::to_value(rotation)?,
        ))
    }

    async fn apply_advance_federated_trust_rotation(
        &self,
        rotation_id: String,
        expected_revision: u64,
        event: crate::federation::FederatedTrustRotationEvent,
        at_ms: i64,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        // Request identity is the idempotency key. Revision is an optimistic
        // concurrency fence for the first application, not part of event identity;
        // a lost ACK may be replayed after later events have advanced the revision.
        let payload = format!("federated-trust-rotation-event:{rotation_id}:{event:?}");
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                let current = self
                    .task_store
                    .get_federated_trust_rotation(&rotation_id)
                    .await?;
                Ok(accepted(
                    "Federated trust rotation event already committed",
                    serde_json::to_value(current)?,
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        let mut rotation = match self
            .task_store
            .get_federated_trust_rotation(&rotation_id)
            .await?
        {
            Some(rotation) => rotation,
            None => return Ok(rejected("federated trust rotation not found")),
        };
        if matches!(
            event,
            crate::federation::FederatedTrustRotationEvent::BeginRollback { .. }
        ) {
            let peer = match self
                .task_store
                .get_federated_cluster(&rotation.peer_cluster_id)
                .await?
            {
                Some(peer) => peer,
                None => return Ok(rejected("federated peer disappeared before rollback")),
            };
            let candidate_ca_is_active =
                peer.trust_bundle_fingerprint
                    .as_deref()
                    .is_some_and(|fingerprint| {
                        fingerprint.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
                    });
            let candidate_ca_is_previous = peer
                .previous_trust_bundle_fingerprint
                .as_deref()
                .is_some_and(|fingerprint| {
                    fingerprint.eq_ignore_ascii_case(&rotation.peer_ca_bundle_fingerprint)
                });
            if candidate_ca_is_active || candidate_ca_is_previous {
                return Ok(rejected(
                    "candidate CA activation is already committed in the peer registry; rollback is forbidden",
                ));
            }
        }
        let previous = format!("{:?}", rotation.phase);
        if let Err(error) = rotation.apply_event(event.clone(), expected_revision, at_ms) {
            return Ok(rejected(&error));
        }
        let next = format!("{:?}", rotation.phase);
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federation.trust_rotation.advance",
            &rotation_id,
            &previous,
            &next,
        );
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_trust_rotation:{rotation_id}"),
                serde_json::to_vec(&rotation)?,
                audit,
            )
            .await?;
        Ok(accepted(
            "Federated trust rotation advanced",
            serde_json::to_value(rotation)?,
        ))
    }

    async fn apply_federated_migration(
        &self,
        migration: crate::federation::FederatedMigration,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty()
            || request_id.len() > 128
            || migration.migration_id.trim().is_empty()
            || migration.migration_id.len() > 96
            || !migration
                .migration_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
            || migration.task_id.trim().is_empty()
        {
            return Ok(rejected(
                "migration_id, task_id and request_id are required",
            ));
        }
        let payload = format!(
            "federated-migration:{}",
            serde_json::json!({
                "migration_id": migration.migration_id, "source_cluster": migration.source_cluster,
                "target_cluster": migration.target_cluster, "task_id": migration.task_id,
                "required_capabilities": migration.required_capabilities, "task_name": migration.task_name,
                "source_config": migration.source_config, "sink_config": migration.sink_config,
                "transform_config": migration.transform_config, "sharding_strategy": migration.sharding_strategy,
                "priority": migration.priority, "tags": migration.tags,
                "deadline_at_ms": migration.deadline_at_ms, "execution_budget_ms": migration.execution_budget_ms,
                "retry_budget": migration.retry_budget, "timeout_policy": migration.timeout_policy
            })
        );
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "Federated migration already created",
                    serde_json::json!({"migration_id": migration.migration_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        if !(0..=3).contains(&migration.priority)
            || migration.task_name.trim().is_empty()
            || migration.source_config.len() > 1_048_576
            || migration.sink_config.len() > 1_048_576
            || !serde_json::from_str::<serde_json::Value>(&migration.source_config)
                .is_ok_and(|value| value.is_object())
            || !serde_json::from_str::<serde_json::Value>(&migration.sink_config)
                .is_ok_and(|value| value.is_object())
        {
            return Ok(rejected(
                "migration requires a task name, priority 0-3, and source/sink JSON objects no larger than 1 MiB",
            ));
        }
        if migration.source_cluster == migration.target_cluster {
            return Ok(rejected("source and target clusters must differ"));
        }
        for cluster_id in [&migration.source_cluster, &migration.target_cluster] {
            match self.task_store.get_federated_cluster(cluster_id).await? {
                Some(cluster)
                    if cluster.status != crate::federation::FederatedClusterStatus::Offline => {}
                _ => {
                    return Ok(rejected(&format!(
                        "cluster '{cluster_id}' is not registered or is offline"
                    )));
                }
            }
        }
        for cluster_id in [&migration.source_cluster, &migration.target_cluster] {
            let cluster = self
                .task_store
                .get_federated_cluster(cluster_id)
                .await?
                .expect("cluster was validated above");
            if !migration
                .required_capabilities
                .is_subset(&cluster.capabilities)
            {
                return Ok(rejected(&format!(
                    "cluster '{cluster_id}' does not satisfy required migration capabilities"
                )));
            }
        }
        if migration.execution_budget_ms == Some(0)
            || migration
                .deadline_at_ms
                .is_some_and(|deadline| deadline <= migration.created_at_ms)
        {
            return Ok(rejected(
                "SLA deadline must be after creation and execution budget must be greater than zero",
            ));
        }
        if migration.state != crate::federation::MigrationState::Planned
            || migration.generation != 0
        {
            return Ok(rejected(
                "new migrations must start in planned state at generation 0",
            ));
        }
        if self
            .task_store
            .get_federated_migration(&migration.migration_id)
            .await?
            .is_some()
        {
            return Ok(rejected("migration_id already exists"));
        }
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_migration.create",
            &migration.migration_id,
            "none",
            "planned",
        );
        let data = serde_json::to_value(&migration)?;
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_migration:{}", migration.migration_id),
                serde_json::to_vec(&migration)?,
                audit,
            )
            .await?;
        Ok(accepted("Federated migration created", data))
    }

    async fn apply_federated_remote_operation(
        &self,
        operation: crate::federation::FederatedRemoteOperation,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty()
            || request_id.len() > 128
            || operation.migration_id.trim().is_empty()
            || operation.source_cluster.trim().is_empty()
            || operation.task_id.trim().is_empty()
        {
            return Ok(rejected(
                "remote operation identity and request_id are required",
            ));
        }
        let source = self
            .task_store
            .get_federated_cluster(&operation.source_cluster)
            .await?;
        if source.is_none_or(|cluster| {
            cluster.status == crate::federation::FederatedClusterStatus::Offline
        }) {
            return Ok(rejected("source cluster is not registered or is offline"));
        }
        let key = format!(
            "federated_remote_op:{}:{:020}",
            operation.migration_id, operation.generation
        );
        let payload = format!(
            "federated-remote-op:{}",
            serde_json::to_string(&serde_json::json!({
                "migration_id": operation.migration_id,
                "source_cluster": operation.source_cluster,
                "generation": operation.generation,
                "task_id": operation.task_id,
                "state": operation.state,
                "checkpoint_digest": operation.checkpoint_digest,
            }))?
        );
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "Federated remote operation already applied",
                    serde_json::to_value(operation)?,
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        let current = self
            .task_store
            .get_federated_remote_operation(&operation.migration_id, operation.generation)
            .await?;
        if let Some(current) = current {
            if current.source_cluster != operation.source_cluster
                || current.task_id != operation.task_id
            {
                return Ok(rejected(
                    "remote operation identity cannot change within a generation",
                ));
            }
            if current.checkpoint_digest != operation.checkpoint_digest {
                return Ok(rejected(
                    "remote checkpoint digest cannot change within a generation",
                ));
            }
            let allowed = matches!(
                (current.state, operation.state),
                (
                    crate::federation::FederatedRemoteOperationState::Prepared,
                    crate::federation::FederatedRemoteOperationState::Compensating
                        | crate::federation::FederatedRemoteOperationState::Committed
                ) | (
                    crate::federation::FederatedRemoteOperationState::Compensating,
                    crate::federation::FederatedRemoteOperationState::Compensated
                )
            ) || current.state == operation.state;
            if !allowed {
                return Ok(rejected("invalid remote operation state transition"));
            }
            if current.state == crate::federation::FederatedRemoteOperationState::Committed
                && operation.state != crate::federation::FederatedRemoteOperationState::Committed
            {
                return Ok(rejected("committed remote operation is immutable"));
            }
            if current.state == crate::federation::FederatedRemoteOperationState::Compensating
                && operation.state != crate::federation::FederatedRemoteOperationState::Compensating
                && operation.state != crate::federation::FederatedRemoteOperationState::Compensated
            {
                return Ok(rejected(
                    "compensating remote operation cannot be committed",
                ));
            }
            if current.state == crate::federation::FederatedRemoteOperationState::Compensated
                && operation.state != crate::federation::FederatedRemoteOperationState::Compensated
            {
                return Ok(rejected("compensated remote operation is immutable"));
            }
        } else if operation.state != crate::federation::FederatedRemoteOperationState::Prepared
            && operation.state != crate::federation::FederatedRemoteOperationState::Compensating
        {
            return Ok(rejected(
                "remote operation must be prepared or compensating before commit",
            ));
        }
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_remote_operation.transition",
            &operation.migration_id,
            "remote_operation",
            &format!("{:?}", operation.state).to_lowercase(),
        );
        let data = serde_json::to_value(&operation)?;
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                key,
                serde_json::to_vec(&operation)?,
                audit,
            )
            .await?;
        Ok(accepted("Federated remote operation committed", data))
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_federated_progress(
        &self,
        migration_id: String,
        expected_generation: u64,
        remote_task_id: Option<String>,
        remote_task_state: Option<String>,
        remote_checkpoint: Option<serde_json::Value>,
        at_ms: i64,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let mut migration = match self
            .task_store
            .get_federated_migration(&migration_id)
            .await?
        {
            Some(value) => value,
            None => return Ok(rejected("federated migration not found")),
        };
        if migration.generation != expected_generation {
            return Ok(rejected("stale federated migration generation"));
        }
        if migration.state == crate::federation::MigrationState::Completed {
            return Ok(rejected("completed migration progress cannot be changed"));
        }
        if migration
            .last_reconciled_at_ms
            .is_some_and(|previous| at_ms < previous)
        {
            return Ok(rejected("stale remote reconciliation timestamp"));
        }
        if migration.remote_task_state.as_deref() == Some("completed")
            && remote_task_state
                .as_deref()
                .is_some_and(|state| state != "completed")
        {
            return Ok(rejected(
                "completed remote task state cannot move backwards",
            ));
        }
        if migration
            .remote_task_state
            .as_deref()
            .is_some_and(|state| matches!(state, "failed" | "cancelled"))
            && remote_task_state
                .as_deref()
                .is_some_and(|state| !matches!(state, "failed" | "cancelled"))
        {
            return Ok(rejected(
                "terminal remote task state cannot move backwards within a generation",
            ));
        }
        if migration
            .remote_checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.get("federation_commit_state"))
            .and_then(serde_json::Value::as_str)
            == Some("committed")
            && remote_checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.get("federation_commit_state"))
                .and_then(serde_json::Value::as_str)
                != Some("committed")
        {
            return Ok(rejected("committed checkpoint state is immutable"));
        }
        let payload = format!(
            "federated-progress:{migration_id}:{expected_generation}:{}:{}",
            remote_task_id.as_deref().unwrap_or(""),
            serde_json::to_string(
                &serde_json::json!({"state": remote_task_state, "checkpoint": remote_checkpoint})
            )?
        );
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "Federated migration progress already applied",
                    serde_json::json!({"migration_id": migration_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        if let (Some(current), Some(incoming)) = (&migration.remote_task_id, &remote_task_id)
            && current != incoming
        {
            return Ok(rejected(
                "remote task ID cannot change within a migration generation",
            ));
        }
        if let Some(remote_task_id) = remote_task_id {
            migration.remote_task_id = Some(remote_task_id);
        }
        if remote_task_state.is_some() {
            migration.remote_task_state = remote_task_state;
        }
        if remote_checkpoint.is_some() {
            migration.remote_checkpoint = remote_checkpoint;
        }
        migration.last_reconciled_at_ms = Some(at_ms);
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_migration.reconcile",
            &migration_id,
            "running",
            "running",
        );
        let data = serde_json::to_value(&migration)?;
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_migration:{migration_id}"),
                serde_json::to_vec(&migration)?,
                audit,
            )
            .await?;
        Ok(accepted("Federated migration progress reconciled", data))
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_federated_transition(
        &self,
        migration_id: String,
        next_state: crate::federation::MigrationState,
        expected_generation: u64,
        at_ms: i64,
        error: Option<String>,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let mut migration = match self
            .task_store
            .get_federated_migration(&migration_id)
            .await?
        {
            Some(value) => value,
            None => return Ok(rejected("federated migration not found")),
        };
        let from = format!("{:?}", migration.state).to_lowercase();
        let payload = format!(
            "federated-transition:{migration_id}:{next_state:?}:{expected_generation}:{error:?}"
        );
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == payload {
                Ok(accepted(
                    "Federated migration transition already applied",
                    serde_json::json!({"migration_id": migration_id, "state": migration.state, "generation": migration.generation}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used with a different payload",
                ))
            };
        }
        if migration.remote_task_state.as_deref() == Some("completed")
            && matches!(
                next_state,
                crate::federation::MigrationState::Failed
                    | crate::federation::MigrationState::Cancelled
            )
        {
            return Ok(rejected(
                "Target data writes are complete; finish the distributed commit instead of cancelling or failing the migration",
            ));
        }
        if next_state == crate::federation::MigrationState::Committing
            && (migration.remote_task_state.as_deref() != Some("completed")
                || migration
                    .remote_checkpoint
                    .as_ref()
                    .and_then(|value| value.get("federation_commit_state"))
                    .and_then(serde_json::Value::as_str)
                    != Some("prepared"))
        {
            return Ok(rejected(
                "target task must be completed and its durable commit must be prepared",
            ));
        }
        if next_state == crate::federation::MigrationState::Completed
            && (migration.remote_task_state.as_deref() != Some("completed")
                || migration
                    .remote_checkpoint
                    .as_ref()
                    .and_then(|value| value.get("federation_commit_state"))
                    .and_then(serde_json::Value::as_str)
                    != Some("committed"))
        {
            return Ok(rejected(
                "target-side distributed commit acknowledgement is required",
            ));
        }
        if next_state == crate::federation::MigrationState::Preparing
            && migration.state == crate::federation::MigrationState::Failed
            && migration
                .retry_budget
                .is_some_and(|budget| migration.generation >= u64::from(budget))
        {
            return Ok(rejected("Federated migration retry budget is exhausted"));
        }
        if let Err(message) = migration.transition(next_state, expected_generation, at_ms, error) {
            return Ok(rejected(&message));
        }
        let to = format!("{:?}", migration.state).to_lowercase();
        let audit = self.audit_event(
            &actor,
            &request_id,
            "federated_migration.transition",
            &migration_id,
            &from,
            &to,
        );
        let data = serde_json::to_value(&migration)?;
        self.task_store
            .save_federated_control(
                &request_id,
                &payload,
                format!("federated_migration:{migration_id}"),
                serde_json::to_vec(&migration)?,
                audit,
            )
            .await?;
        Ok(accepted("Federated migration state advanced", data))
    }

    fn audit_event(
        &self,
        actor: &str,
        request_id: &str,
        action: &str,
        resource_id: &str,
        from_state: &str,
        to_state: &str,
    ) -> Option<crate::task::AuditEvent> {
        self.applied_log_index
            .map(|log_index| crate::task::AuditEvent {
                log_index,
                actor: actor.to_string(),
                request_id: request_id.to_string(),
                action: action.to_string(),
                resource_id: resource_id.to_string(),
                from_state: from_state.to_string(),
                to_state: to_state.to_string(),
                occurred_at_ms: chrono::Utc::now().timestamp_millis(),
            })
    }

    async fn apply_control_task(
        &self,
        task_id: String,
        action: String,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let request_payload = format!("task:{task_id}:{action}");
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == request_payload {
                Ok(accepted(
                    "Control request already committed",
                    serde_json::json!({"task_id": task_id, "request_id": request_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used for a different command",
                ))
            };
        }
        let id: crate::types::TaskId = match task_id.parse() {
            Ok(id) => id,
            Err(_) => return Ok(rejected("Invalid task ID")),
        };
        let Some(mut task) = self.task_store.get_task(&id).await? else {
            return Ok(rejected("Task not found"));
        };
        let previous_state = task_state_name(&task.state).to_string();
        let next = match action.as_str() {
            "pause" => match &task.state {
                crate::types::TaskState::Pending => crate::types::TaskState::Paused,
                crate::types::TaskState::Running { .. } => {
                    let active = self
                        .task_store
                        .list_task_shards(&id)
                        .await?
                        .iter()
                        .filter(|shard| {
                            matches!(shard.state, ShardState::Running { .. })
                                && shard
                                    .lease_expires_at
                                    .as_ref()
                                    .is_some_and(|expiry| *expiry > chrono::Utc::now())
                        })
                        .count();
                    if active == 0 {
                        crate::types::TaskState::Paused
                    } else {
                        crate::types::TaskState::Pausing
                    }
                }
                crate::types::TaskState::Paused => crate::types::TaskState::Paused,
                crate::types::TaskState::Pausing => crate::types::TaskState::Pausing,
                _ => return Ok(rejected("Only pending or running tasks can be paused")),
            },
            "resume" => match &task.state {
                crate::types::TaskState::Paused | crate::types::TaskState::Pausing => {
                    crate::types::TaskState::Running {
                        started_at: task.started_at.unwrap_or_else(chrono::Utc::now),
                        shard_count: task.shard_count,
                    }
                }
                crate::types::TaskState::Running { .. } => task.state.clone(),
                _ => return Ok(rejected("Only paused tasks can be resumed")),
            },
            "fail" => {
                if matches!(
                    task.state,
                    crate::types::TaskState::Completed { .. }
                        | crate::types::TaskState::Cancelled { .. }
                        | crate::types::TaskState::Failed { .. }
                ) {
                    return Ok(rejected("Terminal tasks cannot be failed again"));
                }
                task.mark_failed("SLA execution budget exceeded".into());
                task.state.clone()
            }
            "cancel" => match &task.state {
                crate::types::TaskState::Cancelled { .. } => task.state.clone(),
                crate::types::TaskState::Completed { .. }
                | crate::types::TaskState::Failed { .. } => {
                    return Ok(rejected("Terminal tasks cannot be cancelled"));
                }
                _ => crate::types::TaskState::Cancelled {
                    cancelled_at: chrono::Utc::now(),
                },
            },
            _ => return Ok(rejected("Unsupported task action")),
        };
        task.set_state(next);
        let to_state = task_state_name(&task.state).to_string();
        self.task_store
            .save_control_batch(
                &request_id,
                &request_payload,
                vec![(format!("task:{}", task.task_id), serde_json::to_vec(&task)?)],
                self.audit_event(
                    &actor,
                    &request_id,
                    &format!("task.{action}"),
                    &task_id,
                    &previous_state,
                    &to_state,
                ),
            )
            .await?;
        Ok(accepted(
            "Task control command committed",
            serde_json::json!({"task_id": task_id, "action": action, "request_id": request_id}),
        ))
    }

    async fn apply_retry_task(
        &self,
        task_id: String,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let request_payload = format!("task:{task_id}:retry");
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == request_payload {
                Ok(accepted(
                    "Retry request already committed",
                    serde_json::json!({"task_id": task_id, "request_id": request_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used for a different command",
                ))
            };
        }
        let id: crate::types::TaskId = match task_id.parse() {
            Ok(id) => id,
            Err(_) => return Ok(rejected("Invalid task ID")),
        };
        let Some(mut task) = self.task_store.get_task(&id).await? else {
            return Ok(rejected("Task not found"));
        };
        if matches!(
            &task.state,
            crate::types::TaskState::Cancelled { .. } | crate::types::TaskState::Completed { .. }
        ) {
            return Ok(rejected("Cancelled or completed tasks cannot be retried"));
        }
        let mut shards = self.task_store.list_task_shards(&id).await?;
        if let Some(limit) = task
            .metadata
            .get("sla.max_retries")
            .and_then(|value| value.parse::<u64>().ok())
        {
            let retries: u64 = shards.iter().map(|shard| shard.retry_count).sum();
            if retries >= limit {
                return Ok(rejected("Task retry budget is exhausted"));
            }
        }
        let mut retried = 0usize;
        for shard in &mut shards {
            if matches!(shard.state, ShardState::Failed { .. }) {
                shard.retry_count = shard.retry_count.saturating_add(1);
                shard.set_state_at(ShardState::Pending, chrono::Utc::now());
                retried += 1;
            }
        }
        if retried == 0 {
            self.task_store
                .save_control_batch(
                    &request_id,
                    &request_payload,
                    Vec::new(),
                    self.audit_event(
                        &actor,
                        &request_id,
                        "task.retry",
                        &task_id,
                        "failed",
                        "no_failed_shards",
                    ),
                )
                .await?;
            return Ok(accepted(
                "No failed shards to retry",
                serde_json::json!({"task_id": task_id, "retried_shards": 0}),
            ));
        }
        task.state = crate::types::TaskState::Running {
            started_at: task.started_at.unwrap_or_else(chrono::Utc::now),
            shard_count: task.shard_count,
        };
        task.refresh_from_shards(&shards, chrono::Utc::now());
        let mut records = Vec::with_capacity(shards.len() + 1);
        for shard in &shards {
            records.push((
                format!("shard:{}", shard.shard_id),
                serde_json::to_vec(shard)?,
            ));
        }
        records.push((format!("task:{}", task.task_id), serde_json::to_vec(&task)?));
        self.task_store
            .save_control_batch(
                &request_id,
                &request_payload,
                records,
                self.audit_event(
                    &actor,
                    &request_id,
                    "task.retry",
                    &task_id,
                    "failed",
                    &format!("requeued:{retried}"),
                ),
            )
            .await?;
        Ok(accepted(
            "Failed shards requeued; committed checkpoints preserved",
            serde_json::json!({"task_id": task_id, "retried_shards": retried, "request_id": request_id}),
        ))
    }

    async fn apply_control_node(
        &self,
        node_id: String,
        paused: bool,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if request_id.trim().is_empty() || request_id.len() > 128 {
            return Ok(rejected("request_id must contain 1-128 characters"));
        }
        let request_payload = format!("node:{node_id}:paused={paused}");
        if let Some(existing) = self.task_store.get_control_request(&request_id).await? {
            return if existing == request_payload {
                Ok(accepted(
                    "Node control request already committed",
                    serde_json::json!({"node_id": node_id, "paused": paused}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used for a different command",
                ))
            };
        }
        let id = NodeId::new(node_id.clone());
        let Some(mut node) = self.task_store.get_node(&id).await? else {
            return Ok(rejected("Node not found"));
        };
        if !paused && node.status == crate::types::NodeStatus::Offline {
            return Ok(rejected(
                "Offline nodes must re-register before execution can resume",
            ));
        }
        let previous_status = format!("{:?}", node.status).to_ascii_lowercase();
        node.status = if paused {
            crate::types::NodeStatus::Paused
        } else {
            crate::types::NodeStatus::Healthy
        };
        self.task_store
            .save_control_batch(
                &request_id,
                &request_payload,
                vec![(format!("node:{}", node.node_id), serde_json::to_vec(&node)?)],
                self.audit_event(
                    &actor,
                    &request_id,
                    if paused { "node.pause" } else { "node.resume" },
                    &node_id,
                    &previous_status,
                    if paused { "paused" } else { "healthy" },
                ),
            )
            .await?;
        Ok(accepted(
            if paused {
                "Node execution paused; active leases are allowed to expire or finish"
            } else {
                "Node execution resumed"
            },
            serde_json::json!({"node_id": node_id, "paused": paused, "request_id": request_id}),
        ))
    }

    async fn apply_register_node(
        &self,
        node_id: String,
        address: String,
        tags: std::collections::HashMap<String, String>,
        issued_at_ms: i64,
    ) -> Result<CommandResponse> {
        if node_id.trim().is_empty() || address.trim().is_empty() {
            return Ok(rejected("Node ID and advertised address are required"));
        }
        let existing = self
            .task_store
            .get_node(&NodeId::new(node_id.clone()))
            .await?;
        let registered_at_ms = existing
            .as_ref()
            .map(|node| node.registered_at_ms)
            .unwrap_or(issued_at_ms);
        let node = crate::types::NodeMeta {
            node_id: NodeId::new(node_id.clone()),
            address,
            tags,
            registered_at_ms,
            last_heartbeat_ms: issued_at_ms,
            status: crate::types::NodeStatus::Healthy,
        };
        self.task_store.save_node(&node).await?;
        Ok(accepted(
            "Node registered",
            serde_json::json!({"node_id": node_id, "status": "healthy"}),
        ))
    }

    async fn apply_node_heartbeat(
        &self,
        node_id: String,
        issued_at_ms: i64,
    ) -> Result<CommandResponse> {
        let id = NodeId::new(node_id.clone());
        let Some(mut node) = self.task_store.get_node(&id).await? else {
            return Ok(rejected("Node must register before sending heartbeats"));
        };
        if issued_at_ms < node.last_heartbeat_ms {
            return Ok(rejected("Heartbeat timestamp cannot move backwards"));
        }
        node.last_heartbeat_ms = issued_at_ms;
        if node.status != crate::types::NodeStatus::Paused {
            node.status = crate::types::NodeStatus::Healthy;
        }
        self.task_store.save_node(&node).await?;
        Ok(accepted(
            "Node heartbeat recorded",
            serde_json::json!({"node_id": node_id, "last_heartbeat_ms": issued_at_ms}),
        ))
    }

    async fn apply_mark_node_offline(
        &self,
        node_id: String,
        issued_at_ms: i64,
        heartbeat_timeout_ms: i64,
    ) -> Result<CommandResponse> {
        if heartbeat_timeout_ms < 1_000 {
            return Ok(rejected("Heartbeat timeout must be at least one second"));
        }
        let id = NodeId::new(node_id.clone());
        let Some(mut node) = self.task_store.get_node(&id).await? else {
            return Ok(rejected("Node not found"));
        };
        if node.status == crate::types::NodeStatus::Offline {
            return Ok(accepted(
                "Node already offline",
                serde_json::json!({"node_id": node_id, "requeued_shards": 0}),
            ));
        }
        let cutoff = issued_at_ms.saturating_sub(heartbeat_timeout_ms);
        if node.last_heartbeat_ms > cutoff {
            return Ok(rejected(
                "Node heartbeat is still within the failure-detection window",
            ));
        }
        node.status = crate::types::NodeStatus::Offline;
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid node-offline timestamp".into()))?;
        let mut requeued_shards = Vec::new();
        let mut affected_tasks = Vec::new();
        for mut task in self.task_store.list_tasks().await? {
            let mut shards = self.task_store.list_task_shards(&task.task_id).await?;
            let mut changed = false;
            for shard in &mut shards {
                let owned = matches!(&shard.state, ShardState::Running { node_id: owner, .. } if owner == &id);
                if owned {
                    shard.set_state_at(ShardState::Pending, at);
                    requeued_shards.push(shard.clone());
                    changed = true;
                }
            }
            if changed {
                task.refresh_from_shards(&shards, at);
                affected_tasks.push(task);
            }
        }
        let requeued = requeued_shards.len();
        self.task_store
            .save_node_shards_tasks(&node, &requeued_shards, &affected_tasks)
            .await?;
        Ok(accepted(
            "Node marked offline; owned shards atomically returned to pending",
            serde_json::json!({"node_id": node_id, "requeued_shards": requeued}),
        ))
    }

    async fn persist_shard_and_aggregate(&self, shard: &ShardMeta) -> Result<()> {
        self.persist_shard_and_aggregate_with_manifest(shard, None)
            .await
    }

    async fn persist_shard_and_aggregate_with_manifest(
        &self,
        shard: &ShardMeta,
        batch_manifest: Option<(Vec<u8>, crate::pipeline::cache::BatchManifest)>,
    ) -> Result<()> {
        let mut task = self
            .task_store
            .get_task(&shard.task_id)
            .await?
            .ok_or_else(|| {
                DatspoolError::Internal(format!(
                    "Task {} missing for shard {}",
                    shard.task_id, shard.shard_id
                ))
            })?;
        let previous_state = task_state_name(&task.state).to_string();
        let mut shards = self.task_store.list_task_shards(&shard.task_id).await?;
        if let Some(existing) = shards
            .iter_mut()
            .find(|existing| existing.shard_id == shard.shard_id)
        {
            *existing = shard.clone();
        } else {
            shards.push(shard.clone());
        }
        task.refresh_from_shards(
            &shards,
            chrono::DateTime::from_timestamp_millis(
                shard
                    .updated_at
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(i64::MAX as u128) as i64,
            )
            .unwrap_or_else(chrono::Utc::now),
        );
        if matches!(task.state, crate::types::TaskState::Pausing)
            && !shards
                .iter()
                .any(|candidate| matches!(candidate.state, ShardState::Running { .. }))
        {
            task.set_state(crate::types::TaskState::Paused);
        }
        let next_state = task_state_name(&task.state).to_string();
        let audit = if previous_state != next_state {
            self.applied_log_index
                .map(|log_index| crate::task::AuditEvent {
                    log_index,
                    actor: "system".into(),
                    request_id: format!("raft:{log_index}"),
                    action: format!("task.state.{next_state}"),
                    resource_id: task.task_id.to_string(),
                    from_state: previous_state,
                    to_state: next_state,
                    occurred_at_ms: chrono::Utc::now().timestamp_millis(),
                })
        } else {
            None
        };
        let batch_manifest = if let Some((key, manifest)) = batch_manifest {
            Some((key, serde_json::to_vec(&manifest)?))
        } else {
            None
        };
        self.task_store
            .save_shard_and_task_with_audit(shard, &task, audit, batch_manifest)
            .await
    }

    async fn apply_create_task(
        &self,
        mut task: crate::task::TaskMeta,
        incoming_shards: Vec<ShardMeta>,
        request_id: String,
        actor: String,
    ) -> Result<CommandResponse> {
        if serde_json::from_str::<crate::drivers::factory::SourceConfig>(&task.source_config)
            .is_ok_and(|source| {
                matches!(
                    source,
                    crate::drivers::factory::SourceConfig::ElasticsearchOnline { .. }
                        | crate::drivers::factory::SourceConfig::PostgresOutbox { .. }
                        | crate::drivers::factory::SourceConfig::PostgresPartitionedOutbox { .. }
                )
            })
        {
            task.metadata
                .insert("_dataspool_cdc_cutover_required".into(), "true".into());
        }
        let task_id = task.task_id;
        let request_id = if request_id.trim().is_empty() {
            // Older persisted Raft entries predate CreateTask request IDs.
            format!("legacy-create:{task_id}")
        } else {
            request_id
        };
        if request_id.len() > 128 {
            return Ok(rejected(
                "CreateTask request_id must contain at most 128 characters",
            ));
        }
        if incoming_shards.iter().any(|shard| shard.task_id != task_id) {
            return Ok(rejected("Shard belongs to a different task"));
        }
        let canonical_metadata: std::collections::BTreeMap<_, _> = task.metadata.iter().collect();
        let payload_fingerprint = format!(
            "{:x}",
            sha2::Sha256::digest(serde_json::to_vec(&(
                &task.name,
                &task.source_config,
                &task.sink_config,
                &task.transform_configs,
                &task.sharding_config,
                task.priority,
                canonical_metadata,
            ))?)
        );
        let request_payload = format!("create:{task_id}:{payload_fingerprint}");
        if let Some(existing_payload) = self.task_store.get_control_request(&request_id).await? {
            return if existing_payload == request_payload {
                Ok(accepted(
                    "Task creation already committed",
                    serde_json::json!({"task_id": task_id}),
                ))
            } else {
                Ok(rejected(
                    "request_id was already used for a different command",
                ))
            };
        }

        let existing_task = self.task_store.get_task(&task_id).await?;
        let previous_state = if let Some(existing) = existing_task {
            if existing.name != task.name
                || existing.source_config != task.source_config
                || existing.sink_config != task.sink_config
                || existing.transform_configs != task.transform_configs
                || existing.sharding_config != task.sharding_config
                || existing.priority != task.priority
                || existing.metadata != task.metadata
            {
                return Ok(rejected(
                    "Task creation idempotency key was reused with a different payload",
                ));
            }
            let state = task_state_name(&existing.state).to_string();
            task = existing;
            state
        } else {
            "absent".to_string()
        };

        let mut committed_shards = self.task_store.list_task_shards(&task_id).await?;
        for incoming in incoming_shards {
            if let Some(existing) = committed_shards
                .iter_mut()
                .find(|shard| shard.shard_id == incoming.shard_id)
            {
                // Bootstrap may replace a legacy unleased Running record with Pending.
                if matches!(&existing.state, ShardState::Running { .. })
                    && existing.lease_expires_at.is_none()
                    && existing.lease_id.is_none()
                    && matches!(&incoming.state, ShardState::Pending)
                {
                    *existing = incoming;
                }
            } else {
                committed_shards.push(incoming);
            }
        }
        task.refresh_from_shards(&committed_shards, chrono::Utc::now());
        let to_state = task_state_name(&task.state).to_string();
        let mut records = Vec::with_capacity(committed_shards.len() * 2 + 1);
        records.push((format!("task:{}", task.task_id), serde_json::to_vec(&task)?));
        for shard in &committed_shards {
            records.push((
                format!("shard:{}", shard.shard_id),
                serde_json::to_vec(shard)?,
            ));
            records.push((
                format!("task_shard:{}:{}", shard.task_id, shard.shard_id),
                Vec::new(),
            ));
        }
        self.task_store
            .save_control_batch(
                &request_id,
                &request_payload,
                records,
                self.audit_event(
                    &actor,
                    &request_id,
                    "task.create",
                    &task_id.to_string(),
                    &previous_state,
                    &to_state,
                ),
            )
            .await?;
        Ok(accepted(
            "Task and shards created",
            serde_json::json!({"task_id": task_id, "shards": committed_shards.len(), "request_id": request_id}),
        ))
    }

    async fn apply_claim_shard(
        &self,
        shard_id: String,
        node_id: String,
        claim_id: String,
        issued_at_ms: i64,
        lease_expires_at_ms: i64,
    ) -> Result<CommandResponse> {
        let shard_id = parse_shard_id(&shard_id)?;
        if self
            .task_store
            .get_node(&NodeId::new(node_id.clone()))
            .await?
            .is_some_and(|node| node.status != crate::types::NodeStatus::Healthy)
        {
            return Ok(rejected(
                "Paused or offline nodes cannot claim shards until execution is resumed",
            ));
        }
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        let Some(task) = self.task_store.get_task(&shard.task_id).await? else {
            return Ok(rejected("Parent task not found"));
        };
        if !matches!(
            &task.state,
            crate::types::TaskState::Pending | crate::types::TaskState::Running { .. }
        ) {
            return Ok(rejected("Task is not accepting new shard claims"));
        }
        let previous_expiry = shard
            .lease_expires_at
            .as_ref()
            .filter(|expires| expires.timestamp_millis() > issued_at_ms)
            .copied();
        let same_claim = matches!(&shard.state, ShardState::Running { node_id: owner, .. } if owner.as_str() == node_id)
            && shard.lease_id.as_deref() == Some(claim_id.as_str());
        if let Some(previous_expiry) = previous_expiry.filter(|_| same_claim) {
            return Ok(accepted(
                "Shard claim already committed",
                serde_json::json!({
                    "shard_id": shard_id.to_string(), "node_id": node_id,
                    "claim_id": claim_id, "fencing_token": shard.fencing_token,
                    "lease_expires_at_ms": previous_expiry.timestamp_millis()
                }),
            ));
        }
        let reclaimable = match &shard.state {
            ShardState::Pending => true,
            ShardState::Running { started_at, .. } => shard
                .lease_expires_at
                .as_ref()
                .map(|expires| expires.timestamp_millis() <= issued_at_ms)
                .unwrap_or_else(|| {
                    started_at.timestamp_millis().saturating_add(30_000) <= issued_at_ms
                }),
            _ => false,
        };
        if !reclaimable {
            return Ok(rejected("Shard is not pending or its lease is still valid"));
        }
        if lease_expires_at_ms <= issued_at_ms {
            return Ok(rejected("Lease expiration must be later than claim time"));
        }
        let expires = chrono::DateTime::from_timestamp_millis(lease_expires_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid lease expiration".into()))?;
        let issued_at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid claim timestamp".into()))?;
        let fencing_token = match shard.start_with_lease(
            NodeId::new(node_id.clone()),
            issued_at,
            expires,
            claim_id.clone(),
        ) {
            Some(token) => token,
            None => return Ok(rejected("Shard fencing token exhausted")),
        };
        self.persist_shard_and_aggregate(&shard).await?;
        Ok(accepted(
            "Shard claimed",
            serde_json::json!({
                "shard_id": shard_id.to_string(), "node_id": node_id, "claim_id": claim_id,
                "fencing_token": fencing_token, "lease_expires_at_ms": lease_expires_at_ms
            }),
        ))
    }

    async fn apply_release_lease(
        &self,
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
    ) -> Result<CommandResponse> {
        let shard_id = parse_shard_id(&shard_id)?;
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid release timestamp".into()))?;
        if matches!(&shard.state, ShardState::Pending) && shard.fencing_token == fencing_token {
            return Ok(accepted(
                "Shard lease already released",
                serde_json::json!({"fencing_token": fencing_token}),
            ));
        }
        if !lease_matches(&shard, &node_id, fencing_token, at) {
            return Ok(rejected("Lease release rejected by fencing token"));
        }
        shard.set_state_at(ShardState::Pending, at);
        self.persist_shard_and_aggregate(&shard).await?;
        Ok(accepted(
            "Shard lease released",
            serde_json::json!({"fencing_token": fencing_token}),
        ))
    }

    async fn apply_import_legacy_cache(
        &self,
        shard_id: String,
        batch_id: String,
        records: Vec<crate::pipeline::Record>,
        next_offset: Option<crate::pipeline::OffsetValue>,
        source_count: u64,
    ) -> Result<CommandResponse> {
        if self.task_store.bootstrap_complete().await? {
            return Ok(rejected("Legacy cache import is closed after bootstrap"));
        }
        let parsed_shard_id = parse_shard_id(&shard_id)?;
        let shard = match self.task_store.get_shard(&parsed_shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        if !matches!(&shard.state, ShardState::Pending)
            || shard.lease_expires_at.is_some()
            || shard.lease_id.is_some()
        {
            return Ok(rejected(
                "Legacy cache can only be imported before the shard is first claimed",
            ));
        }
        if !valid_batch_id(&batch_id) || !batch_id.starts_with("0000000000000000-") {
            return Ok(rejected("Invalid legacy cache import ID"));
        }
        if source_count != records.len() as u64 {
            return Ok(rejected(
                "Legacy cache source count does not match its payload",
            ));
        }
        // Legacy cache entries created before cursor-aware batches have no next offset.
        // Preserve the shard's current cursor explicitly so the manifest can still
        // advance through Delivered -> Checkpointed -> Acknowledged.
        let next_offset = Some(next_offset.unwrap_or_else(|| shard.offset.clone()));
        let key = crate::pipeline::cache::LocalBatchCache::<S>::key_for_shard(&shard_id, &batch_id);
        let entry = crate::pipeline::cache::CacheEntry {
            batch_id: batch_id.clone(),
            records,
            next_offset,
            source_count,
        };
        let cache_entry_exists = if let Some(existing) = self
            .task_store
            .kv_store()
            .get_cf(crate::storage::cf::CACHE, &key)
            .await?
        {
            let existing: crate::pipeline::cache::CacheEntry = serde_json::from_slice(&existing)?;
            if existing != entry {
                return Ok(rejected(
                    "Legacy cache import ID was reused with a different payload",
                ));
            }
            true
        } else {
            false
        };
        if !cache_entry_exists && self.task_store.cache_batch_count(&shard_id).await? >= 128 {
            return Ok(rejected("Replicated shard cache is full during bootstrap"));
        }
        let manifest_key = crate::pipeline::cache::BatchManifest::key(&shard_id, &batch_id);
        if let Some(existing) = self
            .task_store
            .kv_store()
            .get_cf(crate::storage::cf::META, &manifest_key)
            .await?
        {
            let existing: crate::pipeline::cache::BatchManifest =
                serde_json::from_slice(&existing)?;
            if existing.payload_digest
                != crate::pipeline::cache::records_payload_digest(&entry.records)?
            {
                return Ok(rejected("Legacy batch manifest payload digest mismatch"));
            }
            if !cache_entry_exists {
                self.task_store
                    .save_cache_batch_with_manifest(&shard_id, &batch_id, &entry, &existing)
                    .await?;
            }
            return Ok(accepted(
                "Legacy cache entry already imported",
                serde_json::json!({"batch_id": batch_id}),
            ));
        }
        let manifest = crate::pipeline::cache::BatchManifest {
            schema_version: 1,
            shard_id: shard_id.clone(),
            batch_id: batch_id.clone(),
            fencing_token: 0,
            source_offset: shard.offset.clone(),
            next_offset: entry.next_offset.clone(),
            source_count: entry.source_count,
            payload_digest: crate::pipeline::cache::records_payload_digest(&entry.records)?,
            target_success_count: None,
            target_failed_count: None,
            target_bytes_written: None,
            status: crate::pipeline::cache::BatchManifestStatus::Prepared,
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        self.task_store
            .save_cache_batch_with_manifest(&shard_id, &batch_id, &entry, &manifest)
            .await?;
        Ok(accepted(
            "Legacy cache entry imported",
            serde_json::json!({"batch_id": batch_id}),
        ))
    }

    async fn apply_cache_batch(&self, request: CacheBatchRequest) -> Result<CommandResponse> {
        let CacheBatchRequest {
            shard_id,
            node_id,
            fencing_token,
            issued_at_ms,
            batch_id,
            records,
            next_offset,
            source_count,
        } = request;
        let parsed_shard_id = parse_shard_id(&shard_id)?;
        let shard = match self.task_store.get_shard(&parsed_shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        if self
            .task_store
            .get_task(&shard.task_id)
            .await?
            .is_some_and(|task| {
                matches!(
                    task.state,
                    crate::types::TaskState::Pausing | crate::types::TaskState::Cancelled { .. }
                )
            })
        {
            return Ok(rejected(
                "Task is pausing or cancelled; no new source batches are accepted",
            ));
        }
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid cache batch timestamp".into()))?;
        if !lease_matches(&shard, &node_id, fencing_token, at) {
            return Ok(rejected("Cache batch rejected by fencing token"));
        }
        if !valid_batch_id(&batch_id) {
            return Ok(rejected(
                "Invalid cache batch ID; expected a 16-hex generation and 6-hex sequence",
            ));
        }
        if source_count != records.len() as u64 {
            return Ok(rejected(
                "Cache batch source count does not match its payload",
            ));
        }
        if self
            .task_store
            .cache_ack_watermark(&shard_id)
            .await?
            .as_deref()
            .is_some_and(|watermark| batch_id.as_str() <= watermark)
        {
            return Ok(accepted(
                "Source batch was already acknowledged",
                serde_json::json!({"batch_id": batch_id}),
            ));
        }
        let key = crate::pipeline::cache::LocalBatchCache::<S>::key_for_shard(&shard_id, &batch_id);
        let entry = crate::pipeline::cache::CacheEntry {
            batch_id: batch_id.clone(),
            records,
            next_offset,
            source_count,
        };
        let cache_entry_exists = if let Some(existing) = self
            .task_store
            .kv_store()
            .get_cf(crate::storage::cf::CACHE, &key)
            .await?
        {
            let existing: crate::pipeline::cache::CacheEntry = serde_json::from_slice(&existing)?;
            if (!existing.batch_id.is_empty() && existing.batch_id != entry.batch_id)
                || existing.records != entry.records
                || existing.next_offset != entry.next_offset
                || existing.source_count != entry.source_count
            {
                return Ok(rejected(
                    "Cache batch ID was reused with a different payload",
                ));
            }
            true
        } else {
            false
        };
        if !cache_entry_exists && self.task_store.cache_batch_count(&shard_id).await? >= 128 {
            return Ok(rejected("Replicated shard cache is full"));
        }
        let manifest_key = crate::pipeline::cache::BatchManifest::key(&shard_id, &batch_id);
        if let Some(existing) = self
            .task_store
            .kv_store()
            .get_cf(crate::storage::cf::META, &manifest_key)
            .await?
        {
            let existing: crate::pipeline::cache::BatchManifest =
                serde_json::from_slice(&existing)?;
            let digest = crate::pipeline::cache::records_payload_digest(&entry.records)?;
            if existing.payload_digest != digest
                || existing.next_offset != entry.next_offset
                || existing.source_count != entry.source_count
            {
                return Ok(rejected("Source batch manifest payload mismatch"));
            }
            if !cache_entry_exists {
                self.task_store
                    .save_cache_batch_with_manifest(&shard_id, &batch_id, &entry, &existing)
                    .await?;
            }
            return Ok(accepted(
                "Source batch already replicated",
                serde_json::json!({"batch_id": batch_id}),
            ));
        }
        let manifest = crate::pipeline::cache::BatchManifest {
            schema_version: 1,
            shard_id: shard_id.clone(),
            batch_id: batch_id.clone(),
            fencing_token,
            source_offset: shard.offset.clone(),
            next_offset: entry.next_offset.clone(),
            source_count: entry.source_count,
            payload_digest: crate::pipeline::cache::records_payload_digest(&entry.records)?,
            target_success_count: None,
            target_failed_count: None,
            target_bytes_written: None,
            status: crate::pipeline::cache::BatchManifestStatus::Prepared,
            created_at_ms: issued_at_ms,
            updated_at_ms: issued_at_ms,
        };
        self.task_store
            .save_cache_batch_with_manifest(&shard_id, &batch_id, &entry, &manifest)
            .await?;
        Ok(accepted(
            "Source batch replicated",
            serde_json::json!({"batch_id": batch_id}),
        ))
    }

    async fn apply_record_batch_delivery(
        &self,
        request: BatchDeliveryRequest,
    ) -> Result<CommandResponse> {
        let BatchDeliveryRequest {
            shard_id,
            node_id,
            fencing_token,
            issued_at_ms,
            batch_id,
            success_count,
            failed_count,
            bytes_written,
        } = request;
        let shard = match self
            .task_store
            .get_shard(&parse_shard_id(&shard_id)?)
            .await?
        {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms).ok_or_else(|| {
            DatspoolError::Serialization("Invalid batch delivery timestamp".into())
        })?;
        if !lease_matches(&shard, &node_id, fencing_token, at) {
            return Ok(rejected("Batch delivery result rejected by fencing token"));
        }
        let key = crate::pipeline::cache::BatchManifest::key(&shard_id, &batch_id);
        let Some(value) = self
            .task_store
            .kv_store()
            .get_cf(crate::storage::cf::META, &key)
            .await?
        else {
            return Ok(rejected("Batch manifest not found"));
        };
        let mut manifest: crate::pipeline::cache::BatchManifest = serde_json::from_slice(&value)?;
        if manifest.fencing_token > fencing_token
            || manifest.shard_id != shard_id
            || manifest.batch_id != batch_id
        {
            return Ok(rejected(
                "Batch manifest identity or fencing generation mismatch",
            ));
        }
        let cache_key =
            crate::pipeline::cache::LocalBatchCache::<S>::key_for_shard(&shard_id, &batch_id);
        let Some(cache_bytes) = self
            .task_store
            .kv_store()
            .get_cf(crate::storage::cf::CACHE, &cache_key)
            .await?
        else {
            return Ok(rejected(
                "Replicated batch payload is missing before delivery acknowledgement",
            ));
        };
        let cache_entry: crate::pipeline::cache::CacheEntry = serde_json::from_slice(&cache_bytes)?;
        let actual_digest = crate::pipeline::cache::records_payload_digest(&cache_entry.records)?;
        if cache_entry.batch_id != batch_id
            || cache_entry.next_offset != manifest.next_offset
            || cache_entry.source_count != manifest.source_count
            || cache_entry.records.len() as u64 != manifest.source_count
            || actual_digest != manifest.payload_digest
        {
            return Ok(rejected(
                "Replicated batch payload does not match its durable manifest",
            ));
        }
        if matches!(
            manifest.status,
            crate::pipeline::cache::BatchManifestStatus::Delivered
                | crate::pipeline::cache::BatchManifestStatus::Checkpointed
                | crate::pipeline::cache::BatchManifestStatus::Acknowledged
        ) {
            return Ok(accepted(
                "Batch delivery result already recorded",
                serde_json::json!({"batch_id": batch_id}),
            ));
        }
        manifest.fencing_token = fencing_token;
        if manifest.status != crate::pipeline::cache::BatchManifestStatus::Prepared {
            return Ok(rejected(
                "Batch manifest is not awaiting destination delivery",
            ));
        }
        if success_count.saturating_add(failed_count) != manifest.source_count {
            return Ok(rejected(
                "Destination result does not account for every source record",
            ));
        }
        manifest.target_success_count = Some(success_count);
        manifest.target_failed_count = Some(failed_count);
        manifest.target_bytes_written = Some(bytes_written);
        manifest.status = crate::pipeline::cache::BatchManifestStatus::Delivered;
        manifest.updated_at_ms = issued_at_ms;
        self.task_store
            .kv_store()
            .put_cf(
                crate::storage::cf::META,
                &key,
                &serde_json::to_vec(&manifest)?,
            )
            .await?;
        Ok(accepted(
            "Batch delivery result recorded",
            serde_json::json!({"batch_id": batch_id}),
        ))
    }

    async fn apply_dead_letters(
        &self,
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        failures: Vec<crate::pipeline::FailedRecord>,
    ) -> Result<CommandResponse> {
        let parsed_shard_id = parse_shard_id(&shard_id)?;
        let shard = match self.task_store.get_shard(&parsed_shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid dead-letter timestamp".into()))?;
        if !lease_matches(&shard, &node_id, fencing_token, at) {
            return Ok(rejected(
                "Dead-letter persistence rejected by fencing token",
            ));
        }
        let count = failures.len();
        let queue = crate::pipeline::DeadLetterQueue::new(
            std::sync::Arc::new(self.task_store.kv_store().clone()),
            &shard_id,
        );
        queue
            .enqueue_at(failures, issued_at_ms.max(0) as u64)
            .await?;
        Ok(accepted(
            "Dead-letter records replicated",
            serde_json::json!({"count": count}),
        ))
    }

    async fn apply_ack_cache_batch(
        &self,
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        cache_key: String,
    ) -> Result<CommandResponse> {
        let parsed_shard_id = parse_shard_id(&shard_id)?;
        let expected_prefix =
            crate::pipeline::cache::LocalBatchCache::<S>::prefix_for_shard(&shard_id);
        let batch_id = match cache_key
            .as_bytes()
            .strip_prefix(expected_prefix.as_slice())
        {
            Some(value) => match std::str::from_utf8(value) {
                Ok(value) if valid_batch_id(value) => value.to_string(),
                _ => return Ok(rejected("Invalid replicated cache key")),
            },
            None => return Ok(rejected("Cache key does not belong to this shard")),
        };
        if self
            .task_store
            .cache_ack_watermark(&shard_id)
            .await?
            .as_deref()
            .is_some_and(|watermark| batch_id.as_str() <= watermark)
        {
            return Ok(accepted(
                "Replicated cache entry already acknowledged",
                serde_json::json!({"batch_id": batch_id}),
            ));
        }
        let shard = match self.task_store.get_shard(&parsed_shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms).ok_or_else(|| {
            DatspoolError::Serialization("Invalid cache acknowledgement timestamp".into())
        })?;
        if !lease_matches(&shard, &node_id, fencing_token, at) {
            return Ok(rejected("Cache acknowledgement rejected by fencing token"));
        }
        match self.task_store.first_cache_batch_key(&shard_id).await? {
            Some(first_key) if first_key == cache_key.as_bytes() => {}
            Some(_) => return Ok(rejected("Cache batches must be acknowledged in FIFO order")),
            None => return Ok(rejected("Replicated cache batch does not exist")),
        }
        let manifest_key = crate::pipeline::cache::BatchManifest::key(&shard_id, &batch_id);
        let Some(manifest_bytes) = self
            .task_store
            .kv_store()
            .get_cf(crate::storage::cf::META, &manifest_key)
            .await?
        else {
            return Ok(rejected("Batch manifest not found during acknowledgement"));
        };
        let mut manifest: crate::pipeline::cache::BatchManifest =
            serde_json::from_slice(&manifest_bytes)?;
        if manifest.fencing_token > fencing_token
            || manifest.status != crate::pipeline::cache::BatchManifestStatus::Checkpointed
        {
            return Ok(rejected(
                "Batch must be checkpointed by the current fencing generation before acknowledgement",
            ));
        }
        manifest.fencing_token = fencing_token;
        manifest.status = crate::pipeline::cache::BatchManifestStatus::Acknowledged;
        manifest.updated_at_ms = issued_at_ms;
        self.task_store
            .ack_cache_batch(&shard_id, &batch_id, cache_key.as_bytes(), &manifest)
            .await?;
        Ok(accepted(
            "Replicated cache entry acknowledged",
            serde_json::json!({"cache_key": cache_key}),
        ))
    }

    async fn apply_renew_lease(
        &self,
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        lease_expires_at_ms: i64,
    ) -> Result<CommandResponse> {
        let shard_id = parse_shard_id(&shard_id)?;
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        let owner_matches = matches!(&shard.state, ShardState::Running { node_id: owner, .. } if owner.as_str() == node_id);
        let lease_valid_at_issue = shard
            .lease_expires_at
            .as_ref()
            .is_some_and(|expires| expires.timestamp_millis() > issued_at_ms);
        let new_expiry = chrono::DateTime::from_timestamp_millis(lease_expires_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid lease expiration".into()))?;
        if owner_matches
            && shard.fencing_token == fencing_token
            && shard.lease_expires_at.as_ref() == Some(&new_expiry)
        {
            return Ok(accepted(
                "Lease renewal already committed",
                serde_json::json!({"fencing_token": fencing_token, "lease_expires_at_ms": lease_expires_at_ms}),
            ));
        }
        let issued_at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid renewal timestamp".into()))?;
        if !owner_matches
            || !lease_valid_at_issue
            || !shard.renew_lease_at(&NodeId::new(node_id), fencing_token, new_expiry, issued_at)
        {
            return Ok(rejected(
                "Lease renewal rejected: owner, fencing token, or lease deadline is stale",
            ));
        }
        self.task_store.save_shard(&shard).await?;
        Ok(accepted(
            "Lease renewed",
            serde_json::json!({"fencing_token": fencing_token, "lease_expires_at_ms": lease_expires_at_ms}),
        ))
    }

    async fn apply_progress(&self, request: ShardProgressRequest) -> Result<CommandResponse> {
        let ShardProgressRequest {
            shard_id,
            node_id,
            fencing_token,
            issued_at_ms,
            processed_records,
            processed_bytes,
            offset,
            batch_id,
        } = request;
        let shard_id = parse_shard_id(&shard_id)?;
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        if !lease_matches(
            &shard,
            &node_id,
            fencing_token,
            chrono::DateTime::from_timestamp_millis(issued_at_ms)
                .ok_or_else(|| DatspoolError::Serialization("Invalid command timestamp".into()))?,
        ) {
            return Ok(rejected("Progress rejected by fencing token"));
        }
        let offset = serde_json::from_str(&offset)
            .map_err(|e| DatspoolError::Serialization(e.to_string()))?;
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid progress timestamp".into()))?;
        let mut manifest_update = None;
        if let Some(batch_id) = batch_id.as_ref() {
            let key = crate::pipeline::cache::BatchManifest::key(&shard_id.to_string(), batch_id);
            let Some(value) = self
                .task_store
                .kv_store()
                .get_cf(crate::storage::cf::META, &key)
                .await?
            else {
                return Ok(rejected("Checkpoint batch manifest not found"));
            };
            let mut manifest: crate::pipeline::cache::BatchManifest =
                serde_json::from_slice(&value)?;
            if manifest.fencing_token > fencing_token
                || manifest.next_offset.as_ref() != Some(&offset)
                || !matches!(
                    manifest.status,
                    crate::pipeline::cache::BatchManifestStatus::Delivered
                        | crate::pipeline::cache::BatchManifestStatus::Checkpointed
                )
            {
                return Ok(rejected(
                    "Batch manifest is not eligible for checkpoint commit",
                ));
            }
            if manifest.status == crate::pipeline::cache::BatchManifestStatus::Checkpointed {
                if shard.offset == offset
                    && shard.records_processed == processed_records
                    && shard.bytes_processed == processed_bytes
                {
                    return Ok(accepted(
                        "Batch checkpoint already committed",
                        serde_json::json!({"batch_id": batch_id}),
                    ));
                }
                return Ok(rejected(
                    "Checkpointed batch conflicts with current shard progress",
                ));
            }
            manifest.fencing_token = fencing_token;
            manifest.status = crate::pipeline::cache::BatchManifestStatus::Checkpointed;
            manifest.updated_at_ms = issued_at_ms;
            manifest_update = Some((key, manifest));
        }
        shard.set_progress_at(processed_records, processed_bytes, offset, at);
        self.persist_shard_and_aggregate_with_manifest(&shard, manifest_update)
            .await?;
        Ok(accepted(
            "Shard progress updated",
            serde_json::json!({"fencing_token": fencing_token}),
        ))
    }

    async fn apply_record_cdc_cutover(
        &self,
        request: CdcCutoverRequest,
    ) -> Result<CommandResponse> {
        let CdcCutoverRequest {
            task_id,
            alias,
            old_index,
            new_index,
            final_watermark,
            applied_watermark,
            fencing_shard_id,
            node_id,
            fencing_token,
            issued_at_ms,
        } = request;
        if alias.trim().is_empty()
            || new_index.trim().is_empty()
            || applied_watermark < final_watermark
        {
            return Ok(rejected(
                "CDC cutover metadata is incomplete or target watermark is behind source",
            ));
        }
        let shard_id = parse_shard_id(&fencing_shard_id)?;
        let Some(shard) = self.task_store.get_shard(&shard_id).await? else {
            return Ok(rejected("CDC cutover fencing shard not found"));
        };
        if shard.task_id.to_string() != task_id
            || !lease_matches(
                &shard,
                &node_id,
                fencing_token,
                chrono::DateTime::from_timestamp_millis(issued_at_ms).ok_or_else(|| {
                    DatspoolError::Serialization("Invalid cutover timestamp".into())
                })?,
            )
        {
            return Ok(rejected(
                "CDC cutover rejected by task identity or fencing token",
            ));
        }
        let Some(mut task) = self.task_store.get_task(&shard.task_id).await? else {
            return Ok(rejected("CDC cutover task not found"));
        };
        if task
            .metadata
            .contains_key("_dataspool_cdc_partition_final_watermarks")
        {
            return Ok(rejected(
                "Cannot replace a committed partitioned CDC vector cutover with a scalar watermark",
            ));
        }
        if task
            .metadata
            .get("_dataspool_cdc_cutover_committed")
            .is_some_and(|value| value == "true")
        {
            let previous_final = task
                .metadata
                .get("_dataspool_cdc_final_watermark")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            let previous_applied = task
                .metadata
                .get("_dataspool_cdc_applied_watermark")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            let previous_alias = task.metadata.get("_dataspool_cdc_alias");
            let previous_index = task.metadata.get("_dataspool_cdc_new_index");
            if final_watermark < previous_final
                || applied_watermark < previous_applied
                || previous_alias.is_some_and(|value| value != &alias)
                || previous_index.is_some_and(|value| value != &new_index)
            {
                return Ok(rejected(
                    "CDC cutover record cannot regress watermarks or change its committed alias destination",
                ));
            }
        }
        task.metadata
            .insert("_dataspool_cdc_cutover_required".into(), "true".into());
        task.metadata.insert("_dataspool_cdc_alias".into(), alias);
        task.metadata
            .insert("_dataspool_cdc_new_index".into(), new_index);
        task.metadata.insert(
            "_dataspool_cdc_final_watermark".into(),
            final_watermark.to_string(),
        );
        task.metadata.insert(
            "_dataspool_cdc_applied_watermark".into(),
            applied_watermark.to_string(),
        );
        task.metadata
            .insert("_dataspool_cdc_cutover_committed".into(), "true".into());
        task.metadata.insert(
            "_dataspool_cdc_cutover_at_ms".into(),
            issued_at_ms.to_string(),
        );
        if let Some(old_index) = old_index {
            task.metadata
                .insert("_dataspool_cdc_old_index".into(), old_index);
        }
        self.task_store.save_task(&task).await?;
        Ok(accepted(
            "CDC cutover committed in Raft",
            serde_json::json!({
                "final_watermark": final_watermark,
                "applied_watermark": applied_watermark,
                "alias": task.metadata.get("_dataspool_cdc_alias"),
            }),
        ))
    }

    async fn apply_record_cdc_partitioned_cutover(
        &self,
        request: CdcPartitionedCutoverRequest,
    ) -> Result<CommandResponse> {
        let CdcPartitionedCutoverRequest {
            task_id,
            alias,
            old_index,
            new_index,
            final_watermarks,
            applied_watermarks,
            fencing_shard_id,
            node_id,
            fencing_token,
            issued_at_ms,
        } = request;
        if alias.trim().is_empty()
            || new_index.trim().is_empty()
            || final_watermarks.is_empty()
            || final_watermarks.len() != applied_watermarks.len()
            || final_watermarks.iter().any(|(partition, final_seq)| {
                applied_watermarks
                    .get(partition)
                    .is_none_or(|applied| applied < final_seq)
            })
        {
            return Ok(rejected(
                "Partitioned CDC cutover vector is incomplete or target partition is behind",
            ));
        }
        let shard_id = parse_shard_id(&fencing_shard_id)?;
        let Some(shard) = self.task_store.get_shard(&shard_id).await? else {
            return Ok(rejected("CDC cutover fencing shard not found"));
        };
        if shard.task_id.to_string() != task_id
            || !lease_matches(
                &shard,
                &node_id,
                fencing_token,
                chrono::DateTime::from_timestamp_millis(issued_at_ms).ok_or_else(|| {
                    DatspoolError::Serialization("Invalid partitioned cutover timestamp".into())
                })?,
            )
        {
            return Ok(rejected(
                "Partitioned CDC cutover rejected by task identity or fencing token",
            ));
        }
        let Some(mut task) = self.task_store.get_task(&shard.task_id).await? else {
            return Ok(rejected("CDC cutover task not found"));
        };
        if task
            .metadata
            .get("_dataspool_cdc_cutover_committed")
            .is_some_and(|v| v == "true")
        {
            if let Some(previous) = task
                .metadata
                .get("_dataspool_cdc_partition_final_watermarks")
            {
                let previous: std::collections::BTreeMap<u32, u64> = serde_json::from_str(previous)
                    .map_err(|e| {
                        DatspoolError::Serialization(format!(
                            "Invalid prior partition final watermark vector: {e}"
                        ))
                    })?;
                let previous_applied: std::collections::BTreeMap<u32, u64> = task
                    .metadata
                    .get("_dataspool_cdc_partition_applied_watermarks")
                    .map(|value| serde_json::from_str(value))
                    .transpose()
                    .map_err(|e| {
                        DatspoolError::Serialization(format!(
                            "Invalid prior partition applied watermark vector: {e}"
                        ))
                    })?
                    .unwrap_or_default();
                let alias_changed = task
                    .metadata
                    .get("_dataspool_cdc_alias")
                    .is_some_and(|value| value != &alias);
                let index_changed = task
                    .metadata
                    .get("_dataspool_cdc_new_index")
                    .is_some_and(|value| value != &new_index);
                let final_regressed = previous.len() != final_watermarks.len()
                    || previous.iter().any(|(partition, sequence)| {
                        final_watermarks
                            .get(partition)
                            .is_none_or(|next| next < sequence)
                    });
                let applied_regressed = previous_applied.len() != applied_watermarks.len()
                    || previous_applied.iter().any(|(partition, sequence)| {
                        applied_watermarks
                            .get(partition)
                            .is_none_or(|next| next < sequence)
                    });
                if final_regressed || applied_regressed || alias_changed || index_changed {
                    return Ok(rejected(
                        "Partitioned CDC cutover cannot regress vectors or change its committed alias destination",
                    ));
                }
            } else if task.metadata.contains_key("_dataspool_cdc_final_watermark") {
                return Ok(rejected(
                    "Cannot change an already committed scalar CDC cutover into a partitioned vector",
                ));
            }
        }
        task.metadata
            .insert("_dataspool_cdc_cutover_required".into(), "true".into());
        task.metadata.insert("_dataspool_cdc_alias".into(), alias);
        task.metadata
            .insert("_dataspool_cdc_new_index".into(), new_index);
        task.metadata.insert(
            "_dataspool_cdc_partition_final_watermarks".into(),
            serde_json::to_string(&final_watermarks)
                .map_err(|e| DatspoolError::Serialization(e.to_string()))?,
        );
        task.metadata.insert(
            "_dataspool_cdc_partition_applied_watermarks".into(),
            serde_json::to_string(&applied_watermarks)
                .map_err(|e| DatspoolError::Serialization(e.to_string()))?,
        );
        task.metadata
            .insert("_dataspool_cdc_cutover_committed".into(), "true".into());
        task.metadata.insert(
            "_dataspool_cdc_cutover_at_ms".into(),
            issued_at_ms.to_string(),
        );
        if let Some(old_index) = old_index {
            task.metadata
                .insert("_dataspool_cdc_old_index".into(), old_index);
        }
        self.task_store.save_task(&task).await?;
        Ok(accepted(
            "Partitioned CDC vector cutover committed in Raft",
            serde_json::json!({
                "final_watermarks": final_watermarks, "applied_watermarks": applied_watermarks,
                "alias": task.metadata.get("_dataspool_cdc_alias")
            }),
        ))
    }

    async fn apply_complete(
        &self,
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        total_records: u64,
    ) -> Result<CommandResponse> {
        let shard_id = parse_shard_id(&shard_id)?;
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        if self
            .task_store
            .get_task(&shard.task_id)
            .await?
            .is_some_and(|task| matches!(task.state, crate::types::TaskState::Cancelled { .. }))
        {
            return Ok(rejected("Task has been cancelled"));
        }
        let task = self.task_store.get_task(&shard.task_id).await?;
        if let Some(task) = task.as_ref()
            && task
                .metadata
                .get("_dataspool_cdc_cutover_required")
                .is_some_and(|value| value == "true")
        {
            if let (Some(final_vector), Some(applied_vector)) = (
                task.metadata
                    .get("_dataspool_cdc_partition_final_watermarks"),
                task.metadata
                    .get("_dataspool_cdc_partition_applied_watermarks"),
            ) {
                let final_vector: std::collections::BTreeMap<u32, u64> =
                    serde_json::from_str(final_vector).map_err(|e| {
                        DatspoolError::Serialization(format!(
                            "Invalid partition final watermark vector: {e}"
                        ))
                    })?;
                let applied_vector: std::collections::BTreeMap<u32, u64> =
                    serde_json::from_str(applied_vector).map_err(|e| {
                        DatspoolError::Serialization(format!(
                            "Invalid partition applied watermark vector: {e}"
                        ))
                    })?;
                let committed = task
                    .metadata
                    .get("_dataspool_cdc_cutover_committed")
                    .is_some_and(|v| v == "true");
                let alias = task
                    .metadata
                    .get("_dataspool_cdc_alias")
                    .is_some_and(|v| !v.trim().is_empty());
                let new_index = task
                    .metadata
                    .get("_dataspool_cdc_new_index")
                    .is_some_and(|v| !v.trim().is_empty());
                let caught_up = !final_vector.is_empty()
                    && final_vector.len() == applied_vector.len()
                    && final_vector.iter().all(|(partition, final_seq)| {
                        applied_vector
                            .get(partition)
                            .is_some_and(|applied| applied >= final_seq)
                    });
                if !committed || !alias || !new_index || !caught_up {
                    return Ok(rejected(
                        "Online shard completion requires a committed partitioned CDC cutover with every partition caught up",
                    ));
                }
            } else {
                let final_watermark = task
                    .metadata
                    .get("_dataspool_cdc_final_watermark")
                    .and_then(|value| value.parse::<u64>().ok());
                let applied_watermark = task
                    .metadata
                    .get("_dataspool_cdc_applied_watermark")
                    .and_then(|value| value.parse::<u64>().ok());
                let committed = task
                    .metadata
                    .get("_dataspool_cdc_cutover_committed")
                    .is_some_and(|value| value == "true");
                let alias = task
                    .metadata
                    .get("_dataspool_cdc_alias")
                    .is_some_and(|value| !value.trim().is_empty());
                let new_index = task
                    .metadata
                    .get("_dataspool_cdc_new_index")
                    .is_some_and(|value| !value.trim().is_empty());
                if !committed
                    || !alias
                    || !new_index
                    || final_watermark.is_none()
                    || applied_watermark
                        .is_none_or(|applied| applied < final_watermark.unwrap_or(u64::MAX))
                {
                    return Ok(rejected(
                        "Online shard completion requires a committed CDC cutover at or beyond the final source watermark",
                    ));
                }
            }
        }
        if matches!(&shard.state, ShardState::Completed { .. })
            && shard.fencing_token == fencing_token
        {
            return Ok(accepted(
                "Shard completion already committed",
                serde_json::json!({"fencing_token": fencing_token}),
            ));
        }
        if !lease_matches(
            &shard,
            &node_id,
            fencing_token,
            chrono::DateTime::from_timestamp_millis(issued_at_ms)
                .ok_or_else(|| DatspoolError::Serialization("Invalid command timestamp".into()))?,
        ) {
            return Ok(rejected("Completion rejected by fencing token"));
        }
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid completion timestamp".into()))?;
        shard.complete_at(total_records, at);
        self.persist_shard_and_aggregate(&shard).await?;
        Ok(accepted(
            "Shard completed",
            serde_json::json!({"fencing_token": fencing_token}),
        ))
    }

    async fn apply_fail(
        &self,
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        error_message: String,
    ) -> Result<CommandResponse> {
        let shard_id = parse_shard_id(&shard_id)?;
        let mut shard = match self.task_store.get_shard(&shard_id).await? {
            Some(shard) => shard,
            None => return Ok(rejected("Shard not found")),
        };
        if self
            .task_store
            .get_task(&shard.task_id)
            .await?
            .is_some_and(|task| matches!(task.state, crate::types::TaskState::Cancelled { .. }))
        {
            return Ok(rejected("Task has been cancelled"));
        }
        if matches!(&shard.state, ShardState::Failed { .. }) && shard.fencing_token == fencing_token
        {
            return Ok(accepted(
                "Shard failure already committed",
                serde_json::json!({"fencing_token": fencing_token}),
            ));
        }
        if !lease_matches(
            &shard,
            &node_id,
            fencing_token,
            chrono::DateTime::from_timestamp_millis(issued_at_ms)
                .ok_or_else(|| DatspoolError::Serialization("Invalid command timestamp".into()))?,
        ) {
            return Ok(rejected("Failure update rejected by fencing token"));
        }
        let at = chrono::DateTime::from_timestamp_millis(issued_at_ms)
            .ok_or_else(|| DatspoolError::Serialization("Invalid failure timestamp".into()))?;
        shard.fail_at(error_message, 1, at);
        self.persist_shard_and_aggregate(&shard).await?;
        Ok(accepted(
            "Shard marked failed",
            serde_json::json!({"fencing_token": fencing_token}),
        ))
    }

    pub async fn validate_lease(
        &self,
        shard_id: &ShardId,
        node_id: &str,
        fencing_token: u64,
    ) -> Result<bool> {
        let Some(shard) = self.task_store.get_shard(shard_id).await? else {
            return Ok(false);
        };
        Ok(lease_matches(
            &shard,
            node_id,
            fencing_token,
            chrono::Utc::now(),
        ))
    }
}

fn lease_matches(
    shard: &ShardMeta,
    node_id: &str,
    fencing_token: u64,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    shard.fencing_token == fencing_token
        && shard
            .lease_expires_at
            .as_ref()
            .is_some_and(|expires| expires > &now)
        && matches!(&shard.state, ShardState::Running { node_id: owner, .. } if owner.as_str() == node_id)
}

fn valid_batch_id(value: &str) -> bool {
    let Some((generation, sequence)) = value.split_once('-') else {
        return false;
    };
    generation.len() == 16
        && sequence.len() == 6
        && generation.bytes().all(|byte| byte.is_ascii_hexdigit())
        && sequence.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn parse_shard_id(value: &str) -> Result<ShardId> {
    value
        .parse()
        .map_err(|e: uuid::Error| DatspoolError::Serialization(e.to_string()))
}

fn accepted(message: &str, data: serde_json::Value) -> CommandResponse {
    CommandResponse {
        success: true,
        message: message.to_string(),
        data: Some(data),
    }
}

fn rejected(message: &str) -> CommandResponse {
    CommandResponse {
        success: false,
        message: message.to_string(),
        data: None,
    }
}

fn task_state_name(state: &crate::types::TaskState) -> &'static str {
    match state {
        crate::types::TaskState::Pending => "pending",
        crate::types::TaskState::Running { .. } => "running",
        crate::types::TaskState::Pausing => "pausing",
        crate::types::TaskState::Paused => "paused",
        crate::types::TaskState::Canceling => "canceling",
        crate::types::TaskState::Cancelled { .. } => "cancelled",
        crate::types::TaskState::Completed { .. } => "completed",
        crate::types::TaskState::Failed { .. } => "failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RocksDbStore;
    use crate::task::{ShardMeta, TaskMeta};
    use tempfile::tempdir;

    fn federated_cluster(id: &str) -> crate::federation::FederatedCluster {
        crate::federation::FederatedCluster {
            cluster_id: id.into(),
            endpoint: format!("https://{id}.example.com:9443"),
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
            capabilities: [
                "elasticsearch-source".to_string(),
                "elasticsearch-sink".to_string(),
            ]
            .into_iter()
            .collect(),
            status: crate::federation::FederatedClusterStatus::Healthy,
            last_seen_at_ms: 100,
            updated_at_ms: 100,
        }
    }

    #[tokio::test]
    async fn federated_certificate_rotation_requires_staging_and_has_bounded_overlap() {
        let dir = tempdir().unwrap();
        let store = TaskStore::new(RocksDbStore::open(dir.path()).unwrap());
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command(Command::UpsertFederatedCluster {
                cluster: federated_cluster("rotation-peer"),
                request_id: "register-rotation-peer".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );

        let invalid_grace = sm
            .apply_command(Command::StageFederatedCertificateRotation {
                cluster_id: "rotation-peer".into(),
                current_fingerprint: "a".repeat(64),
                new_fingerprint: "b".repeat(64),
                at_ms: 1_000,
                grace_period_ms: 1_000,
                request_id: "stage-rotation-invalid-grace".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap();
        assert!(!invalid_grace.success);

        let stage = Command::StageFederatedCertificateRotation {
            cluster_id: "rotation-peer".into(),
            current_fingerprint: "a".repeat(64),
            new_fingerprint: "b".repeat(64),
            at_ms: 1_000,
            grace_period_ms: 120_000,
            request_id: "stage-rotation-peer".into(),
            actor: "peer-old-certificate".into(),
        };
        assert!(sm.apply_command(stage.clone()).await.unwrap().success);
        assert!(sm.apply_command(stage).await.unwrap().success);
        let staged = store
            .get_federated_cluster("rotation-peer")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(staged.identity_fingerprint, "a".repeat(64));
        assert_eq!(
            staged.pending_identity_fingerprint.as_deref(),
            Some("b".repeat(64).as_str())
        );
        assert!(staged.accepts_fingerprint(&"a".repeat(64), 2_000));
        assert!(!staged.accepts_fingerprint(&"b".repeat(64), 2_000));

        let rejected = sm
            .apply_command(Command::StageFederatedCertificateRotation {
                cluster_id: "rotation-peer".into(),
                current_fingerprint: "c".repeat(64),
                new_fingerprint: "d".repeat(64),
                at_ms: 2_000,
                grace_period_ms: 120_000,
                request_id: "stage-with-untrusted-current".into(),
                actor: "attacker".into(),
            })
            .await
            .unwrap();
        assert!(!rejected.success);

        let complete = Command::CompleteFederatedCertificateRotation {
            cluster_id: "rotation-peer".into(),
            new_fingerprint: "b".repeat(64),
            at_ms: 3_000,
            request_id: "complete-rotation-peer".into(),
            actor: "peer-new-certificate-proof".into(),
        };
        assert!(sm.apply_command(complete.clone()).await.unwrap().success);
        assert!(sm.apply_command(complete).await.unwrap().success);
        let active = store
            .get_federated_cluster("rotation-peer")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.identity_fingerprint, "b".repeat(64));
        assert_eq!(
            active.previous_identity_fingerprint.as_deref(),
            Some("a".repeat(64).as_str())
        );
        assert_eq!(active.previous_identity_expires_at_ms, Some(123_000));
        assert!(active.pending_identity_fingerprint.is_none());
        assert!(active.accepts_fingerprint(&"a".repeat(64), 122_999));
        assert!(!active.accepts_fingerprint(&"a".repeat(64), 123_000));
        assert!(active.accepts_fingerprint(&"b".repeat(64), 123_000));
    }

    #[tokio::test]
    async fn staged_trust_bundle_can_roll_back_before_activation_but_not_after() {
        let dir = tempdir().unwrap();
        let store = TaskStore::new(RocksDbStore::open(dir.path()).unwrap());
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let mut peer = federated_cluster("ca-abort-peer");
        let old_pem =
            include_str!("../../tests/fixtures/federation-trust-bundle/old-ca.pem").to_string();
        let new_pem =
            include_str!("../../tests/fixtures/federation-trust-bundle/new-ca.pem").to_string();
        let old_fp = crate::federation::validate_trust_bundle(&old_pem).unwrap();
        let new_fp = crate::federation::validate_trust_bundle(&new_pem).unwrap();
        peer.trust_bundle_pem = Some(old_pem);
        peer.trust_bundle_fingerprint = Some(old_fp.clone());
        assert!(
            sm.apply_command(Command::UpsertFederatedCluster {
                cluster: peer,
                request_id: "register-ca-abort-peer".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::StageFederatedTrustBundle {
                cluster_id: "ca-abort-peer".into(),
                current_fingerprint: old_fp.clone(),
                bundle_pem: new_pem.clone(),
                bundle_fingerprint: new_fp.clone(),
                at_ms: 1_000,
                request_id: "stage-ca-to-abort".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::AbortFederatedTrustBundle {
                cluster_id: "ca-abort-peer".into(),
                bundle_fingerprint: new_fp.clone(),
                at_ms: 2_000,
                request_id: "abort-staged-ca".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        let rolled_back = store
            .get_federated_cluster("ca-abort-peer")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            rolled_back.trust_bundle_fingerprint.as_deref(),
            Some(old_fp.as_str())
        );
        assert!(rolled_back.pending_trust_bundle_pem.is_none());

        assert!(
            sm.apply_command(Command::StageFederatedTrustBundle {
                cluster_id: "ca-abort-peer".into(),
                current_fingerprint: old_fp,
                bundle_pem: new_pem,
                bundle_fingerprint: new_fp.clone(),
                at_ms: 3_000,
                request_id: "stage-ca-to-activate".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::ActivateFederatedTrustBundle {
                cluster_id: "ca-abort-peer".into(),
                bundle_fingerprint: new_fp.clone(),
                at_ms: 4_000,
                overlap_ms: 60_000,
                request_id: "activate-ca-before-abort-attempt".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        let rejected = sm
            .apply_command(Command::AbortFederatedTrustBundle {
                cluster_id: "ca-abort-peer".into(),
                bundle_fingerprint: new_fp,
                at_ms: 5_000,
                request_id: "reject-abort-after-activation".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap();
        assert!(!rejected.success);
    }

    #[tokio::test]
    async fn federated_leaf_rotation_abort_and_rollback_preserve_the_correct_identity() {
        let dir = tempdir().unwrap();
        let store = TaskStore::new(RocksDbStore::open(dir.path()).unwrap());
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let mut peer = federated_cluster("leaf-rollback-peer");
        let old = peer.identity_fingerprint.clone();
        let new = "f".repeat(64);
        assert!(
            sm.apply_command(Command::UpsertFederatedCluster {
                cluster: peer.clone(),
                request_id: "register-leaf-rollback-peer".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::StageFederatedCertificateRotation {
                cluster_id: peer.cluster_id.clone(),
                current_fingerprint: old.clone(),
                new_fingerprint: new.clone(),
                at_ms: 1_000,
                grace_period_ms: 60_000,
                request_id: "stage-leaf-to-abort".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::AbortFederatedCertificateRotation {
                cluster_id: peer.cluster_id.clone(),
                new_fingerprint: new.clone(),
                at_ms: 2_000,
                request_id: "abort-pending-leaf".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        peer = store
            .get_federated_cluster(&peer.cluster_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(peer.identity_fingerprint, old);
        assert!(peer.pending_identity_fingerprint.is_none());

        assert!(
            sm.apply_command(Command::StageFederatedCertificateRotation {
                cluster_id: peer.cluster_id.clone(),
                current_fingerprint: old.clone(),
                new_fingerprint: new.clone(),
                at_ms: 3_000,
                grace_period_ms: 60_000,
                request_id: "stage-leaf-to-complete".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::CompleteFederatedCertificateRotation {
                cluster_id: peer.cluster_id.clone(),
                new_fingerprint: new.clone(),
                at_ms: 4_000,
                request_id: "complete-leaf-before-rollback".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::RollbackFederatedCertificateRotation {
                cluster_id: peer.cluster_id.clone(),
                new_fingerprint: new.clone(),
                at_ms: 5_000,
                request_id: "rollback-completed-leaf".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        let restored = store
            .get_federated_cluster(&peer.cluster_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.identity_fingerprint, old);
        assert!(restored.previous_identity_fingerprint.is_none());
        assert!(restored.pending_identity_fingerprint.is_none());
    }

    #[tokio::test]
    async fn federated_trust_rotation_barrier_recovery_and_retirement_are_raft_persisted() {
        use crate::federation::{
            FederatedTrustRotation, FederatedTrustRotationEvent, FederatedTrustRotationPhase,
        };

        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let mut peer = federated_cluster("coordinator-peer");
        let current_peer_ca =
            include_str!("../../tests/fixtures/federation-test-ca.pem").to_string();
        let current_peer_fingerprint =
            crate::federation::validate_trust_bundle(&current_peer_ca).unwrap();
        peer.trust_bundle_pem = Some(current_peer_ca);
        peer.trust_bundle_fingerprint = Some(current_peer_fingerprint.clone());
        assert!(
            sm.apply_command(Command::UpsertFederatedCluster {
                cluster: peer,
                request_id: "register-coordinator-peer".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );

        let local_ca = include_str!("../../tests/fixtures/federation-rotation/ca.pem").to_string();
        let peer_ca = include_str!("../../tests/fixtures/federation-rotation/ca.pem").to_string();
        let local_fingerprint = crate::federation::validate_trust_bundle(&local_ca).unwrap();
        let peer_fingerprint = crate::federation::validate_trust_bundle(&peer_ca).unwrap();
        let rotation = FederatedTrustRotation {
            rotation_id: "rotation-coordinator-e2e".into(),
            peer_cluster_id: "coordinator-peer".into(),
            current_peer_ca_fingerprint: Some(current_peer_fingerprint.clone()),
            local_ca_bundle_pem: local_ca,
            local_ca_bundle_fingerprint: local_fingerprint.clone(),
            peer_ca_bundle_pem: peer_ca,
            peer_ca_bundle_fingerprint: peer_fingerprint.clone(),
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
            created_at_ms: 1_000,
            updated_at_ms: 1_000,
        };
        let mut conflicting_rotation = rotation.clone();
        conflicting_rotation.rotation_id = "rotation-coordinator-conflict".into();
        let create = Command::CreateFederatedTrustRotation {
            rotation: Box::new(rotation),
            at_ms: 1_000,
            request_id: "create-bilateral-rotation".into(),
            actor: "operator".into(),
        };
        assert!(sm.apply_command(create.clone()).await.unwrap().success);
        assert!(sm.apply_command(create).await.unwrap().success);
        let concurrent_rotation = sm
            .apply_command(Command::CreateFederatedTrustRotation {
                rotation: Box::new(conflicting_rotation),
                at_ms: 1_001,
                request_id: "create-conflicting-bilateral-rotation".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap();
        assert!(
            !concurrent_rotation.success,
            "a second Rotation ID must not concurrently control the same peer"
        );

        assert!(
            sm.apply_command(Command::StageFederatedTrustBundle {
                cluster_id: "coordinator-peer".into(),
                current_fingerprint: current_peer_fingerprint.clone(),
                bundle_pem: include_str!("../../tests/fixtures/federation-rotation/ca.pem").into(),
                bundle_fingerprint: peer_fingerprint.clone(),
                at_ms: 1_900,
                request_id: "stage-coordinator-peer-ca".into(),
                actor: "rotation-controller".into(),
            })
            .await
            .unwrap()
            .success
        );
        let wrong_leaf_stage = sm
            .apply_command(Command::StageFederatedCertificateRotation {
                cluster_id: "coordinator-peer".into(),
                current_fingerprint: "a".repeat(64),
                new_fingerprint: "f".repeat(64),
                at_ms: 1_899,
                grace_period_ms: 60_000,
                request_id: "reject-wrong-bilateral-leaf".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap();
        assert!(
            !wrong_leaf_stage.success,
            "a bilateral rotation must pin the expected peer leaf fingerprint"
        );
        assert!(
            sm.apply_command(Command::StageFederatedCertificateRotation {
                cluster_id: "coordinator-peer".into(),
                current_fingerprint: "a".repeat(64),
                new_fingerprint: "e".repeat(64),
                at_ms: 1_900,
                grace_period_ms: 60_000,
                request_id: "stage-coordinator-peer-leaf".into(),
                actor: "rotation-controller".into(),
            })
            .await
            .unwrap()
            .success
        );
        let direct_ca_abort = sm
            .apply_command(Command::AbortFederatedTrustBundle {
                cluster_id: "coordinator-peer".into(),
                bundle_fingerprint: peer_fingerprint.clone(),
                at_ms: 1_950,
                request_id: "reject-uncoordinated-ca-abort".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap();
        assert!(
            !direct_ca_abort.success,
            "CA abort must require bilateral RollingBack"
        );
        let direct_leaf_abort = sm
            .apply_command(Command::AbortFederatedCertificateRotation {
                cluster_id: "coordinator-peer".into(),
                new_fingerprint: "e".repeat(64),
                at_ms: 1_950,
                request_id: "reject-uncoordinated-leaf-abort".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap();
        assert!(
            !direct_leaf_abort.success,
            "leaf abort must require bilateral RollingBack"
        );

        let revision = std::cell::Cell::new(0_u64);
        let advance = |event, request_id: &str, at_ms| {
            let expected_revision = revision.get();
            let command = Command::AdvanceFederatedTrustRotation {
                rotation_id: "rotation-coordinator-e2e".into(),
                expected_revision,
                event,
                at_ms,
                request_id: request_id.into(),
                actor: "rotation-controller".into(),
            };
            revision.set(expected_revision + 1);
            command
        };
        let events = [
            (
                FederatedTrustRotationEvent::LocalStaged {
                    fingerprint: peer_fingerprint.clone(),
                },
                "local-staged",
                2_000,
            ),
            (
                FederatedTrustRotationEvent::PeerStaged {
                    fingerprint: local_fingerprint.clone(),
                },
                "peer-staged",
                2_100,
            ),
            (
                FederatedTrustRotationEvent::LocalDeploymentReady {
                    leaf_fingerprint: "d".repeat(64),
                },
                "local-ready",
                3_000,
            ),
            (
                FederatedTrustRotationEvent::PeerDeploymentReady {
                    leaf_fingerprint: "e".repeat(64),
                },
                "peer-ready",
                3_100,
            ),
            (
                FederatedTrustRotationEvent::AuthorizeActivation,
                "activation-barrier",
                4_000,
            ),
            (
                FederatedTrustRotationEvent::LocalActivated {
                    previous_expires_at_ms: 65_000,
                },
                "local-activated",
                5_000,
            ),
            (
                FederatedTrustRotationEvent::MarkRecoveryRequired {
                    reason: "peer activation acknowledgement lost".into(),
                },
                "forward-recovery",
                5_500,
            ),
            (
                FederatedTrustRotationEvent::PeerActivated {
                    previous_expires_at_ms: 66_000,
                },
                "peer-activated",
                6_000,
            ),
        ];
        for (event, request, at_ms) in events {
            if request == "activation-barrier" {
                let premature = sm
                    .apply_command(Command::ActivateFederatedTrustBundle {
                        cluster_id: "coordinator-peer".into(),
                        bundle_fingerprint: peer_fingerprint.clone(),
                        at_ms: 4_000,
                        overlap_ms: 60_000,
                        request_id: "reject-ca-activation-before-barrier".into(),
                        actor: "rotation-controller".into(),
                    })
                    .await
                    .unwrap();
                assert!(!premature.success);
            }
            if request == "local-activated" {
                let activated_ca = sm
                    .apply_command(Command::ActivateFederatedTrustBundle {
                        cluster_id: "coordinator-peer".into(),
                        bundle_fingerprint: peer_fingerprint.clone(),
                        at_ms: 5_000,
                        overlap_ms: 60_000,
                        request_id: "activate-ca-after-barrier".into(),
                        actor: "rotation-controller".into(),
                    })
                    .await
                    .unwrap();
                assert!(activated_ca.success, "{}", activated_ca.message);
                let rollback_race = sm
                    .apply_command(Command::AdvanceFederatedTrustRotation {
                        rotation_id: "rotation-coordinator-e2e".into(),
                        expected_revision: revision.get(),
                        event: FederatedTrustRotationEvent::BeginRollback {
                            reason: "rollback races activation".into(),
                        },
                        at_ms: 5_001,
                        request_id: "reject-rollback-after-ca-commit".into(),
                        actor: "rotation-controller".into(),
                    })
                    .await
                    .unwrap();
                assert!(!rollback_race.success);
            }
            let response = sm
                .apply_command(advance(event, request, at_ms))
                .await
                .unwrap();
            assert!(response.success, "{}", response.message);
            if request == "local-activated" {
                let changed_expiry = sm
                    .apply_command(Command::AdvanceFederatedTrustRotation {
                        rotation_id: "rotation-coordinator-e2e".into(),
                        expected_revision: revision.get(),
                        event: FederatedTrustRotationEvent::LocalActivated {
                            previous_expires_at_ms: 66_000,
                        },
                        at_ms: 6_000,
                        request_id: "reject-local-expiry-change".into(),
                        actor: "rotation-controller".into(),
                    })
                    .await
                    .unwrap();
                assert!(
                    !changed_expiry.success,
                    "a replay must not replace the first committed local expiry"
                );
                let direct_abort_after_activate = sm
                    .apply_command(Command::AbortFederatedCertificateRotation {
                        cluster_id: "coordinator-peer".into(),
                        new_fingerprint: "e".repeat(64),
                        at_ms: 5_001,
                        request_id: "reject-leaf-abort-after-activate".into(),
                        actor: "operator".into(),
                    })
                    .await
                    .unwrap();
                assert!(
                    !direct_abort_after_activate.success,
                    "a committed Activate must permanently close the abort path"
                );
                let completed_leaf = sm
                    .apply_command(Command::CompleteFederatedCertificateRotation {
                        cluster_id: "coordinator-peer".into(),
                        new_fingerprint: "e".repeat(64),
                        at_ms: 5_001,
                        request_id: "complete-leaf-after-activate".into(),
                        actor: "rotation-controller".into(),
                    })
                    .await
                    .unwrap();
                assert!(completed_leaf.success, "{}", completed_leaf.message);
                let rollback_leaf_after_activate = sm
                    .apply_command(Command::RollbackFederatedCertificateRotation {
                        cluster_id: "coordinator-peer".into(),
                        new_fingerprint: "e".repeat(64),
                        at_ms: 5_002,
                        request_id: "reject-leaf-rollback-after-activate".into(),
                        actor: "operator".into(),
                    })
                    .await
                    .unwrap();
                assert!(
                    !rollback_leaf_after_activate.success,
                    "a committed Activate must permanently close the leaf rollback path"
                );
            }
        }
        // A lost acknowledgement for an early event may be replayed after later
        // events have advanced the revision; request identity remains idempotent.
        let replay = sm
            .apply_command(Command::AdvanceFederatedTrustRotation {
                rotation_id: "rotation-coordinator-e2e".into(),
                expected_revision: 0,
                event: FederatedTrustRotationEvent::LocalStaged {
                    fingerprint: peer_fingerprint.clone(),
                },
                at_ms: 2_000,
                request_id: "local-staged".into(),
                actor: "rotation-controller".into(),
            })
            .await
            .unwrap();
        assert!(replay.success, "{}", replay.message);
        let barrier_replay = sm
            .apply_command(Command::AdvanceFederatedTrustRotation {
                rotation_id: "rotation-coordinator-e2e".into(),
                expected_revision: revision.get(),
                event: FederatedTrustRotationEvent::AuthorizeActivation,
                at_ms: 6_100,
                request_id: "activation-barrier-replay".into(),
                actor: "rotation-controller".into(),
            })
            .await
            .unwrap();
        assert!(barrier_replay.success, "{}", barrier_replay.message);
        revision.set(revision.get() + 1);
        let state = store
            .get_federated_trust_rotation("rotation-coordinator-e2e")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.phase, FederatedTrustRotationPhase::Overlap);
        assert_eq!(state.local_activation_expires_at_ms, Some(65_000));
        assert_eq!(state.peer_activation_expires_at_ms, Some(66_000));
        let mark_recovery = sm
            .apply_command(Command::AdvanceFederatedTrustRotation {
                rotation_id: "rotation-coordinator-e2e".into(),
                expected_revision: revision.get(),
                event: FederatedTrustRotationEvent::MarkRecoveryRequired {
                    reason: "reconcile lost final completion acknowledgement".into(),
                },
                at_ms: 6_200,
                request_id: "mark-recovery-after-both-activated".into(),
                actor: "rotation-controller".into(),
            })
            .await
            .unwrap();
        assert!(mark_recovery.success, "{}", mark_recovery.message);
        revision.set(revision.get() + 1);
        let resume_forward = sm
            .apply_command(Command::AdvanceFederatedTrustRotation {
                rotation_id: "rotation-coordinator-e2e".into(),
                expected_revision: revision.get(),
                event: FederatedTrustRotationEvent::AuthorizeActivation,
                at_ms: 6_300,
                request_id: "resume-forward-recovery".into(),
                actor: "rotation-controller".into(),
            })
            .await
            .unwrap();
        assert!(resume_forward.success, "{}", resume_forward.message);
        revision.set(revision.get() + 1);
        let recovered_state = store
            .get_federated_trust_rotation("rotation-coordinator-e2e")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered_state.phase, FederatedTrustRotationPhase::Overlap);
        let rollback_after_barrier = sm
            .apply_command(Command::AdvanceFederatedTrustRotation {
                rotation_id: "rotation-coordinator-e2e".into(),
                expected_revision: revision.get(),
                event: FederatedTrustRotationEvent::BeginRollback {
                    reason: "unsafe rollback".into(),
                },
                at_ms: 6_100,
                request_id: "rollback-after-activation-barrier".into(),
                actor: "rotation-controller".into(),
            })
            .await
            .unwrap();
        assert!(!rollback_after_barrier.success);
        let too_early = sm
            .apply_command(Command::AdvanceFederatedTrustRotation {
                rotation_id: "rotation-coordinator-e2e".into(),
                expected_revision: revision.get(),
                event: FederatedTrustRotationEvent::LocalRetired,
                at_ms: 65_999,
                request_id: "retire-too-early".into(),
                actor: "rotation-controller".into(),
            })
            .await
            .unwrap();
        assert!(!too_early.success);

        drop(sm);
        drop(store);
        let reopened = TaskStore::new(RocksDbStore::open(dir.path()).unwrap());
        let recovered = reopened
            .get_federated_trust_rotation("rotation-coordinator-e2e")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.phase, FederatedTrustRotationPhase::Overlap);
        assert_eq!(recovered.revision, revision.get());
        assert_eq!(recovered.local_activation_expires_at_ms, Some(65_000));
        assert_eq!(recovered.peer_activation_expires_at_ms, Some(66_000));
        let mut sm = MetaStateMachine::new(reopened.kv_store().clone());
        let premature_ca_retire = sm
            .apply_command(Command::RetireFederatedTrustBundle {
                cluster_id: "coordinator-peer".into(),
                at_ms: 65_999,
                request_id: "retire-peer-ca-too-early".into(),
                actor: "rotation-controller".into(),
            })
            .await
            .unwrap();
        assert!(!premature_ca_retire.success);
        let ca_retired = sm
            .apply_command(Command::RetireFederatedTrustBundle {
                cluster_id: "coordinator-peer".into(),
                at_ms: 66_000,
                request_id: "retire-peer-ca-after-both-deadlines".into(),
                actor: "rotation-controller".into(),
            })
            .await
            .unwrap();
        assert!(ca_retired.success, "{}", ca_retired.message);
        let peer_after_retire = reopened
            .get_federated_cluster("coordinator-peer")
            .await
            .unwrap()
            .unwrap();
        assert!(peer_after_retire.previous_trust_bundle_pem.is_none());
        let retire_local = Command::AdvanceFederatedTrustRotation {
            rotation_id: "rotation-coordinator-e2e".into(),
            expected_revision: revision.get(),
            event: FederatedTrustRotationEvent::LocalRetired,
            at_ms: 66_000,
            request_id: "retire-local-after-overlap".into(),
            actor: "rotation-controller".into(),
        };
        assert!(sm.apply_command(retire_local).await.unwrap().success);
        let retire_peer = Command::AdvanceFederatedTrustRotation {
            rotation_id: "rotation-coordinator-e2e".into(),
            expected_revision: revision.get() + 1,
            event: FederatedTrustRotationEvent::PeerRetired,
            at_ms: 66_000,
            request_id: "retire-peer-after-overlap".into(),
            actor: "rotation-controller".into(),
        };
        assert!(sm.apply_command(retire_peer).await.unwrap().success);
        let completed = reopened
            .get_federated_trust_rotation("rotation-coordinator-e2e")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.phase, FederatedTrustRotationPhase::Completed);
    }

    #[tokio::test]
    async fn federated_ca_bundle_rotation_is_raft_persisted_and_expires_exactly() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command(Command::UpsertFederatedCluster {
                cluster: federated_cluster("ca-peer"),
                request_id: "register-ca-peer".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        let old_pem = include_str!("../../tests/fixtures/federation-test-ca.pem").to_string();
        let new_pem = include_str!("../../tests/fixtures/federation-rotation/ca.pem").to_string();
        let old_fp = crate::federation::validate_trust_bundle(&old_pem).unwrap();
        let new_fp = crate::federation::validate_trust_bundle(&new_pem).unwrap();
        let stage = Command::StageFederatedTrustBundle {
            cluster_id: "ca-peer".into(),
            current_fingerprint: String::new(),
            bundle_pem: old_pem.clone(),
            bundle_fingerprint: old_fp.clone(),
            at_ms: 1_000,
            request_id: "stage-initial-ca".into(),
            actor: "operator".into(),
        };
        assert!(sm.apply_command(stage.clone()).await.unwrap().success);
        assert!(sm.apply_command(stage).await.unwrap().success);
        assert!(
            sm.apply_command(Command::ActivateFederatedTrustBundle {
                cluster_id: "ca-peer".into(),
                bundle_fingerprint: old_fp.clone(),
                at_ms: 2_000,
                overlap_ms: 120_000,
                request_id: "activate-initial-ca".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        let stage_new = Command::StageFederatedTrustBundle {
            cluster_id: "ca-peer".into(),
            current_fingerprint: old_fp.clone(),
            bundle_pem: new_pem.clone(),
            bundle_fingerprint: new_fp.clone(),
            at_ms: 3_000,
            request_id: "stage-next-ca".into(),
            actor: "operator".into(),
        };
        assert!(sm.apply_command(stage_new.clone()).await.unwrap().success);
        let staged = store
            .get_federated_cluster("ca-peer")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            staged.trust_bundle_fingerprint.as_deref(),
            Some(old_fp.as_str())
        );
        assert!(
            staged
                .effective_trust_bundle(4_000)
                .unwrap()
                .contains("BEGIN CERTIFICATE")
        );
        assert!(sm.apply_command(stage_new.clone()).await.unwrap().success);
        assert!(
            sm.apply_command(Command::ActivateFederatedTrustBundle {
                cluster_id: "ca-peer".into(),
                bundle_fingerprint: new_fp.clone(),
                at_ms: 10_000,
                overlap_ms: 120_000,
                request_id: "activate-next-ca".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        let active = store
            .get_federated_cluster("ca-peer")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            active.trust_bundle_fingerprint.as_deref(),
            Some(new_fp.as_str())
        );
        assert_eq!(
            active.previous_trust_bundle_fingerprint.as_deref(),
            Some(old_fp.as_str())
        );
        assert_eq!(active.previous_trust_bundle_expires_at_ms, Some(130_000));
        let overlap = active.effective_trust_bundle(129_999).unwrap();
        assert_eq!(overlap.matches("BEGIN CERTIFICATE").count(), 2);
        assert_eq!(
            active
                .effective_trust_bundle(130_000)
                .unwrap()
                .matches("BEGIN CERTIFICATE")
                .count(),
            1
        );
        // Reopen the RocksDB-backed registry to verify the active/previous CA state
        // and absolute expiry survive a process restart before retirement.
        drop(sm);
        drop(store);
        let reopened_store = TaskStore::new(RocksDbStore::open(dir.path()).unwrap());
        let reopened = reopened_store
            .get_federated_cluster("ca-peer")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            reopened.trust_bundle_fingerprint.as_deref(),
            Some(new_fp.as_str())
        );
        assert_eq!(
            reopened.previous_trust_bundle_fingerprint.as_deref(),
            Some(old_fp.as_str())
        );
        assert_eq!(reopened.previous_trust_bundle_expires_at_ms, Some(130_000));
        let mut sm = MetaStateMachine::new(reopened_store.kv_store().clone());
        let store = reopened_store;
        let retire = Command::RetireFederatedTrustBundle {
            cluster_id: "ca-peer".into(),
            at_ms: 130_000,
            request_id: "retire-old-ca".into(),
            actor: "operator".into(),
        };
        assert!(sm.apply_command(retire.clone()).await.unwrap().success);
        assert!(sm.apply_command(retire).await.unwrap().success);
        let retired = store
            .get_federated_cluster("ca-peer")
            .await
            .unwrap()
            .unwrap();
        assert!(retired.previous_trust_bundle_pem.is_none());
        assert!(
            sm.apply_command(Command::RetireFederatedTrustBundle {
                cluster_id: "ca-peer".into(),
                at_ms: 131_000,
                request_id: "retire-old-ca".into(),
                actor: "operator".into()
            })
            .await
            .unwrap()
            .success
        );
        assert_eq!(
            retired.trust_bundle_fingerprint.as_deref(),
            Some(new_fp.as_str())
        );
        assert!(crate::federation::validate_trust_bundle("not a PEM bundle").is_err());
        assert!(
            crate::federation::validate_trust_bundle(&format!(
                "{}\n{}",
                old_pem,
                include_str!("../../tests/fixtures/federation-rotation/old-client.key.pem")
            ))
            .is_err()
        );
    }

    #[tokio::test]
    async fn federated_registry_is_replicated_idempotent_and_identity_pinned() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let command = Command::UpsertFederatedCluster {
            cluster: federated_cluster("west"),
            request_id: "register-west".into(),
            actor: "operator".into(),
        };
        assert!(
            sm.apply_command_at(command.clone(), 41)
                .await
                .unwrap()
                .success
        );
        assert!(sm.apply_command_at(command, 42).await.unwrap().success);
        assert_eq!(store.list_federated_clusters().await.unwrap().len(), 1);
        let mut changed = federated_cluster("west");
        changed.identity_fingerprint = "b".repeat(64);
        let response = sm
            .apply_command(Command::UpsertFederatedCluster {
                cluster: changed,
                request_id: "rotate-west".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap();
        assert!(!response.success);
        let heartbeat = Command::HeartbeatFederatedCluster {
            cluster_id: "west".into(),
            identity_fingerprint: "a".repeat(64),
            status: crate::federation::FederatedClusterStatus::Degraded,
            last_seen_at_ms: 101,
            request_id: "west-heartbeat-1".into(),
            actor: "peer".into(),
        };
        assert!(
            sm.apply_command_at(heartbeat.clone(), 43)
                .await
                .unwrap()
                .success
        );
        assert!(sm.apply_command_at(heartbeat, 44).await.unwrap().success);
        let saved = store.get_federated_cluster("west").await.unwrap().unwrap();
        assert_eq!(
            saved.status,
            crate::federation::FederatedClusterStatus::Degraded
        );
        let stale = Command::HeartbeatFederatedCluster {
            cluster_id: "west".into(),
            identity_fingerprint: "a".repeat(64),
            status: crate::federation::FederatedClusterStatus::Healthy,
            last_seen_at_ms: 100,
            request_id: "west-heartbeat-stale".into(),
            actor: "peer".into(),
        };
        assert!(!sm.apply_command(stale).await.unwrap().success);
        assert_eq!(store.list_audit_events(10).await.unwrap().len(), 2);
        let snapshot = store.export_snapshot().await.unwrap();
        let restored_dir = tempdir().unwrap();
        let restored = TaskStore::new(RocksDbStore::open(restored_dir.path()).unwrap());
        restored.replace_snapshot(snapshot).await.unwrap();
        assert_eq!(
            restored.list_federated_clusters().await.unwrap(),
            store.list_federated_clusters().await.unwrap()
        );
    }

    #[tokio::test]
    async fn federated_migration_state_machine_fences_stale_reports_and_retries() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        for id in ["source", "target"] {
            assert!(
                sm.apply_command(Command::UpsertFederatedCluster {
                    cluster: federated_cluster(id),
                    request_id: format!("register-{id}"),
                    actor: "operator".into()
                })
                .await
                .unwrap()
                .success
            );
        }
        let migration = crate::federation::FederatedMigration {
            migration_id: "migration-1".into(),
            source_cluster: "source".into(),
            target_cluster: "target".into(),
            task_id: "task-1".into(),
            required_capabilities: ["elasticsearch-source".to_string()].into_iter().collect(),
            state: crate::federation::MigrationState::Planned,
            generation: 0,
            created_at_ms: 1,
            updated_at_ms: 1,
            last_error: None,
            task_name: "migration-1".into(),
            source_config: "{}".into(),
            sink_config: "{}".into(),
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
            sm.apply_command(Command::CreateFederatedMigration {
                migration: Box::new(migration),
                request_id: "create-migration-1".into(),
                actor: "operator".into()
            })
            .await
            .unwrap()
            .success
        );
        let transition = |next_state, expected_generation, at_ms, request_id: &str| {
            Command::TransitionFederatedMigration {
                migration_id: "migration-1".into(),
                next_state,
                expected_generation,
                at_ms,
                error: None,
                request_id: request_id.into(),
                actor: "operator".into(),
            }
        };
        assert!(
            sm.apply_command(transition(
                crate::federation::MigrationState::Preparing,
                0,
                2,
                "stage-prepare"
            ))
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(transition(
                crate::federation::MigrationState::Running,
                0,
                3,
                "stage-running"
            ))
            .await
            .unwrap()
            .success
        );
        let premature_commit = sm
            .apply_command(transition(
                crate::federation::MigrationState::Committing,
                0,
                4,
                "premature-commit",
            ))
            .await
            .unwrap();
        assert!(
            !premature_commit.success,
            "commit requires target-side completion proof"
        );
        assert!(
            sm.apply_command(Command::UpdateFederatedMigrationProgress {
                migration_id: "migration-1".into(),
                expected_generation: 0,
                remote_task_id: Some("remote-task-g0".into()),
                remote_task_state: Some("failed".into()),
                remote_checkpoint: Some(serde_json::json!({"shards": []})),
                at_ms: 4,
                request_id: "progress-g0".into(),
                actor: "reconciler".into(),
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(transition(
                crate::federation::MigrationState::Failed,
                0,
                4,
                "stage-failed"
            ))
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(transition(
                crate::federation::MigrationState::Preparing,
                0,
                5,
                "retry-migration"
            ))
            .await
            .unwrap()
            .success
        );
        assert!(
            !sm.apply_command(Command::UpdateFederatedMigrationProgress {
                migration_id: "migration-1".into(),
                expected_generation: 0,
                remote_task_id: Some("stale-remote-task".into()),
                remote_task_state: Some("completed".into()),
                remote_checkpoint: None,
                at_ms: 6,
                request_id: "stale-progress".into(),
                actor: "old-leader".into(),
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::UpdateFederatedMigrationProgress {
                migration_id: "migration-1".into(),
                expected_generation: 1,
                remote_task_id: Some("remote-task-g1".into()),
                remote_task_state: Some("completed".into()),
                remote_checkpoint: Some(serde_json::json!({"shards": [{"id": 1}], "federation_commit_state": "prepared"})),
                at_ms: 7,
                request_id: "progress-g1".into(),
                actor: "reconciler".into(),
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(transition(
                crate::federation::MigrationState::Running,
                1,
                8,
                "running-g1"
            ))
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(transition(
                crate::federation::MigrationState::Committing,
                1,
                8,
                "commit-g1"
            ))
            .await
            .unwrap()
            .success
        );
        assert!(sm.apply_command(Command::UpdateFederatedMigrationProgress {
            migration_id: "migration-1".into(), expected_generation: 1,
            remote_task_id: Some("remote-task-g1".into()), remote_task_state: Some("completed".into()),
            remote_checkpoint: Some(serde_json::json!({"shards": [{"id": 1}], "federation_commit_state": "committed"})),
            at_ms: 9, request_id: "progress-committed-g1".into(), actor: "reconciler".into(),
        }).await.unwrap().success);
        assert!(
            sm.apply_command(transition(
                crate::federation::MigrationState::Completed,
                1,
                9,
                "complete-g1"
            ))
            .await
            .unwrap()
            .success
        );
        assert!(
            !sm.apply_command(transition(
                crate::federation::MigrationState::Running,
                0,
                6,
                "stale-running-report"
            ))
            .await
            .unwrap()
            .success
        );
        assert!(
            !sm.apply_command(transition(
                crate::federation::MigrationState::Running,
                1,
                10,
                "running-generation-1"
            ))
            .await
            .unwrap()
            .success,
            "completed migrations are terminal"
        );
        let saved = store
            .get_federated_migration("migration-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.state, crate::federation::MigrationState::Completed);
        assert_eq!(saved.generation, 1);
    }

    #[tokio::test]
    async fn federated_remote_commit_is_durable_idempotent_and_terminal() {
        let dir = tempdir().unwrap();
        let store = TaskStore::new(RocksDbStore::open(dir.path()).unwrap());
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command(Command::UpsertFederatedCluster {
                cluster: federated_cluster("source"),
                request_id: "register-source-remote-op".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        let operation = |state| crate::federation::FederatedRemoteOperation {
            migration_id: "migration-remote-op".into(),
            source_cluster: "source".into(),
            generation: 3,
            task_id: "remote-task".into(),
            state,
            checkpoint_digest: Some("abc123".into()),
            updated_at_ms: 10,
        };
        let prepared = Command::RecordFederatedRemoteOperation {
            operation: Box::new(operation(
                crate::federation::FederatedRemoteOperationState::Prepared,
            )),
            request_id: "prepare-remote-op".into(),
            actor: "peer".into(),
        };
        assert!(sm.apply_command(prepared.clone()).await.unwrap().success);
        assert!(sm.apply_command(prepared).await.unwrap().success);
        // Simulate the target committing durably while the source leader loses
        // the response. A replacement source leader replays the exact request.
        let commit = Command::RecordFederatedRemoteOperation {
            operation: Box::new(operation(
                crate::federation::FederatedRemoteOperationState::Committed,
            )),
            request_id: "commit-remote-op".into(),
            actor: "peer".into(),
        };
        assert!(sm.apply_command(commit.clone()).await.unwrap().success);
        assert!(sm.apply_command(commit).await.unwrap().success);
        let rollback = sm
            .apply_command(Command::RecordFederatedRemoteOperation {
                operation: Box::new(operation(
                    crate::federation::FederatedRemoteOperationState::Compensated,
                )),
                request_id: "rollback-remote-op".into(),
                actor: "peer".into(),
            })
            .await
            .unwrap();
        assert!(!rollback.success);
        let saved = store
            .get_federated_remote_operation("migration-remote-op", 3)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            saved.state,
            crate::federation::FederatedRemoteOperationState::Committed
        );
    }

    #[tokio::test]
    async fn federated_compensation_fence_excludes_commit_and_is_replayable() {
        let dir = tempdir().unwrap();
        let store = TaskStore::new(RocksDbStore::open(dir.path()).unwrap());
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command(Command::UpsertFederatedCluster {
                cluster: federated_cluster("source"),
                request_id: "register-source-compensation".into(),
                actor: "operator".into(),
            })
            .await
            .unwrap()
            .success
        );
        let operation = |state| crate::federation::FederatedRemoteOperation {
            migration_id: "migration-compensation-fence".into(),
            source_cluster: "source".into(),
            generation: 4,
            task_id: "remote-task".into(),
            state,
            checkpoint_digest: Some("checkpoint-4".into()),
            updated_at_ms: 10,
        };
        let propose = |operation, request_id: &str| Command::RecordFederatedRemoteOperation {
            operation: Box::new(operation),
            request_id: request_id.into(),
            actor: "peer".into(),
        };
        assert!(
            sm.apply_command(propose(
                operation(crate::federation::FederatedRemoteOperationState::Prepared),
                "prepare-fence",
            ))
            .await
            .unwrap()
            .success
        );

        let intent = propose(
            operation(crate::federation::FederatedRemoteOperationState::Compensating),
            "begin-compensation-fence",
        );
        assert!(sm.apply_command(intent.clone()).await.unwrap().success);
        assert!(sm.apply_command(intent).await.unwrap().success);

        let concurrent_commit = sm
            .apply_command(propose(
                operation(crate::federation::FederatedRemoteOperationState::Committed),
                "commit-after-compensation-start",
            ))
            .await
            .unwrap();
        assert!(!concurrent_commit.success);

        assert!(
            sm.apply_command(propose(
                operation(crate::federation::FederatedRemoteOperationState::Compensated),
                "finish-compensation-fence",
            ))
            .await
            .unwrap()
            .success
        );
        let late_commit = sm
            .apply_command(propose(
                operation(crate::federation::FederatedRemoteOperationState::Committed),
                "late-commit-after-compensation",
            ))
            .await
            .unwrap();
        assert!(!late_commit.success);
        assert_eq!(
            store
                .get_federated_remote_operation("migration-compensation-fence", 4)
                .await
                .unwrap()
                .unwrap()
                .state,
            crate::federation::FederatedRemoteOperationState::Compensated
        );

        // Cancellation may arrive before the source ever receives a Prepared
        // acknowledgement. The target must still persist a compensation fence
        // for that generation, and a delayed Prepare/Commit must not resurrect it.
        let mut unprepared =
            operation(crate::federation::FederatedRemoteOperationState::Compensating);
        unprepared.generation = 5;
        assert!(
            sm.apply_command(propose(unprepared, "compensate-before-prepare"))
                .await
                .unwrap()
                .success
        );
        let delayed_prepare = operation(crate::federation::FederatedRemoteOperationState::Prepared);
        let mut delayed_prepare = delayed_prepare;
        delayed_prepare.generation = 5;
        assert!(
            !sm.apply_command(propose(
                delayed_prepare,
                "delayed-prepare-after-compensation"
            ))
            .await
            .unwrap()
            .success
        );
    }

    #[tokio::test]
    async fn durable_batch_manifest_requires_delivery_checkpoint_and_ack_in_order() {
        use crate::pipeline::{OffsetValue, Record};
        use serde_json::json;

        let dir = tempdir().unwrap();
        let store = TaskStore::new(RocksDbStore::open(dir.path()).unwrap());
        let task = TaskMeta::new("durable-batch-manifest".into(), "{}".into(), "{}".into());
        let shard = ShardMeta::new(task.task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command(Command::CreateTask {
                task: Box::new(task),
                shards: vec![shard],
                request_id: "create-durable-batch".into(),
                actor: "test".into(),
            })
            .await
            .unwrap()
            .success
        );

        let claimed_at = chrono::Utc::now().timestamp_millis();
        assert!(
            sm.apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                claim_id: "claim-durable-batch".into(),
                issued_at_ms: claimed_at,
                lease_expires_at_ms: claimed_at + 60_000,
            })
            .await
            .unwrap()
            .success
        );
        let claimed = store.get_shard(&shard_id).await.unwrap().unwrap();
        let fencing_token = claimed.fencing_token;
        let batch_id = format!("{fencing_token:016x}-000000");
        let records = vec![Record::with_id(
            "doc-1",
            "elasticsearch",
            json!({"value": 1}),
        )];
        let next_offset = OffsetValue::DocumentCount(1);
        let cache_batch = Command::CacheBatch {
            shard_id: shard_id.to_string(),
            node_id: "worker-a".into(),
            fencing_token,
            issued_at_ms: claimed_at + 1,
            batch_id: batch_id.clone(),
            records,
            next_offset: Some(next_offset.clone()),
            source_count: 1,
        };
        assert!(sm.apply_command(cache_batch.clone()).await.unwrap().success);
        assert!(sm.apply_command(cache_batch).await.unwrap().success);
        let manifest_key =
            crate::pipeline::cache::BatchManifest::key(&shard_id.to_string(), &batch_id);
        let read_manifest = || async {
            let bytes = store
                .kv_store()
                .get_cf(crate::storage::cf::META, &manifest_key)
                .await
                .unwrap()
                .unwrap();
            serde_json::from_slice::<crate::pipeline::cache::BatchManifest>(&bytes).unwrap()
        };
        assert_eq!(
            read_manifest().await.status,
            crate::pipeline::cache::BatchManifestStatus::Prepared
        );

        let delivered = sm
            .apply_command(Command::RecordBatchDelivery {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token,
                issued_at_ms: claimed_at + 2,
                batch_id: batch_id.clone(),
                success_count: 1,
                failed_count: 0,
                bytes_written: 64,
            })
            .await
            .unwrap();
        assert!(delivered.success, "{}", delivered.message);
        assert_eq!(
            read_manifest().await.status,
            crate::pipeline::cache::BatchManifestStatus::Delivered
        );

        let progress_command = Command::UpdateShardProgress {
            shard_id: shard_id.to_string(),
            node_id: "worker-a".into(),
            fencing_token,
            issued_at_ms: claimed_at + 3,
            processed_records: 1,
            processed_bytes: 64,
            offset: serde_json::to_string(&next_offset).unwrap(),
            batch_id: Some(batch_id.clone()),
        };
        let progress = sm.apply_command(progress_command.clone()).await.unwrap();
        assert!(progress.success, "{}", progress.message);
        let replayed_progress = sm.apply_command(progress_command).await.unwrap();
        assert!(replayed_progress.success, "{}", replayed_progress.message);
        assert_eq!(
            read_manifest().await.status,
            crate::pipeline::cache::BatchManifestStatus::Checkpointed
        );
        assert_eq!(
            store
                .get_shard(&shard_id)
                .await
                .unwrap()
                .unwrap()
                .records_processed,
            1
        );

        let cache_key = String::from_utf8(
            crate::pipeline::cache::LocalBatchCache::<RocksDbStore>::key_for_shard(
                &shard_id.to_string(),
                &batch_id,
            ),
        )
        .unwrap();
        let ack = sm
            .apply_command(Command::AckCacheBatch {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token,
                issued_at_ms: claimed_at + 4,
                cache_key,
            })
            .await
            .unwrap();
        assert!(ack.success, "{}", ack.message);
        assert_eq!(
            read_manifest().await.status,
            crate::pipeline::cache::BatchManifestStatus::Acknowledged
        );
        assert_eq!(
            store
                .cache_batch_count(&shard_id.to_string())
                .await
                .unwrap(),
            0
        );

        assert!(
            sm.apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-b".into(),
                claim_id: "claim-after-recovery".into(),
                issued_at_ms: claimed_at + 60_001,
                lease_expires_at_ms: claimed_at + 120_001,
            })
            .await
            .unwrap()
            .success
        );
        let stale_delivery = sm
            .apply_command(Command::RecordBatchDelivery {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token,
                issued_at_ms: claimed_at + 5,
                batch_id,
                success_count: 1,
                failed_count: 0,
                bytes_written: 64,
            })
            .await
            .unwrap();
        assert!(!stale_delivery.success);
        assert_eq!(
            read_manifest().await.status,
            crate::pipeline::cache::BatchManifestStatus::Acknowledged
        );
    }

    #[tokio::test]
    async fn batch_recovers_after_target_commit_before_delivery_ack_without_double_checkpoint() {
        use crate::pipeline::{OffsetValue, Record};
        use crate::storage::kv::{KvStore, cf};
        use serde_json::json;

        let source_dir = tempdir().unwrap();
        let target_dir = tempdir().unwrap();
        let store = TaskStore::new(RocksDbStore::open(source_dir.path()).unwrap());
        let target = RocksDbStore::open(target_dir.path()).unwrap();
        let task = TaskMeta::new("batch-ack-loss-recovery".into(), "{}".into(), "{}".into());
        let shard = ShardMeta::new(task.task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command(Command::CreateTask {
                task: Box::new(task),
                shards: vec![shard],
                request_id: "create-ack-loss".into(),
                actor: "test".into(),
            })
            .await
            .unwrap()
            .success
        );

        let first_claim_at = chrono::Utc::now().timestamp_millis();
        assert!(
            sm.apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                claim_id: "claim-a".into(),
                issued_at_ms: first_claim_at,
                lease_expires_at_ms: first_claim_at + 30_000,
            })
            .await
            .unwrap()
            .success
        );
        let first_fence = store
            .get_shard(&shard_id)
            .await
            .unwrap()
            .unwrap()
            .fencing_token;
        let batch_id = format!("{first_fence:016x}-000000");
        let records = vec![Record::with_id(
            "doc-1",
            "elasticsearch",
            json!({"value": 42}),
        )];
        let next_offset = OffsetValue::DocumentCount(1);
        assert!(
            sm.apply_command(Command::CacheBatch {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token: first_fence,
                issued_at_ms: first_claim_at + 1,
                batch_id: batch_id.clone(),
                records: records.clone(),
                next_offset: Some(next_offset.clone()),
                source_count: 1,
            })
            .await
            .unwrap()
            .success
        );

        let malformed = sm
            .apply_command(Command::CacheBatch {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token: first_fence,
                issued_at_ms: first_claim_at + 2,
                batch_id: format!("{first_fence:016x}-000001"),
                records: records.clone(),
                next_offset: Some(next_offset.clone()),
                source_count: 2,
            })
            .await
            .unwrap();
        assert!(
            !malformed.success,
            "manifest must reject a source count that differs from the payload"
        );
        assert_eq!(
            store
                .cache_batch_count(&shard_id.to_string())
                .await
                .unwrap(),
            1
        );

        // Simulate Elasticsearch committing the document while the response is lost.
        // The Worker dies before RecordBatchDelivery reaches the source Raft cluster.
        let digest = crate::pipeline::cache::records_payload_digest(&records).unwrap();
        let target_key = b"doc:doc-1";
        target
            .put_cf(
                cf::META,
                target_key,
                &serde_json::to_vec(&json!({
                    "batch_id": batch_id, "payload_digest": digest, "document": records[0].payload,
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let before_restart = store.get_shard(&shard_id).await.unwrap().unwrap();
        assert_eq!(before_restart.records_processed, 0);
        assert_eq!(before_restart.offset, OffsetValue::initial());
        let manifest_key =
            crate::pipeline::cache::BatchManifest::key(&shard_id.to_string(), &batch_id);
        let before_manifest = store
            .kv_store()
            .get_cf(cf::META, &manifest_key)
            .await
            .unwrap()
            .unwrap();
        let before_manifest: crate::pipeline::cache::BatchManifest =
            serde_json::from_slice(&before_manifest).unwrap();
        assert_eq!(
            before_manifest.status,
            crate::pipeline::cache::BatchManifestStatus::Prepared
        );
        drop(sm);

        // A replacement Worker claims the expired lease and replays the same durable batch.
        let mut recovered = MetaStateMachine::new(store.kv_store().clone());
        let second_claim_at = first_claim_at + 30_001;
        assert!(
            recovered
                .apply_command(Command::ClaimShard {
                    shard_id: shard_id.to_string(),
                    node_id: "worker-b".into(),
                    claim_id: "claim-b".into(),
                    issued_at_ms: second_claim_at,
                    lease_expires_at_ms: second_claim_at + 30_000,
                })
                .await
                .unwrap()
                .success
        );
        let second_fence = store
            .get_shard(&shard_id)
            .await
            .unwrap()
            .unwrap()
            .fencing_token;
        assert!(second_fence > first_fence);
        let cached = store
            .kv_store()
            .get_cf(
                cf::CACHE,
                &crate::pipeline::cache::LocalBatchCache::<RocksDbStore>::key_for_shard(
                    &shard_id.to_string(),
                    &batch_id,
                ),
            )
            .await
            .unwrap()
            .unwrap();
        let cached: crate::pipeline::cache::CacheEntry = serde_json::from_slice(&cached).unwrap();
        assert_eq!(cached.batch_id, batch_id);
        assert_eq!(cached.records, records);

        // The target's deterministic document identity proves the replay is already applied.
        let target_document = target.get_cf(cf::META, target_key).await.unwrap().unwrap();
        let target_document: serde_json::Value = serde_json::from_slice(&target_document).unwrap();
        assert_eq!(target_document["document"]["value"], 42);
        assert_eq!(target.count_prefix_cf(cf::META, b"doc:").await.unwrap(), 1);

        let delivery = recovered
            .apply_command(Command::RecordBatchDelivery {
                shard_id: shard_id.to_string(),
                node_id: "worker-b".into(),
                fencing_token: second_fence,
                issued_at_ms: second_claim_at + 1,
                batch_id: batch_id.clone(),
                success_count: 1,
                failed_count: 0,
                bytes_written: 64,
            })
            .await
            .unwrap();
        assert!(delivery.success, "{}", delivery.message);
        let progress = Command::UpdateShardProgress {
            shard_id: shard_id.to_string(),
            node_id: "worker-b".into(),
            fencing_token: second_fence,
            issued_at_ms: second_claim_at + 2,
            processed_records: 1,
            processed_bytes: 64,
            offset: serde_json::to_string(&next_offset).unwrap(),
            batch_id: Some(batch_id.clone()),
        };
        assert!(
            recovered
                .apply_command(progress.clone())
                .await
                .unwrap()
                .success
        );
        assert!(recovered.apply_command(progress).await.unwrap().success);
        let checkpointed = store.get_shard(&shard_id).await.unwrap().unwrap();
        assert_eq!(
            checkpointed.records_processed, 1,
            "replayed checkpoint counted the batch twice"
        );
        assert_eq!(checkpointed.offset, next_offset);

        let cache_key = String::from_utf8(
            crate::pipeline::cache::LocalBatchCache::<RocksDbStore>::key_for_shard(
                &shard_id.to_string(),
                &batch_id,
            ),
        )
        .unwrap();
        let ack = recovered
            .apply_command(Command::AckCacheBatch {
                shard_id: shard_id.to_string(),
                node_id: "worker-b".into(),
                fencing_token: second_fence,
                issued_at_ms: second_claim_at + 3,
                cache_key,
            })
            .await
            .unwrap();
        assert!(ack.success, "{}", ack.message);
        assert_eq!(
            store
                .cache_batch_count(&shard_id.to_string())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .get_shard(&shard_id)
                .await
                .unwrap()
                .unwrap()
                .records_processed,
            1
        );
        let manifest_key =
            crate::pipeline::cache::BatchManifest::key(&shard_id.to_string(), &batch_id);
        let manifest = store
            .kv_store()
            .get_cf(cf::META, &manifest_key)
            .await
            .unwrap()
            .unwrap();
        let manifest: crate::pipeline::cache::BatchManifest =
            serde_json::from_slice(&manifest).unwrap();
        assert_eq!(
            manifest.status,
            crate::pipeline::cache::BatchManifestStatus::Acknowledged
        );
        assert_eq!(manifest.fencing_token, second_fence);

        let stale = recovered
            .apply_command(Command::RecordBatchDelivery {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token: first_fence,
                issued_at_ms: first_claim_at + 4,
                batch_id,
                success_count: 1,
                failed_count: 0,
                bytes_written: 64,
            })
            .await
            .unwrap();
        assert!(!stale.success, "old lease must not record a replay result");
        assert_eq!(target.count_prefix_cf(cf::META, b"doc:").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn legacy_cache_import_is_idempotent_and_closed_after_bootstrap() {
        use crate::pipeline::{OffsetValue, Record};
        use serde_json::json;

        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let task = TaskMeta::new("bootstrap-import".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let created = sm
            .apply_command(Command::CreateTask {
                request_id: format!("create:{}", task.task_id),
                actor: "system".into(),
                task: Box::new(task),
                shards: vec![shard],
            })
            .await
            .unwrap();
        assert!(created.success);

        let import = Command::ImportLegacyCache {
            shard_id: shard_id.to_string(),
            batch_id: "0000000000000000-000000".into(),
            records: vec![Record::with_id(
                "legacy-1",
                "elasticsearch",
                json!({"v": 1}),
            )],
            next_offset: Some(OffsetValue::DocumentCount(1)),
            source_count: 1,
        };
        assert!(sm.apply_command(import.clone()).await.unwrap().success);
        assert!(sm.apply_command(import.clone()).await.unwrap().success);
        assert_eq!(
            store
                .cache_batch_count(&shard_id.to_string())
                .await
                .unwrap(),
            1
        );

        assert!(
            sm.apply_command(Command::BootstrapComplete)
                .await
                .unwrap()
                .success
        );
        let mut late_import = import;
        if let Command::ImportLegacyCache { batch_id, .. } = &mut late_import {
            *batch_id = "0000000000000000-000001".into();
        }
        assert!(!sm.apply_command(late_import).await.unwrap().success);
        assert_eq!(
            store
                .cache_batch_count(&shard_id.to_string())
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn node_heartbeat_failure_atomically_requeues_owned_shards_and_fences_old_owner() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let task = TaskMeta::new("node-failure".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        store.save_task(&task).await.unwrap();
        store.save_shard(&shard).await.unwrap();
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let now = chrono::Utc::now().timestamp_millis();

        assert!(
            sm.apply_command(Command::RegisterNode {
                node_id: "worker-a".into(),
                address: "127.0.0.1:9300".into(),
                tags: std::collections::HashMap::new(),
                issued_at_ms: now,
            })
            .await
            .unwrap()
            .success
        );
        let claim = sm
            .apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                claim_id: "claim-a".into(),
                issued_at_ms: now,
                lease_expires_at_ms: now + 60_000,
            })
            .await
            .unwrap();
        assert!(claim.success);

        let heartbeat = sm
            .apply_command(Command::UpdateNodeHeartbeat {
                node_id: "worker-a".into(),
                issued_at_ms: now + 15_000,
            })
            .await
            .unwrap();
        assert!(heartbeat.success);
        let too_early = sm
            .apply_command(Command::MarkNodeOffline {
                node_id: "worker-a".into(),
                issued_at_ms: now + 20_000,
                heartbeat_timeout_ms: 10_000,
            })
            .await
            .unwrap();
        assert!(!too_early.success);

        let offline = sm
            .apply_command(Command::MarkNodeOffline {
                node_id: "worker-a".into(),
                issued_at_ms: now + 26_000,
                heartbeat_timeout_ms: 10_000,
            })
            .await
            .unwrap();
        assert!(offline.success);
        assert_eq!(offline.data.unwrap()["requeued_shards"], 1);
        assert_eq!(
            store
                .get_node(&NodeId::new("worker-a"))
                .await
                .unwrap()
                .unwrap()
                .status,
            crate::types::NodeStatus::Offline
        );
        let requeued = store.get_shard(&shard_id).await.unwrap().unwrap();
        assert!(matches!(requeued.state, ShardState::Pending));
        assert_eq!(
            requeued.fencing_token,
            crate::task::shard::FENCING_TOKEN_BASE + 1
        );

        let offline_claim = sm
            .apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                claim_id: "stale-worker-claim".into(),
                issued_at_ms: now + 27_000,
                lease_expires_at_ms: now + 57_000,
            })
            .await
            .unwrap();
        assert!(!offline_claim.success);

        let reassigned = sm
            .apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-b".into(),
                claim_id: "claim-b".into(),
                issued_at_ms: now + 27_000,
                lease_expires_at_ms: now + 57_000,
            })
            .await
            .unwrap();
        assert!(reassigned.success);
        assert_eq!(
            reassigned.data.unwrap()["fencing_token"],
            crate::task::shard::FENCING_TOKEN_BASE + 2
        );

        let stale_renewal = sm
            .apply_command(Command::RenewShardLease {
                shard_id: shard_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token: crate::task::shard::FENCING_TOKEN_BASE + 1,
                issued_at_ms: now + 28_000,
                lease_expires_at_ms: now + 58_000,
            })
            .await
            .unwrap();
        assert!(!stale_renewal.success);
    }

    #[tokio::test]
    async fn task_aggregate_tracks_progress_completion_failure_and_node_contributions() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let task = TaskMeta::new("distributed-aggregate".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let shard_a = ShardMeta::new(task_id, 0, "{}".into());
        let shard_b = ShardMeta::new(task_id, 1, "{}".into());
        let shard_a_id = shard_a.shard_id;
        let shard_b_id = shard_b.shard_id;
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let now = chrono::Utc::now().timestamp_millis();

        let created = sm
            .apply_command(Command::CreateTask {
                request_id: format!("create:{}", task.task_id),
                actor: "system".into(),
                task: Box::new(task),
                shards: vec![shard_a, shard_b],
            })
            .await
            .unwrap();
        assert!(created.success);

        for (shard_id, node_id) in [(&shard_a_id, "worker-a"), (&shard_b_id, "worker-b")] {
            let claim = sm
                .apply_command(Command::ClaimShard {
                    shard_id: shard_id.to_string(),
                    node_id: node_id.into(),
                    claim_id: format!("claim-{node_id}"),
                    issued_at_ms: now,
                    lease_expires_at_ms: now + 60_000,
                })
                .await
                .unwrap();
            assert!(claim.success, "{}", claim.message);
        }

        let progress = sm
            .apply_command(Command::UpdateShardProgress {
                shard_id: shard_a_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token: crate::task::shard::FENCING_TOKEN_BASE + 1,
                issued_at_ms: now + 1,
                processed_records: 10,
                processed_bytes: 2048,
                offset: serde_json::to_string(&crate::pipeline::OffsetValue::DocumentCount(10))
                    .unwrap(),
                batch_id: None,
            })
            .await
            .unwrap();
        assert!(progress.success);

        let completed = sm
            .apply_command(Command::CompleteShard {
                shard_id: shard_a_id.to_string(),
                node_id: "worker-a".into(),
                fencing_token: crate::task::shard::FENCING_TOKEN_BASE + 1,
                issued_at_ms: now + 2,
                total_records: 10,
            })
            .await
            .unwrap();
        assert!(completed.success);

        let failed_command = Command::FailShard {
            shard_id: shard_b_id.to_string(),
            node_id: "worker-b".into(),
            fencing_token: crate::task::shard::FENCING_TOKEN_BASE + 1,
            issued_at_ms: now + 3,
            error_message: "destination rejected records".into(),
        };
        let failed = sm.apply_command(failed_command.clone()).await.unwrap();
        assert!(failed.success);
        assert!(sm.apply_command(failed_command).await.unwrap().success);

        let aggregate = store
            .get_task_with_aggregate(&task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(aggregate.aggregate.total_shards, 2);
        assert_eq!(aggregate.aggregate.pending_shards, 0);
        assert_eq!(aggregate.aggregate.running_shards, 0);
        assert_eq!(aggregate.aggregate.completed_shards, 1);
        assert_eq!(aggregate.aggregate.failed_shards, 1);
        assert_eq!(aggregate.completed_shards, 1);
        assert_eq!(aggregate.failed_shards, 1);
        assert_eq!(aggregate.aggregate.docs_processed, 10);
        assert_eq!(aggregate.aggregate.bytes_processed, 2048);
        assert!(aggregate.aggregate.throughput_docs_per_sec > 0.0);
        assert!(aggregate.aggregate.throughput_bytes_per_sec > 0.0);
        assert!(aggregate.aggregate.elapsed_ms > 0);
        let duration_ms = aggregate.aggregate.elapsed_ms;
        let rebuilt = store
            .get_task_with_aggregate(&task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rebuilt.aggregate.elapsed_ms, duration_ms);
        assert_eq!(aggregate.aggregate.nodes["worker-a"].completed_shards, 1);
        assert_eq!(aggregate.aggregate.nodes["worker-a"].docs_processed, 10);
        assert_eq!(aggregate.aggregate.nodes["worker-a"].bytes_processed, 2048);
        assert!(aggregate.aggregate.nodes["worker-a"].throughput_docs_per_sec > 0.0);
        assert_eq!(aggregate.aggregate.nodes["worker-b"].failed_shards, 1);
        assert_eq!(aggregate.aggregate.checkpoints.len(), 2);
        assert!(matches!(
            aggregate.state,
            crate::types::TaskState::Failed { .. }
        ));
    }

    #[tokio::test]
    async fn online_shard_completion_requires_committed_cdc_cutover() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let online_source = crate::drivers::factory::SourceConfig::ElasticsearchOnline {
            snapshot: crate::drivers::document::DocumentConfig::local("source-index"),
            change_log_connection: crate::drivers::document::DocumentConfig::local("change-log"),
            cdc: crate::drivers::document::CdcConfig::default(),
        };
        let task = TaskMeta::new(
            "online-cutover-gate".into(),
            serde_json::to_string(&online_source).unwrap(),
            "{}".into(),
        );
        let task_id = task.task_id;
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let now = chrono::Utc::now().timestamp_millis();
        assert!(
            sm.apply_command(Command::CreateTask {
                request_id: format!("create:{task_id}"),
                actor: "test".into(),
                task: Box::new(task),
                shards: vec![shard],
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-cutover".into(),
                claim_id: "claim-cutover".into(),
                issued_at_ms: now,
                lease_expires_at_ms: now + 60_000,
            })
            .await
            .unwrap()
            .success
        );
        let fencing_token = crate::task::shard::FENCING_TOKEN_BASE + 1;
        let complete = || Command::CompleteShard {
            shard_id: shard_id.to_string(),
            node_id: "worker-cutover".into(),
            fencing_token,
            issued_at_ms: now + 1,
            total_records: 0,
        };
        let created_task = store.get_task(&task_id).await.unwrap().unwrap();
        assert_eq!(
            created_task
                .metadata
                .get("_dataspool_cdc_cutover_required")
                .map(String::as_str),
            Some("true")
        );
        let rejected_completion = sm.apply_command(complete()).await.unwrap();
        assert!(
            !rejected_completion.success,
            "online completion must be gated before cutover"
        );
        let recorded = sm
            .apply_command(Command::RecordCdcCutover {
                task_id: task_id.to_string(),
                alias: "live".into(),
                old_index: Some("source-v1".into()),
                new_index: "target-v2".into(),
                final_watermark: 42,
                applied_watermark: 42,
                fencing_shard_id: shard_id.to_string(),
                node_id: "worker-cutover".into(),
                fencing_token,
                issued_at_ms: now + 2,
            })
            .await
            .unwrap();
        assert!(recorded.success, "{}", recorded.message);
        let regressed = sm
            .apply_command(Command::RecordCdcCutover {
                task_id: task_id.to_string(),
                alias: "live".into(),
                old_index: Some("source-v1".into()),
                new_index: "target-v2".into(),
                final_watermark: 41,
                applied_watermark: 41,
                fencing_shard_id: shard_id.to_string(),
                node_id: "worker-cutover".into(),
                fencing_token,
                issued_at_ms: now + 3,
            })
            .await
            .unwrap();
        assert!(
            !regressed.success,
            "Raft must reject cutover watermark regression"
        );
        assert!(sm.apply_command(complete()).await.unwrap().success);
    }

    #[tokio::test]
    async fn partitioned_cdc_cutover_requires_complete_monotonic_vector() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let online_source = crate::drivers::factory::SourceConfig::ElasticsearchOnline {
            snapshot: crate::drivers::document::DocumentConfig::local("source-index"),
            change_log_connection: crate::drivers::document::DocumentConfig::local("change-log"),
            cdc: crate::drivers::document::CdcConfig::default(),
        };
        let task = TaskMeta::new(
            "partitioned-cutover-gate".into(),
            serde_json::to_string(&online_source).unwrap(),
            "{}".into(),
        );
        let task_id = task.task_id;
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let now = chrono::Utc::now().timestamp_millis();
        assert!(
            sm.apply_command(Command::CreateTask {
                request_id: format!("create:{task_id}"),
                actor: "test".into(),
                task: Box::new(task),
                shards: vec![shard],
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-vector".into(),
                claim_id: "claim-vector".into(),
                issued_at_ms: now,
                lease_expires_at_ms: now + 60_000,
            })
            .await
            .unwrap()
            .success
        );
        let fencing_token = crate::task::shard::FENCING_TOKEN_BASE + 1;
        let final_watermarks = [(0, 4), (1, 9)]
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        let behind = [(0, 4), (1, 8)]
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        let make_command =
            |applied_watermarks, issued_at_ms| Command::RecordCdcPartitionedCutover {
                task_id: task_id.to_string(),
                alias: "live".into(),
                old_index: Some("source-v1".into()),
                new_index: "target-v2".into(),
                final_watermarks: final_watermarks.clone(),
                applied_watermarks,
                fencing_shard_id: shard_id.to_string(),
                node_id: "worker-vector".into(),
                fencing_token,
                issued_at_ms,
            };
        let rejected = sm
            .apply_command(make_command(behind, now + 1))
            .await
            .unwrap();
        assert!(
            !rejected.success,
            "every partition must reach its own final watermark"
        );
        let complete_vector = final_watermarks.clone();
        let recorded = sm
            .apply_command(make_command(complete_vector.clone(), now + 2))
            .await
            .unwrap();
        assert!(recorded.success, "{}", recorded.message);
        let regressed = [(0, 3), (1, 9)].into_iter().collect();
        assert!(
            !sm.apply_command(make_command(regressed, now + 3))
                .await
                .unwrap()
                .success
        );
        let completed = sm
            .apply_command(Command::CompleteShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-vector".into(),
                fencing_token,
                issued_at_ms: now + 4,
                total_records: 0,
            })
            .await
            .unwrap();
        assert!(completed.success, "{}", completed.message);
    }

    #[tokio::test]
    async fn stale_fencing_token_cannot_renew_or_complete_reclaimed_shard() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let task = TaskMeta::new("fence-test".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        store.save_task(&task).await.unwrap();
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        store.save_shard(&shard).await.unwrap();
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        let now = chrono::Utc::now().timestamp_millis();

        let first = sm
            .apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "node-a".into(),
                claim_id: "claim-a".into(),
                issued_at_ms: now,
                lease_expires_at_ms: now + 30_000,
            })
            .await
            .unwrap();
        assert!(first.success);
        assert_eq!(
            first.data.as_ref().unwrap()["fencing_token"],
            crate::task::shard::FENCING_TOKEN_BASE + 1
        );

        let reclaimed = sm
            .apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "node-b".into(),
                claim_id: "claim-b".into(),
                issued_at_ms: now + 31_000,
                lease_expires_at_ms: now + 61_000,
            })
            .await
            .unwrap();
        assert!(reclaimed.success);
        assert_eq!(
            reclaimed.data.as_ref().unwrap()["fencing_token"],
            crate::task::shard::FENCING_TOKEN_BASE + 2
        );

        let stale = sm
            .apply_command(Command::CompleteShard {
                shard_id: shard_id.to_string(),
                node_id: "node-a".into(),
                fencing_token: crate::task::shard::FENCING_TOKEN_BASE + 1,
                issued_at_ms: now + 32_000,
                total_records: 100,
            })
            .await
            .unwrap();
        assert!(!stale.success);

        let current = store.get_shard(&shard_id).await.unwrap().unwrap();
        assert_eq!(
            current.fencing_token,
            crate::task::shard::FENCING_TOKEN_BASE + 2
        );
        assert!(
            matches!(current.state, ShardState::Running { ref node_id, .. } if node_id.as_str() == "node-b")
        );
    }

    #[tokio::test]
    async fn duplicate_task_creation_preserves_committed_state_and_shard_identity() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let task = TaskMeta::new("create-idempotency".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let strategy = crate::task::ShardingStrategy::ByHash {
            field: "id".into(),
            num_shards: 2,
        };
        let shards = crate::task::ShardGenerator::generate(task_id, &strategy, "{}");
        let shard_id = shards[0].shard_id;
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command(Command::CreateTask {
                request_id: format!("create:{}", task.task_id),
                actor: "system".into(),
                task: Box::new(task.clone()),
                shards: shards.clone()
            })
            .await
            .unwrap()
            .success
        );
        let now = chrono::Utc::now().timestamp_millis();
        assert!(
            sm.apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "worker-create".into(),
                claim_id: "create-claim".into(),
                issued_at_ms: now,
                lease_expires_at_ms: now + 30_000
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::CreateTask {
                request_id: format!("create:{}", task.task_id),
                actor: "system".into(),
                task: Box::new(task),
                shards
            })
            .await
            .unwrap()
            .success
        );
        let mut conflicting_task = store.get_task(&task_id).await.unwrap().unwrap();
        conflicting_task.name = "different-payload".into();
        let conflict = sm
            .apply_command(Command::CreateTask {
                request_id: format!("create:{}", task_id),
                actor: "system".into(),
                task: Box::new(conflicting_task),
                shards: store.list_task_shards(&task_id).await.unwrap(),
            })
            .await
            .unwrap();
        assert!(!conflict.success);
        let committed = store.get_task(&task_id).await.unwrap().unwrap();
        assert!(matches!(
            committed.state,
            crate::types::TaskState::Running { .. }
        ));
        assert_eq!(store.list_task_shards(&task_id).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn graceful_pause_waits_for_active_lease_checkpoint_and_release() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command(Command::RegisterNode {
                node_id: "graceful-worker".into(),
                address: "127.0.0.1:9001".into(),
                tags: Default::default(),
                issued_at_ms: 1_000
            })
            .await
            .unwrap()
            .success
        );
        let task = TaskMeta::new("graceful-pause".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        assert!(
            sm.apply_command(Command::CreateTask {
                request_id: format!("create:{}", task.task_id),
                actor: "system".into(),
                task: Box::new(task),
                shards: vec![shard]
            })
            .await
            .unwrap()
            .success
        );
        let now = chrono::Utc::now().timestamp_millis();
        let claim = sm
            .apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "graceful-worker".into(),
                claim_id: "graceful-claim".into(),
                issued_at_ms: now,
                lease_expires_at_ms: now + 30_000,
            })
            .await
            .unwrap();
        let fencing_token = claim.data.unwrap()["fencing_token"].as_u64().unwrap();
        assert!(
            sm.apply_command(Command::ControlTask {
                task_id: task_id.to_string(),
                action: "pause".into(),
                request_id: "graceful-pause-1".into(),
                actor: "operator".into()
            })
            .await
            .unwrap()
            .success
        );
        assert!(matches!(
            store.get_task(&task_id).await.unwrap().unwrap().state,
            crate::types::TaskState::Pausing
        ));
        assert!(
            !sm.apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "graceful-worker".into(),
                claim_id: "blocked-claim".into(),
                issued_at_ms: now,
                lease_expires_at_ms: now + 30_000
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::ReleaseShardLease {
                shard_id: shard_id.to_string(),
                node_id: "graceful-worker".into(),
                fencing_token,
                issued_at_ms: now + 1
            })
            .await
            .unwrap()
            .success
        );
        assert!(matches!(
            store.get_task(&task_id).await.unwrap().unwrap().state,
            crate::types::TaskState::Paused
        ));
    }

    #[tokio::test]
    async fn cancellation_stops_new_work_but_allows_inflight_checkpoint_to_commit() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command(Command::RegisterNode {
                node_id: "cancel-worker".into(),
                address: "127.0.0.1:9002".into(),
                tags: Default::default(),
                issued_at_ms: 1_000
            })
            .await
            .unwrap()
            .success
        );
        let task = TaskMeta::new("cancel-inflight".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        assert!(
            sm.apply_command(Command::CreateTask {
                request_id: format!("create:{task_id}"),
                actor: "operator".into(),
                task: Box::new(task),
                shards: vec![shard]
            })
            .await
            .unwrap()
            .success
        );
        let now = chrono::Utc::now().timestamp_millis();
        let claim = sm
            .apply_command(Command::ClaimShard {
                shard_id: shard_id.to_string(),
                node_id: "cancel-worker".into(),
                claim_id: "cancel-claim".into(),
                issued_at_ms: now,
                lease_expires_at_ms: now + 30_000,
            })
            .await
            .unwrap();
        let fencing_token = claim.data.unwrap()["fencing_token"].as_u64().unwrap();
        assert!(
            sm.apply_command(Command::ControlTask {
                task_id: task_id.to_string(),
                action: "cancel".into(),
                request_id: "cancel-inflight-1".into(),
                actor: "operator".into()
            })
            .await
            .unwrap()
            .success
        );
        let progress = sm
            .apply_command(Command::UpdateShardProgress {
                shard_id: shard_id.to_string(),
                node_id: "cancel-worker".into(),
                fencing_token,
                issued_at_ms: now + 1,
                processed_records: 5,
                processed_bytes: 512,
                offset: serde_json::to_string(&crate::pipeline::OffsetValue::DocumentCount(5))
                    .unwrap(),
                batch_id: None,
            })
            .await
            .unwrap();
        assert!(
            progress.success,
            "an in-flight committed batch must checkpoint after cancellation"
        );
        let completion = sm
            .apply_command(Command::CompleteShard {
                shard_id: shard_id.to_string(),
                node_id: "cancel-worker".into(),
                fencing_token,
                issued_at_ms: now + 2,
                total_records: 5,
            })
            .await
            .unwrap();
        assert!(
            !completion.success,
            "cancelled work must not transition to completed"
        );
        assert!(
            sm.apply_command(Command::ReleaseShardLease {
                shard_id: shard_id.to_string(),
                node_id: "cancel-worker".into(),
                fencing_token,
                issued_at_ms: now + 3,
            })
            .await
            .unwrap()
            .success
        );
        let restored = store.get_shard(&shard_id).await.unwrap().unwrap();
        assert_eq!(restored.records_processed, 5);
        assert_eq!(
            restored.offset,
            crate::pipeline::OffsetValue::DocumentCount(5)
        );
        assert!(matches!(
            store.get_task(&task_id).await.unwrap().unwrap().state,
            crate::types::TaskState::Cancelled { .. }
        ));
    }

    #[tokio::test]
    async fn audit_event_contains_actor_request_and_applied_raft_index() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let task = TaskMeta::new("audited-task".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command_at(
                Command::CreateTask {
                    request_id: "audit-create-1".into(),
                    actor: "bob".into(),
                    task: Box::new(task),
                    shards: vec![],
                },
                76,
            )
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command_at(
                Command::ControlTask {
                    task_id: task_id.to_string(),
                    action: "pause".into(),
                    request_id: "audit-pause-1".into(),
                    actor: "alice".into()
                },
                77
            )
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command_at(
                Command::ControlTask {
                    task_id: task_id.to_string(),
                    action: "pause".into(),
                    request_id: "audit-pause-1".into(),
                    actor: "alice".into(),
                },
                78
            )
            .await
            .unwrap()
            .success
        );
        let events = store.list_audit_events(10).await.unwrap();
        assert_eq!(
            events.len(),
            2,
            "an idempotent replay must not duplicate its audit event"
        );
        assert_eq!(events[0].log_index, 77);
        assert_eq!(events[0].actor, "alice");
        assert_eq!(events[0].request_id, "audit-pause-1");
        assert_eq!(events[0].action, "task.pause");
        assert_eq!(events[0].from_state, "pending");
        assert_eq!(events[0].to_state, "paused");
        assert_eq!(events[1].log_index, 76);
        assert_eq!(events[1].actor, "bob");
        assert_eq!(events[1].request_id, "audit-create-1");
        assert_eq!(events[1].action, "task.create");
    }

    #[tokio::test]
    async fn task_control_transitions_are_committed_and_idempotent() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let task = TaskMeta::new("control-plane".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command(Command::CreateTask {
                request_id: format!("create:{}", task.task_id),
                actor: "system".into(),
                task: Box::new(task),
                shards: vec![shard]
            })
            .await
            .unwrap()
            .success
        );

        let pause = Command::ControlTask {
            task_id: task_id.to_string(),
            action: "pause".into(),
            request_id: "pause-1".into(),
            actor: "test".into(),
        };
        assert!(sm.apply_command(pause.clone()).await.unwrap().success);
        assert!(sm.apply_command(pause).await.unwrap().success);
        assert!(matches!(
            store.get_task(&task_id).await.unwrap().unwrap().state,
            crate::types::TaskState::Paused
        ));
        let shard_id = store.list_task_shards(&task_id).await.unwrap()[0].shard_id;
        let now = chrono::Utc::now().timestamp_millis();
        let paused_claim = Command::ClaimShard {
            shard_id: shard_id.to_string(),
            node_id: "worker-paused-task".into(),
            claim_id: "paused-claim".into(),
            issued_at_ms: now,
            lease_expires_at_ms: now + 30_000,
        };
        assert!(
            !sm.apply_command(paused_claim).await.unwrap().success,
            "paused tasks must reject new claims"
        );

        let resume = Command::ControlTask {
            task_id: task_id.to_string(),
            action: "resume".into(),
            request_id: "resume-1".into(),
            actor: "test".into(),
        };
        assert!(sm.apply_command(resume).await.unwrap().success);
        assert!(matches!(
            store.get_task(&task_id).await.unwrap().unwrap().state,
            crate::types::TaskState::Running { .. }
        ));

        let cancel = Command::ControlTask {
            task_id: task_id.to_string(),
            action: "cancel".into(),
            request_id: "cancel-1".into(),
            actor: "test".into(),
        };
        assert!(sm.apply_command(cancel.clone()).await.unwrap().success);
        assert!(sm.apply_command(cancel).await.unwrap().success);
        assert!(matches!(
            store.get_task(&task_id).await.unwrap().unwrap().state,
            crate::types::TaskState::Cancelled { .. }
        ));
        let conflicting = Command::ControlTask {
            task_id: task_id.to_string(),
            action: "resume".into(),
            request_id: "cancel-1".into(),
            actor: "test".into(),
        };
        assert!(!sm.apply_command(conflicting).await.unwrap().success);
    }

    #[tokio::test]
    async fn node_execution_pause_blocks_new_claims_until_resumed() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command(Command::RegisterNode {
                node_id: "worker-control".into(),
                address: "127.0.0.1:9000".into(),
                tags: Default::default(),
                issued_at_ms: 1_000
            })
            .await
            .unwrap()
            .success
        );
        let task = TaskMeta::new("node-control".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let shard = ShardMeta::new(task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        assert!(
            sm.apply_command(Command::CreateTask {
                request_id: format!("create:{}", task.task_id),
                actor: "system".into(),
                task: Box::new(task),
                shards: vec![shard]
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::ControlNodeExecution {
                node_id: "worker-control".into(),
                paused: true,
                request_id: "node-pause-1".into(),
                actor: "test".into()
            })
            .await
            .unwrap()
            .success
        );
        assert!(
            sm.apply_command(Command::UpdateNodeHeartbeat {
                node_id: "worker-control".into(),
                issued_at_ms: 2_000,
            })
            .await
            .unwrap()
            .success
        );
        assert_eq!(
            store
                .get_node(&NodeId::new("worker-control"))
                .await
                .unwrap()
                .unwrap()
                .status,
            crate::types::NodeStatus::Paused
        );
        let now = chrono::Utc::now().timestamp_millis();
        let claim = || Command::ClaimShard {
            shard_id: shard_id.to_string(),
            node_id: "worker-control".into(),
            claim_id: "claim-control".into(),
            issued_at_ms: now,
            lease_expires_at_ms: now + 30_000,
        };
        assert!(!sm.apply_command(claim()).await.unwrap().success);
        assert!(
            sm.apply_command(Command::ControlNodeExecution {
                node_id: "worker-control".into(),
                paused: false,
                request_id: "node-resume-1".into(),
                actor: "test".into()
            })
            .await
            .unwrap()
            .success
        );
        assert!(sm.apply_command(claim()).await.unwrap().success);
    }

    #[tokio::test]
    async fn retry_task_requeues_failed_shards_without_losing_checkpoint() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = TaskStore::new(db);
        let task = TaskMeta::new("retry-control".into(), "{}".into(), "{}".into());
        let task_id = task.task_id;
        let mut shard = ShardMeta::new(task_id, 0, "{}".into());
        shard.offset = crate::pipeline::OffsetValue::DocumentCount(42);
        shard.records_processed = 42;
        shard.bytes_processed = 8192;
        shard.set_state_at(
            ShardState::Failed {
                error: "temporary".into(),
                attempts: 1,
            },
            chrono::Utc::now(),
        );
        let shard_id = shard.shard_id;
        let mut sm = MetaStateMachine::new(store.kv_store().clone());
        assert!(
            sm.apply_command(Command::CreateTask {
                request_id: format!("create:{}", task.task_id),
                actor: "system".into(),
                task: Box::new(task),
                shards: vec![shard]
            })
            .await
            .unwrap()
            .success
        );
        let retry = Command::RetryTask {
            task_id: task_id.to_string(),
            request_id: "retry-1".into(),
            actor: "test".into(),
        };
        let result = sm.apply_command(retry.clone()).await.unwrap();
        assert!(result.success, "{}", result.message);
        assert!(sm.apply_command(retry).await.unwrap().success);
        let restored = store.get_shard(&shard_id).await.unwrap().unwrap();
        assert!(matches!(restored.state, ShardState::Pending));
        assert_eq!(
            restored.offset,
            crate::pipeline::OffsetValue::DocumentCount(42)
        );
        assert_eq!(restored.records_processed, 42);
        assert_eq!(restored.bytes_processed, 8192);
        assert_eq!(restored.retry_count, 1);
        assert_eq!(store.operational_metrics().await.unwrap().retries_total, 1);
    }
}
