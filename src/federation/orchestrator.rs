// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 Dataspool Contributors

//! Leader-driven federated task provisioning and recovery reconciliation.

use std::{path::PathBuf, sync::Arc, time::Duration};

use serde_json::json;
use tracing::{debug, warn};

use crate::{
    CancellationToken, GrpcClient,
    comms::task,
    federation::{
        FederatedClusterStatus, FederatedMigration, MigrationState, MigrationTimeoutPolicy,
    },
    raft::{Command, RaftNode},
    storage::RocksDbStore,
    task::TaskStore,
};

#[derive(Clone)]
pub struct FederatedOrchestrator {
    store: Arc<TaskStore<RocksDbStore>>,
    raft: Arc<RaftNode>,
    local_cluster_id: String,
    ca_file: PathBuf,
    certificate_file: PathBuf,
    private_key_file: PathBuf,
    reconcile_interval: Duration,
    operation_lock: Arc<tokio::sync::Mutex<()>>,
}

impl FederatedOrchestrator {
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
            reconcile_interval: Duration::from_secs(5),
            operation_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub async fn run_until_cancelled(&self, shutdown: CancellationToken) {
        let mut interval = tokio::time::interval(self.reconcile_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    if let Err(error) = self.reconcile_once().await {
                        warn!(%error, "Federated migration reconciliation failed");
                    }
                }
            }
        }
    }

    /// Only the Raft leader performs remote side effects. Every remote write is
    /// keyed by migration ID and generation so a new leader can safely replay it.
    pub async fn reconcile_once(&self) -> crate::Result<()> {
        let _guard = self.operation_lock.lock().await;
        if self.raft.current_leader().await != Some(self.raft.node_id()) {
            return Ok(());
        }
        self.raft.ensure_linearizable().await?;
        for migration in self.store.list_federated_migrations().await? {
            // A migration is owned by its source cluster. Other clusters may
            // replicate the plan but must not impersonate its orchestrator.
            if migration.source_cluster != self.local_cluster_id {
                continue;
            }
            if let Err(error) = self.reconcile_migration(migration.clone()).await {
                self.mark_target_failure(&migration.target_cluster).await?;
                warn!(migration_id = %migration.migration_id, generation = migration.generation, %error,
                    "Unable to reconcile federated migration; it will be retried");
            }
        }
        Ok(())
    }

    async fn reconcile_migration(&self, migration: FederatedMigration) -> crate::Result<()> {
        if migration.state == MigrationState::Completed || migration.state == MigrationState::Paused
        {
            return Ok(());
        }
        if matches!(
            migration.state,
            MigrationState::Cancelled | MigrationState::Failed
        ) {
            return self.compensate_terminal_migration(&migration).await;
        }

        if self.sla_exceeded(&migration) {
            return self.apply_timeout_policy(&migration).await;
        }

        match migration.state {
            MigrationState::Planned => {
                self.transition(&migration, MigrationState::Preparing, None, "prepare")
                    .await?;
            }
            MigrationState::Preparing => {
                if let Some(remote_task_id) = migration.remote_task_id.as_deref() {
                    let mut peer = self.connect_target(&migration.target_cluster).await?;
                    let request_id = format!(
                        "federation:{}:g{}:resume",
                        migration.migration_id, migration.generation
                    );
                    let response = peer
                        .control_remote_task(
                            remote_task_id,
                            &request_id,
                            &self.local_cluster_id,
                            "resume",
                        )
                        .await?;
                    if !response.success {
                        return Err(crate::DatspoolError::Internal(format!(
                            "Target task resume rejected: {}",
                            response.message
                        )));
                    }
                    self.transition(&migration, MigrationState::Running, None, "resumed")
                        .await?;
                } else {
                    self.provision_remote_task(&migration).await?;
                }
            }
            MigrationState::Running | MigrationState::Committing => {
                self.reconcile_remote_task(&migration).await?;
            }
            MigrationState::Completed
            | MigrationState::Failed
            | MigrationState::Cancelled
            | MigrationState::Paused => {}
        }
        Ok(())
    }

    fn sla_exceeded(&self, migration: &FederatedMigration) -> bool {
        // Once the target has prepared its durable commit record, finish the
        // short commit critical section rather than timing out between phases.
        if migration.state == MigrationState::Committing {
            return false;
        }
        let now = chrono::Utc::now().timestamp_millis();
        migration
            .deadline_at_ms
            .is_some_and(|deadline| now >= deadline)
            || migration.execution_budget_ms.is_some_and(|budget| {
                now.saturating_sub(migration.created_at_ms).max(0) as u64 >= budget
            })
            || migration
                .retry_budget
                .is_some_and(|budget| migration.generation > u64::from(budget))
    }

    async fn apply_timeout_policy(&self, migration: &FederatedMigration) -> crate::Result<()> {
        let remote_task_id = migration
            .remote_task_id
            .clone()
            .unwrap_or_else(|| deterministic_remote_task_id(migration));
        let mut peer = self.connect_target(&migration.target_cluster).await?;
        let remote_task_exists = if let Some(remote) = peer
            .get_remote_task(&remote_task_id, &self.local_cluster_id)
            .await?
        {
            if remote
                .task
                .as_ref()
                .is_some_and(|task| task.state.eq_ignore_ascii_case("completed"))
            {
                // The target has crossed the data-write boundary. Finish its durable
                // commit protocol instead of claiming that already-written data was cancelled.
                return self.reconcile_remote_task(migration).await;
            }
            true
        } else {
            false
        };
        if remote_task_exists {
            let action = match migration.timeout_policy {
                MigrationTimeoutPolicy::Pause => "pause",
                MigrationTimeoutPolicy::Cancel | MigrationTimeoutPolicy::Fail => "cancel",
            };
            let request_id = format!(
                "federation:{}:g{}:sla:{}",
                migration.migration_id, migration.generation, action
            );
            if action == "pause" {
                let response = peer
                    .control_remote_task(
                        &remote_task_id,
                        &request_id,
                        &self.local_cluster_id,
                        action,
                    )
                    .await?;
                if !response.success {
                    return Err(crate::DatspoolError::Internal(format!(
                        "Remote SLA action rejected: {}",
                        response.message
                    )));
                }
            } else {
                let response = peer
                    .compensate_remote_migration(
                        &migration.migration_id,
                        migration.generation,
                        &remote_task_id,
                        &request_id,
                        &self.local_cluster_id,
                    )
                    .await?;
                if !response.accepted {
                    return Err(crate::DatspoolError::Internal(format!(
                        "Remote SLA compensation rejected: {}",
                        response.message
                    )));
                }
            }
        }
        let (state, reason) = if migration.state == MigrationState::Committing {
            (
                MigrationState::Failed,
                "SLA budget exceeded during commit; migration failed",
            )
        } else {
            match migration.timeout_policy {
                MigrationTimeoutPolicy::Pause => (
                    MigrationState::Paused,
                    "SLA budget exceeded; remote task paused",
                ),
                MigrationTimeoutPolicy::Cancel => (
                    MigrationState::Cancelled,
                    "SLA budget exceeded; remote task cancelled",
                ),
                MigrationTimeoutPolicy::Fail => (
                    MigrationState::Failed,
                    "SLA budget exceeded; migration failed",
                ),
            }
        };
        self.transition(migration, state, Some(reason.to_string()), "sla-timeout")
            .await
    }

    async fn provision_remote_task(&self, migration: &FederatedMigration) -> crate::Result<()> {
        let mut peer = self.connect_target(&migration.target_cluster).await?;
        if migration.generation > 0 {
            let mut previous = migration.clone();
            previous.generation -= 1;
            let previous_task_id = deterministic_remote_task_id(&previous);
            if peer
                .get_remote_task(&previous_task_id, &self.local_cluster_id)
                .await?
                .is_some()
            {
                let compensation_request = format!(
                    "federation:{}:g{}:retry-compensate",
                    migration.migration_id, previous.generation
                );
                let compensation = peer
                    .compensate_remote_migration(
                        &migration.migration_id,
                        previous.generation,
                        &previous_task_id,
                        &compensation_request,
                        &self.local_cluster_id,
                    )
                    .await?;
                if !compensation.accepted {
                    return Err(crate::DatspoolError::Internal(format!(
                        "Previous generation compensation rejected: {}",
                        compensation.message
                    )));
                }
            }
        }
        let request_id = provision_request_id(migration);
        let mut tags = migration.tags.clone();
        tags.insert(
            "_dataspool_federation_migration_id".into(),
            migration.migration_id.clone(),
        );
        tags.insert(
            "_dataspool_federation_generation".into(),
            migration.generation.to_string(),
        );
        let request = task::CreateTaskRequest {
            name: if migration.task_name.is_empty() {
                migration.migration_id.clone()
            } else {
                migration.task_name.clone()
            },
            source_config: migration.source_config.clone(),
            sink_config: migration.sink_config.clone(),
            transform_config: migration.transform_config.clone().unwrap_or_default(),
            sharding_strategy: migration.sharding_strategy.clone().unwrap_or_default(),
            priority: migration.priority.clamp(0, 3),
            tags,
            request_id,
        };
        let response = peer
            .create_remote_task(request, &self.local_cluster_id)
            .await?;
        if !response.success || response.task_id.is_empty() {
            return Err(crate::DatspoolError::Internal(format!(
                "Target rejected task provisioning: {}",
                response.message
            )));
        }
        self.save_remote_progress(migration, Some(response.task_id), None, None)
            .await?;
        if migration.state == MigrationState::Preparing {
            self.transition(migration, MigrationState::Running, None, "provisioned")
                .await?;
        }
        Ok(())
    }

    async fn reconcile_remote_task(&self, migration: &FederatedMigration) -> crate::Result<()> {
        let Some(remote_task_id) = migration.remote_task_id.as_deref() else {
            return self.provision_remote_task(migration).await;
        };
        let mut peer = self.connect_target(&migration.target_cluster).await?;
        let Some(response) = peer
            .get_remote_task(remote_task_id, &self.local_cluster_id)
            .await?
        else {
            // A target-side data loss or incomplete restore is repaired by replaying
            // the same generation-scoped CreateTask idempotency key.
            return self.provision_remote_task(migration).await;
        };
        let info = response.task.ok_or_else(|| {
            crate::DatspoolError::Internal("Target returned an empty task record".into())
        })?;
        let checkpoints: Vec<serde_json::Value> = response.shards.into_iter().map(|shard| json!({
            "shard_id": shard.shard_id,
            "state": shard.state,
            "processed_records": shard.processed_records,
            "processed_bytes": shard.processed_bytes,
            "fencing_token": shard.fencing_token,
            "lease_expires_at": shard.lease_expires_at.map(|v| json!({"seconds": v.seconds, "nanos": v.nanos})),
            "checkpoint": shard.checkpoint,
            "updated_at": shard.updated_at.map(|v| json!({"seconds": v.seconds, "nanos": v.nanos})),
        })).collect();
        let checkpoint = json!({
            "task_id": info.task_id,
            "state": info.state,
            "total_shards": info.total_shards,
            "completed_shards": info.completed_shards,
            "failed_shards": info.failed_shards,
            "processed_records": info.processed_records,
            "processed_bytes": info.processed_bytes,
            "shards": checkpoints,
        });
        let remote_state = info.state.to_ascii_lowercase();
        self.save_remote_progress(
            migration,
            Some(remote_task_id.to_string()),
            Some(remote_state.clone()),
            Some(checkpoint.clone()),
        )
        .await?;

        match remote_state.as_str() {
            "completed" if migration.state == MigrationState::Running => {
                let request_id = format!(
                    "federation:{}:g{}:prepare-commit",
                    migration.migration_id, migration.generation
                );
                let prepared = peer
                    .prepare_remote_commit(
                        &migration.migration_id,
                        migration.generation,
                        remote_task_id,
                        &request_id,
                        &self.local_cluster_id,
                    )
                    .await?;
                if !prepared.accepted || prepared.state != "prepared" {
                    return Err(crate::DatspoolError::Internal(format!(
                        "Target commit preparation rejected: {}",
                        prepared.message
                    )));
                }
                let mut checkpoint = checkpoint;
                checkpoint["federation_commit_state"] = json!("prepared");
                checkpoint["checkpoint_digest"] = json!(prepared.checkpoint_digest);
                self.save_remote_progress(
                    migration,
                    Some(remote_task_id.to_string()),
                    Some("completed".into()),
                    Some(checkpoint),
                )
                .await?;
                self.transition(
                    migration,
                    MigrationState::Committing,
                    None,
                    "target-validated",
                )
                .await?;
            }
            "completed" if migration.state == MigrationState::Committing => {
                let request_id = format!(
                    "federation:{}:g{}:commit",
                    migration.migration_id, migration.generation
                );
                let committed = peer
                    .commit_remote_migration(
                        &migration.migration_id,
                        migration.generation,
                        remote_task_id,
                        &request_id,
                        &self.local_cluster_id,
                    )
                    .await?;
                if !committed.accepted || committed.state != "committed" {
                    return Err(crate::DatspoolError::Internal(format!(
                        "Target distributed commit rejected: {}",
                        committed.message
                    )));
                }
                let mut checkpoint = checkpoint;
                checkpoint["federation_commit_state"] = json!("committed");
                checkpoint["checkpoint_digest"] = json!(committed.checkpoint_digest);
                self.save_remote_progress(
                    migration,
                    Some(remote_task_id.to_string()),
                    Some("completed".into()),
                    Some(checkpoint),
                )
                .await?;
                self.transition(
                    migration,
                    MigrationState::Completed,
                    None,
                    "target-committed",
                )
                .await?;
            }
            "failed" => {
                self.transition(
                    migration,
                    MigrationState::Failed,
                    Some("Target migration task failed".into()),
                    "target-failed",
                )
                .await?;
            }
            "cancelled" => {
                self.transition(
                    migration,
                    MigrationState::Cancelled,
                    Some("Target migration task was cancelled".into()),
                    "target-cancelled",
                )
                .await?;
            }
            _ => {
                debug!(migration_id = %migration.migration_id, remote_state, "Federated target task remains active")
            }
        }
        Ok(())
    }

    async fn save_remote_progress(
        &self,
        migration: &FederatedMigration,
        remote_task_id: Option<String>,
        remote_task_state: Option<String>,
        remote_checkpoint: Option<serde_json::Value>,
    ) -> crate::Result<()> {
        let request_id = format!(
            "federation:{}:g{}:progress:{}",
            migration.migration_id,
            migration.generation,
            chrono::Utc::now().timestamp_millis()
        );
        self.raft
            .propose(Command::UpdateFederatedMigrationProgress {
                migration_id: migration.migration_id.clone(),
                expected_generation: migration.generation,
                remote_task_id,
                remote_task_state,
                remote_checkpoint,
                at_ms: chrono::Utc::now().timestamp_millis(),
                request_id,
                actor: "federation-reconciler".into(),
            })
            .await?;
        Ok(())
    }

    async fn transition(
        &self,
        migration: &FederatedMigration,
        next_state: MigrationState,
        error: Option<String>,
        suffix: &str,
    ) -> crate::Result<()> {
        let request_id = format!(
            "federation:{}:g{}:{}",
            migration.migration_id, migration.generation, suffix
        );
        let response = self
            .raft
            .propose(Command::TransitionFederatedMigration {
                migration_id: migration.migration_id.clone(),
                next_state,
                expected_generation: migration.generation,
                at_ms: chrono::Utc::now().timestamp_millis(),
                error,
                request_id,
                actor: "federation-reconciler".into(),
            })
            .await?;
        if !response.success {
            return Err(crate::DatspoolError::Internal(response.message));
        }
        Ok(())
    }

    async fn connect_target(&self, cluster_id: &str) -> crate::Result<GrpcClient> {
        let cluster = self
            .store
            .get_federated_cluster(cluster_id)
            .await?
            .ok_or_else(|| {
                crate::DatspoolError::Configuration(format!(
                    "Federated target '{cluster_id}' is not registered"
                ))
            })?;
        if cluster.status == FederatedClusterStatus::Offline {
            return Err(crate::DatspoolError::Internal(format!(
                "Federated target '{cluster_id}' is offline"
            )));
        }
        let peer = if let Some(bundle) =
            cluster.handshake_trust_bundle(chrono::Utc::now().timestamp_millis())
        {
            GrpcClient::connect_mtls_with_ca_pem(
                &cluster.endpoint,
                bundle.as_bytes(),
                &self.certificate_file,
                &self.private_key_file,
            )
            .await?
        } else {
            GrpcClient::connect_mtls(
                &cluster.endpoint,
                &self.ca_file,
                &self.certificate_file,
                &self.private_key_file,
            )
            .await?
        };
        self.record_target_status(&cluster, FederatedClusterStatus::Healthy)
            .await?;
        Ok(peer)
    }

    async fn mark_target_failure(&self, cluster_id: &str) -> crate::Result<()> {
        if let Some(cluster) = self.store.get_federated_cluster(cluster_id).await? {
            let age = chrono::Utc::now()
                .timestamp_millis()
                .saturating_sub(cluster.last_seen_at_ms);
            let status = if age >= self.reconcile_interval.as_millis() as i64 * 3 {
                FederatedClusterStatus::Offline
            } else {
                FederatedClusterStatus::Degraded
            };
            self.record_target_status(&cluster, status).await?;
        }
        Ok(())
    }

    async fn record_target_status(
        &self,
        cluster: &crate::federation::FederatedCluster,
        status: FederatedClusterStatus,
    ) -> crate::Result<()> {
        let now = chrono::Utc::now().timestamp_millis();
        if cluster.status == status && now.saturating_sub(cluster.last_seen_at_ms) < 15_000 {
            return Ok(());
        }
        let request_id = format!(
            "federation-health:{}:{}:{}",
            cluster.cluster_id,
            match status {
                FederatedClusterStatus::Healthy => "healthy",
                FederatedClusterStatus::Degraded => "degraded",
                FederatedClusterStatus::Offline => "offline",
            },
            now / 15_000
        );
        self.raft
            .propose(Command::HeartbeatFederatedCluster {
                cluster_id: cluster.cluster_id.clone(),
                identity_fingerprint: cluster.identity_fingerprint.clone(),
                status,
                last_seen_at_ms: now,
                request_id,
                actor: "federation-reconciler".into(),
            })
            .await?;
        Ok(())
    }

    async fn compensate_terminal_migration(
        &self,
        migration: &FederatedMigration,
    ) -> crate::Result<()> {
        let remote_task_id = migration
            .remote_task_id
            .clone()
            .unwrap_or_else(|| deterministic_remote_task_id(migration));
        let mut peer = self.connect_target(&migration.target_cluster).await?;
        if peer
            .get_remote_task(&remote_task_id, &self.local_cluster_id)
            .await?
            .is_none()
        {
            return Ok(());
        }
        let request_id = format!(
            "federation:{}:g{}:compensate",
            migration.migration_id, migration.generation
        );
        let response = peer
            .compensate_remote_migration(
                &migration.migration_id,
                migration.generation,
                &remote_task_id,
                &request_id,
                &self.local_cluster_id,
            )
            .await?;
        if !response.accepted {
            return Err(crate::DatspoolError::Internal(format!(
                "Remote compensation rejected: {}",
                response.message
            )));
        }
        self.save_remote_progress(
            migration,
            Some(remote_task_id),
            Some("cancelled".into()),
            None,
        )
        .await
    }
}

fn provision_request_id(migration: &FederatedMigration) -> String {
    format!(
        "federation:{}:g{}:provision",
        migration.migration_id, migration.generation
    )
}

fn deterministic_remote_task_id(migration: &FederatedMigration) -> String {
    uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("dataspool:create-task:{}", provision_request_id(migration)).as_bytes(),
    )
    .to_string()
}
