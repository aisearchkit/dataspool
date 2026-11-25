// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Raft command definitions - all cluster state mutations

use crate::task::{ShardMeta, TaskMeta};
use serde::{Deserialize, Serialize};

/// Commands that can be proposed to the Raft log
///
/// All state changes go through Raft consensus. Only the Leader
/// can propose commands. Followers will forward writes to the Leader.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    // ==================== Task Management ====================
    /// Create a new task
    CreateTask {
        task: Box<TaskMeta>,
        shards: Vec<ShardMeta>,
        #[serde(default)]
        request_id: String,
        #[serde(default = "default_command_actor")]
        actor: String,
    },

    /// Update task state
    UpdateTaskState {
        task_id: String,
        state: String, // JSON-serialized TaskState
    },

    /// Apply a task lifecycle transition through the committed state machine.
    ControlTask {
        task_id: String,
        action: String,
        request_id: String,
        actor: String,
    },

    /// Requeue failed shards without resetting their durable checkpoints.
    RetryTask {
        task_id: String,
        request_id: String,
        actor: String,
    },

    /// Pause or resume new shard claims on a worker node.
    ControlNodeExecution {
        node_id: String,
        paused: bool,
        request_id: String,
        actor: String,
    },

    /// Delete a task (soft delete - mark as archived)
    DeleteTask { task_id: String },

    // ==================== Shard Management ====================
    /// Claim a shard (CAS operation)
    ClaimShard {
        shard_id: String,
        node_id: String,
        claim_id: String,
        issued_at_ms: i64,
        lease_expires_at_ms: i64,
    },

    /// Release a shard lease on cooperative shutdown.
    ReleaseShardLease {
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
    },

    /// Import a pre-Raft local cache entry during the one-time cluster bootstrap.
    ImportLegacyCache {
        shard_id: String,
        batch_id: String,
        records: Vec<crate::pipeline::Record>,
        next_offset: Option<crate::pipeline::OffsetValue>,
        source_count: u64,
    },

    /// Mark legacy-state import complete; workers on followers wait for this entry.
    BootstrapComplete,

    /// Replicate a durable source batch before any destination write is attempted.
    CacheBatch {
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        batch_id: String,
        records: Vec<crate::pipeline::Record>,
        next_offset: Option<crate::pipeline::OffsetValue>,
        source_count: u64,
    },

    /// Record a destination-confirmed batch result before advancing its source checkpoint.
    RecordBatchDelivery {
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        batch_id: String,
        success_count: u64,
        failed_count: u64,
        bytes_written: u64,
    },

    /// Replicate permanent sink failures to every state-machine replica.
    PersistDeadLetters {
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        failures: Vec<crate::pipeline::FailedRecord>,
    },

    /// Replicate cache acknowledgement after destination delivery and checkpoint commit.
    AckCacheBatch {
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        cache_key: String,
    },

    /// Renew a shard lease using the current fencing generation.
    RenewShardLease {
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        lease_expires_at_ms: i64,
    },

    /// Update shard progress
    UpdateShardProgress {
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        processed_records: u64,
        processed_bytes: u64,
        offset: String, // JSON-serialized OffsetValue
        #[serde(default)]
        batch_id: Option<String>,
    },

    /// Persist a producer-barriered CDC cutover before online migration completion.
    RecordCdcCutover {
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
    },

    /// Persist a vector-watermarked partitioned CDC cutover. Each partition
    /// has an independent local sequence; maps must contain identical keys.
    RecordCdcPartitionedCutover {
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
    },

    /// Mark shard as completed
    CompleteShard {
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        total_records: u64,
    },

    /// Mark shard as failed
    FailShard {
        shard_id: String,
        node_id: String,
        fencing_token: u64,
        issued_at_ms: i64,
        error_message: String,
    },

    // ==================== Node Management ====================
    /// Register a new node
    RegisterNode {
        node_id: String,
        address: String,
        tags: std::collections::HashMap<String, String>,
        issued_at_ms: i64,
    },

    /// Update node heartbeat using the leader's authoritative clock.
    UpdateNodeHeartbeat { node_id: String, issued_at_ms: i64 },

    /// Mark a node offline only when its last committed heartbeat is older than the timeout.
    MarkNodeOffline {
        node_id: String,
        issued_at_ms: i64,
        heartbeat_timeout_ms: i64,
    },

    // ==================== Federated Operations ====================
    UpsertFederatedCluster {
        cluster: crate::federation::FederatedCluster,
        request_id: String,
        actor: String,
    },
    CreateFederatedMigration {
        migration: Box<crate::federation::FederatedMigration>,
        request_id: String,
        actor: String,
    },
    HeartbeatFederatedCluster {
        cluster_id: String,
        identity_fingerprint: String,
        status: crate::federation::FederatedClusterStatus,
        last_seen_at_ms: i64,
        request_id: String,
        actor: String,
    },
    StageFederatedCertificateRotation {
        cluster_id: String,
        current_fingerprint: String,
        new_fingerprint: String,
        at_ms: i64,
        grace_period_ms: i64,
        request_id: String,
        actor: String,
    },
    CompleteFederatedCertificateRotation {
        cluster_id: String,
        new_fingerprint: String,
        at_ms: i64,
        request_id: String,
        actor: String,
    },
    /// Stage a validated CA bundle without changing the currently trusted roots.
    StageFederatedTrustBundle {
        cluster_id: String,
        current_fingerprint: String,
        bundle_pem: String,
        bundle_fingerprint: String,
        at_ms: i64,
        request_id: String,
        actor: String,
    },
    /// Atomically activate the staged CA bundle and commit the old-root expiry.
    ActivateFederatedTrustBundle {
        cluster_id: String,
        bundle_fingerprint: String,
        at_ms: i64,
        overlap_ms: i64,
        request_id: String,
        actor: String,
    },
    /// Retire old CA material only after the committed overlap deadline.
    RetireFederatedTrustBundle {
        cluster_id: String,
        at_ms: i64,
        request_id: String,
        actor: String,
    },
    TransitionFederatedMigration {
        migration_id: String,
        next_state: crate::federation::MigrationState,
        expected_generation: u64,
        at_ms: i64,
        error: Option<String>,
        request_id: String,
        actor: String,
    },
    UpdateFederatedMigrationProgress {
        migration_id: String,
        expected_generation: u64,
        remote_task_id: Option<String>,
        remote_task_state: Option<String>,
        remote_checkpoint: Option<serde_json::Value>,
        at_ms: i64,
        request_id: String,
        actor: String,
    },
    RecordFederatedRemoteOperation {
        operation: Box<crate::federation::FederatedRemoteOperation>,
        request_id: String,
        actor: String,
    },

    // ==================== Configuration ====================
    /// Update cluster configuration
    UpdateConfig {
        key: String,
        value: String, // JSON-serialized value
    },
}

impl Command {
    /// Get a human-readable description of the command
    pub fn description(&self) -> String {
        match self {
            Command::CreateTask { task, .. } => {
                format!("CreateTask({})", task.task_id)
            }
            Command::UpdateTaskState { task_id, .. } => {
                format!("UpdateTaskState({})", task_id)
            }
            Command::ControlTask {
                task_id, action, ..
            } => format!("ControlTask({}, {})", task_id, action),
            Command::RetryTask { task_id, .. } => format!("RetryTask({})", task_id),
            Command::ControlNodeExecution {
                node_id, paused, ..
            } => format!("ControlNodeExecution({}, paused={})", node_id, paused),
            Command::DeleteTask { task_id } => {
                format!("DeleteTask({})", task_id)
            }
            Command::ClaimShard {
                shard_id, node_id, ..
            } => {
                format!("ClaimShard({}, {})", shard_id, node_id)
            }
            Command::ReleaseShardLease {
                shard_id,
                node_id,
                fencing_token,
                ..
            } => {
                format!(
                    "ReleaseShardLease({}, {}, {})",
                    shard_id, node_id, fencing_token
                )
            }
            Command::ImportLegacyCache {
                shard_id,
                batch_id,
                records,
                ..
            } => {
                format!(
                    "ImportLegacyCache({}, {}, {} records)",
                    shard_id,
                    batch_id,
                    records.len()
                )
            }
            Command::BootstrapComplete => "BootstrapComplete".to_string(),
            Command::CacheBatch {
                shard_id,
                node_id: _,
                batch_id,
                records,
                ..
            } => {
                format!(
                    "CacheBatch({}, {}, {} records)",
                    shard_id,
                    batch_id,
                    records.len()
                )
            }
            Command::PersistDeadLetters {
                shard_id, failures, ..
            } => {
                format!(
                    "PersistDeadLetters({}, {} records)",
                    shard_id,
                    failures.len()
                )
            }
            Command::RecordBatchDelivery {
                shard_id, batch_id, ..
            } => {
                format!("RecordBatchDelivery({}, {})", shard_id, batch_id)
            }
            Command::AckCacheBatch {
                shard_id,
                cache_key,
                ..
            } => {
                format!("AckCacheBatch({}, {})", shard_id, cache_key)
            }
            Command::RenewShardLease {
                shard_id,
                node_id,
                fencing_token,
                ..
            } => {
                format!(
                    "RenewShardLease({}, {}, {})",
                    shard_id, node_id, fencing_token
                )
            }
            Command::UpdateShardProgress { shard_id, .. } => {
                format!("UpdateShardProgress({})", shard_id)
            }
            Command::RecordCdcCutover {
                task_id,
                final_watermark,
                applied_watermark,
                ..
            } => {
                format!("RecordCdcCutover({task_id}, {applied_watermark}/{final_watermark})")
            }
            Command::RecordCdcPartitionedCutover {
                task_id,
                final_watermarks,
                applied_watermarks,
                ..
            } => {
                format!(
                    "RecordCdcPartitionedCutover({task_id}, partitions={})",
                    final_watermarks.len().min(applied_watermarks.len())
                )
            }
            Command::CompleteShard {
                shard_id,
                fencing_token,
                ..
            } => {
                format!("CompleteShard({}, fencing={})", shard_id, fencing_token)
            }
            Command::FailShard {
                shard_id,
                fencing_token,
                ..
            } => {
                format!("FailShard({}, fencing={})", shard_id, fencing_token)
            }
            Command::RegisterNode { node_id, .. } => {
                format!("RegisterNode({})", node_id)
            }
            Command::UpdateNodeHeartbeat { node_id, .. } => {
                format!("UpdateNodeHeartbeat({})", node_id)
            }
            Command::MarkNodeOffline { node_id, .. } => {
                format!("MarkNodeOffline({})", node_id)
            }
            Command::UpdateConfig { key, .. } => format!("UpdateConfig({})", key),
            Command::UpsertFederatedCluster { cluster, .. } => {
                format!("UpsertFederatedCluster({})", cluster.cluster_id)
            }
            Command::CreateFederatedMigration { migration, .. } => {
                format!("CreateFederatedMigration({})", migration.migration_id)
            }
            Command::HeartbeatFederatedCluster {
                cluster_id, status, ..
            } => format!("HeartbeatFederatedCluster({}, {:?})", cluster_id, status),
            Command::StageFederatedCertificateRotation { cluster_id, .. } => {
                format!("StageFederatedCertificateRotation({})", cluster_id)
            }
            Command::CompleteFederatedCertificateRotation { cluster_id, .. } => {
                format!("CompleteFederatedCertificateRotation({})", cluster_id)
            }
            Command::StageFederatedTrustBundle { cluster_id, .. } => {
                format!("StageFederatedTrustBundle({})", cluster_id)
            }
            Command::ActivateFederatedTrustBundle { cluster_id, .. } => {
                format!("ActivateFederatedTrustBundle({})", cluster_id)
            }
            Command::RetireFederatedTrustBundle { cluster_id, .. } => {
                format!("RetireFederatedTrustBundle({})", cluster_id)
            }
            Command::TransitionFederatedMigration {
                migration_id,
                next_state,
                ..
            } => format!(
                "TransitionFederatedMigration({}, {:?})",
                migration_id, next_state
            ),
            Command::UpdateFederatedMigrationProgress { migration_id, .. } => {
                format!("UpdateFederatedMigrationProgress({})", migration_id)
            }
            Command::RecordFederatedRemoteOperation { operation, .. } => {
                format!(
                    "RecordFederatedRemoteOperation({}:{:?})",
                    operation.migration_id, operation.state
                )
            }
        }
    }
}

fn default_command_actor() -> String {
    "system".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_serialization() {
        let cmd = Command::ClaimShard {
            shard_id: "shard-1".to_string(),
            node_id: "node-1".to_string(),
            claim_id: "claim-1".to_string(),
            issued_at_ms: 1_000,
            lease_expires_at_ms: 31_000,
        };

        let json = serde_json::to_string(&cmd).unwrap();
        let decoded: Command = serde_json::from_str(&json).unwrap();

        match decoded {
            Command::ClaimShard {
                shard_id, node_id, ..
            } => {
                assert_eq!(shard_id, "shard-1");
                assert_eq!(node_id, "node-1");
            }
            _ => panic!("Wrong command type"),
        }
    }

    #[test]
    fn test_command_description() {
        let cmd = Command::CreateTask {
            task: Box::new(TaskMeta::new(
                "Test Task".to_string(),
                "{}".to_string(), // source_config
                "{}".to_string(), // sink_config
            )),
            shards: vec![],
            request_id: "create-command-test".into(),
            actor: "test".into(),
        };

        // task_id is generated, so we check the prefix
        assert!(cmd.description().starts_with("CreateTask("));
    }
}
