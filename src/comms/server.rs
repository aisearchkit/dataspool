// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! gRPC server implementation

use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{
    Request, Response, Status,
    transport::{Certificate, Identity, Server, ServerTlsConfig},
};
use tracing::{info, warn};

use crate::raft::RaftNode;
use crate::storage::RocksDbStore;
use crate::task::{ShardGenerator, ShardMeta, ShardingStrategy, TaskMeta, TaskPriority, TaskStore};
use crate::types::{ShardState, TaskState};

use super::generated::{cluster, log, task};
use crate::error::{DatspoolError, Result};

/// gRPC server that implements all Dataspool services
pub struct GrpcServer {
    bind_addr: String,
    cluster_service: Arc<RwLock<ClusterServiceImpl>>,
    task_service: Arc<RwLock<TaskServiceImpl>>,
    log_service: Arc<RwLock<LogServiceImpl>>,
    tls: Option<ServerTlsConfig>,
}

impl GrpcServer {
    pub async fn new(
        bind_addr: &str,
        task_store: Arc<TaskStore<RocksDbStore>>,
        raft_node: Option<Arc<RaftNode>>,
    ) -> Result<Self> {
        Ok(Self {
            bind_addr: bind_addr.to_string(),
            cluster_service: Arc::new(RwLock::new(ClusterServiceImpl)),
            task_service: Arc::new(RwLock::new(TaskServiceImpl {
                task_store,
                raft_node,
            })),
            log_service: Arc::new(RwLock::new(LogServiceImpl)),
            tls: None,
        })
    }

    /// Require client certificates signed by the configured federation CA.
    pub async fn with_mtls_files(
        mut self,
        certificate_file: &std::path::Path,
        private_key_file: &std::path::Path,
        client_ca_file: &std::path::Path,
    ) -> Result<Self> {
        let certificate = tokio::fs::read(certificate_file).await.map_err(|e| {
            DatspoolError::Configuration(format!("Unable to read gRPC certificate: {e}"))
        })?;
        let private_key = tokio::fs::read(private_key_file).await.map_err(|e| {
            DatspoolError::Configuration(format!("Unable to read gRPC private key: {e}"))
        })?;
        let client_ca = tokio::fs::read(client_ca_file).await.map_err(|e| {
            DatspoolError::Configuration(format!("Unable to read gRPC client CA: {e}"))
        })?;
        self.tls = Some(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(certificate, private_key))
                .client_ca_root(Certificate::from_pem(client_ca)),
        );
        Ok(self)
    }

    /// Start the gRPC server until the process receives its shutdown signal.
    pub async fn serve(self) -> Result<()> {
        self.serve_until_cancelled(crate::CancellationToken::new())
            .await
    }

    /// Start the gRPC server with coordinated graceful shutdown.
    pub async fn serve_until_cancelled(self, shutdown: crate::CancellationToken) -> Result<()> {
        let addr = self.bind_addr.parse().map_err(|e| {
            DatspoolError::Config(crate::error::ConfigError::Invalid(format!(
                "Invalid bind address {}: {}",
                self.bind_addr, e
            )))
        })?;

        info!("Starting gRPC server on {}", addr);

        let mut builder = Server::builder();
        if let Some(tls) = self.tls {
            builder = builder.tls_config(tls).map_err(|e| {
                DatspoolError::Configuration(format!("Invalid gRPC mTLS configuration: {e}"))
            })?;
        }
        builder
            .add_service(cluster::cluster_service_server::ClusterServiceServer::new(
                self.cluster_service.clone(),
            ))
            .add_service(task::task_service_server::TaskServiceServer::new(
                self.task_service.clone(),
            ))
            .add_service(log::log_service_server::LogServiceServer::new(
                self.log_service.clone(),
            ))
            .serve_with_shutdown(addr, shutdown.cancelled_owned())
            .await
            .map_err(|e| DatspoolError::Internal(format!("gRPC server error: {}", e)))?;

        Ok(())
    }
}

fn request_actor<T>(request: &Request<T>) -> String {
    request
        .metadata()
        .get("x-dataspool-actor")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty() && value.len() <= 128)
        .unwrap_or("anonymous")
        .to_string()
}

fn raft_status(error: impl std::fmt::Display) -> Status {
    Status::unavailable(format!("Raft command could not be committed: {error}"))
}

async fn verify_peer_identity<T>(
    service: &Arc<RwLock<TaskServiceImpl>>,
    request: &Request<T>,
) -> std::result::Result<Option<String>, Status> {
    let Some(peer) = request.metadata().get("x-dataspool-peer-cluster") else {
        if request.peer_certs().is_some() {
            return Err(Status::unauthenticated(
                "mTLS task RPCs must identify a registered federation peer",
            ));
        }
        return Ok(None);
    };
    let peer_cluster = peer
        .to_str()
        .map_err(|_| Status::unauthenticated("Invalid peer cluster identity"))?;
    if peer_cluster.trim().is_empty() || peer_cluster.len() > 128 {
        return Err(Status::unauthenticated("Invalid peer cluster identity"));
    }
    let certs = request.peer_certs().ok_or_else(|| {
        Status::unauthenticated("Federated RPC requires an mTLS client certificate")
    })?;
    let cert = certs
        .first()
        .ok_or_else(|| Status::unauthenticated("mTLS client certificate is missing"))?;
    let fingerprint = format!("{:x}", Sha256::digest(cert.as_ref()));
    let (store, raft) = {
        let service = service.read().await;
        (service.task_store.clone(), service.raft_node.clone())
    };
    let raft = raft.ok_or_else(|| Status::unavailable("Federated RPC requires Raft"))?;
    raft.ensure_linearizable().await.map_err(raft_status)?;
    let registered = store
        .get_federated_cluster(peer_cluster)
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .ok_or_else(|| Status::unauthenticated("Peer cluster is not registered"))?;
    if !registered
        .identity_fingerprint
        .eq_ignore_ascii_case(&fingerprint)
    {
        return Err(Status::unauthenticated(
            "Peer certificate fingerprint does not match the pinned cluster identity",
        ));
    }
    let now = chrono::Utc::now().timestamp_millis();
    if registered.status != crate::federation::FederatedClusterStatus::Healthy
        || now.saturating_sub(registered.last_seen_at_ms) >= 15_000
    {
        raft.propose(crate::raft::Command::HeartbeatFederatedCluster {
            cluster_id: peer_cluster.to_string(),
            identity_fingerprint: fingerprint,
            status: crate::federation::FederatedClusterStatus::Healthy,
            last_seen_at_ms: now,
            request_id: format!("peer-heartbeat:{peer_cluster}:{}", now / 15_000),
            actor: "authenticated-peer".into(),
        })
        .await
        .map_err(raft_status)?;
    }
    Ok(Some(peer_cluster.to_string()))
}

// ==================== ClusterService Implementation ====================

#[derive(Default)]
struct ClusterServiceImpl;

#[tonic::async_trait]
impl cluster::cluster_service_server::ClusterService for Arc<RwLock<ClusterServiceImpl>> {
    async fn register_node(
        &self,
        request: Request<cluster::RegisterNodeRequest>,
    ) -> std::result::Result<Response<cluster::RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        warn!(
            "RegisterNode called (not implemented yet): node_id={}",
            req.node_id
        );

        Ok(Response::new(cluster::RegisterNodeResponse {
            success: false,
            message: "Not implemented yet".to_string(),
            leader_id: String::new(),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<cluster::HeartbeatRequest>,
    ) -> std::result::Result<Response<cluster::HeartbeatResponse>, Status> {
        let req = request.into_inner();
        warn!(
            "Heartbeat called (not implemented yet): node_id={}",
            req.node_id
        );

        Ok(Response::new(cluster::HeartbeatResponse {
            accepted: false,
            leader_id: String::new(),
        }))
    }

    async fn get_cluster_info(
        &self,
        _request: Request<cluster::ClusterInfoRequest>,
    ) -> std::result::Result<Response<cluster::ClusterInfoResponse>, Status> {
        warn!("GetClusterInfo called (not implemented yet)");

        Ok(Response::new(cluster::ClusterInfoResponse {
            leader_id: String::new(),
            nodes: vec![],
            term: 0,
        }))
    }
}

// ==================== TaskService Implementation ====================

struct TaskServiceImpl {
    task_store: Arc<TaskStore<RocksDbStore>>,
    raft_node: Option<Arc<RaftNode>>,
}

async fn control_task(
    service: &Arc<RwLock<TaskServiceImpl>>,
    req: task::TaskControlRequest,
    action: &str,
    actor: String,
) -> std::result::Result<Response<task::TaskControlResponse>, Status> {
    if req.request_id.trim().is_empty() {
        return Err(Status::invalid_argument("request_id is required"));
    }
    let task_id: crate::types::TaskId = req
        .task_id
        .parse()
        .map_err(|e| Status::invalid_argument(format!("Invalid task_id: {e}")))?;
    let raft = service
        .read()
        .await
        .raft_node
        .clone()
        .ok_or_else(|| Status::unavailable("Task mutations require Raft"))?;
    let result = raft
        .propose(crate::raft::Command::ControlTask {
            task_id: task_id.to_string(),
            action: action.to_string(),
            request_id: req.request_id,
            actor,
        })
        .await
        .map_err(raft_status)?;
    Ok(Response::new(task::TaskControlResponse {
        success: result.success,
        message: result.message,
    }))
}

#[tonic::async_trait]
impl task::task_service_server::TaskService for Arc<RwLock<TaskServiceImpl>> {
    async fn create_task(
        &self,
        request: Request<task::CreateTaskRequest>,
    ) -> std::result::Result<Response<task::CreateTaskResponse>, Status> {
        let actor = verify_peer_identity(self, &request)
            .await?
            .map(|peer| format!("federation:{peer}"))
            .unwrap_or_else(|| request_actor(&request));
        let req = request.into_inner();
        if req.name.trim().is_empty()
            || req.request_id.trim().is_empty()
            || req.request_id.len() > 128
        {
            return Err(Status::invalid_argument(
                "name is required and request_id must contain 1-128 characters",
            ));
        }
        let raft = self.read().await.raft_node.clone().ok_or_else(|| {
            Status::unavailable("Task mutations require an initialized Raft control plane")
        })?;
        let strategy = if req.sharding_strategy.trim().is_empty() {
            ShardingStrategy::None
        } else {
            serde_json::from_str(&req.sharding_strategy)
                .map_err(|e| Status::invalid_argument(format!("Invalid sharding_strategy: {e}")))?
        };
        let deterministic_id = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            format!("dataspool:create-task:{}", req.request_id).as_bytes(),
        );
        let mut task_meta = TaskMeta::new(req.name, req.source_config, req.sink_config);
        task_meta.task_id = crate::types::TaskId::from_uuid(deterministic_id);
        task_meta.priority = match req.priority {
            0 => TaskPriority::Low,
            1 => TaskPriority::Normal,
            2 => TaskPriority::High,
            3 => TaskPriority::Critical,
            _ => return Err(Status::invalid_argument("priority must be between 0 and 3")),
        };
        task_meta.transform_configs =
            (!req.transform_config.trim().is_empty()).then_some(req.transform_config);
        task_meta.sharding_config = (!req.sharding_strategy.trim().is_empty()).then_some(
            serde_json::to_string(&strategy).map_err(|e| Status::internal(e.to_string()))?,
        );
        task_meta.metadata = req.tags;
        let shards =
            ShardGenerator::generate(task_meta.task_id, &strategy, &task_meta.source_config);
        let task_id = task_meta.task_id.to_string();
        let response = raft
            .propose(crate::raft::Command::CreateTask {
                request_id: req.request_id,
                actor,
                task: Box::new(task_meta),
                shards,
            })
            .await
            .map_err(raft_status)?;
        Ok(Response::new(task::CreateTaskResponse {
            success: response.success,
            task_id: if response.success {
                task_id
            } else {
                String::new()
            },
            message: response.message,
        }))
    }

    async fn pause_task(
        &self,
        request: Request<task::TaskControlRequest>,
    ) -> std::result::Result<Response<task::TaskControlResponse>, Status> {
        let actor = verify_peer_identity(self, &request)
            .await?
            .map(|peer| format!("federation:{peer}"))
            .unwrap_or_else(|| request_actor(&request));
        control_task(self, request.into_inner(), "pause", actor).await
    }
    async fn resume_task(
        &self,
        request: Request<task::TaskControlRequest>,
    ) -> std::result::Result<Response<task::TaskControlResponse>, Status> {
        let actor = verify_peer_identity(self, &request)
            .await?
            .map(|peer| format!("federation:{peer}"))
            .unwrap_or_else(|| request_actor(&request));
        control_task(self, request.into_inner(), "resume", actor).await
    }
    async fn cancel_task(
        &self,
        request: Request<task::TaskControlRequest>,
    ) -> std::result::Result<Response<task::TaskControlResponse>, Status> {
        let actor = verify_peer_identity(self, &request)
            .await?
            .map(|peer| format!("federation:{peer}"))
            .unwrap_or_else(|| request_actor(&request));
        control_task(self, request.into_inner(), "cancel", actor).await
    }
    async fn retry_task(
        &self,
        request: Request<task::TaskControlRequest>,
    ) -> std::result::Result<Response<task::TaskControlResponse>, Status> {
        let actor = verify_peer_identity(self, &request)
            .await?
            .map(|peer| format!("federation:{peer}"))
            .unwrap_or_else(|| request_actor(&request));
        let req = request.into_inner();
        if req.request_id.trim().is_empty() {
            return Err(Status::invalid_argument("request_id is required"));
        }
        let task_id: crate::types::TaskId = req
            .task_id
            .parse()
            .map_err(|e| Status::invalid_argument(format!("Invalid task_id: {e}")))?;
        let raft = self
            .read()
            .await
            .raft_node
            .clone()
            .ok_or_else(|| Status::unavailable("Task mutations require Raft"))?;
        let result = raft
            .propose(crate::raft::Command::RetryTask {
                task_id: task_id.to_string(),
                request_id: req.request_id,
                actor,
            })
            .await
            .map_err(raft_status)?;
        Ok(Response::new(task::TaskControlResponse {
            success: result.success,
            message: result.message,
        }))
    }
    async fn control_node_execution(
        &self,
        request: Request<task::NodeExecutionControlRequest>,
    ) -> std::result::Result<Response<task::NodeExecutionControlResponse>, Status> {
        let actor = request_actor(&request);
        let req = request.into_inner();
        if req.node_id.trim().is_empty() || req.request_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "node_id and request_id are required",
            ));
        }
        let raft = self
            .read()
            .await
            .raft_node
            .clone()
            .ok_or_else(|| Status::unavailable("Node control requires Raft"))?;
        let result = raft
            .propose(crate::raft::Command::ControlNodeExecution {
                node_id: req.node_id,
                paused: req.paused,
                request_id: req.request_id,
                actor,
            })
            .await
            .map_err(raft_status)?;
        Ok(Response::new(task::NodeExecutionControlResponse {
            success: result.success,
            message: result.message,
        }))
    }

    async fn get_task(
        &self,
        request: Request<task::GetTaskRequest>,
    ) -> std::result::Result<Response<task::GetTaskResponse>, Status> {
        let _peer = verify_peer_identity(self, &request).await?;
        let req = request.into_inner();
        let task_id = req.task_id.parse().map_err(|error| {
            Status::invalid_argument(format!("Invalid task_id '{}': {error}", req.task_id))
        })?;
        let service = self.read().await;
        let task_store = service.task_store.clone();
        let raft_node = service.raft_node.clone();
        drop(service);
        if let Some(raft) = raft_node {
            let leader = raft.current_leader().await;
            if leader != Some(raft.node_id()) {
                if let Some(leader) = leader {
                    return Err(Status::failed_precondition(format!(
                        "GetTask requires the current Raft leader for a linearizable read; leader_id={leader}"
                    )));
                }
                return Err(Status::unavailable(
                    "Raft cluster has no elected leader; retry GetTask",
                ));
            }
            raft.ensure_linearizable().await.map_err(|error| {
                Status::unavailable(format!("Raft read barrier failed: {error}"))
            })?;
        }
        let Some((task, shards)) = task_store
            .get_task_with_aggregate_and_shards(&task_id)
            .await
            .map_err(|error| Status::internal(format!("Failed to read task: {error}")))?
        else {
            return Err(Status::not_found(format!(
                "Task '{}' not found",
                req.task_id
            )));
        };
        Ok(Response::new(task_to_response(task, shards)))
    }

    async fn list_tasks(
        &self,
        request: Request<task::ListTasksRequest>,
    ) -> std::result::Result<Response<task::ListTasksResponse>, Status> {
        let req = request.into_inner();
        let service = self.read().await;
        let task_store = service.task_store.clone();
        let raft_node = service.raft_node.clone();
        drop(service);
        if let Some(raft) = raft_node {
            if raft.current_leader().await != Some(raft.node_id()) {
                return Err(Status::failed_precondition(
                    "ListTasks requires the current Raft leader",
                ));
            }
            raft.ensure_linearizable().await.map_err(|error| {
                Status::unavailable(format!("Raft read barrier failed: {error}"))
            })?;
        }
        let mut tasks = task_store
            .list_tasks()
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        if !req.state.trim().is_empty() {
            let expected = req.state.to_ascii_lowercase();
            tasks.retain(|task| task_state_name(&task.state).eq_ignore_ascii_case(&expected));
        }
        if !req.tag_key.is_empty() {
            tasks.retain(|task| {
                task.metadata
                    .get(&req.tag_key)
                    .is_some_and(|value| req.tag_value.is_empty() || value == &req.tag_value)
            });
        }
        match req.sort_by.as_str() {
            "priority" => tasks.sort_by_key(|task| task.priority),
            "name" => tasks.sort_by(|a, b| a.name.cmp(&b.name)),
            "" | "created_at" => tasks.sort_by_key(|task| task.created_at),
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unsupported sort_by '{other}'"
                )));
            }
        }
        if req.descending {
            tasks.reverse();
        }
        let total = tasks.len() as u64;
        let page = req.page.max(1);
        let page_size = if req.page_size == 0 {
            20
        } else {
            req.page_size.clamp(1, 100)
        };
        let start = (page as usize - 1).saturating_mul(page_size as usize);
        let mut summaries = Vec::new();
        for mut task_meta in tasks.into_iter().skip(start).take(page_size as usize) {
            let shards = task_store
                .list_task_shards(&task_meta.task_id)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
            task_meta.refresh_from_shards(&shards, chrono::Utc::now());
            summaries.push(
                task_to_response(task_meta, Vec::new())
                    .task
                    .expect("task summary"),
            );
        }
        Ok(Response::new(task::ListTasksResponse {
            tasks: summaries,
            total,
            page,
            page_size,
        }))
    }

    async fn assign_shard(
        &self,
        request: Request<task::AssignShardRequest>,
    ) -> std::result::Result<Response<task::AssignShardResponse>, Status> {
        let req = request.into_inner();
        warn!(
            "AssignShard called (not implemented yet): shard_id={}",
            req.shard_id
        );

        Ok(Response::new(task::AssignShardResponse {
            success: false,
            message: "Not implemented yet".to_string(),
            shard_config: String::new(),
        }))
    }

    async fn report_progress(
        &self,
        request: Request<task::ReportProgressRequest>,
    ) -> std::result::Result<Response<task::ReportProgressResponse>, Status> {
        let req = request.into_inner();
        warn!(
            "ReportProgress called (not implemented yet): shard_id={}",
            req.shard_id
        );

        Ok(Response::new(task::ReportProgressResponse {
            accepted: false,
        }))
    }

    async fn complete_shard(
        &self,
        request: Request<task::CompleteShardRequest>,
    ) -> std::result::Result<Response<task::CompleteShardResponse>, Status> {
        let req = request.into_inner();
        warn!(
            "CompleteShard called (not implemented yet): shard_id={}",
            req.shard_id
        );

        Ok(Response::new(task::CompleteShardResponse {
            success: false,
            has_next_shard: false,
            next_shard_id: String::new(),
        }))
    }

    async fn fail_shard(
        &self,
        request: Request<task::FailShardRequest>,
    ) -> std::result::Result<Response<task::FailShardResponse>, Status> {
        let req = request.into_inner();
        warn!(
            "FailShard called (not implemented yet): shard_id={}",
            req.shard_id
        );

        Ok(Response::new(task::FailShardResponse {
            should_retry: false,
        }))
    }

    async fn prepare_federated_commit(
        &self,
        request: Request<task::FederatedOperationRequest>,
    ) -> std::result::Result<Response<task::FederatedOperationResponse>, Status> {
        let peer = verify_peer_identity(self, &request).await?.ok_or_else(|| {
            Status::unauthenticated("Federated commit requires an authenticated peer")
        })?;
        let req = request.into_inner();
        if req.migration_id.trim().is_empty()
            || req.request_id.trim().is_empty()
            || req.request_id.len() > 128
        {
            return Err(Status::invalid_argument(
                "migration_id and request_id are required",
            ));
        }
        let task_id: crate::types::TaskId = req
            .task_id
            .parse()
            .map_err(|e| Status::invalid_argument(format!("Invalid task_id: {e}")))?;
        let (store, raft) = {
            let service = self.read().await;
            (service.task_store.clone(), service.raft_node.clone())
        };
        let raft = raft.ok_or_else(|| Status::unavailable("Federated commit requires Raft"))?;
        let (mut task_meta, shards) = store
            .get_task_with_aggregate_and_shards(&task_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Target task not found"))?;
        task_meta.refresh_from_shards(&shards, chrono::Utc::now());
        if task_meta.metadata.get("_dataspool_federation_migration_id") != Some(&req.migration_id)
            || task_meta.metadata.get("_dataspool_federation_generation")
                != Some(&req.generation.to_string())
        {
            return Err(Status::failed_precondition(
                "Target task is not tagged for this migration generation",
            ));
        }
        if !matches!(task_meta.state, TaskState::Completed { .. })
            || shards.is_empty()
            || shards
                .iter()
                .any(|shard| !matches!(shard.state, ShardState::Completed { .. }))
        {
            return Err(Status::failed_precondition(
                "Target task must have every shard completed before commit preparation",
            ));
        }
        let checkpoint_bytes = serde_json::to_vec(&task_meta.aggregate.checkpoints)
            .map_err(|e| Status::internal(e.to_string()))?;
        let checkpoint_digest = format!("{:x}", Sha256::digest(checkpoint_bytes));
        let operation = crate::federation::FederatedRemoteOperation {
            migration_id: req.migration_id,
            source_cluster: peer,
            generation: req.generation,
            task_id: task_id.to_string(),
            state: crate::federation::FederatedRemoteOperationState::Prepared,
            checkpoint_digest: Some(checkpoint_digest.clone()),
            updated_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        let result = raft
            .propose(crate::raft::Command::RecordFederatedRemoteOperation {
                operation: Box::new(operation),
                request_id: req.request_id,
                actor: "federation-peer".into(),
            })
            .await
            .map_err(raft_status)?;
        if !result.success {
            return Err(Status::failed_precondition(result.message));
        }
        Ok(Response::new(task::FederatedOperationResponse {
            accepted: true,
            state: "prepared".into(),
            checkpoint_digest,
            message: result.message,
        }))
    }

    async fn commit_federated_migration(
        &self,
        request: Request<task::FederatedOperationRequest>,
    ) -> std::result::Result<Response<task::FederatedOperationResponse>, Status> {
        let peer = verify_peer_identity(self, &request).await?.ok_or_else(|| {
            Status::unauthenticated("Federated commit requires an authenticated peer")
        })?;
        let req = request.into_inner();
        if req.migration_id.trim().is_empty()
            || req.task_id.trim().is_empty()
            || req.request_id.trim().is_empty()
            || req.request_id.len() > 128
        {
            return Err(Status::invalid_argument(
                "migration_id, task_id and request_id are required",
            ));
        }
        let (store, raft) = {
            let service = self.read().await;
            (service.task_store.clone(), service.raft_node.clone())
        };
        let raft = raft.ok_or_else(|| Status::unavailable("Federated commit requires Raft"))?;
        let mut operation = store
            .get_federated_remote_operation(&req.migration_id, req.generation)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::failed_precondition("Remote migration was not prepared"))?;
        if operation.source_cluster != peer || operation.task_id != req.task_id {
            return Err(Status::unauthenticated(
                "Federated operation identity does not match the authenticated peer",
            ));
        }
        operation.state = crate::federation::FederatedRemoteOperationState::Committed;
        operation.updated_at_ms = chrono::Utc::now().timestamp_millis();
        let result = raft
            .propose(crate::raft::Command::RecordFederatedRemoteOperation {
                operation: Box::new(operation.clone()),
                request_id: req.request_id,
                actor: "federation-peer".into(),
            })
            .await
            .map_err(raft_status)?;
        if !result.success {
            return Err(Status::failed_precondition(result.message));
        }
        Ok(Response::new(task::FederatedOperationResponse {
            accepted: true,
            state: "committed".into(),
            checkpoint_digest: operation.checkpoint_digest.unwrap_or_default(),
            message: result.message,
        }))
    }

    async fn compensate_federated_migration(
        &self,
        request: Request<task::FederatedOperationRequest>,
    ) -> std::result::Result<Response<task::FederatedOperationResponse>, Status> {
        let peer = verify_peer_identity(self, &request).await?.ok_or_else(|| {
            Status::unauthenticated("Federated compensation requires an authenticated peer")
        })?;
        let req = request.into_inner();
        if req.migration_id.trim().is_empty()
            || req.task_id.trim().is_empty()
            || req.request_id.trim().is_empty()
            || req.request_id.len() > 128
        {
            return Err(Status::invalid_argument(
                "migration_id, task_id and request_id are required",
            ));
        }
        let (store, raft) = {
            let service = self.read().await;
            (service.task_store.clone(), service.raft_node.clone())
        };
        let raft =
            raft.ok_or_else(|| Status::unavailable("Federated compensation requires Raft"))?;
        let task_id: crate::types::TaskId = req
            .task_id
            .parse()
            .map_err(|e| Status::invalid_argument(format!("Invalid task_id: {e}")))?;
        let existing = store
            .get_federated_remote_operation(&req.migration_id, req.generation)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if let Some(operation) = existing.as_ref() {
            if operation.source_cluster != peer || operation.task_id != task_id.to_string() {
                return Err(Status::unauthenticated(
                    "Federated operation identity does not match the authenticated peer",
                ));
            }
            match operation.state {
                crate::federation::FederatedRemoteOperationState::Committed => {
                    return Err(Status::failed_precondition(
                        "Committed migration cannot be compensated",
                    ));
                }
                crate::federation::FederatedRemoteOperationState::Compensated => {
                    return Ok(Response::new(task::FederatedOperationResponse {
                        accepted: true,
                        state: "compensated".into(),
                        checkpoint_digest: operation.checkpoint_digest.clone().unwrap_or_default(),
                        message: "Compensation already committed".into(),
                    }));
                }
                crate::federation::FederatedRemoteOperationState::Prepared => {}
            }
        }
        let (remote_task, mut shards) = store
            .get_task_with_aggregate_and_shards(&task_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Target task not found"))?;
        if remote_task
            .metadata
            .get("_dataspool_federation_migration_id")
            != Some(&req.migration_id)
            || remote_task.metadata.get("_dataspool_federation_generation")
                != Some(&req.generation.to_string())
        {
            return Err(Status::failed_precondition(
                "Target task is not tagged for this migration generation",
            ));
        }
        if !matches!(
            remote_task.state,
            TaskState::Completed { .. } | TaskState::Cancelled { .. } | TaskState::Failed { .. }
        ) {
            let result = raft
                .propose(crate::raft::Command::ControlTask {
                    task_id: task_id.to_string(),
                    action: "cancel".into(),
                    request_id: format!("{}:cancel", req.request_id),
                    actor: "federation-compensation".into(),
                })
                .await
                .map_err(raft_status)?;
            if !result.success {
                return Err(Status::failed_precondition(result.message));
            }
            let (_, refreshed) = store
                .get_task_with_aggregate_and_shards(&task_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Target task disappeared during compensation"))?;
            shards = refreshed;
        }
        if shards
            .iter()
            .any(|shard| matches!(shard.state, ShardState::Running { .. }))
        {
            return Err(Status::unavailable(
                "Target workers are still draining; compensation will be retried",
            ));
        }

        let sink_config: crate::drivers::SinkConfig =
            serde_json::from_str(&remote_task.sink_config).map_err(|error| {
                Status::failed_precondition(format!("Invalid target sink config: {error}"))
            })?;
        let mut sink = crate::drivers::factory::create_sink(&sink_config).map_err(|error| {
            Status::failed_precondition(format!("Target sink cannot be compensated: {error}"))
        })?;
        sink.connect().await.map_err(|error| {
            Status::unavailable(format!(
                "Unable to connect target sink for compensation: {error}"
            ))
        })?;
        let compensated = sink
            .compensate_migration(&req.migration_id, req.generation)
            .await
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "Sink compensation failed and remains recoverable: {error}"
                ))
            });
        let close_result = sink.close().await;
        let deleted = compensated?;
        close_result.map_err(|error| {
            Status::unavailable(format!(
                "Compensation completed but sink close failed: {error}"
            ))
        })?;
        let operation = crate::federation::FederatedRemoteOperation {
            migration_id: req.migration_id,
            source_cluster: peer,
            generation: req.generation,
            task_id: task_id.to_string(),
            state: crate::federation::FederatedRemoteOperationState::Compensated,
            checkpoint_digest: existing.and_then(|value| value.checkpoint_digest),
            updated_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        let result = raft
            .propose(crate::raft::Command::RecordFederatedRemoteOperation {
                operation: Box::new(operation),
                request_id: req.request_id,
                actor: "federation-peer".into(),
            })
            .await
            .map_err(raft_status)?;
        if !result.success {
            return Err(Status::failed_precondition(result.message));
        }
        Ok(Response::new(task::FederatedOperationResponse {
            accepted: true,
            state: "compensated".into(),
            checkpoint_digest: String::new(),
            message: format!(
                "{}; removed {} migration-owned documents",
                result.message, deleted
            ),
        }))
    }
}

fn timestamp_from_system_time(value: std::time::SystemTime) -> Option<prost_types::Timestamp> {
    let duration = value.duration_since(std::time::UNIX_EPOCH).ok()?;
    Some(prost_types::Timestamp {
        seconds: duration.as_secs().min(i64::MAX as u64) as i64,
        nanos: duration.subsec_nanos() as i32,
    })
}

fn timestamp_from_datetime(value: chrono::DateTime<chrono::Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

fn task_state_name(state: &TaskState) -> &'static str {
    match state {
        TaskState::Pending => "Pending",
        TaskState::Running { .. } => "Running",
        TaskState::Pausing => "Pausing",
        TaskState::Paused => "Paused",
        TaskState::Canceling => "Canceling",
        TaskState::Cancelled { .. } => "Cancelled",
        TaskState::Completed { .. } => "Completed",
        TaskState::Failed { .. } => "Failed",
    }
}

fn shard_state_name(state: &ShardState) -> &'static str {
    match state {
        ShardState::Pending => "Pending",
        ShardState::Running { .. } => "Running",
        ShardState::Completed { .. } => "Completed",
        ShardState::Failed { .. } => "Failed",
    }
}

fn task_to_response(task: TaskMeta, shards: Vec<ShardMeta>) -> task::GetTaskResponse {
    let completed_at = match &task.state {
        TaskState::Completed { finished_at, .. } => Some(timestamp_from_datetime(*finished_at)),
        TaskState::Failed { failed_at, .. } => Some(timestamp_from_datetime(*failed_at)),
        TaskState::Cancelled { cancelled_at } => Some(timestamp_from_datetime(*cancelled_at)),
        _ => None,
    };
    let started_at = task
        .started_at
        .map(timestamp_from_datetime)
        .or_else(|| match &task.state {
            TaskState::Running { started_at, .. } => Some(timestamp_from_datetime(*started_at)),
            _ => None,
        });
    let mut nodes = task.aggregate.nodes.into_iter().collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.0.cmp(&right.0));
    let node_infos = nodes
        .into_iter()
        .map(|(node_id, node)| task::NodeTaskInfo {
            node_id,
            pending_shards: node.pending_shards as u64,
            running_shards: node.running_shards as u64,
            completed_shards: node.completed_shards as u64,
            failed_shards: node.failed_shards as u64,
            processed_records: node.docs_processed,
            processed_bytes: node.bytes_processed,
            throughput_docs_per_sec: node.throughput_docs_per_sec,
            throughput_bytes_per_sec: node.throughput_bytes_per_sec,
        })
        .collect();
    let task_info = task::TaskInfo {
        task_id: task.task_id.to_string(),
        name: task.name,
        state: task_state_name(&task.state).to_string(),
        created_at: timestamp_from_system_time(task.created_at),
        started_at,
        completed_at,
        total_shards: task.aggregate.total_shards as u64,
        completed_shards: task.aggregate.completed_shards as u64,
        failed_shards: task.aggregate.failed_shards as u64,
        processed_records: task.aggregate.docs_processed,
        pending_shards: task.aggregate.pending_shards as u64,
        running_shards: task.aggregate.running_shards as u64,
        processed_bytes: task.aggregate.bytes_processed,
        elapsed_ms: task.aggregate.elapsed_ms,
        throughput_docs_per_sec: task.aggregate.throughput_docs_per_sec,
        throughput_bytes_per_sec: task.aggregate.throughput_bytes_per_sec,
        nodes: node_infos,
    };
    let shard_infos = shards
        .into_iter()
        .map(|shard| {
            let assigned_node = match &shard.state {
                ShardState::Running { node_id, .. } => node_id.to_string(),
                _ => shard
                    .last_owner_node_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            };
            task::ShardInfo {
                shard_id: shard.shard_id.to_string(),
                task_id: shard.task_id.to_string(),
                state: shard_state_name(&shard.state).to_string(),
                assigned_node,
                processed_records: shard.records_processed,
                // Total source cardinality is not currently stored in ShardMeta.
                total_records: 0,
                processed_bytes: shard.bytes_processed,
                fencing_token: shard.fencing_token,
                lease_expires_at: shard.lease_expires_at.map(timestamp_from_datetime),
                checkpoint: serde_json::to_string(&shard.offset).unwrap_or_default(),
                updated_at: timestamp_from_system_time(shard.updated_at),
            }
        })
        .collect();
    task::GetTaskResponse {
        task: Some(task_info),
        shards: shard_infos,
    }
}

#[cfg(test)]
mod task_query_tests {
    use super::*;
    use crate::comms::generated::task::task_service_server::TaskService;
    use crate::storage::RocksDbStore;
    use crate::task::{ShardMeta, TaskMeta};
    use crate::types::TaskId;
    use tempfile::tempdir;

    #[tokio::test]
    async fn get_task_returns_live_aggregate_and_shard_checkpoints_over_grpc() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = Arc::new(TaskStore::new(db));
        let task_meta = TaskMeta::new("grpc-query".into(), "{}".into(), "{}".into());
        let task_id = task_meta.task_id;
        let mut shard = ShardMeta::new(task_id, 0, "{}".into());
        let started_at = chrono::Utc::now();
        shard
            .start_with_lease(
                crate::types::NodeId::new("worker-a"),
                started_at,
                started_at + chrono::Duration::seconds(60),
                "grpc-query-lease".into(),
            )
            .unwrap();
        shard.records_processed = 42;
        shard.bytes_processed = 4096;
        shard.offset = crate::pipeline::OffsetValue::DocumentCount(42);
        store.save_task(&task_meta).await.unwrap();
        store.save_shard(&shard).await.unwrap();

        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        let server = GrpcServer::new(&address.to_string(), store, None)
            .await
            .unwrap();
        let shutdown = crate::CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_task = tokio::spawn(async move {
            server.serve_until_cancelled(server_shutdown).await.unwrap();
        });

        let mut client = None;
        for _ in 0..40 {
            if let Ok(connected) = crate::GrpcClient::connect(&address.to_string()).await {
                client = Some(connected);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let mut client = client.expect("TaskService gRPC server did not become ready");
        let response = client
            .task()
            .get_task(task::GetTaskRequest {
                task_id: task_id.to_string(),
            })
            .await
            .unwrap()
            .into_inner();
        let info = response.task.unwrap();
        assert_eq!(info.task_id, task_id.to_string());
        assert_eq!(info.name, "grpc-query");
        assert_eq!(info.total_shards, 1);
        assert_eq!(info.running_shards, 1);
        assert_eq!(info.state, "Running");
        assert!(info.started_at.is_some());
        assert_eq!(info.processed_records, 42);
        assert_eq!(info.processed_bytes, 4096);
        assert_eq!(info.nodes.len(), 1);
        assert_eq!(info.nodes[0].node_id, "worker-a");
        assert_eq!(info.nodes[0].processed_records, 42);
        assert_eq!(response.shards.len(), 1);
        assert_eq!(response.shards[0].processed_records, 42);
        assert!(response.shards[0].fencing_token > 0);
        assert!(response.shards[0].lease_expires_at.is_some());
        assert!(response.shards[0].checkpoint.contains("42"));

        shutdown.cancel();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn get_task_distinguishes_invalid_and_missing_ids() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let service = Arc::new(RwLock::new(TaskServiceImpl {
            task_store: Arc::new(TaskStore::new(db)),
            raft_node: None,
        }));
        let invalid = service
            .get_task(Request::new(task::GetTaskRequest {
                task_id: "not-a-uuid".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(invalid.code(), tonic::Code::InvalidArgument);
        let missing = service
            .get_task(Request::new(task::GetTaskRequest {
                task_id: TaskId::new().to_string(),
            }))
            .await
            .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::NotFound);
    }
    #[tokio::test]
    async fn list_tasks_filters_tags_sorts_priority_and_paginates() {
        let dir = tempdir().unwrap();
        let db = RocksDbStore::open(dir.path()).unwrap();
        let store = Arc::new(TaskStore::new(db));
        let mut low = TaskMeta::new("low-priority".into(), "{}".into(), "{}".into());
        low.priority = TaskPriority::Low;
        low.metadata.insert("environment".into(), "prod".into());
        let mut high = TaskMeta::new("high-priority".into(), "{}".into(), "{}".into());
        high.priority = TaskPriority::Critical;
        high.metadata.insert("environment".into(), "prod".into());
        let mut other = TaskMeta::new("other-environment".into(), "{}".into(), "{}".into());
        other.metadata.insert("environment".into(), "test".into());
        for task_meta in [&low, &high, &other] {
            store.save_task(task_meta).await.unwrap();
        }
        let service = Arc::new(RwLock::new(TaskServiceImpl {
            task_store: store,
            raft_node: None,
        }));
        let response = service
            .list_tasks(Request::new(task::ListTasksRequest {
                page: 1,
                page_size: 1,
                state: "pending".into(),
                tag_key: "environment".into(),
                tag_value: "prod".into(),
                sort_by: "priority".into(),
                descending: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.total, 2);
        assert_eq!(response.page, 1);
        assert_eq!(response.page_size, 1);
        assert_eq!(response.tasks.len(), 1);
        assert_eq!(response.tasks[0].name, "high-priority");
    }
}

// ==================== LogService Implementation ====================

#[derive(Default)]
struct LogServiceImpl;

#[tonic::async_trait]
impl log::log_service_server::LogService for Arc<RwLock<LogServiceImpl>> {
    type StreamLogsStream =
        tokio_stream::wrappers::ReceiverStream<std::result::Result<log::LogEntry, Status>>;

    async fn stream_logs(
        &self,
        request: Request<log::StreamLogsRequest>,
    ) -> std::result::Result<Response<Self::StreamLogsStream>, Status> {
        let req = request.into_inner();
        warn!(
            "StreamLogs called (not implemented yet): task_id={}",
            req.task_id
        );

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}
