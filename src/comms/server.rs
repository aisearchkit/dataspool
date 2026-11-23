// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! gRPC server implementation

use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, transport::Server};
use tracing::{info, warn};

use crate::raft::RaftNode;
use crate::storage::RocksDbStore;
use crate::task::{ShardMeta, TaskMeta, TaskStore};
use crate::types::{ShardState, TaskState};

use super::generated::{cluster, log, task};
use crate::error::{DatspoolError, Result};

/// gRPC server that implements all Dataspool services
pub struct GrpcServer {
    bind_addr: String,
    cluster_service: Arc<RwLock<ClusterServiceImpl>>,
    task_service: Arc<RwLock<TaskServiceImpl>>,
    log_service: Arc<RwLock<LogServiceImpl>>,
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
        })
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

        Server::builder()
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

#[tonic::async_trait]
impl task::task_service_server::TaskService for Arc<RwLock<TaskServiceImpl>> {
    async fn create_task(
        &self,
        request: Request<task::CreateTaskRequest>,
    ) -> std::result::Result<Response<task::CreateTaskResponse>, Status> {
        let req = request.into_inner();
        warn!("CreateTask called (not implemented yet): name={}", req.name);

        Ok(Response::new(task::CreateTaskResponse {
            success: false,
            task_id: String::new(),
            message: "Not implemented yet".to_string(),
        }))
    }

    async fn get_task(
        &self,
        request: Request<task::GetTaskRequest>,
    ) -> std::result::Result<Response<task::GetTaskResponse>, Status> {
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
