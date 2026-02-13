// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! gRPC server implementation

use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, warn};

use crate::error::{DatspoolError, Result};
use super::generated::{cluster, task, log};

/// gRPC server that implements all Dataspool services
pub struct GrpcServer {
    bind_addr: String,
    cluster_service: Arc<RwLock<ClusterServiceImpl>>,
    task_service: Arc<RwLock<TaskServiceImpl>>,
    log_service: Arc<RwLock<LogServiceImpl>>,
}

impl GrpcServer {
    pub async fn new(bind_addr: &str) -> Result<Self> {
        Ok(Self {
            bind_addr: bind_addr.to_string(),
            cluster_service: Arc::new(RwLock::new(ClusterServiceImpl)),
            task_service: Arc::new(RwLock::new(TaskServiceImpl)),
            log_service: Arc::new(RwLock::new(LogServiceImpl)),
        })
    }

    /// Start the gRPC server (blocking)
    pub async fn serve(self) -> Result<()> {
        let addr = self.bind_addr.parse()
            .map_err(|e| DatspoolError::Config(
                crate::error::ConfigError::Invalid(format!("Invalid bind address {}: {}", self.bind_addr, e))
            ))?;

        info!("Starting gRPC server on {}", addr);

        Server::builder()
            .add_service(cluster::cluster_service_server::ClusterServiceServer::new(
                self.cluster_service.clone()
            ))
            .add_service(task::task_service_server::TaskServiceServer::new(
                self.task_service.clone()
            ))
            .add_service(log::log_service_server::LogServiceServer::new(
                self.log_service.clone()
            ))
            .serve(addr)
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
        warn!("RegisterNode called (not implemented yet): node_id={}", req.node_id);
        
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
        warn!("Heartbeat called (not implemented yet): node_id={}", req.node_id);
        
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

#[derive(Default)]
struct TaskServiceImpl;

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
        warn!("GetTask called (not implemented yet): task_id={}", req.task_id);
        
        Err(Status::unimplemented("Not implemented yet"))
    }

    async fn assign_shard(
        &self,
        request: Request<task::AssignShardRequest>,
    ) -> std::result::Result<Response<task::AssignShardResponse>, Status> {
        let req = request.into_inner();
        warn!("AssignShard called (not implemented yet): shard_id={}", req.shard_id);
        
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
        warn!("ReportProgress called (not implemented yet): shard_id={}", req.shard_id);
        
        Ok(Response::new(task::ReportProgressResponse {
            accepted: false,
        }))
    }

    async fn complete_shard(
        &self,
        request: Request<task::CompleteShardRequest>,
    ) -> std::result::Result<Response<task::CompleteShardResponse>, Status> {
        let req = request.into_inner();
        warn!("CompleteShard called (not implemented yet): shard_id={}", req.shard_id);
        
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
        warn!("FailShard called (not implemented yet): shard_id={}", req.shard_id);
        
        Ok(Response::new(task::FailShardResponse {
            should_retry: false,
        }))
    }
}

// ==================== LogService Implementation ====================

#[derive(Default)]
struct LogServiceImpl;

#[tonic::async_trait]
impl log::log_service_server::LogService for Arc<RwLock<LogServiceImpl>> {
    type StreamLogsStream = tokio_stream::wrappers::ReceiverStream<
        std::result::Result<log::LogEntry, Status>
    >;

    async fn stream_logs(
        &self,
        request: Request<log::StreamLogsRequest>,
    ) -> std::result::Result<Response<Self::StreamLogsStream>, Status> {
        let req = request.into_inner();
        warn!("StreamLogs called (not implemented yet): task_id={}", req.task_id);
        
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}
