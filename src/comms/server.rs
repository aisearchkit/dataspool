// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! gRPC server implementation

use sha2::{Digest, Sha256};
#[cfg(feature = "test-fault-injection")]
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::RwLock;
use tonic::{
    Request, Response, Status,
    transport::{Certificate, Identity, Server, ServerTlsConfig},
};
#[cfg(feature = "test-fault-injection")]
use tower::{Layer, Service};
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
    task_store: Arc<TaskStore<RocksDbStore>>,
    tls_files: Option<(PathBuf, PathBuf, PathBuf)>,
    #[cfg(feature = "test-fault-injection")]
    rotation_proof_ack_drop_marker: Option<PathBuf>,
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
                task_store: task_store.clone(),
                raft_node,
            })),
            task_store,
            tls_files: None,
            log_service: Arc::new(RwLock::new(LogServiceImpl)),
            tls: None,
            #[cfg(feature = "test-fault-injection")]
            rotation_proof_ack_drop_marker: None,
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
        self.tls_files = Some((
            certificate_file.to_path_buf(),
            private_key_file.to_path_buf(),
            client_ca_file.to_path_buf(),
        ));
        Ok(self)
    }

    /// Drop the next successful certificate-proof response after its Raft
    /// command commits. This is available only to process-crash integration tests.
    #[cfg(feature = "test-fault-injection")]
    pub fn with_rotation_proof_ack_drop_marker(mut self, marker: PathBuf) -> Self {
        self.rotation_proof_ack_drop_marker = Some(marker);
        self
    }

    /// Start the gRPC server until the process receives its shutdown signal.
    pub async fn serve(self) -> Result<()> {
        self.serve_until_cancelled(crate::CancellationToken::new())
            .await
    }

    /// Start the gRPC server with coordinated graceful shutdown. When federated CA
    /// bundles change in Raft, the listener drains in-flight RPCs and reloads TLS.
    pub async fn serve_until_cancelled(self, shutdown: crate::CancellationToken) -> Result<()> {
        let addr = self.bind_addr.parse().map_err(|e| {
            DatspoolError::Config(crate::error::ConfigError::Invalid(format!(
                "Invalid bind address {}: {}",
                self.bind_addr, e
            )))
        })?;
        info!("Starting gRPC server on {}", addr);

        loop {
            let tls = self.load_tls_config().await?;
            let tls_fingerprint = self.tls_material_fingerprint().await?;
            #[cfg(feature = "test-fault-injection")]
            let mut builder = Server::builder().layer(DropRotationProofAckLayer::new(
                self.rotation_proof_ack_drop_marker.clone(),
            ));
            #[cfg(not(feature = "test-fault-injection"))]
            let mut builder = Server::builder();
            if let Some(tls) = tls {
                builder = builder.tls_config(tls).map_err(|e| {
                    DatspoolError::Configuration(format!("Invalid gRPC mTLS configuration: {e}"))
                })?;
            }
            let listener_shutdown = shutdown.child_token();
            let server = builder
                .add_service(cluster::cluster_service_server::ClusterServiceServer::new(
                    self.cluster_service.clone(),
                ))
                .add_service(task::task_service_server::TaskServiceServer::new(
                    self.task_service.clone(),
                ))
                .add_service(log::log_service_server::LogServiceServer::new(
                    self.log_service.clone(),
                ))
                .serve_with_shutdown(addr, listener_shutdown.clone().cancelled_owned());
            tokio::pin!(server);
            let mut refresh = tokio::time::interval(std::time::Duration::from_secs(3));
            refresh.tick().await;
            loop {
                tokio::select! {
                    result = &mut server => {
                        result.map_err(|e| DatspoolError::Internal(format!("gRPC server error: {e}")))?;
                        return Ok(());
                    }
                    _ = shutdown.cancelled() => {
                        listener_shutdown.cancel();
                        match tokio::time::timeout(std::time::Duration::from_secs(10), &mut server).await {
                            Ok(Ok(())) => {},
                            Ok(Err(error)) => return Err(DatspoolError::Internal(format!("gRPC shutdown error: {error}"))),
                            Err(_) => warn!("gRPC graceful shutdown exceeded 10 seconds; force-closing listener"),
                        }
                        return Ok(());
                    }
                    _ = refresh.tick(), if self.tls_files.is_some() => {
                        let current = self.tls_material_fingerprint().await?;
                        if current != tls_fingerprint {
                            info!("Federated TLS trust material changed; gracefully restarting gRPC listener");
                            listener_shutdown.cancel();
                            match tokio::time::timeout(std::time::Duration::from_secs(10), &mut server).await {
                                Ok(Ok(())) => {},
                                Ok(Err(error)) => return Err(DatspoolError::Internal(format!("gRPC TLS reload shutdown error: {error}"))),
                                Err(_) => warn!("gRPC connections did not drain within 10 seconds; force-closing listener for TLS reload"),
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn effective_client_ca_pem(&self, configured_ca: &[u8]) -> Result<Vec<u8>> {
        let peers = self.task_store.list_federated_clusters().await?;
        if peers.is_empty() {
            return Ok(configured_ca.to_vec());
        }
        let all_peers_managed = peers.iter().all(|peer| peer.trust_bundle_pem.is_some());
        let mut bundles = Vec::<String>::new();
        if !all_peers_managed {
            bundles.push(String::from_utf8_lossy(configured_ca).into_owned());
        }
        let now = chrono::Utc::now().timestamp_millis();
        for peer in peers {
            if let Some(bundle) = peer.handshake_trust_bundle(now)
                && !bundles.contains(&bundle)
            {
                bundles.push(bundle);
            }
        }
        if bundles.is_empty() {
            return Ok(configured_ca.to_vec());
        }
        Ok(bundles.join("\n").into_bytes())
    }

    async fn load_tls_config(&self) -> Result<Option<ServerTlsConfig>> {
        let Some((certificate_file, private_key_file, client_ca_file)) = &self.tls_files else {
            return Ok(self.tls.clone());
        };
        let certificate = tokio::fs::read(certificate_file).await.map_err(|e| {
            DatspoolError::Configuration(format!("Unable to read gRPC certificate: {e}"))
        })?;
        let private_key = tokio::fs::read(private_key_file).await.map_err(|e| {
            DatspoolError::Configuration(format!("Unable to read gRPC private key: {e}"))
        })?;
        let configured_ca = tokio::fs::read(client_ca_file).await.map_err(|e| {
            DatspoolError::Configuration(format!("Unable to read gRPC client CA: {e}"))
        })?;
        let client_ca = self.effective_client_ca_pem(&configured_ca).await?;
        Ok(Some(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(certificate, private_key))
                .client_ca_root(Certificate::from_pem(client_ca)),
        ))
    }

    async fn tls_material_fingerprint(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        let Some((certificate_file, private_key_file, client_ca_file)) = &self.tls_files else {
            return Ok(String::new());
        };
        let certificate = tokio::fs::read(certificate_file).await.map_err(|e| {
            DatspoolError::Configuration(format!("Unable to read gRPC certificate: {e}"))
        })?;
        let private_key = tokio::fs::read(private_key_file).await.map_err(|e| {
            DatspoolError::Configuration(format!("Unable to read gRPC private key: {e}"))
        })?;
        let configured_ca = tokio::fs::read(client_ca_file).await.map_err(|e| {
            DatspoolError::Configuration(format!("Unable to read gRPC client CA: {e}"))
        })?;
        let client_ca = self.effective_client_ca_pem(&configured_ca).await?;
        let mut digest = Sha256::new();
        digest.update(certificate);
        digest.update(private_key);
        digest.update(client_ca);
        Ok(format!("{:x}", digest.finalize()))
    }
}

#[cfg(feature = "test-fault-injection")]
#[derive(Clone)]
struct DropRotationProofAckLayer {
    marker: Option<Arc<PathBuf>>,
    dropped: Arc<AtomicBool>,
}

#[cfg(feature = "test-fault-injection")]
impl DropRotationProofAckLayer {
    fn new(marker: Option<PathBuf>) -> Self {
        Self {
            marker: marker.map(Arc::new),
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[cfg(feature = "test-fault-injection")]
impl<S> Layer<S> for DropRotationProofAckLayer {
    type Service = DropRotationProofAck<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DropRotationProofAck {
            inner,
            marker: self.marker.clone(),
            dropped: self.dropped.clone(),
        }
    }
}

#[cfg(feature = "test-fault-injection")]
#[derive(Clone)]
struct DropRotationProofAck<S> {
    inner: S,
    marker: Option<Arc<PathBuf>>,
    dropped: Arc<AtomicBool>,
}

#[cfg(feature = "test-fault-injection")]
impl<S, B> Service<tonic::codegen::http::Request<B>> for DropRotationProofAck<S>
where
    S: Service<
            tonic::codegen::http::Request<B>,
            Response = tonic::codegen::http::Response<tonic::body::BoxBody>,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        let path = request.uri().path().to_ascii_lowercase();
        let mut inner = self.inner.clone();
        let marker = self.marker.clone();
        let dropped = self.dropped.clone();
        Box::pin(async move {
            let response = inner.call(request).await?;
            if path.contains("provefederatedcertificaterotation")
                && let Some(marker) = marker.as_ref()
                && !dropped.swap(true, Ordering::SeqCst)
            {
                std::fs::write(marker.as_ref(), b"proof-committed-response-dropped")
                    .expect("persist process-crash ACK-drop marker");
                return Ok(tonic::codegen::http::Response::builder()
                    .status(tonic::codegen::http::StatusCode::SERVICE_UNAVAILABLE)
                    .body(tonic::body::empty_body())
                    .expect("fault response is valid"));
            }
            Ok(response)
        })
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
    if !registered.accepts_fingerprint(&fingerprint, chrono::Utc::now().timestamp_millis()) {
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

fn invert_trust_rotation_event(
    event: crate::federation::FederatedTrustRotationEvent,
) -> crate::federation::FederatedTrustRotationEvent {
    use crate::federation::FederatedTrustRotationEvent as Event;
    match event {
        Event::LocalStaged { fingerprint } => Event::PeerStaged { fingerprint },
        Event::PeerStaged { fingerprint } => Event::LocalStaged { fingerprint },
        Event::LocalDeploymentReady { leaf_fingerprint } => {
            Event::PeerDeploymentReady { leaf_fingerprint }
        }
        Event::PeerDeploymentReady { leaf_fingerprint } => {
            Event::LocalDeploymentReady { leaf_fingerprint }
        }
        Event::AuthorizeActivation => Event::AuthorizeActivation,
        Event::LocalActivated {
            previous_expires_at_ms,
        } => Event::PeerActivated {
            previous_expires_at_ms,
        },
        Event::PeerActivated {
            previous_expires_at_ms,
        } => Event::LocalActivated {
            previous_expires_at_ms,
        },
        Event::LocalRetired => Event::PeerRetired,
        Event::PeerRetired => Event::LocalRetired,
        Event::BeginRollback { reason } => Event::BeginRollback { reason },
        Event::LocalRolledBack => Event::PeerRolledBack,
        Event::PeerRolledBack => Event::LocalRolledBack,
        Event::MarkRecoveryRequired { reason } => Event::MarkRecoveryRequired { reason },
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

    async fn stage_federated_certificate_rotation(
        &self,
        request: Request<task::FederatedCertificateRotationRequest>,
    ) -> std::result::Result<Response<task::FederatedCertificateRotationResponse>, Status> {
        let peer = verify_peer_identity(self, &request).await?.ok_or_else(|| {
            Status::unauthenticated(
                "Certificate rotation requires an authenticated federation peer",
            )
        })?;
        let current_fingerprint = request
            .peer_certs()
            .and_then(|certs| {
                certs
                    .first()
                    .map(|cert| format!("{:x}", Sha256::digest(cert.as_ref())))
            })
            .ok_or_else(|| {
                Status::unauthenticated("Current mTLS client certificate is required")
            })?;
        let req = request.into_inner();
        if req.request_id.trim().is_empty() || req.request_id.len() > 128 {
            return Err(Status::invalid_argument(
                "request_id must contain 1-128 characters",
            ));
        }
        let raft = self
            .read()
            .await
            .raft_node
            .clone()
            .ok_or_else(|| Status::unavailable("Certificate rotation requires Raft"))?;
        let result = raft
            .propose(crate::raft::Command::StageFederatedCertificateRotation {
                cluster_id: peer,
                current_fingerprint,
                new_fingerprint: req.new_fingerprint,
                at_ms: chrono::Utc::now().timestamp_millis(),
                grace_period_ms: req.grace_period_ms,
                request_id: req.request_id,
                actor: "federation-certificate-rotation".into(),
            })
            .await
            .map_err(raft_status)?;
        Ok(Response::new(task::FederatedCertificateRotationResponse {
            accepted: result.success,
            state: if result.success { "staged" } else { "rejected" }.into(),
            previous_fingerprint_expires_at_ms: 0,
            message: result.message,
        }))
    }

    async fn prove_federated_certificate_rotation(
        &self,
        request: Request<task::FederatedCertificateRotationRequest>,
    ) -> std::result::Result<Response<task::FederatedCertificateRotationResponse>, Status> {
        let peer_header = request
            .metadata()
            .get("x-dataspool-peer-cluster")
            .ok_or_else(|| Status::unauthenticated("Peer cluster identity is required"))?;
        let peer = peer_header
            .to_str()
            .map_err(|_| Status::unauthenticated("Invalid peer cluster identity"))?
            .to_string();
        if peer.trim().is_empty() || peer.len() > 128 {
            return Err(Status::unauthenticated("Invalid peer cluster identity"));
        }
        let fingerprint = request
            .peer_certs()
            .and_then(|certs| {
                certs
                    .first()
                    .map(|cert| format!("{:x}", Sha256::digest(cert.as_ref())))
            })
            .ok_or_else(|| Status::unauthenticated("New mTLS client certificate is required"))?;
        let req = request.into_inner();
        if req.request_id.trim().is_empty() || req.request_id.len() > 128 {
            return Err(Status::invalid_argument(
                "request_id must contain 1-128 characters",
            ));
        }
        if !req.new_fingerprint.eq_ignore_ascii_case(&fingerprint) {
            return Err(Status::unauthenticated(
                "Proof request fingerprint does not match the presented certificate",
            ));
        }
        let (store, raft) = {
            let service = self.read().await;
            (service.task_store.clone(), service.raft_node.clone())
        };
        let raft = raft.ok_or_else(|| Status::unavailable("Certificate rotation requires Raft"))?;
        raft.ensure_linearizable().await.map_err(raft_status)?;
        let registered = store
            .get_federated_cluster(&peer)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::unauthenticated("Peer cluster is not registered"))?;
        if registered
            .identity_fingerprint
            .eq_ignore_ascii_case(&fingerprint)
            && registered.previous_identity_fingerprint.is_some()
            && registered.previous_identity_expires_at_ms.is_some()
        {
            return Ok(Response::new(task::FederatedCertificateRotationResponse {
                accepted: true,
                state: "active_with_overlap".into(),
                previous_fingerprint_expires_at_ms: registered
                    .previous_identity_expires_at_ms
                    .unwrap_or_default(),
                message: "Certificate rotation was already completed".into(),
            }));
        }
        if !registered
            .pending_identity_fingerprint
            .as_deref()
            .is_some_and(|pending| pending.eq_ignore_ascii_case(&fingerprint))
        {
            return Err(Status::unauthenticated(
                "Presented certificate is not the staged rotation identity",
            ));
        }
        let now = chrono::Utc::now().timestamp_millis();
        let grace_period_ms = registered
            .pending_rotation_grace_period_ms
            .unwrap_or_default();
        let result = raft
            .propose(crate::raft::Command::CompleteFederatedCertificateRotation {
                cluster_id: peer,
                new_fingerprint: fingerprint,
                at_ms: now,
                request_id: req.request_id,
                actor: "federation-certificate-proof".into(),
            })
            .await
            .map_err(raft_status)?;
        Ok(Response::new(task::FederatedCertificateRotationResponse {
            accepted: result.success,
            state: if result.success {
                "active_with_overlap"
            } else {
                "rejected"
            }
            .into(),
            previous_fingerprint_expires_at_ms: now.saturating_add(grace_period_ms),
            message: result.message,
        }))
    }

    async fn create_federated_trust_rotation(
        &self,
        request: Request<task::FederatedTrustRotationRequest>,
    ) -> std::result::Result<Response<task::FederatedTrustRotationResponse>, Status> {
        let peer = verify_peer_identity(self, &request).await?.ok_or_else(|| {
            Status::unauthenticated("Trust rotation requires an authenticated federation peer")
        })?;
        let req = request.into_inner();
        if req.rotation_id.trim().is_empty()
            || req.rotation_id.len() > 96
            || req.request_id.trim().is_empty()
            || req.request_id.len() > 128
            || !(60_000..=604_800_000).contains(&req.overlap_ms)
        {
            return Err(Status::invalid_argument(
                "rotation_id, request_id and a 1-minute to 7-day overlap are required",
            ));
        }
        let local_ca_fingerprint =
            crate::federation::validate_trust_bundle(&req.peer_ca_bundle_pem)
                .map_err(Status::invalid_argument)?;
        let peer_ca_fingerprint =
            crate::federation::validate_trust_bundle(&req.local_ca_bundle_pem)
                .map_err(Status::invalid_argument)?;
        let (store, raft) = {
            let service = self.read().await;
            (service.task_store.clone(), service.raft_node.clone())
        };
        let raft = raft.ok_or_else(|| Status::unavailable("Trust rotation requires Raft"))?;
        raft.ensure_linearizable().await.map_err(raft_status)?;
        let registered_peers = store
            .list_federated_clusters()
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        if registered_peers
            .iter()
            .any(|registered| registered.trust_bundle_pem.is_none())
        {
            return Err(Status::failed_precondition(
                "all registered federation peers must have Raft-managed CA bundles before rotation; static CA fallback would retain trust in a retired root",
            ));
        }
        let registered = store
            .get_federated_cluster(&peer)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::unauthenticated("Peer cluster is not registered"))?;
        if registered.trust_bundle_fingerprint.is_none() {
            return Err(Status::failed_precondition(
                "peer CA trust must be bootstrapped before bilateral rotation",
            ));
        }
        let now = chrono::Utc::now().timestamp_millis();
        let rotation = crate::federation::FederatedTrustRotation {
            rotation_id: req.rotation_id.clone(),
            peer_cluster_id: peer.clone(),
            current_peer_ca_fingerprint: registered.trust_bundle_fingerprint.clone(),
            local_ca_bundle_pem: req.peer_ca_bundle_pem,
            local_ca_bundle_fingerprint: local_ca_fingerprint,
            peer_ca_bundle_pem: req.local_ca_bundle_pem,
            peer_ca_bundle_fingerprint: peer_ca_fingerprint,
            local_leaf_fingerprint: req.peer_leaf_fingerprint,
            peer_leaf_fingerprint: req.local_leaf_fingerprint.clone(),
            overlap_ms: req.overlap_ms,
            phase: crate::federation::FederatedTrustRotationPhase::Staging,
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
        let created = raft
            .propose(crate::raft::Command::CreateFederatedTrustRotation {
                rotation: Box::new(rotation),
                at_ms: now,
                request_id: req.request_id.clone(),
                actor: format!("federation-peer:{peer}"),
            })
            .await
            .map_err(raft_status)?;
        if !registered
            .pending_identity_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| {
                fingerprint.eq_ignore_ascii_case(&req.local_leaf_fingerprint)
            })
            && !registered
                .identity_fingerprint
                .eq_ignore_ascii_case(&req.local_leaf_fingerprint)
        {
            let staged_identity = raft
                .propose(crate::raft::Command::StageFederatedCertificateRotation {
                    cluster_id: peer.clone(),
                    current_fingerprint: registered.identity_fingerprint.clone(),
                    new_fingerprint: req.local_leaf_fingerprint.clone(),
                    at_ms: now,
                    grace_period_ms: req.overlap_ms,
                    request_id: format!(
                        "federation-trust:{}:stage-peer-leaf-local",
                        req.rotation_id
                    ),
                    actor: format!("federation-peer:{peer}"),
                })
                .await
                .map_err(raft_status)?;
            if !staged_identity.success {
                return Ok(Response::new(task::FederatedTrustRotationResponse {
                    accepted: false,
                    state_json: String::new(),
                    message: staged_identity.message,
                }));
            }
        }
        let mut rotation = store
            .get_federated_trust_rotation(&req.rotation_id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| {
                Status::internal(
                    if created.success {
                        "created trust rotation was not persisted"
                    } else {
                        created.message.as_str()
                    }
                    .to_string(),
                )
            })?;
        if !rotation.local_staged {
            let staged = raft
                .propose(crate::raft::Command::StageFederatedTrustBundle {
                    cluster_id: peer.clone(),
                    current_fingerprint: rotation
                        .current_peer_ca_fingerprint
                        .clone()
                        .unwrap_or_default(),
                    bundle_pem: rotation.peer_ca_bundle_pem.clone(),
                    bundle_fingerprint: rotation.peer_ca_bundle_fingerprint.clone(),
                    at_ms: now,
                    request_id: format!("federation-trust:{}:stage-local", req.rotation_id),
                    actor: format!("federation-peer:{peer}"),
                })
                .await
                .map_err(raft_status)?;
            if !staged.success {
                return Ok(Response::new(task::FederatedTrustRotationResponse {
                    accepted: false,
                    state_json: serde_json::to_string(&rotation).unwrap_or_default(),
                    message: staged.message,
                }));
            }
            let event = crate::federation::FederatedTrustRotationEvent::LocalStaged {
                fingerprint: rotation.peer_ca_bundle_fingerprint.clone(),
            };
            let applied = raft
                .propose(crate::raft::Command::AdvanceFederatedTrustRotation {
                    rotation_id: req.rotation_id.clone(),
                    expected_revision: rotation.revision,
                    event,
                    at_ms: now,
                    request_id: format!("federation-trust:{}:event-local-staged", req.rotation_id),
                    actor: format!("federation-peer:{peer}"),
                })
                .await
                .map_err(raft_status)?;
            if !applied.success {
                return Ok(Response::new(task::FederatedTrustRotationResponse {
                    accepted: false,
                    state_json: serde_json::to_string(&rotation).unwrap_or_default(),
                    message: applied.message,
                }));
            }
            rotation = store
                .get_federated_trust_rotation(&req.rotation_id)
                .await
                .map_err(|error| Status::internal(error.to_string()))?
                .ok_or_else(|| Status::internal("trust rotation disappeared after stage"))?;
        }
        Ok(Response::new(task::FederatedTrustRotationResponse {
            accepted: true,
            state_json: serde_json::to_string(&rotation)
                .map_err(|error| Status::internal(error.to_string()))?,
            message: "peer CA bundle staged; bilateral rotation record is durable".into(),
        }))
    }

    async fn advance_federated_trust_rotation(
        &self,
        request: Request<task::FederatedTrustRotationRequest>,
    ) -> std::result::Result<Response<task::FederatedTrustRotationResponse>, Status> {
        let incoming: crate::federation::FederatedTrustRotationEvent =
            serde_json::from_str(&request.get_ref().event_json).map_err(|error| {
                Status::invalid_argument(format!("invalid trust rotation event: {error}"))
            })?;
        let presented_fingerprint = request
            .peer_certs()
            .and_then(|certs| {
                certs
                    .first()
                    .map(|cert| format!("{:x}", Sha256::digest(cert.as_ref())))
            })
            .ok_or_else(|| {
                Status::unauthenticated("Trust rotation requires an mTLS client certificate")
            })?;
        let peer = match verify_peer_identity(self, &request).await {
            Ok(Some(peer)) => peer,
            Ok(None) => {
                return Err(Status::unauthenticated(
                    "Trust rotation requires an authenticated federation peer",
                ));
            }
            Err(status)
                if status.code() == tonic::Code::Unauthenticated
                    && matches!(
                        incoming,
                        crate::federation::FederatedTrustRotationEvent::LocalDeploymentReady { .. }
                    ) =>
            {
                let peer = request
                    .metadata()
                    .get("x-dataspool-peer-cluster")
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| !value.trim().is_empty() && value.len() <= 128)
                    .ok_or_else(|| Status::unauthenticated("Peer cluster identity is required"))?
                    .to_string();
                let (store, raft) = {
                    let service = self.read().await;
                    (service.task_store.clone(), service.raft_node.clone())
                };
                let raft =
                    raft.ok_or_else(|| Status::unavailable("Trust rotation requires Raft"))?;
                raft.ensure_linearizable().await.map_err(raft_status)?;
                let registered = store
                    .get_federated_cluster(&peer)
                    .await
                    .map_err(|error| Status::internal(error.to_string()))?
                    .ok_or_else(|| Status::unauthenticated("Peer cluster is not registered"))?;
                let rotation = store
                    .get_federated_trust_rotation(&request.get_ref().rotation_id)
                    .await
                    .map_err(|error| Status::internal(error.to_string()))?
                    .filter(|rotation| rotation.peer_cluster_id == peer)
                    .ok_or_else(|| Status::not_found("trust rotation not found for peer"))?;
                let requested_leaf = match &incoming {
                    crate::federation::FederatedTrustRotationEvent::LocalDeploymentReady {
                        leaf_fingerprint,
                    } => leaf_fingerprint,
                    _ => unreachable!(),
                };
                if !requested_leaf.eq_ignore_ascii_case(&presented_fingerprint)
                    || !rotation
                        .peer_leaf_fingerprint
                        .eq_ignore_ascii_case(&presented_fingerprint)
                    || !registered
                        .pending_identity_fingerprint
                        .as_deref()
                        .is_some_and(|pending| pending.eq_ignore_ascii_case(&presented_fingerprint))
                {
                    return Err(Status::unauthenticated(
                        "deployment readiness must prove possession of the staged peer leaf certificate",
                    ));
                }
                let completed = raft
                    .propose(crate::raft::Command::CompleteFederatedCertificateRotation {
                        cluster_id: peer.clone(),
                        new_fingerprint: presented_fingerprint.clone(),
                        at_ms: chrono::Utc::now().timestamp_millis(),
                        request_id: format!(
                            "federation-trust:{}:complete-peer-leaf",
                            request.get_ref().rotation_id
                        ),
                        actor: format!("federation-peer:{peer}:leaf-proof"),
                    })
                    .await
                    .map_err(raft_status)?;
                if !completed.success {
                    return Err(Status::unauthenticated(completed.message));
                }
                peer
            }
            Err(status) => return Err(status),
        };
        if let crate::federation::FederatedTrustRotationEvent::LocalDeploymentReady {
            leaf_fingerprint,
        } = &incoming
            && !leaf_fingerprint.eq_ignore_ascii_case(&presented_fingerprint)
        {
            return Err(Status::unauthenticated(
                "deployment readiness leaf fingerprint does not match the presented mTLS certificate",
            ));
        }
        let req = request.into_inner();
        if req.rotation_id.trim().is_empty()
            || req.request_id.trim().is_empty()
            || req.request_id.len() > 128
        {
            return Err(Status::invalid_argument(
                "rotation_id and request_id are required",
            ));
        }
        let incoming: crate::federation::FederatedTrustRotationEvent =
            serde_json::from_str(&req.event_json).map_err(|error| {
                Status::invalid_argument(format!("invalid trust rotation event: {error}"))
            })?;
        let (store, raft) = {
            let service = self.read().await;
            (service.task_store.clone(), service.raft_node.clone())
        };
        let raft = raft.ok_or_else(|| Status::unavailable("Trust rotation requires Raft"))?;
        raft.ensure_linearizable().await.map_err(raft_status)?;
        let rotation = store
            .get_federated_trust_rotation(&req.rotation_id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .filter(|rotation| rotation.peer_cluster_id == peer)
            .ok_or_else(|| {
                Status::not_found("federated trust rotation not found for authenticated peer")
            })?;
        let event = invert_trust_rotation_event(incoming);
        let payload = format!(
            "federated-trust-rotation-event:{}:{event:?}",
            req.rotation_id
        );
        if let Some(existing) = store
            .get_control_request(&req.request_id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
        {
            if existing == payload {
                let current = store
                    .get_federated_trust_rotation(&req.rotation_id)
                    .await
                    .map_err(|error| Status::internal(error.to_string()))?;
                return Ok(Response::new(task::FederatedTrustRotationResponse {
                    accepted: true,
                    state_json: serde_json::to_string(&current)
                        .map_err(|error| Status::internal(error.to_string()))?,
                    message: "federated trust rotation event already committed".into(),
                }));
            }
            return Err(Status::already_exists(
                "request_id was already used for a different trust rotation event",
            ));
        }
        let now = chrono::Utc::now().timestamp_millis();
        let event_at_ms = match &event {
            crate::federation::FederatedTrustRotationEvent::LocalActivated {
                previous_expires_at_ms,
            }
            | crate::federation::FederatedTrustRotationEvent::PeerActivated {
                previous_expires_at_ms,
            } => previous_expires_at_ms.saturating_sub(rotation.overlap_ms),
            _ => now,
        };
        match &event {
            crate::federation::FederatedTrustRotationEvent::LocalActivated { .. }
                if !rotation.activation_barrier_committed
                    || !matches!(
                        rotation.phase,
                        crate::federation::FederatedTrustRotationPhase::Activating
                            | crate::federation::FederatedTrustRotationPhase::RecoveryRequired
                    ) =>
            {
                return Err(Status::failed_precondition(
                    "remote activation barrier is not committed",
                ));
            }
            crate::federation::FederatedTrustRotationEvent::LocalRetired
                if rotation.local_activation_expires_at_ms.is_none()
                    || rotation.peer_activation_expires_at_ms.is_none()
                    || now
                        < rotation
                            .local_activation_expires_at_ms
                            .unwrap()
                            .max(rotation.peer_activation_expires_at_ms.unwrap()) =>
            {
                return Err(Status::failed_precondition(
                    "both activation deadlines must expire before remote retirement",
                ));
            }
            crate::federation::FederatedTrustRotationEvent::LocalRolledBack
                if rotation.local_activation_expires_at_ms.is_some()
                    || rotation.peer_activation_expires_at_ms.is_some()
                    || rotation.phase
                        != crate::federation::FederatedTrustRotationPhase::RollingBack =>
            {
                return Err(Status::failed_precondition(
                    "remote rollback is permitted only before activation",
                ));
            }
            _ => {}
        }
        match &event {
            crate::federation::FederatedTrustRotationEvent::LocalActivated {
                previous_expires_at_ms,
            } => {
                let activated = raft
                    .propose(crate::raft::Command::ActivateFederatedTrustBundle {
                        cluster_id: peer.clone(),
                        bundle_fingerprint: rotation.peer_ca_bundle_fingerprint.clone(),
                        at_ms: event_at_ms,
                        overlap_ms: rotation.overlap_ms,
                        request_id: format!("federation-trust:{}:activate-local", req.rotation_id),
                        actor: format!("federation-peer:{peer}"),
                    })
                    .await
                    .map_err(raft_status)?;
                if !activated.success {
                    return Ok(Response::new(task::FederatedTrustRotationResponse {
                        accepted: false,
                        state_json: serde_json::to_string(&rotation).unwrap_or_default(),
                        message: activated.message,
                    }));
                }
                if event_at_ms.saturating_add(rotation.overlap_ms) != *previous_expires_at_ms {
                    return Err(Status::failed_precondition(
                        "activation expiry does not match the agreed overlap",
                    ));
                }
            }
            crate::federation::FederatedTrustRotationEvent::LocalRetired => {
                let retired = raft
                    .propose(crate::raft::Command::RetireFederatedTrustBundle {
                        cluster_id: peer.clone(),
                        at_ms: now,
                        request_id: format!("federation-trust:{}:retire-local", req.rotation_id),
                        actor: format!("federation-peer:{peer}"),
                    })
                    .await
                    .map_err(raft_status)?;
                if !retired.success {
                    return Ok(Response::new(task::FederatedTrustRotationResponse {
                        accepted: false,
                        state_json: serde_json::to_string(&rotation).unwrap_or_default(),
                        message: retired.message,
                    }));
                }
            }
            crate::federation::FederatedTrustRotationEvent::LocalRolledBack => {
                let registered = store
                    .get_federated_cluster(&peer)
                    .await
                    .map_err(|error| Status::internal(error.to_string()))?
                    .ok_or_else(|| Status::not_found("peer cluster disappeared during rollback"))?;
                if registered
                    .pending_identity_fingerprint
                    .as_deref()
                    .is_some_and(|fingerprint| {
                        fingerprint.eq_ignore_ascii_case(&rotation.peer_leaf_fingerprint)
                    })
                {
                    let aborted_leaf = raft
                        .propose(crate::raft::Command::AbortFederatedCertificateRotation {
                            cluster_id: peer.clone(),
                            new_fingerprint: rotation.peer_leaf_fingerprint.clone(),
                            at_ms: now,
                            request_id: format!(
                                "federation-trust:{}:abort-local-leaf",
                                req.rotation_id
                            ),
                            actor: format!("federation-peer:{peer}"),
                        })
                        .await
                        .map_err(raft_status)?;
                    if !aborted_leaf.success {
                        return Ok(Response::new(task::FederatedTrustRotationResponse {
                            accepted: false,
                            state_json: serde_json::to_string(&rotation).unwrap_or_default(),
                            message: aborted_leaf.message,
                        }));
                    }
                } else if registered
                    .identity_fingerprint
                    .eq_ignore_ascii_case(&rotation.peer_leaf_fingerprint)
                {
                    let rolled_back_leaf = raft
                        .propose(crate::raft::Command::RollbackFederatedCertificateRotation {
                            cluster_id: peer.clone(),
                            new_fingerprint: rotation.peer_leaf_fingerprint.clone(),
                            at_ms: now,
                            request_id: format!(
                                "federation-trust:{}:rollback-local-leaf",
                                req.rotation_id
                            ),
                            actor: format!("federation-peer:{peer}"),
                        })
                        .await
                        .map_err(raft_status)?;
                    if !rolled_back_leaf.success {
                        return Ok(Response::new(task::FederatedTrustRotationResponse {
                            accepted: false,
                            state_json: serde_json::to_string(&rotation).unwrap_or_default(),
                            message: rolled_back_leaf.message,
                        }));
                    }
                }
                let aborted = raft
                    .propose(crate::raft::Command::AbortFederatedTrustBundle {
                        cluster_id: peer.clone(),
                        bundle_fingerprint: rotation.peer_ca_bundle_fingerprint.clone(),
                        at_ms: now,
                        request_id: format!("federation-trust:{}:abort-local", req.rotation_id),
                        actor: format!("federation-peer:{peer}"),
                    })
                    .await
                    .map_err(raft_status)?;
                if !aborted.success {
                    return Ok(Response::new(task::FederatedTrustRotationResponse {
                        accepted: false,
                        state_json: serde_json::to_string(&rotation).unwrap_or_default(),
                        message: aborted.message,
                    }));
                }
            }
            _ => {}
        }
        let advanced = raft
            .propose(crate::raft::Command::AdvanceFederatedTrustRotation {
                rotation_id: req.rotation_id.clone(),
                expected_revision: rotation.revision,
                event,
                at_ms: event_at_ms,
                request_id: req.request_id,
                actor: format!("federation-peer:{peer}"),
            })
            .await
            .map_err(raft_status)?;
        let state = store
            .get_federated_trust_rotation(&req.rotation_id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(task::FederatedTrustRotationResponse {
            accepted: advanced.success,
            state_json: serde_json::to_string(&state)
                .map_err(|error| Status::internal(error.to_string()))?,
            message: advanced.message,
        }))
    }

    async fn get_federated_trust_rotation(
        &self,
        request: Request<task::FederatedTrustRotationRequest>,
    ) -> std::result::Result<Response<task::FederatedTrustRotationResponse>, Status> {
        let peer = verify_peer_identity(self, &request).await?.ok_or_else(|| {
            Status::unauthenticated("Trust rotation requires an authenticated federation peer")
        })?;
        let req = request.into_inner();
        let store = self.read().await.task_store.clone();
        let rotation = store
            .get_federated_trust_rotation(&req.rotation_id)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .filter(|rotation| rotation.peer_cluster_id == peer)
            .ok_or_else(|| {
                Status::not_found("federated trust rotation not found for authenticated peer")
            })?;
        let peer_record = store
            .get_federated_cluster(&peer)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found("registered peer cluster not found"))?;
        let snapshot = crate::federation::FederatedTrustRotationSnapshot {
            rotation,
            peer_active_ca_fingerprint: peer_record.trust_bundle_fingerprint,
            peer_pending_ca_fingerprint: peer_record.pending_trust_bundle_fingerprint,
            peer_previous_ca_fingerprint: peer_record.previous_trust_bundle_fingerprint,
            peer_previous_ca_expires_at_ms: peer_record.previous_trust_bundle_expires_at_ms,
            peer_active_identity_fingerprint: peer_record.identity_fingerprint,
            peer_pending_identity_fingerprint: peer_record.pending_identity_fingerprint,
        };
        Ok(Response::new(task::FederatedTrustRotationResponse {
            accepted: true,
            state_json: serde_json::to_string(&snapshot)
                .map_err(|error| Status::internal(error.to_string()))?,
            message: "federated trust rotation and peer registry snapshot".into(),
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
        if operation.state == crate::federation::FederatedRemoteOperationState::Compensating
            || operation.state == crate::federation::FederatedRemoteOperationState::Compensated
        {
            return Err(Status::failed_precondition(
                "Remote migration compensation has started; commit is fenced",
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
        let mut existing = store
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
                crate::federation::FederatedRemoteOperationState::Prepared
                | crate::federation::FederatedRemoteOperationState::Compensating => {}
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

        // Commit and compensation are mutually exclusive Raft state transitions.
        // Persist the compensation fence before cancelling workers or deleting sink
        // documents, so a concurrent/replayed Commit cannot win after deletion starts.
        let intent = crate::federation::FederatedRemoteOperation {
            migration_id: req.migration_id.clone(),
            source_cluster: peer.clone(),
            generation: req.generation,
            task_id: task_id.to_string(),
            state: crate::federation::FederatedRemoteOperationState::Compensating,
            checkpoint_digest: existing
                .as_ref()
                .and_then(|value| value.checkpoint_digest.clone()),
            updated_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        let intent_result = raft
            .propose(crate::raft::Command::RecordFederatedRemoteOperation {
                operation: Box::new(intent.clone()),
                request_id: format!(
                    "federation-compensation-begin:{:x}",
                    Sha256::digest(req.request_id.as_bytes())
                ),
                actor: "federation-compensation".into(),
            })
            .await
            .map_err(raft_status)?;
        if !intent_result.success {
            return Err(Status::failed_precondition(intent_result.message));
        }
        existing = Some(intent);

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

#[cfg(test)]
mod federation_e2e_tests;
