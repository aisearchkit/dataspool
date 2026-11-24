// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 Dataspool Contributors

//! Cross-cluster federation tests using independent Raft groups and real mTLS gRPC.

use super::*;
use crate::{
    federation::{
        FederatedCluster, FederatedClusterStatus, FederatedMigration,
        FederatedRemoteOperationState, MigrationState, MigrationTimeoutPolicy,
    },
    raft::{Command, RaftNode, raft_config, raft_router},
    storage::RocksDbStore,
    task::TaskStore,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use openraft::BasicNode;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken as ShutdownToken;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tower::{Layer, Service};

const CLIENT_CERT: &[u8] = include_bytes!("../../../tests/fixtures/federation-test-client.pem");
const SERVER_CERT: &[u8] = include_bytes!("../../../tests/fixtures/federation-test-server.pem");
const SERVER_KEY: &[u8] = include_bytes!("../../../tests/fixtures/federation-test-server.key.pem");
const CA_CERT: &[u8] = include_bytes!("../../../tests/fixtures/federation-test-ca.pem");

fn cert_der(pem: &[u8]) -> Vec<u8> {
    let encoded = std::str::from_utf8(pem)
        .unwrap()
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect::<String>();
    STANDARD.decode(encoded).unwrap()
}

fn client_fingerprint() -> String {
    format!("{:x}", Sha256::digest(cert_der(CLIENT_CERT)))
}

struct TestRaftCluster {
    _directory: TempDir,
    store: RocksDbStore,
    task_store: Arc<TaskStore<RocksDbStore>>,
    node: RaftNode,
    node_id: u64,
    raft_addr: String,
    raft_token: String,
    raft_server: Option<JoinHandle<()>>,
}

impl TestRaftCluster {
    async fn start(cluster_id: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = RocksDbStore::open(directory.path().join("app")).unwrap();
        let task_store = Arc::new(TaskStore::new(store.clone()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raft_addr = listener.local_addr().unwrap().to_string();
        let node_id = crate::raft::stable_node_id(cluster_id);
        let raft_token = format!("{cluster_id}-raft-test-token-0123456789");
        let node = RaftNode::new(
            node_id,
            directory.path().join("raft"),
            TaskStore::new(store.clone()),
            raft_config(Duration::from_millis(120), Duration::from_millis(500)),
            raft_token.clone(),
        )
        .await
        .unwrap();
        let router = raft_router(node.handle(), raft_token.clone());
        let raft_server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        node.initialize(BTreeMap::from([(
            node_id,
            BasicNode::new(raft_addr.clone()),
        )]))
        .await
        .unwrap();
        node.wait_for_leader(Duration::from_secs(10)).await.unwrap();
        node.ensure_linearizable().await.unwrap();
        Self {
            _directory: directory,
            store,
            task_store,
            node,
            node_id,
            raft_addr,
            raft_token,
            raft_server: Some(raft_server),
        }
    }

    async fn restart(&mut self) {
        if let Some(server) = self.raft_server.take() {
            server.abort();
            let _ = server.await;
        }
        self.node.shutdown().await.unwrap();
        let listener = TcpListener::bind(&self.raft_addr).await.unwrap();
        let node = RaftNode::new(
            self.node_id,
            self._directory.path().join("raft"),
            TaskStore::new(self.store.clone()),
            raft_config(Duration::from_millis(120), Duration::from_millis(500)),
            self.raft_token.clone(),
        )
        .await
        .unwrap();
        let router = raft_router(node.handle(), self.raft_token.clone());
        self.raft_server = Some(tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        }));
        node.wait_for_leader(Duration::from_secs(15)).await.unwrap();
        node.ensure_linearizable().await.unwrap();
        self.node = node;
    }

    async fn register_peer(&self, peer_id: &str, endpoint: String) {
        let now = chrono::Utc::now().timestamp_millis();
        let response = self
            .node
            .propose(Command::UpsertFederatedCluster {
                cluster: FederatedCluster {
                    cluster_id: peer_id.into(),
                    endpoint,
                    identity_fingerprint: client_fingerprint(),
                    version: env!("CARGO_PKG_VERSION").into(),
                    capabilities: BTreeSet::from([
                        "task-provisioning".into(),
                        "checkpoint-exchange".into(),
                        "distributed-commit".into(),
                    ]),
                    status: FederatedClusterStatus::Healthy,
                    last_seen_at_ms: now,
                    updated_at_ms: now,
                },
                request_id: format!("register-peer:{peer_id}"),
                actor: "e2e-test".into(),
            })
            .await
            .unwrap();
        assert!(response.success, "{}", response.message);
    }
}

#[derive(Clone, Default)]
struct AckFaults {
    prepare: Arc<AtomicBool>,
    commit: Arc<AtomicBool>,
}

#[derive(Clone)]
struct DropFederationAckLayer(AckFaults);

impl<S> Layer<S> for DropFederationAckLayer {
    type Service = DropFederationAck<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DropFederationAck {
            inner,
            faults: self.0.clone(),
        }
    }
}

#[derive(Clone)]
struct DropFederationAck<S> {
    inner: S,
    faults: AckFaults,
}

impl<S, B, Body> Service<tonic::codegen::http::Request<B>> for DropFederationAck<S>
where
    S: Service<tonic::codegen::http::Request<B>, Response = tonic::codegen::http::Response<Body>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    Body: Send + 'static,
    B: Send + 'static,
{
    type Response = tonic::codegen::http::Response<Body>;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        let path = request.uri().path().to_string();
        let mut inner = self.inner.clone();
        let faults = self.faults.clone();
        Box::pin(async move {
            let mut response = inner.call(request).await?;
            let drop_ack = if path.ends_with("/PrepareFederatedCommit") {
                faults.prepare.swap(false, Ordering::SeqCst)
            } else if path.ends_with("/CommitFederatedMigration") {
                faults.commit.swap(false, Ordering::SeqCst)
            } else {
                false
            };
            if drop_ack {
                *response.status_mut() = tonic::codegen::http::StatusCode::SERVICE_UNAVAILABLE;
            }
            Ok(response)
        })
    }
}

struct TcpFaultProxy {
    endpoint: String,
    blocked: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl TcpFaultProxy {
    async fn start(backend: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let blocked = Arc::new(AtomicBool::new(false));
        let gate = blocked.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    break;
                };
                if gate.load(Ordering::SeqCst) {
                    // A partition drops the full TCP stream in both directions.
                    drop(inbound);
                    continue;
                }
                let backend = backend.clone();
                tokio::spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(&backend).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        });
        Self {
            endpoint,
            blocked,
            task,
        }
    }

    fn set_partitioned(&self, partitioned: bool) {
        self.blocked.store(partitioned, Ordering::SeqCst);
    }
}

async fn start_target_grpc(
    target: &TestRaftCluster,
    faults: AckFaults,
) -> (String, ShutdownToken, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task_service = Arc::new(RwLock::new(TaskServiceImpl {
        task_store: target.task_store.clone(),
        raft_node: Some(Arc::new(target.node.clone())),
    }));
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(SERVER_CERT, SERVER_KEY))
        .client_ca_root(Certificate::from_pem(CA_CERT));
    let shutdown = ShutdownToken::new();
    let server_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        Server::builder()
            .layer(DropFederationAckLayer(faults))
            .tls_config(tls)
            .unwrap()
            .add_service(task::task_service_server::TaskServiceServer::new(
                task_service,
            ))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                server_shutdown.cancelled_owned(),
            )
            .await
            .unwrap();
    });
    (address.to_string(), shutdown, task)
}

async fn complete_remote_task(target: &TestRaftCluster, task_id: &str) {
    let parsed: crate::types::TaskId = task_id.parse().unwrap();
    let (_, shards) = target
        .task_store
        .get_task_with_aggregate_and_shards(&parsed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(shards.len(), 1, "test migration must create one shard");
    let shard = &shards[0];
    let now = chrono::Utc::now().timestamp_millis();
    let claim = target
        .node
        .propose(Command::ClaimShard {
            shard_id: shard.shard_id.to_string(),
            node_id: "e2e-worker".into(),
            claim_id: "e2e-claim-1".into(),
            issued_at_ms: now,
            lease_expires_at_ms: now + 60_000,
        })
        .await
        .unwrap();
    assert!(claim.success, "{}", claim.message);
    let claimed = target
        .task_store
        .get_shard(&shard.shard_id)
        .await
        .unwrap()
        .unwrap();
    let complete = target
        .node
        .propose(Command::CompleteShard {
            shard_id: shard.shard_id.to_string(),
            node_id: "e2e-worker".into(),
            fencing_token: claimed.fencing_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            total_records: 3,
        })
        .await
        .unwrap();
    assert!(complete.success, "{}", complete.message);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_raft_clusters_recover_from_partition_lost_acks_and_source_crash() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut source = TestRaftCluster::start("federation-source-e2e").await;
    let mut target = TestRaftCluster::start("federation-target-e2e").await;
    let faults = AckFaults::default();
    let (target_addr, target_shutdown, target_server) =
        start_target_grpc(&target, faults.clone()).await;
    let proxy = TcpFaultProxy::start(target_addr).await;

    source
        .register_peer("federation-target-e2e", proxy.endpoint.clone())
        .await;
    target
        .register_peer("federation-source-e2e", "https://localhost:1".into())
        .await;

    let now = chrono::Utc::now().timestamp_millis();
    let migration = FederatedMigration {
        migration_id: "e2e-migration-commit-loss".into(),
        source_cluster: "federation-source-e2e".into(),
        target_cluster: "federation-target-e2e".into(),
        task_id: "e2e-logical-task".into(),
        required_capabilities: BTreeSet::from(["task-provisioning".into()]),
        state: MigrationState::Planned,
        generation: 0,
        created_at_ms: now,
        updated_at_ms: now,
        last_error: None,
        task_name: "federated-e2e-task".into(),
        source_config: "{}".into(),
        sink_config: "{}".into(),
        transform_config: None,
        sharding_strategy: None,
        priority: 1,
        tags: HashMap::new(),
        remote_task_id: None,
        remote_task_state: None,
        remote_checkpoint: None,
        last_reconciled_at_ms: None,
        deadline_at_ms: None,
        execution_budget_ms: None,
        retry_budget: Some(3),
        timeout_policy: MigrationTimeoutPolicy::Fail,
    };
    let created = source
        .node
        .propose(Command::CreateFederatedMigration {
            migration: Box::new(migration),
            request_id: "e2e-create-migration".into(),
            actor: "e2e-test".into(),
        })
        .await
        .unwrap();
    assert!(created.success, "{}", created.message);

    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let ca = cert_dir.join("federation-test-ca.pem");
    let client_cert = cert_dir.join("federation-test-client.pem");
    let client_key = cert_dir.join("federation-test-client.key.pem");
    let orchestrator = crate::federation::orchestrator::FederatedOrchestrator::new(
        source.task_store.clone(),
        Arc::new(source.node.clone()),
        "federation-source-e2e",
        ca.clone(),
        client_cert.clone(),
        client_key.clone(),
    );

    // First prove a symmetric TCP partition: the source cannot establish the
    // mTLS gRPC channel, and no target task is provisioned while isolated.
    orchestrator.reconcile_once().await.unwrap(); // Planned -> Preparing
    proxy.set_partitioned(true);
    orchestrator.reconcile_once().await.unwrap();
    assert!(target.task_store.list_tasks().await.unwrap().is_empty());
    proxy.set_partitioned(false);
    orchestrator.reconcile_once().await.unwrap(); // provision exactly one task

    let mut persisted = source
        .task_store
        .get_federated_migration("e2e-migration-commit-loss")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, MigrationState::Running);
    let remote_task_id = persisted.remote_task_id.clone().unwrap();
    assert_eq!(target.task_store.list_tasks().await.unwrap().len(), 1);

    complete_remote_task(&target, &remote_task_id).await;

    // Drop the real gRPC Prepare response only after the target handler has
    // committed Prepared to its independent Raft group.
    faults.prepare.store(true, Ordering::SeqCst);
    orchestrator.reconcile_once().await.unwrap();
    let prepared = target
        .task_store
        .get_federated_remote_operation("e2e-migration-commit-loss", 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.state, FederatedRemoteOperationState::Prepared);
    persisted = source
        .task_store
        .get_federated_migration("e2e-migration-commit-loss")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, MigrationState::Running);

    // Retry Prepare with the same idempotency key and advance to Committing.
    orchestrator.reconcile_once().await.unwrap();
    persisted = source
        .task_store
        .get_federated_migration("e2e-migration-commit-loss")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, MigrationState::Committing);

    // Commit is durably applied on target, but the response is dropped by the
    // gRPC fault layer. The source must remain Committing until reconciliation.
    faults.commit.store(true, Ordering::SeqCst);
    orchestrator.reconcile_once().await.unwrap();
    let committed = target
        .task_store
        .get_federated_remote_operation("e2e-migration-commit-loss", 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.state, FederatedRemoteOperationState::Committed);
    persisted = source
        .task_store
        .get_federated_migration("e2e-migration-commit-loss")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, MigrationState::Committing);

    // Simulate a source process crash after target commit and before source
    // acknowledgement persistence. Restart from the same durable RocksDB/Raft log.
    drop(orchestrator);
    source.restart().await;
    let recovered = crate::federation::orchestrator::FederatedOrchestrator::new(
        source.task_store.clone(),
        Arc::new(source.node.clone()),
        "federation-source-e2e",
        ca,
        client_cert,
        client_key,
    );
    recovered.reconcile_once().await.unwrap();
    let completed = source
        .task_store
        .get_federated_migration("e2e-migration-commit-loss")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.state, MigrationState::Completed);
    assert_eq!(
        completed.remote_task_id.as_deref(),
        Some(remote_task_id.as_str())
    );
    assert_eq!(target.task_store.list_tasks().await.unwrap().len(), 1);
    assert_eq!(
        target
            .task_store
            .get_federated_remote_operation("e2e-migration-commit-loss", 0)
            .await
            .unwrap()
            .unwrap()
            .state,
        FederatedRemoteOperationState::Committed
    );

    // A further leader reconciliation is a no-op: no duplicate remote task and
    // no second migration execution can be scheduled.
    recovered.reconcile_once().await.unwrap();
    assert_eq!(target.task_store.list_tasks().await.unwrap().len(), 1);

    proxy.task.abort();
    target_shutdown.cancel();
    let _ = target_server.await;
    if let Some(server) = source.raft_server.take() {
        server.abort();
    }
    if let Some(server) = target.raft_server.take() {
        server.abort();
    }
    source.node.shutdown().await.unwrap();
    target.node.shutdown().await.unwrap();
}
