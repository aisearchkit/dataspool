// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 Dataspool Contributors

//! Cross-cluster federation tests using independent Raft groups and real mTLS gRPC.

use super::*;
use crate::{
    federation::{
        FederatedCluster, FederatedClusterStatus, FederatedMigration,
        FederatedRemoteOperationState, MigrationState, MigrationTimeoutPolicy,
    },
    raft::{Command, RaftNode, raft_config, raft_router, test_router_with_shared_raft},
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
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    net::{TcpListener, TcpSocket},
    task::JoinHandle,
};
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
    node: Option<RaftNode>,
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
            node: Some(node),
            node_id,
            raft_addr,
            raft_token,
            raft_server: Some(raft_server),
        }
    }

    fn node(&self) -> &RaftNode {
        self.node.as_ref().expect("test Raft node is running")
    }

    async fn restart(&mut self) {
        if let Some(server) = self.raft_server.take() {
            server.abort();
            let _ = server.await;
        }
        let old_node = self.node.take().expect("test Raft node is running");
        old_node.shutdown().await.unwrap();
        drop(old_node);
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
        self.node = Some(node);
    }

    async fn register_peer(&self, peer_id: &str, endpoint: String) {
        let now = chrono::Utc::now().timestamp_millis();
        let response = self
            .node()
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

#[derive(Clone)]
struct AckFaults {
    prepare: Arc<AtomicBool>,
    commit: Arc<AtomicBool>,
    prepare_dropped: Arc<AtomicBool>,
    commit_dropped: Arc<AtomicBool>,
    observed_paths: Arc<Mutex<Vec<String>>>,
}

impl Default for AckFaults {
    fn default() -> Self {
        Self {
            prepare: Arc::new(AtomicBool::new(false)),
            commit: Arc::new(AtomicBool::new(false)),
            prepare_dropped: Arc::new(AtomicBool::new(false)),
            commit_dropped: Arc::new(AtomicBool::new(false)),
            observed_paths: Arc::new(Mutex::new(Vec::new())),
        }
    }
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

impl<S, B> Service<tonic::codegen::http::Request<B>> for DropFederationAck<S>
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
        let path = request.uri().path().to_string();
        let mut inner = self.inner.clone();
        let faults = self.faults.clone();
        Box::pin(async move {
            let mut response = inner.call(request).await?;
            faults.observed_paths.lock().unwrap().push(path.clone());
            let lowered = path.to_ascii_lowercase();
            let drop_ack = if lowered.contains("preparefederatedcommit") {
                let drop = faults.prepare.swap(false, Ordering::SeqCst);
                if drop {
                    faults.prepare_dropped.store(true, Ordering::SeqCst);
                }
                drop
            } else if lowered.contains("commitfederatedmigration") {
                let drop = faults.commit.swap(false, Ordering::SeqCst);
                if drop {
                    faults.commit_dropped.store(true, Ordering::SeqCst);
                }
                drop
            } else {
                false
            };
            if drop_ack {
                // Return a transport failure without the original gRPC trailers;
                // changing only HTTP status would leave grpc-status=0 in the
                // response and Tonic could correctly treat that as success.
                response = tonic::codegen::http::Response::builder()
                    .status(tonic::codegen::http::StatusCode::SERVICE_UNAVAILABLE)
                    .body(tonic::body::empty_body())
                    .expect("fault response is valid");
            }
            Ok(response)
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChaosEvent {
    Delay(u64),
    Partition,
    Heal,
    DropCommitAck,
    CrashSourceFollower,
    RestartSourceFollower,
    CrashTargetFollower,
    RestartTargetFollower,
    DuplicateReconcile,
}

#[derive(Clone, Copy, Debug)]
struct ChaosConfig {
    rounds: usize,
    max_delay_ms: u64,
    partition_weight: u32,
    ack_loss_weight: u32,
    crash_restart_weight: u32,
    duplicate_reconcile_weight: u32,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            rounds: 32,
            max_delay_ms: 20,
            partition_weight: 2,
            ack_loss_weight: 2,
            crash_restart_weight: 2,
            duplicate_reconcile_weight: 2,
        }
    }
}

/// Small deterministic PRNG so a failing chaos sequence can be replayed by seed.
struct ChaosScheduler {
    seed: u64,
    state: u64,
    config: ChaosConfig,
    emitted: usize,
}

impl ChaosScheduler {
    fn new(seed: u64, config: ChaosConfig) -> Self {
        assert!(
            config.partition_weight
                + config.ack_loss_weight
                + config.crash_restart_weight
                + config.duplicate_reconcile_weight
                <= 10,
            "chaos event weights must sum to at most 10"
        );
        assert!(
            config.rounds > 0 && config.rounds <= 512,
            "chaos rounds must be between 1 and 512"
        );
        assert!(
            config.max_delay_ms <= 5_000,
            "chaos delay must not exceed 5000ms"
        );
        Self {
            seed,
            state: if seed == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                seed
            },
            config,
            emitted: 0,
        }
    }

    fn next_u64(&mut self) -> u64 {
        // xorshift64*: deterministic across platforms and Rust versions.
        let mut value = self.state;
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
        self.state = value;
        value.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn next_event(&mut self) -> Option<ChaosEvent> {
        if self.emitted >= self.config.rounds {
            return None;
        }
        self.emitted += 1;
        let bucket = self.next_u64() % 100;
        let partition_end = self.config.partition_weight as u64 * 10;
        let ack_end = partition_end + self.config.ack_loss_weight as u64 * 10;
        let restart_end = ack_end + self.config.crash_restart_weight as u64 * 10;
        let duplicate_end = restart_end + self.config.duplicate_reconcile_weight as u64 * 10;
        let event = if bucket < partition_end {
            if self.next_u64() & 1 == 0 {
                ChaosEvent::Partition
            } else {
                ChaosEvent::Heal
            }
        } else if bucket < ack_end {
            ChaosEvent::DropCommitAck
        } else if bucket < restart_end {
            match self.next_u64() % 4 {
                0 => ChaosEvent::CrashSourceFollower,
                1 => ChaosEvent::RestartSourceFollower,
                2 => ChaosEvent::CrashTargetFollower,
                _ => ChaosEvent::RestartTargetFollower,
            }
        } else if bucket < duplicate_end {
            ChaosEvent::DuplicateReconcile
        } else {
            ChaosEvent::Delay(self.next_u64() % (self.config.max_delay_ms + 1))
        };
        Some(event)
    }

    fn seed(&self) -> u64 {
        self.seed
    }
}

fn chaos_env_u64(name: &str, default: u64) -> u64 {
    let Ok(value) = std::env::var(name) else {
        return default;
    };
    let parsed = value
        .strip_prefix("0x")
        .map(|hex| u64::from_str_radix(hex, 16))
        .unwrap_or_else(|| value.parse::<u64>());
    parsed.unwrap_or_else(|error| panic!("invalid {name}={value:?}: {error}"))
}

struct ChaosArtifactRecorder {
    seed: u64,
    config: ChaosConfig,
    events: Vec<String>,
    source_snapshot: Vec<String>,
    target_snapshot: Vec<String>,
    path: Option<PathBuf>,
}

impl ChaosArtifactRecorder {
    fn new(seed: u64, config: ChaosConfig) -> Self {
        let path = std::env::var_os("FEDERATION_CHAOS_ARTIFACT_DIR").map(|directory| {
            PathBuf::from(directory).join(format!("federation-chaos-seed-{seed:016x}.json"))
        });
        Self {
            seed,
            config,
            events: Vec::new(),
            source_snapshot: Vec::new(),
            target_snapshot: Vec::new(),
            path,
        }
    }

    fn record_event(&mut self, event: ChaosEvent) {
        self.record_note(format!("{event:?}"));
    }

    fn record_note(&mut self, note: String) {
        self.events
            .push(format!("{:04}: {note}", self.events.len() + 1));
        self.persist();
    }

    async fn capture(&mut self, source: &TestRaftGroup, target: &TestRaftGroup) {
        self.source_snapshot = source
            .diagnostic_snapshot("ha-leader-change-migration")
            .await;
        self.target_snapshot = target
            .diagnostic_snapshot("ha-leader-change-migration")
            .await;
        self.persist();
    }

    fn persist(&self) {
        let Some(path) = &self.path else {
            return;
        };
        if let Some(parent) = path.parent()
            && let Err(error) = std::fs::create_dir_all(parent)
        {
            eprintln!("failed to create chaos artifact directory: {error}");
            return;
        }
        let payload = serde_json::json!({
            "schema_version": 1,
            "seed": format!("0x{:016x}", self.seed),
            "seed_decimal": self.seed,
            "config": {
                "rounds": self.config.rounds,
                "max_delay_ms": self.config.max_delay_ms,
                "partition_weight": self.config.partition_weight,
                "ack_loss_weight": self.config.ack_loss_weight,
                "crash_restart_weight": self.config.crash_restart_weight,
                "duplicate_reconcile_weight": self.config.duplicate_reconcile_weight,
            },
            "events": self.events,
            "source_raft_members": self.source_snapshot,
            "target_raft_members": self.target_snapshot,
            "replay": format!(
                "FEDERATION_CHAOS_SEED=0x{:016x} FEDERATION_CHAOS_ROUNDS={} FEDERATION_CHAOS_ARTIFACT_DIR=artifacts/federation-chaos cargo test --lib --all-features comms::server::federation_e2e_tests::federation_mtls_recovery_survives_two_source_and_target_leader_changes -- --exact --nocapture",
                self.seed,
                self.config.rounds,
            ),
        });
        let temporary = path.with_extension("json.tmp");
        match serde_json::to_vec_pretty(&payload)
            .map_err(std::io::Error::other)
            .and_then(|bytes| std::fs::write(&temporary, bytes))
            .and_then(|()| std::fs::rename(&temporary, path))
        {
            Ok(()) => {}
            Err(error) => eprintln!(
                "failed to persist chaos artifact {}: {error}",
                path.display()
            ),
        }
    }
}

struct TcpFaultProxy {
    endpoint: String,
    blocked: Arc<AtomicBool>,
    delay_ms: Arc<AtomicU64>,
    connections: Arc<Mutex<Vec<JoinHandle<()>>>>,
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
        let delay_ms = Arc::new(AtomicU64::new(0));
        let connections = Arc::new(Mutex::new(Vec::<JoinHandle<()>>::new()));
        let gate = blocked.clone();
        let delay_gate = delay_ms.clone();
        let active_connections = connections.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    break;
                };
                if gate.load(Ordering::SeqCst) {
                    drop(inbound);
                    continue;
                }
                let backend = backend.clone();
                let delay_gate = delay_gate.clone();
                let connection = tokio::spawn(async move {
                    let delay = delay_gate.load(Ordering::SeqCst);
                    if delay > 0 {
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(&backend).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
                active_connections.lock().unwrap().push(connection);
            }
        });
        Self {
            endpoint,
            blocked,
            delay_ms,
            connections,
            task,
        }
    }

    fn set_partitioned(&self, partitioned: bool) {
        self.blocked.store(partitioned, Ordering::SeqCst);
        if partitioned {
            // Abort already-established bidirectional streams as well as
            // rejecting new accepts: this models a real link break, not only
            // a connection-establishment failure.
            for connection in self.connections.lock().unwrap().drain(..) {
                connection.abort();
            }
        }
    }

    fn set_delay(&self, delay: Duration) {
        self.delay_ms
            .store(delay.as_millis() as u64, Ordering::SeqCst);
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
        raft_node: Some(Arc::new(target.node().clone())),
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
        .node()
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
        .node()
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

#[test]
fn federation_chaos_schedule_is_reproducible_and_bounded() {
    let config = ChaosConfig {
        rounds: 256,
        max_delay_ms: 17,
        ..ChaosConfig::default()
    };
    let mut first = ChaosScheduler::new(0xCAFE_BABE_2026, config);
    let mut replay = ChaosScheduler::new(0xCAFE_BABE_2026, config);
    let mut alternate_seed = ChaosScheduler::new(0xCAFE_BABE_2027, config);
    let first_events = std::iter::from_fn(|| first.next_event()).collect::<Vec<_>>();
    let replay_events = std::iter::from_fn(|| replay.next_event()).collect::<Vec<_>>();
    let alternate_events = std::iter::from_fn(|| alternate_seed.next_event()).collect::<Vec<_>>();
    assert_eq!(
        first_events, replay_events,
        "same seed must replay exact event order"
    );
    assert_eq!(first_events.len(), config.rounds);
    assert_ne!(
        first_events, alternate_events,
        "different seeds should vary the schedule"
    );
    assert!(
        first_events
            .iter()
            .any(|event| matches!(event, ChaosEvent::Delay(ms) if *ms <= config.max_delay_ms))
    );
    assert!(first_events.contains(&ChaosEvent::Partition));
    assert!(first_events.contains(&ChaosEvent::Heal));
    assert!(first_events.contains(&ChaosEvent::DropCommitAck));
    assert!(first_events.contains(&ChaosEvent::CrashSourceFollower));
    assert!(first_events.contains(&ChaosEvent::RestartSourceFollower));
    assert!(first_events.contains(&ChaosEvent::CrashTargetFollower));
    assert!(first_events.contains(&ChaosEvent::RestartTargetFollower));
    assert!(first_events.contains(&ChaosEvent::DuplicateReconcile));
    assert!(
        first.next_event().is_none(),
        "scheduler must stop at configured round count"
    );
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

    // The registry stores both the local cluster identity and its federation
    // peers; migration creation validates both endpoints are registered/healthy.
    source
        .register_peer("federation-source-e2e", "https://127.0.0.1:1".into())
        .await;
    source
        .register_peer("federation-target-e2e", proxy.endpoint.clone())
        .await;
    target
        .register_peer("federation-target-e2e", "https://127.0.0.1:1".into())
        .await;
    target
        .register_peer("federation-source-e2e", "https://127.0.0.1:1".into())
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
        .node()
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
        Arc::new(source.node().clone()),
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
    assert!(
        faults.prepare_dropped.load(Ordering::SeqCst),
        "Prepare response fault was not triggered; observed gRPC paths: {:?}",
        faults.observed_paths.lock().unwrap()
    );
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
    assert!(faults.commit_dropped.load(Ordering::SeqCst));
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
        Arc::new(source.node().clone()),
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
    source.node().shutdown().await.unwrap();
    target.node().shutdown().await.unwrap();
}

/// Three-member Raft group used to exercise federation recovery across actual
/// leader replacement (rather than restarting the same single-member group).
struct TestRaftGroup {
    directories: Vec<TempDir>,
    stores: Vec<RocksDbStore>,
    nodes: Vec<Option<RaftNode>>,
    tokens: String,
    servers: Vec<Option<JoinHandle<()>>>,
    server_shutdowns: Vec<Option<ShutdownToken>>,
    raft_handles: Vec<Arc<RwLock<Option<Arc<crate::raft::Raft>>>>>,
    ids: [u64; 3],
}

impl TestRaftGroup {
    async fn start(token: &str) -> Self {
        let mut directories = Vec::new();
        let mut stores = Vec::new();
        let mut listeners = Vec::new();
        let mut addresses = Vec::new();
        for _ in 0..3 {
            let directory = tempfile::tempdir().unwrap();
            stores.push(RocksDbStore::open(directory.path().join("app")).unwrap());
            let socket = TcpSocket::new_v4().unwrap();
            socket.set_reuseaddr(true).unwrap();
            socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let listener = socket.listen(1024).unwrap();
            addresses.push(listener.local_addr().unwrap().to_string());
            directories.push(directory);
            listeners.push(listener);
        }
        let ids = [11, 22, 33];
        let members = ids
            .iter()
            .zip(&addresses)
            .map(|(id, address)| (*id, BasicNode::new(address.clone())))
            .collect::<BTreeMap<_, _>>();
        let config = raft_config(Duration::from_millis(150), Duration::from_millis(600));
        let mut nodes = Vec::new();
        let mut servers = Vec::new();
        let mut server_shutdowns = Vec::new();
        let mut raft_handles = Vec::new();
        for index in 0..3 {
            let node = RaftNode::new(
                ids[index],
                directories[index].path().join("raft"),
                TaskStore::new(stores[index].clone()),
                config.clone(),
                token.to_string(),
            )
            .await
            .unwrap();
            let raft_handle = Arc::new(RwLock::new(Some(node.handle())));
            let router = test_router_with_shared_raft(raft_handle.clone(), token.to_string());
            let listener = listeners.remove(0);
            let shutdown = ShutdownToken::new();
            let server_shutdown = shutdown.clone();
            servers.push(Some(tokio::spawn(async move {
                let _ = axum::serve(listener, router)
                    .with_graceful_shutdown(server_shutdown.cancelled_owned())
                    .await;
            })));
            server_shutdowns.push(Some(shutdown));
            raft_handles.push(raft_handle);
            nodes.push(Some(node));
        }
        nodes[0]
            .as_ref()
            .unwrap()
            .initialize(members)
            .await
            .unwrap();
        let group = Self {
            directories,
            stores,
            nodes,
            tokens: token.to_string(),
            servers,
            server_shutdowns,
            raft_handles,
            ids,
        };
        group.wait_for_leader(None).await;
        group
    }

    fn node(&self, index: usize) -> &RaftNode {
        self.nodes[index].as_ref().expect("Raft member is running")
    }

    fn task_store(&self, index: usize) -> Arc<TaskStore<RocksDbStore>> {
        Arc::new(TaskStore::new(self.stores[index].clone()))
    }

    async fn diagnostic_snapshot(&self, migration_id: &str) -> Vec<String> {
        let mut members = Vec::with_capacity(self.stores.len());
        for index in 0..self.stores.len() {
            let store = self.task_store(index);
            let migration = store
                .get_federated_migration(migration_id)
                .await
                .map(|value| format!("{value:#?}"))
                .unwrap_or_else(|error| format!("read_error: {error}"));
            let remote_operation = store
                .get_federated_remote_operation(migration_id, 0)
                .await
                .map(|value| format!("{value:#?}"))
                .unwrap_or_else(|error| format!("read_error: {error}"));
            let tasks = store
                .list_tasks()
                .await
                .map(|value| format!("{value:#?}"))
                .unwrap_or_else(|error| format!("read_error: {error}"));
            let raft_metrics = self.nodes[index]
                .as_ref()
                .map(|node| format!("{:#?}", node.handle().metrics().borrow().clone()))
                .unwrap_or_else(|| "offline".to_string());
            members.push(format!(
                "member_id={} running={} raft_metrics={} migration={} remote_operation={} tasks={}",
                self.ids[index],
                self.nodes[index].is_some(),
                raft_metrics,
                migration,
                remote_operation,
                tasks,
            ));
        }
        members
    }

    async fn wait_for_leader(&self, excluded: Option<usize>) -> usize {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                for (index, node) in self.nodes.iter().enumerate() {
                    if Some(index) != excluded
                        && let Some(node) = node
                        && node.is_leader().await
                    {
                        return index;
                    }
                }
                tokio::time::sleep(Duration::from_millis(75)).await;
            }
        })
        .await
        .expect("three-member Raft group did not elect a leader")
    }

    async fn stop(&mut self, index: usize) {
        if let Some(node) = self.nodes[index].take() {
            node.shutdown().await.unwrap();
            drop(node);
        }
        *self.raft_handles[index].write().await = None;
    }

    async fn restart(&mut self, index: usize) {
        // Drop the old, shut-down Raft handle before reopening its RocksDB path.
        *self.raft_handles[index].write().await = None;
        let node = RaftNode::new(
            self.ids[index],
            self.directories[index].path().join("raft"),
            TaskStore::new(self.stores[index].clone()),
            raft_config(Duration::from_millis(150), Duration::from_millis(600)),
            self.tokens.clone(),
        )
        .await
        .unwrap();
        *self.raft_handles[index].write().await = Some(node.handle());
        self.nodes[index] = Some(node);
    }

    async fn shutdown_all(&mut self) {
        for index in 0..3 {
            self.stop(index).await;
        }
        for shutdown in self.server_shutdowns.iter_mut().filter_map(Option::take) {
            shutdown.cancel();
        }
        for server in self.servers.iter_mut().filter_map(Option::take) {
            server.abort();
            let _ = server.await;
        }
    }

    async fn wait_replicated_migration(&self, migration_id: &str, expected: MigrationState) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let mut all = true;
                for (index, store) in self.stores.iter().enumerate() {
                    if self.nodes[index].is_none() {
                        continue;
                    }
                    let value = TaskStore::new(store.clone())
                        .get_federated_migration(migration_id)
                        .await
                        .unwrap();
                    if value.is_none_or(|migration| migration.state != expected) {
                        all = false;
                        break;
                    }
                }
                if all {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("federated migration did not replicate to every Raft member");
    }

    async fn wait_replicated_remote_operation(
        &self,
        migration_id: &str,
        generation: u64,
        expected: FederatedRemoteOperationState,
    ) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let mut all = true;
                for (index, store) in self.stores.iter().enumerate() {
                    if self.nodes[index].is_none() {
                        continue;
                    }
                    let value = TaskStore::new(store.clone())
                        .get_federated_remote_operation(migration_id, generation)
                        .await
                        .unwrap();
                    if value.is_none_or(|operation| operation.state != expected) {
                        all = false;
                        break;
                    }
                }
                if all {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("remote operation did not replicate to every Raft member");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn federation_mtls_recovery_survives_two_source_and_target_leader_changes() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut source = TestRaftGroup::start("federation-source-ha-token").await;
    let mut target = TestRaftGroup::start("federation-target-ha-token").await;
    let source_leader = source.wait_for_leader(None).await;
    let target_leader = target.wait_for_leader(None).await;

    let target_service = Arc::new(RwLock::new(TaskServiceImpl {
        task_store: target.task_store(target_leader),
        raft_node: Some(Arc::new(target.node(target_leader).clone())),
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_addr = listener.local_addr().unwrap().to_string();
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(SERVER_CERT, SERVER_KEY))
        .client_ca_root(Certificate::from_pem(CA_CERT));
    let shutdown = ShutdownToken::new();
    let server_shutdown = shutdown.clone();
    let faults = AckFaults::default();
    let server_faults = faults.clone();
    let server_service = target_service.clone();
    let server = tokio::spawn(async move {
        Server::builder()
            .layer(DropFederationAckLayer(server_faults))
            .tls_config(tls)
            .unwrap()
            .add_service(task::task_service_server::TaskServiceServer::new(
                server_service,
            ))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                server_shutdown.cancelled_owned(),
            )
            .await
            .unwrap();
    });

    let proxy = TcpFaultProxy::start(target_addr.clone()).await;
    let endpoint = proxy.endpoint.clone();
    for (group, local, peer, peer_endpoint) in [
        (
            &source,
            "federation-source-ha",
            "federation-target-ha",
            endpoint.clone(),
        ),
        (
            &target,
            "federation-target-ha",
            "federation-source-ha",
            "https://127.0.0.1:1".into(),
        ),
    ] {
        for index in 0..3 {
            let node = group.node(index);
            let now = chrono::Utc::now().timestamp_millis();
            for (cluster_id, cluster_endpoint) in [
                (local, "https://127.0.0.1:1".to_string()),
                (peer, peer_endpoint.clone()),
            ] {
                let result = node
                    .propose(Command::UpsertFederatedCluster {
                        cluster: FederatedCluster {
                            cluster_id: cluster_id.into(),
                            endpoint: cluster_endpoint,
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
                        request_id: format!("ha-register:{cluster_id}"),
                        actor: "e2e-ha".into(),
                    })
                    .await
                    .unwrap();
                assert!(result.success, "{}", result.message);
            }
        }
    }

    let now = chrono::Utc::now().timestamp_millis();
    let migration = FederatedMigration {
        migration_id: "ha-leader-change-migration".into(),
        source_cluster: "federation-source-ha".into(),
        target_cluster: "federation-target-ha".into(),
        task_id: "ha-logical-task".into(),
        required_capabilities: BTreeSet::from(["task-provisioning".into()]),
        state: MigrationState::Planned,
        generation: 0,
        created_at_ms: now,
        updated_at_ms: now,
        last_error: None,
        task_name: "ha-federated-task".into(),
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
        retry_budget: Some(5),
        timeout_policy: MigrationTimeoutPolicy::Fail,
    };
    let created = source
        .node(source_leader)
        .propose(Command::CreateFederatedMigration {
            migration: Box::new(migration),
            request_id: "ha-create-migration".into(),
            actor: "e2e-ha".into(),
        })
        .await
        .unwrap();
    assert!(created.success, "{}", created.message);
    source
        .wait_replicated_migration("ha-leader-change-migration", MigrationState::Planned)
        .await;

    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let orchestrator = |index: usize, group: &TestRaftGroup| {
        crate::federation::orchestrator::FederatedOrchestrator::new(
            group.task_store(index),
            Arc::new(group.node(index).clone()),
            "federation-source-ha",
            cert_dir.join("federation-test-ca.pem"),
            cert_dir.join("federation-test-client.pem"),
            cert_dir.join("federation-test-client.key.pem"),
        )
    };
    // A source follower may run the reconciliation entry point, but it must
    // not perform remote side effects or provision a second copy of the task.
    let follower_index = (source_leader + 1) % 3;
    let follower = orchestrator(follower_index, &source);
    follower.reconcile_once().await.unwrap();
    assert!(
        target
            .task_store(target_leader)
            .list_tasks()
            .await
            .unwrap()
            .is_empty()
    );
    drop(follower);

    let first = orchestrator(source_leader, &source);
    first.reconcile_once().await.unwrap(); // Planned -> Preparing
    proxy.set_partitioned(true);
    first.reconcile_once().await.unwrap();
    assert!(
        target
            .task_store(target_leader)
            .list_tasks()
            .await
            .unwrap()
            .is_empty()
    );
    proxy.set_partitioned(false);
    first.reconcile_once().await.unwrap();
    first.reconcile_once().await.unwrap();
    let remote_id = source
        .task_store(source_leader)
        .get_federated_migration("ha-leader-change-migration")
        .await
        .unwrap()
        .unwrap()
        .remote_task_id
        .unwrap();
    complete_remote_task_on_member(&target, target_leader, &remote_id).await;

    // Prepare is acknowledged, but the Commit response is deliberately lost
    // after the target has durably committed. This leaves source in Committing.
    first.reconcile_once().await.unwrap();
    faults.commit.store(true, Ordering::SeqCst);
    first.reconcile_once().await.unwrap();
    assert!(faults.commit_dropped.load(Ordering::SeqCst));
    let source_before_crash = source
        .task_store(source_leader)
        .get_federated_migration("ha-leader-change-migration")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source_before_crash.state, MigrationState::Committing);
    let committed = target
        .task_store(target_leader)
        .get_federated_remote_operation("ha-leader-change-migration", 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.state, FederatedRemoteOperationState::Committed);

    // Ensure the durable target commit has reached every member before the
    // original leader is stopped; the new leader must recover that same record.
    target
        .wait_replicated_remote_operation(
            "ha-leader-change-migration",
            0,
            FederatedRemoteOperationState::Committed,
        )
        .await;

    // Partition the inter-cluster link, then replace both leaders while each
    // Raft group retains its own quorum. Neither side may infer remote progress
    // from an unreachable peer.
    proxy.set_partitioned(true);
    target.stop(target_leader).await;
    source.stop(source_leader).await;
    let target_leader_2 = target.wait_for_leader(Some(target_leader)).await;
    let source_leader_2 = source.wait_for_leader(Some(source_leader)).await;
    *target_service.write().await = TaskServiceImpl {
        task_store: target.task_store(target_leader_2),
        raft_node: Some(Arc::new(target.node(target_leader_2).clone())),
    };
    target
        .wait_replicated_remote_operation(
            "ha-leader-change-migration",
            0,
            FederatedRemoteOperationState::Committed,
        )
        .await;
    target.restart(target_leader).await;
    target
        .wait_replicated_remote_operation(
            "ha-leader-change-migration",
            0,
            FederatedRemoteOperationState::Committed,
        )
        .await;
    source.restart(source_leader).await;
    source
        .wait_replicated_migration("ha-leader-change-migration", MigrationState::Committing)
        .await;

    // Source leader #1 crashes after target commit. Replacement leader recovers
    // the source state machine and completes the migration via the target's
    // durable idempotent Commit response.
    drop(first);
    let second = orchestrator(source_leader_2, &source);
    // The peer remains partitioned: source must stay Committing, not create a
    // replacement task or incorrectly mark the migration complete.
    second.reconcile_once().await.unwrap();
    let isolated = source
        .task_store(source_leader_2)
        .get_federated_migration("ha-leader-change-migration")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(isolated.state, MigrationState::Committing);

    // Deterministic chaos schedule: every emitted event is reproducible from
    // its seed. While the commit is in-doubt, an open link always loses its ACK;
    // a partitioned link fails before reaching the target. This keeps the
    // scenario in the recovery window until the schedule has been exhausted.
    let mut config = ChaosConfig::default();
    config.rounds = chaos_env_u64("FEDERATION_CHAOS_ROUNDS", config.rounds as u64) as usize;
    config.max_delay_ms = chaos_env_u64("FEDERATION_CHAOS_MAX_DELAY_MS", config.max_delay_ms);
    let seed = chaos_env_u64("FEDERATION_CHAOS_SEED", 0xDADA_2026_1124);
    let mut chaos = ChaosScheduler::new(seed, config);
    let mut artifacts = ChaosArtifactRecorder::new(seed, config);
    artifacts.capture(&source, &target).await;
    let source_follower = (0..3)
        .find(|index| *index != source_leader && *index != source_leader_2)
        .expect("source group has one non-leader member");
    let target_follower = (0..3)
        .find(|index| *index != target_leader && *index != target_leader_2)
        .expect("target group has one non-leader member");
    let mut partitioned = true;
    let mut source_follower_running = true;
    let mut target_follower_running = true;
    let mut source_crash_used = false;
    let mut target_crash_used = false;
    let mut event_log = Vec::new();
    while let Some(event) = chaos.next_event() {
        event_log.push(format!("{event:?}"));
        artifacts.record_event(event);
        match event {
            ChaosEvent::Delay(milliseconds) => {
                proxy.set_delay(Duration::from_millis(milliseconds));
            }
            ChaosEvent::Partition => {
                partitioned = true;
                proxy.set_partitioned(true);
            }
            ChaosEvent::Heal => {
                partitioned = false;
                proxy.set_partitioned(false);
            }
            ChaosEvent::DropCommitAck => {
                faults.commit.store(true, Ordering::SeqCst);
            }
            ChaosEvent::CrashSourceFollower => {
                if source_follower_running && !source_crash_used {
                    source.stop(source_follower).await;
                    source_follower_running = false;
                    source_crash_used = true;
                }
            }
            ChaosEvent::RestartSourceFollower => {
                if !source_follower_running && source_crash_used {
                    source.restart(source_follower).await;
                    source_follower_running = true;
                    source
                        .wait_replicated_migration(
                            "ha-leader-change-migration",
                            MigrationState::Committing,
                        )
                        .await;
                }
            }
            ChaosEvent::CrashTargetFollower => {
                if target_follower_running && !target_crash_used {
                    target.stop(target_follower).await;
                    target_follower_running = false;
                    target_crash_used = true;
                }
            }
            ChaosEvent::RestartTargetFollower => {
                if !target_follower_running && target_crash_used {
                    target.restart(target_follower).await;
                    target_follower_running = true;
                    target
                        .wait_replicated_remote_operation(
                            "ha-leader-change-migration",
                            0,
                            FederatedRemoteOperationState::Committed,
                        )
                        .await;
                }
            }
            ChaosEvent::DuplicateReconcile => {
                // Duplicate retries are exercised while isolated so neither
                // call can accidentally close the in-doubt commit window.
                partitioned = true;
                proxy.set_partitioned(true);
                let leader = source.wait_for_leader(None).await;
                let replay = orchestrator(leader, &source);
                replay.reconcile_once().await.unwrap();
                replay.reconcile_once().await.unwrap();
            }
        }

        let leader = source.wait_for_leader(None).await;
        let current_orchestrator = orchestrator(leader, &source);
        if !partitioned {
            // Force one durable target commit with a lost response. A duplicate
            // Commit is idempotent and must not create another remote task.
            faults.commit.store(true, Ordering::SeqCst);
        }
        current_orchestrator.reconcile_once().await.unwrap();
        let current = source
            .task_store(leader)
            .get_federated_migration("ha-leader-change-migration")
            .await
            .unwrap()
            .unwrap();
        artifacts.capture(&source, &target).await;
        assert_eq!(
            current.state,
            MigrationState::Committing,
            "seed {} event {} ({event:?}) must remain in-doubt; log={event_log:?}",
            chaos.seed(),
            event_log.len()
        );
        assert_eq!(
            target
                .task_store(target_leader_2)
                .list_tasks()
                .await
                .unwrap()
                .len(),
            1,
            "seed {} event {} duplicated remote task; log={event_log:?}",
            chaos.seed(),
            event_log.len()
        );
        proxy.set_delay(Duration::ZERO);
    }
    if !source_follower_running {
        source.restart(source_follower).await;
        source_follower_running = true;
    }
    if !target_follower_running {
        target.restart(target_follower).await;
    }
    // A DropCommitAck event that occurred while partitioned cannot be consumed
    // by an RPC. Do not carry that pending one-shot fault into the explicit
    // network-healed recovery phase; the final retry must be allowed to observe
    // the target's durable Committed response.
    faults.prepare.store(false, Ordering::SeqCst);
    faults.commit.store(false, Ordering::SeqCst);
    proxy.set_partitioned(false);
    second.reconcile_once().await.unwrap();
    target
        .wait_replicated_remote_operation(
            "ha-leader-change-migration",
            0,
            FederatedRemoteOperationState::Committed,
        )
        .await;
    for member in 0..3 {
        assert_eq!(
            target.task_store(member).list_tasks().await.unwrap().len(),
            1,
            "target member {member} must converge to exactly one provisioned task"
        );
    }
    source
        .wait_replicated_migration("ha-leader-change-migration", MigrationState::Completed)
        .await;

    // Ensure the chaos follower is back online and has applied the terminal
    // record, whether the seeded schedule restarted it earlier or left it
    // offline until this point.
    if !source_follower_running {
        source.restart(source_follower).await;
    }
    source
        .wait_replicated_migration("ha-leader-change-migration", MigrationState::Completed)
        .await;
    artifacts.capture(&source, &target).await;

    // Crash the replacement leader as well: a second election must preserve the
    // terminal state and reconciliation must remain a no-op on the new leader.
    second.reconcile_once().await.unwrap();
    source.stop(source_leader_2).await;
    let source_leader_3 = source.wait_for_leader(Some(source_leader_2)).await;
    let third = orchestrator(source_leader_3, &source);
    third.reconcile_once().await.unwrap();
    source
        .wait_replicated_migration("ha-leader-change-migration", MigrationState::Completed)
        .await;
    assert_eq!(
        target
            .task_store(target_leader_2)
            .list_tasks()
            .await
            .unwrap()
            .len(),
        1,
        "leader changes must not duplicate remote task provisioning"
    );
    assert_eq!(
        target
            .task_store(target_leader_2)
            .get_federated_remote_operation("ha-leader-change-migration", 0)
            .await
            .unwrap()
            .unwrap()
            .state,
        FederatedRemoteOperationState::Committed
    );
    artifacts.capture(&source, &target).await;

    // A stale generation from the pre-failover reconciler cannot mutate the
    // completed migration. Replaying an identical request ID is idempotent;
    // reusing that ID for a different payload is rejected.
    let stale_transition = Command::TransitionFederatedMigration {
        migration_id: "ha-leader-change-migration".into(),
        next_state: MigrationState::Failed,
        expected_generation: 99,
        at_ms: chrono::Utc::now().timestamp_millis(),
        error: Some("stale old-leader report".into()),
        request_id: "ha-stale-generation-transition".into(),
        actor: "stale-leader".into(),
    };
    assert!(
        !source
            .node(source_leader_3)
            .propose(stale_transition.clone())
            .await
            .unwrap()
            .success
    );
    assert!(
        !source
            .node(source_leader_3)
            .propose(stale_transition)
            .await
            .unwrap()
            .success
    );
    let conflicting_reuse = Command::TransitionFederatedMigration {
        migration_id: "ha-leader-change-migration".into(),
        next_state: MigrationState::Cancelled,
        expected_generation: 99,
        at_ms: chrono::Utc::now().timestamp_millis(),
        error: None,
        request_id: "ha-stale-generation-transition".into(),
        actor: "stale-leader".into(),
    };
    assert!(
        !source
            .node(source_leader_3)
            .propose(conflicting_reuse)
            .await
            .unwrap()
            .success
    );
    assert_eq!(
        source
            .task_store(source_leader_3)
            .get_federated_migration("ha-leader-change-migration")
            .await
            .unwrap()
            .unwrap()
            .state,
        MigrationState::Completed
    );

    // Repeatedly flap the inter-cluster TCP path and run terminal reconciliation.
    // The operation must converge without recreating tasks or changing terminal state.
    for cycle in 0..20 {
        artifacts.record_note(format!("terminal_reconcile_flap cycle={cycle}"));
        proxy.set_partitioned(true);
        third.reconcile_once().await.unwrap();
        proxy.set_partitioned(false);
        third.reconcile_once().await.unwrap();
        assert_eq!(
            target
                .task_store(target_leader_2)
                .list_tasks()
                .await
                .unwrap()
                .len(),
            1,
            "fault cycle {cycle} must not duplicate target task"
        );
        assert_eq!(
            source
                .task_store(source_leader_3)
                .get_federated_migration("ha-leader-change-migration")
                .await
                .unwrap()
                .unwrap()
                .state,
            MigrationState::Completed
        );
        artifacts.capture(&source, &target).await;
    }

    artifacts.record_note("stale_fencing_token_rejection_check".to_string());
    // Old shard fencing tokens must not be able to mutate the completed shard.
    let parsed: crate::types::TaskId = remote_id.parse().unwrap();
    let (_, shards) = target
        .task_store(target_leader_2)
        .get_task_with_aggregate_and_shards(&parsed)
        .await
        .unwrap()
        .unwrap();
    let shard = &shards[0];
    let current = target
        .task_store(target_leader_2)
        .get_shard(&shard.shard_id)
        .await
        .unwrap()
        .unwrap();
    let stale_fence = target
        .node(target_leader_2)
        .propose(Command::CompleteShard {
            shard_id: shard.shard_id.to_string(),
            node_id: "ha-e2e-worker".into(),
            fencing_token: current.fencing_token.saturating_sub(1),
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            total_records: 999,
        })
        .await
        .unwrap();
    assert!(!stale_fence.success, "stale fencing token must be rejected");
    let unchanged = target
        .task_store(target_leader_2)
        .get_shard(&shard.shard_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        unchanged.records_processed, 3,
        "stale fencing token changed committed shard data"
    );
    artifacts.capture(&source, &target).await;

    proxy.set_partitioned(true);
    proxy.task.abort();
    shutdown.cancel();
    let _ = server.await;
    source.shutdown_all().await;
    target.shutdown_all().await;
}

async fn complete_remote_task_on_member(group: &TestRaftGroup, member: usize, task_id: &str) {
    let parsed: crate::types::TaskId = task_id.parse().unwrap();
    let (_, shards) = group
        .task_store(member)
        .get_task_with_aggregate_and_shards(&parsed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(shards.len(), 1);
    let shard = &shards[0];
    let now = chrono::Utc::now().timestamp_millis();
    let claimed = group
        .node(member)
        .propose(Command::ClaimShard {
            shard_id: shard.shard_id.to_string(),
            node_id: "ha-e2e-worker".into(),
            claim_id: "ha-e2e-claim".into(),
            issued_at_ms: now,
            lease_expires_at_ms: now + 60_000,
        })
        .await
        .unwrap();
    assert!(claimed.success, "{}", claimed.message);
    let state = group
        .task_store(member)
        .get_shard(&shard.shard_id)
        .await
        .unwrap()
        .unwrap();
    let completed = group
        .node(member)
        .propose(Command::CompleteShard {
            shard_id: shard.shard_id.to_string(),
            node_id: "ha-e2e-worker".into(),
            fencing_token: state.fencing_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            total_records: 3,
        })
        .await
        .unwrap();
    assert!(completed.success, "{}", completed.message);
}
