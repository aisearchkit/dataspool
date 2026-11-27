// SPDX-License-Identifier: AGPL-3.0-or-later
//! Process-level certificate-rotation recovery across two independent Raft groups.
//! Each Raft member is a real child process and is terminated with SIGKILL.

#![cfg(unix)]

use dataspool::{
    comms::{GrpcClient, GrpcServer},
    federation::{
        FederatedCluster, FederatedClusterStatus, FederatedTrustRotation,
        FederatedTrustRotationEvent, FederatedTrustRotationPhase,
    },
    raft::{Command, RaftNode, raft_config},
    storage::RocksDbStore,
    task::TaskStore,
};
use openraft::BasicNode;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::{Child, Command as ProcessCommand, Stdio},
    time::Duration,
};
use tempfile::TempDir;

const OLD_FINGERPRINT: &str = "bb449e4e87f0e0fbf9f14d08981ca9002cca4255d3a497d5a619efc84ad3f5b2";
const NEW_FINGERPRINT: &str = "2a4be85981a297ad770d988445905efea9af66b726b8e5a42c4de8618e7bd06b";
const OLD_FINGERPRINT_CA: &str = "3b97b354a97ed41e7b6cbfc79dc0f76bf105128e32e42e72adbbaaa50e67d365";
const NEW_CA_FINGERPRINT: &str = "cc000414290fd64c9bc3cb369ed3047fd41a52d7ae89a6049bbbed6c38274f4b";
const ROTATION_CA: &[u8] = include_bytes!("fixtures/federation-rotation/ca.pem");
const ROTATION_SERVER_CERT: &[u8] = include_bytes!("fixtures/federation-rotation/server.pem");
const ROTATION_SERVER_KEY: &[u8] = include_bytes!("fixtures/federation-rotation/server.key.pem");
const ROTATION_OLD_CERT: &[u8] = include_bytes!("fixtures/federation-rotation/old-client.pem");
const ROTATION_OLD_KEY: &[u8] = include_bytes!("fixtures/federation-rotation/old-client.key.pem");
const ROTATION_NEW_CERT: &[u8] = include_bytes!("fixtures/federation-trust-bundle/new-client.pem");
const ROTATION_NEW_KEY: &[u8] =
    include_bytes!("fixtures/federation-trust-bundle/new-client.key.pem");
const ROTATION_NEW_CA: &[u8] = include_bytes!("fixtures/federation-trust-bundle/new-ca.pem");
const ASYMMETRIC_SOURCE_CA: &[u8] = include_bytes!("fixtures/federation-trust-bundle/old-ca.pem");
const ASYMMETRIC_SOURCE_CERT: &[u8] =
    include_bytes!("fixtures/federation-trust-bundle/old-server.pem");
const ASYMMETRIC_SOURCE_KEY: &[u8] =
    include_bytes!("fixtures/federation-trust-bundle/old-server.key.pem");
const ASYMMETRIC_SOURCE_CLIENT: &[u8] =
    include_bytes!("fixtures/federation-trust-bundle/old-client.pem");
const ASYMMETRIC_SOURCE_CLIENT_KEY: &[u8] =
    include_bytes!("fixtures/federation-trust-bundle/old-client.key.pem");
const ASYMMETRIC_TARGET_CA: &[u8] = include_bytes!("fixtures/federation-trust-bundle/new-ca.pem");
const ASYMMETRIC_TARGET_CERT: &[u8] =
    include_bytes!("fixtures/federation-trust-bundle/new-server.pem");
const ASYMMETRIC_TARGET_KEY: &[u8] =
    include_bytes!("fixtures/federation-trust-bundle/new-server.key.pem");
const ASYMMETRIC_TARGET_CLIENT: &[u8] =
    include_bytes!("fixtures/federation-trust-bundle/new-client.pem");
const ASYMMETRIC_TARGET_CLIENT_KEY: &[u8] =
    include_bytes!("fixtures/federation-trust-bundle/new-client.key.pem");
const RAFT_TOKEN: &str = "federated-rotation-process-test-token-0123456789";

/// The two federation directions must use independent trust roots: Source
/// authenticates Target with Target's CA while Target authenticates Source
/// with Source's CA. A shared CA fixture would not catch reversed CA wiring.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn federation_mtls_accepts_asymmetric_source_and_target_roots() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let source_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();
    let write = |dir: &TempDir, name: &str, bytes: &[u8]| {
        let path = dir.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    };

    let source_cert = write(&source_dir, "server.pem", ASYMMETRIC_SOURCE_CERT);
    let source_key = write(&source_dir, "server.key.pem", ASYMMETRIC_SOURCE_KEY);
    let source_client_ca = write(&source_dir, "client-ca.pem", ASYMMETRIC_TARGET_CA);
    let target_cert = write(&target_dir, "server.pem", ASYMMETRIC_TARGET_CERT);
    let target_key = write(&target_dir, "server.key.pem", ASYMMETRIC_TARGET_KEY);
    let target_client_ca = write(&target_dir, "client-ca.pem", ASYMMETRIC_SOURCE_CA);
    let source_root = write(&source_dir, "source-ca.pem", ASYMMETRIC_SOURCE_CA);
    let target_root = write(&target_dir, "target-ca.pem", ASYMMETRIC_TARGET_CA);
    let source_client = write(&source_dir, "source-client.pem", ASYMMETRIC_SOURCE_CLIENT);
    let source_client_key = write(
        &source_dir,
        "source-client.key.pem",
        ASYMMETRIC_SOURCE_CLIENT_KEY,
    );
    let target_client = write(&target_dir, "target-client.pem", ASYMMETRIC_TARGET_CLIENT);
    let target_client_key = write(
        &target_dir,
        "target-client.key.pem",
        ASYMMETRIC_TARGET_CLIENT_KEY,
    );

    let source_db = RocksDbStore::open(source_dir.path().join("source-db")).unwrap();
    let target_db = RocksDbStore::open(target_dir.path().join("target-db")).unwrap();
    let source_addr = reserve_local_addr();
    let target_addr = reserve_local_addr();
    let source_server = GrpcServer::new(
        &source_addr,
        std::sync::Arc::new(TaskStore::new(source_db)),
        None,
    )
    .await
    .unwrap()
    .with_mtls_files(&source_cert, &source_key, &source_client_ca)
    .await
    .unwrap();
    let target_server = GrpcServer::new(
        &target_addr,
        std::sync::Arc::new(TaskStore::new(target_db)),
        None,
    )
    .await
    .unwrap()
    .with_mtls_files(&target_cert, &target_key, &target_client_ca)
    .await
    .unwrap();
    let source_task = tokio::spawn(async move {
        source_server.serve().await.unwrap();
    });
    let target_task = tokio::spawn(async move {
        target_server.serve().await.unwrap();
    });

    // Source -> Target: Target's server certificate is rooted at Target CA;
    // its inbound client trust is Source CA.
    let source_to_target = connect_mtls_retry(
        &format!("https://{target_addr}"),
        &target_root,
        &source_client,
        &source_client_key,
    )
    .await;
    drop(source_to_target);
    // Target -> Source exercises the opposite CA direction independently.
    let target_to_source = connect_mtls_retry(
        &format!("https://{source_addr}"),
        &source_root,
        &target_client,
        &target_client_key,
    )
    .await;
    drop(target_to_source);

    source_task.abort();
    target_task.abort();
}

fn child_process(
    role: &str,
    phase: &str,
    root: &Path,
    raft_addr: &str,
    grpc_addr: &str,
    marker: &Path,
) -> Child {
    ProcessCommand::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "federated_certificate_rotation_recovers_after_sigkill_across_both_raft_processes",
            "--nocapture",
        ])
        .env("DATASPOOL_ROTATION_PROCESS_ROLE", role)
        .env("DATASPOOL_ROTATION_PROCESS_PHASE", phase)
        .env("DATASPOOL_ROTATION_PROCESS_ROOT", root)
        .env("DATASPOOL_ROTATION_PROCESS_RAFT_ADDR", raft_addr)
        .env("DATASPOOL_ROTATION_PROCESS_MARKER", marker)
        .env("DATASPOOL_ROTATION_PROCESS_GRPC_ADDR", grpc_addr)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn independent Raft process")
}

async fn connect_mtls_retry(endpoint: &str, ca: &Path, cert: &Path, key: &Path) -> GrpcClient {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match GrpcClient::connect_mtls(endpoint, ca, cert, key).await {
                Ok(client) => return client,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .expect("recovered mTLS server did not become ready")
}

async fn wait_for_marker(path: &Path, child: &mut Child) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if path.exists() {
                return;
            }
            if let Some(status) = child.try_wait().expect("poll child") {
                panic!("child exited before marker {}: {status}", path.display());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {}", path.display()));
}

fn assert_sigkill(child: &mut Child, reason: &str) {
    child.kill().expect("send SIGKILL to Raft process");
    let status = child.wait().expect("wait for killed Raft process");
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(
        status.signal(),
        Some(9),
        "{reason}: child must receive SIGKILL"
    );
}

async fn run_child_process() {
    // This binary enables both rustls ring and aws-lc through its dependency
    // graph; child processes must select the same provider explicitly as the
    // existing in-process mTLS federation tests do.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let role = std::env::var("DATASPOOL_ROTATION_PROCESS_ROLE").unwrap();
    let phase = std::env::var("DATASPOOL_ROTATION_PROCESS_PHASE").unwrap();
    let root = PathBuf::from(std::env::var("DATASPOOL_ROTATION_PROCESS_ROOT").unwrap());
    let raft_addr = std::env::var("DATASPOOL_ROTATION_PROCESS_RAFT_ADDR").unwrap();
    let marker = PathBuf::from(std::env::var("DATASPOOL_ROTATION_PROCESS_MARKER").unwrap());
    let grpc_addr = std::env::var("DATASPOOL_ROTATION_PROCESS_GRPC_ADDR").unwrap();
    let node_id = if role == "source" { 81_101 } else { 82_202 };
    let peer_id = if role == "source" {
        "target-process"
    } else {
        "source-process"
    };
    let db = RocksDbStore::open(root.join("app")).expect("open durable application store");
    let store = TaskStore::new(db.clone());
    let node = RaftNode::new(
        node_id,
        root.join("raft"),
        store.clone(),
        raft_config(Duration::from_millis(120), Duration::from_millis(500)),
        RAFT_TOKEN.into(),
    )
    .await
    .expect("open persistent Raft node");
    let listener = tokio::net::TcpListener::bind(&raft_addr)
        .await
        .expect("bind stable Raft address");
    let router = dataspool::raft_router(node.handle(), RAFT_TOKEN.into());
    let raft_server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve Raft RPC");
    });
    if !node.is_initialized().await.unwrap() {
        node.initialize(BTreeMap::from([(
            node_id,
            BasicNode::new(raft_addr.clone()),
        )]))
        .await
        .expect("initialize single-member Raft group");
    }
    node.wait_for_leader(Duration::from_secs(15))
        .await
        .expect("elect Raft leader");
    node.ensure_linearizable()
        .await
        .expect("linearizable Raft read");

    if phase == "stage" {
        let now = chrono::Utc::now().timestamp_millis();
        let cluster = FederatedCluster {
            cluster_id: peer_id.into(),
            endpoint: "https://127.0.0.1:1".into(),
            identity_fingerprint: OLD_FINGERPRINT.into(),
            previous_identity_fingerprint: None,
            previous_identity_expires_at_ms: None,
            pending_identity_fingerprint: None,
            rotation_started_at_ms: None,
            pending_rotation_grace_period_ms: None,
            trust_bundle_pem: Some(String::from_utf8(ROTATION_CA.to_vec()).unwrap()),
            trust_bundle_fingerprint: Some(
                dataspool::federation::validate_trust_bundle(
                    std::str::from_utf8(ROTATION_CA).unwrap(),
                )
                .unwrap(),
            ),
            pending_trust_bundle_pem: None,
            pending_trust_bundle_fingerprint: None,
            trust_bundle_staged_at_ms: None,
            previous_trust_bundle_pem: None,
            previous_trust_bundle_fingerprint: None,
            previous_trust_bundle_expires_at_ms: None,
            version: env!("CARGO_PKG_VERSION").into(),
            capabilities: BTreeSet::new(),
            status: FederatedClusterStatus::Healthy,
            last_seen_at_ms: now,
            updated_at_ms: now,
        };
        let registered = node
            .propose(Command::UpsertFederatedCluster {
                cluster,
                request_id: format!("register-process-{role}"),
                actor: "process-crash-e2e".into(),
            })
            .await
            .unwrap();
        assert!(registered.success, "{}", registered.message);
        let staged = node
            .propose(Command::StageFederatedCertificateRotation {
                cluster_id: peer_id.into(),
                current_fingerprint: OLD_FINGERPRINT.into(),
                new_fingerprint: NEW_FINGERPRINT.into(),
                at_ms: now,
                grace_period_ms: 300_000,
                request_id: format!("stage-process-{role}"),
                actor: "old-certificate".into(),
            })
            .await
            .unwrap();
        assert!(staged.success, "{}", staged.message);
        let persisted = store.get_federated_cluster(peer_id).await.unwrap().unwrap();
        assert_eq!(
            persisted.pending_identity_fingerprint.as_deref(),
            Some(NEW_FINGERPRINT)
        );
        let new_ca_pem = String::from_utf8(ROTATION_NEW_CA.to_vec()).unwrap();
        let staged_ca = node
            .propose(Command::StageFederatedTrustBundle {
                cluster_id: peer_id.into(),
                current_fingerprint: persisted.trust_bundle_fingerprint.clone().unwrap(),
                bundle_pem: new_ca_pem,
                bundle_fingerprint: NEW_CA_FINGERPRINT.into(),
                at_ms: now,
                request_id: format!("stage-ca-process-{role}"),
                actor: "process-crash-e2e".into(),
            })
            .await
            .unwrap();
        assert!(staged_ca.success, "{}", staged_ca.message);
        let persisted = store.get_federated_cluster(peer_id).await.unwrap().unwrap();
        assert_eq!(
            persisted.pending_trust_bundle_fingerprint.as_deref(),
            Some(NEW_CA_FINGERPRINT)
        );
        std::fs::write(&marker, b"stage-committed").unwrap();
    } else if phase == "proof" {
        let staged = store.get_federated_cluster(peer_id).await.unwrap().unwrap();
        assert_eq!(staged.identity_fingerprint, OLD_FINGERPRINT);
        assert_eq!(
            staged.pending_identity_fingerprint.as_deref(),
            Some(NEW_FINGERPRINT)
        );
        assert_eq!(
            staged.pending_trust_bundle_fingerprint.as_deref(),
            Some(NEW_CA_FINGERPRINT)
        );
        let activated_at = chrono::Utc::now().timestamp_millis();
        let activated_ca = node
            .propose(Command::ActivateFederatedTrustBundle {
                cluster_id: peer_id.into(),
                bundle_fingerprint: NEW_CA_FINGERPRINT.into(),
                at_ms: activated_at,
                overlap_ms: 60_000,
                request_id: format!("activate-ca-process-{role}"),
                actor: "process-crash-e2e".into(),
            })
            .await
            .unwrap();
        assert!(activated_ca.success, "{}", activated_ca.message);
        let activated = store.get_federated_cluster(peer_id).await.unwrap().unwrap();
        assert_eq!(
            activated.trust_bundle_fingerprint.as_deref(),
            Some(NEW_CA_FINGERPRINT)
        );
        assert_eq!(
            activated.previous_trust_bundle_fingerprint.as_deref(),
            Some(OLD_FINGERPRINT_CA)
        );
        assert_eq!(
            activated.previous_trust_bundle_expires_at_ms,
            Some(activated_at + 60_000)
        );
        let cert_dir = root.join("proof-certs");
        std::fs::create_dir_all(&cert_dir).unwrap();
        let server_cert = cert_dir.join("server.pem");
        let server_key = cert_dir.join("server.key.pem");
        let ca = cert_dir.join("ca.pem");
        let new_cert = cert_dir.join("new-client.pem");
        let new_key = cert_dir.join("new-client.key.pem");
        std::fs::write(&server_cert, ROTATION_SERVER_CERT).unwrap();
        std::fs::write(&server_key, ROTATION_SERVER_KEY).unwrap();
        std::fs::write(&ca, ROTATION_CA).unwrap();
        std::fs::write(&new_cert, ROTATION_NEW_CERT).unwrap();
        std::fs::write(&new_key, ROTATION_NEW_KEY).unwrap();
        let dropped_ack = root.join("proof-ack-dropped");
        let grpc = GrpcServer::new(
            &grpc_addr,
            std::sync::Arc::new(store.clone()),
            Some(std::sync::Arc::new(node.clone())),
        )
        .await
        .unwrap()
        .with_mtls_files(&server_cert, &server_key, &ca)
        .await
        .unwrap()
        .with_rotation_proof_ack_drop_marker(dropped_ack.clone());
        tokio::spawn(async move {
            grpc.serve()
                .await
                .expect("serve proof fault-injection mTLS endpoint");
        });
        let mut new_client =
            connect_mtls_retry(&format!("https://{grpc_addr}"), &ca, &new_cert, &new_key).await;
        let proof = new_client
            .prove_federated_certificate_rotation(
                peer_id,
                NEW_FINGERPRINT,
                &format!("proof-process-{role}"),
            )
            .await;
        assert!(
            proof.is_err(),
            "fault proxy must hide the committed Proof response"
        );
        assert!(
            dropped_ack.exists(),
            "Proof handler must commit before ACK is dropped"
        );
        let persisted = store.get_federated_cluster(peer_id).await.unwrap().unwrap();
        assert_eq!(persisted.identity_fingerprint, NEW_FINGERPRINT);
        assert_eq!(
            persisted.previous_identity_fingerprint.as_deref(),
            Some(OLD_FINGERPRINT)
        );
        // The handler committed Proof, then the test-only gRPC middleware
        // replaced its successful response with UNAVAILABLE. The parent now
        // SIGKILLs this process before the client can retry.
        std::fs::write(&marker, b"proof-committed-response-dropped").unwrap();
    } else if phase == "verify" {
        let active = store.get_federated_cluster(peer_id).await.unwrap().unwrap();
        assert_eq!(active.identity_fingerprint, NEW_FINGERPRINT);
        assert_eq!(
            active.previous_identity_fingerprint.as_deref(),
            Some(OLD_FINGERPRINT)
        );
        assert!(active.pending_identity_fingerprint.is_none());
        assert_eq!(
            active.trust_bundle_fingerprint.as_deref(),
            Some(NEW_CA_FINGERPRINT)
        );
        assert_eq!(
            active.previous_trust_bundle_fingerprint.as_deref(),
            Some(OLD_FINGERPRINT_CA)
        );
        let ca_expiry = active.previous_trust_bundle_expires_at_ms.unwrap();
        assert!(
            active
                .effective_trust_bundle(ca_expiry - 1)
                .unwrap()
                .contains("BEGIN CERTIFICATE")
        );
        let replay_ca = node
            .propose(Command::ActivateFederatedTrustBundle {
                cluster_id: peer_id.into(),
                bundle_fingerprint: NEW_CA_FINGERPRINT.into(),
                at_ms: ca_expiry + 60_000,
                overlap_ms: 60_000,
                request_id: format!("activate-ca-process-{role}"),
                actor: "lost-ack-replay-after-restart".into(),
            })
            .await
            .unwrap();
        assert!(replay_ca.success, "{}", replay_ca.message);
        let after_ca_replay = store.get_federated_cluster(peer_id).await.unwrap().unwrap();
        assert_eq!(
            after_ca_replay.previous_trust_bundle_expires_at_ms,
            Some(ca_expiry)
        );
        assert_eq!(
            after_ca_replay.effective_trust_bundle(ca_expiry).unwrap(),
            String::from_utf8(ROTATION_NEW_CA.to_vec()).unwrap()
        );
        let expiry = active.previous_identity_expires_at_ms.unwrap();
        assert!(active.accepts_fingerprint(OLD_FINGERPRINT, expiry - 1));
        assert!(!active.accepts_fingerprint(OLD_FINGERPRINT, expiry));
        assert!(active.accepts_fingerprint(NEW_FINGERPRINT, expiry));

        // A restarted former leader replaying the old Stage operation cannot
        // re-authorize the old certificate or overwrite the committed identity.
        let stale_stage = node
            .propose(Command::StageFederatedCertificateRotation {
                cluster_id: peer_id.into(),
                current_fingerprint: OLD_FINGERPRINT.into(),
                new_fingerprint: "c".repeat(64),
                at_ms: expiry,
                grace_period_ms: 60_000,
                request_id: format!("stale-old-leader-{role}"),
                actor: "stale-old-leader".into(),
            })
            .await
            .unwrap();
        assert!(
            !stale_stage.success,
            "expired old identity must not stage another rotation"
        );

        // Replaying the already committed Proof is idempotent and cannot move
        // the overlap deadline, even after both Raft processes restarted.
        let replay = node
            .propose(Command::CompleteFederatedCertificateRotation {
                cluster_id: peer_id.into(),
                new_fingerprint: NEW_FINGERPRINT.into(),
                at_ms: expiry + 60_000,
                request_id: format!("proof-process-{role}"),
                actor: "proof-replay-after-restart".into(),
            })
            .await
            .unwrap();
        assert!(replay.success, "{}", replay.message);
        let after_replay = store.get_federated_cluster(peer_id).await.unwrap().unwrap();
        assert_eq!(after_replay.previous_identity_expires_at_ms, Some(expiry));
        assert!(!after_replay.accepts_fingerprint(OLD_FINGERPRINT, expiry + 60_000));
        let cert_dir = root.join("rotation-certs");
        std::fs::create_dir_all(&cert_dir).unwrap();
        let cert = cert_dir.join("server.pem");
        let key = cert_dir.join("server.key.pem");
        let ca = cert_dir.join("ca.pem");
        std::fs::write(&cert, ROTATION_SERVER_CERT).unwrap();
        std::fs::write(&key, ROTATION_SERVER_KEY).unwrap();
        std::fs::write(&ca, ROTATION_CA).unwrap();
        let grpc = GrpcServer::new(
            &grpc_addr,
            std::sync::Arc::new(store.clone()),
            Some(std::sync::Arc::new(node.clone())),
        )
        .await
        .unwrap()
        .with_mtls_files(&cert, &key, &ca)
        .await
        .unwrap();
        tokio::spawn(async move {
            grpc.serve().await.expect("serve recovered mTLS endpoint");
        });
        std::fs::write(&marker, b"recovered-and-verified").unwrap();
        // Keep the recovered Raft and mTLS servers alive while the parent
        // reconnects using both expired and active leaf certificates.
        std::future::pending::<()>().await;
    } else {
        panic!("unknown child phase: {phase}");
    }

    if phase != "verify" {
        // Keep the actual Raft process alive until the parent sends SIGKILL.
        std::future::pending::<()>().await;
    }
    node.shutdown().await.unwrap();
    raft_server.abort();
}

fn reserve_local_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn six_member_child_process(
    cluster: &str,
    member: usize,
    root: &Path,
    addresses: &str,
    ready_dir: &Path,
    control: &Path,
    markers: &Path,
) -> Child {
    ProcessCommand::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "federated_trust_bundle_rotation_survives_sigkill_of_both_three_member_raft_leaders",
            "--nocapture",
        ])
        .env("DATASPOOL_TRUST_HA_CHILD", "1")
        .env("DATASPOOL_TRUST_HA_CLUSTER", cluster)
        .env("DATASPOOL_TRUST_HA_MEMBER", member.to_string())
        .env("DATASPOOL_TRUST_HA_ROOT", root)
        .env("DATASPOOL_TRUST_HA_ADDRESSES", addresses)
        .env("DATASPOOL_TRUST_HA_READY_DIR", ready_dir)
        .env("DATASPOOL_TRUST_HA_CONTROL", control)
        .env("DATASPOOL_TRUST_HA_MARKERS", markers)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn six-member Raft child")
}

#[allow(clippy::too_many_arguments)]
fn coordinator_raft_child_process(
    cluster: &str,
    member: usize,
    root: &Path,
    addresses: &str,
    ready_dir: &Path,
    control: &Path,
    markers: &Path,
    grpc_addr: &str,
    peer_grpc_addr: &str,
    cert_dir: &Path,
    ack_marker: &Path,
    ack_gate: &Path,
) -> Child {
    ProcessCommand::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "federated_trust_rotation_coordinator_process_recovers_after_remote_activate_ack_loss",
            "--nocapture",
        ])
        .env("DATASPOOL_TRUST_HA_CHILD", "1")
        .env("DATASPOOL_TRUST_HA_CLUSTER", cluster)
        .env("DATASPOOL_TRUST_HA_MEMBER", member.to_string())
        .env("DATASPOOL_TRUST_HA_ROOT", root)
        .env("DATASPOOL_TRUST_HA_ADDRESSES", addresses)
        .env("DATASPOOL_TRUST_HA_READY_DIR", ready_dir)
        .env("DATASPOOL_TRUST_HA_CONTROL", control)
        .env("DATASPOOL_TRUST_HA_MARKERS", markers)
        .env("DATASPOOL_TRUST_HA_GRPC_ADDR", grpc_addr)
        .env("DATASPOOL_TRUST_HA_PEER_GRPC_ADDR", peer_grpc_addr)
        .env("DATASPOOL_TRUST_HA_CERT_DIR", cert_dir)
        .env("DATASPOOL_TRUST_HA_ACK_MARKER", ack_marker)
        .env("DATASPOOL_TRUST_HA_ACK_GATE", ack_gate)
        .env("DATASPOOL_TRUST_HA_COORDINATOR_MODE", "1")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn three-member Raft node with federation gRPC endpoint")
}

async fn run_rollback_coordinator_process() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let phase = std::env::var("DATASPOOL_ROLLBACK_COORDINATOR_PHASE").unwrap();
    let source_endpoint = std::env::var("DATASPOOL_ROLLBACK_SOURCE_ENDPOINT").unwrap();
    let target_endpoint = std::env::var("DATASPOOL_ROLLBACK_TARGET_ENDPOINT").unwrap();
    let cert_dir = PathBuf::from(std::env::var("DATASPOOL_ROLLBACK_CERT_DIR").unwrap());
    let marker = PathBuf::from(std::env::var("DATASPOOL_ROLLBACK_MARKER").unwrap());
    let ca = cert_dir.join("ca.pem");
    let client_cert = cert_dir.join("client.pem");
    let client_key = cert_dir.join("client.key.pem");
    let mut coordinator = dataspool::FederatedTrustRotationProcessCoordinator::connect(
        "source",
        "target",
        "rollback-process-rotation",
        "rollback-process",
        &source_endpoint,
        &target_endpoint,
        &ca,
        &ca,
        &client_cert,
        &client_key,
        &client_cert,
        &client_key,
    )
    .await
    .unwrap();
    if matches!(phase.as_str(), "initial" | "recover-first") {
        let result = coordinator
            .rollback_before_activation("operator requested rollback before activation")
            .await;
        let fault_boundary = if phase == "initial" {
            "source BeginRollback ACK"
        } else {
            "target rollback acknowledgement"
        };
        panic!("{fault_boundary} should be dropped after Raft commit: {result:?}");
    }
    let (source, target) = coordinator
        .rollback_before_activation("operator requested rollback before activation")
        .await
        .unwrap();
    assert_eq!(
        source.rotation.phase,
        FederatedTrustRotationPhase::RolledBack
    );
    assert_eq!(
        target.rotation.phase,
        FederatedTrustRotationPhase::RolledBack
    );
    assert!(!source.rotation.local_staged && !source.rotation.peer_staged);
    assert!(!target.rotation.local_staged && !target.rotation.peer_staged);
    assert!(source.rotation.local_activation_expires_at_ms.is_none());
    assert!(source.rotation.peer_activation_expires_at_ms.is_none());
    assert!(target.rotation.local_activation_expires_at_ms.is_none());
    assert!(target.rotation.peer_activation_expires_at_ms.is_none());

    // A replay after the terminal state is durable must be a pure read: no
    // revision changes, no new stage, and no activation deadline appears.
    let source_revision = source.rotation.revision;
    let target_revision = target.rotation.revision;
    let (source_replay, target_replay) = coordinator
        .rollback_before_activation("operator requested rollback before activation")
        .await
        .unwrap();
    assert_eq!(source_replay.rotation.revision, source_revision);
    assert_eq!(target_replay.rotation.revision, target_revision);
    assert_eq!(
        source_replay.rotation.phase,
        FederatedTrustRotationPhase::RolledBack
    );
    assert_eq!(
        target_replay.rotation.phase,
        FederatedTrustRotationPhase::RolledBack
    );
    std::fs::write(&marker, b"bilateral-rollback-recovered").unwrap();
}

fn rollback_coordinator_process_child(
    phase: &str,
    source_endpoint: &str,
    target_endpoint: &str,
    cert_dir: &Path,
    marker: &Path,
) -> Child {
    ProcessCommand::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "federated_trust_rotation_rollback_recovers_after_local_and_remote_ack_loss",
            "--nocapture",
        ])
        .env("DATASPOOL_ROLLBACK_COORDINATOR_PHASE", phase)
        .env("DATASPOOL_ROLLBACK_SOURCE_ENDPOINT", source_endpoint)
        .env("DATASPOOL_ROLLBACK_TARGET_ENDPOINT", target_endpoint)
        .env("DATASPOOL_ROLLBACK_CERT_DIR", cert_dir)
        .env("DATASPOOL_ROLLBACK_MARKER", marker)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn independent rollback coordinator process")
}

#[allow(clippy::too_many_arguments)]
fn rollback_raft_child_process(
    cluster: &str,
    member: usize,
    root: &Path,
    addresses: &str,
    ready_dir: &Path,
    control: &Path,
    markers: &Path,
    grpc_addr: &str,
    peer_grpc_addr: &str,
    cert_dir: &Path,
    ack_marker: &Path,
    ack_gate: &Path,
    drop_after: usize,
) -> Child {
    ProcessCommand::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "federated_trust_rotation_rollback_recovers_after_local_and_remote_ack_loss",
            "--nocapture",
        ])
        .env("DATASPOOL_TRUST_HA_CHILD", "1")
        .env("DATASPOOL_TRUST_HA_CLUSTER", cluster)
        .env("DATASPOOL_TRUST_HA_MEMBER", member.to_string())
        .env("DATASPOOL_TRUST_HA_ROOT", root)
        .env("DATASPOOL_TRUST_HA_ADDRESSES", addresses)
        .env("DATASPOOL_TRUST_HA_READY_DIR", ready_dir)
        .env("DATASPOOL_TRUST_HA_CONTROL", control)
        .env("DATASPOOL_TRUST_HA_MARKERS", markers)
        .env("DATASPOOL_TRUST_HA_GRPC_ADDR", grpc_addr)
        .env("DATASPOOL_TRUST_HA_PEER_GRPC_ADDR", peer_grpc_addr)
        .env("DATASPOOL_TRUST_HA_CERT_DIR", cert_dir)
        .env("DATASPOOL_TRUST_HA_ACK_MARKER", ack_marker)
        .env("DATASPOOL_TRUST_HA_ACK_GATE", ack_gate)
        .env("DATASPOOL_TRUST_HA_ACK_DROP_AFTER", drop_after.to_string())
        .env("DATASPOOL_TRUST_HA_COORDINATOR_MODE", "1")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn rollback Raft member process")
}

fn coordinator_process_child(
    phase: &str,
    source_endpoint: &str,
    target_endpoint: &str,
    cert_dir: &Path,
    expiry_file: &Path,
    marker: &Path,
) -> Child {
    ProcessCommand::new(std::env::current_exe().expect("integration test executable"))
        .args([
            "--exact",
            "federated_trust_rotation_coordinator_process_recovers_after_remote_activate_ack_loss",
            "--nocapture",
        ])
        .env("DATASPOOL_COORDINATOR_PROCESS_PHASE", phase)
        .env("DATASPOOL_COORDINATOR_SOURCE_ENDPOINT", source_endpoint)
        .env("DATASPOOL_COORDINATOR_TARGET_ENDPOINT", target_endpoint)
        .env("DATASPOOL_COORDINATOR_CERT_DIR", cert_dir)
        .env("DATASPOOL_COORDINATOR_EXPIRY_FILE", expiry_file)
        .env("DATASPOOL_COORDINATOR_MARKER", marker)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn independent coordinator OS process")
}

fn write_control(path: &Path, value: &str) {
    let next = path.with_extension("next");
    std::fs::write(&next, value).unwrap();
    std::fs::rename(next, path).unwrap();
}

fn write_atomic_marker(path: &Path, value: &str) {
    let name = path.file_name().unwrap().to_string_lossy();
    let next = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));
    std::fs::write(&next, value).unwrap();
    std::fs::rename(next, path).unwrap();
}

fn read_marker_index(path: &Path) -> Option<usize> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

async fn wait_for_file(path: &Path, children: &mut [Child]) {
    tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            if path.exists() {
                return;
            }
            for child in children.iter_mut() {
                if let Some(status) = child.try_wait().expect("poll six-member child") {
                    use std::os::unix::process::ExitStatusExt;
                    if status.signal() != Some(9) {
                        panic!(
                            "six-member child exited unexpectedly before {}: {status}",
                            path.display()
                        );
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {}", path.display()));
}

async fn seed_coordinator_rotation(
    node: &RaftNode,
    store: &TaskStore<RocksDbStore>,
    cluster_id: &str,
    peer_id: &str,
    peer_endpoint: &str,
    rotation_id: &str,
    authorize_activation: bool,
) {
    let now = chrono::Utc::now().timestamp_millis();
    let old_ca_pem = String::from_utf8(ROTATION_CA.to_vec()).unwrap();
    let new_ca_pem = String::from_utf8(ROTATION_NEW_CA.to_vec()).unwrap();
    let registered = node
        .propose(Command::UpsertFederatedCluster {
            cluster: FederatedCluster {
                cluster_id: peer_id.into(),
                endpoint: format!("https://{peer_endpoint}"),
                identity_fingerprint: OLD_FINGERPRINT.into(),
                previous_identity_fingerprint: None,
                previous_identity_expires_at_ms: None,
                pending_identity_fingerprint: None,
                rotation_started_at_ms: None,
                pending_rotation_grace_period_ms: None,
                trust_bundle_pem: Some(old_ca_pem.clone()),
                trust_bundle_fingerprint: Some(OLD_FINGERPRINT_CA.into()),
                pending_trust_bundle_pem: None,
                pending_trust_bundle_fingerprint: None,
                trust_bundle_staged_at_ms: None,
                previous_trust_bundle_pem: None,
                previous_trust_bundle_fingerprint: None,
                previous_trust_bundle_expires_at_ms: None,
                version: env!("CARGO_PKG_VERSION").into(),
                capabilities: BTreeSet::new(),
                status: FederatedClusterStatus::Healthy,
                last_seen_at_ms: now,
                updated_at_ms: now,
            },
            request_id: format!("coordinator-process-register-{cluster_id}"),
            actor: "coordinator-process-e2e".into(),
        })
        .await
        .unwrap();
    assert!(registered.success, "{}", registered.message);

    let rotation = FederatedTrustRotation {
        rotation_id: rotation_id.into(),
        peer_cluster_id: peer_id.into(),
        current_peer_ca_fingerprint: Some(OLD_FINGERPRINT_CA.into()),
        local_ca_bundle_pem: new_ca_pem.clone(),
        local_ca_bundle_fingerprint: NEW_CA_FINGERPRINT.into(),
        peer_ca_bundle_pem: new_ca_pem.clone(),
        peer_ca_bundle_fingerprint: NEW_CA_FINGERPRINT.into(),
        local_leaf_fingerprint: NEW_FINGERPRINT.into(),
        peer_leaf_fingerprint: NEW_FINGERPRINT.into(),
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
        last_error: None,
        revision: 0,
        created_at_ms: now,
        updated_at_ms: now,
    };
    let created = node
        .propose(Command::CreateFederatedTrustRotation {
            rotation: Box::new(rotation),
            at_ms: now,
            request_id: format!("coordinator-process-create-{cluster_id}"),
            actor: "coordinator-process-e2e".into(),
        })
        .await
        .unwrap();
    assert!(created.success, "{}", created.message);

    let staged_leaf = node
        .propose(Command::StageFederatedCertificateRotation {
            cluster_id: peer_id.into(),
            current_fingerprint: OLD_FINGERPRINT.into(),
            new_fingerprint: NEW_FINGERPRINT.into(),
            at_ms: now,
            grace_period_ms: 60_000,
            request_id: format!("coordinator-process-stage-leaf-{cluster_id}"),
            actor: "coordinator-process-e2e".into(),
        })
        .await
        .unwrap();
    assert!(staged_leaf.success, "{}", staged_leaf.message);
    let staged_ca = node
        .propose(Command::StageFederatedTrustBundle {
            cluster_id: peer_id.into(),
            current_fingerprint: OLD_FINGERPRINT_CA.into(),
            bundle_pem: new_ca_pem,
            bundle_fingerprint: NEW_CA_FINGERPRINT.into(),
            at_ms: now,
            request_id: format!("coordinator-process-stage-ca-{cluster_id}"),
            actor: "coordinator-process-e2e".into(),
        })
        .await
        .unwrap();
    assert!(staged_ca.success, "{}", staged_ca.message);

    let mut events = vec![
        FederatedTrustRotationEvent::LocalStaged {
            fingerprint: NEW_CA_FINGERPRINT.into(),
        },
        FederatedTrustRotationEvent::PeerStaged {
            fingerprint: NEW_CA_FINGERPRINT.into(),
        },
        FederatedTrustRotationEvent::LocalDeploymentReady {
            leaf_fingerprint: NEW_FINGERPRINT.into(),
        },
        FederatedTrustRotationEvent::PeerDeploymentReady {
            leaf_fingerprint: NEW_FINGERPRINT.into(),
        },
    ];
    if authorize_activation {
        events.push(FederatedTrustRotationEvent::AuthorizeActivation);
    }
    for (index, event) in events.into_iter().enumerate() {
        let current = store
            .get_federated_trust_rotation(rotation_id)
            .await
            .unwrap()
            .unwrap();
        let result = node
            .propose(Command::AdvanceFederatedTrustRotation {
                rotation_id: current.rotation_id,
                expected_revision: current.revision,
                event,
                at_ms: now + index as i64,
                request_id: format!("{rotation_id}-event-{cluster_id}-{index}"),
                actor: "coordinator-process-e2e".into(),
            })
            .await
            .unwrap();
        assert!(result.success, "{}", result.message);
    }
    let final_rotation = store
        .get_federated_trust_rotation(rotation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        final_rotation.phase,
        if authorize_activation {
            FederatedTrustRotationPhase::Activating
        } else {
            FederatedTrustRotationPhase::ActivationBarrier
        }
    );
    assert_eq!(
        final_rotation.activation_barrier_committed,
        authorize_activation
    );
}

async fn run_six_member_child() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cluster = std::env::var("DATASPOOL_TRUST_HA_CLUSTER").unwrap();
    let member: usize = std::env::var("DATASPOOL_TRUST_HA_MEMBER")
        .unwrap()
        .parse()
        .unwrap();
    let root = PathBuf::from(std::env::var("DATASPOOL_TRUST_HA_ROOT").unwrap());
    let addresses = std::env::var("DATASPOOL_TRUST_HA_ADDRESSES")
        .unwrap()
        .split(';')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(addresses.len(), 3);
    let ready_dir = PathBuf::from(std::env::var("DATASPOOL_TRUST_HA_READY_DIR").unwrap());
    let control = PathBuf::from(std::env::var("DATASPOOL_TRUST_HA_CONTROL").unwrap());
    let markers = PathBuf::from(std::env::var("DATASPOOL_TRUST_HA_MARKERS").unwrap());
    let base_id = if cluster == "source" { 91_000 } else { 92_000 };
    let node_id = base_id + member as u64;
    let peer_id = if std::env::var_os("DATASPOOL_TRUST_HA_COORDINATOR_MODE").is_some() {
        if cluster == "source" {
            "target"
        } else {
            "source"
        }
    } else if cluster == "source" {
        "trust-target-ha"
    } else {
        "trust-source-ha"
    };
    let db = RocksDbStore::open(root.join("app")).expect("open member application RocksDB");
    let store = TaskStore::new(db.clone());
    let node = RaftNode::new(
        node_id,
        root.join("raft"),
        store.clone(),
        raft_config(Duration::from_millis(140), Duration::from_millis(650)),
        RAFT_TOKEN.into(),
    )
    .await
    .expect("open persistent multi-member Raft node");
    let listener = tokio::net::TcpListener::bind(&addresses[member])
        .await
        .expect("bind six-member Raft RPC listener");
    let router = dataspool::raft_router(node.handle(), RAFT_TOKEN.into());
    let _raft_server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("serve six-member Raft RPC");
    });
    std::fs::write(
        ready_dir.join(format!("{cluster}-{member}.ready")),
        b"ready",
    )
    .unwrap();
    if !node.is_initialized().await.unwrap() {
        for index in 0..3 {
            let ready = ready_dir.join(format!("{cluster}-{index}.ready"));
            tokio::time::timeout(Duration::from_secs(20), async {
                while !ready.exists() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("all three members must bind before initial bootstrap");
        }
    }
    let members = (0..3)
        .map(|index| {
            (
                base_id + index as u64,
                BasicNode::new(addresses[index].clone()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if member == 0 {
        node.initialize(members)
            .await
            .expect("initialize three-member Raft membership");
    }
    node.wait_for_leader(Duration::from_secs(25))
        .await
        .expect("elect multi-member Raft leader");
    if member == 0
        && let Ok(grpc_addr) = std::env::var("DATASPOOL_TRUST_HA_GRPC_ADDR")
    {
        let cert_dir = PathBuf::from(std::env::var("DATASPOOL_TRUST_HA_CERT_DIR").unwrap());
        let server_cert = cert_dir.join("server.pem");
        let server_key = cert_dir.join("server.key.pem");
        let client_ca = cert_dir.join("ca.pem");
        let mut grpc = GrpcServer::new(
            &grpc_addr,
            std::sync::Arc::new(store.clone()),
            Some(std::sync::Arc::new(node.clone())),
        )
        .await
        .unwrap()
        .with_mtls_files(&server_cert, &server_key, &client_ca)
        .await
        .unwrap();
        let ack_marker = PathBuf::from(std::env::var("DATASPOOL_TRUST_HA_ACK_MARKER").unwrap());
        let drop_after = std::env::var("DATASPOOL_TRUST_HA_ACK_DROP_AFTER")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .or_else(|| (cluster == "target").then_some(1));
        if let Some(drop_after) = drop_after
            && !ack_marker.exists()
        {
            grpc = grpc
                .with_rotation_event_ack_drop_marker(ack_marker)
                .with_rotation_event_ack_gate(PathBuf::from(
                    std::env::var("DATASPOOL_TRUST_HA_ACK_GATE").unwrap(),
                ))
                .with_rotation_event_ack_drop_after(drop_after);
        }
        tokio::spawn(async move {
            grpc.serve()
                .await
                .expect("serve Raft-backed federation gRPC endpoint");
        });
    }
    let process_id = std::process::id();
    std::fs::write(
        ready_dir.join(format!("{cluster}-{member}-{process_id}.ready")),
        b"ready",
    )
    .unwrap();

    let mut last_command = String::new();
    let mut previous_leader = None;
    loop {
        if let Some(leader) = node.current_leader().await
            && leader != previous_leader.unwrap_or(0)
        {
            previous_leader = Some(leader);
            let leader_index = leader.saturating_sub(base_id) as usize;
            write_atomic_marker(
                &markers.join(format!("{cluster}.leader")),
                &leader_index.to_string(),
            );
            write_atomic_marker(
                &markers.join(format!("{cluster}.member-{member}.leader")),
                &leader_index.to_string(),
            );
        }
        if !markers
            .join(format!("{cluster}.member-{member}-{process_id}.candidate"))
            .exists()
            && let Some(state) = store.get_federated_cluster(peer_id).await.unwrap()
            && state.pending_trust_bundle_fingerprint.as_deref() == Some(NEW_CA_FINGERPRINT)
        {
            std::fs::write(
                markers.join(format!("{cluster}.member-{member}-{process_id}.candidate")),
                b"candidate-recovered",
            )
            .unwrap();
        }
        let command = std::fs::read_to_string(&control).unwrap_or_default();
        if command != last_command
            && let Some(expected) = command.strip_prefix("verify-partial-recovery:")
        {
            let expected_expiry: i64 = expected.parse().unwrap();
            tokio::time::timeout(Duration::from_secs(25), async {
                loop {
                    let peer = store.get_federated_cluster(peer_id).await.unwrap();
                    let rotation = store.get_federated_trust_rotation("coordinator-process-rotation").await.unwrap();
                    let recovered = match (cluster.as_str(), peer, rotation) {
                        ("source", Some(peer), Some(rotation)) => {
                            rotation.phase == FederatedTrustRotationPhase::Activating
                                && rotation.local_activation_expires_at_ms == Some(expected_expiry)
                                && rotation.peer_activation_expires_at_ms.is_none()
                                && peer.trust_bundle_fingerprint.as_deref() == Some(NEW_CA_FINGERPRINT)
                                && peer.previous_trust_bundle_fingerprint.as_deref() == Some(OLD_FINGERPRINT_CA)
                                && peer.pending_trust_bundle_pem.is_none()
                                && peer.previous_trust_bundle_expires_at_ms == Some(expected_expiry)
                        }
                        ("target", Some(peer), Some(rotation)) => {
                            rotation.phase == FederatedTrustRotationPhase::Overlap
                                && rotation.local_activation_expires_at_ms == Some(expected_expiry)
                                && rotation.peer_activation_expires_at_ms == Some(expected_expiry)
                                && peer.trust_bundle_fingerprint.as_deref() == Some(NEW_CA_FINGERPRINT)
                                && peer.previous_trust_bundle_fingerprint.as_deref() == Some(OLD_FINGERPRINT_CA)
                                && peer.pending_trust_bundle_pem.is_none()
                                && peer.previous_trust_bundle_expires_at_ms == Some(expected_expiry)
                        }
                        _ => false,
                    };
                    if recovered {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
            })
            .await
            .expect("each restarted Raft member must recover its own durable partial-activation state before coordinator replay");
            std::fs::write(
                markers.join(format!("{cluster}.partial-recovered-{member}")),
                b"partial-state-recovered",
            )
            .unwrap();
            last_command = command.clone();
        }
        if command != last_command
            && let Some(expected) = command.strip_prefix("verify-coordinator:")
        {
            let expected_expiry: i64 = expected.parse().unwrap();
            tokio::time::timeout(Duration::from_secs(25), async {
                loop {
                    let peer = store.get_federated_cluster(peer_id).await.unwrap();
                    let rotation = store.get_federated_trust_rotation("coordinator-process-rotation").await.unwrap();
                    if let (Some(peer), Some(rotation)) = (peer, rotation)
                        && peer.trust_bundle_fingerprint.as_deref() == Some(NEW_CA_FINGERPRINT)
                        && peer.previous_trust_bundle_expires_at_ms == Some(expected_expiry)
                        && peer.pending_trust_bundle_pem.is_none()
                        && rotation.phase == FederatedTrustRotationPhase::Overlap
                        && rotation.local_activation_expires_at_ms == Some(expected_expiry)
                        && rotation.peer_activation_expires_at_ms == Some(expected_expiry)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
            })
            .await
            .expect("both Raft state machine and coordinator record must converge to the same Overlap expiry");
            std::fs::write(
                markers.join(format!("{cluster}.coordinator-verified-{member}")),
                b"verified",
            )
            .unwrap();
            last_command = command.clone();
        }
        if command != last_command
            && let Some(expected) = command.strip_prefix("verify:")
        {
            let expected_expiry: i64 = expected.parse().unwrap();
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    if let Some(state) = store.get_federated_cluster(peer_id).await.unwrap()
                        && state.trust_bundle_fingerprint.as_deref() == Some(NEW_CA_FINGERPRINT)
                        && state.previous_trust_bundle_expires_at_ms == Some(expected_expiry)
                        && state.pending_trust_bundle_pem.is_none()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
            })
            .await
            .expect("local state machine must converge after leader restart");
            std::fs::write(
                markers.join(format!("{cluster}.verified-{member}")),
                b"verified",
            )
            .unwrap();
            last_command = command.clone();
        }
        if command != last_command
            && let Some(leader_index) = command.strip_prefix("uncommitted-probe:")
        {
            let leader_index: usize = leader_index.parse().unwrap();
            assert!(leader_index < 3, "invalid isolated leader index");
            let leader_id = base_id + leader_index as u64;
            if node_id == leader_id {
                for peer_index in 0..3 {
                    let peer_id = base_id + peer_index as u64;
                    if peer_id != node_id {
                        node.set_peer_blocked(peer_id, true).await;
                    }
                }
                let now = chrono::Utc::now().timestamp_millis();
                let probe_id = format!("uncommitted-probe-{cluster}");
                let probe = Command::UpsertFederatedCluster {
                    cluster: FederatedCluster {
                        cluster_id: probe_id,
                        endpoint: "https://127.0.0.1:1".into(),
                        identity_fingerprint: OLD_FINGERPRINT.into(),
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
                        version: env!("CARGO_PKG_VERSION").into(),
                        capabilities: BTreeSet::new(),
                        status: FederatedClusterStatus::Healthy,
                        last_seen_at_ms: now,
                        updated_at_ms: now,
                    },
                    request_id: format!("uncommitted-probe-request-{cluster}"),
                    actor: "raft-uncommitted-recovery-e2e".into(),
                };
                let before = node.handle().metrics().borrow().last_log_index.unwrap_or(0);
                let _ =
                    tokio::time::timeout(Duration::from_secs(3), node.handle().client_write(probe))
                        .await;
                tokio::time::timeout(Duration::from_secs(8), async {
                    loop {
                        let metrics = node.handle().metrics();
                        let metrics = metrics.borrow();
                        let last_log = metrics.last_log_index.unwrap_or(0);
                        let last_applied = metrics.last_applied.map(|log| log.index).unwrap_or(0);
                        if last_log > before && last_applied <= before {
                            break;
                        }
                        drop(metrics);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .expect(
                    "isolated leader must persist an uncommitted log entry without applying it",
                );
                std::fs::write(
                    markers.join(format!("{cluster}.uncommitted-appended-{member}")),
                    b"appended-not-committed",
                )
                .unwrap();
            } else {
                node.set_peer_blocked(leader_id, true).await;
                std::fs::write(
                    markers.join(format!("{cluster}.isolated-follower-{member}")),
                    b"follower-isolated-from-old-leader",
                )
                .unwrap();
            }
            last_command = command.clone();
        }
        if command != last_command
            && let Some(expected) = command.strip_prefix("verify-no-uncommitted:")
        {
            let (expiry_value, generation) =
                expected.split_once(':').unwrap_or((expected, "default"));
            let expected_expiry: i64 = expiry_value.parse().unwrap();
            tokio::time::timeout(Duration::from_secs(25), async {
                loop {
                    let probe = store
                        .get_federated_cluster(&format!("uncommitted-probe-{cluster}"))
                        .await
                        .unwrap();
                    assert!(probe.is_none(), "uncommitted Raft command must never be applied");
                    let peer = store.get_federated_cluster(peer_id).await.unwrap();
                    let rotation = store
                        .get_federated_trust_rotation("coordinator-process-rotation")
                        .await
                        .unwrap();
                    let metrics = node.handle().metrics();
                    let metrics = metrics.borrow();
                    let log_is_fully_applied = metrics.last_log_index
                        == metrics.last_applied.map(|log| log.index);
                    drop(metrics);
                    if let (Some(peer), Some(rotation)) = (peer, rotation)
                        && peer.trust_bundle_fingerprint.as_deref() == Some(NEW_CA_FINGERPRINT)
                        && peer.previous_trust_bundle_fingerprint.as_deref() == Some(OLD_FINGERPRINT_CA)
                        && peer.previous_trust_bundle_expires_at_ms == Some(expected_expiry)
                        && peer.pending_trust_bundle_pem.is_none()
                        && rotation.phase == FederatedTrustRotationPhase::Overlap
                        && rotation.local_activation_expires_at_ms == Some(expected_expiry)
                        && rotation.peer_activation_expires_at_ms == Some(expected_expiry)
                        && log_is_fully_applied
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
            })
            .await
            .expect("restarted members must discard uncommitted probe and preserve committed rotation state");
            std::fs::write(
                markers.join(format!("{cluster}.no-uncommitted-{generation}-{member}")),
                b"uncommitted-entry-not-applied",
            )
            .unwrap();
            last_command = command.clone();
        }
        if command != last_command && node.is_leader().await {
            if command.strip_prefix("seed:").is_some() {
                let peer_endpoint = std::env::var("DATASPOOL_TRUST_HA_PEER_GRPC_ADDR").unwrap();
                seed_coordinator_rotation(
                    &node,
                    &store,
                    &cluster,
                    peer_id,
                    &peer_endpoint,
                    "coordinator-process-rotation",
                    true,
                )
                .await;
                std::fs::write(
                    markers.join(format!("{cluster}.coordinator-seeded")),
                    b"seeded",
                )
                .unwrap();
                last_command = command;
            } else if command == "seed-rollback" {
                let peer_endpoint = std::env::var("DATASPOOL_TRUST_HA_PEER_GRPC_ADDR").unwrap();
                seed_coordinator_rotation(
                    &node,
                    &store,
                    &cluster,
                    peer_id,
                    &peer_endpoint,
                    "rollback-process-rotation",
                    false,
                )
                .await;
                std::fs::write(
                    markers.join(format!("{cluster}.rollback-seeded")),
                    b"seeded-before-activation-barrier",
                )
                .unwrap();
                last_command = command;
            } else if command == "stage" {
                let now = chrono::Utc::now().timestamp_millis();
                let old_ca = String::from_utf8(ROTATION_CA.to_vec()).unwrap();
                let registered = node
                    .propose(Command::UpsertFederatedCluster {
                        cluster: FederatedCluster {
                            cluster_id: peer_id.into(),
                            endpoint: "https://127.0.0.1:1".into(),
                            identity_fingerprint: OLD_FINGERPRINT.into(),
                            previous_identity_fingerprint: None,
                            previous_identity_expires_at_ms: None,
                            pending_identity_fingerprint: None,
                            rotation_started_at_ms: None,
                            pending_rotation_grace_period_ms: None,
                            trust_bundle_pem: Some(old_ca.clone()),
                            trust_bundle_fingerprint: Some(
                                dataspool::federation::validate_trust_bundle(&old_ca).unwrap(),
                            ),
                            pending_trust_bundle_pem: None,
                            pending_trust_bundle_fingerprint: None,
                            trust_bundle_staged_at_ms: None,
                            previous_trust_bundle_pem: None,
                            previous_trust_bundle_fingerprint: None,
                            previous_trust_bundle_expires_at_ms: None,
                            version: env!("CARGO_PKG_VERSION").into(),
                            capabilities: BTreeSet::new(),
                            status: FederatedClusterStatus::Healthy,
                            last_seen_at_ms: now,
                            updated_at_ms: now,
                        },
                        request_id: format!("six-member-register-{cluster}"),
                        actor: "six-process-crash-e2e".into(),
                    })
                    .await
                    .unwrap();
                assert!(registered.success, "{}", registered.message);
                let staged = node
                    .propose(Command::StageFederatedTrustBundle {
                        cluster_id: peer_id.into(),
                        current_fingerprint: OLD_FINGERPRINT_CA.into(),
                        bundle_pem: String::from_utf8(ROTATION_NEW_CA.to_vec()).unwrap(),
                        bundle_fingerprint: NEW_CA_FINGERPRINT.into(),
                        at_ms: now,
                        request_id: format!("six-member-stage-{cluster}"),
                        actor: "six-process-crash-e2e".into(),
                    })
                    .await
                    .unwrap();
                assert!(staged.success, "{}", staged.message);
                // This is the durable commit marker. The parent SIGKILLs the
                // actual leader immediately after observing it.
                let committed = markers.join(format!("{cluster}.stage-committed"));
                if !committed.exists() {
                    std::fs::write(committed, member.to_string()).unwrap();
                }
                last_command = command;
            } else if let Some(timestamp) = command.strip_prefix("activate:") {
                let activated_at: i64 = timestamp.parse().unwrap();
                let activated = node
                    .propose(Command::ActivateFederatedTrustBundle {
                        cluster_id: peer_id.into(),
                        bundle_fingerprint: NEW_CA_FINGERPRINT.into(),
                        at_ms: activated_at,
                        overlap_ms: 60_000,
                        request_id: format!("six-member-activate-{cluster}"),
                        actor: "six-process-crash-e2e".into(),
                    })
                    .await
                    .unwrap();
                assert!(activated.success, "{}", activated.message);
                let state = store.get_federated_cluster(peer_id).await.unwrap().unwrap();
                assert_eq!(
                    state.previous_trust_bundle_expires_at_ms,
                    Some(activated_at + 60_000)
                );
                // Deliberately do not publish an acknowledgement marker: the
                // parent kills this process at the commit-before-ACK boundary.
                let committed = markers.join(format!("{cluster}.activate-committed"));
                if !committed.exists() {
                    std::fs::write(committed, member.to_string()).unwrap();
                }
                last_command = command;
            } else if let Some(timestamp) = command.strip_prefix("replay:") {
                let expiry: i64 = timestamp.parse().unwrap();
                let replay = node
                    .propose(Command::ActivateFederatedTrustBundle {
                        cluster_id: peer_id.into(),
                        bundle_fingerprint: NEW_CA_FINGERPRINT.into(),
                        at_ms: expiry + 120_000,
                        overlap_ms: 60_000,
                        request_id: format!("six-member-activate-{cluster}"),
                        actor: "six-process-lost-ack-replay".into(),
                    })
                    .await
                    .unwrap();
                assert!(replay.success, "{}", replay.message);
                let state = store.get_federated_cluster(peer_id).await.unwrap().unwrap();
                assert_eq!(state.previous_trust_bundle_expires_at_ms, Some(expiry));
                std::fs::write(
                    markers.join(format!("{cluster}.replayed")),
                    member.to_string(),
                )
                .unwrap();
                last_command = command;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn federated_trust_bundle_rotation_survives_sigkill_of_both_three_member_raft_leaders() {
    if std::env::var_os("DATASPOOL_TRUST_HA_CHILD").is_some() {
        run_six_member_child().await;
        return;
    }
    let root = TempDir::new().unwrap();
    let ready = root.path().join("ready");
    let markers = root.path().join("markers");
    std::fs::create_dir_all(&ready).unwrap();
    std::fs::create_dir_all(&markers).unwrap();
    let mut groups = Vec::new();
    for cluster in ["source", "target"] {
        let cluster_root = root.path().join(cluster);
        std::fs::create_dir_all(&cluster_root).unwrap();
        let addresses = (0..3).map(|_| reserve_local_addr()).collect::<Vec<_>>();
        let address_list = addresses.join(";");
        let control = root.path().join(format!("{cluster}.control"));
        let mut children = Vec::new();
        for member in 0..3 {
            let member_root = cluster_root.join(format!("member-{member}"));
            std::fs::create_dir_all(&member_root).unwrap();
            children.push(six_member_child_process(
                cluster,
                member,
                &member_root,
                &address_list,
                &ready,
                &control,
                &markers,
            ));
        }
        groups.push((cluster.to_string(), addresses, control, children));
    }
    for (cluster, _, _, children) in &mut groups {
        for member in 0..3 {
            wait_for_file(&ready.join(format!("{cluster}-{member}.ready")), children).await;
        }
        wait_for_file(&markers.join(format!("{cluster}.leader")), children).await;
    }

    for (cluster, _, control, children) in &mut groups {
        write_control(control, "stage");
        wait_for_file(
            &markers.join(format!("{cluster}.stage-committed")),
            children,
        )
        .await;
    }
    let mut first_leaders = Vec::new();
    for (cluster, _, _, children) in &mut groups {
        let member: usize =
            std::fs::read_to_string(markers.join(format!("{cluster}.stage-committed")))
                .unwrap()
                .parse()
                .unwrap();
        assert_sigkill(
            &mut children[member],
            "SIGKILL leader after CA Stage commit",
        );
        first_leaders.push((cluster.clone(), member));
    }
    for (cluster, _, _, children) in &mut groups {
        let leader_file = markers.join(format!("{cluster}.leader"));
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(index) = read_marker_index(&leader_file)
                    && index < children.len()
                    && children[index].try_wait().unwrap().is_none()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        })
        .await
        .expect("quorum must elect a replacement leader after Stage SIGKILL");
    }

    for (cluster, member) in &first_leaders {
        let group = groups.iter_mut().find(|group| &group.0 == cluster).unwrap();
        let member_root = root.path().join(cluster).join(format!("member-{member}"));
        let address_list = group.1.join(";");
        group.3[*member] = six_member_child_process(
            cluster,
            *member,
            &member_root,
            &address_list,
            &ready,
            &group.2,
            &markers,
        );
        let pid = group.3[*member].id();
        wait_for_file(
            &ready.join(format!("{cluster}-{member}-{pid}.ready")),
            &mut group.3,
        )
        .await;
        wait_for_file(
            &markers.join(format!("{cluster}.member-{member}-{pid}.candidate")),
            &mut group.3,
        )
        .await;
    }

    // Verify both surviving quorum members have applied the staged candidate,
    // not merely that a replacement leader was elected.
    for (cluster, killed_member) in &first_leaders {
        let group = groups.iter_mut().find(|group| &group.0 == cluster).unwrap();
        for member in 0..3 {
            if member == *killed_member {
                continue;
            }
            let pid = group.3[member].id();
            wait_for_file(
                &markers.join(format!("{cluster}.member-{member}-{pid}.candidate")),
                &mut group.3,
            )
            .await;
        }
    }

    let activate_at = chrono::Utc::now().timestamp_millis();
    let expiry = activate_at + 60_000;
    for (cluster, _, control, children) in &mut groups {
        write_control(control, &format!("activate:{activate_at}"));
        wait_for_file(
            &markers.join(format!("{cluster}.activate-committed")),
            children,
        )
        .await;
        let member: usize =
            std::fs::read_to_string(markers.join(format!("{cluster}.activate-committed")))
                .unwrap()
                .parse()
                .unwrap();
        assert_sigkill(
            &mut children[member],
            "SIGKILL leader after CA Activate commit before ACK",
        );
    }
    for (cluster, _, control, children) in &mut groups {
        let leader_file = markers.join(format!("{cluster}.leader"));
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(index) = read_marker_index(&leader_file)
                    && index < children.len()
                    && children[index].try_wait().unwrap().is_none()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        })
        .await
        .expect("quorum must elect a replacement leader after Activate SIGKILL");
        write_control(control, &format!("replay:{expiry}"));
        wait_for_file(&markers.join(format!("{cluster}.replayed")), children).await;
    }

    // Restart the Activate-killed leaders. Stage-killed leaders were already
    // restarted and proved to have recovered their pending CA before Activate.
    for (cluster, addresses, control, children) in &mut groups {
        let activate_killed: usize =
            std::fs::read_to_string(markers.join(format!("{cluster}.activate-committed")))
                .unwrap()
                .parse()
                .unwrap();
        let member_root = root
            .path()
            .join(&*cluster)
            .join(format!("member-{activate_killed}"));
        let address_list = addresses.join(";");
        children[activate_killed] = six_member_child_process(
            &*cluster,
            activate_killed,
            &member_root,
            &address_list,
            &ready,
            control,
            &markers,
        );
        let pid = children[activate_killed].id();
        wait_for_file(
            &ready.join(format!("{cluster}-{activate_killed}-{pid}.ready")),
            children,
        )
        .await;
        write_control(control, &format!("verify:{expiry}"));
        for member in 0..3 {
            wait_for_file(
                &markers.join(format!("{cluster}.verified-{member}")),
                children,
            )
            .await;
        }
    }
    for (_, _, _, children) in &mut groups {
        for child in children {
            if child.try_wait().unwrap().is_none() {
                assert_sigkill(child, "cleanup six-member Raft process");
            }
        }
    }
}

async fn run_coordinator_process() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let phase = std::env::var("DATASPOOL_COORDINATOR_PROCESS_PHASE").unwrap();
    let source_endpoint = std::env::var("DATASPOOL_COORDINATOR_SOURCE_ENDPOINT").unwrap();
    let target_endpoint = std::env::var("DATASPOOL_COORDINATOR_TARGET_ENDPOINT").unwrap();
    let cert_dir = PathBuf::from(std::env::var("DATASPOOL_COORDINATOR_CERT_DIR").unwrap());
    let expiry_file = PathBuf::from(std::env::var("DATASPOOL_COORDINATOR_EXPIRY_FILE").unwrap());
    let marker = PathBuf::from(std::env::var("DATASPOOL_COORDINATOR_MARKER").unwrap());
    let ca = cert_dir.join("ca.pem");
    let client_cert = cert_dir.join("client.pem");
    let client_key = cert_dir.join("client.key.pem");
    let mut coordinator = dataspool::FederatedTrustRotationProcessCoordinator::connect(
        "source",
        "target",
        "coordinator-process-rotation",
        "coordinator-process",
        &source_endpoint,
        &target_endpoint,
        &ca,
        &ca,
        &client_cert,
        &client_key,
        &client_cert,
        &client_key,
    )
    .await
    .unwrap();
    if phase == "initial" {
        let expiry = chrono::Utc::now().timestamp_millis() + 60_000;
        std::fs::write(&expiry_file, expiry.to_string()).unwrap();
        let result = coordinator.activate_both_at(expiry).await;
        panic!("target Activate ACK should remain gated after Raft commit: {result:?}");
    }
    let expiry: i64 = std::fs::read_to_string(&expiry_file)
        .unwrap()
        .parse()
        .unwrap();
    let (source, target) = coordinator.activate_both_at(expiry).await.unwrap();
    assert_eq!(source.rotation.phase, FederatedTrustRotationPhase::Overlap);
    assert_eq!(target.rotation.phase, FederatedTrustRotationPhase::Overlap);
    assert_eq!(source.rotation.local_activation_expires_at_ms, Some(expiry));
    assert_eq!(source.rotation.peer_activation_expires_at_ms, Some(expiry));
    assert_eq!(target.rotation.local_activation_expires_at_ms, Some(expiry));
    assert_eq!(target.rotation.peer_activation_expires_at_ms, Some(expiry));
    std::fs::write(&marker, b"coordinator-recovered-overlap").unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn federated_trust_rotation_coordinator_process_recovers_after_remote_activate_ack_loss() {
    if std::env::var_os("DATASPOOL_TRUST_HA_CHILD").is_some() {
        run_six_member_child().await;
        return;
    }
    if std::env::var_os("DATASPOOL_COORDINATOR_PROCESS_PHASE").is_some() {
        run_coordinator_process().await;
        return;
    }

    let root = TempDir::new().unwrap();
    let ready = root.path().join("ready");
    let markers = root.path().join("markers");
    let cert_dir = root.path().join("certs");
    std::fs::create_dir_all(&ready).unwrap();
    std::fs::create_dir_all(&markers).unwrap();
    std::fs::create_dir_all(&cert_dir).unwrap();
    for (name, bytes) in [
        ("ca.pem", ROTATION_CA),
        ("server.pem", ROTATION_SERVER_CERT),
        ("server.key.pem", ROTATION_SERVER_KEY),
        ("client.pem", ROTATION_OLD_CERT),
        ("client.key.pem", ROTATION_OLD_KEY),
    ] {
        std::fs::write(cert_dir.join(name), bytes).unwrap();
    }
    let source_grpc = reserve_local_addr();
    let target_grpc = reserve_local_addr();
    let ack_marker = root.path().join("target-activate-committed.marker");
    let ack_gate = root.path().join("target-activate-ack.release");
    let expiry_file = root.path().join("activation-expiry.txt");
    let initial_marker = root.path().join("coordinator-initial.marker");
    let recovered_marker = root.path().join("coordinator-recovered.marker");
    let mut groups = Vec::new();
    for cluster in ["source", "target"] {
        let cluster_root = root.path().join(cluster);
        std::fs::create_dir_all(&cluster_root).unwrap();
        let addresses = (0..3).map(|_| reserve_local_addr()).collect::<Vec<_>>();
        let address_list = addresses.join(";");
        let control = root.path().join(format!("{cluster}.control"));
        let mut children = Vec::new();
        for member in 0..3 {
            let member_root = cluster_root.join(format!("member-{member}"));
            std::fs::create_dir_all(&member_root).unwrap();
            let (grpc, peer_grpc) = if cluster == "source" {
                (&source_grpc, &target_grpc)
            } else {
                (&target_grpc, &source_grpc)
            };
            children.push(coordinator_raft_child_process(
                cluster,
                member,
                &member_root,
                &address_list,
                &ready,
                &control,
                &markers,
                grpc,
                peer_grpc,
                &cert_dir,
                &ack_marker,
                &ack_gate,
            ));
        }
        groups.push((cluster.to_string(), addresses, control, children));
    }
    for (cluster, _, _, children) in &mut groups {
        for member in 0..3 {
            wait_for_file(&ready.join(format!("{cluster}-{member}.ready")), children).await;
        }
        wait_for_file(&markers.join(format!("{cluster}.leader")), children).await;
    }
    for (cluster, _, control, children) in &mut groups {
        write_control(control, "seed:coordinator-process");
        wait_for_file(
            &markers.join(format!("{cluster}.coordinator-seeded")),
            children,
        )
        .await;
    }

    let mut coordinator = coordinator_process_child(
        "initial",
        &format!("https://{source_grpc}"),
        &format!("https://{target_grpc}"),
        &cert_dir,
        &expiry_file,
        &initial_marker,
    );
    wait_for_file(&ack_marker, std::slice::from_mut(&mut coordinator)).await;
    let expiry: i64 = std::fs::read_to_string(&expiry_file)
        .unwrap()
        .parse()
        .unwrap();
    assert_sigkill(
        &mut coordinator,
        "SIGKILL independent coordinator while remote Activate ACK is gated",
    );
    // Release the remote server's blocked response only after the coordinator
    // has died. The Raft processes and their RocksDB directories remain intact.
    std::fs::write(&ack_gate, b"release").unwrap();

    // Inspect both durable Raft snapshots before restarting the coordinator:
    // Source has committed its local Activate but has not recorded peer ACK;
    // Target has committed its local Activate and the corresponding peer event.
    let read_snapshot = |endpoint: String, peer_id: &'static str| {
        let ca = cert_dir.join("ca.pem");
        let cert = cert_dir.join("client.pem");
        let key = cert_dir.join("client.key.pem");
        async move {
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    if let Ok(mut client) =
                        GrpcClient::connect_mtls(&endpoint, &ca, &cert, &key).await
                        && let Ok(response) = client
                            .get_remote_trust_rotation(peer_id, "coordinator-process-rotation")
                            .await
                        && response.accepted
                        && let Ok(snapshot) = serde_json::from_str::<
                            dataspool::federation::FederatedTrustRotationSnapshot,
                        >(&response.state_json)
                    {
                        return snapshot;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("committed coordinator crash-boundary state must be readable")
        }
    };
    let source_before_restart = read_snapshot(format!("https://{source_grpc}"), "target").await;
    let target_before_restart = read_snapshot(format!("https://{target_grpc}"), "source").await;
    assert_eq!(
        source_before_restart.rotation.phase,
        FederatedTrustRotationPhase::Activating
    );
    assert_eq!(
        source_before_restart
            .rotation
            .local_activation_expires_at_ms,
        Some(expiry)
    );
    assert_eq!(
        source_before_restart.rotation.peer_activation_expires_at_ms,
        None
    );
    assert_eq!(
        source_before_restart.peer_previous_ca_expires_at_ms,
        Some(expiry)
    );
    assert_eq!(
        target_before_restart.rotation.phase,
        FederatedTrustRotationPhase::Overlap
    );
    assert_eq!(
        target_before_restart
            .rotation
            .local_activation_expires_at_ms,
        Some(expiry)
    );
    assert_eq!(
        target_before_restart.rotation.peer_activation_expires_at_ms,
        Some(expiry)
    );
    assert_eq!(
        target_before_restart.peer_previous_ca_expires_at_ms,
        Some(expiry)
    );

    // Stop every Raft member in both clusters as well as the coordinator.
    // No graceful shutdown is allowed here: this is a process-level power-loss
    // simulation and all six member-local RocksDB/Raft directories must survive.
    for (cluster, _, control, children) in &mut groups {
        write_control(control, "hold");
        std::fs::remove_file(markers.join(format!("{cluster}.leader"))).unwrap();
        for member in 0..3 {
            std::fs::remove_file(ready.join(format!("{cluster}-{member}.ready"))).unwrap();
        }
        for child in children {
            assert_sigkill(
                child,
                "SIGKILL all Raft members for full-cluster restart recovery",
            );
        }
    }

    // Restart all six members in a deliberately interleaved order. Each process
    // opens the exact same persistent directory and listener address; no Raft
    // state or application database is recreated.
    for cluster in ["target", "source"] {
        let group = groups.iter_mut().find(|group| group.0 == cluster).unwrap();
        for member in [2usize, 0, 1] {
            let member_root = root.path().join(cluster).join(format!("member-{member}"));
            let address_list = group.1.join(";");
            group.3[member] = coordinator_raft_child_process(
                cluster,
                member,
                &member_root,
                &address_list,
                &ready,
                &group.2,
                &markers,
                if cluster == "source" {
                    &source_grpc
                } else {
                    &target_grpc
                },
                if cluster == "source" {
                    &target_grpc
                } else {
                    &source_grpc
                },
                &cert_dir,
                &ack_marker,
                &ack_gate,
            );
        }
    }
    for (cluster, _, _, children) in &mut groups {
        for member in 0..3 {
            let pid = children[member].id();
            wait_for_file(
                &ready.join(format!("{cluster}-{member}-{pid}.ready")),
                children,
            )
            .await;
        }
        wait_for_file(&markers.join(format!("{cluster}.leader")), children).await;
    }

    // Check the asymmetric durable commit boundary on every restarted member
    // before allowing the coordinator to issue any recovery command.
    for (cluster, _, control, children) in &mut groups {
        write_control(control, &format!("verify-partial-recovery:{expiry}"));
        for member in 0..3 {
            wait_for_file(
                &markers.join(format!("{cluster}.partial-recovered-{member}")),
                children,
            )
            .await;
        }
    }

    let mut recovered = coordinator_process_child(
        "recover",
        &format!("https://{source_grpc}"),
        &format!("https://{target_grpc}"),
        &cert_dir,
        &expiry_file,
        &recovered_marker,
    );
    wait_for_file(&recovered_marker, std::slice::from_mut(&mut recovered)).await;
    let recovered_status = recovered
        .wait()
        .expect("wait for recovered coordinator process");
    assert!(
        recovered_status.success(),
        "recovered coordinator failed: {recovered_status}"
    );

    for (cluster, _, control, children) in &mut groups {
        write_control(control, &format!("verify-coordinator:{expiry}"));
        for member in 0..3 {
            wait_for_file(
                &markers.join(format!("{cluster}.coordinator-verified-{member}")),
                children,
            )
            .await;
        }
    }
    // Exercise a genuinely uncommitted Raft entry on each cluster. Isolate the
    // current leader from both followers, append a probe that cannot reach a
    // quorum, then stop that leader before stopping the follower quorum.
    for (cluster, addresses, control, children) in &mut groups {
        let leader_file = markers.join(format!("{cluster}.leader"));
        let isolated_leader = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(leader) = read_marker_index(&leader_file)
                    && leader < 3
                {
                    return leader;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("current leader marker must contain a valid member index");
        for member in 0..3 {
            std::fs::remove_file(markers.join(format!("{cluster}.isolated-follower-{member}")))
                .ok();
            std::fs::remove_file(markers.join(format!("{cluster}.uncommitted-appended-{member}")))
                .ok();
            std::fs::remove_file(markers.join(format!("{cluster}.member-{member}.leader"))).ok();
        }
        write_control(control, &format!("uncommitted-probe:{isolated_leader}"));
        wait_for_file(
            &markers.join(format!("{cluster}.uncommitted-appended-{isolated_leader}")),
            children,
        )
        .await;
        for member in 0..3 {
            if member != isolated_leader {
                wait_for_file(
                    &markers.join(format!("{cluster}.isolated-follower-{member}")),
                    children,
                )
                .await;
            }
        }
        for member in 0..3 {
            if member != isolated_leader {
                let member_leader = markers.join(format!("{cluster}.member-{member}.leader"));
                tokio::time::timeout(Duration::from_secs(15), async {
                    loop {
                        if let Ok(value) = std::fs::read_to_string(&member_leader)
                            && value
                                .trim()
                                .parse::<usize>()
                                .is_ok_and(|leader| leader != isolated_leader)
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(40)).await;
                    }
                })
                .await
                .expect("both followers must observe the replacement leader");
            }
        }
        // Prevent restarted processes from replaying the fault-injection command.
        write_control(control, "hold");

        // Kill the isolated leader first so its uncommitted entry exists only
        // in its own durable log when the two-member quorum is restarted.
        assert_sigkill(
            &mut children[isolated_leader],
            "SIGKILL isolated leader with an uncommitted probe entry",
        );
        for (member, child) in children.iter_mut().enumerate() {
            if member != isolated_leader {
                assert_sigkill(child, "SIGKILL follower quorum for restart");
            }
        }
        std::fs::remove_file(&leader_file).ok();
        for member in 0..3 {
            std::fs::remove_file(ready.join(format!("{cluster}-{member}.ready"))).ok();
        }

        // Start only the two followers first. They must elect a leader using
        // their durable quorum without the isolated leader's uncommitted log.
        let follower_members = (0..3)
            .filter(|member| *member != isolated_leader)
            .collect::<Vec<_>>();
        for member in &follower_members {
            let member_root = root
                .path()
                .join(cluster.as_str())
                .join(format!("member-{member}"));
            let address_list = addresses.join(";");
            children[*member] = coordinator_raft_child_process(
                cluster.as_str(),
                *member,
                &member_root,
                &address_list,
                &ready,
                control,
                &markers,
                if cluster == "source" {
                    &source_grpc
                } else {
                    &target_grpc
                },
                if cluster == "source" {
                    &target_grpc
                } else {
                    &source_grpc
                },
                &cert_dir,
                &ack_marker,
                &ack_gate,
            );
        }
        for member in &follower_members {
            let pid = children[*member].id();
            wait_for_file(
                &ready.join(format!("{cluster}-{member}-{pid}.ready")),
                children,
            )
            .await;
        }
        wait_for_file(&leader_file, children).await;
        let mut quorum_leader = None;
        for member in 0..3 {
            if member != isolated_leader {
                let member_leader = markers.join(format!("{cluster}.member-{member}.leader"));
                wait_for_file(&member_leader, children).await;
                let observed = tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        if let Some(leader) = read_marker_index(&member_leader)
                            && leader < 3
                        {
                            return leader;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .expect("member leader marker must contain a valid member index");
                assert_ne!(observed, isolated_leader);
                assert!(quorum_leader.is_none_or(|leader| leader == observed));
                quorum_leader = Some(observed);
            }
        }
        write_control(control, &format!("verify-no-uncommitted:{expiry}:quorum"));
        for member in 0..3 {
            if member != isolated_leader {
                wait_for_file(
                    &markers.join(format!("{cluster}.no-uncommitted-quorum-{member}")),
                    children,
                )
                .await;
            }
        }

        // Rejoin the old leader last. Its uncommitted suffix must be truncated
        // by the elected quorum leader, never applied as a committed command.
        let member_root = root
            .path()
            .join(cluster.as_str())
            .join(format!("member-{isolated_leader}"));
        let address_list = addresses.join(";");
        children[isolated_leader] = coordinator_raft_child_process(
            cluster.as_str(),
            isolated_leader,
            &member_root,
            &address_list,
            &ready,
            control,
            &markers,
            if cluster == "source" {
                &source_grpc
            } else {
                &target_grpc
            },
            if cluster == "source" {
                &target_grpc
            } else {
                &source_grpc
            },
            &cert_dir,
            &ack_marker,
            &ack_gate,
        );
        let pid = children[isolated_leader].id();
        wait_for_file(
            &ready.join(format!("{cluster}-{isolated_leader}-{pid}.ready")),
            children,
        )
        .await;
        write_control(control, &format!("verify-no-uncommitted:{expiry}:all"));
        for member in 0..3 {
            wait_for_file(
                &markers.join(format!("{cluster}.no-uncommitted-all-{member}")),
                children,
            )
            .await;
        }
    }

    for (_, _, _, children) in &mut groups {
        for child in children {
            assert!(
                child.try_wait().unwrap().is_none(),
                "a Raft member process exited during uncommitted-entry recovery"
            );
            assert_sigkill(child, "cleanup coordinator recovery Raft member process");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn federated_trust_rotation_rollback_recovers_after_local_and_remote_ack_loss() {
    if std::env::var_os("DATASPOOL_TRUST_HA_CHILD").is_some() {
        run_six_member_child().await;
        return;
    }
    if std::env::var_os("DATASPOOL_ROLLBACK_COORDINATOR_PHASE").is_some() {
        run_rollback_coordinator_process().await;
        return;
    }

    let root = TempDir::new().unwrap();
    let ready = root.path().join("ready");
    let markers = root.path().join("markers");
    let cert_dir = root.path().join("certs");
    std::fs::create_dir_all(&ready).unwrap();
    std::fs::create_dir_all(&markers).unwrap();
    std::fs::create_dir_all(&cert_dir).unwrap();
    for (name, bytes) in [
        ("ca.pem", ROTATION_CA),
        ("server.pem", ROTATION_SERVER_CERT),
        ("server.key.pem", ROTATION_SERVER_KEY),
        ("client.pem", ROTATION_OLD_CERT),
        ("client.key.pem", ROTATION_OLD_KEY),
    ] {
        std::fs::write(cert_dir.join(name), bytes).unwrap();
    }

    let source_grpc = reserve_local_addr();
    let target_grpc = reserve_local_addr();
    let source_ack_marker = root.path().join("source-rollback-begin-committed.marker");
    let source_ack_gate = root.path().join("source-rollback-begin-ack.release");
    let target_ack_marker = root.path().join("target-rollback-committed.marker");
    let target_ack_gate = root.path().join("target-rollback-ack.release");
    let initial_marker = root.path().join("rollback-coordinator-initial.marker");
    let recovered_marker = root.path().join("rollback-coordinator-recovered.marker");
    let mut groups = Vec::new();
    for cluster in ["source", "target"] {
        let cluster_root = root.path().join(cluster);
        std::fs::create_dir_all(&cluster_root).unwrap();
        let addresses = (0..3).map(|_| reserve_local_addr()).collect::<Vec<_>>();
        let address_list = addresses.join(";");
        let control = root.path().join(format!("{cluster}.rollback.control"));
        let mut children = Vec::new();
        for member in 0..3 {
            let member_root = cluster_root.join(format!("member-{member}"));
            std::fs::create_dir_all(&member_root).unwrap();
            let (grpc_addr, peer_grpc_addr, ack_marker, ack_gate, drop_after) =
                if cluster == "source" {
                    (
                        &source_grpc,
                        &target_grpc,
                        &source_ack_marker,
                        &source_ack_gate,
                        0,
                    )
                } else {
                    (
                        &target_grpc,
                        &source_grpc,
                        &target_ack_marker,
                        &target_ack_gate,
                        1,
                    )
                };
            children.push(rollback_raft_child_process(
                cluster,
                member,
                &member_root,
                &address_list,
                &ready,
                &control,
                &markers,
                grpc_addr,
                peer_grpc_addr,
                &cert_dir,
                ack_marker,
                ack_gate,
                drop_after,
            ));
        }
        groups.push((cluster.to_string(), addresses, control, children));
    }
    for (cluster, _, _, children) in &mut groups {
        for member in 0..3 {
            wait_for_file(&ready.join(format!("{cluster}-{member}.ready")), children).await;
        }
        wait_for_file(&markers.join(format!("{cluster}.leader")), children).await;
    }
    for (cluster, _, control, children) in &mut groups {
        write_control(control, "seed-rollback");
        wait_for_file(
            &markers.join(format!("{cluster}.rollback-seeded")),
            children,
        )
        .await;
    }

    let mut initial = rollback_coordinator_process_child(
        "initial",
        &format!("https://{source_grpc}"),
        &format!("https://{target_grpc}"),
        &cert_dir,
        &initial_marker,
    );
    wait_for_file(&source_ack_marker, std::slice::from_mut(&mut initial)).await;
    assert_sigkill(
        &mut initial,
        "SIGKILL coordinator after local BeginRollback commit before ACK",
    );
    std::fs::write(&source_ack_gate, b"release").unwrap();

    let read_snapshot = |endpoint: String, peer_id: &'static str| {
        let ca = cert_dir.join("ca.pem");
        let cert = cert_dir.join("client.pem");
        let key = cert_dir.join("client.key.pem");
        async move {
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    if let Ok(mut client) =
                        GrpcClient::connect_mtls(&endpoint, &ca, &cert, &key).await
                        && let Ok(response) = client
                            .get_remote_trust_rotation(peer_id, "rollback-process-rotation")
                            .await
                        && response.accepted
                        && let Ok(snapshot) = serde_json::from_str::<
                            dataspool::federation::FederatedTrustRotationSnapshot,
                        >(&response.state_json)
                    {
                        return snapshot;
                    }
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
            })
            .await
            .expect("rollback crash-boundary state must be readable")
        }
    };
    let source_after_crash = read_snapshot(format!("https://{source_grpc}"), "target").await;
    let target_after_crash = read_snapshot(format!("https://{target_grpc}"), "source").await;
    assert_eq!(
        source_after_crash.rotation.phase,
        FederatedTrustRotationPhase::RollingBack
    );
    assert_eq!(
        target_after_crash.rotation.phase,
        FederatedTrustRotationPhase::ActivationBarrier
    );
    assert!(
        source_after_crash
            .rotation
            .local_activation_expires_at_ms
            .is_none()
    );
    assert!(
        source_after_crash
            .rotation
            .peer_activation_expires_at_ms
            .is_none()
    );
    assert!(
        target_after_crash
            .rotation
            .local_activation_expires_at_ms
            .is_none()
    );
    assert!(
        target_after_crash
            .rotation
            .peer_activation_expires_at_ms
            .is_none()
    );

    // Resume once: the peer's BeginRollback commits, then its first local
    // rollback acknowledgement commits but its response is dropped. Kill this
    // coordinator too, preserving the second partial-rollback boundary.
    let first_recovery_marker = root
        .path()
        .join("rollback-coordinator-first-recovery.marker");
    let mut first_recovery = rollback_coordinator_process_child(
        "recover-first",
        &format!("https://{source_grpc}"),
        &format!("https://{target_grpc}"),
        &cert_dir,
        &first_recovery_marker,
    );
    wait_for_file(
        &target_ack_marker,
        std::slice::from_mut(&mut first_recovery),
    )
    .await;
    assert_sigkill(
        &mut first_recovery,
        "SIGKILL coordinator after remote rollback commit before ACK",
    );
    std::fs::write(&target_ack_gate, b"release").unwrap();

    let source_after_remote_crash = read_snapshot(format!("https://{source_grpc}"), "target").await;
    let target_after_remote_crash = read_snapshot(format!("https://{target_grpc}"), "source").await;
    assert_eq!(
        source_after_remote_crash.rotation.phase,
        FederatedTrustRotationPhase::RollingBack
    );
    assert!(source_after_remote_crash.rotation.local_staged);
    assert!(source_after_remote_crash.rotation.peer_staged);
    assert_eq!(
        target_after_remote_crash.rotation.phase,
        FederatedTrustRotationPhase::RollingBack
    );
    assert!(!target_after_remote_crash.rotation.local_staged);
    assert!(target_after_remote_crash.rotation.peer_staged);
    assert!(
        target_after_remote_crash
            .peer_pending_ca_fingerprint
            .is_none()
    );
    assert!(
        target_after_remote_crash
            .peer_pending_identity_fingerprint
            .is_none()
    );
    assert_eq!(
        target_after_remote_crash
            .peer_active_ca_fingerprint
            .as_deref(),
        Some(OLD_FINGERPRINT_CA)
    );
    assert_eq!(
        target_after_remote_crash.peer_active_identity_fingerprint,
        OLD_FINGERPRINT
    );

    let mut recovered = rollback_coordinator_process_child(
        "recover-final",
        &format!("https://{source_grpc}"),
        &format!("https://{target_grpc}"),
        &cert_dir,
        &recovered_marker,
    );
    wait_for_file(&recovered_marker, std::slice::from_mut(&mut recovered)).await;
    let status = recovered
        .wait()
        .expect("wait for recovered rollback coordinator");
    assert!(
        status.success(),
        "rollback coordinator recovery failed: {status}"
    );

    let source_final = read_snapshot(format!("https://{source_grpc}"), "target").await;
    let target_final = read_snapshot(format!("https://{target_grpc}"), "source").await;
    for snapshot in [&source_final, &target_final] {
        assert_eq!(
            snapshot.rotation.phase,
            FederatedTrustRotationPhase::RolledBack
        );
        assert!(!snapshot.rotation.local_staged && !snapshot.rotation.peer_staged);
        assert!(snapshot.rotation.local_activation_expires_at_ms.is_none());
        assert!(snapshot.rotation.peer_activation_expires_at_ms.is_none());
        assert_eq!(
            snapshot.peer_active_ca_fingerprint.as_deref(),
            Some(OLD_FINGERPRINT_CA)
        );
        assert!(snapshot.peer_pending_ca_fingerprint.is_none());
        assert!(snapshot.peer_previous_ca_fingerprint.is_none());
        assert_eq!(snapshot.peer_active_identity_fingerprint, OLD_FINGERPRINT);
        assert!(snapshot.peer_pending_identity_fingerprint.is_none());
    }
    for (_, _, _, children) in &mut groups {
        for child in children {
            assert!(
                child.try_wait().unwrap().is_none(),
                "rollback must not restart or kill Raft members"
            );
            assert_sigkill(child, "cleanup rollback six-member Raft process");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn federated_certificate_rotation_recovers_after_sigkill_across_both_raft_processes() {
    if std::env::var_os("DATASPOOL_ROTATION_PROCESS_PHASE").is_some() {
        run_child_process().await;
        return;
    }

    let source_root = TempDir::new().unwrap();
    let target_root = TempDir::new().unwrap();
    let source_addr = reserve_local_addr();
    let target_addr = reserve_local_addr();
    let source_grpc_addr = reserve_local_addr();
    let target_grpc_addr = reserve_local_addr();
    let source_stage = source_root.path().join("stage.ready");
    let target_stage = target_root.path().join("stage.ready");
    let mut source = child_process(
        "source",
        "stage",
        source_root.path(),
        &source_addr,
        &source_grpc_addr,
        &source_stage,
    );
    let mut target = child_process(
        "target",
        "stage",
        target_root.path(),
        &target_addr,
        &target_grpc_addr,
        &target_stage,
    );
    wait_for_marker(&source_stage, &mut source).await;
    wait_for_marker(&target_stage, &mut target).await;

    // Stage is durably committed on both independent clusters, but Proof has
    // not happened. Kill both OS processes without running destructors.
    assert_sigkill(&mut source, "crash after Stage before Proof");
    assert_sigkill(&mut target, "crash after Stage before Proof");

    let source_proof = source_root.path().join("proof.ready");
    let target_proof = target_root.path().join("proof.ready");
    let mut source = child_process(
        "source",
        "proof",
        source_root.path(),
        &source_addr,
        &source_grpc_addr,
        &source_proof,
    );
    let mut target = child_process(
        "target",
        "proof",
        target_root.path(),
        &target_addr,
        &target_grpc_addr,
        &target_proof,
    );
    wait_for_marker(&source_proof, &mut source).await;
    wait_for_marker(&target_proof, &mut target).await;

    // Both CA Activate commands and certificate Proof commands are committed.
    // The child has not reported completion to the parent; kill both processes
    // at that commit-before-ack boundary and replay Activate after restart.
    assert_sigkill(&mut source, "crash after Proof commit before ACK");
    assert_sigkill(&mut target, "crash after Proof commit before ACK");

    // Let the protocol's minimum one-minute overlap expire in wall-clock time
    // before testing fresh mTLS reconnects with the old leaf certificate.
    tokio::time::sleep(Duration::from_secs(61)).await;

    // Restart both clusters concurrently from their original durable RocksDB
    // directories and verify the active identity, overlap deadline and replay.
    let source_verified = source_root.path().join("verified.ready");
    let target_verified = target_root.path().join("verified.ready");
    let mut source = child_process(
        "source",
        "verify",
        source_root.path(),
        &source_addr,
        &source_grpc_addr,
        &source_verified,
    );
    let mut target = child_process(
        "target",
        "verify",
        target_root.path(),
        &target_addr,
        &target_grpc_addr,
        &target_verified,
    );
    wait_for_marker(&source_verified, &mut source).await;
    wait_for_marker(&target_verified, &mut target).await;
    let cert_dir = source_root.path().join("client-certs");
    std::fs::create_dir_all(&cert_dir).unwrap();
    let write = |name: &str, bytes: &[u8]| {
        let path = cert_dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    };
    let ca = write("ca.pem", ROTATION_CA);
    let old_cert = write("old-client.pem", ROTATION_OLD_CERT);
    let old_key = write("old-client.key.pem", ROTATION_OLD_KEY);
    let new_cert = write("new-client.pem", ROTATION_NEW_CERT);
    let new_key = write("new-client.key.pem", ROTATION_NEW_KEY);

    for (endpoint, peer_id) in [
        (format!("https://{source_grpc_addr}"), "target-process"),
        (format!("https://{target_grpc_addr}"), "source-process"),
    ] {
        // The old CA is no longer in the effective inbound trust bundle, so
        // a fresh TLS handshake using its old client leaf must fail outright.
        match GrpcClient::connect_mtls(&endpoint, &ca, &old_cert, &old_key).await {
            Err(_) => {}
            Ok(mut expired_old) => assert!(
                expired_old
                    .get_remote_task("00000000-0000-0000-0000-000000000001", peer_id)
                    .await
                    .is_err(),
                "expired old CA completed an authenticated RPC to {endpoint}"
            ),
        }

        // The new active certificate can reconnect and replay the completed
        // proof. The server must return the committed result without extending
        // the previous-certificate deadline.
        let mut active_new = connect_mtls_retry(&endpoint, &ca, &new_cert, &new_key).await;
        let replay = active_new
            .prove_federated_certificate_rotation(
                peer_id,
                NEW_FINGERPRINT,
                if peer_id == "target-process" {
                    "proof-process-source"
                } else {
                    "proof-process-target"
                },
            )
            .await
            .unwrap();
        assert!(replay.accepted, "{}", replay.message);
    }
    assert_sigkill(&mut source, "cleanup recovered source Raft process");
    assert_sigkill(&mut target, "cleanup recovered target Raft process");
}
