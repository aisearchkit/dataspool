// SPDX-License-Identifier: AGPL-3.0-or-later
//! Process-level certificate-rotation recovery across two independent Raft groups.
//! Each Raft member is a real child process and is terminated with SIGKILL.

#![cfg(unix)]

use dataspool::{
    comms::{GrpcClient, GrpcServer},
    federation::{FederatedCluster, FederatedClusterStatus},
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
const RAFT_TOKEN: &str = "federated-rotation-process-test-token-0123456789";

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
