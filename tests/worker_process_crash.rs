// SPDX-License-Identifier: AGPL-3.0-or-later
//! Process-level recovery proof: a parent sends SIGKILL after a real Elasticsearch
//! Bulk commit and before Raft records delivery, then starts a fresh worker process.

#![cfg(feature = "test-fault-injection")]

use dataspool::agent::{ShardClaimer, ShardExecutor};
use dataspool::drivers::document::{DocumentConfig, verify_source_target};
use dataspool::drivers::factory::{SinkConfig, SourceConfig};
use dataspool::raft::{Command, RaftNode, default_raft_config};
use dataspool::storage::index::{TieredConfig, TieredIndexStore};
use dataspool::storage::kv::KvStore;
use dataspool::storage::{RocksDbStore, cf};
use dataspool::task::{ShardMeta, TaskMeta, TaskStore};
use openraft::BasicNode;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

fn es_url() -> String {
    ["ES_7102_URL", "ES_717_URL", "ES_8X_URL", "ES_9X_URL"]
        .into_iter()
        .find_map(|key| std::env::var(key).ok())
        .expect("one live Elasticsearch endpoint must be configured")
        .trim_end_matches('/')
        .to_string()
}

fn child_command(
    phase: &str,
    root: &std::path::Path,
    marker: &std::path::Path,
    shard_id: Option<&str>,
) -> Child {
    let mut command =
        ProcessCommand::new(std::env::current_exe().expect("current test executable"));
    command
        .args([
            "--exact",
            "worker_process_crash_is_recovered_after_sigkill",
            "--nocapture",
        ])
        .env("DATASPOOL_WORKER_PHASE", phase)
        .env("DATASPOOL_WORKER_ROOT", root)
        .env("DATASPOOL_CRASH_MARKER", marker)
        .env("ES_WORKER_E2E_URL", es_url())
        .env(
            "ES_WORKER_TARGET_URL",
            std::env::var("ES_WORKER_TARGET_URL").expect("separate target cluster endpoint"),
        )
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    if let Some(shard_id) = shard_id {
        command.env("DATASPOOL_WORKER_SHARD_ID", shard_id);
    }
    command.spawn().expect("spawn isolated worker process")
}

async fn run_worker_process(phase: &str) {
    let root = PathBuf::from(std::env::var("DATASPOOL_WORKER_ROOT").unwrap());
    let base_url = std::env::var("ES_WORKER_E2E_URL").unwrap();
    let target_url = std::env::var("ES_WORKER_TARGET_URL").unwrap();
    let app_dir = root.join("app");
    let raft_dir = root.join("raft");
    let db = RocksDbStore::open(&app_dir).unwrap();
    let store = Arc::new(TaskStore::new(db.clone()));
    let token = "worker-process-e2e-raft-token-0123456789".to_string();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let raft = RaftNode::new(
        1,
        &raft_dir,
        (*store).clone(),
        default_raft_config(),
        token.clone(),
    )
    .await
    .unwrap();
    let router = dataspool::raft_router(raft.handle(), token);
    let rpc = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    if phase == "crash" {
        raft.initialize(BTreeMap::from([(1, BasicNode::new(address.to_string()))]))
            .await
            .unwrap();
    }
    raft.wait_for_leader(std::time::Duration::from_secs(15))
        .await
        .unwrap();

    let shard_id = if phase == "crash" {
        let mut source = DocumentConfig::local("dataspool-worker-crash-source");
        source.urls = vec![base_url.clone()];
        source.batch_size = 3;
        let mut sink = DocumentConfig::local("dataspool-worker-crash-target");
        sink.urls = vec![target_url];
        let task = TaskMeta::new(
            "worker-process-sigkill-e2e".into(),
            serde_json::to_string(&SourceConfig::Document { config: source }).unwrap(),
            serde_json::to_string(&SinkConfig::Document { config: sink }).unwrap(),
        );
        let shard = ShardMeta::new(task.task_id, 0, "{}".into());
        let shard_id = shard.shard_id;
        let response = raft
            .propose(Command::CreateTask {
                request_id: format!("worker-process-e2e:{}", task.task_id),
                actor: "process-e2e".into(),
                task: Box::new(task),
                shards: vec![shard],
            })
            .await
            .unwrap();
        assert!(response.success, "{}", response.message);
        shard_id
    } else {
        std::env::var("DATASPOOL_WORKER_SHARD_ID")
            .unwrap()
            .parse()
            .unwrap()
    };

    let claimer = ShardClaimer::new(
        "worker-process".into(),
        store.clone(),
        Arc::new(raft.clone()),
    )
    .with_lease_seconds(3)
    .unwrap();
    let lease = claimer
        .try_claim()
        .await
        .unwrap()
        .expect("pending shard claim");
    if phase == "crash" {
        std::fs::write(
            root.join("identity.json"),
            serde_json::to_vec(&json!({"shard_id": shard_id.to_string()})).unwrap(),
        )
        .unwrap();
    }
    let index_store = Arc::new(RwLock::new(TieredIndexStore::new(
        TieredConfig::default(),
        Arc::new(db),
    )));
    let executor = ShardExecutor::new_with_raft(store.clone(), index_store, Arc::new(raft.clone()))
        .with_batch_size(3);
    if phase == "crash" {
        // This call runs the production factory -> DocumentSource -> DocumentSink
        // -> durable batch manifest -> fenced Elasticsearch Bulk path.
        executor
            .with_process_crash_after_sink_before_delivery()
            .execute_with_lease(&shard_id.to_string(), lease, CancellationToken::new())
            .await
            .unwrap();
        panic!("crash hook did not block after destination commit");
    }

    executor
        .execute_with_lease(&shard_id.to_string(), lease, CancellationToken::new())
        .await
        .unwrap();
    let shard = store.get_shard(&shard_id).await.unwrap().unwrap();
    assert!(matches!(
        shard.state,
        dataspool::types::ShardState::Completed { .. }
    ));
    let batch_prefix = format!("batch_manifest:{}:", shard_id);
    let entries = store
        .kv_store()
        .prefix_scan_cf(cf::META, batch_prefix.as_bytes())
        .await
        .unwrap();
    assert!(
        !entries.is_empty(),
        "recovered process must retain durable batch manifests"
    );
    for (_, bytes) in entries {
        let manifest: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(manifest["status"], "acknowledged");
    }
    raft.shutdown().await.unwrap();
    rpc.abort();
}

#[tokio::test]
async fn worker_process_crash_is_recovered_after_sigkill() {
    if let Ok(phase) = std::env::var("DATASPOOL_WORKER_PHASE") {
        run_worker_process(&phase).await;
        return;
    }

    let base_url = es_url();
    let target_url =
        std::env::var("ES_WORKER_TARGET_URL").expect("separate target cluster endpoint");
    let http = reqwest::Client::new();
    let info: Value = http
        .get(format!("{base_url}/"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let version = info["version"]["number"].as_str().unwrap_or_default();
    assert!(
        version.starts_with("7.10.")
            || version.starts_with("7.17.")
            || version.starts_with("8.")
            || version.starts_with("9.")
    );

    let _ = http
        .delete(format!("{base_url}/dataspool-worker-crash-source"))
        .send()
        .await;
    let _ = http
        .delete(format!("{target_url}/dataspool-worker-crash-target"))
        .send()
        .await;
    http.put(format!("{base_url}/dataspool-worker-crash-source"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    http.put(format!("{target_url}/dataspool-worker-crash-target"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let source_bulk = r#"{"index":{"_id":"crash-1"}}
{"value":1}
{"index":{"_id":"crash-2"}}
{"value":2}
{"index":{"_id":"crash-3"}}
{"value":3}
"#;
    http.post(format!("{base_url}/dataspool-worker-crash-source/_bulk"))
        .header("content-type", "application/x-ndjson")
        .body(source_bulk)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    http.post(format!("{base_url}/dataspool-worker-crash-source/_refresh"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let root = tempfile::tempdir().unwrap();
    let marker = root.path().join("bulk-committed.marker");
    let mut worker = child_command("crash", root.path(), &marker, None);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(90);
    while !marker.exists() {
        if let Some(status) = worker.try_wait().unwrap() {
            panic!("crash worker exited before destination commit marker: {status}");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for Bulk-commit crash point"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // `Child::kill` maps to SIGKILL on Unix (equivalent to kill -9).
    worker.kill().expect("send SIGKILL to worker process");
    let status = worker.wait().unwrap();
    assert!(
        !status.success(),
        "killed worker must not exit successfully"
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(9),
            "worker must be terminated by SIGKILL"
        );
    }

    // The crashed worker cannot release its lease; wait for the committed
    // three-second lease to expire before the replacement claims the shard.
    tokio::time::sleep(std::time::Duration::from_millis(3_250)).await;
    let identity: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("identity.json")).unwrap()).unwrap();
    let shard_id = identity["shard_id"].as_str().unwrap();
    let mut recovery = child_command(
        "recover",
        root.path(),
        &root.path().join("recovery.marker"),
        Some(shard_id),
    );
    let output = recovery.wait().unwrap();
    assert!(output.success(), "recovery worker failed: {output}");

    let mut source = DocumentConfig::local("dataspool-worker-crash-source");
    source.urls = vec![base_url.clone()];
    let mut target = DocumentConfig::local("dataspool-worker-crash-target");
    target.urls = vec![target_url];
    let report = verify_source_target(&source, &target, 2).await.unwrap();
    assert!(
        report.is_consistent(),
        "source/target mismatch after SIGKILL recovery: {report:?}"
    );
    assert_eq!(report.source.document_count, 3);
}
