// SPDX-License-Identifier: AGPL-3.0-or-later
//! Process-level CDC recovery with a five-member Raft cluster. The CDC worker
//! is itself a persistent Raft member and is SIGKILLed after target Bulk commit.

#![cfg(feature = "test-fault-injection")]

use dataspool::agent::ShardClaimer;
use dataspool::drivers::document::{CdcConfig, DocumentConfig, ElasticsearchCdcSource};
use dataspool::drivers::factory::{SinkConfig, SourceConfig};
use dataspool::pipeline::{OffsetValue, Source};
use dataspool::raft::{Command, RaftNode, raft_config};
use dataspool::storage::RocksDbStore;
use dataspool::task::{ShardMeta, TaskMeta, TaskStore};
use openraft::BasicNode;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::Arc;
use std::time::Duration;

const PARENT_NODE_IDS: [u64; 4] = [511, 522, 533, 544];
const WORKER_NODE_ID: u64 = 555;
const RAFT_TOKEN: &str = "cdc-worker-joint-failover-token-0123456789";

fn env_required(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{key} is required"))
}

async fn wait_for_file(path: &std::path::Path, child: &mut Child, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !path.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("worker exited before writing {}: {status}", path.display());
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn apply_cdc_batch(
    source_url: &str,
    target_url: &str,
    outbox: &str,
    marker: Option<&std::path::Path>,
) -> u64 {
    let mut connection = DocumentConfig::local(outbox);
    connection.urls = vec![source_url.to_string()];
    let mut cdc = ElasticsearchCdcSource::new(
        connection,
        CdcConfig {
            change_index: outbox.to_string(),
            ..CdcConfig::default()
        },
    );
    cdc.connect().await.unwrap();
    let (batch, checkpoint) = cdc.read_batch(&OffsetValue::initial(), 100).await.unwrap();
    assert_eq!(
        batch.len(),
        1,
        "joint crash test expects exactly one Outbox event"
    );
    let record = batch.records().first().unwrap();
    let event_id = record
        .get_metadata("_cdc_event_id")
        .expect("CDC source must expose stable Outbox event identity");
    let document_id = record.id.as_deref().unwrap();
    let target_index = record.get_metadata("_target_index").unwrap();
    assert_eq!(record.get_metadata("_op"), Some("index"));
    let value = record.payload["value"].as_i64().unwrap();
    let bulk = format!(
        "{}\n{}\n",
        serde_json::to_string(&json!({"update":{"_index":target_index,"_id":document_id}})).unwrap(),
        serde_json::to_string(&json!({
            "script": {
                "lang": "painless",
                "source": "if (ctx._source.applied_events == null) { ctx._source.applied_events = []; } if (!ctx._source.applied_events.contains(params.event_id)) { ctx._source.applied_events.add(params.event_id); ctx._source.effect_count = (ctx._source.effect_count == null ? 0 : ctx._source.effect_count) + 1; ctx._source.value = params.value; }",
                "params": {"event_id":event_id,"value":value}
            },
            "upsert": {"applied_events":[],"effect_count":0,"value":value},
            "scripted_upsert": true
        })).unwrap()
    );
    let response = reqwest::Client::new()
        .post(format!("{target_url}/_bulk"))
        .header("content-type", "application/x-ndjson")
        .body(bulk)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body: Value = response.json().await.unwrap();
    assert!(status.is_success(), "CDC Bulk failed: {body}");
    assert_eq!(body["errors"], false, "CDC Bulk item failed: {body}");
    cdc.close().await.unwrap();
    if let Some(marker) = marker {
        std::fs::write(
            marker,
            serde_json::to_vec(&json!({"last_sequence":1})).unwrap(),
        )
        .unwrap();
    }
    match checkpoint {
        OffsetValue::Custom(value) => value["last_sequence"].as_u64().unwrap(),
        _ => panic!("CDC source returned a non-custom checkpoint"),
    }
}

async fn run_worker_process(phase: &str) {
    let source = env_required("DATASPOOL_CDC_FAILOVER_SOURCE");
    let target = env_required("DATASPOOL_CDC_FAILOVER_TARGET");
    let outbox = env_required("DATASPOOL_CDC_FAILOVER_OUTBOX");
    let root = std::path::PathBuf::from(env_required("DATASPOOL_CDC_FAILOVER_ROOT"));
    let address = env_required("DATASPOOL_CDC_FAILOVER_ADDRESS");
    let app_dir = root.join("worker-app");
    let raft_dir = root.join("worker-raft");
    let db = RocksDbStore::open(&app_dir).unwrap();
    let store = Arc::new(TaskStore::new(db.clone()));
    let node = RaftNode::new(
        WORKER_NODE_ID,
        &raft_dir,
        (*store).clone(),
        raft_config(Duration::from_millis(150), Duration::from_millis(600)),
        RAFT_TOKEN.into(),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
    let router = dataspool::raft_router(node.handle(), RAFT_TOKEN.into());
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    std::fs::write(env_required("DATASPOOL_CDC_FAILOVER_READY"), b"ready").unwrap();
    node.wait_for_leader(Duration::from_secs(60)).await.unwrap();

    let lease_seconds = if phase == "crash" { 3 } else { 30 };
    let claimer = ShardClaimer::new(
        "cdc-worker-process-555".into(),
        store.clone(),
        Arc::new(node.clone()),
    )
    .with_lease_seconds(lease_seconds)
    .unwrap();
    let lease = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if let Some(lease) = claimer.try_claim().await.unwrap() {
                break lease;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("worker could not claim CDC shard");
    let task_id = store
        .get_shard(&lease.shard_id)
        .await
        .unwrap()
        .unwrap()
        .task_id;

    if phase == "crash" {
        std::fs::write(
            env_required("DATASPOOL_CDC_FAILOVER_FIRST_LEASE"),
            serde_json::to_vec(&json!({
                "task_id":task_id.to_string(), "shard_id":lease.shard_id.to_string(),
                "node_id":lease.node_id.to_string(), "fencing_token":lease.fencing_token
            }))
            .unwrap(),
        )
        .unwrap();
        apply_cdc_batch(
            &source,
            &target,
            &outbox,
            Some(std::path::Path::new(&env_required(
                "DATASPOOL_CDC_FAILOVER_BULK_MARKER",
            ))),
        )
        .await;
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }

    let first: Value = serde_json::from_slice(
        &std::fs::read(env_required("DATASPOOL_CDC_FAILOVER_FIRST_LEASE")).unwrap(),
    )
    .unwrap();
    assert!(
        lease.fencing_token > first["fencing_token"].as_u64().unwrap(),
        "restarted worker must claim a higher fencing generation"
    );
    let watermark = apply_cdc_batch(&source, &target, &outbox, None).await;
    let progress = node
        .propose(Command::UpdateShardProgress {
            shard_id: lease.shard_id.to_string(),
            node_id: lease.node_id.to_string(),
            fencing_token: lease.fencing_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            processed_records: 1,
            processed_bytes: 0,
            offset: serde_json::to_string(&OffsetValue::Custom(
                json!({"driver":"elasticsearch_cdc","last_sequence":watermark}),
            ))
            .unwrap(),
            batch_id: None,
        })
        .await
        .unwrap();
    assert!(
        progress.success,
        "recovered CDC checkpoint was rejected: {}",
        progress.message
    );

    let premature = node
        .propose(Command::CompleteShard {
            shard_id: lease.shard_id.to_string(),
            node_id: lease.node_id.to_string(),
            fencing_token: lease.fencing_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            total_records: 1,
        })
        .await
        .unwrap();
    assert!(
        !premature.success,
        "online shard must not complete before the Raft cutover record"
    );

    let alias = env_required("DATASPOOL_CDC_FAILOVER_ALIAS");
    let old_index = env_required("DATASPOOL_CDC_FAILOVER_OLD_INDEX");
    let new_index = env_required("DATASPOOL_CDC_FAILOVER_NEW_INDEX");
    let http = reqwest::Client::new();
    let switched = http
        .post(format!("{target}/_aliases"))
        .json(&json!({"actions":[
            {"remove":{"index":old_index,"alias":alias}},
            {"add":{"index":new_index,"alias":alias}}
        ]}))
        .send()
        .await
        .unwrap();
    assert!(
        switched.status().is_success(),
        "alias switch failed: {}",
        switched.text().await.unwrap_or_default()
    );
    let cutover = node
        .propose(Command::RecordCdcCutover {
            task_id: task_id.to_string(),
            alias,
            old_index: Some(old_index),
            new_index,
            final_watermark: watermark,
            applied_watermark: watermark,
            fencing_shard_id: lease.shard_id.to_string(),
            node_id: lease.node_id.to_string(),
            fencing_token: lease.fencing_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
        })
        .await
        .unwrap();
    assert!(
        cutover.success,
        "Raft CDC cutover record failed: {}",
        cutover.message
    );
    let complete = node
        .propose(Command::CompleteShard {
            shard_id: lease.shard_id.to_string(),
            node_id: lease.node_id.to_string(),
            fencing_token: lease.fencing_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            total_records: 1,
        })
        .await
        .unwrap();
    assert!(
        complete.success,
        "Cutover Completion Gate rejected completed watermark: {}",
        complete.message
    );
    node.shutdown().await.unwrap();
    server.abort();
}

async fn start_parent_node(
    id: u64,
    address: &str,
    db: RocksDbStore,
    raft_dir: &std::path::Path,
) -> (RaftNode, tokio::task::JoinHandle<()>) {
    let node = RaftNode::new(
        id,
        raft_dir,
        TaskStore::new(db),
        raft_config(Duration::from_millis(150), Duration::from_millis(600)),
        RAFT_TOKEN.into(),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let router = dataspool::raft_router(node.handle(), RAFT_TOKEN.into());
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (node, server)
}

#[allow(clippy::too_many_arguments)]
fn spawn_worker(
    phase: &str,
    root: &std::path::Path,
    address: &str,
    source: &str,
    target: &str,
    outbox: &str,
    alias: &str,
    old_index: &str,
    new_index: &str,
    ready: &std::path::Path,
    first_lease: &std::path::Path,
    bulk_marker: &std::path::Path,
) -> Child {
    ProcessCommand::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "cdc_worker_crash_and_raft_failover_replays_without_duplicate_effects",
            "--nocapture",
        ])
        .env("DATASPOOL_CDC_FAILOVER_PHASE", phase)
        .env("DATASPOOL_CDC_FAILOVER_ROOT", root)
        .env("DATASPOOL_CDC_FAILOVER_ADDRESS", address)
        .env("DATASPOOL_CDC_FAILOVER_SOURCE", source)
        .env("DATASPOOL_CDC_FAILOVER_TARGET", target)
        .env("DATASPOOL_CDC_FAILOVER_OUTBOX", outbox)
        .env("DATASPOOL_CDC_FAILOVER_ALIAS", alias)
        .env("DATASPOOL_CDC_FAILOVER_OLD_INDEX", old_index)
        .env("DATASPOOL_CDC_FAILOVER_NEW_INDEX", new_index)
        .env("DATASPOOL_CDC_FAILOVER_READY", ready)
        .env("DATASPOOL_CDC_FAILOVER_FIRST_LEASE", first_lease)
        .env("DATASPOOL_CDC_FAILOVER_BULK_MARKER", bulk_marker)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn CDC Raft worker process")
}

#[tokio::test]
async fn cdc_worker_crash_and_raft_failover_replays_without_duplicate_effects() {
    if let Ok(phase) = std::env::var("DATASPOOL_CDC_FAILOVER_PHASE") {
        run_worker_process(&phase).await;
        return;
    }
    let source = ["ES_7102_URL", "ES_717_URL", "ES_8X_URL", "ES_9X_URL"]
        .into_iter()
        .find_map(|key| std::env::var(key).ok())
        .expect("live Elasticsearch source URL")
        .trim_end_matches('/')
        .to_string();
    let target = env_required("ES_WORKER_TARGET_URL")
        .trim_end_matches('/')
        .to_string();
    assert_ne!(
        source, target,
        "source and target must be independent clusters"
    );
    let suffix = std::process::id();
    let source_index = format!("dataspool-cdc-joint-source-{suffix}");
    let outbox = format!("dataspool-cdc-joint-outbox-{suffix}");
    let target_index = format!("dataspool-cdc-joint-target-{suffix}");
    let old_index = format!("dataspool-cdc-joint-old-{suffix}");
    let alias = format!("dataspool-cdc-joint-live-{suffix}");
    let http = reqwest::Client::new();
    for (url, index, mapping) in [
        (
            &source,
            &source_index,
            json!({"mappings":{"properties":{"value":{"type":"long"}}}}),
        ),
        (
            &source,
            &outbox,
            json!({"mappings":{"properties":{"sequence":{"type":"long"},"operation":{"type":"keyword"},"document_id":{"type":"keyword"},"target_index":{"type":"keyword"},"document":{"type":"object"}}}}),
        ),
        (&target, &target_index, json!({})),
        (&target, &old_index, json!({})),
    ] {
        let _ = http.delete(format!("{url}/{index}")).send().await;
        let response = http
            .put(format!("{url}/{index}"))
            .json(&mapping)
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "create {index}: {}",
            response.text().await.unwrap_or_default()
        );
    }
    let seed = "{\"index\":{\"_id\":\"cdc-doc-1\"}}\n{\"value\":1}\n";
    let response = http
        .post(format!("{source}/{source_index}/_bulk"))
        .header("content-type", "application/x-ndjson")
        .body(seed)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let response = http
        .put(format!("{source}/{source_index}/_doc/cdc-doc-1"))
        .json(&json!({"value":99}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let event = json!({"sequence":1,"operation":"index","document_id":"cdc-doc-1","target_index":target_index,"document":{"value":99}});
    let event_body = format!(
        "{{\"index\":{{\"_id\":\"event-1\"}}}}\n{}\n",
        serde_json::to_string(&event).unwrap()
    );
    let response = http
        .post(format!("{source}/{outbox}/_bulk"))
        .header("content-type", "application/x-ndjson")
        .body(event_body)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    for index in [&source_index, &outbox] {
        let _ = http
            .post(format!("{source}/{index}/_refresh"))
            .send()
            .await
            .unwrap();
    }
    let old_alias = http
        .post(format!("{target}/_aliases"))
        .json(&json!({"actions":[{"add":{"index":old_index,"alias":alias}}]}))
        .send()
        .await
        .unwrap();
    assert!(old_alias.status().is_success());

    let root = tempfile::tempdir().unwrap();
    let mut parent_addresses = Vec::new();
    for _ in PARENT_NODE_IDS {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        parent_addresses.push(listener.local_addr().unwrap().to_string());
        drop(listener);
    }
    let worker_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let worker_address = worker_listener.local_addr().unwrap().to_string();
    drop(worker_listener);
    let mut parent_nodes = Vec::new();
    let mut parent_servers = Vec::new();
    let mut parent_stores = Vec::new();
    let mut parent_dbs = Vec::new();
    for (index, id) in PARENT_NODE_IDS.iter().enumerate() {
        let app = root.path().join(format!("parent-{id}/app"));
        let raft_dir = root.path().join(format!("parent-{id}/raft"));
        let db = RocksDbStore::open(&app).unwrap();
        parent_stores.push(Arc::new(TaskStore::new(db.clone())));
        parent_dbs.push(db.clone());
        let (node, server) = start_parent_node(*id, &parent_addresses[index], db, &raft_dir).await;
        parent_nodes.push(node);
        parent_servers.push(server);
    }
    let worker_ready = root.path().join("worker-ready");
    let first_lease_file = root.path().join("first-lease.json");
    let bulk_marker = root.path().join("target-bulk-committed");
    let mut worker = spawn_worker(
        "crash",
        root.path(),
        &worker_address,
        &source,
        &target,
        &outbox,
        &alias,
        &old_index,
        &target_index,
        &worker_ready,
        &first_lease_file,
        &bulk_marker,
    );
    wait_for_file(&worker_ready, &mut worker, Duration::from_secs(30)).await;

    let members = PARENT_NODE_IDS
        .iter()
        .zip(&parent_addresses)
        .map(|(id, address)| (*id, BasicNode::new(address.clone())))
        .chain(std::iter::once((
            WORKER_NODE_ID,
            BasicNode::new(worker_address.clone()),
        )))
        .collect::<BTreeMap<_, _>>();
    parent_nodes[0].initialize(members).await.unwrap();
    let leader_id = parent_nodes[0]
        .wait_for_leader(Duration::from_secs(20))
        .await
        .unwrap();
    let leader_index = PARENT_NODE_IDS.iter().position(|id| *id == leader_id);

    let mut snapshot = DocumentConfig::local(&source_index);
    snapshot.urls = vec![source.clone()];
    let mut changes = DocumentConfig::local(&outbox);
    changes.urls = vec![source.clone()];
    let mut sink = DocumentConfig::local(&target_index);
    sink.urls = vec![target.clone()];
    let task = TaskMeta::new(
        "cdc-worker-joint-crash-failover".into(),
        serde_json::to_string(&SourceConfig::ElasticsearchOnline {
            snapshot,
            change_log_connection: changes,
            cdc: CdcConfig {
                change_index: outbox.clone(),
                ..CdcConfig::default()
            },
        })
        .unwrap(),
        serde_json::to_string(&SinkConfig::Document { config: sink }).unwrap(),
    );
    let task_id = task.task_id;
    let shard = ShardMeta::new(task_id, 0, "{}".into());
    let shard_id = shard.shard_id;
    // Any parent member can forward the proposal, including when the worker
    // process itself won the initial election.
    let created = parent_nodes[0]
        .propose(Command::CreateTask {
            request_id: format!("cdc-joint-create:{task_id}"),
            actor: "cdc-joint-e2e".into(),
            task: Box::new(task),
            shards: vec![shard],
        })
        .await
        .unwrap();
    assert!(created.success, "{}", created.message);
    wait_for_file(&bulk_marker, &mut worker, Duration::from_secs(60)).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(pre_crash_shard) = parent_stores[0].get_shard(&shard_id).await.unwrap() {
                assert_eq!(
                    pre_crash_shard.offset,
                    OffsetValue::initial(),
                    "target Bulk must be committed while the Raft CDC checkpoint is still uncommitted"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("claimed shard state did not replicate before the worker crash");
    worker
        .kill()
        .expect("SIGKILL worker after destination Bulk success and before checkpoint");
    let killed = worker.wait().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(killed.signal(), Some(9), "CDC worker must receive SIGKILL");
    }

    // Stop the elected leader as well. If it was the worker process, its death
    // already removed the leader; otherwise stop the parent-owned leader now.
    if let Some(index) = leader_index {
        parent_nodes[index].shutdown().await.unwrap();
        parent_servers[index].abort();
        let _ = (&mut parent_servers[index]).await;
    }
    let survivor_index = (0..PARENT_NODE_IDS.len())
        .find(|index| Some(*index) != leader_index)
        .unwrap();
    let new_leader = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(current) = parent_nodes[survivor_index].current_leader().await
                && current != leader_id
            {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("three surviving Raft voters did not elect a replacement leader");
    assert_ne!(
        new_leader, leader_id,
        "Raft leader must change after the injected failure"
    );
    tokio::time::sleep(Duration::from_millis(3_300)).await;

    let recovery_ready = root.path().join("worker-recovery-ready");
    let mut recovery_worker = spawn_worker(
        "recover",
        root.path(),
        &worker_address,
        &source,
        &target,
        &outbox,
        &alias,
        &old_index,
        &target_index,
        &recovery_ready,
        &first_lease_file,
        &bulk_marker,
    );
    wait_for_file(
        &recovery_ready,
        &mut recovery_worker,
        Duration::from_secs(30),
    )
    .await;
    let recovery_status = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if let Some(status) = recovery_worker.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("recovery worker did not finish checkpoint and cutover");
    assert!(
        recovery_status.success(),
        "recovery worker failed: {recovery_status}"
    );

    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(current) = parent_nodes[survivor_index].current_leader().await
                && current != WORKER_NODE_ID
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("parent Raft quorum did not elect a leader after the recovered worker exited");
    let first_lease: Value =
        serde_json::from_slice(&std::fs::read(&first_lease_file).unwrap()).unwrap();
    let stale = parent_nodes[survivor_index]
        .propose(Command::UpdateShardProgress {
            shard_id: first_lease["shard_id"].as_str().unwrap().to_string(),
            node_id: first_lease["node_id"].as_str().unwrap().to_string(),
            fencing_token: first_lease["fencing_token"].as_u64().unwrap(),
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            processed_records: 999,
            processed_bytes: 0,
            offset: serde_json::to_string(&OffsetValue::Custom(
                json!({"driver":"elasticsearch_cdc","last_sequence":999}),
            ))
            .unwrap(),
            batch_id: None,
        })
        .await
        .unwrap();
    assert!(
        !stale.success,
        "the crashed worker's fencing token must be rejected after recovery"
    );

    // Restart the stopped parent Raft leader from its original durable storage.
    if let Some(index) = leader_index {
        let raft_dir = root
            .path()
            .join(format!("parent-{}/raft", PARENT_NODE_IDS[index]));
        let (restarted, server) = start_parent_node(
            PARENT_NODE_IDS[index],
            &parent_addresses[index],
            parent_dbs[index].clone(),
            &raft_dir,
        )
        .await;
        parent_nodes[index] = restarted;
        parent_servers[index] = server;
    }
    for store in &parent_stores {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let task = store.get_task(&task_id).await.unwrap();
                let shard = store.get_shard(&shard_id).await.unwrap();
                if task.as_ref().is_some_and(|task| {
                    task.metadata
                        .get("_dataspool_cdc_cutover_committed")
                        .is_some_and(|value| value == "true")
                }) && shard.as_ref().is_some_and(|shard| {
                    matches!(shard.state, dataspool::types::ShardState::Completed { .. })
                        && shard.offset
                            == OffsetValue::Custom(
                                json!({"driver":"elasticsearch_cdc","last_sequence":1}),
                            )
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("Raft member did not converge on recovered CDC checkpoint and cutover");
    }
    let target_doc: Value = http
        .get(format!("{target}/{target_index}/_doc/cdc-doc-1"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let source_doc: Value = http
        .get(format!("{source}/{source_index}/_doc/cdc-doc-1"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        target_doc["_source"]["value"],
        source_doc["_source"]["value"]
    );
    assert_eq!(
        target_doc["_source"]["effect_count"], 1,
        "CDC replay must not duplicate the business effect"
    );
    assert_eq!(
        target_doc["_source"]["applied_events"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let alias_state: Value = http
        .get(format!("{target}/_alias/{alias}"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(alias_state.as_object().unwrap().len(), 1);
    assert!(alias_state.get(&target_index).is_some());

    for (index, node) in parent_nodes.iter().enumerate() {
        let _ = node.shutdown().await;
        if parent_servers[index].is_finished() {
            continue;
        }
        parent_servers[index].abort();
    }
    for (url, index) in [
        (&source, &source_index),
        (&source, &outbox),
        (&target, &target_index),
        (&target, &old_index),
    ] {
        let _ = http.delete(format!("{url}/{index}")).send().await;
    }
}
