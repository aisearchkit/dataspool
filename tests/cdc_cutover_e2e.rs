// SPDX-License-Identifier: AGPL-3.0-or-later
//! Live CDC catch-up and Raft-gated alias cutover across independent ES clusters.

use async_trait::async_trait;
use dataspool::agent::ShardClaimer;
use dataspool::drivers::document::{
    CdcConfig, CdcCutoverConfig, CdcRaftFence, CdcWriteBarrier, DocumentConfig,
    ElasticsearchCdcSource, cutover_with_raft, verify_source_target,
};
use dataspool::drivers::factory::{SinkConfig, SourceConfig};
use dataspool::pipeline::{OffsetValue, Source};
use dataspool::raft::{Command, RaftNode, default_raft_config};
use dataspool::storage::RocksDbStore;
use dataspool::task::{ShardMeta, TaskMeta, TaskStore};
use openraft::BasicNode;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::Arc;
use std::time::Duration;

struct TestWriteBarrier {
    watermark: u64,
    paused: bool,
}

#[async_trait]
impl CdcWriteBarrier for TestWriteBarrier {
    async fn pause_and_capture_final_watermark(&mut self) -> dataspool::Result<u64> {
        self.paused = true;
        Ok(self.watermark)
    }

    async fn resume_writes(&mut self) -> dataspool::Result<()> {
        self.paused = false;
        Ok(())
    }
}

fn required_url(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must point to an independent Elasticsearch cluster"))
        .trim_end_matches('/')
        .to_string()
}

async fn ensure_ok(response: reqwest::Response, context: &str) {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert!(status.is_success(), "{context}: HTTP {status}: {body}");
}

#[allow(clippy::too_many_arguments)]
async fn append_event(
    http: &reqwest::Client,
    source_url: &str,
    changes: &str,
    id: &str,
    sequence: u64,
    operation: &str,
    document_id: &str,
    payload: Value,
    target_index: &str,
) {
    let event = json!({
        "sequence": sequence,
        "operation": operation,
        "document_id": document_id,
        "target_index": target_index,
        "document": payload,
    });
    let body = format!(
        "{{\"index\":{{\"_id\":\"{id}\"}}}}\n{}\n",
        serde_json::to_string(&event).unwrap()
    );
    ensure_ok(
        http.post(format!("{source_url}/{changes}/_bulk"))
            .header("content-type", "application/x-ndjson")
            .body(body)
            .send()
            .await
            .unwrap(),
        "append CDC outbox event",
    )
    .await;
}

#[tokio::test]
async fn live_cdc_snapshot_catchup_and_raft_alias_cutover() {
    if std::env::var("DATASPOOL_CDC_ALIAS_CRASH_CHILD").as_deref() == Ok("1") {
        let target = std::env::var("DATASPOOL_CDC_ALIAS_TARGET").unwrap();
        let old = std::env::var("DATASPOOL_CDC_ALIAS_OLD").unwrap();
        let new = std::env::var("DATASPOOL_CDC_ALIAS_NEW").unwrap();
        let alias = std::env::var("DATASPOOL_CDC_ALIAS_NAME").unwrap();
        let response = reqwest::Client::new()
            .post(format!("{target}/_aliases"))
            .json(&json!({"actions":[{"remove":{"index":old,"alias":alias}},{"add":{"index":new,"alias":alias}}]}))
            .send().await.unwrap();
        ensure_ok(response, "child process atomic alias switch").await;
        std::fs::write(
            std::env::var("DATASPOOL_CDC_ALIAS_MARKER").unwrap(),
            b"alias-committed",
        )
        .unwrap();
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }

    let source_url = required_url("ES_CDC_SOURCE_URL");
    let target_url = required_url("ES_CDC_TARGET_URL");
    assert_ne!(
        source_url, target_url,
        "source and target must be independent clusters"
    );
    let http = reqwest::Client::new();
    let suffix = format!("{}", std::process::id());
    let source_index = format!("dataspool-cdc-source-{suffix}");
    let changes = format!("dataspool-cdc-outbox-{suffix}");
    let old_index = format!("dataspool-cdc-old-{suffix}");
    let new_index = format!("dataspool-cdc-new-{suffix}");
    let alias = format!("dataspool-cdc-live-{suffix}");

    for (url, index) in [
        (&source_url, &source_index),
        (&source_url, &changes),
        (&target_url, &old_index),
        (&target_url, &new_index),
    ] {
        let _ = http.delete(format!("{url}/{index}")).send().await;
        let request = http.put(format!("{url}/{index}"));
        let response = if index == &changes {
            request
                .json(&json!({"mappings":{"properties":{
                    "sequence":{"type":"long"},
                    "operation":{"type":"keyword"},
                    "document_id":{"type":"keyword"},
                    "target_index":{"type":"keyword"},
                    "routing":{"type":"keyword"},
                    "document":{"type":"object"}
                }}}))
                .send()
                .await
                .unwrap()
        } else {
            request.send().await.unwrap()
        };
        ensure_ok(response, "create index").await;
    }

    let initial = "{\"index\":{\"_id\":\"doc-1\"}}\n{\"value\":1}\n{\"index\":{\"_id\":\"doc-2\"}}\n{\"value\":2}\n{\"index\":{\"_id\":\"doc-3\"}}\n{\"value\":3}\n";
    ensure_ok(
        http.post(format!("{source_url}/{source_index}/_bulk"))
            .header("content-type", "application/x-ndjson")
            .body(initial)
            .send()
            .await
            .unwrap(),
        "seed source snapshot",
    )
    .await;
    ensure_ok(
        http.post(format!("{source_url}/{source_index}/_refresh"))
            .send()
            .await
            .unwrap(),
        "refresh snapshot",
    )
    .await;
    ensure_ok(
        http.post(format!("{source_url}/{changes}/_refresh"))
            .send()
            .await
            .unwrap(),
        "refresh empty outbox",
    )
    .await;

    // An empty outbox still needs an explicit numeric mapping: Elasticsearch
    // cannot sort an unmapped sequence field on the initial watermark query.
    ensure_ok(
        http.put(format!("{source_url}/{changes}/_mapping"))
            .json(&json!({"properties":{"sequence":{"type":"long"}}}))
            .send()
            .await
            .unwrap(),
        "map CDC sequence as long",
    )
    .await;

    // Capture the outbox watermark before opening the snapshot view.
    let mut cdc_config = DocumentConfig::local(changes.clone());
    cdc_config.urls = vec![source_url.clone()];
    let mut cdc = ElasticsearchCdcSource::new(
        cdc_config,
        CdcConfig {
            change_index: changes.clone(),
            ..CdcConfig::default()
        },
    );
    cdc.connect().await.unwrap();
    let snapshot_watermark = cdc.capture_watermark().await.unwrap();
    assert_eq!(snapshot_watermark, 0);

    // The producer continues mutating the source while the snapshot is copied.
    let producer_http = http.clone();
    let producer_url = source_url.clone();
    let producer_changes = changes.clone();
    let producer_source = source_index.clone();
    let producer_target_index = new_index.clone();
    let producer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        ensure_ok(
            producer_http
                .put(format!("{producer_url}/{producer_source}/_doc/doc-1"))
                .json(&json!({"value":101}))
                .send()
                .await
                .unwrap(),
            "mutate source document",
        )
        .await;
        append_event(
            &producer_http,
            &producer_url,
            &producer_changes,
            "evt-1",
            1,
            "index",
            "doc-1",
            json!({"value":101}),
            &producer_target_index,
        )
        .await;
        ensure_ok(
            producer_http
                .delete(format!("{producer_url}/{producer_source}/_doc/doc-2"))
                .send()
                .await
                .unwrap(),
            "delete source document",
        )
        .await;
        append_event(
            &producer_http,
            &producer_url,
            &producer_changes,
            "evt-2",
            2,
            "delete",
            "doc-2",
            json!({}),
            &producer_target_index,
        )
        .await;
        ensure_ok(
            producer_http
                .put(format!("{producer_url}/{producer_source}/_doc/doc-4"))
                .json(&json!({"value":404}))
                .send()
                .await
                .unwrap(),
            "create source document",
        )
        .await;
        append_event(
            &producer_http,
            &producer_url,
            &producer_changes,
            "evt-3",
            3,
            "index",
            "doc-4",
            json!({"value":404}),
            &producer_target_index,
        )
        .await;
        ensure_ok(
            producer_http
                .post(format!("{producer_url}/{producer_source}/_refresh"))
                .send()
                .await
                .unwrap(),
            "refresh mutated source",
        )
        .await;
        ensure_ok(
            producer_http
                .post(format!("{producer_url}/{producer_changes}/_refresh"))
                .send()
                .await
                .unwrap(),
            "refresh outbox",
        )
        .await;
    });

    let snapshot: Value = http
        .get(format!(
            "{source_url}/{source_index}/_search?size=100&sort=_doc"
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut snapshot_bulk = String::new();
    for hit in snapshot["hits"]["hits"].as_array().unwrap() {
        snapshot_bulk.push_str(
            &serde_json::to_string(&json!({"index":{"_index":new_index,"_id":hit["_id"]}}))
                .unwrap(),
        );
        snapshot_bulk.push('\n');
        snapshot_bulk.push_str(&serde_json::to_string(&hit["_source"]).unwrap());
        snapshot_bulk.push('\n');
    }
    ensure_ok(
        http.post(format!("{target_url}/_bulk"))
            .header("content-type", "application/x-ndjson")
            .body(snapshot_bulk)
            .send()
            .await
            .unwrap(),
        "copy snapshot to target",
    )
    .await;
    producer.await.unwrap();

    // Consume every post-snapshot outbox event, then apply index/delete operations.
    let (events, checkpoint) = cdc.read_batch(&OffsetValue::initial(), 100).await.unwrap();
    assert_eq!(
        events.len(),
        3,
        "all concurrent source mutations must be represented in the outbox"
    );
    let mut cdc_bulk = String::new();
    for record in events.records() {
        let id = record.id.as_deref().unwrap();
        let target = record.get_metadata("_target_index").unwrap();
        if record.get_metadata("_op") == Some("delete") {
            cdc_bulk.push_str(
                &serde_json::to_string(&json!({"delete":{"_index":target,"_id":id}})).unwrap(),
            );
            cdc_bulk.push('\n');
        } else {
            cdc_bulk.push_str(
                &serde_json::to_string(&json!({"index":{"_index":target,"_id":id}})).unwrap(),
            );
            cdc_bulk.push('\n');
            cdc_bulk.push_str(&serde_json::to_string(&record.payload).unwrap());
            cdc_bulk.push('\n');
        }
    }
    ensure_ok(
        http.post(format!("{target_url}/_bulk"))
            .header("content-type", "application/x-ndjson")
            .body(cdc_bulk)
            .send()
            .await
            .unwrap(),
        "apply CDC changes to target",
    )
    .await;
    ensure_ok(
        http.post(format!("{target_url}/{new_index}/_refresh"))
            .send()
            .await
            .unwrap(),
        "refresh CDC target",
    )
    .await;
    assert_eq!(
        checkpoint,
        OffsetValue::Custom(json!({"driver":"elasticsearch_cdc","last_sequence":3}))
    );
    cdc.close().await.unwrap();

    // Create a one-member Raft state machine and claim the online shard.
    let directory = tempfile::tempdir().unwrap();
    let db = RocksDbStore::open(directory.path().join("app")).unwrap();
    let store = Arc::new(TaskStore::new(db));
    let raft = RaftNode::new(
        1,
        directory.path().join("raft"),
        (*store).clone(),
        default_raft_config(),
        "cdc-cutover-raft-token-0123456789".into(),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = dataspool::raft_router(raft.handle(), "cdc-cutover-raft-token-0123456789".into());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    raft.initialize(BTreeMap::from([(1, BasicNode::new(address.to_string()))]))
        .await
        .unwrap();
    raft.wait_for_leader(Duration::from_secs(12)).await.unwrap();
    let mut online_source = DocumentConfig::local(&source_index);
    online_source.urls = vec![source_url.clone()];
    let mut change_connection = DocumentConfig::local(&changes);
    change_connection.urls = vec![source_url.clone()];
    let mut target_connection = DocumentConfig::local(&new_index);
    target_connection.urls = vec![target_url.clone()];
    let task = TaskMeta::new(
        "live-cdc-cutover-e2e".into(),
        serde_json::to_string(&SourceConfig::ElasticsearchOnline {
            snapshot: online_source,
            change_log_connection: change_connection,
            cdc: CdcConfig {
                change_index: changes.clone(),
                ..CdcConfig::default()
            },
        })
        .unwrap(),
        serde_json::to_string(&SinkConfig::Document {
            config: target_connection,
        })
        .unwrap(),
    );
    let task_id = task.task_id;
    let shard = ShardMeta::new(task_id, 0, "{}".into());
    let shard_id = shard.shard_id;
    let created = raft
        .propose(Command::CreateTask {
            request_id: format!("cdc-cutover-create:{task_id}"),
            actor: "cdc-e2e".into(),
            task: Box::new(task),
            shards: vec![shard],
        })
        .await
        .unwrap();
    assert!(created.success, "{}", created.message);
    let claimer = ShardClaimer::new(
        "cdc-cutover-worker".into(),
        store.clone(),
        Arc::new(raft.clone()),
    )
    .with_lease_seconds(30)
    .unwrap();
    let lease = claimer.try_claim().await.unwrap().unwrap();
    let premature_complete = raft
        .propose(Command::CompleteShard {
            shard_id: shard_id.to_string(),
            node_id: lease.node_id.to_string(),
            fencing_token: lease.fencing_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            total_records: 0,
        })
        .await
        .unwrap();
    assert!(
        !premature_complete.success,
        "online shard must not complete before Raft cutover record"
    );

    // Persist the CDC checkpoint in Raft before cutover; the barrier reads it back.
    let progress = raft
        .propose(Command::UpdateShardProgress {
            shard_id: shard_id.to_string(),
            node_id: lease.node_id.to_string(),
            fencing_token: lease.fencing_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            processed_records: 6,
            processed_bytes: 0,
            offset: serde_json::to_string(&checkpoint).unwrap(),
            batch_id: None,
        })
        .await
        .unwrap();
    assert!(progress.success, "{}", progress.message);
    let checkpoint_store = store.clone();
    let checkpoint_shard = shard_id;
    let applied_watermark = move || {
        let store = checkpoint_store.clone();
        async move {
            let shard = store.get_shard(&checkpoint_shard).await?.unwrap();
            match shard.offset {
                OffsetValue::Custom(value) => Ok(value["last_sequence"].as_u64().unwrap_or(0)),
                _ => Ok(0),
            }
        }
    };

    // The alias API may already have committed when a caller loses its response.
    // Pre-switch it to model that ambiguous network response, then run the
    // Raft-gated retry: alias inspection must make the retry idempotent.
    ensure_ok(
        http.post(format!("{target_url}/_aliases"))
            .json(&json!({"actions":[{"add":{"index":old_index,"alias":alias}}]}))
            .send()
            .await
            .unwrap(),
        "create old alias",
    )
    .await;
    // Kill a real child process immediately after Elasticsearch commits the
    // atomic alias update but before the parent can persist the Raft cutover.
    let crash_dir = tempfile::tempdir().unwrap();
    let marker = crash_dir.path().join("alias-committed.marker");
    let mut alias_worker = ProcessCommand::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "live_cdc_snapshot_catchup_and_raft_alias_cutover",
            "--nocapture",
        ])
        .env("DATASPOOL_CDC_ALIAS_CRASH_CHILD", "1")
        .env("DATASPOOL_CDC_ALIAS_TARGET", &target_url)
        .env("DATASPOOL_CDC_ALIAS_OLD", &old_index)
        .env("DATASPOOL_CDC_ALIAS_NEW", &new_index)
        .env("DATASPOOL_CDC_ALIAS_NAME", &alias)
        .env("DATASPOOL_CDC_ALIAS_MARKER", &marker)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !marker.exists() {
        if let Some(status) = alias_worker.try_wait().unwrap() {
            panic!("alias worker exited before commit marker: {status}");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "alias child did not commit in time"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    alias_worker
        .kill()
        .expect("SIGKILL alias worker after alias commit");
    let killed = alias_worker.wait().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            killed.signal(),
            Some(9),
            "alias worker must be killed by SIGKILL"
        );
    }
    let mut barrier = TestWriteBarrier {
        watermark: 3,
        paused: false,
    };
    let fence = CdcRaftFence {
        task_id: task_id.to_string(),
        shard_id: shard_id.to_string(),
        node_id: lease.node_id.to_string(),
        fencing_token: lease.fencing_token,
    };
    let cutover_config = CdcCutoverConfig {
        target_base_url: target_url.clone(),
        alias: alias.clone(),
        old_index: Some(old_index.clone()),
        new_index: new_index.clone(),
        timeout: Duration::from_secs(5),
    };
    let stale_fence = CdcRaftFence {
        fencing_token: lease.fencing_token.saturating_sub(1),
        ..fence.clone()
    };
    let stale_result = cutover_with_raft(
        &http,
        &raft,
        &cutover_config,
        &stale_fence,
        &mut barrier,
        || async { Ok(3) },
    )
    .await;
    assert!(
        stale_result.is_err(),
        "stale lease must not commit the cutover record"
    );
    let unavailable_config = CdcCutoverConfig {
        target_base_url: "http://127.0.0.1:1".into(),
        ..cutover_config.clone()
    };
    let network_interrupted = cutover_with_raft(
        &http,
        &raft,
        &unavailable_config,
        &fence,
        &mut barrier,
        || async { Ok(3) },
    )
    .await;
    assert!(
        network_interrupted.is_err(),
        "target network interruption must leave cutover uncommitted"
    );
    let result = cutover_with_raft(
        &http,
        &raft,
        &cutover_config,
        &fence,
        &mut barrier,
        applied_watermark,
    )
    .await
    .unwrap();
    assert_eq!(result.final_watermark, 3);
    assert_eq!(result.applied_watermark, 3);
    assert!(!barrier.paused, "producer barrier must always be released");
    let persisted_task = store.get_task(&task_id).await.unwrap().unwrap();
    assert_eq!(
        persisted_task
            .metadata
            .get("_dataspool_cdc_cutover_committed")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        persisted_task
            .metadata
            .get("_dataspool_cdc_final_watermark")
            .map(String::as_str),
        Some("3")
    );
    assert_eq!(
        persisted_task
            .metadata
            .get("_dataspool_cdc_applied_watermark")
            .map(String::as_str),
        Some("3")
    );

    let alias_state: Value = http
        .get(format!("{target_url}/_alias/{alias}"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(alias_state.as_object().unwrap().len(), 1);
    assert!(
        alias_state.get(&new_index).is_some(),
        "alias must point exclusively at the new index"
    );
    let source_config = {
        let mut config = DocumentConfig::local(&source_index);
        config.urls = vec![source_url.clone()];
        config
    };
    let target_config = {
        let mut config = DocumentConfig::local(&new_index);
        config.urls = vec![target_url.clone()];
        config
    };
    let report = verify_source_target(&source_config, &target_config, 2)
        .await
        .unwrap();
    assert!(
        report.is_consistent(),
        "source and target diverged after CDC catch-up: {report:?}"
    );
    assert_eq!(report.source.document_count, 3);

    let shard = store.get_shard(&shard_id).await.unwrap().unwrap();
    assert_eq!(shard.offset, checkpoint);
    let complete = raft
        .propose(Command::CompleteShard {
            shard_id: shard_id.to_string(),
            node_id: lease.node_id.to_string(),
            fencing_token: lease.fencing_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            total_records: 6,
        })
        .await
        .unwrap();
    assert!(complete.success, "{}", complete.message);
    raft.shutdown().await.unwrap();
    server.abort();

    // Best-effort cleanup; source and target are intentionally independent endpoints.
    for (url, index) in [
        (&source_url, &source_index),
        (&source_url, &changes),
        (&target_url, &old_index),
        (&target_url, &new_index),
    ] {
        let _ = http.delete(format!("{url}/{index}")).send().await;
    }
}
