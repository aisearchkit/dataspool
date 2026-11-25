// SPDX-License-Identifier: AGPL-3.0-or-later
//! PostgreSQL Transactional Outbox + real Raft cutover gate integration test.

use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use dataspool::agent::ShardClaimer;
use dataspool::drivers::document::{CdcCutoverConfig, CdcRaftFence, cutover_with_raft};
use dataspool::drivers::factory::{SinkConfig, SourceConfig};
use dataspool::drivers::relational::{PostgresOutboxConfig, PostgresTransactionalOutbox};
use dataspool::pipeline::OffsetValue;
use dataspool::raft::{Command, RaftNode, default_raft_config};
use dataspool::storage::RocksDbStore;
use dataspool::task::{ShardMeta, TaskMeta, TaskStore};
use openraft::BasicNode;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn postgres_transactional_outbox_drives_raft_gated_cutover() {
    let database_url =
        std::env::var("DATASPOOL_TEST_POSTGRES_URL").expect("PostgreSQL CI service URL");
    let stream_key = format!("postgres-cutover-{}", Uuid::new_v4());
    let outbox = PostgresTransactionalOutbox::connect(&database_url, &stream_key)
        .await
        .unwrap();
    outbox.migrate().await.unwrap();
    sqlx::query("CREATE TABLE IF NOT EXISTS dataspool_postgres_cutover_source (stream_key TEXT NOT NULL, document_id TEXT NOT NULL, payload JSONB NOT NULL, PRIMARY KEY(stream_key, document_id))")
        .execute(outbox.pool()).await.unwrap();
    let mut writer = outbox.begin_write().await.unwrap();
    sqlx::query("INSERT INTO dataspool_postgres_cutover_source(stream_key, document_id, payload) VALUES ($1, $2, $3)")
        .bind(&stream_key).bind("doc-1").bind(json!({"value":42}))
        .execute(writer.connection()).await.unwrap();
    let sequence = writer
        .append_event(
            Uuid::new_v4(),
            "index",
            "doc-1",
            "new-index",
            &json!({"value":42}),
        )
        .await
        .unwrap();
    writer.commit().await.unwrap();

    // The production pipeline source reads the same committed Outbox event.
    let mut source =
        dataspool::drivers::relational::PostgresOutboxSource::new(PostgresOutboxConfig {
            connection_env: "DATASPOOL_TEST_POSTGRES_URL".into(),
            stream_key: stream_key.clone(),
        });
    use dataspool::pipeline::Source;
    source.connect().await.unwrap();
    let (batch, checkpoint) = source
        .read_batch(&OffsetValue::initial(), 10)
        .await
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert!(batch.records()[0].get_metadata("_cdc_event_id").is_some());
    assert_eq!(batch.records()[0].get_metadata("_op"), Some("index"));
    assert_eq!(
        checkpoint,
        OffsetValue::Custom(json!({"driver":"postgres_outbox","last_sequence":sequence}))
    );
    source.close().await.unwrap();

    let root = tempfile::tempdir().unwrap();
    let db = RocksDbStore::open(root.path().join("app")).unwrap();
    let store = Arc::new(TaskStore::new(db));
    let raft = RaftNode::new(
        71,
        root.path().join("raft"),
        (*store).clone(),
        default_raft_config(),
        "postgres-cutover-raft-token-0123456789".into(),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = dataspool::raft_router(
        raft.handle(),
        "postgres-cutover-raft-token-0123456789".into(),
    );
    let raft_server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    raft.initialize(BTreeMap::from([(71, BasicNode::new(address.to_string()))]))
        .await
        .unwrap();
    raft.wait_for_leader(Duration::from_secs(15)).await.unwrap();

    let task = TaskMeta::new(
        "postgres-transactional-outbox-cutover".into(),
        serde_json::to_string(&SourceConfig::PostgresOutbox {
            config: PostgresOutboxConfig {
                connection_env: "DATASPOOL_TEST_POSTGRES_URL".into(),
                stream_key: stream_key.clone(),
            },
        })
        .unwrap(),
        serde_json::to_string(&SinkConfig::Document {
            config: dataspool::drivers::document::DocumentConfig::local("new-index"),
        })
        .unwrap(),
    );
    let task_id = task.task_id;
    let shard = ShardMeta::new(task_id, 0, "{}".into());
    let shard_id = shard.shard_id;
    let created = raft
        .propose(Command::CreateTask {
            request_id: format!("postgres-outbox-create:{task_id}"),
            actor: "postgres-outbox-e2e".into(),
            task: Box::new(task),
            shards: vec![shard],
        })
        .await
        .unwrap();
    assert!(created.success, "{}", created.message);
    let claimer = ShardClaimer::new(
        "postgres-outbox-worker".into(),
        store.clone(),
        Arc::new(raft.clone()),
    )
    .with_lease_seconds(30)
    .unwrap();
    let lease = claimer.try_claim().await.unwrap().unwrap();
    let progress = raft
        .propose(Command::UpdateShardProgress {
            shard_id: shard_id.to_string(),
            node_id: lease.node_id.to_string(),
            fencing_token: lease.fencing_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            processed_records: 1,
            processed_bytes: 0,
            offset: serde_json::to_string(&checkpoint).unwrap(),
            batch_id: None,
        })
        .await
        .unwrap();
    assert!(progress.success, "{}", progress.message);

    // Minimal HTTP endpoint models the target's atomic alias API; Raft and the
    // PostgreSQL producer barrier are real implementations in this test.
    let alias_app = Router::new()
        .route("/_alias/live", get(|| async { StatusCode::NOT_FOUND }))
        .route(
            "/_aliases",
            post(|| async { Json(json!({"acknowledged":true})) }),
        );
    let alias_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let alias_url = format!("http://{}", alias_listener.local_addr().unwrap());
    let alias_server =
        tokio::spawn(async move { axum::serve(alias_listener, alias_app).await.unwrap() });
    let config = CdcCutoverConfig {
        target_base_url: alias_url,
        alias: "live".into(),
        old_index: Some("old-index".into()),
        new_index: "new-index".into(),
        timeout: Duration::from_secs(3),
    };
    let fence = CdcRaftFence {
        task_id: task_id.to_string(),
        shard_id: shard_id.to_string(),
        node_id: lease.node_id.to_string(),
        fencing_token: lease.fencing_token,
    };
    let mut barrier = outbox.clone();
    let stale_fence = CdcRaftFence {
        fencing_token: lease.fencing_token.saturating_sub(1),
        ..fence.clone()
    };
    let stale_result = cutover_with_raft(
        &reqwest::Client::new(),
        &raft,
        &config,
        &stale_fence,
        &mut barrier,
        || async { Ok(sequence) },
    )
    .await;
    assert!(
        stale_result.is_err(),
        "stale fencing generation must not commit the cutover"
    );
    assert!(
        barrier.is_paused().await.unwrap(),
        "producer gate must stay closed after alias success but Raft rejection"
    );
    let applied_store = store.clone();
    let applied_shard = shard_id;
    let result = cutover_with_raft(
        &reqwest::Client::new(),
        &raft,
        &config,
        &fence,
        &mut barrier,
        move || {
            let store = applied_store.clone();
            async move {
                let shard = store.get_shard(&applied_shard).await?.unwrap();
                match shard.offset {
                    OffsetValue::Custom(value) => Ok(value["last_sequence"].as_u64().unwrap_or(0)),
                    _ => Ok(0),
                }
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(result.final_watermark, sequence);
    assert_eq!(result.applied_watermark, sequence);
    assert!(
        !barrier.is_paused().await.unwrap(),
        "writes resume only after Raft cutover is committed"
    );
    let persisted = store.get_task(&task_id).await.unwrap().unwrap();
    assert_eq!(
        persisted
            .metadata
            .get("_dataspool_cdc_cutover_committed")
            .map(String::as_str),
        Some("true")
    );
    let completed = raft
        .propose(Command::CompleteShard {
            shard_id: shard_id.to_string(),
            node_id: lease.node_id.to_string(),
            fencing_token: lease.fencing_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
            total_records: 1,
        })
        .await
        .unwrap();
    assert!(completed.success, "{}", completed.message);

    alias_server.abort();
    raft.shutdown().await.unwrap();
    raft_server.abort();
    sqlx::query("DELETE FROM dataspool_postgres_cutover_source WHERE stream_key = $1")
        .bind(&stream_key)
        .execute(outbox.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM dataspool_cdc_outbox WHERE stream_key = $1")
        .bind(&stream_key)
        .execute(outbox.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM dataspool_cdc_outbox_gate WHERE stream_key = $1")
        .bind(&stream_key)
        .execute(outbox.pool())
        .await
        .unwrap();
}
