// SPDX-License-Identifier: AGPL-3.0-or-later
//! Live PostgreSQL tests for transactional source writes and durable CDC barrier.

use dataspool::drivers::document::CdcWriteBarrier;
use dataspool::drivers::relational::{
    PostgresOutboxConfig, PostgresOutboxSource, PostgresTransactionalOutbox,
};
use dataspool::pipeline::{OffsetValue, Source};
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("DATASPOOL_TEST_POSTGRES_URL")
        .expect("DATASPOOL_TEST_POSTGRES_URL must point to the CI PostgreSQL service")
}

async fn producer(stream: &str) -> PostgresTransactionalOutbox {
    let outbox = PostgresTransactionalOutbox::connect(&database_url(), stream)
        .await
        .unwrap();
    outbox.migrate().await.unwrap();
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS dataspool_outbox_test_documents (\
            stream_key TEXT NOT NULL, document_id TEXT NOT NULL, payload JSONB NOT NULL,\
            PRIMARY KEY (stream_key, document_id)\
        )",
    )
    .execute(outbox.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM dataspool_outbox_test_documents WHERE stream_key = $1")
        .bind(stream)
        .execute(outbox.pool())
        .await
        .unwrap();
    outbox
}

#[tokio::test]
async fn postgres_outbox_is_atomic_ordered_and_barrier_survives_restart() {
    let stream = format!("outbox-integration-{}", Uuid::new_v4());
    let outbox = producer(&stream).await;

    // Source mutation and Outbox event share one transaction.
    let mut writer = outbox.begin_write().await.unwrap();
    sqlx::query("INSERT INTO dataspool_outbox_test_documents (stream_key, document_id, payload) VALUES ($1, $2, $3)")
        .bind(&stream).bind("doc-1").bind(json!({"value":1}))
        .execute(writer.connection()).await.unwrap();
    let first_event_id = Uuid::new_v4();
    let first_sequence = writer
        .append_event(
            first_event_id,
            "index",
            "doc-1",
            "target-index",
            &json!({"value":1}),
        )
        .await
        .unwrap();
    writer.commit().await.unwrap();

    // An ambiguous producer retry with the same event ID and payload is
    // idempotent: it returns the original sequence and does not append twice.
    let mut retry = outbox.begin_write().await.unwrap();
    sqlx::query("UPDATE dataspool_outbox_test_documents SET payload = $1 WHERE stream_key = $2 AND document_id = $3")
        .bind(json!({"value":1})).bind(&stream).bind("doc-1")
        .execute(retry.connection()).await.unwrap();
    let replay_sequence = retry
        .append_event(
            first_event_id,
            "index",
            "doc-1",
            "target-index",
            &json!({"value":1}),
        )
        .await
        .unwrap();
    assert_eq!(replay_sequence, first_sequence);
    retry.commit().await.unwrap();
    assert_eq!(outbox.watermark().await.unwrap(), first_sequence);

    // Dropping an uncommitted producer transaction rolls back both sides.
    {
        let mut abandoned = outbox.begin_write().await.unwrap();
        sqlx::query("INSERT INTO dataspool_outbox_test_documents (stream_key, document_id, payload) VALUES ($1, $2, $3)")
            .bind(&stream).bind("rolled-back").bind(json!({"value":2}))
            .execute(abandoned.connection()).await.unwrap();
        abandoned
            .append_event(
                Uuid::new_v4(),
                "index",
                "rolled-back",
                "target-index",
                &json!({"value":2}),
            )
            .await
            .unwrap();
    }
    let missing: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM dataspool_outbox_test_documents WHERE stream_key = $1 AND document_id = 'rolled-back'")
        .bind(&stream).fetch_one(outbox.pool()).await.unwrap();
    assert_eq!(missing.0, 0);
    let mut no_event = outbox.begin_write().await.unwrap();
    sqlx::query("INSERT INTO dataspool_outbox_test_documents (stream_key, document_id, payload) VALUES ($1, $2, $3)")
        .bind(&stream).bind("missing-event").bind(json!({"value":7}))
        .execute(no_event.connection()).await.unwrap();
    assert!(
        no_event.commit().await.is_err(),
        "source mutation without an Outbox event must be rolled back"
    );
    let missing_event: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM dataspool_outbox_test_documents WHERE stream_key = $1 AND document_id = 'missing-event'")
        .bind(&stream).fetch_one(outbox.pool()).await.unwrap();
    assert_eq!(missing_event.0, 0);
    assert_eq!(
        outbox
            .read_after(first_sequence as i64, 100)
            .await
            .unwrap()
            .len(),
        0
    );
    let mut cdc_source = PostgresOutboxSource::new(PostgresOutboxConfig {
        connection_env: "DATASPOOL_TEST_POSTGRES_URL".into(),
        stream_key: stream.clone(),
    });
    cdc_source.connect().await.unwrap();
    let (batch, checkpoint) = cdc_source
        .read_batch(&OffsetValue::initial(), 100)
        .await
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.records()[0].get_metadata("_op"), Some("index"));
    assert!(batch.records()[0].get_metadata("_cdc_event_id").is_some());
    assert_eq!(
        checkpoint,
        OffsetValue::Custom(json!({"driver":"postgres_outbox","last_sequence":first_sequence}))
    );
    cdc_source.close().await.unwrap();

    // A barrier cannot pass an in-flight producer transaction; once it does,
    // it captures the final committed watermark and rejects all new writes.
    let mut in_flight = outbox.begin_write().await.unwrap();
    sqlx::query("INSERT INTO dataspool_outbox_test_documents (stream_key, document_id, payload) VALUES ($1, $2, $3)")
        .bind(&stream).bind("doc-2").bind(json!({"value":2}))
        .execute(in_flight.connection()).await.unwrap();
    let in_flight_sequence = in_flight
        .append_event(
            Uuid::new_v4(),
            "index",
            "doc-2",
            "target-index",
            &json!({"value":2}),
        )
        .await
        .unwrap();
    let barrier_outbox = outbox.clone();
    let barrier_task = tokio::spawn(async move {
        let mut barrier = barrier_outbox;
        barrier.pause_and_capture_final_watermark().await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !barrier_task.is_finished(),
        "cutover barrier must wait for the in-flight source transaction"
    );
    in_flight.commit().await.unwrap();
    let final_watermark = tokio::time::timeout(Duration::from_secs(5), barrier_task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_watermark, in_flight_sequence);
    assert_eq!(
        in_flight_sequence,
        first_sequence + 1,
        "rolled-back Outbox writes must not consume a stream sequence"
    );

    // The pause is persisted in PostgreSQL, not held in process memory.
    let restarted = PostgresTransactionalOutbox::connect(&database_url(), &stream)
        .await
        .unwrap();
    assert!(restarted.is_paused().await.unwrap());
    assert!(
        restarted.begin_write().await.is_err(),
        "writes must remain blocked while the durable gate is paused"
    );
    let events = restarted
        .read_after(first_sequence as i64 - 1, 100)
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );
    assert_eq!(events.last().unwrap().sequence as u64, final_watermark);

    let mut resumed = restarted.clone();
    resumed.resume_writes().await.unwrap();
    assert!(!resumed.is_paused().await.unwrap());
    let mut resumed_writer = resumed.begin_write().await.unwrap();
    sqlx::query("INSERT INTO dataspool_outbox_test_documents (stream_key, document_id, payload) VALUES ($1, $2, $3)")
        .bind(&stream).bind("doc-3").bind(json!({"value":3}))
        .execute(resumed_writer.connection()).await.unwrap();
    let third_sequence = resumed_writer
        .append_event(
            Uuid::new_v4(),
            "index",
            "doc-3",
            "target-index",
            &json!({"value":3}),
        )
        .await
        .unwrap();
    resumed_writer.commit().await.unwrap();
    assert!(third_sequence > final_watermark);

    // Concurrent producer transactions are serialized by the per-stream gate,
    // so sequence order is also commit order and cannot skip an earlier event.
    let mut tasks = Vec::new();
    for index in 0..8 {
        let producer = resumed.clone();
        let stream_key = stream.clone();
        tasks.push(tokio::spawn(async move {
            let id = format!("concurrent-{index}");
            let mut writer = producer.begin_write().await.unwrap();
            sqlx::query("INSERT INTO dataspool_outbox_test_documents (stream_key, document_id, payload) VALUES ($1, $2, $3)")
                .bind(&stream_key).bind(&id).bind(json!({"value":index}))
                .execute(writer.connection()).await.unwrap();
            let sequence = writer.append_event(Uuid::new_v4(), "index", &id, "target-index", &json!({"value":index})).await.unwrap();
            writer.commit().await.unwrap();
            sequence
        }));
    }
    let mut concurrent_sequences = Vec::new();
    for task in tasks {
        concurrent_sequences.push(task.await.unwrap());
    }
    concurrent_sequences.sort_unstable();
    assert!(
        concurrent_sequences
            .windows(2)
            .all(|pair| pair[0] < pair[1])
    );

    sqlx::query("DELETE FROM dataspool_outbox_test_documents WHERE stream_key = $1")
        .bind(&stream)
        .execute(outbox.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM dataspool_cdc_outbox WHERE stream_key = $1")
        .bind(&stream)
        .execute(outbox.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM dataspool_cdc_outbox_gate WHERE stream_key = $1")
        .bind(&stream)
        .execute(outbox.pool())
        .await
        .unwrap();
}
