// SPDX-License-Identifier: AGPL-3.0-or-later
//! Live PostgreSQL tests for transactional source writes and durable CDC barrier.

use dataspool::drivers::document::CdcWriteBarrier;
use dataspool::drivers::relational::{
    PostgresOutboxConfig, PostgresOutboxSource, PostgresTransactionalOutbox,
};
use dataspool::pipeline::{OffsetValue, Source};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};
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

    // Kill a backend while a producer transaction is open. The failed
    // transaction must not leak a source mutation or Outbox event, and the
    // pool must establish a fresh connection for the next write.
    let mut disconnected = outbox.begin_write().await.unwrap();
    let backend_pid: (i32,) = sqlx::query_as("SELECT pg_backend_pid()")
        .fetch_one(disconnected.connection())
        .await
        .unwrap();
    let terminated: (bool,) = sqlx::query_as("SELECT pg_terminate_backend($1)")
        .bind(backend_pid.0)
        .fetch_one(outbox.pool())
        .await
        .unwrap();
    assert!(
        terminated.0,
        "test must terminate the active producer backend"
    );
    assert!(
        sqlx::query("SELECT 1")
            .execute(disconnected.connection())
            .await
            .is_err()
    );
    drop(disconnected);
    let mut after_disconnect = outbox.begin_write().await.unwrap();
    let recovery_sequence = after_disconnect
        .append_event(
            Uuid::new_v4(),
            "index",
            "after-disconnect",
            "target-index",
            &json!({"recovered":true}),
        )
        .await
        .unwrap();
    after_disconnect.commit().await.unwrap();

    assert_eq!(
        outbox
            .read_after(first_sequence as i64, 100)
            .await
            .unwrap()
            .len(),
        1
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
    assert_eq!(batch.len(), 2);
    assert_eq!(batch.records()[0].get_metadata("_op"), Some("index"));
    assert!(batch.records()[0].get_metadata("_cdc_event_id").is_some());
    assert_eq!(
        checkpoint,
        OffsetValue::Custom(json!({"driver":"postgres_outbox","last_sequence":recovery_sequence}))
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
        recovery_sequence + 1,
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
    assert_eq!(events.len(), 3);
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

    // Database trigger guard rejects direct source-table writes that bypass
    // the producer API, while writes through begin_write are accepted.
    sqlx::query("CREATE TABLE IF NOT EXISTS dataspool_outbox_guard_test (stream_key TEXT NOT NULL, document_id TEXT PRIMARY KEY, payload JSONB NOT NULL)")
        .execute(outbox.pool()).await.unwrap();
    outbox
        .install_write_guard("public", "dataspool_outbox_guard_test")
        .await
        .unwrap();
    sqlx::raw_sql("DROP ROLE IF EXISTS dataspool_outbox_app_test; CREATE ROLE dataspool_outbox_app_test LOGIN; GRANT SELECT, INSERT, UPDATE, DELETE ON public.dataspool_outbox_guard_test TO dataspool_outbox_app_test;")
        .execute(outbox.pool()).await.unwrap();
    outbox
        .restrict_application_dml(
            "public",
            "dataspool_outbox_guard_test",
            "dataspool_outbox_app_test",
        )
        .await
        .unwrap();
    let privileges: (bool, bool) = sqlx::query_as("SELECT has_table_privilege('dataspool_outbox_app_test', 'public.dataspool_outbox_guard_test', 'SELECT'), has_table_privilege('dataspool_outbox_app_test', 'public.dataspool_outbox_guard_test', 'INSERT')")
        .fetch_one(outbox.pool()).await.unwrap();
    assert!(privileges.0, "application role retains read access");
    assert!(
        !privileges.1,
        "application role must lose direct INSERT privilege"
    );
    let mut app_transaction = outbox.pool().begin().await.unwrap();
    sqlx::query("SET LOCAL ROLE dataspool_outbox_app_test")
        .execute(&mut *app_transaction)
        .await
        .unwrap();
    let app_write = sqlx::query("INSERT INTO dataspool_outbox_guard_test(stream_key, document_id, payload) VALUES ($1, 'role-bypass', '{}'::jsonb)")
        .bind(&stream).execute(&mut *app_transaction).await;
    assert!(
        app_write.is_err(),
        "restricted application role cannot bypass the writer service"
    );
    app_transaction.rollback().await.unwrap();

    let direct_write = sqlx::query("INSERT INTO dataspool_outbox_guard_test(stream_key, document_id, payload) VALUES ($1, 'bypass', '{}'::jsonb)")
        .bind(&stream).execute(outbox.pool()).await;
    assert!(
        direct_write.is_err(),
        "direct DML must be rejected by the installed trigger"
    );
    let mut guarded = outbox.begin_write().await.unwrap();
    sqlx::query("INSERT INTO dataspool_outbox_guard_test(stream_key, document_id, payload) VALUES ($1, 'guarded', '{}'::jsonb)")
        .bind(&stream).execute(guarded.connection()).await.unwrap();
    guarded
        .append_event(
            Uuid::new_v4(),
            "index",
            "guarded",
            "target-index",
            &json!({}),
        )
        .await
        .unwrap();
    guarded.commit().await.unwrap();

    // Concurrent producer transactions are serialized by the per-stream gate.
    // Record real throughput and latency instead of asserting a machine-specific
    // performance threshold; the output is the CI baseline for capacity work.
    let deadlocks_before: (i64,) =
        sqlx::query_as("SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()")
            .fetch_one(outbox.pool())
            .await
            .unwrap();
    let monitor_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .unwrap();
    let max_lock_wait_sessions = Arc::new(AtomicI64::new(0));
    let monitor_max = max_lock_wait_sessions.clone();
    let monitor = tokio::spawn(async move {
        for _ in 0..120 {
            let (waiting,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'")
                .fetch_one(&monitor_pool).await.unwrap();
            monitor_max.fetch_max(waiting, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        monitor_pool.close().await;
    });
    // Eight independent application pools emulate multiple producer
    // processes; each is capped at four PostgreSQL connections.
    let mut application_instances = Vec::new();
    for _ in 0..8 {
        application_instances.push(
            PostgresTransactionalOutbox::connect_with_max_connections(&database_url(), &stream, 4)
                .await
                .unwrap(),
        );
    }
    let started = std::time::Instant::now();
    let mut tasks = Vec::new();
    for index in 0..32 {
        let producer = application_instances[index % application_instances.len()].clone();
        let stream_key = stream.clone();
        tasks.push(tokio::spawn(async move {
            let id = format!("concurrent-{index}");
            let transaction_started = std::time::Instant::now();
            let mut writer = producer.begin_write().await.unwrap();
            sqlx::query("SELECT pg_sleep(0.005)").execute(writer.connection()).await.unwrap();
            sqlx::query("INSERT INTO dataspool_outbox_test_documents (stream_key, document_id, payload) VALUES ($1, $2, $3)")
                .bind(&stream_key).bind(&id).bind(json!({"value":index}))
                .execute(writer.connection()).await.unwrap();
            let sequence = writer.append_event(Uuid::new_v4(), "index", &id, "target-index", &json!({"value":index})).await.unwrap();
            writer.commit().await.unwrap();
            (sequence, transaction_started.elapsed())
        }));
    }
    let mut concurrent_sequences = Vec::new();
    let mut transaction_latencies = Vec::new();
    for task in tasks {
        let (sequence, latency) = task.await.unwrap();
        concurrent_sequences.push(sequence);
        transaction_latencies.push(latency);
    }
    concurrent_sequences.sort_unstable();
    assert!(
        concurrent_sequences
            .windows(2)
            .all(|pair| pair[0] < pair[1])
    );
    monitor.await.unwrap();
    transaction_latencies.sort_unstable();
    let percentile = |percent: usize| {
        transaction_latencies
            [(transaction_latencies.len() * percent / 100).min(transaction_latencies.len() - 1)]
    };
    let p50 = percentile(50);
    let p95 = percentile(95);
    let p99 = percentile(99);
    let elapsed = started.elapsed();
    let deadlocks_after: (i64,) =
        sqlx::query_as("SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()")
            .fetch_one(outbox.pool())
            .await
            .unwrap();
    let max_lock_waits = max_lock_wait_sessions.load(Ordering::Relaxed);
    println!(
        "POSTGRES_OUTBOX_LOAD stream={} writers=32 elapsed_ms={} throughput_ops_s={:.1} p50_transaction_ms={} p95_transaction_ms={} p99_transaction_ms={} deadlocks_delta={} max_sampled_lock_wait_sessions={}",
        stream,
        elapsed.as_millis(),
        32.0 / elapsed.as_secs_f64(),
        p50.as_millis(),
        p95.as_millis(),
        p99.as_millis(),
        deadlocks_after.0 - deadlocks_before.0,
        max_lock_waits
    );

    // Compare four independent stream gates using the same eight app pools.
    // This shows whether sharding unrelated workloads across stream_key values
    // removes the single-row serialization ceiling.
    let partition_keys = (0..4)
        .map(|partition| format!("{stream}-partition-{partition}"))
        .collect::<Vec<_>>();
    let partitioned = application_instances
        .iter()
        .map(|app| {
            partition_keys
                .iter()
                .map(|key| PostgresTransactionalOutbox::new(app.pool().clone(), key.clone()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    for producer in &partitioned[0] {
        producer.migrate().await.unwrap();
    }
    let partition_started = std::time::Instant::now();
    let mut partition_tasks = Vec::new();
    for index in 0..32 {
        let producer = partitioned[index % 8][index % 4].clone();
        partition_tasks.push(tokio::spawn(async move {
            let id = format!("partitioned-{index}");
            let started = std::time::Instant::now();
            let mut writer = producer.begin_write().await.unwrap();
            sqlx::query("SELECT pg_sleep(0.005)")
                .execute(writer.connection())
                .await
                .unwrap();
            writer
                .append_event(
                    Uuid::new_v4(),
                    "index",
                    &id,
                    "target-index",
                    &json!({"value":index}),
                )
                .await
                .unwrap();
            writer.commit().await.unwrap();
            started.elapsed()
        }));
    }
    let mut partition_latencies = Vec::new();
    for task in partition_tasks {
        partition_latencies.push(task.await.unwrap());
    }
    partition_latencies.sort_unstable();
    let partition_elapsed = partition_started.elapsed();
    println!(
        "POSTGRES_OUTBOX_PARTITIONED streams=4 app_instances=8 writers=32 elapsed_ms={} throughput_ops_s={:.1} p50_transaction_ms={} p95_transaction_ms={} p99_transaction_ms={}",
        partition_elapsed.as_millis(),
        32.0 / partition_elapsed.as_secs_f64(),
        partition_latencies[partition_latencies.len() / 2].as_millis(),
        partition_latencies
            [(partition_latencies.len() * 95 / 100).min(partition_latencies.len() - 1)]
        .as_millis(),
        partition_latencies
            [(partition_latencies.len() * 99 / 100).min(partition_latencies.len() - 1)]
        .as_millis(),
    );
    for key in &partition_keys {
        sqlx::query("DELETE FROM dataspool_cdc_outbox WHERE stream_key = $1")
            .bind(key)
            .execute(outbox.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM dataspool_cdc_outbox_gate WHERE stream_key = $1")
            .bind(key)
            .execute(outbox.pool())
            .await
            .unwrap();
    }

    // Retention is coordinated with all registered consumers. No consumer
    // checkpoint may be skipped, and a lagging consumer pins the safe watermark.
    let consumer_a = format!("consumer-a-{stream}");
    let consumer_b = format!("consumer-b-{stream}");
    outbox.register_consumer(&consumer_a).await.unwrap();
    outbox.register_consumer(&consumer_b).await.unwrap();
    assert_eq!(
        outbox.prune_consumed_events().await.unwrap(),
        0,
        "unstarted consumer pins the log at zero"
    );
    let highest = outbox.watermark().await.unwrap();
    assert!(
        outbox
            .record_consumer_checkpoint(&consumer_a, highest + 1)
            .await
            .is_err(),
        "a consumer cannot checkpoint beyond the committed stream watermark"
    );
    outbox
        .record_consumer_checkpoint(&consumer_a, highest)
        .await
        .unwrap();
    outbox
        .record_consumer_checkpoint(&consumer_b, 5)
        .await
        .unwrap();
    assert!(
        outbox
            .record_consumer_checkpoint(&consumer_b, 4)
            .await
            .is_err(),
        "consumer checkpoints must be monotonic"
    );
    let pruned = outbox.prune_consumed_events().await.unwrap();
    assert_eq!(
        pruned, 5,
        "only events through the slowest consumer checkpoint can be pruned"
    );
    assert_eq!(
        outbox.watermark().await.unwrap(),
        highest,
        "pruning must not rewind the durable sequence watermark"
    );
    let remaining = outbox.read_after(0, 100).await.unwrap();
    assert!(remaining.iter().all(|event| event.sequence > 5));
    outbox
        .record_consumer_checkpoint(&consumer_b, highest)
        .await
        .unwrap();
    let remaining_pruned = outbox.prune_consumed_events().await.unwrap();
    assert_eq!(remaining_pruned, highest as u64 - 5);
    assert!(outbox.read_after(0, 100).await.unwrap().is_empty());

    sqlx::query("DELETE FROM dataspool_cdc_outbox_consumers WHERE stream_key = $1")
        .bind(&stream)
        .execute(outbox.pool())
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER IF EXISTS dataspool_outbox_guard_dataspool_outbox_guard_test ON public.dataspool_outbox_guard_test")
        .execute(outbox.pool()).await.unwrap();
    sqlx::query("DELETE FROM dataspool_outbox_guard_test WHERE document_id = 'guarded'")
        .execute(outbox.pool())
        .await
        .unwrap();
    sqlx::raw_sql("DROP OWNED BY dataspool_outbox_app_test; DROP ROLE dataspool_outbox_app_test;")
        .execute(outbox.pool())
        .await
        .unwrap();
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
