// SPDX-License-Identifier: AGPL-3.0-or-later
//! Live PostgreSQL tests for transactional source writes and durable CDC barrier.

use dataspool::drivers::document::CdcWriteBarrier;
use dataspool::drivers::factory::{SourceConfig, create_source};
use dataspool::drivers::relational::{
    PartitionedCdcWriteBarrier, PostgresOutboxConfig, PostgresOutboxEventInput,
    PostgresOutboxSource, PostgresPartitionedOutbox, PostgresPartitionedOutboxConfig,
    PostgresTransactionalOutbox,
};
use dataspool::pipeline::{OffsetValue, Source};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
};
use std::time::Duration;
use uuid::Uuid;

static POSTGRES_OUTBOX_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn database_url() -> String {
    std::env::var("DATASPOOL_TEST_POSTGRES_URL")
        .expect("DATASPOOL_TEST_POSTGRES_URL must point to the CI PostgreSQL service")
}

async fn producer(stream: &str) -> PostgresTransactionalOutbox {
    // Multiple integration tests share one PostgreSQL service and can start
    // concurrently. Serialize idempotent DDL setup because PostgreSQL's
    // CREATE TABLE IF NOT EXISTS is not a concurrency-safe migration lock.
    static MIGRATE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _migration_guard = MIGRATE_LOCK.lock().await;
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
    let _test_guard = POSTGRES_OUTBOX_TEST_LOCK.lock().await;
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
    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(outbox.pool())
        .await
        .unwrap();
    let wal_before_32: (i64,) = sqlx::query_as("SELECT wal_bytes::bigint FROM pg_stat_wal")
        .fetch_one(outbox.pool())
        .await
        .unwrap();
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
    let baseline_active_peak = Arc::new(AtomicI64::new(0));
    let baseline_active = baseline_active_peak.clone();
    let monitor = tokio::spawn(async move {
        for _ in 0..120 {
            let (waiting,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'")
                .fetch_one(&monitor_pool).await.unwrap();
            monitor_max.fetch_max(waiting, Ordering::Relaxed);
            let (active,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = 'dataspool-outbox-load-32' AND state = 'active'")
                .fetch_one(&monitor_pool).await.unwrap();
            baseline_active.fetch_max(active, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        monitor_pool.close().await;
    });
    // Eight independent application pools emulate multiple producer
    // processes; each is capped at four PostgreSQL connections.
    let mut baseline_url = url::Url::parse(&database_url()).unwrap();
    baseline_url
        .query_pairs_mut()
        .append_pair("application_name", "dataspool-outbox-load-32");
    let mut application_instances = Vec::new();
    for _ in 0..8 {
        application_instances.push(
            PostgresTransactionalOutbox::connect_with_max_connections(
                baseline_url.as_str(),
                &stream,
                4,
            )
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
    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(outbox.pool())
        .await
        .unwrap();
    let wal_after_32: (i64,) = sqlx::query_as("SELECT wal_bytes::bigint FROM pg_stat_wal")
        .fetch_one(outbox.pool())
        .await
        .unwrap();
    let deadlocks_after: (i64,) =
        sqlx::query_as("SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()")
            .fetch_one(outbox.pool())
            .await
            .unwrap();
    let max_lock_waits = max_lock_wait_sessions.load(Ordering::Relaxed);
    println!(
        "POSTGRES_OUTBOX_LOAD stream={} producer_tasks=32 pool_connections=32 active_connections_peak={} elapsed_ms={} throughput_ops_s={:.1} p50_transaction_ms={} p95_transaction_ms={} p99_transaction_ms={} deadlocks_delta={} max_sampled_lock_wait_sessions={} wal_bytes_delta={}",
        stream,
        baseline_active_peak.load(Ordering::Relaxed),
        elapsed.as_millis(),
        32.0 / elapsed.as_secs_f64(),
        p50.as_millis(),
        p95.as_millis(),
        p99.as_millis(),
        deadlocks_after.0 - deadlocks_before.0,
        max_lock_waits,
        (wal_after_32.0 - wal_before_32.0).max(0)
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

    // Release the initial 8 x 4 pools. The next matrix separates producer
    // task concurrency from actual PostgreSQL connection concurrency.
    for app in &application_instances {
        app.pool().close().await;
    }

    // Increase database concurrency independently from producer task count:
    // 128/256/512 tasks use 16/32/64 actual pool connections respectively.
    // This remains within the stock PostgreSQL service connection budget while
    // exposing pool queueing separately from gate-lock contention.
    for (concurrency, total_pool_connections) in [(128usize, 16usize), (256, 32), (512, 64)] {
        let per_pool_connections = (total_pool_connections / 8) as u32;
        let mut scale_instances = Vec::with_capacity(8);
        for _ in 0..8 {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(per_pool_connections)
                .after_connect(|connection, _| {
                    Box::pin(async move {
                        sqlx::query("SET application_name = 'dataspool-outbox-load'")
                            .execute(connection)
                            .await?;
                        Ok(())
                    })
                })
                .connect(&database_url())
                .await
                .unwrap();
            scale_instances.push(PostgresTransactionalOutbox::new(pool, stream.clone()));
        }
        sqlx::query("SELECT pg_stat_force_next_flush()")
            .execute(outbox.pool())
            .await
            .unwrap();
        let wal_before: (i64,) = sqlx::query_as("SELECT wal_bytes::bigint FROM pg_stat_wal")
            .fetch_one(outbox.pool())
            .await
            .unwrap();
        let deadlocks_before_scale: (i64,) = sqlx::query_as(
            "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()",
        )
        .fetch_one(outbox.pool())
        .await
        .unwrap();
        let scale_monitor_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url())
            .await
            .unwrap();
        let sampling = Arc::new(AtomicBool::new(true));
        let sampling_flag = sampling.clone();
        let sampled_lock_waits = Arc::new(AtomicI64::new(0));
        let sampled_max = sampled_lock_waits.clone();
        let active_connections_peak = Arc::new(AtomicI64::new(0));
        let active_peak = active_connections_peak.clone();
        let lock_monitor = tokio::spawn(async move {
            while sampling_flag.load(Ordering::Relaxed) {
                let (waiting,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'")
                    .fetch_one(&scale_monitor_pool).await.unwrap();
                sampled_max.fetch_max(waiting, Ordering::Relaxed);
                let (active,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = 'dataspool-outbox-load' AND state = 'active'")
                    .fetch_one(&scale_monitor_pool).await.unwrap();
                active_peak.fetch_max(active, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            scale_monitor_pool.close().await;
        });
        let scale_started = std::time::Instant::now();
        let mut scale_tasks = Vec::with_capacity(concurrency);
        for index in 0..concurrency {
            let producer = scale_instances[index % scale_instances.len()].clone();
            let id = format!("scale-{concurrency}-{index}");
            scale_tasks.push(tokio::spawn(async move {
                let started = std::time::Instant::now();
                let mut writer = producer.begin_write().await.unwrap();
                sqlx::query("SELECT pg_sleep(0.001)")
                    .execute(writer.connection())
                    .await
                    .unwrap();
                writer
                    .append_event(
                        Uuid::new_v4(),
                        "index",
                        &id,
                        "target-index",
                        &json!({"load":index}),
                    )
                    .await
                    .unwrap();
                writer.commit().await.unwrap();
                started.elapsed()
            }));
        }
        let mut latencies = Vec::with_capacity(concurrency);
        for task in scale_tasks {
            latencies.push(task.await.unwrap());
        }
        sampling.store(false, Ordering::Relaxed);
        lock_monitor.await.unwrap();
        latencies.sort_unstable();
        let elapsed = scale_started.elapsed();
        let percentile = |p: usize| latencies[(latencies.len() * p / 100).min(latencies.len() - 1)];
        sqlx::query("SELECT pg_stat_force_next_flush()")
            .execute(outbox.pool())
            .await
            .unwrap();
        let wal_after: (i64,) = sqlx::query_as("SELECT wal_bytes::bigint FROM pg_stat_wal")
            .fetch_one(outbox.pool())
            .await
            .unwrap();
        let deadlocks_after_scale: (i64,) = sqlx::query_as(
            "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()",
        )
        .fetch_one(outbox.pool())
        .await
        .unwrap();
        println!(
            "POSTGRES_OUTBOX_LOAD stream={} producer_tasks={} pool_connections={} active_connections_peak={} elapsed_ms={} throughput_ops_s={:.1} p50_ms={} p95_ms={} p99_ms={} max_lock_wait_sessions={} deadlocks_delta={} wal_bytes_delta={}",
            stream,
            concurrency,
            total_pool_connections,
            active_connections_peak.load(Ordering::Relaxed),
            elapsed.as_millis(),
            concurrency as f64 / elapsed.as_secs_f64(),
            percentile(50).as_millis(),
            percentile(95).as_millis(),
            percentile(99).as_millis(),
            sampled_lock_waits.load(Ordering::Relaxed),
            deadlocks_after_scale.0 - deadlocks_before_scale.0,
            (wal_after.0 - wal_before.0).max(0),
        );
        for app in &scale_instances {
            app.pool().close().await;
        }
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

async fn postgres_drop_commit_response_proxy(
    listener: tokio::net::TcpListener,
    backend_addr: std::net::SocketAddr,
    dropped: tokio::sync::oneshot::Sender<()>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut client, _) = listener.accept().await.unwrap();
    let mut backend = tokio::net::TcpStream::connect(backend_addr).await.unwrap();

    // Relay the PostgreSQL startup packet (length + protocol + parameters).
    let mut startup_len = [0_u8; 4];
    client.read_exact(&mut startup_len).await.unwrap();
    let mut startup_len_value = u32::from_be_bytes(startup_len) as usize;
    let mut startup_rest = vec![0; startup_len_value - 4];
    client.read_exact(&mut startup_rest).await.unwrap();

    // SQLx may probe TLS with the PostgreSQL SSLRequest before sending its
    // StartupMessage. The server's one-byte N response is not a framed
    // backend message, so relay it separately before switching to framing.
    if startup_len_value == 8
        && u32::from_be_bytes(startup_rest[..4].try_into().unwrap()) == 80_877_103
    {
        backend.write_all(&startup_len).await.unwrap();
        backend.write_all(&startup_rest).await.unwrap();
        let mut ssl_response = [0_u8; 1];
        backend.read_exact(&mut ssl_response).await.unwrap();
        client.write_all(&ssl_response).await.unwrap();
        assert_eq!(
            ssl_response[0], b'N',
            "test proxy requires TLS-disabled PostgreSQL"
        );
        client.read_exact(&mut startup_len).await.unwrap();
        startup_len_value = u32::from_be_bytes(startup_len) as usize;
        startup_rest = vec![0; startup_len_value - 4];
        client.read_exact(&mut startup_rest).await.unwrap();
    }

    backend
        .write_all(&(startup_len_value as u32).to_be_bytes())
        .await
        .unwrap();
    backend.write_all(&startup_rest).await.unwrap();

    let commit_forwarded = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let (mut backend_read, mut backend_write) = tokio::io::split(backend);
    let frontend_commit = commit_forwarded.clone();
    let frontend = tokio::spawn(async move {
        loop {
            let mut tag = [0_u8; 1];
            if client_read.read_exact(&mut tag).await.is_err() {
                break;
            }
            let mut length = [0_u8; 4];
            client_read.read_exact(&mut length).await?;
            let length = u32::from_be_bytes(length) as usize;
            let mut body = vec![0; length - 4];
            client_read.read_exact(&mut body).await?;
            if tag[0] == b'Q' || tag[0] == b'P' {
                let query = String::from_utf8_lossy(&body).to_ascii_uppercase();
                // Match a complete SQL statement, not identifiers such as
                // dataspool_ambiguous_commit_documents.
                if query
                    .split(char::from(0))
                    .any(|part| part.trim() == "COMMIT")
                {
                    eprintln!("[postgres-fault-proxy] forwarding COMMIT query");
                    frontend_commit.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
            backend_write.write_all(&tag).await?;
            backend_write
                .write_all(&(length as u32).to_be_bytes())
                .await?;
            backend_write.write_all(&body).await?;
        }
        Ok::<(), std::io::Error>(())
    });
    let backend_commit = commit_forwarded;
    let response = tokio::spawn(async move {
        loop {
            let mut tag = [0_u8; 1];
            if backend_read.read_exact(&mut tag).await.is_err() {
                break;
            }
            let mut length = [0_u8; 4];
            backend_read.read_exact(&mut length).await?;
            let length = u32::from_be_bytes(length) as usize;
            let mut body = vec![0; length - 4];
            backend_read.read_exact(&mut body).await?;
            if tag[0] == b'Z' && backend_commit.load(std::sync::atomic::Ordering::SeqCst) {
                eprintln!("[postgres-fault-proxy] dropped ReadyForQuery after server COMMIT");
                let _ = dropped.send(());
                // Explicitly half-close the client-facing socket. The frontend
                // task still owns the read half, so dropping this write half
                // alone would not reliably deliver EOF to the SQL client.
                client_write.shutdown().await?;
                break;
            }
            client_write.write_all(&tag).await?;
            client_write
                .write_all(&(length as u32).to_be_bytes())
                .await?;
            client_write.write_all(&body).await?;
        }
        Ok::<(), std::io::Error>(())
    });
    tokio::select! {
        result = frontend => eprintln!("[postgres-fault-proxy] frontend task ended: {result:?}"),
        result = response => eprintln!("[postgres-fault-proxy] backend task ended: {result:?}"),
    }
}

#[tokio::test]
async fn postgres_outbox_batches_are_atomic_ordered_and_bounded() {
    let _test_guard = POSTGRES_OUTBOX_TEST_LOCK.lock().await;
    use dataspool::drivers::relational::PostgresOutboxEventInput;

    let stream = format!("outbox-batch-{}", Uuid::new_v4());
    let outbox = producer(&stream).await;
    let mut writer = outbox.begin_write().await.unwrap();
    let events = (0..25)
        .map(|index| PostgresOutboxEventInput {
            event_id: Uuid::new_v4(),
            operation: "index".into(),
            document_id: format!("batch-{index}"),
            target_index: "batch-target".into(),
            document: json!({"ordinal": index}),
        })
        .collect::<Vec<_>>();
    let sequences = writer.append_events(&events).await.unwrap();
    assert_eq!(sequences.len(), 25);
    assert!(sequences.windows(2).all(|pair| pair[0] + 1 == pair[1]));
    writer.commit().await.unwrap();
    let watermark_after_batch = outbox.watermark().await.unwrap();

    // Replaying the complete batch after an ambiguous COMMIT must return the
    // original sequence vector without reserving a second sequence range.
    let mut retry = outbox.begin_write().await.unwrap();
    assert_eq!(retry.append_events(&events).await.unwrap(), sequences);
    retry.commit().await.unwrap();
    assert_eq!(outbox.watermark().await.unwrap(), watermark_after_batch);

    // A validation failure halfway through a batch poisons the producer
    // transaction; callers cannot accidentally commit its earlier events.
    let mut invalid_batch = outbox.begin_write().await.unwrap();
    let invalid_events = vec![
        PostgresOutboxEventInput {
            event_id: Uuid::new_v4(),
            operation: "index".into(),
            document_id: "must-rollback".into(),
            target_index: "batch-target".into(),
            document: json!({"ordinal": 26}),
        },
        PostgresOutboxEventInput {
            event_id: Uuid::new_v4(),
            operation: "unsupported".into(),
            document_id: "invalid".into(),
            target_index: "batch-target".into(),
            document: json!({"ordinal": 27}),
        },
    ];
    assert!(invalid_batch.append_events(&invalid_events).await.is_err());
    assert!(invalid_batch.commit().await.is_err());
    assert_eq!(outbox.watermark().await.unwrap(), watermark_after_batch);

    let stored = outbox.read_after(0, 100).await.unwrap();
    assert_eq!(stored.len(), 25);
    assert!(
        stored
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );

    let first = format!("batch-consumer-a-{stream}");
    let second = format!("batch-consumer-b-{stream}");
    outbox.register_consumer(&first).await.unwrap();
    outbox.register_consumer(&second).await.unwrap();
    let watermark = outbox.watermark().await.unwrap();
    outbox
        .record_consumer_checkpoints(&[(&first, watermark), (&second, watermark)])
        .await
        .unwrap();
    assert_eq!(outbox.prune_consumed_events_batch(7).await.unwrap(), 7);
    assert_eq!(outbox.read_after(0, 100).await.unwrap().len(), 18);
    assert_eq!(outbox.prune_consumed_events_batch(7).await.unwrap(), 7);
    assert_eq!(outbox.prune_consumed_events_batch(7).await.unwrap(), 7);
    assert_eq!(outbox.prune_consumed_events_batch(7).await.unwrap(), 4);
    assert!(outbox.read_after(0, 100).await.unwrap().is_empty());
    let retention_stats = outbox.retention_stats().await.unwrap();
    assert!(retention_stats.total_bytes > 0);
    assert!(retention_stats.autovacuum_count >= 0);
    println!(
        "POSTGRES_OUTBOX_RETENTION stream={} live_rows_estimate={} dead_rows_estimate={} table_bytes={} autovacuum_count={} last_autovacuum={:?} cluster_wal_bytes={:?}",
        stream,
        retention_stats.live_rows_estimate,
        retention_stats.dead_rows_estimate,
        retention_stats.total_bytes,
        retention_stats.autovacuum_count,
        retention_stats.last_autovacuum,
        retention_stats.cluster_wal_bytes,
    );
}

#[tokio::test]
async fn postgres_partitioned_outbox_uses_vector_checkpoints_and_cutover_barrier() {
    let _test_guard = POSTGRES_OUTBOX_TEST_LOCK.lock().await;
    let stream = format!("outbox-partitioned-{}", Uuid::new_v4());
    let base = producer(&stream).await;
    let mut partition_url = url::Url::parse(&database_url()).unwrap();
    partition_url
        .query_pairs_mut()
        .append_pair("application_name", "dataspool-outbox-partitioned");
    base.pool().close().await;
    let partition_pool = PostgresTransactionalOutbox::connect_with_max_connections(
        partition_url.as_str(),
        &stream,
        32,
    )
    .await
    .unwrap();
    let partitioned =
        PostgresPartitionedOutbox::new(partition_pool.pool().clone(), &stream, 4).unwrap();
    partitioned.migrate().await.unwrap();
    assert_eq!(
        partitioned.partition_for_key(b"customer-42"),
        partitioned.partition_for_key(b"customer-42")
    );
    let incompatible_topology =
        PostgresPartitionedOutbox::new(partition_pool.pool().clone(), &stream, 3).unwrap();
    assert!(
        incompatible_topology.migrate().await.is_err(),
        "partition topology cannot change in place"
    );

    // Each partition has an independent gate and local sequence. Producers
    // on different partitions do not serialize behind one stream-wide row.
    let partition_monitor_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url())
        .await
        .unwrap();
    let partition_sampling = Arc::new(AtomicBool::new(true));
    let partition_sampling_flag = partition_sampling.clone();
    let partition_active_peak = Arc::new(AtomicI64::new(0));
    let partition_active = partition_active_peak.clone();
    let partition_lock_wait_peak = Arc::new(AtomicI64::new(0));
    let partition_lock_waits = partition_lock_wait_peak.clone();
    let partition_lock_samples = Arc::new(tokio::sync::Mutex::new(std::collections::BTreeMap::<
        String,
        i64,
    >::new()));
    let partition_lock_samples_task = partition_lock_samples.clone();
    let partition_wait_samples = Arc::new(tokio::sync::Mutex::new(std::collections::BTreeMap::<
        String,
        i64,
    >::new()));
    let partition_wait_samples_task = partition_wait_samples.clone();
    let partition_monitor = tokio::spawn(async move {
        while partition_sampling_flag.load(Ordering::Relaxed) {
            let (active,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = 'dataspool-outbox-partitioned' AND state = 'active'")
                .fetch_one(&partition_monitor_pool).await.unwrap();
            partition_active.fetch_max(active, Ordering::Relaxed);
            let (waiting,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = 'dataspool-outbox-partitioned' AND wait_event_type = 'Lock'")
                .fetch_one(&partition_monitor_pool).await.unwrap();
            partition_lock_waits.fetch_max(waiting, Ordering::Relaxed);
            let waits = sqlx::query_as::<_, (String, i64)>("SELECT COALESCE(wait_event_type, 'CPU_OR_RUNNING') || ':' || COALESCE(wait_event, 'none'), COUNT(*) FROM pg_stat_activity WHERE application_name = 'dataspool-outbox-partitioned' GROUP BY 1")
                .fetch_all(&partition_monitor_pool).await.unwrap();
            {
                let mut samples = partition_wait_samples_task.lock().await;
                for (name, count) in waits {
                    *samples.entry(name).or_default() += count;
                }
            }
            let locks = sqlx::query_as::<_, (String, i64)>("SELECT COALESCE(c.relname, l.locktype) || ':' || l.mode || ':' || CASE WHEN l.granted THEN 'granted' ELSE 'waiting' END, COUNT(*) FROM pg_locks l JOIN pg_stat_activity a USING (pid) LEFT JOIN pg_class c ON c.oid = l.relation WHERE a.application_name = 'dataspool-outbox-partitioned' GROUP BY 1")
                .fetch_all(&partition_monitor_pool).await.unwrap();
            {
                let mut samples = partition_lock_samples_task.lock().await;
                for (name, count) in locks {
                    *samples.entry(name).or_default() += count;
                }
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        partition_monitor_pool.close().await;
    });
    let partition_started = std::time::Instant::now();
    let mut tasks = Vec::new();
    for partition_id in 0..4u32 {
        for ordinal in 0..8u32 {
            let partitioned = partitioned.clone();
            let stream_key = stream.clone();
            tasks.push(tokio::spawn(async move {
                let document_id = format!("p{partition_id}-{ordinal}");
                let started = std::time::Instant::now();
                let mut writer = partitioned.begin_write(partition_id).await.unwrap();
                sqlx::query("SELECT pg_sleep(0.005)").execute(writer.connection()).await.unwrap();
                let events = (0..4u32).map(|batch_index| PostgresOutboxEventInput {
                    event_id: Uuid::new_v4(), operation: "index".into(),
                    document_id: format!("{document_id}-{batch_index}"), target_index: "partition-target".into(),
                    document: json!({"partition":partition_id,"ordinal":ordinal,"batch":batch_index}),
                }).collect::<Vec<_>>();
                let source_ids = events.iter().map(|event| event.document_id.clone()).collect::<Vec<_>>();
                sqlx::query("INSERT INTO dataspool_outbox_test_documents (stream_key, document_id, payload) SELECT $1, source_id, jsonb_build_object('partition', $2, 'ordinal', $3) FROM unnest($4::text[]) AS source_id")
                    .bind(&stream_key).bind(partition_id as i64).bind(ordinal as i64).bind(&source_ids)
                    .execute(writer.connection()).await.unwrap();
                let sequences = writer.append_events(&events).await.unwrap();
                writer.commit().await.unwrap();
                (sequences.len(), started.elapsed())
            }));
        }
    }
    let mut partitioned_latencies = Vec::new();
    let mut partitioned_event_count = 0usize;
    for task in tasks {
        let (count, latency) = task.await.unwrap();
        partitioned_event_count += count;
        partitioned_latencies.push(latency);
    }
    partitioned_latencies.sort_unstable();
    partition_sampling.store(false, Ordering::Relaxed);
    partition_monitor.await.unwrap();
    let partitioned_elapsed = partition_started.elapsed();
    println!(
        "POSTGRES_OUTBOX_PARTITIONED stream={} partitions=4 producer_tasks=32 batch_events={} pool_connections=32 active_connections_peak={} peak_lock_wait_sessions={} elapsed_ms={} throughput_events_s={:.1} p50_transaction_ms={} p95_transaction_ms={} p99_transaction_ms={} wait_event_samples={:?} pg_locks_samples={:?}",
        stream,
        partitioned_event_count,
        partition_active_peak.load(Ordering::Relaxed),
        partition_lock_wait_peak.load(Ordering::Relaxed),
        partitioned_elapsed.as_millis(),
        partitioned_event_count as f64 / partitioned_elapsed.as_secs_f64(),
        partitioned_latencies[partitioned_latencies.len() / 2].as_millis(),
        partitioned_latencies
            [(partitioned_latencies.len() * 95 / 100).min(partitioned_latencies.len() - 1)]
        .as_millis(),
        partitioned_latencies
            [(partitioned_latencies.len() * 99 / 100).min(partitioned_latencies.len() - 1)]
        .as_millis(),
        *partition_wait_samples.lock().await,
        *partition_lock_samples.lock().await,
    );

    let vector = partitioned.vector_watermark().await.unwrap();
    assert_eq!(vector, (0..4u32).map(|partition| (partition, 32)).collect());
    let offsets = (0..4u32).map(|partition| (partition, 0)).collect();
    let vector_batches = partitioned.read_vector_after(&offsets, 100).await.unwrap();
    assert_eq!(vector_batches.values().map(Vec::len).sum::<usize>(), 128);
    let source_config = SourceConfig::PostgresPartitionedOutbox {
        config: PostgresPartitionedOutboxConfig {
            connection_env: "DATASPOOL_TEST_POSTGRES_URL".into(),
            stream_key: stream.clone(),
            partition_count: 4,
        },
    };
    let mut source = create_source(&source_config).unwrap();
    source.connect().await.unwrap();
    let (source_batch, source_checkpoint) = source
        .read_batch(&OffsetValue::initial(), 200)
        .await
        .unwrap();
    assert_eq!(source_batch.len(), 128);
    let source_vector: std::collections::BTreeMap<u32, u64> = match &source_checkpoint {
        OffsetValue::Custom(value) => {
            serde_json::from_value(value["partition_watermarks"].clone()).unwrap()
        }
        other => panic!("expected vector checkpoint, got {other:?}"),
    };
    assert_eq!(source_vector, vector);
    assert_eq!(
        source_batch
            .records()
            .iter()
            .filter(|record| record.get_metadata("_partition_id").is_some())
            .count(),
        128
    );
    let (empty_batch, same_checkpoint) = source.read_batch(&source_checkpoint, 100).await.unwrap();
    assert!(empty_batch.is_empty());
    assert_eq!(same_checkpoint, source_checkpoint);
    source.close().await.unwrap();
    for partition_id in 0..4u32 {
        let events = partitioned
            .read_partition_after(partition_id, 0, 100)
            .await
            .unwrap();
        assert_eq!(events.len(), 32);
        assert!(
            events
                .windows(2)
                .all(|pair| pair[0].local_sequence < pair[1].local_sequence)
        );
        assert!(
            events
                .iter()
                .all(|event| event.partition_id == partition_id)
        );
    }

    let consumer = format!("partition-consumer-{stream}");
    partitioned.register_consumer(&consumer).await.unwrap();
    let mut incomplete = vector.clone();
    incomplete.remove(&3);
    assert!(
        partitioned
            .record_consumer_checkpoint_vector(&consumer, &incomplete)
            .await
            .is_err()
    );
    partitioned
        .record_consumer_checkpoint_vector(&consumer, &vector)
        .await
        .unwrap();
    let regressed_vector = (0..4u32)
        .map(|partition| (partition, if partition == 0 { 7 } else { 8 }))
        .collect();
    assert!(
        partitioned
            .record_consumer_checkpoint_vector(&consumer, &regressed_vector)
            .await
            .is_err()
    );
    let restarted_pool = PostgresTransactionalOutbox::connect(&database_url(), &stream)
        .await
        .unwrap();
    let restarted_partitioned =
        PostgresPartitionedOutbox::new(restarted_pool.pool().clone(), &stream, 4).unwrap();
    assert_eq!(
        restarted_partitioned
            .consumer_checkpoint_vector(&consumer)
            .await
            .unwrap(),
        vector
    );
    assert_eq!(partitioned.prune_partition_batch(0, 3).await.unwrap(), 3);
    let partition_retention = partitioned.retention_stats().await.unwrap();
    assert!(partition_retention.total_bytes > 0);
    assert_eq!(
        partitioned
            .read_partition_after(0, 0, 100)
            .await
            .unwrap()
            .len(),
        29
    );

    let mut barrier = partitioned.clone();
    let final_vector = barrier.pause_and_capture_vector_watermark().await.unwrap();
    assert_eq!(final_vector, vector);
    for partition_id in 0..4u32 {
        assert!(partitioned.begin_write(partition_id).await.is_err());
    }
    barrier.resume_partitioned_writes().await.unwrap();
    let replay_id = Uuid::new_v4();
    let replay_payload = json!({"ok":true});
    let mut resumed = partitioned.begin_write(2).await.unwrap();
    let allocated = resumed
        .append_event(
            replay_id,
            "index",
            "after-resume",
            "partition-target",
            &replay_payload,
        )
        .await
        .unwrap();
    resumed.commit().await.unwrap();
    let mut replay = partitioned.begin_write(2).await.unwrap();
    assert_eq!(
        replay
            .append_event(
                replay_id,
                "index",
                "after-resume",
                "partition-target",
                &replay_payload
            )
            .await
            .unwrap(),
        allocated
    );
    replay.commit().await.unwrap();
    assert_eq!(partitioned.vector_watermark().await.unwrap()[&2], 33);
}

#[tokio::test]
async fn postgres_partitioned_outbox_batch_is_idempotent_across_partitions() {
    let _test_guard = POSTGRES_OUTBOX_TEST_LOCK.lock().await;
    let stream = format!("outbox-partitioned-batch-{}", Uuid::new_v4());
    let base = producer(&stream).await;
    let partitioned = PostgresPartitionedOutbox::new(base.pool().clone(), &stream, 4).unwrap();
    partitioned.migrate().await.unwrap();
    partitioned.migrate().await.unwrap();

    let events = (0..3u32)
        .map(|index| PostgresOutboxEventInput {
            event_id: Uuid::new_v4(),
            operation: "index".into(),
            document_id: format!("batch-{index}"),
            target_index: "batch-target".into(),
            document: json!({"value":index}),
        })
        .collect::<Vec<_>>();
    let mut writer = partitioned.begin_write(1).await.unwrap();
    let first = writer.append_events(&events).await.unwrap();
    let replay = writer.append_events(&events).await.unwrap();
    assert_eq!(
        first, replay,
        "batch replay must return original local sequences"
    );
    writer.commit().await.unwrap();
    assert_eq!(partitioned.vector_watermark().await.unwrap()[&1], 3);

    let mut cross_partition_replay = partitioned.begin_write(2).await.unwrap();
    assert!(
        cross_partition_replay.append_events(&events).await.is_err(),
        "event IDs must remain globally unique across logical partitions"
    );
    assert!(
        cross_partition_replay.commit().await.is_err(),
        "a failed idempotency check must poison and roll back the producer transaction"
    );
    assert_eq!(partitioned.vector_watermark().await.unwrap()[&2], 0);
}

#[tokio::test]
async fn postgres_ambiguous_commit_response_is_recovered_by_event_id() {
    let _test_guard = POSTGRES_OUTBOX_TEST_LOCK.lock().await;
    use sqlx::Row;
    let stream = format!("outbox-ambiguous-{}", Uuid::new_v4());
    let direct = producer(&stream).await;
    sqlx::query("CREATE TABLE IF NOT EXISTS dataspool_ambiguous_commit_documents (stream_key TEXT NOT NULL, document_id TEXT NOT NULL, payload JSONB NOT NULL, PRIMARY KEY(stream_key, document_id))")
        .execute(direct.pool()).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let parsed = url::Url::parse(&database_url()).unwrap();
    let backend_addr = format!(
        "{}:{}",
        parsed.host_str().unwrap(),
        parsed.port().unwrap_or(5432)
    )
    .parse()
    .unwrap();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(postgres_drop_commit_response_proxy(
        listener,
        backend_addr,
        dropped_tx,
    ));

    let mut proxied_url = parsed;
    proxied_url.set_host(Some("127.0.0.1")).unwrap();
    proxied_url.set_port(Some(proxy_addr.port())).unwrap();
    proxied_url
        .query_pairs_mut()
        .append_pair("sslmode", "disable");
    let proxied =
        PostgresTransactionalOutbox::connect_with_max_connections(proxied_url.as_str(), &stream, 1)
            .await
            .unwrap();
    let event_id = Uuid::new_v4();
    let payload = json!({"account":"acct-1","balance":42});
    let mut writer = proxied.begin_write().await.unwrap();
    sqlx::query("INSERT INTO dataspool_ambiguous_commit_documents(stream_key, document_id, payload) VALUES ($1, $2, $3) ON CONFLICT(stream_key, document_id) DO UPDATE SET payload = EXCLUDED.payload")
        .bind(&stream).bind("acct-1").bind(&payload).execute(writer.connection()).await.unwrap();
    let first_sequence = writer
        .append_event(event_id, "index", "acct-1", "accounts", &payload)
        .await
        .unwrap();
    let commit_result = tokio::time::timeout(Duration::from_secs(10), writer.commit())
        .await
        .expect("producer must observe the dropped COMMIT response promptly");
    assert!(
        commit_result.is_err(),
        "client must observe a lost COMMIT response"
    );
    tokio::time::timeout(Duration::from_secs(5), dropped_rx)
        .await
        .unwrap()
        .unwrap();

    // Verify from an independent connection: PostgreSQL committed even though
    // the producer received a transport error instead of ReadyForQuery.
    let source_rows = sqlx::query("SELECT payload FROM dataspool_ambiguous_commit_documents WHERE stream_key = $1 AND document_id = 'acct-1'")
        .bind(&stream).fetch_all(direct.pool()).await.unwrap();
    assert_eq!(source_rows.len(), 1);
    assert_eq!(
        source_rows[0]
            .try_get::<serde_json::Value, _>("payload")
            .unwrap(),
        payload
    );
    let events = direct.read_after(0, 100).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_id, event_id.to_string());
    assert_eq!(events[0].sequence as u64, first_sequence);

    // Retry with the same event ID and an idempotent source upsert. It must
    // return the original sequence and must not duplicate either side.
    let mut retry = direct.begin_write().await.unwrap();
    sqlx::query("INSERT INTO dataspool_ambiguous_commit_documents(stream_key, document_id, payload) VALUES ($1, $2, $3) ON CONFLICT(stream_key, document_id) DO UPDATE SET payload = EXCLUDED.payload")
        .bind(&stream).bind("acct-1").bind(&payload).execute(retry.connection()).await.unwrap();
    let retry_sequence = retry
        .append_event(event_id, "index", "acct-1", "accounts", &payload)
        .await
        .unwrap();
    assert_eq!(retry_sequence, first_sequence);
    retry.commit().await.unwrap();
    assert_eq!(direct.watermark().await.unwrap(), first_sequence);
    assert_eq!(direct.read_after(0, 100).await.unwrap().len(), 1);
}
