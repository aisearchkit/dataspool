// SPDX-License-Identifier: AGPL-3.0-or-later
//! PostgreSQL transactional Outbox and durable producer write barrier.
//!
//! Writers acquire the stream gate row with `FOR UPDATE`, perform their source
//! mutation through the returned transaction connection, append the Outbox row,
//! and commit once. The gate lock serializes commit order per stream. Cutover
//! takes the same lock, so it waits for all accepted writes and prevents later
//! writes until an explicit resume.

use crate::drivers::document::cdc::CdcWriteBarrier;
use crate::error::{DatspoolError, DriverError, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Digest;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Postgres, QueryBuilder, Row, Transaction};
use std::time::Duration;

const PARTITIONED_EVENT_ID_BUCKETS: i16 = 16;

fn partitioned_event_id_bucket(event_id: uuid::Uuid) -> i16 {
    i16::from(event_id.as_bytes()[0] % PARTITIONED_EVENT_ID_BUCKETS as u8)
}

fn db_error(context: &str, error: sqlx::Error) -> DatspoolError {
    DatspoolError::Driver(DriverError::DataError(format!("{context}: {error}")))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresOutboxEvent {
    pub sequence: i64,
    pub event_id: String,
    pub stream_key: String,
    pub operation: String,
    pub document_id: String,
    pub target_index: String,
    pub document: Value,
}

/// One event in an atomic producer batch.
#[derive(Debug, Clone)]
pub struct PostgresOutboxEventInput {
    pub event_id: uuid::Uuid,
    pub operation: String,
    pub document_id: String,
    pub target_index: String,
    pub document: Value,
}

/// PostgreSQL storage and maintenance counters for the Outbox table.
#[derive(Debug, Clone)]
pub struct PostgresOutboxRetentionStats {
    pub live_rows_estimate: i64,
    pub dead_rows_estimate: i64,
    pub total_bytes: i64,
    pub autovacuum_count: i64,
    pub last_autovacuum: Option<String>,
    /// Cluster-wide counter; isolate the PostgreSQL instance before attributing
    /// this delta to one Outbox stream.
    pub cluster_wal_bytes: Option<i64>,
}

/// PostgreSQL transactional Outbox for one logical CDC stream.
#[derive(Clone)]
pub struct PostgresTransactionalOutbox {
    pool: PgPool,
    stream_key: String,
}

impl PostgresTransactionalOutbox {
    pub async fn connect(database_url: &str, stream_key: impl Into<String>) -> Result<Self> {
        Self::connect_with_max_connections(database_url, stream_key, 16).await
    }

    /// Connect with an explicit per-application-instance pool limit. This is
    /// useful when several producer processes share the same PostgreSQL
    /// cluster and their aggregate connection budget must be bounded.
    pub async fn connect_with_max_connections(
        database_url: &str,
        stream_key: impl Into<String>,
        max_connections: u32,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .acquire_timeout(Duration::from_secs(15))
            .connect(database_url)
            .await
            .map_err(|error| db_error("connect PostgreSQL transactional Outbox", error))?;
        Ok(Self::new(pool, stream_key))
    }

    pub fn new(pool: PgPool, stream_key: impl Into<String>) -> Self {
        Self {
            pool,
            stream_key: stream_key.into(),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Create the durable gate and append-only event tables. Safe to run at startup.
    pub async fn migrate(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dataspool_cdc_outbox_gate (\
                stream_key TEXT PRIMARY KEY, paused BOOLEAN NOT NULL DEFAULT FALSE,\
                last_sequence BIGINT NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()\
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("create Outbox gate", error))?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dataspool_cdc_outbox (\
                sequence BIGINT NOT NULL, event_id TEXT NOT NULL UNIQUE,\
                stream_key TEXT NOT NULL, operation TEXT NOT NULL,\
                document_id TEXT NOT NULL, target_index TEXT NOT NULL,\
                document JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),\
                PRIMARY KEY (stream_key, sequence)\
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("create Outbox events", error))?;
        sqlx::query("INSERT INTO dataspool_cdc_outbox_gate (stream_key) VALUES ($1) ON CONFLICT (stream_key) DO NOTHING")
            .bind(&self.stream_key).execute(&self.pool).await.map_err(|error| db_error("initialize Outbox gate", error))?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dataspool_cdc_outbox_consumers (\
                stream_key TEXT NOT NULL REFERENCES dataspool_cdc_outbox_gate(stream_key),\
                consumer_key TEXT NOT NULL, last_sequence BIGINT NOT NULL DEFAULT 0,\
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),\
                PRIMARY KEY (stream_key, consumer_key)\
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("create Outbox consumer checkpoints", error))?;
        Ok(())
    }

    /// Begin one source mutation. Execute all source-table SQL through
    /// `writer.connection()`, append the corresponding event, then commit.
    /// Do not execute source writes outside this transaction if they must be
    /// covered by the CDC guarantee.
    pub async fn begin_write(&self) -> Result<PostgresOutboxWrite<'_>> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin source write transaction", error))?;
        let row = sqlx::query(
            "SELECT paused FROM dataspool_cdc_outbox_gate WHERE stream_key = $1 FOR UPDATE",
        )
        .bind(&self.stream_key)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|error| db_error("lock Outbox write gate", error))?;
        let paused: bool = row
            .try_get("paused")
            .map_err(|error| db_error("read Outbox write gate", error))?;
        if paused {
            transaction
                .rollback()
                .await
                .map_err(|error| db_error("rollback paused source write", error))?;
            return Err(DatspoolError::Internal(format!(
                "CDC stream '{}' is paused for cutover",
                self.stream_key
            )));
        }
        sqlx::query("SELECT set_config('dataspool.outbox_stream_key', $1, TRUE)")
            .bind(&self.stream_key)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| db_error("authorize guarded source writes", error))?;
        Ok(PostgresOutboxWrite {
            transaction: Some(transaction),
            stream_key: self.stream_key.clone(),
            event_appended: false,
            failed: false,
        })
    }

    pub async fn read_after(&self, sequence: i64, limit: i64) -> Result<Vec<PostgresOutboxEvent>> {
        let rows = sqlx::query(
            "SELECT sequence, event_id::text AS event_id, stream_key, operation, document_id, target_index, document \
             FROM dataspool_cdc_outbox WHERE stream_key = $1 AND sequence > $2 ORDER BY sequence ASC LIMIT $3",
        ).bind(&self.stream_key).bind(sequence).bind(limit.clamp(1, 10_000))
            .fetch_all(&self.pool).await.map_err(|error| db_error("read transactional Outbox events", error))?;
        rows.into_iter()
            .map(|row| {
                Ok(PostgresOutboxEvent {
                    sequence: row
                        .try_get("sequence")
                        .map_err(|error| db_error("decode Outbox sequence", error))?,
                    event_id: row
                        .try_get("event_id")
                        .map_err(|error| db_error("decode Outbox event ID", error))?,
                    stream_key: row
                        .try_get("stream_key")
                        .map_err(|error| db_error("decode Outbox stream", error))?,
                    operation: row
                        .try_get("operation")
                        .map_err(|error| db_error("decode Outbox operation", error))?,
                    document_id: row
                        .try_get("document_id")
                        .map_err(|error| db_error("decode Outbox document ID", error))?,
                    target_index: row
                        .try_get("target_index")
                        .map_err(|error| db_error("decode Outbox target", error))?,
                    document: row
                        .try_get("document")
                        .map_err(|error| db_error("decode Outbox document", error))?,
                })
            })
            .collect()
    }

    pub async fn watermark(&self) -> Result<u64> {
        let row = sqlx::query("SELECT last_sequence AS watermark FROM dataspool_cdc_outbox_gate WHERE stream_key = $1")
            .bind(&self.stream_key).fetch_one(&self.pool).await
            .map_err(|error| db_error("read Outbox watermark", error))?;
        let watermark: i64 = row
            .try_get("watermark")
            .map_err(|error| db_error("decode Outbox watermark", error))?;
        u64::try_from(watermark)
            .map_err(|_| DatspoolError::Internal("negative PostgreSQL Outbox watermark".into()))
    }

    /// Install a trigger that rejects direct DML unless the transaction was
    /// opened through `begin_write()` for this stream. Run as the table owner.
    /// The application role must not own the protected table or have privileges
    /// to disable/drop triggers.
    pub async fn install_write_guard(&self, schema: &str, table: &str) -> Result<()> {
        fn ident(value: &str) -> Result<String> {
            if value.is_empty() || value.contains('\0') {
                return Err(DatspoolError::Configuration(
                    "invalid PostgreSQL identifier".into(),
                ));
            }
            Ok(format!("\"{}\"", value.replace('\"', "\"\"")))
        }
        let schema_ident = ident(schema)?;
        let table_ident = ident(table)?;
        let trigger_ident = ident(&format!("dataspool_outbox_guard_{}", table))?;
        sqlx::query(
            r#"
            CREATE OR REPLACE FUNCTION public.dataspool_require_outbox_write()
            RETURNS trigger
            LANGUAGE plpgsql SECURITY INVOKER
            AS $$
            BEGIN
              IF current_setting('dataspool.outbox_stream_key', TRUE) IS DISTINCT FROM TG_ARGV[0] THEN
                RAISE EXCEPTION 'source table write requires Dataspool transactional Outbox stream %', TG_ARGV[0]
                  USING ERRCODE = '42501';
              END IF;
              IF TG_OP = 'DELETE' THEN
                RETURN OLD;
              END IF;
              RETURN NEW;
            END;
            $$
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("create Outbox write guard function", error))?;
        let ddl = format!(
            r#"
            DROP TRIGGER IF EXISTS {trigger_ident} ON {schema_ident}.{table_ident};
            CREATE TRIGGER {trigger_ident}
            BEFORE INSERT OR UPDATE OR DELETE ON {schema_ident}.{table_ident}
            FOR EACH ROW EXECUTE FUNCTION public.dataspool_require_outbox_write('{}');
            "#,
            self.stream_key.replace('\'', "''")
        );
        sqlx::raw_sql(&ddl)
            .execute(&self.pool)
            .await
            .map_err(|error| db_error("install Outbox source write guard", error))?;
        Ok(())
    }

    /// Remove direct table mutation privileges from an application login.
    /// Run as the table owner or a role with sufficient grant authority. The
    /// Outbox writer must use a separate, controlled database role that retains
    /// the required DML privileges.
    pub async fn restrict_application_dml(
        &self,
        schema: &str,
        table: &str,
        application_role: &str,
    ) -> Result<()> {
        fn ident(value: &str) -> Result<String> {
            if value.is_empty() || value.contains('\0') {
                return Err(DatspoolError::Configuration(
                    "invalid PostgreSQL identifier".into(),
                ));
            }
            Ok(format!("\"{}\"", value.replace('\"', "\"\"")))
        }
        let schema_ident = ident(schema)?;
        let table_ident = ident(table)?;
        let role_ident = ident(application_role)?;
        let ddl = format!(
            r#"
            REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
            ON TABLE {schema_ident}.{table_ident} FROM {role_ident};
            GRANT SELECT ON TABLE {schema_ident}.{table_ident} TO {role_ident};
            "#
        );
        sqlx::raw_sql(&ddl)
            .execute(&self.pool)
            .await
            .map_err(|error| db_error("restrict application role source-table DML", error))?;
        Ok(())
    }

    /// Register a consumer before it starts reading. A new consumer begins at
    /// sequence zero so pruning cannot discard its backlog.
    pub async fn register_consumer(&self, consumer_key: &str) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error("begin consumer registration", e))?;
        sqlx::query(
            "SELECT stream_key FROM dataspool_cdc_outbox_gate WHERE stream_key = $1 FOR UPDATE",
        )
        .bind(&self.stream_key)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| db_error("lock stream for consumer registration", e))?;
        sqlx::query("INSERT INTO dataspool_cdc_outbox_consumers (stream_key, consumer_key, last_sequence) VALUES ($1, $2, 0) ON CONFLICT (stream_key, consumer_key) DO NOTHING")
            .bind(&self.stream_key).bind(consumer_key).execute(&mut *tx).await.map_err(|e| db_error("register Outbox consumer", e))?;
        tx.commit()
            .await
            .map_err(|e| db_error("commit consumer registration", e))
    }

    /// Advance multiple registered consumers in one transaction. Each update
    /// is validated against the committed stream watermark and its previous
    /// value before any checkpoint becomes visible.
    pub async fn record_consumer_checkpoints(&self, checkpoints: &[(&str, u64)]) -> Result<()> {
        if checkpoints.is_empty() {
            return Ok(());
        }
        let mut requested = std::collections::BTreeMap::new();
        for (consumer, sequence) in checkpoints {
            let sequence = i64::try_from(*sequence).map_err(|_| {
                DatspoolError::Internal("consumer checkpoint exceeds PostgreSQL BIGINT".into())
            })?;
            if let Some(previous) = requested.insert(*consumer, sequence)
                && sequence < previous
            {
                return Err(DatspoolError::Internal(format!(
                    "consumer checkpoint regression within batch: {sequence} < {previous}"
                )));
            }
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error("begin batched consumer checkpoints", e))?;
        let gate = sqlx::query(
            "SELECT last_sequence FROM dataspool_cdc_outbox_gate WHERE stream_key = $1 FOR UPDATE",
        )
        .bind(&self.stream_key)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| db_error("lock stream for batched checkpoints", e))?;
        let watermark: i64 = gate
            .try_get("last_sequence")
            .map_err(|e| db_error("decode batched checkpoint watermark", e))?;
        for (consumer, sequence) in requested {
            if sequence > watermark {
                return Err(DatspoolError::Internal(format!(
                    "consumer checkpoint {sequence} exceeds committed Outbox watermark {watermark}"
                )));
            }
            let row = sqlx::query(
                "SELECT last_sequence FROM dataspool_cdc_outbox_consumers WHERE stream_key = $1 AND consumer_key = $2 FOR UPDATE",
            ).bind(&self.stream_key).bind(consumer).fetch_optional(&mut *tx).await
                .map_err(|e| db_error("read batched consumer checkpoint", e))?
                .ok_or_else(|| DatspoolError::Configuration(format!(
                    "Outbox consumer '{consumer}' is not registered"
                )))?;
            let previous: i64 = row
                .try_get("last_sequence")
                .map_err(|e| db_error("decode previous batched checkpoint", e))?;
            if sequence < previous {
                return Err(DatspoolError::Internal(format!(
                    "consumer checkpoint regression: {sequence} < {previous}"
                )));
            }
            sqlx::query(
                "UPDATE dataspool_cdc_outbox_consumers SET last_sequence = $3, updated_at = clock_timestamp() WHERE stream_key = $1 AND consumer_key = $2",
            ).bind(&self.stream_key).bind(consumer).bind(sequence).execute(&mut *tx).await
                .map_err(|e| db_error("advance batched consumer checkpoint", e))?;
        }
        tx.commit()
            .await
            .map_err(|e| db_error("commit batched consumer checkpoints", e))
    }

    pub async fn record_consumer_checkpoint(
        &self,
        consumer_key: &str,
        sequence: u64,
    ) -> Result<()> {
        self.record_consumer_checkpoints(&[(consumer_key, sequence)])
            .await
    }

    /// Delete only events at or below the slowest registered consumer's
    /// checkpoint. With no registered consumers, nothing is eligible.
    /// Delete eligible events in bounded transactions. The legacy unbounded
    /// method drains the safe range in chunks for compatibility.
    pub async fn prune_consumed_events(&self) -> Result<u64> {
        let mut total = 0;
        loop {
            let deleted = self.prune_consumed_events_batch(1_000).await?;
            total += deleted;
            if deleted < 1_000 {
                return Ok(total);
            }
        }
    }

    /// Delete at most `batch_size` rows using bounded CTE selection. The
    /// consumer watermark and gate lock are revalidated for
    /// every chunk, limiting WAL bursts and transaction/lock duration.
    pub async fn prune_consumed_events_batch(&self, batch_size: u32) -> Result<u64> {
        let started = std::time::Instant::now();
        let wal_before = sqlx::query("SELECT wal_bytes::bigint AS wal_bytes FROM pg_stat_wal")
            .fetch_one(&self.pool)
            .await
            .ok()
            .and_then(|row| row.try_get::<i64, _>("wal_bytes").ok());
        let batch_size = batch_size.clamp(1, 10_000) as i64;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error("begin bounded Outbox pruning", e))?;
        sqlx::query(
            "SELECT stream_key FROM dataspool_cdc_outbox_gate WHERE stream_key = $1 FOR UPDATE",
        )
        .bind(&self.stream_key)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| db_error("lock stream for bounded Outbox pruning", e))?;
        let minimum = sqlx::query(
            "SELECT MIN(last_sequence) AS minimum FROM dataspool_cdc_outbox_consumers WHERE stream_key = $1",
        ).bind(&self.stream_key).fetch_one(&mut *tx).await
            .map_err(|e| db_error("read bounded pruning watermark", e))?;
        let Some(watermark) = minimum
            .try_get::<Option<i64>, _>("minimum")
            .map_err(|e| db_error("decode bounded pruning watermark", e))?
        else {
            tx.rollback()
                .await
                .map_err(|e| db_error("rollback pruning without consumers", e))?;
            return Ok(0);
        };
        let deleted = sqlx::query(
            "WITH victims AS (SELECT ctid FROM dataspool_cdc_outbox WHERE stream_key = $1 AND sequence <= $2 ORDER BY sequence LIMIT $3) DELETE FROM dataspool_cdc_outbox AS events USING victims WHERE events.ctid = victims.ctid",
        ).bind(&self.stream_key).bind(watermark).bind(batch_size).execute(&mut *tx).await
            .map_err(|e| db_error("delete bounded Outbox event batch", e))?.rows_affected();
        tx.commit()
            .await
            .map_err(|e| db_error("commit bounded Outbox pruning", e))?;
        let elapsed = started.elapsed();
        let wal_after = sqlx::query("SELECT wal_bytes::bigint AS wal_bytes FROM pg_stat_wal")
            .fetch_one(&self.pool)
            .await
            .ok()
            .and_then(|row| row.try_get::<i64, _>("wal_bytes").ok());
        let wal_delta = wal_before
            .zip(wal_after)
            .map(|(before, after)| (after - before).max(0));
        tracing::info!(
            stream_key = %self.stream_key,
            deleted_rows = deleted,
            batch_limit = batch_size,
            elapsed_ms = elapsed.as_millis() as u64,
            rows_per_second = deleted as f64 / elapsed.as_secs_f64().max(0.001),
            wal_bytes_delta = ?wal_delta,
            "PostgreSQL Outbox retention batch completed"
        );
        Ok(deleted)
    }

    /// Observe table bloat/autovacuum counters and the optional cluster WAL
    /// counter. `n_live_tup`/`n_dead_tup` are PostgreSQL estimates, not exact
    /// counts; run ANALYZE/VACUUM before comparing them across long intervals.
    pub async fn retention_stats(&self) -> Result<PostgresOutboxRetentionStats> {
        let row = sqlx::query(
            "SELECT n_live_tup, n_dead_tup, autovacuum_count, last_autovacuum::text AS last_autovacuum, pg_total_relation_size(relid) AS total_bytes              FROM pg_stat_user_tables WHERE schemaname = current_schema() AND relname = 'dataspool_cdc_outbox'",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| db_error("read Outbox retention statistics", error))?
        .ok_or_else(|| DatspoolError::Internal("Outbox table statistics are unavailable in the current schema".into()))?;
        let cluster_wal_bytes =
            sqlx::query("SELECT wal_bytes::bigint AS wal_bytes FROM pg_stat_wal")
                .fetch_one(&self.pool)
                .await
                .ok()
                .and_then(|row| row.try_get::<i64, _>("wal_bytes").ok());
        Ok(PostgresOutboxRetentionStats {
            live_rows_estimate: row
                .try_get("n_live_tup")
                .map_err(|error| db_error("decode Outbox live tuple estimate", error))?,
            dead_rows_estimate: row
                .try_get("n_dead_tup")
                .map_err(|error| db_error("decode Outbox dead tuple estimate", error))?,
            total_bytes: row
                .try_get("total_bytes")
                .map_err(|error| db_error("decode Outbox table size", error))?,
            autovacuum_count: row
                .try_get("autovacuum_count")
                .map_err(|error| db_error("decode Outbox autovacuum count", error))?,
            last_autovacuum: row
                .try_get("last_autovacuum")
                .map_err(|error| db_error("decode Outbox last autovacuum", error))?,
            cluster_wal_bytes,
        })
    }

    pub async fn is_paused(&self) -> Result<bool> {
        let row = sqlx::query("SELECT paused FROM dataspool_cdc_outbox_gate WHERE stream_key = $1")
            .bind(&self.stream_key)
            .fetch_one(&self.pool)
            .await
            .map_err(|error| db_error("read Outbox pause state", error))?;
        row.try_get("paused")
            .map_err(|error| db_error("decode Outbox pause state", error))
    }
}

/// Event identity in a partition-local ordering domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresPartitionedOutboxEvent {
    pub partition_id: u32,
    pub local_sequence: i64,
    pub event_id: String,
    pub stream_key: String,
    pub operation: String,
    pub document_id: String,
    pub target_index: String,
    pub document: Value,
}

/// A vector watermark is a partial order: each partition advances independently.
pub type PostgresOutboxVectorWatermark = std::collections::BTreeMap<u32, u64>;

/// Write barrier for partitioned streams. The complete vector is captured while
/// every partition gate is locked in ascending partition order.
#[async_trait]
pub trait PartitionedCdcWriteBarrier: Send {
    async fn pause_and_capture_vector_watermark(&mut self)
    -> Result<PostgresOutboxVectorWatermark>;
    async fn resume_partitioned_writes(&mut self) -> Result<()>;
}

/// Experimental partitioned Outbox implementation. A logical stream is split
/// into independently ordered partitions; there is deliberately no total order
/// between different partitions.
#[derive(Clone)]
pub struct PostgresPartitionedOutbox {
    pool: PgPool,
    stream_key: String,
    partition_count: u32,
}

impl PostgresPartitionedOutbox {
    pub fn new(pool: PgPool, stream_key: impl Into<String>, partition_count: u32) -> Result<Self> {
        if partition_count == 0 || partition_count > 4096 {
            return Err(DatspoolError::Configuration(
                "partition_count must be between 1 and 4096".into(),
            ));
        }
        Ok(Self {
            pool,
            stream_key: stream_key.into(),
            partition_count,
        })
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dataspool_cdc_outbox_partition_streams (\
                stream_key TEXT PRIMARY KEY, partition_count INTEGER NOT NULL CHECK (partition_count > 0),                created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())",
        ).execute(&self.pool).await.map_err(|e| db_error("create partitioned stream registry", e))?;
        sqlx::query("INSERT INTO dataspool_cdc_outbox_partition_streams (stream_key, partition_count) VALUES ($1, $2) ON CONFLICT (stream_key) DO NOTHING")
            .bind(&self.stream_key).bind(self.partition_count as i32).execute(&self.pool).await
            .map_err(|e| db_error("register partition topology", e))?;
        let registered: i32 = sqlx::query("SELECT partition_count FROM dataspool_cdc_outbox_partition_streams WHERE stream_key = $1")
            .bind(&self.stream_key).fetch_one(&self.pool).await
            .map_err(|e| db_error("read registered partition topology", e))?.try_get("partition_count")
            .map_err(|e| db_error("decode registered partition topology", e))?;
        if registered != self.partition_count as i32 {
            return Err(DatspoolError::Configuration(format!(
                "partition topology for stream '{}' is immutable: registered {}, requested {}",
                self.stream_key, registered, self.partition_count
            )));
        }
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dataspool_cdc_outbox_partition_gates (\
                stream_key TEXT NOT NULL, partition_id INTEGER NOT NULL, paused BOOLEAN NOT NULL DEFAULT FALSE,\
                last_sequence BIGINT NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),\
                PRIMARY KEY (stream_key, partition_id))",
        ).execute(&self.pool).await.map_err(|e| db_error("create partitioned Outbox gates", e))?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dataspool_cdc_outbox_partitioned (\
                stream_key TEXT NOT NULL, partition_id INTEGER NOT NULL, local_sequence BIGINT NOT NULL,\
                event_id TEXT NOT NULL, event_id_bucket SMALLINT NOT NULL, operation TEXT NOT NULL, document_id TEXT NOT NULL,\
                target_index TEXT NOT NULL, document JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),\
                PRIMARY KEY (stream_key, partition_id, local_sequence),\
                FOREIGN KEY (stream_key, partition_id) REFERENCES dataspool_cdc_outbox_partition_gates(stream_key, partition_id))",
        ).execute(&self.pool).await.map_err(|e| db_error("create partitioned Outbox events", e))?;
        sqlx::query("ALTER TABLE dataspool_cdc_outbox_partitioned ADD COLUMN IF NOT EXISTS event_id_bucket SMALLINT")
            .execute(&self.pool).await.map_err(|e| db_error("add partitioned event ID bucket", e))?;
        sqlx::query("UPDATE dataspool_cdc_outbox_partitioned SET event_id_bucket = get_byte(decode(replace(event_id, '-', ''), 'hex'), 0) % 16 WHERE event_id_bucket IS NULL")
            .execute(&self.pool).await.map_err(|e| db_error("backfill partitioned event ID buckets", e))?;
        sqlx::query("ALTER TABLE dataspool_cdc_outbox_partitioned ALTER COLUMN event_id_bucket SET NOT NULL")
            .execute(&self.pool).await.map_err(|e| db_error("require partitioned event ID bucket", e))?;
        sqlx::query("ALTER TABLE dataspool_cdc_outbox_partitioned DROP CONSTRAINT IF EXISTS dataspool_cdc_outbox_partitioned_event_id_key")
            .execute(&self.pool).await.map_err(|e| db_error("remove global event ID index", e))?;
        sqlx::query("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dataspool_cdc_outbox_partitioned_event_id_bucket_check') THEN ALTER TABLE dataspool_cdc_outbox_partitioned ADD CONSTRAINT dataspool_cdc_outbox_partitioned_event_id_bucket_check CHECK (event_id_bucket = get_byte(decode(replace(event_id, '-', ''), 'hex'), 0) % 16); END IF; END $$")
            .execute(&self.pool).await.map_err(|e| db_error("enforce deterministic event ID bucket", e))?;
        for bucket in 0..PARTITIONED_EVENT_ID_BUCKETS {
            let index = format!("dataspool_cdc_outbox_partitioned_event_id_{bucket:02}_uidx");
            let ddl = format!(
                "CREATE UNIQUE INDEX IF NOT EXISTS {index} ON dataspool_cdc_outbox_partitioned (event_id) WHERE event_id_bucket = {bucket}"
            );
            sqlx::query(&ddl)
                .execute(&self.pool)
                .await
                .map_err(|e| db_error("create bucketed event ID idempotency index", e))?;
        }
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dataspool_cdc_outbox_partition_consumers (\
                stream_key TEXT NOT NULL, consumer_key TEXT NOT NULL, partition_id INTEGER NOT NULL,\
                last_sequence BIGINT NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),\
                PRIMARY KEY (stream_key, consumer_key, partition_id),\
                FOREIGN KEY (stream_key, partition_id) REFERENCES dataspool_cdc_outbox_partition_gates(stream_key, partition_id))",
        ).execute(&self.pool).await.map_err(|e| db_error("create partitioned consumer checkpoints", e))?;
        sqlx::query(
            "INSERT INTO dataspool_cdc_outbox_partition_gates (stream_key, partition_id) \
             SELECT $1, generated.partition_id FROM generate_series(0, $2 - 1) AS generated(partition_id) \
             ON CONFLICT (stream_key, partition_id) DO NOTHING",
        )
        .bind(&self.stream_key)
        .bind(self.partition_count as i32)
        .execute(&self.pool)
        .await
        .map_err(|e| db_error("initialize partition gates", e))?;
        Ok(())
    }

    pub async fn begin_write(
        &self,
        partition_id: u32,
    ) -> Result<PostgresPartitionedOutboxWrite<'_>> {
        self.validate_partition(partition_id)?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error("begin partitioned source write", e))?;
        sqlx::query("SELECT set_config('dataspool.outbox_stream_key', $1, TRUE)")
            .bind(&self.stream_key)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|e| db_error("authorize partitioned source writes", e))?;
        Ok(PostgresPartitionedOutboxWrite {
            transaction: Some(transaction),
            stream_key: self.stream_key.clone(),
            partition_id,
            gate_locked: false,
            event_appended: false,
            failed: false,
        })
    }

    /// Stable SHA-256 based assignment. The partition count is immutable for
    /// a stream, so a key keeps its partition for the stream's lifetime.
    pub fn partition_for_key(&self, key: &[u8]) -> u32 {
        let digest = sha2::Sha256::digest(key);
        let value = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
        value % self.partition_count
    }

    fn validate_partition(&self, partition_id: u32) -> Result<()> {
        if partition_id >= self.partition_count {
            return Err(DatspoolError::Configuration(format!(
                "partition_id {partition_id} is outside 0..{}",
                self.partition_count
            )));
        }
        Ok(())
    }

    pub async fn vector_watermark(&self) -> Result<PostgresOutboxVectorWatermark> {
        let rows = sqlx::query("SELECT partition_id, last_sequence FROM dataspool_cdc_outbox_partition_gates WHERE stream_key = $1 ORDER BY partition_id")
            .bind(&self.stream_key).fetch_all(&self.pool).await.map_err(|e| db_error("read partition watermark vector", e))?;
        let mut vector = PostgresOutboxVectorWatermark::new();
        for row in rows {
            let partition: i32 = row
                .try_get("partition_id")
                .map_err(|e| db_error("decode partition id", e))?;
            let sequence: i64 = row
                .try_get("last_sequence")
                .map_err(|e| db_error("decode partition sequence", e))?;
            vector.insert(
                partition as u32,
                u64::try_from(sequence)
                    .map_err(|_| DatspoolError::Internal("negative partition watermark".into()))?,
            );
        }
        if vector.len() != self.partition_count as usize {
            return Err(DatspoolError::Internal(
                "partition watermark vector is incomplete".into(),
            ));
        }
        Ok(vector)
    }

    pub async fn register_consumer(&self, consumer_key: &str) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error("begin partition consumer registration", e))?;
        let gates = sqlx::query("SELECT partition_id FROM dataspool_cdc_outbox_partition_gates WHERE stream_key = $1 ORDER BY partition_id FOR UPDATE")
            .bind(&self.stream_key).fetch_all(&mut *tx).await.map_err(|e| db_error("lock partitions for consumer registration", e))?;
        if gates.len() != self.partition_count as usize {
            return Err(DatspoolError::Internal(
                "cannot register a consumer for an incomplete partition topology".into(),
            ));
        }
        sqlx::query(
            "INSERT INTO dataspool_cdc_outbox_partition_consumers (stream_key, consumer_key, partition_id, last_sequence) \
             SELECT stream_key, $2, partition_id, 0 FROM dataspool_cdc_outbox_partition_gates WHERE stream_key = $1 \
             ON CONFLICT (stream_key, consumer_key, partition_id) DO NOTHING",
        ).bind(&self.stream_key).bind(consumer_key).execute(&mut *tx).await
            .map_err(|e| db_error("register partitioned Outbox consumer", e))?;
        tx.commit()
            .await
            .map_err(|e| db_error("commit partition consumer registration", e))
    }

    /// Atomically advance the complete vector for one consumer. Partial vectors
    /// are rejected so a checkpoint always describes every partition.
    pub async fn record_consumer_checkpoint_vector(
        &self,
        consumer_key: &str,
        checkpoint: &PostgresOutboxVectorWatermark,
    ) -> Result<()> {
        if checkpoint.len() != self.partition_count as usize
            || (0..self.partition_count).any(|partition| !checkpoint.contains_key(&partition))
        {
            return Err(DatspoolError::Configuration(
                "consumer checkpoint must contain every partition exactly once".into(),
            ));
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error("begin vector checkpoint", e))?;
        let gates = sqlx::query("SELECT partition_id, last_sequence FROM dataspool_cdc_outbox_partition_gates WHERE stream_key = $1 ORDER BY partition_id FOR UPDATE")
            .bind(&self.stream_key).fetch_all(&mut *tx).await.map_err(|e| db_error("lock partitions for vector checkpoint", e))?;
        for gate in gates {
            let partition: i32 = gate
                .try_get("partition_id")
                .map_err(|e| db_error("decode checkpoint partition", e))?;
            let current: i64 = gate
                .try_get("last_sequence")
                .map_err(|e| db_error("decode partition high watermark", e))?;
            let requested = i64::try_from(checkpoint[&(partition as u32)]).map_err(|_| {
                DatspoolError::Configuration("partition checkpoint exceeds BIGINT".into())
            })?;
            if requested > current {
                return Err(DatspoolError::Internal(format!(
                    "partition {partition} checkpoint {requested} exceeds committed watermark {current}"
                )));
            }
            let previous = sqlx::query("SELECT last_sequence FROM dataspool_cdc_outbox_partition_consumers WHERE stream_key = $1 AND consumer_key = $2 AND partition_id = $3 FOR UPDATE")
                .bind(&self.stream_key).bind(consumer_key).bind(partition)
                .fetch_optional(&mut *tx).await.map_err(|e| db_error("read prior vector checkpoint", e))?
                .ok_or_else(|| DatspoolError::Configuration(format!("consumer '{consumer_key}' is not registered for partition {partition}")))?;
            let previous: i64 = previous
                .try_get("last_sequence")
                .map_err(|e| db_error("decode prior vector checkpoint", e))?;
            if requested < previous {
                return Err(DatspoolError::Internal(format!(
                    "partition {partition} checkpoint regression: {requested} < {previous}"
                )));
            }
            sqlx::query("UPDATE dataspool_cdc_outbox_partition_consumers SET last_sequence = $4, updated_at = clock_timestamp() WHERE stream_key = $1 AND consumer_key = $2 AND partition_id = $3")
                .bind(&self.stream_key).bind(consumer_key).bind(partition).bind(requested)
                .execute(&mut *tx).await.map_err(|e| db_error("advance vector checkpoint", e))?;
        }
        tx.commit()
            .await
            .map_err(|e| db_error("commit vector checkpoint", e))
    }

    /// Read a consumer's durable vector checkpoint after reconnect/restart.
    pub async fn consumer_checkpoint_vector(
        &self,
        consumer_key: &str,
    ) -> Result<PostgresOutboxVectorWatermark> {
        let rows = sqlx::query("SELECT partition_id, last_sequence FROM dataspool_cdc_outbox_partition_consumers WHERE stream_key = $1 AND consumer_key = $2 ORDER BY partition_id")
            .bind(&self.stream_key).bind(consumer_key).fetch_all(&self.pool).await
            .map_err(|e| db_error("read durable partition checkpoint vector", e))?;
        let mut vector = PostgresOutboxVectorWatermark::new();
        for row in rows {
            let partition: i32 = row
                .try_get("partition_id")
                .map_err(|e| db_error("decode durable checkpoint partition", e))?;
            let sequence: i64 = row
                .try_get("last_sequence")
                .map_err(|e| db_error("decode durable checkpoint sequence", e))?;
            vector.insert(
                partition as u32,
                u64::try_from(sequence).map_err(|_| {
                    DatspoolError::Internal("negative durable partition checkpoint".into())
                })?,
            );
        }
        if vector.len() != self.partition_count as usize {
            return Err(DatspoolError::Configuration(format!(
                "consumer '{consumer_key}' is not registered across the complete partition topology"
            )));
        }
        Ok(vector)
    }

    /// Read each partition independently from a complete vector offset. The
    /// returned map preserves partition identity and local ordering domains.
    pub async fn read_vector_after(
        &self,
        offsets: &PostgresOutboxVectorWatermark,
        limit_per_partition: i64,
    ) -> Result<std::collections::BTreeMap<u32, Vec<PostgresPartitionedOutboxEvent>>> {
        if offsets.len() != self.partition_count as usize
            || (0..self.partition_count).any(|partition| !offsets.contains_key(&partition))
        {
            return Err(DatspoolError::Configuration(
                "partition offsets must contain every partition".into(),
            ));
        }
        let mut result = std::collections::BTreeMap::new();
        for (partition, sequence) in offsets {
            result.insert(
                *partition,
                self.read_partition_after(*partition, *sequence, limit_per_partition)
                    .await?,
            );
        }
        Ok(result)
    }

    pub async fn read_partition_after(
        &self,
        partition_id: u32,
        local_sequence: u64,
        limit: i64,
    ) -> Result<Vec<PostgresPartitionedOutboxEvent>> {
        self.validate_partition(partition_id)?;
        let local_sequence = i64::try_from(local_sequence).map_err(|_| {
            DatspoolError::Configuration("partition offset exceeds PostgreSQL BIGINT".into())
        })?;
        let rows = sqlx::query("SELECT partition_id, local_sequence, event_id, stream_key, operation, document_id, target_index, document FROM dataspool_cdc_outbox_partitioned WHERE stream_key = $1 AND partition_id = $2 AND local_sequence > $3 ORDER BY local_sequence LIMIT $4")
            .bind(&self.stream_key).bind(partition_id as i32).bind(local_sequence).bind(limit.clamp(1, 10_000))
            .fetch_all(&self.pool).await.map_err(|e| db_error("read partitioned Outbox events", e))?;
        rows.into_iter()
            .map(|row| {
                Ok(PostgresPartitionedOutboxEvent {
                    partition_id: row
                        .try_get::<i32, _>("partition_id")
                        .map_err(|e| db_error("decode event partition", e))?
                        as u32,
                    local_sequence: row
                        .try_get("local_sequence")
                        .map_err(|e| db_error("decode local sequence", e))?,
                    event_id: row
                        .try_get("event_id")
                        .map_err(|e| db_error("decode partition event ID", e))?,
                    stream_key: row
                        .try_get("stream_key")
                        .map_err(|e| db_error("decode partition stream", e))?,
                    operation: row
                        .try_get("operation")
                        .map_err(|e| db_error("decode partition operation", e))?,
                    document_id: row
                        .try_get("document_id")
                        .map_err(|e| db_error("decode partition document ID", e))?,
                    target_index: row
                        .try_get("target_index")
                        .map_err(|e| db_error("decode partition target", e))?,
                    document: row
                        .try_get("document")
                        .map_err(|e| db_error("decode partition document", e))?,
                })
            })
            .collect()
    }

    /// Observe the partitioned event table's dead tuples, size, autovacuum,
    /// and cluster-wide WAL counter (where supported by the PostgreSQL version).
    pub async fn retention_stats(&self) -> Result<PostgresOutboxRetentionStats> {
        let row = sqlx::query(
            "SELECT n_live_tup, n_dead_tup, autovacuum_count, last_autovacuum::text AS last_autovacuum, pg_total_relation_size(relid) AS total_bytes FROM pg_stat_user_tables WHERE schemaname = current_schema() AND relname = 'dataspool_cdc_outbox_partitioned'",
        ).fetch_optional(&self.pool).await.map_err(|e| db_error("read partitioned Outbox retention statistics", e))?
            .ok_or_else(|| DatspoolError::Internal("partitioned Outbox table statistics are unavailable in the current schema".into()))?;
        let cluster_wal_bytes =
            sqlx::query("SELECT wal_bytes::bigint AS wal_bytes FROM pg_stat_wal")
                .fetch_one(&self.pool)
                .await
                .ok()
                .and_then(|row| row.try_get::<i64, _>("wal_bytes").ok());
        Ok(PostgresOutboxRetentionStats {
            live_rows_estimate: row
                .try_get("n_live_tup")
                .map_err(|e| db_error("decode partitioned live tuple estimate", e))?,
            dead_rows_estimate: row
                .try_get("n_dead_tup")
                .map_err(|e| db_error("decode partitioned dead tuple estimate", e))?,
            total_bytes: row
                .try_get("total_bytes")
                .map_err(|e| db_error("decode partitioned table size", e))?,
            autovacuum_count: row
                .try_get("autovacuum_count")
                .map_err(|e| db_error("decode partitioned autovacuum count", e))?,
            last_autovacuum: row
                .try_get("last_autovacuum")
                .map_err(|e| db_error("decode partitioned last autovacuum", e))?,
            cluster_wal_bytes,
        })
    }

    pub async fn prune_partition_batch(&self, partition_id: u32, batch_size: u32) -> Result<u64> {
        let started = std::time::Instant::now();
        let wal_before = sqlx::query("SELECT wal_bytes::bigint AS wal_bytes FROM pg_stat_wal")
            .fetch_one(&self.pool)
            .await
            .ok()
            .and_then(|row| row.try_get::<i64, _>("wal_bytes").ok());
        self.validate_partition(partition_id)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error("begin partition retention batch", e))?;
        sqlx::query("SELECT partition_id FROM dataspool_cdc_outbox_partition_gates WHERE stream_key = $1 AND partition_id = $2 FOR UPDATE")
            .bind(&self.stream_key).bind(partition_id as i32).fetch_one(&mut *tx).await.map_err(|e| db_error("lock partition for retention", e))?;
        let row = sqlx::query("SELECT MIN(last_sequence) AS watermark FROM dataspool_cdc_outbox_partition_consumers WHERE stream_key = $1 AND partition_id = $2")
            .bind(&self.stream_key).bind(partition_id as i32).fetch_one(&mut *tx).await.map_err(|e| db_error("read partition retention watermark", e))?;
        let Some(watermark) = row
            .try_get::<Option<i64>, _>("watermark")
            .map_err(|e| db_error("decode partition retention watermark", e))?
        else {
            tx.rollback()
                .await
                .map_err(|e| db_error("rollback untracked partition retention", e))?;
            return Ok(0);
        };
        let deleted = sqlx::query("WITH victims AS (SELECT ctid FROM dataspool_cdc_outbox_partitioned WHERE stream_key = $1 AND partition_id = $2 AND local_sequence <= $3 ORDER BY local_sequence LIMIT $4) DELETE FROM dataspool_cdc_outbox_partitioned events USING victims WHERE events.ctid = victims.ctid")
            .bind(&self.stream_key).bind(partition_id as i32).bind(watermark).bind(batch_size.clamp(1, 10_000) as i64)
            .execute(&mut *tx).await.map_err(|e| db_error("delete partition retention batch", e))?.rows_affected();
        tx.commit()
            .await
            .map_err(|e| db_error("commit partition retention batch", e))?;
        let elapsed = started.elapsed();
        let wal_after = sqlx::query("SELECT wal_bytes::bigint AS wal_bytes FROM pg_stat_wal")
            .fetch_one(&self.pool)
            .await
            .ok()
            .and_then(|row| row.try_get::<i64, _>("wal_bytes").ok());
        let wal_delta = wal_before
            .zip(wal_after)
            .map(|(before, after)| (after - before).max(0));
        tracing::info!(stream_key = %self.stream_key, partition_id, deleted_rows = deleted,
            elapsed_ms = elapsed.as_millis() as u64,
            rows_per_second = deleted as f64 / elapsed.as_secs_f64().max(0.001),
            wal_bytes_delta = ?wal_delta,
            "PostgreSQL partitioned Outbox retention batch completed");
        Ok(deleted)
    }
}

#[async_trait]
impl PartitionedCdcWriteBarrier for PostgresPartitionedOutbox {
    async fn pause_and_capture_vector_watermark(
        &mut self,
    ) -> Result<PostgresOutboxVectorWatermark> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db_error("begin partition vector cutover", e))?;
        let rows = sqlx::query("SELECT partition_id, last_sequence FROM dataspool_cdc_outbox_partition_gates WHERE stream_key = $1 ORDER BY partition_id FOR UPDATE")
            .bind(&self.stream_key).fetch_all(&mut *tx).await.map_err(|e| db_error("lock all partitions for cutover", e))?;
        if rows.len() != self.partition_count as usize {
            return Err(DatspoolError::Internal(
                "cannot cut over an incomplete partition set".into(),
            ));
        }
        let mut vector = PostgresOutboxVectorWatermark::new();
        for row in rows {
            let partition: i32 = row
                .try_get("partition_id")
                .map_err(|e| db_error("decode cutover partition", e))?;
            let sequence: i64 = row
                .try_get("last_sequence")
                .map_err(|e| db_error("decode cutover local watermark", e))?;
            vector.insert(
                partition as u32,
                u64::try_from(sequence).map_err(|_| {
                    DatspoolError::Internal("negative partition cutover watermark".into())
                })?,
            );
        }
        sqlx::query("UPDATE dataspool_cdc_outbox_partition_gates SET paused = TRUE, updated_at = clock_timestamp() WHERE stream_key = $1")
            .bind(&self.stream_key).execute(&mut *tx).await.map_err(|e| db_error("pause all Outbox partitions", e))?;
        tx.commit()
            .await
            .map_err(|e| db_error("commit vector cutover barrier", e))?;
        Ok(vector)
    }

    async fn resume_partitioned_writes(&mut self) -> Result<()> {
        sqlx::query("UPDATE dataspool_cdc_outbox_partition_gates SET paused = FALSE, updated_at = clock_timestamp() WHERE stream_key = $1")
            .bind(&self.stream_key).execute(&self.pool).await.map_err(|e| db_error("resume all Outbox partitions", e))?;
        Ok(())
    }
}

pub struct PostgresPartitionedOutboxWrite<'a> {
    transaction: Option<Transaction<'a, Postgres>>,
    stream_key: String,
    partition_id: u32,
    gate_locked: bool,
    event_appended: bool,
    failed: bool,
}

impl PostgresPartitionedOutboxWrite<'_> {
    async fn lock_write_gate(&mut self) -> Result<()> {
        if self.gate_locked {
            return Ok(());
        }
        let stream_key = self.stream_key.clone();
        let partition_id = self.partition_id;
        let row = sqlx::query(
            "SELECT paused FROM dataspool_cdc_outbox_partition_gates WHERE stream_key = $1 AND partition_id = $2 FOR UPDATE",
        )
        .bind(&stream_key)
        .bind(partition_id as i32)
        .fetch_one(self.connection())
        .await
        .map_err(|e| db_error("lock partitioned Outbox gate", e))?;
        let paused: bool = row
            .try_get("paused")
            .map_err(|e| db_error("read partition pause state", e))?;
        if paused {
            return Err(DatspoolError::Internal(format!(
                "CDC stream '{}' partition {} is paused",
                self.stream_key, self.partition_id
            )));
        }
        self.gate_locked = true;
        Ok(())
    }

    pub fn connection(&mut self) -> &mut sqlx::PgConnection {
        self.transaction
            .as_mut()
            .expect("transaction already committed or rolled back")
    }

    /// Append a batch within this partition using one idempotency lookup,
    /// one contiguous sequence reservation, and one multi-row INSERT.
    /// Returned sequences preserve input order; replayed IDs retain their
    /// original local sequence.
    pub async fn append_events(&mut self, events: &[PostgresOutboxEventInput]) -> Result<Vec<u64>> {
        let result = self.append_events_inner(events).await;
        if result.is_err() {
            self.failed = true;
        } else {
            self.event_appended = true;
        }
        result
    }

    async fn append_events_inner(
        &mut self,
        events: &[PostgresOutboxEventInput],
    ) -> Result<Vec<u64>> {
        self.lock_write_gate().await?;
        if events.is_empty() || events.len() > 5_000 {
            return Err(DatspoolError::Configuration(
                "partitioned Outbox batches must contain 1..=5,000 events".into(),
            ));
        }
        let mut seen = std::collections::HashSet::with_capacity(events.len());
        for event in events {
            if !seen.insert(event.event_id) {
                return Err(DatspoolError::Configuration(format!(
                    "duplicate event ID {} in one partitioned Outbox batch",
                    event.event_id
                )));
            }
            if !matches!(event.operation.as_str(), "index" | "delete")
                || event.document_id.is_empty()
                || event.target_index.is_empty()
                || (event.operation == "index" && !event.document.is_object())
            {
                return Err(DatspoolError::Driver(DriverError::DataError(
                    "invalid partitioned Outbox event".into(),
                )));
            }
        }
        let mut ids_by_bucket = std::collections::BTreeMap::<i16, Vec<String>>::new();
        for event in events {
            ids_by_bucket
                .entry(partitioned_event_id_bucket(event.event_id))
                .or_default()
                .push(event.event_id.to_string());
        }
        let mut lookup = QueryBuilder::<Postgres>::new(
            "SELECT partition_id, local_sequence, stream_key, event_id, operation, document_id, target_index, document FROM dataspool_cdc_outbox_partitioned WHERE ",
        );
        for (index, (bucket, bucket_ids)) in ids_by_bucket.iter().enumerate() {
            if index > 0 {
                lookup.push(" OR ");
            }
            lookup
                .push("(event_id_bucket = ")
                .push(*bucket)
                .push(" AND event_id = ANY(")
                .push_bind(bucket_ids)
                .push("))");
        }
        let rows = lookup
            .build()
            .fetch_all(self.connection())
            .await
            .map_err(|error| db_error("resolve partitioned batch idempotency keys", error))?;
        let mut existing = std::collections::HashMap::with_capacity(rows.len());
        for row in rows {
            let id: String = row
                .try_get("event_id")
                .map_err(|e| db_error("decode partitioned event ID", e))?;
            let same = row.try_get::<String, _>("stream_key").ok().as_deref()
                == Some(self.stream_key.as_str())
                && row.try_get::<i32, _>("partition_id").ok() == Some(self.partition_id as i32);
            if !same {
                return Err(DatspoolError::Driver(DriverError::DataError(format!(
                    "partitioned Outbox event ID {id} already belongs to another stream or partition"
                ))));
            }
            existing.insert(id, row);
        }
        let mut sequences = vec![None; events.len()];
        let mut new_indices = Vec::with_capacity(events.len());
        for (index, event) in events.iter().enumerate() {
            if let Some(row) = existing.get(&event.event_id.to_string()) {
                let same = row.try_get::<String, _>("operation").ok().as_deref()
                    == Some(event.operation.as_str())
                    && row.try_get::<String, _>("document_id").ok().as_deref()
                        == Some(event.document_id.as_str())
                    && row.try_get::<String, _>("target_index").ok().as_deref()
                        == Some(event.target_index.as_str())
                    && row.try_get::<Value, _>("document").ok().as_ref() == Some(&event.document);
                if !same {
                    return Err(DatspoolError::Driver(DriverError::DataError(format!(
                        "partitioned Outbox event ID {} was reused with different content",
                        event.event_id
                    ))));
                }
                let sequence: i64 = row
                    .try_get("local_sequence")
                    .map_err(|e| db_error("decode replayed partition sequence", e))?;
                sequences[index] =
                    Some(u64::try_from(sequence).map_err(|_| {
                        DatspoolError::Internal("negative partition sequence".into())
                    })?);
            } else {
                new_indices.push(index);
            }
        }
        if !new_indices.is_empty() {
            let count = i64::try_from(new_indices.len())
                .map_err(|_| DatspoolError::Configuration("partition batch too large".into()))?;
            let stream_key = self.stream_key.clone();
            let partition_id = self.partition_id;
            let gate = sqlx::query(
                "UPDATE dataspool_cdc_outbox_partition_gates SET last_sequence = last_sequence + $3, updated_at = clock_timestamp() WHERE stream_key = $1 AND partition_id = $2 RETURNING last_sequence - $3 + 1 AS first_sequence",
            ).bind(&stream_key).bind(partition_id as i32).bind(count)
                .fetch_one(self.connection()).await
                .map_err(|e| db_error("reserve partition sequence range", e))?;
            let first: i64 = gate
                .try_get("first_sequence")
                .map_err(|e| db_error("decode partition sequence range", e))?;
            let mut query = QueryBuilder::<Postgres>::new(
                "INSERT INTO dataspool_cdc_outbox_partitioned (stream_key, partition_id, local_sequence, event_id, event_id_bucket, operation, document_id, target_index, document) ",
            );
            query.push_values(
                new_indices.iter().enumerate(),
                |mut row, (offset, index)| {
                    let event = &events[*index];
                    row.push_bind(&stream_key)
                        .push_bind(partition_id as i32)
                        .push_bind(first + offset as i64)
                        .push_bind(event.event_id.to_string())
                        .push_bind(partitioned_event_id_bucket(event.event_id))
                        .push_bind(&event.operation)
                        .push_bind(&event.document_id)
                        .push_bind(&event.target_index)
                        .push_bind(&event.document);
                },
            );
            query
                .build()
                .execute(self.connection())
                .await
                .map_err(|e| db_error("bulk insert partitioned Outbox events", e))?;
            for (offset, index) in new_indices.into_iter().enumerate() {
                sequences[index] =
                    Some(u64::try_from(first + offset as i64).map_err(|_| {
                        DatspoolError::Internal("negative partition sequence".into())
                    })?);
            }
        }
        sequences
            .into_iter()
            .map(|value| {
                value.ok_or_else(|| {
                    DatspoolError::Internal("missing partition batch sequence".into())
                })
            })
            .collect()
    }

    pub async fn append_event(
        &mut self,
        event_id: uuid::Uuid,
        operation: &str,
        document_id: &str,
        target_index: &str,
        document: &Value,
    ) -> Result<u64> {
        let stream_key = self.stream_key.clone();
        let partition_id = self.partition_id;
        let result = async {
            self.lock_write_gate().await?;
            if !matches!(operation, "index" | "delete") || document_id.is_empty() || target_index.is_empty() || (operation == "index" && !document.is_object()) {
                return Err(DatspoolError::Driver(DriverError::DataError("invalid partitioned Outbox event".into())));
            }
            let existing = sqlx::query("SELECT partition_id, local_sequence, stream_key, operation, document_id, target_index, document FROM dataspool_cdc_outbox_partitioned WHERE event_id_bucket = $1 AND event_id = $2")
                .bind(partitioned_event_id_bucket(event_id)).bind(event_id.to_string()).fetch_optional(self.connection()).await.map_err(|e| db_error("check partition event idempotency", e))?;
            if let Some(row) = existing {
                let same = row.try_get::<String, _>("stream_key").ok().as_deref() == Some(self.stream_key.as_str())
                    && row.try_get::<i32, _>("partition_id").ok() == Some(self.partition_id as i32)
                    && row.try_get::<String, _>("operation").ok().as_deref() == Some(operation)
                    && row.try_get::<String, _>("document_id").ok().as_deref() == Some(document_id)
                    && row.try_get::<String, _>("target_index").ok().as_deref() == Some(target_index)
                    && row.try_get::<Value, _>("document").ok().as_ref() == Some(document);
                if !same { return Err(DatspoolError::Driver(DriverError::DataError("partitioned Outbox event ID reused with different content or partition".into()))); }
                let sequence: i64 = row.try_get("local_sequence").map_err(|e| db_error("decode idempotent local sequence", e))?;
                return u64::try_from(sequence).map_err(|_| DatspoolError::Internal("negative local sequence".into()));
            }
            let row = sqlx::query("UPDATE dataspool_cdc_outbox_partition_gates SET last_sequence = last_sequence + 1, updated_at = clock_timestamp() WHERE stream_key = $1 AND partition_id = $2 RETURNING last_sequence")
                .bind(&stream_key).bind(partition_id as i32).fetch_one(self.connection()).await.map_err(|e| db_error("allocate partition-local sequence", e))?;
            let sequence: i64 = row.try_get("last_sequence").map_err(|e| db_error("decode partition-local sequence", e))?;
            sqlx::query("INSERT INTO dataspool_cdc_outbox_partitioned (stream_key, partition_id, local_sequence, event_id, event_id_bucket, operation, document_id, target_index, document) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)")
                .bind(&stream_key).bind(partition_id as i32).bind(sequence).bind(event_id.to_string()).bind(partitioned_event_id_bucket(event_id)).bind(operation).bind(document_id).bind(target_index).bind(document)
                .execute(self.connection()).await.map_err(|e| db_error("insert partitioned Outbox event", e))?;
            u64::try_from(sequence).map_err(|_| DatspoolError::Internal("negative local sequence".into()))
        }.await;
        if result.is_err() {
            self.failed = true;
        } else {
            self.event_appended = true;
        }
        result
    }

    pub async fn commit(mut self) -> Result<()> {
        let tx = self
            .transaction
            .take()
            .expect("transaction already committed");
        if self.failed || !self.event_appended {
            tx.rollback()
                .await
                .map_err(|e| db_error("rollback failed or empty partition write", e))?;
            return Err(DatspoolError::Internal("partitioned source transaction requires at least one successful Outbox event and no failed append".into()));
        }
        tx.commit()
            .await
            .map_err(|e| db_error("commit partitioned source and Outbox event", e))
    }
}

pub struct PostgresOutboxWrite<'a> {
    transaction: Option<Transaction<'a, Postgres>>,
    stream_key: String,
    event_appended: bool,
    failed: bool,
}

impl PostgresOutboxWrite<'_> {
    /// The source mutation must use this connection so it commits atomically with the Outbox event.
    pub fn connection(&mut self) -> &mut sqlx::PgConnection {
        self.transaction
            .as_mut()
            .expect("transaction already committed or rolled back")
    }

    /// Append several source-change events in the same producer transaction.
    /// The caller performs all source mutations through `connection()`, calls
    /// this once, then commits once. Sequence allocation remains serialized by
    /// the stream gate and follows input order.
    pub async fn append_events(&mut self, events: &[PostgresOutboxEventInput]) -> Result<Vec<u64>> {
        let result = self.append_events_inner(events).await;
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    async fn append_events_inner(
        &mut self,
        events: &[PostgresOutboxEventInput],
    ) -> Result<Vec<u64>> {
        if events.is_empty() {
            return Err(DatspoolError::Configuration(
                "a batch must contain at least one Outbox event".into(),
            ));
        }
        if events.len() > 5_000 {
            return Err(DatspoolError::Configuration(
                "Outbox batches are limited to 5,000 events per transaction".into(),
            ));
        }

        let mut event_ids = std::collections::HashSet::with_capacity(events.len());
        for event in events {
            if !event_ids.insert(event.event_id) {
                return Err(DatspoolError::Configuration(format!(
                    "duplicate event ID {} in one Outbox batch",
                    event.event_id
                )));
            }
            if !matches!(event.operation.as_str(), "index" | "delete")
                || event.document_id.is_empty()
                || event.target_index.is_empty()
                || (event.operation == "index" && !event.document.is_object())
            {
                return Err(DatspoolError::Driver(DriverError::DataError(
                    "Outbox events require operation index/delete, non-empty document/target identifiers, and object documents for index operations".into(),
                )));
            }
        }

        // Resolve all idempotency keys with one round trip. Existing rows must
        // match byte-for-byte JSON semantics and all routing fields.
        let ids = events
            .iter()
            .map(|event| event.event_id.to_string())
            .collect::<Vec<_>>();
        let existing_rows = sqlx::query(
            "SELECT sequence, event_id, stream_key, operation, document_id, target_index, document              FROM dataspool_cdc_outbox WHERE event_id = ANY($1)",
        )
        .bind(&ids)
        .fetch_all(self.connection())
        .await
        .map_err(|error| db_error("resolve batched Outbox idempotency keys", error))?;
        let mut existing = std::collections::HashMap::with_capacity(existing_rows.len());
        for row in existing_rows {
            let event_id: String = row
                .try_get("event_id")
                .map_err(|error| db_error("decode existing batch event ID", error))?;
            let same = row.try_get::<String, _>("stream_key").ok().as_deref()
                == Some(self.stream_key.as_str());
            if !same {
                return Err(DatspoolError::Driver(DriverError::DataError(format!(
                    "Outbox event ID {event_id} already belongs to another stream"
                ))));
            }
            existing.insert(event_id, row);
        }

        let mut sequences = vec![None; events.len()];
        let mut new_indices = Vec::with_capacity(events.len());
        for (index, event) in events.iter().enumerate() {
            if let Some(row) = existing.get(&event.event_id.to_string()) {
                let same_event = row.try_get::<String, _>("operation").ok().as_deref()
                    == Some(event.operation.as_str())
                    && row.try_get::<String, _>("document_id").ok().as_deref()
                        == Some(event.document_id.as_str())
                    && row.try_get::<String, _>("target_index").ok().as_deref()
                        == Some(event.target_index.as_str())
                    && row.try_get::<Value, _>("document").ok().as_ref() == Some(&event.document);
                if !same_event {
                    return Err(DatspoolError::Driver(DriverError::DataError(format!(
                        "Outbox event ID {} was reused with different event content",
                        event.event_id
                    ))));
                }
                let sequence: i64 = row
                    .try_get("sequence")
                    .map_err(|error| db_error("decode existing batch sequence", error))?;
                sequences[index] = Some(u64::try_from(sequence).map_err(|_| {
                    DatspoolError::Internal("negative PostgreSQL Outbox sequence".into())
                })?);
            } else {
                new_indices.push(index);
            }
        }

        if !new_indices.is_empty() {
            let stream_key = self.stream_key.clone();
            let new_count = i64::try_from(new_indices.len())
                .map_err(|_| DatspoolError::Configuration("Outbox batch is too large".into()))?;
            let gate = sqlx::query(
                "UPDATE dataspool_cdc_outbox_gate SET last_sequence = last_sequence + $2, updated_at = clock_timestamp() WHERE stream_key = $1 RETURNING last_sequence - $2 + 1 AS first_sequence",
            )
            .bind(&stream_key)
            .bind(new_count)
            .fetch_one(self.connection())
            .await
            .map_err(|error| db_error("reserve contiguous Outbox sequence range", error))?;
            let first_sequence: i64 = gate
                .try_get("first_sequence")
                .map_err(|error| db_error("decode first reserved Outbox sequence", error))?;

            let mut query = QueryBuilder::<Postgres>::new(
                "INSERT INTO dataspool_cdc_outbox (sequence, event_id, stream_key, operation, document_id, target_index, document) ",
            );
            query.push_values(
                new_indices.iter().enumerate(),
                |mut row, (offset, index)| {
                    let event = &events[*index];
                    row.push_bind(first_sequence + offset as i64)
                        .push_bind(event.event_id.to_string())
                        .push_bind(&stream_key)
                        .push_bind(&event.operation)
                        .push_bind(&event.document_id)
                        .push_bind(&event.target_index)
                        .push_bind(&event.document);
                },
            );
            query
                .build()
                .execute(self.connection())
                .await
                .map_err(|error| db_error("bulk insert transactional Outbox events", error))?;

            for (offset, index) in new_indices.into_iter().enumerate() {
                sequences[index] =
                    Some(u64::try_from(first_sequence + offset as i64).map_err(|_| {
                        DatspoolError::Internal("negative PostgreSQL Outbox sequence".into())
                    })?);
            }
        }
        self.event_appended = true;
        sequences
            .into_iter()
            .map(|sequence| {
                sequence.ok_or_else(|| {
                    DatspoolError::Internal("missing allocated Outbox batch sequence".into())
                })
            })
            .collect()
    }

    pub async fn append_event(
        &mut self,
        event_id: uuid::Uuid,
        operation: &str,
        document_id: &str,
        target_index: &str,
        document: &Value,
    ) -> Result<u64> {
        let result = self
            .append_event_inner(event_id, operation, document_id, target_index, document)
            .await;
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    async fn append_event_inner(
        &mut self,
        event_id: uuid::Uuid,
        operation: &str,
        document_id: &str,
        target_index: &str,
        document: &Value,
    ) -> Result<u64> {
        if !matches!(operation, "index" | "delete")
            || document_id.is_empty()
            || target_index.is_empty()
        {
            return Err(DatspoolError::Driver(DriverError::DataError(
                "Outbox events require operation index/delete and non-empty document/target identifiers".into(),
            )));
        }
        if operation == "index" && !document.is_object() {
            return Err(DatspoolError::Driver(DriverError::DataError(
                "Outbox index events require a JSON object document".into(),
            )));
        }
        let stream_key = self.stream_key.clone();
        let existing = sqlx::query(
            "SELECT sequence, stream_key, operation, document_id, target_index, document FROM dataspool_cdc_outbox WHERE event_id = $1",
        )
        .bind(event_id.to_string())
        .fetch_optional(self.connection())
        .await
        .map_err(|error| db_error("check Outbox event idempotency key", error))?;
        if let Some(existing) = existing {
            let same_event = existing.try_get::<String, _>("stream_key").ok().as_deref()
                == Some(stream_key.as_str())
                && existing.try_get::<String, _>("operation").ok().as_deref() == Some(operation)
                && existing.try_get::<String, _>("document_id").ok().as_deref()
                    == Some(document_id)
                && existing
                    .try_get::<String, _>("target_index")
                    .ok()
                    .as_deref()
                    == Some(target_index)
                && existing.try_get::<Value, _>("document").ok().as_ref() == Some(document);
            if !same_event {
                return Err(DatspoolError::Driver(DriverError::DataError(
                    "Outbox event ID was reused with different event content".into(),
                )));
            }
            let sequence: i64 = existing
                .try_get("sequence")
                .map_err(|error| db_error("decode idempotent Outbox sequence", error))?;
            self.event_appended = true;
            return u64::try_from(sequence).map_err(|_| {
                DatspoolError::Internal("negative PostgreSQL Outbox sequence".into())
            });
        }
        let sequence_row = sqlx::query(
            "UPDATE dataspool_cdc_outbox_gate SET last_sequence = last_sequence + 1, updated_at = clock_timestamp() WHERE stream_key = $1 RETURNING last_sequence",
        )
        .bind(&stream_key)
        .fetch_one(self.connection())
        .await
        .map_err(|error| db_error("allocate ordered Outbox sequence", error))?;
        let sequence: i64 = sequence_row
            .try_get("last_sequence")
            .map_err(|error| db_error("decode allocated Outbox sequence", error))?;
        sqlx::query(
            "INSERT INTO dataspool_cdc_outbox (sequence, event_id, stream_key, operation, document_id, target_index, document) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(sequence)
        .bind(event_id.to_string())
        .bind(stream_key)
        .bind(operation)
        .bind(document_id)
        .bind(target_index)
        .bind(document)
        .execute(self.connection())
        .await
        .map_err(|error| db_error("append transactional Outbox event", error))?;
        self.event_appended = true;
        u64::try_from(sequence)
            .map_err(|_| DatspoolError::Internal("negative PostgreSQL Outbox sequence".into()))
    }

    pub async fn commit(mut self) -> Result<()> {
        let transaction = self
            .transaction
            .take()
            .expect("transaction already committed");
        if self.failed || !self.event_appended {
            transaction.rollback().await.map_err(|error| {
                db_error("rollback failed or empty source write transaction", error)
            })?;
            let reason = if self.failed {
                "source write transaction cannot commit after a failed Outbox append"
            } else {
                "source write transaction cannot commit without at least one Outbox event"
            };
            return Err(DatspoolError::Internal(reason.into()));
        }
        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit source mutation and Outbox event", error))
    }
}

#[async_trait]
impl CdcWriteBarrier for PostgresTransactionalOutbox {
    async fn pause_and_capture_final_watermark(&mut self) -> Result<u64> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin CDC cutover barrier", error))?;
        // FOR UPDATE waits for every in-flight producer transaction and blocks
        // new producers until the durable paused state has been committed.
        let gate = sqlx::query(
            "SELECT last_sequence FROM dataspool_cdc_outbox_gate WHERE stream_key = $1 FOR UPDATE",
        )
        .bind(&self.stream_key)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|error| db_error("acquire CDC cutover barrier", error))?;
        let watermark: i64 = gate
            .try_get("last_sequence")
            .map_err(|error| db_error("read final gate sequence", error))?;
        sqlx::query("UPDATE dataspool_cdc_outbox_gate SET paused = TRUE, updated_at = clock_timestamp() WHERE stream_key = $1")
            .bind(&self.stream_key).execute(&mut *transaction).await
            .map_err(|error| db_error("persist CDC producer pause", error))?;
        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit durable CDC cutover barrier", error))?;
        u64::try_from(watermark)
            .map_err(|_| DatspoolError::Internal("negative final Outbox watermark".into()))
    }

    async fn resume_writes(&mut self) -> Result<()> {
        sqlx::query("UPDATE dataspool_cdc_outbox_gate SET paused = FALSE, updated_at = clock_timestamp() WHERE stream_key = $1")
            .bind(&self.stream_key).execute(&self.pool).await
            .map_err(|error| db_error("resume PostgreSQL CDC producer writes", error))?;
        Ok(())
    }
}

/// Configuration for consuming the PostgreSQL transactional Outbox directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresOutboxConfig {
    /// Environment variable containing the PostgreSQL URL. The secret itself
    /// is never replicated in the Raft task configuration.
    pub connection_env: String,
    pub stream_key: String,
}

/// Continuous pipeline source backed by the committed PostgreSQL Outbox rows.
pub struct PostgresOutboxSource {
    config: PostgresOutboxConfig,
    pool: Option<PgPool>,
    last_sequence: i64,
}

impl PostgresOutboxSource {
    pub fn new(config: PostgresOutboxConfig) -> Self {
        Self {
            config,
            pool: None,
            last_sequence: 0,
        }
    }

    fn restore_offset(&mut self, offset: &crate::pipeline::OffsetValue) -> Result<()> {
        match offset {
            crate::pipeline::OffsetValue::Custom(value)
                if value["driver"].as_str() == Some("postgres_outbox") =>
            {
                self.last_sequence = value["last_sequence"].as_i64().ok_or_else(|| {
                    DatspoolError::Internal(
                        "PostgreSQL Outbox checkpoint has invalid last_sequence".into(),
                    )
                })?;
            }
            crate::pipeline::OffsetValue::DocumentCount(0)
            | crate::pipeline::OffsetValue::ByteOffset(0) => self.last_sequence = 0,
            _ => {
                return Err(DatspoolError::Internal(
                    "offset does not belong to the PostgreSQL Outbox source".into(),
                ));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl crate::pipeline::Source for PostgresOutboxSource {
    async fn connect(&mut self) -> Result<()> {
        let connection_url = std::env::var(&self.config.connection_env).map_err(|_| {
            DatspoolError::Configuration(format!(
                "PostgreSQL Outbox connection environment variable '{}' is not set",
                self.config.connection_env
            ))
        })?;
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(15))
            .connect(&connection_url)
            .await
            .map_err(|error| db_error("connect PostgreSQL Outbox source", error))?;
        sqlx::query("SELECT 1 FROM dataspool_cdc_outbox WHERE stream_key = $1 LIMIT 0")
            .bind(&self.config.stream_key)
            .execute(&pool)
            .await
            .map_err(|error| db_error("validate PostgreSQL Outbox source schema", error))?;
        self.pool = Some(pool);
        Ok(())
    }

    async fn read_batch(
        &mut self,
        offset: &crate::pipeline::OffsetValue,
        batch_size: usize,
    ) -> Result<(crate::pipeline::RecordBatch, crate::pipeline::OffsetValue)> {
        self.restore_offset(offset)?;
        let pool = self.pool.as_ref().ok_or_else(|| {
            DatspoolError::Internal("PostgreSQL Outbox source is not connected".into())
        })?;
        let rows = sqlx::query(
            "SELECT sequence, event_id::text AS event_id, operation, document_id, target_index, document \
             FROM dataspool_cdc_outbox WHERE stream_key = $1 AND sequence > $2 ORDER BY sequence ASC LIMIT $3",
        ).bind(&self.config.stream_key).bind(self.last_sequence).bind(batch_size.clamp(1, 10_000) as i64)
            .fetch_all(pool).await.map_err(|error| db_error("read PostgreSQL Outbox batch", error))?;
        let mut batch = crate::pipeline::RecordBatch::with_capacity(rows.len());
        for row in rows {
            let sequence: i64 = row
                .try_get("sequence")
                .map_err(|error| db_error("decode Outbox sequence", error))?;
            let event_id: String = row
                .try_get("event_id")
                .map_err(|error| db_error("decode Outbox event ID", error))?;
            let operation: String = row
                .try_get("operation")
                .map_err(|error| db_error("decode Outbox operation", error))?;
            let document_id: String = row
                .try_get("document_id")
                .map_err(|error| db_error("decode Outbox document ID", error))?;
            let target_index: String = row
                .try_get("target_index")
                .map_err(|error| db_error("decode Outbox target index", error))?;
            let document: Value = row
                .try_get("document")
                .map_err(|error| db_error("decode Outbox document", error))?;
            let mut record =
                crate::pipeline::Record::with_id(document_id, "postgres_outbox", document);
            record.add_metadata("_cdc_event_id", event_id);
            record.add_metadata("_op", operation);
            record.add_metadata("_target_index", target_index);
            record.add_metadata("_sequence", sequence.to_string());
            self.last_sequence = sequence;
            batch.push(record);
        }
        let next = crate::pipeline::OffsetValue::Custom(serde_json::json!({
            "driver":"postgres_outbox", "last_sequence":self.last_sequence
        }));
        Ok((batch, next))
    }

    fn is_continuous(&self) -> bool {
        true
    }

    async fn estimate_count(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }
        Ok(())
    }

    fn source_type(&self) -> &str {
        "postgres_outbox"
    }
}

/// Configuration for the partitioned PostgreSQL Outbox pipeline source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresPartitionedOutboxConfig {
    pub connection_env: String,
    pub stream_key: String,
    pub partition_count: u32,
}

/// Continuous pipeline source with a vector checkpoint, one local offset per
/// partition. Records are interleaved fairly; their metadata retains partition
/// identity because local sequence values are not comparable across partitions.
pub struct PostgresPartitionedOutboxSource {
    config: PostgresPartitionedOutboxConfig,
    pool: Option<PgPool>,
    outbox: Option<PostgresPartitionedOutbox>,
    offsets: PostgresOutboxVectorWatermark,
}

impl PostgresPartitionedOutboxSource {
    pub fn new(config: PostgresPartitionedOutboxConfig) -> Result<Self> {
        if config.partition_count == 0 || config.partition_count > 4096 {
            return Err(DatspoolError::Configuration(
                "partition_count must be between 1 and 4096".into(),
            ));
        }
        Ok(Self {
            config,
            pool: None,
            outbox: None,
            offsets: PostgresOutboxVectorWatermark::new(),
        })
    }

    fn restore_offset(&mut self, offset: &crate::pipeline::OffsetValue) -> Result<()> {
        match offset {
            crate::pipeline::OffsetValue::Custom(value)
                if value["driver"].as_str() == Some("postgres_partitioned_outbox") =>
            {
                let offsets: PostgresOutboxVectorWatermark =
                    serde_json::from_value(value["partition_watermarks"].clone()).map_err(|e| {
                        DatspoolError::Internal(format!(
                            "invalid partitioned Outbox vector checkpoint: {e}"
                        ))
                    })?;
                if offsets.len() != self.config.partition_count as usize
                    || (0..self.config.partition_count)
                        .any(|partition| !offsets.contains_key(&partition))
                {
                    return Err(DatspoolError::Internal(
                        "partitioned Outbox checkpoint is incomplete".into(),
                    ));
                }
                self.offsets = offsets;
            }
            crate::pipeline::OffsetValue::DocumentCount(0)
            | crate::pipeline::OffsetValue::ByteOffset(0) => {
                self.offsets = (0..self.config.partition_count)
                    .map(|partition| (partition, 0))
                    .collect();
            }
            _ => {
                return Err(DatspoolError::Internal(
                    "offset does not belong to the partitioned PostgreSQL Outbox source".into(),
                ));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl crate::pipeline::Source for PostgresPartitionedOutboxSource {
    async fn connect(&mut self) -> Result<()> {
        let connection_url = std::env::var(&self.config.connection_env).map_err(|_| {
            DatspoolError::Configuration(format!(
                "PostgreSQL Outbox connection environment variable '{}' is not set",
                self.config.connection_env
            ))
        })?;
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(15))
            .connect(&connection_url)
            .await
            .map_err(|e| db_error("connect partitioned PostgreSQL Outbox source", e))?;
        let outbox = PostgresPartitionedOutbox::new(
            pool.clone(),
            self.config.stream_key.clone(),
            self.config.partition_count,
        )?;
        outbox.vector_watermark().await?;
        self.offsets = (0..self.config.partition_count)
            .map(|partition| (partition, 0))
            .collect();
        self.pool = Some(pool);
        self.outbox = Some(outbox);
        Ok(())
    }

    async fn read_batch(
        &mut self,
        offset: &crate::pipeline::OffsetValue,
        batch_size: usize,
    ) -> Result<(crate::pipeline::RecordBatch, crate::pipeline::OffsetValue)> {
        self.restore_offset(offset)?;
        let outbox = self.outbox.as_ref().ok_or_else(|| {
            DatspoolError::Internal("partitioned PostgreSQL Outbox source is not connected".into())
        })?;
        let batch_size = batch_size.clamp(1, 10_000);
        let batches = outbox
            .read_vector_after(&self.offsets, batch_size as i64)
            .await?;
        let mut queues = batches
            .into_iter()
            .map(|(partition, events)| (partition, events, 0usize))
            .collect::<Vec<_>>();
        let mut batch = crate::pipeline::RecordBatch::with_capacity(batch_size);
        while batch.len() < batch_size {
            let mut progressed = false;
            for (partition, events, cursor) in &mut queues {
                if batch.len() >= batch_size {
                    break;
                }
                if let Some(event) = events.get(*cursor) {
                    let mut record = crate::pipeline::Record::with_id(
                        event.document_id.clone(),
                        "postgres_partitioned_outbox",
                        event.document.clone(),
                    );
                    record.add_metadata("_cdc_event_id", event.event_id.clone());
                    record.add_metadata("_op", event.operation.clone());
                    record.add_metadata("_target_index", event.target_index.clone());
                    record.add_metadata("_partition_id", partition.to_string());
                    record.add_metadata("_sequence", event.local_sequence.to_string());
                    self.offsets.insert(*partition, event.local_sequence as u64);
                    batch.push(record);
                    *cursor += 1;
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
        let next = crate::pipeline::OffsetValue::Custom(serde_json::json!({
            "driver": "postgres_partitioned_outbox",
            "partition_watermarks": self.offsets,
        }));
        Ok((batch, next))
    }

    fn is_continuous(&self) -> bool {
        true
    }
    async fn estimate_count(&self) -> Result<Option<u64>> {
        Ok(None)
    }
    async fn close(&mut self) -> Result<()> {
        self.outbox = None;
        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }
        Ok(())
    }
    fn source_type(&self) -> &str {
        "postgres_partitioned_outbox"
    }
}
