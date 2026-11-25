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
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Postgres, Row, Transaction};
use std::time::Duration;

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
        Ok(deleted)
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
        if events.is_empty() {
            return Err(DatspoolError::Configuration(
                "a batch must contain at least one Outbox event".into(),
            ));
        }
        let mut sequences = Vec::with_capacity(events.len());
        for event in events {
            sequences.push(
                self.append_event(
                    event.event_id,
                    &event.operation,
                    &event.document_id,
                    &event.target_index,
                    &event.document,
                )
                .await?,
            );
        }
        Ok(sequences)
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
