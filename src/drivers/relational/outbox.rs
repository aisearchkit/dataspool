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

/// PostgreSQL transactional Outbox for one logical CDC stream.
#[derive(Clone)]
pub struct PostgresTransactionalOutbox {
    pool: PgPool,
    stream_key: String,
}

impl PostgresTransactionalOutbox {
    pub async fn connect(database_url: &str, stream_key: impl Into<String>) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
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
        Ok(PostgresOutboxWrite {
            transaction: Some(transaction),
            stream_key: self.stream_key.clone(),
            event_appended: false,
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
}

impl PostgresOutboxWrite<'_> {
    /// The source mutation must use this connection so it commits atomically with the Outbox event.
    pub fn connection(&mut self) -> &mut sqlx::PgConnection {
        self.transaction
            .as_mut()
            .expect("transaction already committed or rolled back")
    }

    pub async fn append_event(
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
        if !self.event_appended {
            transaction
                .rollback()
                .await
                .map_err(|error| db_error("rollback source write without Outbox event", error))?;
            return Err(DatspoolError::Internal(
                "source write transaction cannot commit without at least one Outbox event".into(),
            ));
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
