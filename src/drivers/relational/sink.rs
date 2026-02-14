// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Relational database sink implementation

use async_trait::async_trait;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{Execute, QueryBuilder};
use tracing::{debug, info, warn};

use super::config::{DatabaseType, RelationalConfig};
use crate::error::{DatspoolError, DriverError};
use crate::pipeline::{Record, RecordBatch, Sink, WriteResult};

/// Write mode for handling conflicts
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// INSERT - fail on duplicate keys
    Insert,
    /// REPLACE/UPSERT - overwrite on duplicate keys
    Replace,
    /// INSERT IGNORE - skip duplicates silently
    InsertIgnore,
}

/// Relational database connection pool (enum for different DB types)
enum ConnectionPool {
    Mysql(MySqlPool),
    Postgresql(PgPool),
}

/// Relational database data sink
pub struct RelationalSink {
    config: RelationalConfig,
    pool: Option<ConnectionPool>,
    write_mode: WriteMode,
    pending_records: usize,
}

impl RelationalSink {
    pub fn new(config: RelationalConfig) -> Result<Self, DatspoolError> {
        config.validate().map_err(|e| {
            DatspoolError::Driver(DriverError::DataError(format!(
                "Invalid relational config: {}",
                e
            )))
        })?;

        Ok(Self {
            config,
            pool: None,
            write_mode: WriteMode::Replace, // Default to UPSERT
            pending_records: 0,
        })
    }

    /// Set write mode
    pub fn with_write_mode(mut self, mode: WriteMode) -> Self {
        self.write_mode = mode;
        self
    }

    /// Build connection pool based on database type
    async fn create_pool(&self) -> Result<ConnectionPool, DatspoolError> {
        match self.config.db_type {
            DatabaseType::Mysql => {
                let opts: MySqlConnectOptions =
                    self.config.connection_url().parse().map_err(|e| {
                        DatspoolError::Driver(DriverError::ConnectionFailed(format!(
                            "Invalid MySQL URL: {}",
                            e
                        )))
                    })?;

                let pool = MySqlPoolOptions::new()
                    .max_connections(10) // More connections for writes
                    .acquire_timeout(self.config.connection_timeout)
                    .connect_with(opts)
                    .await
                    .map_err(|e| {
                        DatspoolError::Driver(DriverError::ConnectionFailed(format!(
                            "Failed to connect to MySQL: {}",
                            e
                        )))
                    })?;

                Ok(ConnectionPool::Mysql(pool))
            }
            DatabaseType::Postgresql => {
                let opts: PgConnectOptions =
                    self.config.connection_url().parse().map_err(|e| {
                        DatspoolError::Driver(DriverError::ConnectionFailed(format!(
                            "Invalid PostgreSQL URL: {}",
                            e
                        )))
                    })?;

                let pool = PgPoolOptions::new()
                    .max_connections(10)
                    .acquire_timeout(self.config.connection_timeout)
                    .connect_with(opts)
                    .await
                    .map_err(|e| {
                        DatspoolError::Driver(DriverError::ConnectionFailed(format!(
                            "Failed to connect to PostgreSQL: {}",
                            e
                        )))
                    })?;

                Ok(ConnectionPool::Postgresql(pool))
            }
            DatabaseType::Sqlite => {
                Err(DatspoolError::Driver(DriverError::Unsupported(
                    "SQLite sink not yet implemented".to_string(),
                )))
            }
        }
    }

    /// Extract column names and values from a Record
    fn extract_record_data(&self, record: &Record) -> Result<(Vec<String>, Vec<serde_json::Value>), DatspoolError> {
        let obj = match &record.payload {
            serde_json::Value::Object(map) => map,
            _ => {
                return Err(DatspoolError::Driver(DriverError::DataError(
                    "Record payload must be a JSON object".to_string(),
                )))
            }
        };

        let mut columns = Vec::new();
        let mut values = Vec::new();

        for (key, value) in obj.iter() {
            columns.push(key.clone());
            values.push(value.clone());
        }

        if columns.is_empty() {
            return Err(DatspoolError::Driver(DriverError::DataError(
                "Record has no fields".to_string(),
            )));
        }

        Ok((columns, values))
    }
}

#[async_trait]
impl Sink for RelationalSink {
    fn sink_type(&self) -> &str {
        match self.config.db_type {
            DatabaseType::Mysql => "relational:mysql",
            DatabaseType::Postgresql => "relational:postgresql",
            DatabaseType::Sqlite => "relational:sqlite",
        }
    }

    async fn connect(&mut self) -> Result<(), DatspoolError> {
        info!(
            "Connecting to {} at {}:{}/{}",
            self.sink_type(),
            self.config.host,
            self.config.get_port(),
            self.config.database
        );

        let pool = self.create_pool().await?;
        self.pool = Some(pool);

        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<WriteResult, DatspoolError> {
        let pool = self.pool.as_ref().ok_or_else(|| {
            DatspoolError::Driver(DriverError::DataError("Not connected".to_string()))
        })?;

        if batch.is_empty() {
            return Ok(WriteResult::success(0, 0));
        }

        let record_count = batch.len();
        debug!("Writing batch of {} records to {}", record_count, self.config.table);

        let result = match pool {
            ConnectionPool::Mysql(mysql_pool) => {
                self.write_batch_mysql(mysql_pool, &batch).await?
            }
            ConnectionPool::Postgresql(pg_pool) => {
                self.write_batch_postgres(pg_pool, &batch).await?
            }
        };

        self.pending_records += record_count;

        Ok(result)
    }

    async fn flush(&mut self) -> Result<(), DatspoolError> {
        if self.pending_records > 0 {
            debug!("Flushed {} pending records", self.pending_records);
            self.pending_records = 0;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), DatspoolError> {
        if let Some(pool) = self.pool.take() {
            match pool {
                ConnectionPool::Mysql(mysql_pool) => {
                    mysql_pool.close().await;
                    info!("MySQL connection closed");
                }
                ConnectionPool::Postgresql(pg_pool) => {
                    pg_pool.close().await;
                    info!("PostgreSQL connection closed");
                }
            }
        }
        Ok(())
    }
}

impl RelationalSink {
    /// Write batch to MySQL using bulk INSERT
    async fn write_batch_mysql(
        &self,
        pool: &MySqlPool,
        batch: &RecordBatch,
    ) -> Result<WriteResult, DatspoolError> {
        if batch.is_empty() {
            return Ok(WriteResult::success(0, 0));
        }

        // Extract schema from first record
        let first_record = &batch.records()[0];
        let (columns, _) = self.extract_record_data(first_record)?;

        // Build INSERT query based on write mode
        let table_quoted = format!("`{}`", self.config.table);
        let columns_quoted: Vec<String> = columns.iter().map(|c| format!("`{}`", c)).collect();
        let placeholders = columns.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

        let query_prefix = match self.write_mode {
            WriteMode::Insert => format!(
                "INSERT INTO {} ({}) VALUES",
                table_quoted,
                columns_quoted.join(", ")
            ),
            WriteMode::Replace => format!(
                "REPLACE INTO {} ({}) VALUES",
                table_quoted,
                columns_quoted.join(", ")
            ),
            WriteMode::InsertIgnore => format!(
                "INSERT IGNORE INTO {} ({}) VALUES",
                table_quoted,
                columns_quoted.join(", ")
            ),
        };

        // Build multi-row INSERT
        let mut query_builder = QueryBuilder::new(&query_prefix);
        let mut is_first = true;

        for record in batch.records() {
            let (_, values) = self.extract_record_data(record)?;

            if !is_first {
                query_builder.push(", ");
            }
            is_first = false;

            query_builder.push("(");
            let mut is_first_value = true;
            for value in values {
                if !is_first_value {
                    query_builder.push(", ");
                }
                is_first_value = false;

                // Convert JSON value to SQL
                match value {
                    serde_json::Value::Null => {
                        query_builder.push("NULL");
                    }
                    serde_json::Value::Bool(b) => {
                        query_builder.push(if b { "1" } else { "0" });
                    }
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            query_builder.push_bind(i);
                        } else if let Some(f) = n.as_f64() {
                            query_builder.push_bind(f);
                        } else {
                            query_builder.push_bind(n.to_string());
                        }
                    }
                    serde_json::Value::String(s) => {
                        query_builder.push_bind(s);
                    }
                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                        // Serialize complex types as JSON strings
                        query_builder.push_bind(value.to_string());
                    }
                };
            }
            query_builder.push(")");
        }

        let query = query_builder.build();
        let sql = query.sql();
        debug!("Executing MySQL INSERT: {}", sql);

        let result = query.execute(pool).await.map_err(|e| {
            DatspoolError::Driver(DriverError::QueryError(format!(
                "Failed to insert batch: {}",
                e
            )))
        })?;

        let rows_affected = result.rows_affected();
        Ok(WriteResult::success(rows_affected as usize, 0))
    }

    /// Write batch to PostgreSQL using bulk INSERT
    async fn write_batch_postgres(
        &self,
        pool: &PgPool,
        batch: &RecordBatch,
    ) -> Result<WriteResult, DatspoolError> {
        if batch.is_empty() {
            return Ok(WriteResult::success(0, 0));
        }

        // Extract schema from first record
        let first_record = &batch.records()[0];
        let (columns, _) = self.extract_record_data(first_record)?;

        // Build INSERT query (PostgreSQL doesn't have REPLACE, use ON CONFLICT)
        let table_quoted = format!("\"{}\"", self.config.table);
        let columns_quoted: Vec<String> = columns.iter().map(|c| format!("\"{}\"", c)).collect();

        let query_prefix = format!(
            "INSERT INTO {} ({})",
            table_quoted,
            columns_quoted.join(", ")
        );

        let mut query_builder = QueryBuilder::new(&query_prefix);
        query_builder.push(" VALUES ");

        let mut is_first = true;
        for record in batch.records() {
            let (_, values) = self.extract_record_data(record)?;

            if !is_first {
                query_builder.push(", ");
            }
            is_first = false;

            query_builder.push("(");
            let mut is_first_value = true;
            for value in values {
                if !is_first_value {
                    query_builder.push(", ");
                }
                is_first_value = false;

                match value {
                    serde_json::Value::Null => {
                        query_builder.push("NULL");
                    }
                    serde_json::Value::Bool(b) => {
                        query_builder.push_bind(b);
                    }
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            query_builder.push_bind(i);
                        } else if let Some(f) = n.as_f64() {
                            query_builder.push_bind(f);
                        } else {
                            query_builder.push_bind(n.to_string());
                        }
                    }
                    serde_json::Value::String(s) => {
                        query_builder.push_bind(s);
                    }
                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                        query_builder.push_bind(serde_json::to_string(&value).unwrap());
                    }
                };
            }
            query_builder.push(")");
        }

        // Add ON CONFLICT clause for UPSERT mode
        if self.write_mode == WriteMode::Replace {
            let pk_quoted = format!("\"{}\"", self.config.pk_column);
            query_builder.push(format!(" ON CONFLICT ({}) DO UPDATE SET ", pk_quoted));
            
            let mut is_first_col = true;
            for col in &columns {
                if col == &self.config.pk_column {
                    continue; // Skip PK in update clause
                }
                if !is_first_col {
                    query_builder.push(", ");
                }
                is_first_col = false;
                query_builder.push(format!("\"{}\" = EXCLUDED.\"{}\"", col, col));
            }
        } else if self.write_mode == WriteMode::InsertIgnore {
            query_builder.push(" ON CONFLICT DO NOTHING");
        }

        let query = query_builder.build();
        let sql = query.sql();
        debug!("Executing PostgreSQL INSERT: {}", sql);

        let result = query.execute(pool).await.map_err(|e| {
            DatspoolError::Driver(DriverError::QueryError(format!(
                "Failed to insert batch: {}",
                e
            )))
        })?;

        let rows_affected = result.rows_affected();
        Ok(WriteResult::success(rows_affected as usize, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::Record;
    use std::time::Duration;

    fn create_test_config(db_type: DatabaseType) -> RelationalConfig {
        RelationalConfig {
            db_type,
            host: "localhost".to_string(),
            port: None,
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "secret".to_string(),
            table: "users".to_string(),
            where_clause: None,
            pk_column: "id".to_string(),
            batch_size: 1000,
            connection_timeout: Duration::from_secs(30),
            ssl: super::super::config::SslConfig::default(),
        }
    }

    #[test]
    fn test_sink_creation() {
        let config = create_test_config(DatabaseType::Mysql);
        let sink = RelationalSink::new(config).unwrap();
        assert_eq!(sink.sink_type(), "relational:mysql");
        assert_eq!(sink.write_mode, WriteMode::Replace);
    }

    #[test]
    fn test_sink_with_write_mode() {
        let config = create_test_config(DatabaseType::Postgresql);
        let sink = RelationalSink::new(config)
            .unwrap()
            .with_write_mode(WriteMode::Insert);
        assert_eq!(sink.write_mode, WriteMode::Insert);
    }

    #[test]
    fn test_extract_record_data() {
        let config = create_test_config(DatabaseType::Mysql);
        let sink = RelationalSink::new(config).unwrap();

        let record = Record::new(
            "test",
            serde_json::json!({
                "id": 1,
                "name": "Alice",
                "active": true
            }),
        );

        let (columns, values) = sink.extract_record_data(&record).unwrap();
        assert_eq!(columns.len(), 3);
        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"name".to_string()));
        assert!(columns.contains(&"active".to_string()));
        assert_eq!(values.len(), 3);
    }
}
