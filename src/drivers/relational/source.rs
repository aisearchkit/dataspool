// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Relational database source implementation

use async_trait::async_trait;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgRow};
use sqlx::Row;
use tracing::{debug, info};

use super::config::{DatabaseType, RelationalConfig};
use crate::error::{DatspoolError, DriverError};
use crate::pipeline::{OffsetValue, Record, RecordBatch, Source};

/// Relational database connection pool (enum for different DB types)
enum ConnectionPool {
    Mysql(MySqlPool),
    Postgresql(PgPool),
}

/// Relational database data source
pub struct RelationalSource {
    config: RelationalConfig,
    pool: Option<ConnectionPool>,
    total_count: Option<u64>,
}

impl RelationalSource {
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
            total_count: None,
        })
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
                    .max_connections(5)
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
                    .max_connections(5)
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
                    "SQLite not yet implemented".to_string(),
                )))
            }
        }
    }

    /// Build SELECT query with WHERE and LIMIT
    fn build_query(&self, offset_value: &OffsetValue, limit: usize) -> String {
        let table_quoted = match self.config.db_type {
            DatabaseType::Mysql => format!("`{}`", self.config.table),
            DatabaseType::Postgresql => format!("\"{}\"", self.config.table),
            DatabaseType::Sqlite => format!("\"{}\"", self.config.table),
        };

        let pk_quoted = match self.config.db_type {
            DatabaseType::Mysql => format!("`{}`", self.config.pk_column),
            DatabaseType::Postgresql => format!("\"{}\"", self.config.pk_column),
            DatabaseType::Sqlite => format!("\"{}\"", self.config.pk_column),
        };

        let base_query = format!("SELECT * FROM {}", table_quoted);

        let where_clause = match (&self.config.where_clause, offset_value) {
            (Some(user_where), OffsetValue::PrimaryKey(last_pk)) => {
                // Combine user WHERE with offset condition
                format!(" WHERE ({}) AND {} > {}", user_where, pk_quoted, last_pk)
            }
            (Some(user_where), _) => {
                format!(" WHERE {}", user_where)
            }
            (None, OffsetValue::PrimaryKey(last_pk)) => {
                format!(" WHERE {} > {}", pk_quoted, last_pk)
            }
            _ => String::new(),
        };

        let order_clause = format!(" ORDER BY {} ASC", pk_quoted);
        let limit_clause = format!(" LIMIT {}", limit);

        format!(
            "{}{}{}{}",
            base_query, where_clause, order_clause, limit_clause
        )
    }

    /// Convert MySQL row to Record
    fn mysql_row_to_record(&self, row: MySqlRow) -> Result<Record, DatspoolError> {
        use sqlx::Column;

        // Extract primary key as document ID
        let id = row
            .try_get::<serde_json::Value, _>(&self.config.pk_column[..])
            .map_err(|e| {
                DatspoolError::Driver(DriverError::DataError(format!(
                    "Failed to extract PK {}: {}",
                    self.config.pk_column, e
                )))
            })?;

        // Convert entire row to JSON
        let mut json_obj = serde_json::Map::new();

        for column in row.columns() {
            let column_name: &str = column.name();
            if let Ok(value) = row.try_get::<serde_json::Value, _>(column_name) {
                json_obj.insert(column_name.to_string(), value);
            }
        }

        let payload = serde_json::Value::Object(json_obj);

        Ok(Record::with_id(
            id.to_string(),
            "relational",
            payload,
        ))
    }

    /// Convert PostgreSQL row to Record
    fn pg_row_to_record(&self, row: PgRow) -> Result<Record, DatspoolError> {
        use sqlx::Column;

        // Extract primary key as document ID
        let id = row
            .try_get::<serde_json::Value, _>(&self.config.pk_column[..])
            .map_err(|e| {
                DatspoolError::Driver(DriverError::DataError(format!(
                    "Failed to extract PK {}: {}",
                    self.config.pk_column, e
                )))
            })?;

        // Convert entire row to JSON
        let mut json_obj = serde_json::Map::new();

        for column in row.columns() {
            let column_name: &str = column.name();
            if let Ok(value) = row.try_get::<serde_json::Value, _>(column_name) {
                json_obj.insert(column_name.to_string(), value);
            }
        }

        let payload = serde_json::Value::Object(json_obj);

        Ok(Record::with_id(
            id.to_string(),
            "relational",
            payload,
        ))
    }
}

#[async_trait]
impl Source for RelationalSource {
    fn source_type(&self) -> &str {
        match self.config.db_type {
            DatabaseType::Mysql => "relational:mysql",
            DatabaseType::Postgresql => "relational:postgresql",
            DatabaseType::Sqlite => "relational:sqlite",
        }
    }

    async fn connect(&mut self) -> Result<(), DatspoolError> {
        info!(
            "Connecting to {} at {}:{}/{}",
            self.source_type(),
            self.config.host,
            self.config.get_port(),
            self.config.database
        );

        let pool = self.create_pool().await?;
        self.pool = Some(pool);

        Ok(())
    }

    async fn read_batch(
        &mut self,
        offset: &OffsetValue,
        batch_size: usize,
    ) -> Result<(RecordBatch, OffsetValue), DatspoolError> {
        let pool = self.pool.as_ref().ok_or_else(|| {
            DatspoolError::Driver(DriverError::DataError("Not connected".to_string()))
        })?;

        // Build query with offset
        let query = self.build_query(offset, batch_size);
        debug!("Executing query: {}", query);

        // Execute query based on pool type
        let (records, last_pk) = match pool {
            ConnectionPool::Mysql(mysql_pool) => {
                let rows = sqlx::query(&query).fetch_all(mysql_pool).await.map_err(|e| {
                    DatspoolError::Driver(DriverError::DataError(format!(
                        "Failed to fetch MySQL rows: {}",
                        e
                    )))
                })?;

                debug!("Fetched {} MySQL rows", rows.len());

                let mut records = Vec::with_capacity(rows.len());
                let mut last_pk: Option<serde_json::Value> = None;

                for row in rows {
                    // Extract PK before converting to record
                    let pk_value = row
                        .try_get::<serde_json::Value, _>(&self.config.pk_column[..])
                        .map_err(|e| {
                            DatspoolError::Driver(DriverError::DataError(format!(
                                "Failed to extract PK: {}",
                                e
                            )))
                        })?;

                    last_pk = Some(pk_value.clone());

                    let record = self.mysql_row_to_record(row)?;
                    records.push(record);
                }

                (records, last_pk)
            }
            ConnectionPool::Postgresql(pg_pool) => {
                let rows = sqlx::query(&query).fetch_all(pg_pool).await.map_err(|e| {
                    DatspoolError::Driver(DriverError::DataError(format!(
                        "Failed to fetch PostgreSQL rows: {}",
                        e
                    )))
                })?;

                debug!("Fetched {} PostgreSQL rows", rows.len());

                let mut records = Vec::with_capacity(rows.len());
                let mut last_pk: Option<serde_json::Value> = None;

                for row in rows {
                    let pk_value = row
                        .try_get::<serde_json::Value, _>(&self.config.pk_column[..])
                        .map_err(|e| {
                            DatspoolError::Driver(DriverError::DataError(format!(
                                "Failed to extract PK: {}",
                                e
                            )))
                        })?;

                    last_pk = Some(pk_value.clone());

                    let record = self.pg_row_to_record(row)?;
                    records.push(record);
                }

                (records, last_pk)
            }
        };

        // Create batch
        let mut batch = RecordBatch::new();
        for record in records {
            batch.push(record);
        }

        // New offset is the last PK value
        let new_offset = if let Some(pk) = last_pk {
            OffsetValue::PrimaryKey(pk)
        } else {
            offset.clone()
        };

        Ok((batch, new_offset))
    }

    async fn estimate_count(&self) -> Result<Option<u64>, DatspoolError> {
        if let Some(count) = self.total_count {
            return Ok(Some(count));
        }

        let pool = self.pool.as_ref().ok_or_else(|| {
            DatspoolError::Driver(DriverError::DataError("Not connected".to_string()))
        })?;

        let table_quoted = match self.config.db_type {
            DatabaseType::Mysql => format!("`{}`", self.config.table),
            DatabaseType::Postgresql => format!("\"{}\"", self.config.table),
            DatabaseType::Sqlite => format!("\"{}\"", self.config.table),
        };

        // Build COUNT query with WHERE clause if specified
        let count_query = if let Some(ref where_clause) = self.config.where_clause {
            format!(
                "SELECT COUNT(*) as count FROM {} WHERE {}",
                table_quoted, where_clause
            )
        } else {
            format!("SELECT COUNT(*) as count FROM {}", table_quoted)
        };

        let count: i64 = match pool {
            ConnectionPool::Mysql(mysql_pool) => {
                let row: (i64,) = sqlx::query_as(&count_query)
                    .fetch_one(mysql_pool)
                    .await
                    .map_err(|e| {
                        DatspoolError::Driver(DriverError::DataError(format!(
                            "Failed to count MySQL rows: {}",
                            e
                        )))
                    })?;
                row.0
            }
            ConnectionPool::Postgresql(pg_pool) => {
                let row: (i64,) = sqlx::query_as(&count_query)
                    .fetch_one(pg_pool)
                    .await
                    .map_err(|e| {
                        DatspoolError::Driver(DriverError::DataError(format!(
                            "Failed to count PostgreSQL rows: {}",
                            e
                        )))
                    })?;
                row.0
            }
        };

        Ok(Some(count as u64))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn create_mysql_config() -> RelationalConfig {
        RelationalConfig {
            db_type: DatabaseType::Mysql,
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

    fn create_pg_config() -> RelationalConfig {
        RelationalConfig {
            db_type: DatabaseType::Postgresql,
            host: "localhost".to_string(),
            port: None,
            database: "testdb".to_string(),
            username: "postgres".to_string(),
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
    fn test_build_query_mysql_initial() {
        let config = create_mysql_config();
        let source = RelationalSource::new(config).unwrap();
        let query = source.build_query(&OffsetValue::initial(), 1000);

        assert!(query.contains("SELECT * FROM `users`"));
        assert!(query.contains("ORDER BY `id` ASC"));
        assert!(query.contains("LIMIT 1000"));
        assert!(!query.contains("WHERE"));
    }

    #[test]
    fn test_build_query_mysql_with_offset() {
        let config = create_mysql_config();
        let source = RelationalSource::new(config).unwrap();
        let offset = OffsetValue::PrimaryKey(serde_json::json!(100));
        let query = source.build_query(&offset, 1000);

        assert!(query.contains("WHERE `id` > 100"));
        assert!(query.contains("ORDER BY `id` ASC"));
    }

    #[test]
    fn test_build_query_postgresql() {
        let config = create_pg_config();
        let source = RelationalSource::new(config).unwrap();
        let query = source.build_query(&OffsetValue::initial(), 500);

        assert!(query.contains("SELECT * FROM \"users\""));
        assert!(query.contains("ORDER BY \"id\" ASC"));
        assert!(query.contains("LIMIT 500"));
    }

    #[test]
    fn test_source_type() {
        let mysql_source = RelationalSource::new(create_mysql_config()).unwrap();
        assert_eq!(mysql_source.source_type(), "relational:mysql");

        let pg_source = RelationalSource::new(create_pg_config()).unwrap();
        assert_eq!(pg_source.source_type(), "relational:postgresql");
    }
}
