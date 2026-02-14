// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! MySQL source implementation

use async_trait::async_trait;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::Row;
use tracing::{debug, info};

use super::config::MysqlConfig;
use crate::error::{DatspoolError, DriverError};
use crate::pipeline::{OffsetValue, Record, RecordBatch, Source};

/// MySQL data source
pub struct MysqlSource {
    config: MysqlConfig,
    pool: Option<MySqlPool>,
    total_count: Option<u64>,
}

impl MysqlSource {
    pub fn new(config: MysqlConfig) -> Result<Self, DatspoolError> {
        config.validate().map_err(|e| {
            DatspoolError::Driver(DriverError::DataError(format!("Invalid MySQL config: {}", e)))
        })?;

        Ok(Self {
            config,
            pool: None,
            total_count: None,
        })
    }

    /// Build connection pool
    async fn create_pool(&self) -> Result<MySqlPool, DatspoolError> {
        let opts: MySqlConnectOptions = self
            .config
            .connection_url()
            .parse()
            .map_err(|e| {
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

        Ok(pool)
    }

    /// Build SELECT query with WHERE and LIMIT
    fn build_query(&self, offset_value: &OffsetValue, limit: usize) -> String {
        let base_query = format!(
            "SELECT * FROM `{}`",
            self.config.table
        );

        let where_clause = match (&self.config.where_clause, offset_value) {
            (Some(user_where), OffsetValue::PrimaryKey(last_pk)) => {
                // Combine user WHERE with offset condition
                format!(
                    " WHERE ({}) AND `{}` > {}",
                    user_where,
                    self.config.pk_column,
                    last_pk
                )
            }
            (Some(user_where), _) => {
                format!(" WHERE {}", user_where)
            }
            (None, OffsetValue::PrimaryKey(last_pk)) => {
                format!(" WHERE `{}` > {}", self.config.pk_column, last_pk)
            }
            _ => String::new(),
        };

        let order_clause = format!(" ORDER BY `{}` ASC", self.config.pk_column);
        let limit_clause = format!(" LIMIT {}", limit);

        format!("{}{}{}{}", base_query, where_clause, order_clause, limit_clause)
    }

    /// Convert MySQL row to Record
    fn row_to_record(&self, row: MySqlRow) -> Result<Record, DatspoolError> {
        use sqlx::Column;
        
        // Extract primary key as document ID
        let id = row.try_get::<serde_json::Value, _>(&self.config.pk_column[..])
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
            // Get value as JSON (sqlx supports this for most types)
            if let Ok(value) = row.try_get::<serde_json::Value, _>(column_name) {
                json_obj.insert(column_name.to_string(), value);
            }
        }

        let payload = serde_json::Value::Object(json_obj);

        Ok(Record::with_id(
            id.to_string(),
            "mysql",
            payload,
        ))
    }
}

#[async_trait]
impl Source for MysqlSource {
    fn source_type(&self) -> &str {
        "mysql"
    }

    async fn connect(&mut self) -> Result<(), DatspoolError> {
        info!(
            "Connecting to MySQL at {}:{}/{}",
            self.config.host, self.config.port, self.config.database
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

        // Execute query
        let rows = sqlx::query(&query)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                DatspoolError::Driver(DriverError::DataError(format!(
                    "Failed to fetch rows: {}",
                    e
                )))
            })?;

        let row_count = rows.len();
        debug!("Fetched {} rows", row_count);

        // Convert rows to records
        let mut records = Vec::with_capacity(row_count);
        let mut last_pk: Option<serde_json::Value> = None;

        for row in rows {
            // Extract PK before converting to record
            let pk_value = row.try_get::<serde_json::Value, _>(&self.config.pk_column[..])
                .map_err(|e| {
                    DatspoolError::Driver(DriverError::DataError(format!(
                        "Failed to extract PK: {}",
                        e
                    )))
                })?;
            
            last_pk = Some(pk_value.clone());
            
            let record = self.row_to_record(row)?;
            records.push(record);
        }

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

        // Build COUNT query with WHERE clause if specified
        let count_query = if let Some(ref where_clause) = self.config.where_clause {
            format!(
                "SELECT COUNT(*) as count FROM `{}` WHERE {}",
                self.config.table, where_clause
            )
        } else {
            format!("SELECT COUNT(*) as count FROM `{}`", self.config.table)
        };

        let row: (i64,) = sqlx::query_as(&count_query)
            .fetch_one(pool)
            .await
            .map_err(|e| {
                DatspoolError::Driver(DriverError::DataError(format!(
                    "Failed to count rows: {}",
                    e
                )))
            })?;

        Ok(Some(row.0 as u64))
    }

    async fn close(&mut self) -> Result<(), DatspoolError> {
        if let Some(pool) = self.pool.take() {
            pool.close().await;
            info!("MySQL connection closed");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_query_initial() {
        let config = MysqlConfig {
            host: "localhost".to_string(),
            port: 3306,
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "secret".to_string(),
            table: "users".to_string(),
            where_clause: None,
            pk_column: "id".to_string(),
            batch_size: 1000,
            connection_timeout: std::time::Duration::from_secs(30),
            ssl: super::super::config::SslConfig::default(),
        };

        let source = MysqlSource::new(config).unwrap();
        let query = source.build_query(&OffsetValue::initial(), 1000);

        assert!(query.contains("SELECT * FROM `users`"));
        assert!(query.contains("ORDER BY `id` ASC"));
        assert!(query.contains("LIMIT 1000"));
        assert!(!query.contains("WHERE"));
    }

    #[test]
    fn test_build_query_with_offset() {
        let config = MysqlConfig {
            host: "localhost".to_string(),
            port: 3306,
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "secret".to_string(),
            table: "users".to_string(),
            where_clause: None,
            pk_column: "id".to_string(),
            batch_size: 1000,
            connection_timeout: std::time::Duration::from_secs(30),
            ssl: super::super::config::SslConfig::default(),
        };

        let source = MysqlSource::new(config).unwrap();
        let offset = OffsetValue::PrimaryKey(serde_json::json!(100));
        let query = source.build_query(&offset, 1000);

        assert!(query.contains("WHERE `id` > 100"));
        assert!(query.contains("ORDER BY `id` ASC"));
    }

    #[test]
    fn test_build_query_with_where_clause() {
        let config = MysqlConfig {
            host: "localhost".to_string(),
            port: 3306,
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "secret".to_string(),
            table: "users".to_string(),
            where_clause: Some("status = 'active'".to_string()),
            pk_column: "id".to_string(),
            batch_size: 1000,
            connection_timeout: std::time::Duration::from_secs(30),
            ssl: super::super::config::SslConfig::default(),
        };

        let source = MysqlSource::new(config).unwrap();
        let offset = OffsetValue::PrimaryKey(serde_json::json!(50));
        let query = source.build_query(&offset, 500);

        assert!(query.contains("WHERE (status = 'active') AND `id` > 50"));
        assert!(query.contains("LIMIT 500"));
    }
}
