// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! MySQL configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// MySQL connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlConfig {
    /// MySQL host (e.g., "localhost" or "192.168.1.10")
    pub host: String,
    
    /// MySQL port (default: 3306)
    #[serde(default = "default_mysql_port")]
    pub port: u16,
    
    /// Database name
    pub database: String,
    
    /// Username for authentication
    pub username: String,
    
    /// Password for authentication
    #[serde(skip_serializing)]
    pub password: String,
    
    /// Table name to migrate
    pub table: String,
    
    /// Optional WHERE clause for filtering
    #[serde(default)]
    pub where_clause: Option<String>,
    
    /// Primary key column for sharding and offset tracking
    #[serde(default = "default_pk_column")]
    pub pk_column: String,
    
    /// Batch size for SELECT queries
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    
    /// Connection timeout
    #[serde(with = "humantime_serde", default = "default_connection_timeout")]
    pub connection_timeout: Duration,
    
    /// SSL/TLS configuration
    #[serde(default)]
    pub ssl: SslConfig,
}

fn default_mysql_port() -> u16 {
    3306
}

fn default_pk_column() -> String {
    "id".to_string()
}

fn default_batch_size() -> usize {
    1000
}

fn default_connection_timeout() -> Duration {
    Duration::from_secs(30)
}

/// SSL/TLS configuration for MySQL
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SslConfig {
    #[serde(default)]
    pub enabled: bool,
    
    pub ca_cert: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

impl MysqlConfig {
    /// Build connection URL
    pub fn connection_url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.username,
            self.password,
            self.host,
            self.port,
            self.database
        )
    }
    
    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.host.is_empty() {
            return Err("host cannot be empty".to_string());
        }
        
        if self.database.is_empty() {
            return Err("database cannot be empty".to_string());
        }
        
        if self.table.is_empty() {
            return Err("table cannot be empty".to_string());
        }
        
        if self.pk_column.is_empty() {
            return Err("pk_column cannot be empty".to_string());
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_url() {
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
            connection_timeout: Duration::from_secs(30),
            ssl: SslConfig::default(),
        };
        
        assert_eq!(
            config.connection_url(),
            "mysql://root:secret@localhost:3306/testdb"
        );
    }

    #[test]
    fn test_validation() {
        let mut config = MysqlConfig {
            host: "".to_string(),
            port: 3306,
            database: "testdb".to_string(),
            username: "root".to_string(),
            password: "secret".to_string(),
            table: "users".to_string(),
            where_clause: None,
            pk_column: "id".to_string(),
            batch_size: 1000,
            connection_timeout: Duration::from_secs(30),
            ssl: SslConfig::default(),
        };
        
        assert!(config.validate().is_err());
        
        config.host = "localhost".to_string();
        assert!(config.validate().is_ok());
    }
}
