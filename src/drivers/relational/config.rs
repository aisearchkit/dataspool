// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Relational database configuration

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Supported relational database types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    /// MySQL (5.7+, 8.0+)
    Mysql,
    /// PostgreSQL (12+, 13+, 14+, 15+)
    Postgresql,
    /// SQLite (future support)
    Sqlite,
}

impl DatabaseType {
    /// Get default port for database type
    pub fn default_port(&self) -> u16 {
        match self {
            DatabaseType::Mysql => 3306,
            DatabaseType::Postgresql => 5432,
            DatabaseType::Sqlite => 0, // N/A for file-based DB
        }
    }
    
    /// Get protocol prefix for connection URL
    pub fn protocol(&self) -> &str {
        match self {
            DatabaseType::Mysql => "mysql",
            DatabaseType::Postgresql => "postgresql",
            DatabaseType::Sqlite => "sqlite",
        }
    }
}

/// Relational database connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationalConfig {
    /// Database type (mysql, postgresql, sqlite)
    pub db_type: DatabaseType,
    
    /// Database host (e.g., "localhost" or "192.168.1.10")
    pub host: String,
    
    /// Database port (if None, uses default for db_type)
    #[serde(default)]
    pub port: Option<u16>,
    
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

fn default_pk_column() -> String {
    "id".to_string()
}

fn default_batch_size() -> usize {
    1000
}

fn default_connection_timeout() -> Duration {
    Duration::from_secs(30)
}

/// SSL/TLS configuration for relational databases
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SslConfig {
    #[serde(default)]
    pub enabled: bool,
    
    pub ca_cert: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

impl RelationalConfig {
    /// Get effective port (user-specified or default)
    pub fn get_port(&self) -> u16 {
        self.port.unwrap_or_else(|| self.db_type.default_port())
    }
    
    /// Build connection URL
    pub fn connection_url(&self) -> String {
        let protocol = self.db_type.protocol();
        let port = self.get_port();
        
        match self.db_type {
            DatabaseType::Sqlite => {
                // SQLite uses file path instead of host:port
                format!("{}://{}", protocol, self.database)
            }
            _ => {
                format!(
                    "{}://{}:{}@{}:{}/{}",
                    protocol,
                    self.username,
                    self.password,
                    self.host,
                    port,
                    self.database
                )
            }
        }
    }
    
    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.host.is_empty() && self.db_type != DatabaseType::Sqlite {
            return Err("host cannot be empty for network databases".to_string());
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
    fn test_database_type_defaults() {
        assert_eq!(DatabaseType::Mysql.default_port(), 3306);
        assert_eq!(DatabaseType::Postgresql.default_port(), 5432);
        assert_eq!(DatabaseType::Mysql.protocol(), "mysql");
        assert_eq!(DatabaseType::Postgresql.protocol(), "postgresql");
    }

    #[test]
    fn test_mysql_connection_url() {
        let config = RelationalConfig {
            db_type: DatabaseType::Mysql,
            host: "localhost".to_string(),
            port: None, // Should use default 3306
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
        
        assert_eq!(config.get_port(), 3306);
        assert_eq!(
            config.connection_url(),
            "mysql://root:secret@localhost:3306/testdb"
        );
    }

    #[test]
    fn test_postgresql_connection_url() {
        let config = RelationalConfig {
            db_type: DatabaseType::Postgresql,
            host: "pg.example.com".to_string(),
            port: Some(5433), // Custom port
            database: "analytics".to_string(),
            username: "postgres".to_string(),
            password: "pgpass".to_string(),
            table: "events".to_string(),
            where_clause: Some("created_at > '2024-01-01'".to_string()),
            pk_column: "event_id".to_string(),
            batch_size: 5000,
            connection_timeout: Duration::from_secs(60),
            ssl: SslConfig::default(),
        };
        
        assert_eq!(config.get_port(), 5433);
        assert_eq!(
            config.connection_url(),
            "postgresql://postgres:pgpass@pg.example.com:5433/analytics"
        );
    }

    #[test]
    fn test_validation() {
        let mut config = RelationalConfig {
            db_type: DatabaseType::Mysql,
            host: "".to_string(),
            port: None,
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
        
        config.table = "".to_string();
        assert!(config.validate().is_err());
    }
}
