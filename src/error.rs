// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use thiserror::Error;

/// Root error type for Dataspool
#[derive(Error, Debug)]
pub enum DatspoolError {
    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Config parsing: {0}")]
    Config(#[from] ConfigError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Raft error: {0}")]
    Raft(#[from] RaftError),

    #[error("Driver error: {0}")]
    Driver(#[from] DriverError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Configuration related errors
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to load config file: {0}")]
    LoadFailed(String),

    #[error("Invalid configuration: {0}")]
    Invalid(String),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("YAML parse error: {0}")]
    YamlError(#[from] serde_yaml::Error),
}

/// Storage layer errors
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Write failed: {0}")]
    WriteFailed(String),

    #[error("Read failed: {0}")]
    ReadFailed(String),

    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),
}

/// Raft consensus errors
#[derive(Error, Debug)]
pub enum RaftError {
    #[error("Not the leader, current leader: {0:?}")]
    NotLeader(Option<String>),

    #[error("Proposal failed: {0}")]
    ProposalFailed(String),

    #[error("Election timeout")]
    ElectionTimeout,

    #[error("Network error: {0}")]
    Network(String),

    #[error("Storage error: {0}")]
    Storage(String),
}

/// Driver errors (data source/sink)
#[derive(Error, Debug)]
pub enum DriverError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Authentication failed for user {user}")]
    AuthenticationFailed { user: String },

    #[error("Query timeout after {timeout_secs}s")]
    QueryTimeout { timeout_secs: u64 },

    #[error("Invalid query: {0}")]
    InvalidQuery(String),
    
    #[error("Query execution failed: {0}")]
    QueryError(String),

    #[error("Data error: {0}")]
    DataError(String),
    
    #[error("Feature not supported: {0}")]
    Unsupported(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Convenience type alias
pub type Result<T> = std::result::Result<T, DatspoolError>;

impl From<serde_json::Error> for DatspoolError {
    fn from(e: serde_json::Error) -> Self {
        DatspoolError::Serialization(e.to_string())
    }
}
