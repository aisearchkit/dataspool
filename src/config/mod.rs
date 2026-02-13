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

use crate::error::{ConfigError, Result};
use crate::types::NodeId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Main configuration structure for Dataspool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatspoolConfig {
    pub node: NodeConfig,
    pub cluster: ClusterConfig,
    pub server: ServerConfig,
    pub agent: AgentConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

/// Node identification configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub name: String,
    #[serde(default)]
    pub id: Option<String>,
}

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub peers: Vec<String>,
    #[serde(with = "humantime_serde", default = "default_election_timeout")]
    pub election_timeout: Duration,
    #[serde(with = "humantime_serde", default = "default_heartbeat_interval")]
    pub heartbeat_interval: Duration,
}

fn default_election_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_millis(500)
}

/// Server listener configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub grpc_port: u16,
    pub http_port: u16,
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,
    #[serde(default)]
    pub tls: TlsConfig,
}

fn default_bind_addr() -> String {
    "0.0.0.0".to_string()
}

/// TLS configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub ca_file: Option<PathBuf>,
}

/// Agent configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub resources: AgentResources,
    #[serde(default)]
    pub tags: Vec<String>,
}

fn default_enabled() -> bool {
    true
}

/// Agent resource limits
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResources {
    #[serde(default = "default_max_concurrent_tasks")]
    pub max_concurrent_tasks: usize,
    #[serde(default = "default_memory_limit_mb")]
    pub memory_limit_mb: u64,
    #[serde(default)]
    pub read_rate_limit_mb_per_sec: u64,
}

impl Default for AgentResources {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: default_max_concurrent_tasks(),
            memory_limit_mb: default_memory_limit_mb(),
            read_rate_limit_mb_per_sec: 0,
        }
    }
}

fn default_max_concurrent_tasks() -> usize {
    num_cpus::get()
}

fn default_memory_limit_mb() -> u64 {
    2048
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    #[serde(default = "default_storage_engine")]
    pub engine: StorageEngine,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageEngine {
    RocksDb,
    Sled,
}

fn default_storage_engine() -> StorageEngine {
    StorageEngine::RocksDb
}

/// Authentication configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthConfig {
    pub jwt_secret: Option<String>,
    #[serde(with = "humantime_serde", default = "default_token_expiry")]
    pub token_expiry: Duration,
    #[serde(default)]
    pub api_keys: Vec<String>,
}

fn default_token_expiry() -> Duration {
    Duration::from_secs(24 * 3600) // 24 hours
}

/// Observability configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_log_format")]
    pub log_format: LogFormat,
    #[serde(default = "default_metrics_enabled")]
    pub metrics_enabled: bool,
    #[serde(default)]
    pub tracing: TracingConfig,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            log_format: default_log_format(),
            metrics_enabled: default_metrics_enabled(),
            tracing: TracingConfig::default(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> LogFormat {
    LogFormat::Json
}

fn default_metrics_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    Text,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TracingConfig {
    #[serde(default)]
    pub enabled: bool,
    pub endpoint: Option<String>,
}

impl DatspoolConfig {
    /// Load configuration from a YAML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|e| ConfigError::LoadFailed(e.to_string()))?;
        
        let mut config: Self = serde_yaml::from_str(&content)
            .map_err(ConfigError::from)?;
        
        // Apply environment variable overrides
        config.apply_env_overrides()?;
        
        // Validate configuration
        config.validate()?;
        
        Ok(config)
    }

    /// Apply environment variable overrides
    fn apply_env_overrides(&mut self) -> Result<()> {
        // Example: DATASPOOL_SERVER_HTTP_PORT overrides server.http_port
        if let Ok(port) = std::env::var("DATASPOOL_SERVER_HTTP_PORT") {
            self.server.http_port = port.parse()
                .map_err(|e| ConfigError::Invalid(format!("Invalid HTTP port: {}", e)))?;
        }

        if let Ok(port) = std::env::var("DATASPOOL_SERVER_GRPC_PORT") {
            self.server.grpc_port = port.parse()
                .map_err(|e| ConfigError::Invalid(format!("Invalid gRPC port: {}", e)))?;
        }

        if let Ok(secret) = std::env::var("DATASPOOL_JWT_SECRET") {
            self.auth.jwt_secret = Some(secret);
        }

        Ok(())
    }

    /// Validate configuration
    fn validate(&self) -> Result<()> {
        if self.node.name.is_empty() {
            return Err(ConfigError::MissingField("node.name".to_string()).into());
        }

        if self.server.grpc_port == 0 {
            return Err(ConfigError::Invalid("server.grpc_port cannot be 0".to_string()).into());
        }

        if self.server.http_port == 0 {
            return Err(ConfigError::Invalid("server.http_port cannot be 0".to_string()).into());
        }

        if self.server.tls.enabled {
            if self.server.tls.cert_file.is_none() {
                return Err(ConfigError::MissingField("server.tls.cert_file".to_string()).into());
            }
            if self.server.tls.key_file.is_none() {
                return Err(ConfigError::MissingField("server.tls.key_file".to_string()).into());
            }
        }

        Ok(())
    }

    /// Get gRPC listen address
    pub fn grpc_addr(&self) -> SocketAddr {
        format!("{}:{}", self.server.bind_addr, self.server.grpc_port)
            .parse()
            .expect("Invalid gRPC address")
    }

    /// Get HTTP listen address
    pub fn http_addr(&self) -> SocketAddr {
        format!("{}:{}", self.server.bind_addr, self.server.http_port)
            .parse()
            .expect("Invalid HTTP address")
    }

    /// Get or generate node ID
    pub fn node_id(&self) -> NodeId {
        if let Some(ref id) = self.node.id {
            NodeId::new(id.clone())
        } else {
            // Generate based on node name and hostname
            let hostname = hostname::get()
                .unwrap_or_else(|_| std::ffi::OsString::from("unknown"))
                .to_string_lossy()
                .to_string();
            NodeId::new(format!("{}-{}", self.node.name, hostname))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_config_from_yaml() {
        let yaml = r#"
node:
  name: "test-node"
  id: "node-001"

cluster:
  peers:
    - "192.168.1.10:50051"
    - "192.168.1.11:50051"
  election_timeout: "5s"
  heartbeat_interval: "500ms"

server:
  grpc_port: 50051
  http_port: 8080

agent:
  enabled: true
  resources:
    max_concurrent_tasks: 4
  tags:
    - "test"

storage:
  data_dir: "/tmp/dataspool"
  engine: "rocksdb"

observability:
  log_level: "debug"
  log_format: "json"
"#;
        
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(yaml.as_bytes()).unwrap();
        file.flush().unwrap();
        
        let config = DatspoolConfig::from_file(file.path()).unwrap();
        
        assert_eq!(config.node.name, "test-node");
        assert_eq!(config.server.grpc_port, 50051);
        assert_eq!(config.cluster.peers.len(), 2);
        assert!(config.agent.enabled);
    }

    #[test]
    fn test_validation() {
        let mut config = DatspoolConfig {
            node: NodeConfig {
                name: "".to_string(),
                id: None,
            },
            cluster: ClusterConfig {
                peers: vec![],
                election_timeout: Duration::from_secs(5),
                heartbeat_interval: Duration::from_millis(500),
            },
            server: ServerConfig {
                grpc_port: 50051,
                http_port: 8080,
                bind_addr: "0.0.0.0".to_string(),
                tls: TlsConfig::default(),
            },
            agent: AgentConfig {
                enabled: true,
                resources: AgentResources::default(),
                tags: vec![],
            },
            storage: StorageConfig {
                data_dir: PathBuf::from("/tmp"),
                engine: StorageEngine::RocksDb,
            },
            auth: AuthConfig::default(),
            observability: ObservabilityConfig::default(),
        };

        assert!(config.validate().is_err());
        
        config.node.name = "test".to_string();
        assert!(config.validate().is_ok());
    }
}
