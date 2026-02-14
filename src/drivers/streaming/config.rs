// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Streaming platform configuration

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Supported streaming platforms
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamingPlatform {
    /// Apache Kafka / Confluent Platform
    Kafka,
    /// AutoMQ (Kafka-compatible cloud-native)
    AutoMq,
    /// RedPanda (Kafka API compatible)
    Redpanda,
    /// Apache Pulsar (future)
    Pulsar,
}

impl StreamingPlatform {
    pub fn is_kafka_compatible(&self) -> bool {
        matches!(self, StreamingPlatform::Kafka | StreamingPlatform::AutoMq | StreamingPlatform::Redpanda)
    }
}

/// Message serialization format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SerializationFormat {
    /// Plain JSON
    Json,
    /// Avro with Schema Registry
    Avro,
    /// Protobuf
    Protobuf,
    /// Raw bytes
    Raw,
}

/// Offset strategy for consumers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OffsetStrategy {
    /// Start from earliest message
    Earliest,
    /// Start from latest message  
    Latest,
    /// Resume from last committed offset
    Stored,
    /// Start from specific offset
    Specific,
}

/// Partitioning strategy for producers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    /// Hash by message key
    KeyHash,
    /// Round-robin across partitions
    RoundRobin,
    /// Random partition
    Random,
    /// Custom partitioner (implement in code)
    Custom,
}

/// Compression algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

/// Streaming platform configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingConfig {
    /// Platform type
    pub platform: StreamingPlatform,

    /// Bootstrap servers (comma-separated or array)
    pub brokers: Vec<String>,

    /// Topics to consume from (source) or produce to (sink)
    pub topics: Vec<String>,

    /// Consumer group ID (for sources)
    #[serde(default)]
    pub consumer_group: Option<String>,

    /// Offset strategy (for sources)
    #[serde(default = "default_offset_strategy")]
    pub offset_strategy: OffsetStrategy,

    /// Specific offset (if offset_strategy = Specific)
    #[serde(default)]
    pub specific_offset: Option<i64>,

    /// Partitioning strategy (for sinks)
    #[serde(default = "default_partition_strategy")]
    pub partition_strategy: PartitionStrategy,

    /// Message serialization format
    #[serde(default = "default_serialization")]
    pub serialization: SerializationFormat,

    /// Schema Registry URL (for Avro/Protobuf)
    #[serde(default)]
    pub schema_registry_url: Option<String>,

    /// Compression codec
    #[serde(default)]
    pub compression: Compression,

    /// Batch size for consuming/producing
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Consumer poll timeout
    #[serde(with = "humantime_serde", default = "default_poll_timeout")]
    pub poll_timeout: Duration,

    /// Producer flush timeout
    #[serde(with = "humantime_serde", default = "default_flush_timeout")]
    pub flush_timeout: Duration,

    /// Enable exactly-once semantics (Kafka transactions)
    #[serde(default)]
    pub exactly_once: bool,

    /// Transactional ID (required for exactly-once)
    #[serde(default)]
    pub transactional_id: Option<String>,

    /// SASL authentication
    #[serde(default)]
    pub sasl: Option<SaslConfig>,

    /// SSL/TLS configuration
    #[serde(default)]
    pub ssl: Option<SslConfig>,

    /// Additional Kafka client properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// SASL authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaslConfig {
    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI)
    pub mechanism: String,

    /// Username
    pub username: String,

    /// Password
    #[serde(skip_serializing)]
    pub password: String,
}

/// SSL/TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SslConfig {
    /// Enable SSL/TLS
    #[serde(default)]
    pub enabled: bool,

    /// CA certificate path
    pub ca_cert: Option<String>,

    /// Client certificate path
    pub client_cert: Option<String>,

    /// Client key path
    pub client_key: Option<String>,

    /// Skip certificate verification (insecure!)
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

fn default_offset_strategy() -> OffsetStrategy {
    OffsetStrategy::Stored
}

fn default_partition_strategy() -> PartitionStrategy {
    PartitionStrategy::KeyHash
}

fn default_serialization() -> SerializationFormat {
    SerializationFormat::Json
}

fn default_batch_size() -> usize {
    100
}

fn default_poll_timeout() -> Duration {
    Duration::from_secs(1)
}

fn default_flush_timeout() -> Duration {
    Duration::from_secs(30)
}

impl StreamingConfig {
    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.brokers.is_empty() {
            return Err("brokers cannot be empty".to_string());
        }

        if self.topics.is_empty() {
            return Err("topics cannot be empty".to_string());
        }

        if self.exactly_once && self.transactional_id.is_none() {
            return Err("transactional_id required for exactly-once semantics".to_string());
        }

        if matches!(self.serialization, SerializationFormat::Avro | SerializationFormat::Protobuf) {
            if self.schema_registry_url.is_none() {
                return Err("schema_registry_url required for Avro/Protobuf".to_string());
            }
        }

        Ok(())
    }

    /// Get Kafka client configuration map
    pub fn to_kafka_config(&self) -> HashMap<String, String> {
        let mut config = HashMap::new();

        // Bootstrap servers
        config.insert(
            "bootstrap.servers".to_string(),
            self.brokers.join(","),
        );

        // Consumer group
        if let Some(ref group) = self.consumer_group {
            config.insert("group.id".to_string(), group.clone());
        }

        // Compression
        if !matches!(self.compression, Compression::None) {
            config.insert(
                "compression.type".to_string(),
                format!("{:?}", self.compression).to_lowercase(),
            );
        }

        // Exactly-once
        if self.exactly_once {
            config.insert("enable.idempotence".to_string(), "true".to_string());
            if let Some(ref txn_id) = self.transactional_id {
                config.insert("transactional.id".to_string(), txn_id.clone());
            }
        }

        // SASL
        if let Some(ref sasl) = self.sasl {
            config.insert("security.protocol".to_string(), "SASL_SSL".to_string());
            config.insert("sasl.mechanism".to_string(), sasl.mechanism.clone());
            config.insert("sasl.username".to_string(), sasl.username.clone());
            config.insert("sasl.password".to_string(), sasl.password.clone());
        }

        // SSL
        if let Some(ref ssl) = self.ssl {
            if ssl.enabled {
                config.insert("security.protocol".to_string(), "SSL".to_string());
                if let Some(ref ca) = ssl.ca_cert {
                    config.insert("ssl.ca.location".to_string(), ca.clone());
                }
                if let Some(ref cert) = ssl.client_cert {
                    config.insert("ssl.certificate.location".to_string(), cert.clone());
                }
                if let Some(ref key) = ssl.client_key {
                    config.insert("ssl.key.location".to_string(), key.clone());
                }
            }
        }

        // Merge custom properties
        config.extend(self.properties.clone());

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_config_generation() {
        let config = StreamingConfig {
            platform: StreamingPlatform::Kafka,
            brokers: vec!["localhost:9092".to_string()],
            topics: vec!["events".to_string()],
            consumer_group: Some("dataspool".to_string()),
            offset_strategy: OffsetStrategy::Earliest,
            specific_offset: None,
            partition_strategy: PartitionStrategy::KeyHash,
            serialization: SerializationFormat::Json,
            schema_registry_url: None,
            compression: Compression::Snappy,
            batch_size: 100,
            poll_timeout: Duration::from_secs(1),
            flush_timeout: Duration::from_secs(30),
            exactly_once: false,
            transactional_id: None,
            sasl: None,
            ssl: None,
            properties: HashMap::new(),
        };

        let kafka_config = config.to_kafka_config();
        assert_eq!(kafka_config.get("bootstrap.servers"), Some(&"localhost:9092".to_string()));
        assert_eq!(kafka_config.get("group.id"), Some(&"dataspool".to_string()));
        assert_eq!(kafka_config.get("compression.type"), Some(&"snappy".to_string()));
    }

    #[test]
    fn test_validation() {
        let mut config = StreamingConfig {
            platform: StreamingPlatform::Kafka,
            brokers: vec![],
            topics: vec!["test".to_string()],
            consumer_group: None,
            offset_strategy: OffsetStrategy::Earliest,
            specific_offset: None,
            partition_strategy: PartitionStrategy::KeyHash,
            serialization: SerializationFormat::Json,
            schema_registry_url: None,
            compression: Compression::None,
            batch_size: 100,
            poll_timeout: Duration::from_secs(1),
            flush_timeout: Duration::from_secs(30),
            exactly_once: false,
            transactional_id: None,
            sasl: None,
            ssl: None,
            properties: HashMap::new(),
        };

        assert!(config.validate().is_err());  // Empty brokers

        config.brokers = vec!["localhost:9092".to_string()];
        assert!(config.validate().is_ok());
    }
}
