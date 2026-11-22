// SPDX-License-Identifier: AGPL-3.0-or-later
//! Elasticsearch change-log adapter and guarded alias cutover.
//!
//! Elasticsearch does not expose a native, general-purpose document CDC feed.
//! This adapter consumes an append-only change index populated by an application
//! outbox, database-log connector, or another CDC producer.

use crate::error::{DatspoolError, DriverError, Result};
use crate::pipeline::{OffsetValue, Record, RecordBatch, Source};
use async_trait::async_trait;
use reqwest::{Client, ClientBuilder};
use serde_json::{Value, json};
use std::time::Duration;
use tracing::{debug, info};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct CdcConfig {
    /// Index containing append-only change events.
    pub change_index: String,
    /// Monotonically increasing and globally unique sequence field.
    pub sequence_field: String,
    pub operation_field: String,
    pub document_id_field: String,
    pub target_index_field: String,
    pub document_field: String,
    pub routing_field: String,
    pub batch_size: usize,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            change_index: "dataspool-change-log".into(),
            sequence_field: "sequence".into(),
            operation_field: "operation".into(),
            document_id_field: "document_id".into(),
            target_index_field: "target_index".into(),
            document_field: "document".into(),
            routing_field: "routing".into(),
            batch_size: 1_000,
        }
    }
}

pub struct ElasticsearchCdcSource {
    connection: crate::drivers::document::DocumentConfig,
    cdc: CdcConfig,
    client: Option<Client>,
    base_url: String,
    last_sequence: u64,
}

impl ElasticsearchCdcSource {
    pub fn new(connection: crate::drivers::document::DocumentConfig, cdc: CdcConfig) -> Self {
        Self {
            connection,
            cdc,
            client: None,
            base_url: String::new(),
            last_sequence: 0,
        }
    }

    fn build_client(&self) -> Result<Client> {
        let mut builder = ClientBuilder::new()
            .timeout(self.connection.timeout())
            .danger_accept_invalid_certs(!self.connection.verify_ssl);
        if let (Some(username), Some(password)) =
            (&self.connection.username, &self.connection.password)
        {
            use base64::Engine;
            let token =
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Basic {token}"))
                    .map_err(|e| DriverError::ConnectionFailed(e.to_string()))?,
            );
            builder = builder.default_headers(headers);
        }
        builder
            .build()
            .map_err(|e| DriverError::ConnectionFailed(e.to_string()).into())
    }

    fn restore_offset(&mut self, offset: &OffsetValue) -> Result<()> {
        if let OffsetValue::Custom(value) = offset
            && value["driver"].as_str() == Some("elasticsearch_cdc")
        {
            self.last_sequence = value["last_sequence"].as_u64().ok_or_else(|| {
                DriverError::DataError("CDC checkpoint is missing last_sequence".into())
            })?;
        }
        Ok(())
    }

    fn checkpoint(&self) -> OffsetValue {
        OffsetValue::Custom(json!({
            "driver": "elasticsearch_cdc",
            "last_sequence": self.last_sequence
        }))
    }

    /// Set the first sequence to consume. Events at or below this watermark
    /// are already represented by the snapshot and must not be replayed.
    pub fn set_start_sequence(&mut self, sequence: u64) {
        self.last_sequence = sequence;
    }

    /// Capture the current change-log high watermark before opening the
    /// snapshot PIT. The producer must publish an event only after its source
    /// mutation is visible to searches, and must retain events after this value.
    pub async fn capture_watermark(&self) -> Result<u64> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| DriverError::ConnectionFailed("CDC source is not connected".into()))?;
        let response = client
            .post(format!(
                "{}/{}/_search",
                self.base_url, self.cdc.change_index
            ))
            .json(&json!({
                "size": 1,
                "track_total_hits": false,
                "sort": [{ (&self.cdc.sequence_field): { "order": "desc" } }],
                "_source": [self.cdc.sequence_field]
            }))
            .send()
            .await
            .map_err(|e| DriverError::DataError(format!("CDC watermark query failed: {e}")))?;
        if !response.status().is_success() {
            return Err(DriverError::DataError(format!(
                "CDC watermark query returned HTTP {}",
                response.status()
            ))
            .into());
        }
        let body: Value = response
            .json()
            .await
            .map_err(|e| DriverError::DataError(format!("Invalid CDC watermark response: {e}")))?;
        let hits = body["hits"]["hits"].as_array().ok_or_else(|| {
            DriverError::DataError("CDC watermark response has no hits array".into())
        })?;
        match hits.first() {
            Some(hit) => hit["_source"][&self.cdc.sequence_field]
                .as_u64()
                .ok_or_else(|| {
                    DriverError::DataError(format!(
                        "CDC watermark field '{}' is not an unsigned integer",
                        self.cdc.sequence_field
                    ))
                    .into()
                }),
            None => Ok(0),
        }
    }
}

#[async_trait]
impl Source for ElasticsearchCdcSource {
    async fn connect(&mut self) -> Result<()> {
        let urls = self.connection.parse_urls()?;
        let base_url = urls.first().ok_or_else(|| {
            DriverError::DataError("No Elasticsearch CDC source URL configured".into())
        })?;
        self.base_url = base_url.as_str().trim_end_matches('/').to_string();
        self.client = Some(self.build_client()?);
        let response = self
            .client
            .as_ref()
            .unwrap()
            .get(format!("{}/", self.base_url))
            .send()
            .await
            .map_err(|e| DriverError::ConnectionFailed(e.to_string()))?;
        if !response.status().is_success() {
            return Err(DriverError::ConnectionFailed(format!(
                "CDC source health check failed: HTTP {}",
                response.status()
            ))
            .into());
        }
        info!(change_index = %self.cdc.change_index, "Connected Elasticsearch CDC source");
        Ok(())
    }

    async fn read_batch(
        &mut self,
        offset: &OffsetValue,
        batch_size: usize,
    ) -> Result<(RecordBatch, OffsetValue)> {
        self.restore_offset(offset)?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| DriverError::ConnectionFailed("CDC source is not connected".into()))?;
        let size = batch_size.max(1).min(self.cdc.batch_size.max(1));
        let query = json!({
            "size": size,
            "track_total_hits": false,
            "query": { "range": { (&self.cdc.sequence_field): { "gt": self.last_sequence } } },
            "sort": [{ (&self.cdc.sequence_field): { "order": "asc" } }]
        });
        let response = client
            .post(format!(
                "{}/{}/_search",
                self.base_url, self.cdc.change_index
            ))
            .json(&query)
            .send()
            .await
            .map_err(|e| DriverError::DataError(format!("CDC search failed: {e}")))?;
        if !response.status().is_success() {
            return Err(DriverError::DataError(format!(
                "CDC search returned HTTP {}",
                response.status()
            ))
            .into());
        }
        let body: Value = response
            .json()
            .await
            .map_err(|e| DriverError::DataError(format!("Invalid CDC response: {e}")))?;
        let hits = body["hits"]["hits"]
            .as_array()
            .ok_or_else(|| DriverError::DataError("CDC response has no hits array".into()))?;
        let mut batch = RecordBatch::with_capacity(hits.len());
        for hit in hits {
            let source = &hit["_source"];
            let sequence = source[&self.cdc.sequence_field].as_u64().ok_or_else(|| {
                DriverError::DataError(format!(
                    "CDC event has invalid sequence field '{}'",
                    self.cdc.sequence_field
                ))
            })?;
            if sequence <= self.last_sequence {
                return Err(DatspoolError::Internal(format!(
                    "CDC sequence is not strictly increasing: {} <= {}",
                    sequence, self.last_sequence
                )));
            }
            let operation = source[&self.cdc.operation_field]
                .as_str()
                .unwrap_or("index");
            if operation != "index" && operation != "delete" {
                return Err(DriverError::DataError(format!(
                    "Unsupported CDC operation '{operation}'"
                ))
                .into());
            }
            let id = source[&self.cdc.document_id_field]
                .as_str()
                .ok_or_else(|| DriverError::DataError("CDC event is missing document_id".into()))?;
            let target_index = source[&self.cdc.target_index_field]
                .as_str()
                .ok_or_else(|| {
                    DriverError::DataError("CDC event is missing target_index".into())
                })?;
            let payload = if operation == "delete" {
                json!({})
            } else {
                source[&self.cdc.document_field].clone()
            };
            if operation == "index" && payload.is_null() {
                return Err(DriverError::DataError(
                    "CDC index event is missing document payload".into(),
                )
                .into());
            }
            let mut record = Record::with_id(id, "elasticsearch_cdc", payload);
            record.add_metadata("_op", operation);
            record.add_metadata("_target_index", target_index);
            if let Some(routing) = source[&self.cdc.routing_field].as_str() {
                record.add_metadata("_routing", routing);
            }
            record.add_metadata("_cdc_sequence", sequence.to_string());
            batch.push(record);
            self.last_sequence = sequence;
        }
        debug!(
            records = batch.len(),
            watermark = self.last_sequence,
            "Read CDC batch"
        );
        Ok((batch, self.checkpoint()))
    }

    fn is_continuous(&self) -> bool {
        true
    }

    async fn estimate_count(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        self.client = None;
        Ok(())
    }

    fn source_type(&self) -> &str {
        "elasticsearch_cdc"
    }
}

/// Atomically move an alias only after the consumer has caught up to the
/// source's final sequence watermark. The caller supplies an authenticated client.
pub async fn switch_alias_after_catchup(
    client: &Client,
    target_base_url: &str,
    alias: &str,
    old_index: Option<&str>,
    new_index: &str,
    applied_watermark: u64,
    required_watermark: u64,
) -> Result<()> {
    if applied_watermark < required_watermark {
        return Err(DatspoolError::Internal(format!(
            "Cutover blocked: target watermark {} is behind source watermark {}",
            applied_watermark, required_watermark
        )));
    }
    let mut actions = Vec::new();
    if let Some(old) = old_index {
        actions.push(json!({"remove": {"index": old, "alias": alias}}));
    }
    actions.push(json!({"add": {"index": new_index, "alias": alias}}));
    let response = client
        .post(format!(
            "{}/_aliases",
            target_base_url.trim_end_matches('/')
        ))
        .json(&json!({"actions": actions}))
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| DriverError::DataError(format!("Alias cutover request failed: {e}")))?;
    if !response.status().is_success() {
        return Err(DriverError::DataError(format!(
            "Alias cutover failed: HTTP {}: {}",
            response.status(),
            response.text().await.unwrap_or_default()
        ))
        .into());
    }
    info!(
        alias,
        new_index, applied_watermark, "Elasticsearch alias cutover completed"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdc_source_is_continuous() {
        let source = ElasticsearchCdcSource::new(
            crate::drivers::document::DocumentConfig::local("change-log"),
            CdcConfig::default(),
        );
        assert!(source.is_continuous());
    }

    #[test]
    fn cdc_offset_round_trips() {
        let config = CdcConfig::default();
        let connection = crate::drivers::document::DocumentConfig::local("change-log");
        let mut source = ElasticsearchCdcSource::new(connection, config);
        source.last_sequence = 42;
        let offset = source.checkpoint();
        let mut resumed = ElasticsearchCdcSource::new(
            crate::drivers::document::DocumentConfig::local("change-log"),
            CdcConfig::default(),
        );
        resumed.restore_offset(&offset).unwrap();
        assert_eq!(resumed.last_sequence, 42);
    }

    #[test]
    fn cutover_requires_caught_up_watermark() {
        let client = Client::new();
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(switch_alias_after_catchup(
                &client,
                "http://127.0.0.1:1",
                "live",
                None,
                "v2",
                9,
                10,
            ));
        assert!(result.is_err());
    }
}
