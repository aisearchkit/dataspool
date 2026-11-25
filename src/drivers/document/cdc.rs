// SPDX-License-Identifier: AGPL-3.0-or-later
//! Elasticsearch change-log adapter and guarded alias cutover.
//!
//! Elasticsearch does not expose a native, general-purpose document CDC feed.
//! This adapter consumes an append-only change index populated by an application
//! outbox, database-log connector, or another CDC producer.

use crate::error::{DatspoolError, DriverError, Result};
use crate::pipeline::{OffsetValue, Record, RecordBatch, Source};
use crate::raft::{Command, RaftNode};
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
            if let Some(event_id) = hit["_id"].as_str() {
                record.add_metadata("_cdc_event_id", event_id);
            }
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

/// Producer-specific write barrier. Implementations must stop accepting source
/// mutations and return the final, durable outbox sequence after all accepted
/// mutations have corresponding searchable CDC events.
#[async_trait]
pub trait CdcWriteBarrier: Send {
    async fn pause_and_capture_final_watermark(&mut self) -> Result<u64>;
    async fn resume_writes(&mut self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct CdcCutoverConfig {
    pub target_base_url: String,
    pub alias: String,
    pub old_index: Option<String>,
    pub new_index: String,
    pub timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct CdcRaftFence {
    pub task_id: String,
    pub shard_id: String,
    pub node_id: String,
    pub fencing_token: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CdcCutoverResult {
    pub final_watermark: u64,
    pub applied_watermark: u64,
}

/// Wait until the target reaches the producer's final watermark and atomically
/// switch the alias. The producer remains paused while this function runs.
async fn wait_for_catchup_and_switch<F, Fut>(
    client: &Client,
    config: &CdcCutoverConfig,
    final_watermark: u64,
    mut read_applied_watermark: F,
) -> Result<CdcCutoverResult>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<u64>>,
{
    let started = std::time::Instant::now();
    loop {
        let applied = read_applied_watermark().await?;
        if applied >= final_watermark {
            switch_alias_after_catchup(
                client,
                &config.target_base_url,
                &config.alias,
                config.old_index.as_deref(),
                &config.new_index,
                applied,
                final_watermark,
            )
            .await?;
            return Ok(CdcCutoverResult {
                final_watermark,
                applied_watermark: applied,
            });
        }
        if started.elapsed() >= config.timeout {
            return Err(DatspoolError::Internal(format!(
                "CDC cutover timed out: target watermark {applied} is behind final source watermark {final_watermark}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Coordinate a no-gap cutover and resume producer writes after the alias switch.
/// Use `cutover_with_raft` for production: it keeps writes paused until the Raft
/// cutover record has committed.
pub async fn cutover_with_write_barrier<B, F, Fut>(
    client: &Client,
    config: &CdcCutoverConfig,
    barrier: &mut B,
    read_applied_watermark: F,
) -> Result<CdcCutoverResult>
where
    B: CdcWriteBarrier,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<u64>>,
{
    let final_watermark = barrier.pause_and_capture_final_watermark().await?;
    let cutover =
        wait_for_catchup_and_switch(client, config, final_watermark, read_applied_watermark).await;
    let resumed = barrier.resume_writes().await;
    match (cutover, resumed) {
        (Ok(result), Ok(())) => Ok(result),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(resume_error)) => Err(DatspoolError::Internal(format!(
            "Alias cutover succeeded but producer writes could not be resumed: {resume_error}"
        ))),
        (Err(error), Err(resume_error)) => {
            tracing::error!(%resume_error, "Producer writes could not be resumed after failed CDC cutover");
            Err(error)
        }
    }
}

/// Run the PostgreSQL/producer write barrier, wait for catch-up, switch the alias,
/// and commit the cutover record to Raft before resuming writes. If the Raft
/// proposal fails after the alias switch, the durable producer pause is retained;
/// a retry can safely re-run the idempotent alias switch and Raft proposal.
pub async fn cutover_with_raft<B, F, Fut>(
    client: &Client,
    raft: &RaftNode,
    config: &CdcCutoverConfig,
    fence: &CdcRaftFence,
    barrier: &mut B,
    read_applied_watermark: F,
) -> Result<CdcCutoverResult>
where
    B: CdcWriteBarrier,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<u64>>,
{
    let final_watermark = barrier.pause_and_capture_final_watermark().await?;
    let result = match wait_for_catchup_and_switch(
        client,
        config,
        final_watermark,
        read_applied_watermark,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            if let Err(resume_error) = barrier.resume_writes().await {
                tracing::error!(%resume_error, "Failed to resume producer after pre-Raft cutover failure");
            }
            return Err(error);
        }
    };
    let response = raft
        .propose(Command::RecordCdcCutover {
            task_id: fence.task_id.clone(),
            alias: config.alias.clone(),
            old_index: config.old_index.clone(),
            new_index: config.new_index.clone(),
            final_watermark: result.final_watermark,
            applied_watermark: result.applied_watermark,
            fencing_shard_id: fence.shard_id.clone(),
            node_id: fence.node_id.clone(),
            fencing_token: fence.fencing_token,
            issued_at_ms: chrono::Utc::now().timestamp_millis(),
        })
        .await?;
    if !response.success {
        // Deliberately leave the persistent producer gate closed. Resuming here
        // would permit writes after the alias changed but before Raft recorded it.
        return Err(DatspoolError::Internal(format!(
            "CDC alias cutover succeeded but Raft rejected its completion record: {}",
            response.message
        )));
    }
    barrier.resume_writes().await?;
    Ok(result)
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
    // A retry after the alias API committed but before the Raft cutover record
    // must be safe. If the alias already points exclusively at the requested
    // destination, treat the switch as idempotently complete.
    let current = client
        .get(format!(
            "{}/_alias/{}",
            target_base_url.trim_end_matches('/'),
            alias
        ))
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|error| DriverError::DataError(format!("Alias state query failed: {error}")))?;
    if current.status().is_success() {
        let mapping: Value = current.json().await.map_err(|error| {
            DriverError::DataError(format!("Invalid alias state response: {error}"))
        })?;
        let already_switched = mapping.as_object().is_some_and(|indices| {
            indices.len() == 1
                && indices
                    .get(new_index)
                    .is_some_and(|entry| entry["aliases"].get(alias).is_some())
        });
        if already_switched {
            info!(
                alias,
                new_index, applied_watermark, "Elasticsearch alias cutover already committed"
            );
            return Ok(());
        }
    } else if current.status() != reqwest::StatusCode::NOT_FOUND {
        return Err(DriverError::DataError(format!(
            "Alias state query failed: HTTP {}: {}",
            current.status(),
            current.text().await.unwrap_or_default()
        ))
        .into());
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

    struct TestWriteBarrier {
        paused: bool,
        final_watermark: u64,
    }

    #[async_trait]
    impl CdcWriteBarrier for TestWriteBarrier {
        async fn pause_and_capture_final_watermark(&mut self) -> Result<u64> {
            self.paused = true;
            Ok(self.final_watermark)
        }
        async fn resume_writes(&mut self) -> Result<()> {
            self.paused = false;
            Ok(())
        }
    }

    #[tokio::test]
    async fn timed_out_cutover_resumes_source_writes_without_switching_alias() {
        let client = Client::new();
        let mut barrier = TestWriteBarrier {
            paused: false,
            final_watermark: 10,
        };
        let config = CdcCutoverConfig {
            target_base_url: "http://127.0.0.1:1".into(),
            alias: "live".into(),
            old_index: Some("old".into()),
            new_index: "new".into(),
            timeout: Duration::from_millis(1),
        };
        let result =
            cutover_with_write_barrier(&client, &config, &mut barrier, || async { Ok(9) }).await;
        assert!(result.is_err());
        assert!(!barrier.paused, "producer writes must resume after timeout");
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
