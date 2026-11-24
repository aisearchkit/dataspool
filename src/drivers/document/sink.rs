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

//! Document store sink implementation

use super::config::DocumentConfig;
use crate::error::{DriverError, Result};
use crate::pipeline::{
    AdaptiveBackpressure, BackpressureConfig, ClusterPressure, FailedRecord, Record, RecordBatch,
    Sink, WriteResult,
};
use async_trait::async_trait;
use reqwest::{Client, ClientBuilder};
use serde_json::{Value, json};
use sha2::Digest;
use tracing::info;

fn is_retryable_bulk_failure(status: u16, error_type: &str) -> bool {
    matches!(status, 429 | 502 | 503 | 504)
        || matches!(
            error_type,
            "es_rejected_execution_exception" | "unavailable_shards_exception"
        )
}

/// Document store data sink
///
/// Uses Bulk API for efficient batch writes.
pub struct DocumentSink {
    config: DocumentConfig,
    client: Option<Client>,
    base_url: String,
    target_index: String,
    backpressure: AdaptiveBackpressure,
    node_pressure: ClusterNodePressure,
    last_rejected_total: u64,
    pressure_initialized: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ClusterNodePressure {
    pub nodes: usize,
    pub write_active: u64,
    pub write_queue: u64,
    pub write_rejected: u64,
    pub write_rejected_delta: u64,
    pub write_queue_capacity: u64,
    pub queue_capacity_known: bool,
    pub queue_utilization: f64,
    pub per_node: Vec<NodeWritePressure>,
}

#[derive(Debug, Clone, Default)]
pub struct NodeWritePressure {
    pub node_id: String,
    pub name: String,
    pub write_active: u64,
    pub write_queue: u64,
    pub write_rejected: u64,
    pub write_queue_capacity: u64,
}

fn parse_node_pressure(
    stats: &Value,
    info: &Value,
    previous_rejected: u64,
    initialized: bool,
) -> (ClusterNodePressure, u64) {
    let mut pressure = ClusterNodePressure::default();
    let stats_nodes = stats["nodes"].as_object();
    let info_nodes = info["nodes"].as_object();
    if let Some(nodes) = stats_nodes {
        pressure.nodes = nodes.len();
        for (node_id, node) in nodes {
            let pool = node["thread_pool"]["write"]
                .as_object()
                .or_else(|| node["thread_pool"]["bulk"].as_object());
            let Some(pool) = pool else {
                continue;
            };
            let info_pool = info_nodes
                .and_then(|nodes| nodes.get(node_id))
                .and_then(|node| {
                    node["thread_pool"]["write"]
                        .as_object()
                        .or_else(|| node["thread_pool"]["bulk"].as_object())
                });
            let capacity = info_pool
                .and_then(|pool| {
                    pool.get("queue_size")
                        .or_else(|| pool.get("max_queue_size"))
                })
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let item = NodeWritePressure {
                node_id: node_id.clone(),
                name: node["name"].as_str().unwrap_or_default().to_string(),
                write_active: pool.get("active").and_then(Value::as_u64).unwrap_or(0),
                write_queue: pool.get("queue").and_then(Value::as_u64).unwrap_or(0),
                write_rejected: pool.get("rejected").and_then(Value::as_u64).unwrap_or(0),
                write_queue_capacity: capacity,
            };
            pressure.write_active += item.write_active;
            pressure.write_queue += item.write_queue;
            pressure.write_rejected += item.write_rejected;
            pressure.write_queue_capacity += item.write_queue_capacity;
            pressure.per_node.push(item);
        }
    }
    pressure.queue_capacity_known = pressure.write_queue_capacity > 0;
    if pressure.queue_capacity_known {
        pressure.queue_utilization =
            (pressure.write_queue as f64 / pressure.write_queue_capacity as f64).clamp(0.0, 1.0);
    }
    let rejected_delta = if initialized {
        pressure.write_rejected.saturating_sub(previous_rejected)
    } else {
        0
    };
    (pressure, rejected_delta)
}

fn validate_compensation_identity(migration_id: &str, _generation: u64) -> Result<()> {
    if migration_id.is_empty()
        || migration_id.len() > 96
        || !migration_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(DriverError::DataError(
            "Invalid compensation identity: migration_id must be 1-96 ASCII alphanumeric/dash/underscore characters".into()
        ).into());
    }
    Ok(())
}

impl DocumentSink {
    /// Create new document store sink
    pub fn new(config: DocumentConfig, target_index: String) -> Self {
        Self {
            config,
            client: None,
            base_url: String::new(),
            target_index,
            backpressure: AdaptiveBackpressure::new(BackpressureConfig::default()),
            node_pressure: ClusterNodePressure::default(),
            last_rejected_total: 0,
            pressure_initialized: false,
        }
    }

    /// Override the adaptive worker and batch limits (useful for task-level tuning).
    pub fn with_backpressure(mut self, config: BackpressureConfig) -> Self {
        self.backpressure = AdaptiveBackpressure::new(config);
        self
    }

    /// Build HTTP client
    fn build_client(&self) -> Result<Client> {
        let mut builder = ClientBuilder::new()
            .timeout(self.config.timeout())
            .danger_accept_invalid_certs(!self.config.verify_ssl);

        // Add Basic Auth as default header if configured
        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            use base64::Engine;
            let credentials = format!("{}:{}", username, password);
            let encoded = base64::engine::general_purpose::STANDARD.encode(credentials.as_bytes());

            let mut headers = reqwest::header::HeaderMap::new();
            let auth_value = format!("Basic {}", encoded);
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&auth_value).map_err(|e| {
                    DriverError::ConnectionFailed(format!("Invalid auth header: {}", e))
                })?,
            );
            builder = builder.default_headers(headers);
        }

        builder.build().map_err(|e| {
            DriverError::ConnectionFailed(format!("Failed to build HTTP client: {}", e)).into()
        })
    }

    /// Build bulk request body in NDJSON format
    #[cfg(test)]
    fn build_bulk_body(&self, batch: &RecordBatch) -> String {
        Self::build_bulk_body_for(&self.target_index, batch)
    }

    /// Last observed write-thread-pool pressure across cluster nodes.
    pub fn node_pressure(&self) -> ClusterNodePressure {
        self.node_pressure.clone()
    }

    async fn refresh_node_pressure(&mut self, client: &Client) {
        let stats_response = client
            .get(format!("{}/_nodes/stats/thread_pool", self.base_url))
            .send()
            .await;
        let Ok(stats_response) = stats_response else {
            return;
        };
        if !stats_response.status().is_success() {
            return;
        }
        let Ok(stats) = stats_response.json::<Value>().await else {
            return;
        };

        // Stats provide active/queue/rejected; node info provides configured queue capacity.
        // If capacity is unavailable, utilization remains explicitly unknown (0.0) and
        // latency/429 feedback still controls AIMD.
        let info_response = client
            .get(format!("{}/_nodes/info/thread_pool", self.base_url))
            .send()
            .await;
        let info = match info_response {
            Ok(response) if response.status().is_success() => {
                response.json::<Value>().await.unwrap_or(Value::Null)
            }
            _ => Value::Null,
        };
        let (mut pressure, rejected_delta) = parse_node_pressure(
            &stats,
            &info,
            self.last_rejected_total,
            self.pressure_initialized,
        );
        pressure.write_rejected_delta = rejected_delta;
        self.last_rejected_total = pressure.write_rejected;
        self.pressure_initialized = true;
        self.node_pressure = pressure;
    }

    fn build_bulk_body_for(target_index: &str, batch: &RecordBatch) -> String {
        let mut body = String::new();
        for record in batch.records() {
            let index = record.get_metadata("_target_index").unwrap_or(target_index);
            let operation = record.get_metadata("_op").unwrap_or("index");
            let mut meta = serde_json::Map::new();
            meta.insert("_index".into(), json!(index));
            if let Some(id) = &record.id {
                meta.insert("_id".into(), json!(id));
            }
            if let Some(routing) = record.get_metadata("_routing") {
                meta.insert("routing".into(), json!(routing));
            }
            if let Some(version) = record.get_metadata("_dataspool_fencing_token")
                && record.id.is_some()
                && let Ok(version) = version.parse::<i64>()
            {
                meta.insert("version".into(), json!(version));
                meta.insert("version_type".into(), json!("external_gte"));
            }
            let operation = if record
                .get_metadata("_dataspool_compensation_migration_id")
                .is_some()
            {
                "create"
            } else {
                operation
            };
            let mut action = serde_json::Map::new();
            action.insert(operation.to_string(), Value::Object(meta));
            body.push_str(&Value::Object(action).to_string());
            body.push('\n');
            if operation != "delete" {
                let mut payload = record.payload.clone();
                if let Value::Object(ref mut fields) = payload {
                    let mut datas_pool = fields
                        .get("_dataspool")
                        .and_then(Value::as_object)
                        .cloned()
                        .unwrap_or_default();
                    if let (Some(migration_id), Some(generation)) = (
                        record.get_metadata("_dataspool_compensation_migration_id"),
                        record.get_metadata("_dataspool_compensation_generation"),
                    ) {
                        let payload_digest = format!(
                            "{:x}",
                            sha2::Sha256::digest(record.payload.to_string().as_bytes())
                        );
                        datas_pool.insert("migration_id".into(), json!(migration_id));
                        datas_pool.insert(
                            "generation".into(),
                            json!(generation.parse::<u64>().unwrap_or_default()),
                        );
                        datas_pool.insert("payload_digest".into(), json!(payload_digest));
                    }
                    if let (Some(fencing_version), Some(shard_id), Some(batch_id)) = (
                        record.get_metadata("_dataspool_fencing_token"),
                        record.get_metadata("_dataspool_fencing_shard_id"),
                        record.get_metadata("_dataspool_fencing_batch_id"),
                    ) {
                        datas_pool.insert(
                            "fencing_version".into(),
                            json!(fencing_version.parse::<i64>().unwrap_or_default()),
                        );
                        datas_pool.insert("fencing_shard_id".into(), json!(shard_id));
                        datas_pool.insert("fencing_batch_id".into(), json!(batch_id));
                    }
                    if !datas_pool.is_empty() {
                        fields.insert("_dataspool".into(), Value::Object(datas_pool));
                    }
                }
                body.push_str(&payload.to_string());
                body.push('\n');
            }
        }
        body
    }

    async fn write_worker(
        client: Client,
        url: String,
        target_index: String,
        records: Vec<Record>,
    ) -> Result<(WriteResult, std::time::Duration)> {
        let started = std::time::Instant::now();
        let mut pending = records;
        let mut result = WriteResult::default();
        let mut attempt = 0_u32;
        const MAX_RETRIES: u32 = 8;
        while !pending.is_empty() {
            let body =
                Self::build_bulk_body_for(&target_index, &RecordBatch::from(pending.clone()));
            result.bytes_written += body.len() as u64;
            let response = client
                .post(&url)
                .header("Content-Type", "application/x-ndjson")
                .body(body)
                .send()
                .await
                .map_err(|e| {
                    DriverError::DataError(format!(
                        "Bulk worker request failed; durable cache retains batch: {e}"
                    ))
                })?;
            let status = response.status();
            if !status.is_success() {
                if matches!(status.as_u16(), 429 | 502 | 503 | 504) && attempt < MAX_RETRIES {
                    result.retried_count += pending.len();
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100_u64.saturating_mul(1 << attempt.min(4)),
                    ))
                    .await;
                    attempt += 1;
                    continue;
                }
                for record in pending.drain(..) {
                    if matches!(status.as_u16(), 429 | 502 | 503 | 504) {
                        result.rejected_count += 1;
                    } else {
                        result.failed_records.push(FailedRecord {
                            record,
                            status: status.as_u16(),
                            error_type: "bulk_http_error".into(),
                            reason: format!("HTTP {status}"),
                        });
                    }
                    result.failed_count += 1;
                }
                result.add_error(format!("Bulk worker failed: HTTP {status}"));
                break;
            }
            let response: Value = response.json().await.map_err(|e| {
                DriverError::DataError(format!(
                    "Invalid bulk response; durable cache retains batch: {e}"
                ))
            })?;
            let items = response["items"].as_array().ok_or_else(|| {
                DriverError::DataError("Bulk response missing items array".into())
            })?;
            if items.len() != pending.len() {
                return Err(DriverError::DataError(format!(
                    "Bulk item count mismatch: sent {}, received {}",
                    pending.len(),
                    items.len()
                ))
                .into());
            }
            let mut retry = Vec::new();
            for (record, item) in pending.into_iter().zip(items) {
                let item = item
                    .as_object()
                    .and_then(|m| m.values().next())
                    .ok_or_else(|| {
                        DriverError::DataError("Bulk response item is not an action object".into())
                    })?;
                let status = item["status"].as_u64().ok_or_else(|| {
                    DriverError::DataError("Bulk response item is missing status".into())
                })? as u16;
                if (200..300).contains(&status) {
                    result.success_count += 1;
                    continue;
                }
                let error_type = item["error"]["type"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_string();
                let reason = item["error"]["reason"]
                    .as_str()
                    .unwrap_or("no reason supplied")
                    .to_string();
                if status == 409 && error_type == "version_conflict_engine_exception" {
                    if let (Some(migration_id), Some(generation), Some(document_id)) = (
                        record.get_metadata("_dataspool_compensation_migration_id"),
                        record.get_metadata("_dataspool_compensation_generation"),
                        record.id.as_deref(),
                    ) {
                        // Create-only replay is successful only when the existing document
                        // carries this exact migration generation marker. Never hide a
                        // pre-existing document ID collision as an idempotent success.
                        let index = record
                            .get_metadata("_target_index")
                            .unwrap_or(&target_index);
                        let existing = client
                            .post(format!(
                                "{}/{}/_mget",
                                url.trim_end_matches("/_bulk"),
                                index
                            ))
                            .json(&json!({"ids":[document_id]}))
                            .send()
                            .await;
                        let matches_operation = match existing {
                            Ok(response) if response.status().is_success() => {
                                match response.json::<Value>().await {
                                    Ok(value) => value["docs"]
                                        .as_array()
                                        .and_then(|docs| docs.first())
                                        .is_some_and(|doc| {
                                            doc["found"] == true
                                                && doc["_source"]["_dataspool"]["migration_id"]
                                                    == migration_id
                                                && doc["_source"]["_dataspool"]["generation"]
                                                    == generation.parse::<u64>().unwrap_or_default()
                                                && doc["_source"]["_dataspool"]["payload_digest"]
                                                    == format!(
                                                        "{:x}",
                                                        sha2::Sha256::digest(
                                                            record.payload.to_string().as_bytes()
                                                        )
                                                    )
                                        }),
                                    Err(_) => false,
                                }
                            }
                            _ => false,
                        };
                        if matches_operation {
                            result.success_count += 1;
                        } else {
                            // A create conflict outside this exact payload identity is not
                            // a DLQ-able record: acknowledging it could falsely complete a
                            // migration while silently skipping or replacing user data.
                            result.failed_count += 1;
                            result.add_error(format!(
                                "Compensatable create conflict for document {document_id}: existing document is not the same migration generation and payload"
                            ));
                        }
                    } else if let (Some(version), Some(shard_id), Some(document_id)) = (
                        record
                            .get_metadata("_dataspool_fencing_token")
                            .and_then(|value| value.parse::<i64>().ok()),
                        record.get_metadata("_dataspool_fencing_shard_id"),
                        record.id.as_deref(),
                    ) {
                        // A conflict is idempotent only if the stored document proves that
                        // this same shard has already written it with an equal/newer fence.
                        // An unrelated pre-existing document must never advance the checkpoint.
                        let index = record
                            .get_metadata("_target_index")
                            .unwrap_or(&target_index);
                        let existing = client
                            .post(format!(
                                "{}/{}/_mget",
                                url.trim_end_matches("/_bulk"),
                                index
                            ))
                            .json(&json!({"ids":[document_id]}))
                            .send()
                            .await;
                        let fenced_by_same_shard = match existing {
                            Ok(response) if response.status().is_success() => {
                                match response.json::<Value>().await {
                                    Ok(value) => value["docs"]
                                        .as_array()
                                        .and_then(|docs| docs.first())
                                        .is_some_and(|doc| {
                                            doc["found"] == true
                                                && doc["_version"]
                                                    .as_i64()
                                                    .is_some_and(|stored| stored >= version)
                                                && doc["_source"]["_dataspool"]["fencing_version"]
                                                    .as_i64()
                                                    .is_some_and(|stored| stored >= version)
                                                && doc["_source"]["_dataspool"]["fencing_shard_id"]
                                                    == shard_id
                                        }),
                                    Err(_) => false,
                                }
                            }
                            _ => false,
                        };
                        if fenced_by_same_shard {
                            result.success_count += 1;
                        } else {
                            result.failed_count += 1;
                            result.add_error(format!(
                                "Fenced write conflict for document {document_id}: stored version is not proven to belong to shard {shard_id}"
                            ));
                        }
                    } else {
                        result.failed_count += 1;
                        result.add_error("Version conflict cannot be treated as an idempotent fenced write: missing fencing identity".into());
                    }
                    continue;
                }
                if is_retryable_bulk_failure(status, &error_type) && attempt < MAX_RETRIES {
                    retry.push(record);
                } else {
                    result.failed_count += 1;
                    if is_retryable_bulk_failure(status, &error_type) {
                        result.rejected_count += 1;
                    } else {
                        result.failed_records.push(FailedRecord {
                            record,
                            status,
                            error_type: error_type.clone(),
                            reason: reason.clone(),
                        });
                    }
                    result.add_error(format!("HTTP {status} {error_type}: {reason}"));
                }
            }
            if !retry.is_empty() {
                result.retried_count += retry.len();
            }
            pending = retry;
            if !pending.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(
                    100_u64.saturating_mul(1 << attempt.min(4)),
                ))
                .await;
                attempt += 1;
            }
        }
        Ok((result, started.elapsed()))
    }

    /// Write documents in create-only mode and tag them with a migration identity.
    /// Existing documents are never overwritten, making this mode safely compensatable.
    pub async fn write_batch_compensatable(
        &mut self,
        batch: RecordBatch,
        migration_id: &str,
        generation: u64,
    ) -> Result<WriteResult> {
        validate_compensation_identity(migration_id, generation)?;
        let mut records = batch.into_records();
        for record in &mut records {
            if record.id.is_none() {
                return Err(DriverError::Unsupported(
                    "Compensatable Elasticsearch writes require stable document IDs".into(),
                )
                .into());
            }
            if !record.payload.is_object() {
                return Err(DriverError::DataError(
                    "Compensatable Elasticsearch writes require object documents".into(),
                )
                .into());
            }
            if record
                .get_metadata("_op")
                .is_some_and(|operation| operation != "index")
            {
                return Err(DriverError::Unsupported(
                    "Compensatable Elasticsearch writes support append-only index operations only"
                        .into(),
                )
                .into());
            }
            if record
                .get_metadata("_target_index")
                .is_some_and(|index| index != self.target_index)
            {
                return Err(DriverError::Unsupported(
                    "Compensatable Elasticsearch writes must target the configured index".into(),
                )
                .into());
            }
            if record.payload.get("_dataspool").is_some() {
                return Err(DriverError::DataError(
                    "Document payload uses reserved _dataspool field".into(),
                )
                .into());
            }
            record.add_metadata("_dataspool_compensation_migration_id", migration_id);
            record.add_metadata("_dataspool_compensation_generation", generation.to_string());
        }
        self.write_batch(RecordBatch::from(records)).await
    }

    /// Replay-safe compensation for documents created by `write_batch_compensatable`.
    /// The journal is stored in Elasticsearch so a restarted worker can resume a started
    /// operation. The query is generation-scoped and cannot delete pre-existing documents.
    pub async fn compensate_migration(&self, migration_id: &str, generation: u64) -> Result<u64> {
        validate_compensation_identity(migration_id, generation)?;
        let client = self.client.as_ref().ok_or_else(|| {
            DriverError::ConnectionFailed("Not connected to document store".into())
        })?;
        let operation_id = format!("{}-{}", migration_id, generation);
        let index_response = client
            .put(format!("{}/.dataspool-compensation", self.base_url))
            .send()
            .await
            .map_err(|error| {
                DriverError::DataError(format!(
                    "Failed to initialize compensation journal: {error}"
                ))
            })?;
        if !index_response.status().is_success() {
            let status = index_response.status();
            let body = index_response.json::<Value>().await.unwrap_or(Value::Null);
            if status.as_u16() != 400
                || body["error"]["type"] != "resource_already_exists_exception"
            {
                return Err(DriverError::DataError(format!(
                    "Failed to initialize compensation journal: HTTP {status}: {body}"
                ))
                .into());
            }
        }
        let journal_url = format!(
            "{}/.dataspool-compensation/_doc/{}",
            self.base_url, operation_id
        );
        let journal = client.get(&journal_url).send().await.map_err(|error| {
            DriverError::DataError(format!("Failed to read compensation journal: {error}"))
        })?;
        if journal.status().is_success() {
            let value: Value = journal.json().await.map_err(|error| {
                DriverError::DataError(format!("Invalid compensation journal: {error}"))
            })?;
            if value["_source"]["state"] == "completed" {
                return Ok(value["_source"]["deleted"].as_u64().unwrap_or(0));
            }
        } else if journal.status().as_u16() != 404 {
            return Err(DriverError::DataError(format!(
                "Compensation journal lookup failed: HTTP {}",
                journal.status()
            ))
            .into());
        }

        let started = client
            .put(&journal_url)
            .json(&json!({
                "migration_id": migration_id,
                "generation": generation,
                "state": "started",
                "updated_at_ms": chrono::Utc::now().timestamp_millis(),
            }))
            .send()
            .await
            .map_err(|error| {
                DriverError::DataError(format!("Failed to persist compensation intent: {error}"))
            })?;
        if !started.status().is_success() {
            return Err(DriverError::DataError(format!(
                "Failed to persist compensation intent: HTTP {}",
                started.status()
            ))
            .into());
        }

        let response = client
            .post(format!(
                "{}/{}/_delete_by_query?refresh=true&conflicts=abort",
                self.base_url, self.target_index
            ))
            .json(&json!({"query":{"bool":{"filter":[
                {"term":{"_dataspool.migration_id.keyword":migration_id}},
                {"term":{"_dataspool.generation":generation}}
            ]}}}))
            .send()
            .await
            .map_err(|error| {
                DriverError::DataError(format!(
                    "Compensation delete request failed; journal remains started: {error}"
                ))
            })?;
        let status = response.status();
        let deleted = if status.as_u16() == 404 {
            // No target index means no migration-owned documents were created.
            0
        } else {
            if !status.is_success() {
                return Err(DriverError::DataError(format!(
                    "Compensation delete failed: HTTP {status}; journal remains started"
                ))
                .into());
            }
            let result: Value = response.json().await.map_err(|error| {
                DriverError::DataError(format!(
                    "Invalid compensation response; journal remains started: {error}"
                ))
            })?;
            if result["timed_out"] == true
                || result["version_conflicts"].as_u64().unwrap_or(0) > 0
                || result["failures"]
                    .as_array()
                    .is_some_and(|failures| !failures.is_empty())
            {
                return Err(DriverError::DataError(
                    "Elasticsearch reported partial compensation failures; operation remains recoverable".into()
                ).into());
            }
            result["deleted"].as_u64().unwrap_or(0)
        };
        let completed = client
            .put(&journal_url)
            .json(&json!({
                "migration_id": migration_id,
                "generation": generation,
                "state": "completed",
                "deleted": deleted,
                "updated_at_ms": chrono::Utc::now().timestamp_millis(),
            }))
            .send()
            .await
            .map_err(|error| {
                DriverError::DataError(format!(
                    "Compensation completed but journal finalization failed: {error}"
                ))
            })?;
        if !completed.status().is_success() {
            return Err(DriverError::DataError(format!(
                "Compensation completed but journal finalization failed: HTTP {}",
                completed.status()
            ))
            .into());
        }
        Ok(deleted)
    }

    /// Parse bulk response and extract errors
    #[cfg(test)]
    fn parse_bulk_response(&self, response: &Value, total: usize) -> WriteResult {
        let items = match response["items"].as_array() {
            Some(items) => items,
            None => {
                let mut result = WriteResult::default();
                result.add_error("No items array in bulk response".to_string());
                result.failed_count = total;
                return result;
            }
        };

        let mut result = WriteResult::default();

        for item in items {
            let index_result = &item["index"];

            // Check status code
            let status = index_result["status"].as_u64().unwrap_or(500);

            if (200..300).contains(&status) {
                result.success_count += 1;
            } else {
                result.failed_count += 1;

                // Extract error message
                if let Some(error) = index_result["error"].as_object() {
                    let error_type = error
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    let error_reason = error
                        .get("reason")
                        .and_then(|v| v.as_str())
                        .unwrap_or("no reason");

                    result.add_error(format!("{}: {}", error_type, error_reason));
                } else {
                    result.add_error(format!("HTTP {}", status));
                }
            }
        }

        result
    }
}

#[async_trait]
impl Sink for DocumentSink {
    async fn connect(&mut self) -> Result<()> {
        info!("Connecting to document store sink: {:?}", self.config.urls);

        let urls = self.config.parse_urls()?;
        if urls.is_empty() {
            return Err(
                DriverError::DataError("No document store URLs provided".to_string()).into(),
            );
        }

        self.base_url = urls[0].as_str().trim_end_matches('/').to_string();

        let client = self.build_client()?;

        // Verify connection
        let url = format!("{}/", self.base_url);
        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|e| DriverError::ConnectionFailed(format!("Failed to connect: {}", e)))?;

        if !response.status().is_success() {
            return Err(DriverError::ConnectionFailed(format!(
                "Connection failed: HTTP {}",
                response.status()
            ))
            .into());
        }

        self.client = Some(client);

        info!(
            "Connected to document store, target index: {}",
            self.target_index
        );
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<WriteResult> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| DriverError::ConnectionFailed("Not connected to document store".into()))?
            .clone();
        if batch.is_empty() {
            return Ok(WriteResult::success(0, 0));
        }
        self.refresh_node_pressure(&client).await;
        let pressure = self.node_pressure.clone();
        let current = self.backpressure.observe(ClusterPressure {
            status_code: 200,
            // Elasticsearch exposes this as a process-lifetime counter; only per-request
            // rejections may drive the AIMD rejection signal.
            rejected: pressure.write_rejected_delta,
            queue_utilization: pressure.queue_utilization,
            ..Default::default()
        });
        if !current.pause.is_zero() {
            tokio::time::sleep(current.pause).await;
        }
        let records = batch.into_records();
        let worker_limit = current.max_in_flight.max(1).min(records.len().max(1));
        let fair_share = records.len().div_ceil(worker_limit);
        let chunk_size = current.batch_size.max(1).min(fair_share.max(1));
        let chunks = records
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();
        let url = format!("{}/_bulk", self.base_url);
        let mut workers = tokio::task::JoinSet::new();
        let mut iter = chunks.into_iter();
        let mut aggregate = WriteResult::default();
        loop {
            while workers.len() < worker_limit {
                let Some(chunk) = iter.next() else {
                    break;
                };
                workers.spawn(Self::write_worker(
                    client.clone(),
                    url.clone(),
                    self.target_index.clone(),
                    chunk,
                ));
            }
            let Some(joined) = workers.join_next().await else {
                break;
            };
            let (result, took) = joined
                .map_err(|e| DriverError::DataError(format!("Bulk worker join failed: {e}")))??;
            aggregate.success_count += result.success_count;
            aggregate.failed_count += result.failed_count;
            aggregate.rejected_count += result.rejected_count;
            aggregate.retried_count += result.retried_count;
            aggregate.bytes_written += result.bytes_written;
            aggregate.failed_records.extend(result.failed_records);
            aggregate.errors.extend(result.errors);
            let window = self.backpressure.observe(ClusterPressure {
                took,
                status_code: if result.rejected_count > 0 { 429 } else { 200 },
                rejected: result.rejected_count as u64,
                queue_utilization: pressure.queue_utilization,
            });
            if !window.pause.is_zero() {
                tokio::time::sleep(window.pause).await;
            }
        }
        Ok(aggregate)
    }

    async fn write_batch_compensatable(
        &mut self,
        batch: RecordBatch,
        migration_id: &str,
        generation: u64,
    ) -> Result<WriteResult> {
        DocumentSink::write_batch_compensatable(self, batch, migration_id, generation).await
    }

    async fn compensate_migration(&mut self, migration_id: &str, generation: u64) -> Result<u64> {
        DocumentSink::compensate_migration(self, migration_id, generation).await
    }

    async fn write_batch_fenced(
        &mut self,
        batch: RecordBatch,
        fencing_version: i64,
        shard_id: &str,
        batch_id: &str,
    ) -> Result<WriteResult> {
        if fencing_version <= 0 {
            return Err(
                DriverError::DataError("Invalid Elasticsearch fencing version".into()).into(),
            );
        }
        let mut records = batch.into_records();
        for record in &mut records {
            if record.id.is_none() {
                return Err(DriverError::Unsupported(
                    "Fenced Elasticsearch writes require stable document IDs".into(),
                )
                .into());
            }
            if record.get_metadata("_op") != Some("delete") && !record.payload.is_object() {
                return Err(DriverError::Unsupported(
                    "Fenced Elasticsearch writes require object document payloads to persist fencing provenance".into(),
                ).into());
            }
            record.add_metadata("_dataspool_fencing_token", fencing_version.to_string());
            record.add_metadata("_dataspool_fencing_shard_id", shard_id.to_string());
            record.add_metadata("_dataspool_fencing_batch_id", batch_id.to_string());
        }
        self.write_batch(RecordBatch::from(records)).await
    }

    async fn flush(&mut self) -> Result<()> {
        // Bulk API is synchronous, no buffering to flush
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.client = None;
        info!("Document store sink closed");
        Ok(())
    }

    fn sink_type(&self) -> &str {
        "document"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::Record;
    use serde_json::json;

    #[test]
    fn test_bulk_retry_classification() {
        assert!(is_retryable_bulk_failure(429, "unknown"));
        assert!(is_retryable_bulk_failure(
            500,
            "es_rejected_execution_exception"
        ));
        assert!(!is_retryable_bulk_failure(400, "mapper_parsing_exception"));
    }

    #[tokio::test]
    async fn retries_http_429_and_reports_retried_documents() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        async fn read_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            loop {
                let read = stream.read(&mut chunk).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers =
                        String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
                    let content_length = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if request.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
            }
            request
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut bulk_requests = 0;
            for _ in 0..5 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request(&mut stream).await;
                let first_line = String::from_utf8_lossy(&request)
                    .lines()
                    .next()
                    .unwrap_or("")
                    .to_string();
                let (status, body) = if first_line.contains("GET /_nodes/stats/thread_pool") {
                    ("200 OK", r#"{"nodes":{}}"#.to_string())
                } else if first_line.contains("GET /") {
                    ("200 OK", r#"{"version":{"number":"8.19.8"}}"#.to_string())
                } else if first_line.contains("POST /_bulk") {
                    bulk_requests += 1;
                    if bulk_requests == 1 {
                        (
                            "429 Too Many Requests",
                            r#"{"error":"simulated rejection"}"#.to_string(),
                        )
                    } else {
                        ("200 OK", r#"{"errors":false,"items":[{"index":{"status":201}},{"index":{"status":201}}]}"#.to_string())
                    }
                } else {
                    ("404 Not Found", "{}".to_string())
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });

        let mut config = DocumentConfig::local("target");
        config.urls = vec![format!("http://{address}")];
        let mut sink = DocumentSink::new(config, "target".into());
        sink.connect().await.unwrap();
        let records = vec![
            Record::with_id("one", "test", json!({"value":1})),
            Record::with_id("two", "test", json!({"value":2})),
        ];
        let result = sink.write_batch(RecordBatch::from(records)).await.unwrap();
        assert_eq!(result.success_count, 2);
        assert_eq!(result.failed_count, 0);
        assert_eq!(result.retried_count, 2);
        sink.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn partial_bulk_success_retries_only_transient_documents_and_preserves_dlq_items() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        async fn read_request(stream: &mut tokio::net::TcpStream) -> String {
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            loop {
                let read = stream.read(&mut chunk).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers =
                        String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
                    let content_length = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if request.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
            }
            String::from_utf8_lossy(&request).into_owned()
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut request_sizes = Vec::new();
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request(&mut stream).await;
                let header_end = request.find("\r\n\r\n").unwrap() + 4;
                let action_lines = request[header_end..].lines().step_by(2).count();
                request_sizes.push(action_lines);
                let body = if attempt == 0 {
                    r#"{"errors":true,"items":[{"index":{"status":201}},{"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"invalid field"}}},{"index":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"queue full"}}}]}"#
                } else {
                    r#"{"errors":false,"items":[{"index":{"status":201}}]}"#
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
            request_sizes
        });

        let records = vec![
            Record::with_id("success", "test", json!({"v": 1})),
            Record::with_id("permanent-failure", "test", json!({"v": 2})),
            Record::with_id("transient-failure", "test", json!({"v": 3})),
        ];
        let (result, _) = DocumentSink::write_worker(
            Client::new(),
            format!("http://{address}/_bulk"),
            "target".into(),
            records,
        )
        .await
        .unwrap();
        let request_sizes = server.await.unwrap();
        assert_eq!(
            request_sizes,
            vec![3, 1],
            "only the transiently rejected document may be retried"
        );
        assert_eq!(result.success_count, 2);
        assert_eq!(result.failed_count, 1);
        assert_eq!(result.retried_count, 1);
        assert_eq!(result.rejected_count, 0);
        assert_eq!(result.failed_records.len(), 1);
        assert_eq!(
            result.failed_records[0].record.id.as_deref(),
            Some("permanent-failure")
        );
        assert_eq!(result.failed_records[0].status, 400);
        assert_eq!(
            result.failed_records[0].error_type,
            "mapper_parsing_exception"
        );
    }

    #[tokio::test]
    async fn compensatable_create_conflict_with_unowned_document_is_fatal_not_dlq() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        async fn read_request(stream: &mut tokio::net::TcpStream) {
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            loop {
                let read = stream.read(&mut chunk).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers =
                        String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
                    let content_length = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if request.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for (status, body) in [
                (
                    "200 OK",
                    r#"{"errors":true,"items":[{"create":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"document already exists"}}}]}"#,
                ),
                (
                    "200 OK",
                    r#"{"docs":[{"_id":"doc-1","found":true,"_source":{"_dataspool":{"migration_id":"another-migration","generation":0,"payload_digest":"different"}}}]}"#,
                ),
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                read_request(&mut stream).await;
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });

        let mut record = Record::with_id("doc-1", "elasticsearch", json!({"value": 1}));
        record.add_metadata("_dataspool_compensation_migration_id", "migration-1");
        record.add_metadata("_dataspool_compensation_generation", "0");
        let (result, _) = DocumentSink::write_worker(
            Client::new(),
            format!("http://{address}/_bulk"),
            "target".into(),
            vec![record],
        )
        .await
        .unwrap();
        assert_eq!(result.failed_count, 1);
        assert!(result.failed_records.is_empty());
        assert!(!result.errors.is_empty());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn compensation_journal_makes_replay_after_lost_response_idempotent() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        async fn read_request(stream: &mut tokio::net::TcpStream) -> String {
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            loop {
                let read = stream.read(&mut chunk).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers =
                        String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
                    let content_length = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .and_then(|value| value.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    if request.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
            }
            String::from_utf8_lossy(&request).to_string()
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut delete_requests = 0;
            for request_number in 0..7 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request(&mut stream).await;
                let first_line = request.lines().next().unwrap_or_default();
                let (status, body) = match request_number {
                    0 if first_line.starts_with("PUT /.dataspool-compensation ") => {
                        ("200 OK", "{}")
                    }
                    1 if first_line.starts_with("GET /.dataspool-compensation/_doc/") => {
                        ("404 Not Found", "{}")
                    }
                    2 if first_line.starts_with("PUT /.dataspool-compensation/_doc/") => {
                        ("201 Created", "{}")
                    }
                    3 if first_line.starts_with("POST /target/_delete_by_query") => {
                        delete_requests += 1;
                        (
                            "200 OK",
                            r#"{"deleted":2,"failures":[],"timed_out":false,"version_conflicts":0}"#,
                        )
                    }
                    4 if first_line.starts_with("PUT /.dataspool-compensation/_doc/") => {
                        ("200 OK", "{}")
                    }
                    5 if first_line.starts_with("PUT /.dataspool-compensation ") => (
                        "400 Bad Request",
                        r#"{"error":{"type":"resource_already_exists_exception"}}"#,
                    ),
                    6 if first_line.starts_with("GET /.dataspool-compensation/_doc/") => {
                        ("200 OK", r#"{"_source":{"state":"completed","deleted":2}}"#)
                    }
                    _ => panic!("unexpected compensation request: {first_line}"),
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
            delete_requests
        });

        let mut sink = DocumentSink::new(DocumentConfig::local("target"), "target".into());
        sink.base_url = format!("http://{address}");
        sink.client = Some(Client::new());
        assert_eq!(
            sink.compensate_migration("migration-1", 1).await.unwrap(),
            2
        );
        // Models a source leader losing the compensation response and replaying it.
        assert_eq!(
            sink.compensate_migration("migration-1", 1).await.unwrap(),
            2
        );
        assert_eq!(server.await.unwrap(), 1, "delete-by-query must run once");
    }

    #[test]
    fn parses_cluster_node_queue_capacity_and_rejection_deltas() {
        let stats = json!({"nodes":{
            "n1":{"name":"data-1","thread_pool":{"write":{"active":2,"queue":50,"rejected":12}}},
            "n2":{"name":"data-2","thread_pool":{"write":{"active":1,"queue":10,"rejected":3}}}
        }});
        let info = json!({"nodes":{
            "n1":{"thread_pool":{"write":{"queue_size":100}}},
            "n2":{"thread_pool":{"write":{"queue_size":100}}}
        }});
        let (first, first_delta) = parse_node_pressure(&stats, &info, 0, false);
        assert_eq!(first.nodes, 2);
        assert_eq!(first.write_queue, 60);
        assert_eq!(first.write_queue_capacity, 200);
        assert_eq!(first.queue_utilization, 0.3);
        assert!(first.queue_capacity_known);
        assert_eq!(
            first_delta, 0,
            "first sample establishes the counter baseline"
        );

        let (next, delta) = parse_node_pressure(&stats, &info, 10, true);
        assert_eq!(next.write_rejected, 15);
        assert_eq!(delta, 5);

        let (unknown, _) = parse_node_pressure(&stats, &Value::Null, 0, false);
        assert!(!unknown.queue_capacity_known);
        assert_eq!(unknown.queue_utilization, 0.0);
    }

    #[test]
    fn test_create_sink() {
        let config = DocumentConfig::local("source-index");
        let sink = DocumentSink::new(config, "target-index".to_string());

        assert_eq!(sink.sink_type(), "document");
        assert_eq!(sink.target_index, "target-index");
    }

    #[test]
    fn test_build_bulk_body() {
        let config = DocumentConfig::local("test");
        let sink = DocumentSink::new(config, "target".to_string());

        let mut batch = RecordBatch::new();
        batch.push(Record::with_id("doc1", "test", json!({"field": "value1"})));
        batch.push(Record::new("test", json!({"field": "value2"})));

        let body = sink.build_bulk_body(&batch);

        // Should have 4 lines: action, source, action, source
        assert_eq!(body.lines().count(), 4);
        assert!(body.contains(r#""_id":"doc1""#));
        assert!(body.contains(r#""field":"value1""#));
    }

    #[test]
    fn test_build_bulk_body_supports_cdc_delete() {
        let sink = DocumentSink::new(
            DocumentConfig::local("default-target"),
            "default-target".into(),
        );
        let mut record = Record::with_id("doc-1", "elasticsearch_cdc", json!({}));
        record.add_metadata("_op", "delete");
        record.add_metadata("_target_index", "events-2026");
        record.add_metadata("_routing", "tenant-42");
        let body = sink.build_bulk_body(&RecordBatch::from(vec![record]));
        assert_eq!(body.lines().count(), 1);
        assert!(body.contains(r#""delete""#));
        assert!(body.contains(r#""_index":"events-2026""#));
        assert!(body.contains(r#""routing":"tenant-42""#));
    }

    #[test]
    fn compensatable_bulk_writes_use_create_and_embed_generation_marker() {
        let sink = DocumentSink::new(DocumentConfig::local("target"), "target".into());
        let mut record = Record::with_id("doc-1", "test", json!({"value": 1}));
        record.add_metadata("_dataspool_compensation_migration_id", "migration-a");
        record.add_metadata("_dataspool_compensation_generation", "7");
        let body = sink.build_bulk_body(&RecordBatch::from(vec![record]));
        let mut lines = body.lines();
        let action: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        let source: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert!(action.get("create").is_some());
        assert_eq!(source["_dataspool"]["migration_id"], "migration-a");
        assert_eq!(source["_dataspool"]["generation"], 7);
        assert_eq!(
            source["_dataspool"]["payload_digest"],
            format!(
                "{:x}",
                sha2::Sha256::digest(json!({"value": 1}).to_string().as_bytes())
            )
        );
        assert_eq!(source["value"], 1);
    }

    #[test]
    fn compensation_identity_rejects_unsafe_ids_and_zero_generation() {
        assert!(validate_compensation_identity("migration-1", 0).is_ok());
        assert!(validate_compensation_identity("migration-1", 1).is_ok());
        assert!(validate_compensation_identity("../index", 1).is_err());
        assert!(validate_compensation_identity("", 1).is_err());
    }

    #[test]
    fn test_build_bulk_body_includes_external_fencing_version() {
        let sink = DocumentSink::new(DocumentConfig::local("test"), "target".to_string());
        let mut record = Record::with_id("doc-fenced", "test", json!({"value": 1}));
        record.add_metadata("_dataspool_fencing_token", "4611686018428387904");
        let body = sink.build_bulk_body(&RecordBatch::from(vec![record]));
        assert!(body.contains("\"version\":4611686018428387904"));
        assert!(body.contains("\"version_type\":\"external_gte\""));
    }

    #[test]
    fn test_parse_bulk_response() {
        let config = DocumentConfig::local("test");
        let sink = DocumentSink::new(config, "target".to_string());

        let response = json!({
            "took": 10,
            "errors": true,
            "items": [
                {
                    "index": {
                        "_index": "target",
                        "_id": "1",
                        "status": 201
                    }
                },
                {
                    "index": {
                        "_index": "target",
                        "_id": "2",
                        "status": 400,
                        "error": {
                            "type": "mapper_parsing_exception",
                            "reason": "failed to parse field"
                        }
                    }
                }
            ]
        });

        let result = sink.parse_bulk_response(&response, 2);

        assert_eq!(result.success_count, 1);
        assert_eq!(result.failed_count, 1);
        assert_eq!(result.errors.len(), 1);
        assert!(result.errors[0].contains("mapper_parsing_exception"));
    }
}
