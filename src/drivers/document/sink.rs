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
}

#[derive(Debug, Clone, Default)]
pub struct ClusterNodePressure {
    pub nodes: usize,
    pub write_active: u64,
    pub write_queue: u64,
    pub write_rejected: u64,
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
        let mut body = String::new();

        for record in batch.records() {
            // CDC records may target a concrete index and may represent deletes.
            let target_index = record
                .get_metadata("_target_index")
                .unwrap_or(&self.target_index);
            let operation = record.get_metadata("_op").unwrap_or("index");
            let mut action_metadata = serde_json::Map::new();
            action_metadata.insert("_index".into(), json!(target_index));
            if let Some(id) = &record.id {
                action_metadata.insert("_id".into(), json!(id));
            }
            let mut action = serde_json::Map::new();
            action.insert(operation.to_string(), Value::Object(action_metadata));
            let action = Value::Object(action);
            body.push_str(&action.to_string());
            body.push('\n');

            // Delete actions contain no source line in Bulk NDJSON.
            if operation == "delete" {
                continue;
            }
            body.push_str(&record.payload.to_string());
            body.push('\n');
        }

        body
    }

    /// Last observed write-thread-pool pressure across cluster nodes.
    pub fn node_pressure(&self) -> ClusterNodePressure {
        self.node_pressure.clone()
    }

    async fn refresh_node_pressure(&mut self, client: &Client) {
        let response = client
            .get(format!("{}/_nodes/stats/thread_pool", self.base_url))
            .send()
            .await;
        let Ok(response) = response else {
            return;
        };
        if !response.status().is_success() {
            return;
        }
        let Ok(body) = response.json::<Value>().await else {
            return;
        };
        let mut pressure = ClusterNodePressure::default();
        if let Some(nodes) = body["nodes"].as_object() {
            pressure.nodes = nodes.len();
            for (node_id, node) in nodes {
                let pool = node["thread_pool"]["write"]
                    .as_object()
                    .or_else(|| node["thread_pool"]["bulk"].as_object());
                if let Some(pool) = pool {
                    let item = NodeWritePressure {
                        node_id: node_id.clone(),
                        name: node["name"].as_str().unwrap_or_default().to_string(),
                        write_active: pool.get("active").and_then(Value::as_u64).unwrap_or(0),
                        write_queue: pool.get("queue").and_then(Value::as_u64).unwrap_or(0),
                        write_rejected: pool.get("rejected").and_then(Value::as_u64).unwrap_or(0),
                    };
                    pressure.write_active += item.write_active;
                    pressure.write_queue += item.write_queue;
                    pressure.write_rejected += item.write_rejected;
                    pressure.per_node.push(item);
                }
            }
        }
        pressure.queue_utilization = pressure.write_queue as f64
            / (pressure.write_queue + pressure.write_active).max(1) as f64;
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
            let mut action = serde_json::Map::new();
            action.insert(operation.to_string(), Value::Object(meta));
            body.push_str(&Value::Object(action).to_string());
            body.push('\n');
            if operation != "delete" {
                body.push_str(&record.payload.to_string());
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
        const MAX_RETRIES: u32 = 5;
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

            if status >= 200 && status < 300 {
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
            rejected: 0,
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
                ..Default::default()
            });
            if !window.pause.is_zero() {
                tokio::time::sleep(window.pause).await;
            }
        }
        Ok(aggregate)
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
            for _ in 0..4 {
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
        let body = sink.build_bulk_body(&RecordBatch::from(vec![record]));
        assert_eq!(body.lines().count(), 1);
        assert!(body.contains(r#""delete""#));
        assert!(body.contains(r#""_index":"events-2026""#));
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
