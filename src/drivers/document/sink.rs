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
use crate::pipeline::{RecordBatch, Sink, WriteResult};
use async_trait::async_trait;
use reqwest::{Client, ClientBuilder};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

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
}

impl DocumentSink {
    /// Create new document store sink
    pub fn new(config: DocumentConfig, target_index: String) -> Self {
        Self {
            config,
            client: None,
            base_url: String::new(),
            target_index,
        }
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
            .ok_or_else(|| {
                DriverError::ConnectionFailed("Not connected to document store".to_string())
            })?
            .clone();

        if batch.is_empty() {
            return Ok(WriteResult::success(0, 0));
        }

        const MAX_RETRIES: u32 = 5;
        let mut pending = batch.into_records();
        let mut result = WriteResult::default();
        let mut attempt = 0_u32;
        let url = format!("{}/_bulk", self.base_url);

        while !pending.is_empty() {
            let request_batch = RecordBatch::from(pending.clone());
            let body = self.build_bulk_body(&request_batch);
            result.bytes_written += body.len() as u64;

            let response = client
                .post(&url)
                .header("Content-Type", "application/x-ndjson")
                .body(body)
                .send()
                .await
                .map_err(|e| {
                    DriverError::DataError(format!(
                        "Bulk request failed; durable cache must retain batch for replay: {}",
                        e
                    ))
                })?;

            let status = response.status();
            if !status.is_success() {
                if matches!(status.as_u16(), 429 | 502 | 503 | 504) && attempt < MAX_RETRIES {
                    let delay = std::time::Duration::from_millis(
                        100_u64.saturating_mul(1 << attempt.min(4)),
                    );
                    warn!(
                        http_status = status.as_u16(),
                        retry = attempt + 1,
                        ?delay,
                        "Retrying transient Elasticsearch bulk request"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                    continue;
                }
                result.failed_count += pending.len();
                if matches!(status.as_u16(), 429 | 502 | 503 | 504) {
                    result.rejected_count += pending.len();
                }
                result.add_error(format!(
                    "Bulk API failed after {} attempts: HTTP {}",
                    attempt + 1,
                    status
                ));
                break;
            }

            let response_body: Value = response.json().await.map_err(|e| {
                DriverError::DataError(format!(
                    "Failed to parse bulk response; cache must retain batch: {}",
                    e
                ))
            })?;
            let items = response_body["items"].as_array().ok_or_else(|| {
                DriverError::DataError(
                    "Bulk response has no items array; cache must retain batch".into(),
                )
            })?;

            if items.len() != pending.len() {
                return Err(DriverError::DataError(format!(
                    "Bulk response item count mismatch: sent {}, received {}; cache must retain batch",
                    pending.len(), items.len()
                )).into());
            }

            let mut retry = Vec::new();
            for (record, item) in pending.into_iter().zip(items) {
                let operation = item.as_object().and_then(|m| m.values().next());
                let item_status =
                    operation.and_then(|v| v["status"].as_u64()).unwrap_or(500) as u16;
                if (200..300).contains(&item_status) {
                    result.success_count += 1;
                    continue;
                }

                let error_type = operation
                    .and_then(|v| v["error"]["type"].as_str())
                    .unwrap_or("unknown");
                let reason = operation
                    .and_then(|v| v["error"]["reason"].as_str())
                    .unwrap_or("no reason supplied");
                let transient = is_retryable_bulk_failure(item_status, error_type);

                if transient && attempt < MAX_RETRIES {
                    retry.push(record);
                } else {
                    result.failed_count += 1;
                    if transient {
                        result.rejected_count += 1;
                    }
                    result.add_error(format!("HTTP {} {}: {}", item_status, error_type, reason));
                }
            }

            pending = retry;
            if !pending.is_empty() {
                let delay =
                    std::time::Duration::from_millis(100_u64.saturating_mul(1 << attempt.min(4)));
                warn!(
                    retry_documents = pending.len(),
                    retry = attempt + 1,
                    ?delay,
                    "Retrying only rejected Elasticsearch bulk items"
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }

        if result.failed_count > 0 {
            warn!(
                success = result.success_count,
                failed = result.failed_count,
                "Elasticsearch bulk completed with permanent item failures"
            );
        } else {
            debug!(
                success = result.success_count,
                "Elasticsearch bulk write succeeded"
            );
        }
        Ok(result)
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
