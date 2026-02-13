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
use crate::error::{Error, Result};
use crate::pipeline::{RecordBatch, Sink, WriteResult};
use async_trait::async_trait;
use reqwest::{Client, ClientBuilder};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

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

        // Basic auth
        if let (Some(ref username), Some(ref password)) =
            (&self.config.username, &self.config.password)
        {
            builder = builder.basic_auth(username, Some(password));
        }

        builder
            .build()
            .map_err(|e| Error::Connection(format!("Failed to build HTTP client: {}", e)))
    }

    /// Build bulk request body in NDJSON format
    fn build_bulk_body(&self, batch: &RecordBatch) -> String {
        let mut body = String::new();

        for record in batch.records() {
            // Action line
            let action = if let Some(ref id) = record.id {
                json!({
                    "index": {
                        "_index": &self.target_index,
                        "_id": id
                    }
                })
            } else {
                json!({
                    "index": {
                        "_index": &self.target_index
                    }
                })
            };

            body.push_str(&action.to_string());
            body.push('\n');

            // Source line
            body.push_str(&record.payload.to_string());
            body.push('\n');
        }

        body
    }

    /// Parse bulk response and extract errors
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
                    let error_type = error.get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    let error_reason = error.get("reason")
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
            return Err(Error::Config("No document store URLs provided".to_string()));
        }

        self.base_url = urls[0].as_str().trim_end_matches('/').to_string();

        let client = self.build_client()?;

        // Verify connection
        let url = format!("{}/", self.base_url);
        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::Connection(format!("Failed to connect: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::Connection(format!(
                "Connection failed: HTTP {}",
                response.status()
            )));
        }

        self.client = Some(client);

        info!("Connected to document store, target index: {}", self.target_index);
        Ok(())
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> Result<WriteResult> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("Not connected to document store".to_string()))?;

        if batch.is_empty() {
            return Ok(WriteResult::success(0, 0));
        }

        let batch_size = batch.len();
        debug!("Writing batch: {} documents", batch_size);

        // Build bulk request body
        let body = self.build_bulk_body(&batch);
        let body_bytes = body.len() as u64;

        // Send bulk request
        let url = format!("{}/_bulk", self.base_url);

        let response = client
            .post(&url)
            .header("Content-Type", "application/x-ndjson")
            .body(body)
            .send()
            .await
            .map_err(|e| Error::DataSink(format!("Bulk request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::DataSink(format!(
                "Bulk API failed: HTTP {}",
                response.status()
            )));
        }

        let response_body: Value = response
            .json()
            .await
            .map_err(|e| Error::DataSink(format!("Failed to parse bulk response: {}", e)))?;

        // Parse response
        let mut result = self.parse_bulk_response(&response_body, batch_size);
        result.bytes_written = body_bytes;

        if result.failed_count > 0 {
            warn!(
                "Bulk write partial failure: {}/{} failed",
                result.failed_count, batch_size
            );
        } else {
            debug!("Bulk write success: {} documents", result.success_count);
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
        batch.push(Record::with_id(
            "doc1",
            "test",
            json!({"field": "value1"}),
        ));
        batch.push(Record::new("test", json!({"field": "value2"})));

        let body = sink.build_bulk_body(&batch);

        // Should have 4 lines: action, source, action, source
        assert_eq!(body.lines().count(), 4);
        assert!(body.contains(r#""_id":"doc1""#));
        assert!(body.contains(r#""field":"value1""#));
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
