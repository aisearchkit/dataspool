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

//! Document store source implementation

use super::config::DocumentConfig;
use crate::error::{DatspoolError, DriverError, Result};
use crate::pipeline::{OffsetValue, Record, RecordBatch, Source};
use async_trait::async_trait;
use reqwest::{Client, ClientBuilder};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

/// Document store API mode
#[derive(Debug, Clone, Copy, PartialEq)]
enum ApiMode {
    /// Scroll API
    Scroll,
    /// Point-in-Time API
    PointInTime,
}

/// Internal scroll state
#[derive(Debug, Clone)]
struct ScrollState {
    scroll_id: String,
    docs_read: u64,
}

/// Internal PIT state
#[derive(Debug, Clone)]
struct PitState {
    pit_id: String,
    docs_read: u64,
    search_after: Option<Vec<Value>>,
}

/// Document store data source
///
/// Reads documents from HTTP-based document stores using REST API.
pub struct DocumentSource {
    config: DocumentConfig,
    client: Option<Client>,
    base_url: String,
    api_mode: ApiMode,
    scroll_state: Option<ScrollState>,
    pit_state: Option<PitState>,
    total_docs: Option<u64>,
}

impl DocumentSource {
    /// Create new document store source
    pub fn new(config: DocumentConfig) -> Self {
        Self {
            config,
            client: None,
            base_url: String::new(),
            api_mode: ApiMode::Scroll,
            scroll_state: None,
            pit_state: None,
            total_docs: None,
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

    /// Detect version and select API mode
    async fn detect_api_mode(&mut self, client: &Client, base_url: &str) -> Result<ApiMode> {
        if self.config.force_scroll {
            info!("Force using Scroll API (configured)");
            return Ok(ApiMode::Scroll);
        }

        // Get cluster info
        let url = format!("{}/", base_url);
        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|e| DriverError::ConnectionFailed(format!("Failed to connect: {}", e)))?;

        if !response.status().is_success() {
            return Err(DriverError::ConnectionFailed(format!(
                "Failed to get cluster info: HTTP {}",
                response.status()
            ))
            .into());
        }

        let body: Value = response
            .json()
            .await
            .map_err(|e| DriverError::DataError(format!("Failed to parse cluster info: {}", e)))?;

        let version_str = body["version"]["number"]
            .as_str()
            .ok_or_else(|| DriverError::DataError("Cannot detect version".to_string()))?;

        info!("Detected version: {}", version_str);

        // Parse version (e.g., "7.17.0" -> (7, 17))
        let version_parts: Vec<u32> = version_str
            .split('.')
            .filter_map(|s| s.parse().ok())
            .collect();

        let (major, minor) = match version_parts.as_slice() {
            [maj, min, ..] => (*maj, *min),
            _ => {
                warn!("Cannot parse version {}, using Scroll API", version_str);
                return Ok(ApiMode::Scroll);
            }
        };

        // Stable PIT pagination via _shard_doc requires Elasticsearch 7.12+.
        // Elasticsearch 7.10/7.11 supports PIT but lacks this tie-breaker, so use Scroll.
        let mode = if major >= 8 || (major == 7 && minor >= 12) || major >= 2 {
            info!("Using Point-in-Time API (version {})", version_str);
            ApiMode::PointInTime
        } else {
            info!("Using Scroll API (version {})", version_str);
            ApiMode::Scroll
        };

        Ok(mode)
    }

    /// Initialize scroll search
    async fn init_scroll(&mut self, client: &Client) -> Result<ScrollState> {
        let scroll_timeout = format!("{}m", self.config.scroll_timeout_mins);

        let mut search_body = json!({
            "size": self.config.batch_size,
            "sort": ["_doc"]
        });

        if let Some(ref query) = self.config.query {
            search_body["query"] = query.clone();
        }

        debug!(
            "Initializing scroll: index={}, size={}",
            self.config.index, self.config.batch_size
        );

        let url = format!(
            "{}/{}/_search?scroll={}",
            self.base_url, self.config.index, scroll_timeout
        );

        let response = client
            .post(&url)
            .json(&search_body)
            .send()
            .await
            .map_err(|e| DriverError::DataError(format!("Failed to init scroll: {}", e)))?;

        if !response.status().is_success() {
            return Err(DriverError::DataError(format!(
                "Scroll init failed: HTTP {}",
                response.status()
            ))
            .into());
        }

        let body: Value = response.json().await.map_err(|e| {
            DriverError::DataError(format!("Failed to parse scroll response: {}", e))
        })?;

        let scroll_id = body["_scroll_id"]
            .as_str()
            .ok_or_else(|| DriverError::DataError("No _scroll_id in response".to_string()))?
            .to_string();

        // Extract total count
        let total = body["hits"]["total"]["value"]
            .as_u64()
            .or_else(|| body["hits"]["total"].as_u64());

        if let Some(total) = total {
            info!("Total documents in index: {}", total);
            self.total_docs = Some(total);
        }

        Ok(ScrollState {
            scroll_id,
            docs_read: 0,
        })
    }

    /// Continue scroll pagination
    async fn continue_scroll(&self, client: &Client, scroll_id: &str) -> Result<Value> {
        let scroll_timeout = format!("{}m", self.config.scroll_timeout_mins);

        let url = format!("{}/_search/scroll", self.base_url);

        let response = client
            .post(&url)
            .json(&json!({
                "scroll": scroll_timeout,
                "scroll_id": scroll_id
            }))
            .send()
            .await
            .map_err(|e| DriverError::DataError(format!("Failed to continue scroll: {}", e)))?;

        if !response.status().is_success() {
            return Err(DriverError::DataError(format!(
                "Scroll continuation failed: HTTP {}",
                response.status()
            ))
            .into());
        }

        response.json().await.map_err(|e| {
            DriverError::DataError(format!("Failed to parse scroll response: {}", e)).into()
        })
    }

    /// Clear scroll context
    async fn clear_scroll(&self, client: &Client, scroll_id: &str) -> Result<()> {
        let url = format!("{}/_search/scroll", self.base_url);

        let _ = client
            .delete(&url)
            .json(&json!({ "scroll_id": scroll_id }))
            .send()
            .await;

        debug!("Cleared scroll context");
        Ok(())
    }

    /// Initialize PIT
    async fn init_pit(&mut self, client: &Client) -> Result<PitState> {
        let keep_alive = format!("{}m", self.config.scroll_timeout_mins);

        debug!("Initializing PIT: index={}", self.config.index);

        // Open PIT
        let url = format!(
            "{}/{}/_pit?keep_alive={}",
            self.base_url, self.config.index, keep_alive
        );

        let response = client
            .post(&url)
            .send()
            .await
            .map_err(|e| DriverError::DataError(format!("Failed to open PIT: {}", e)))?;

        if !response.status().is_success() {
            return Err(DriverError::DataError(format!(
                "PIT open failed: HTTP {}",
                response.status()
            ))
            .into());
        }

        let body: Value = response
            .json()
            .await
            .map_err(|e| DriverError::DataError(format!("Failed to parse PIT response: {}", e)))?;

        let pit_id = body["id"]
            .as_str()
            .ok_or_else(|| DriverError::DataError("No PIT id in response".to_string()))?
            .to_string();

        info!("Opened PIT: {}", pit_id);

        // First search to get total count
        let mut search_body = json!({
            "size": self.config.batch_size,
            // _shard_doc is stable and unique within a PIT snapshot (ES 7.12+).
            "sort": [{"_shard_doc": "asc"}],
            "pit": {
                "id": &pit_id,
                "keep_alive": &keep_alive
            },
            "track_total_hits": true
        });

        if let Some(ref query) = self.config.query {
            search_body["query"] = query.clone();
        }

        let search_url = format!("{}/_search", self.base_url);

        let response = client
            .post(&search_url)
            .json(&search_body)
            .send()
            .await
            .map_err(|e| DriverError::DataError(format!("Failed to search with PIT: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(DriverError::DataError(format!(
                "PIT search failed: HTTP {status}: {body}"
            ))
            .into());
        }

        let body: Value = response.json::<Value>().await.map_err(|e| {
            DatspoolError::Driver(DriverError::DataError(format!(
                "Failed to parse search response: {}",
                e
            )))
        })?;
        let pit_id = body["pit_id"].as_str().unwrap_or(&pit_id).to_string();

        // Extract total count
        let total = body["hits"]["total"]["value"]
            .as_u64()
            .or_else(|| body["hits"]["total"].as_u64());

        if let Some(total) = total {
            info!("Total documents in index: {}", total);
            self.total_docs = Some(total);
        }

        Ok(PitState {
            pit_id,
            docs_read: 0,
            search_after: None,
        })
    }

    /// Search with PIT and search_after
    async fn search_pit(
        &self,
        client: &Client,
        pit_id: &str,
        search_after: Option<&Vec<Value>>,
    ) -> Result<Value> {
        let keep_alive = format!("{}m", self.config.scroll_timeout_mins);

        let mut search_body = json!({
            "size": self.config.batch_size,
            "sort": [{"_shard_doc": "asc"}],
            "pit": {
                "id": pit_id,
                "keep_alive": &keep_alive
            }
        });

        if let Some(ref query) = self.config.query {
            search_body["query"] = query.clone();
        }

        if let Some(after) = search_after {
            search_body["search_after"] = json!(after);
        }

        let url = format!("{}/_search", self.base_url);

        let response = client
            .post(&url)
            .json(&search_body)
            .send()
            .await
            .map_err(|e| DriverError::DataError(format!("Failed to search with PIT: {}", e)))?;

        if !response.status().is_success() {
            return Err(DriverError::DataError(format!(
                "PIT search failed: HTTP {}",
                response.status()
            ))
            .into());
        }

        response.json().await.map_err(|e| {
            DriverError::DataError(format!("Failed to parse search response: {}", e)).into()
        })
    }

    /// Close PIT
    async fn close_pit(&self, client: &Client, pit_id: &str) -> Result<()> {
        let url = format!("{}/_pit", self.base_url);

        let _ = client
            .delete(&url)
            .json(&json!({ "id": pit_id }))
            .send()
            .await;

        debug!("Closed PIT");
        Ok(())
    }

    /// Parse hits into RecordBatch
    fn parse_hits(&self, hits: &Value) -> Result<RecordBatch> {
        let hits_array = hits["hits"]
            .as_array()
            .ok_or_else(|| DriverError::DataError("No hits array in response".to_string()))?;

        let mut batch = RecordBatch::with_capacity(hits_array.len());

        for hit in hits_array {
            let id = hit["_id"].as_str().map(|s| s.to_string());
            let source = hit["_source"].clone();

            if source.is_null() {
                warn!("Document has no _source, skipping");
                continue;
            }

            let mut record = if let Some(id) = id {
                Record::with_id(id, "document", source)
            } else {
                Record::new("document", source)
            };

            // Add metadata
            if let Some(index) = hit["_index"].as_str() {
                record.add_metadata("_index", index);
            }
            if let Some(doc_type) = hit["_type"].as_str() {
                record.add_metadata("_type", doc_type);
            }

            batch.push(record);
        }

        Ok(batch)
    }
}

impl DocumentSource {
    /// Read batch using PIT API
    async fn read_batch_pit(
        &mut self,
        client: &Client,
        offset: &OffsetValue,
    ) -> Result<(RecordBatch, OffsetValue)> {
        // Restore the live PIT cursor from the durable shard checkpoint after restart.
        if self.pit_state.is_none() {
            let restored = match offset {
                OffsetValue::Custom(value)
                    if value["driver"].as_str() == Some("elasticsearch_pit") =>
                {
                    let pit_id = value["pit_id"]
                        .as_str()
                        .ok_or_else(|| {
                            DriverError::DataError("PIT checkpoint is missing pit_id".into())
                        })?
                        .to_string();
                    let search_after = value["search_after"].as_array().cloned();
                    let docs_read = value["docs_read"].as_u64().unwrap_or(0);
                    Some(PitState {
                        pit_id,
                        docs_read,
                        search_after,
                    })
                }
                _ => None,
            };
            self.pit_state = Some(match restored {
                Some(state) => {
                    info!(
                        docs_read = state.docs_read,
                        "Restored Elasticsearch PIT checkpoint"
                    );
                    state
                }
                None => self.init_pit(client).await?,
            });
        }

        let (pit_id, search_after, docs_read) = {
            let state = self
                .pit_state
                .as_ref()
                .ok_or_else(|| DriverError::DataError("No PIT state".to_string()))?;
            (
                state.pit_id.clone(),
                state.search_after.clone(),
                state.docs_read,
            )
        };

        // Search with PIT. Elasticsearch may rotate the PIT id on each response.
        let body = match self
            .search_pit(client, &pit_id, search_after.as_ref())
            .await
        {
            Ok(body) => body,
            Err(error) => {
                let message = error.to_string();
                let expired = message.contains("PIT search failed: HTTP 404")
                    || message.contains("search_context_missing_exception")
                    || message.contains("No search context found");
                if !expired {
                    return Err(error);
                }
                // PIT IDs are server-side leases, not durable bookmarks. If a process
                // restart outlives keep_alive, open a fresh snapshot and restart from
                // its beginning. Stable destination IDs make this replay idempotent.
                warn!(
                    "Saved PIT expired; reopening snapshot from the beginning: {}",
                    message
                );
                self.pit_state = Some(self.init_pit(client).await?);
                let state = self.pit_state.as_ref().ok_or_else(|| {
                    DriverError::DataError("PIT state missing after reopen".into())
                })?;
                self.search_pit(client, &state.pit_id, None).await?
            }
        };
        if let Some(rotated_id) = body["pit_id"].as_str()
            && let Some(state) = self.pit_state.as_mut()
        {
            state.pit_id = rotated_id.to_string();
        }

        // Parse hits
        let batch = self.parse_hits(&body["hits"])?;
        let batch_size = batch.len();

        // Extract search_after from last hit
        let search_after = if batch_size > 0 {
            body["hits"]["hits"]
                .as_array()
                .and_then(|hits| hits.last())
                .and_then(|hit| hit["sort"].as_array())
                .map(|sort| sort.to_vec())
        } else {
            None
        };

        // Count raw hits, including documents skipped because _source is disabled.
        let hits_read = body["hits"]["hits"].as_array().map_or(0, Vec::len) as u64;
        let new_docs_read = docs_read + hits_read;

        // Update PIT state and emit a restart-safe opaque cursor.
        let state = self
            .pit_state
            .as_mut()
            .ok_or_else(|| DriverError::DataError("No PIT state".to_string()))?;
        state.docs_read = new_docs_read;
        state.search_after = search_after;
        let new_offset = OffsetValue::Custom(json!({
            "driver": "elasticsearch_pit",
            "pit_id": state.pit_id,
            "docs_read": new_docs_read,
            "search_after": state.search_after,
        }));

        debug!(
            "Read batch (PIT): {} docs, total: {}",
            batch_size, new_docs_read
        );

        Ok((batch, new_offset))
    }

    /// Read batch using Scroll API
    async fn read_batch_scroll(&mut self, client: &Client) -> Result<(RecordBatch, OffsetValue)> {
        // Initialize scroll if first read
        if self.scroll_state.is_none() {
            let state = self.init_scroll(client).await?;
            self.scroll_state = Some(state);
        }

        let scroll_state = self
            .scroll_state
            .as_ref()
            .ok_or_else(|| DriverError::DataError("No scroll state".to_string()))?;

        // Continue scroll
        let body = self
            .continue_scroll(client, &scroll_state.scroll_id)
            .await?;

        // Parse hits
        let batch = self.parse_hits(&body["hits"])?;
        let batch_size = batch.len();

        // Update offset
        let new_docs_read = scroll_state.docs_read + batch_size as u64;
        let new_offset = OffsetValue::DocumentCount(new_docs_read);

        // Update scroll state
        if let Some(ref mut state) = self.scroll_state {
            state.docs_read = new_docs_read;
        }

        debug!(
            "Read batch (Scroll): {} docs, total: {}",
            batch_size, new_docs_read
        );

        Ok((batch, new_offset))
    }
}

#[async_trait]
impl Source for DocumentSource {
    async fn connect(&mut self) -> Result<()> {
        info!("Connecting to document store: {:?}", self.config.urls);

        let urls = self.config.parse_urls()?;
        if urls.is_empty() {
            return Err(
                DriverError::DataError("No document store URLs provided".to_string()).into(),
            );
        }

        self.base_url = urls[0].as_str().trim_end_matches('/').to_string();

        let client = self.build_client()?;

        // Detect API mode
        let base_url = self.base_url.clone();
        self.api_mode = self.detect_api_mode(&client, &base_url).await?;

        self.client = Some(client);

        Ok(())
    }

    async fn read_batch(
        &mut self,
        offset: &OffsetValue,
        _batch_size: usize,
    ) -> Result<(RecordBatch, OffsetValue)> {
        self.config.batch_size = _batch_size.max(1);
        let client = self.client.clone().ok_or_else(|| {
            DriverError::ConnectionFailed("Not connected to document store".to_string())
        })?;

        match self.api_mode {
            ApiMode::PointInTime => self.read_batch_pit(&client, offset).await,
            ApiMode::Scroll => self.read_batch_scroll(&client).await,
        }
    }

    async fn estimate_count(&self) -> Result<Option<u64>> {
        Ok(self.total_docs)
    }

    async fn close(&mut self) -> Result<()> {
        // Clean up scroll or PIT
        if let Some(client) = &self.client {
            if let Some(ref state) = self.scroll_state {
                self.clear_scroll(client, &state.scroll_id).await?;
            }
            if let Some(ref state) = self.pit_state {
                self.close_pit(client, &state.pit_id).await?;
            }
        }

        self.client = None;
        self.scroll_state = None;
        self.pit_state = None;

        info!("Document store source closed");
        Ok(())
    }

    fn source_type(&self) -> &str {
        "document"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_source() {
        let config = DocumentConfig::local("test-index");
        let source = DocumentSource::new(config);

        assert_eq!(source.source_type(), "document");
        assert!(source.client.is_none());
    }

    #[test]
    fn test_parse_hits() {
        let config = DocumentConfig::local("test");
        let source = DocumentSource::new(config);

        let hits_response = json!({
            "hits": [
                {
                    "_id": "doc1",
                    "_index": "test-index",
                    "_type": "_doc",
                    "_source": {
                        "field1": "value1",
                        "field2": 42
                    }
                },
                {
                    "_id": "doc2",
                    "_index": "test-index",
                    "_type": "_doc",
                    "_source": {
                        "field1": "value2",
                        "field2": 100
                    }
                }
            ]
        });

        let batch = source.parse_hits(&hits_response).unwrap();
        assert_eq!(batch.len(), 2);

        let records = batch.into_records();
        assert_eq!(records[0].id.as_ref().unwrap(), "doc1");
        assert_eq!(records[0].get_metadata("_index"), Some("test-index"));
        assert_eq!(records[1].id.as_ref().unwrap(), "doc2");
    }
}
