// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 Dataspool Contributors

//! Independent, streaming source/target verification for document migrations.

use super::DocumentConfig;
use crate::error::{DriverError, Result};
use reqwest::{Client, ClientBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const PIT_KEEP_ALIVE: &str = "2m";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexVerification {
    pub index: String,
    pub document_count: u64,
    /// Order-independent SHA-256 aggregate over Elasticsearch document IDs.
    pub id_set_digest: String,
    /// Order-independent SHA-256 aggregate over canonical _source, excluding `_dataspool`.
    pub content_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerificationReport {
    pub source: IndexVerification,
    pub target: IndexVerification,
    pub count_matches: bool,
    pub id_set_matches: bool,
    pub content_matches: bool,
}

impl VerificationReport {
    pub fn is_consistent(&self) -> bool {
        self.count_matches && self.id_set_matches && self.content_matches
    }
}

#[derive(Default)]
struct DigestAccumulator {
    xor: [u8; 32],
    sum: [u8; 32],
}

impl DigestAccumulator {
    fn push(&mut self, value: &[u8]) {
        let digest = Sha256::digest(value);
        for (slot, byte) in self.xor.iter_mut().zip(digest.iter()) {
            *slot ^= byte;
        }
        let mut carry = 0_u16;
        for index in (0..32).rev() {
            let total = self.sum[index] as u16 + digest[index] as u16 + carry;
            self.sum[index] = total as u8;
            carry = total >> 8;
        }
    }

    fn finish(&self) -> String {
        let mut combined = [0_u8; 64];
        combined[..32].copy_from_slice(&self.xor);
        combined[32..].copy_from_slice(&self.sum);
        format!("{:x}", Sha256::digest(combined))
    }
}

fn client_for(config: &DocumentConfig) -> Result<Client> {
    let mut builder = ClientBuilder::new()
        .timeout(config.timeout())
        .danger_accept_invalid_certs(!config.verify_ssl);
    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        use base64::Engine;
        let token =
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Basic {token}"))
                .map_err(|error| DriverError::ConnectionFailed(error.to_string()))?,
        );
        builder = builder.default_headers(headers);
    } else if let Some(api_key) = &config.api_key {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("ApiKey {api_key}"))
                .map_err(|error| DriverError::ConnectionFailed(error.to_string()))?,
        );
        builder = builder.default_headers(headers);
    }
    builder
        .build()
        .map_err(|error| DriverError::ConnectionFailed(error.to_string()).into())
}

async fn verify_index(config: &DocumentConfig, page_size: usize) -> Result<IndexVerification> {
    let base_url = config
        .parse_urls()?
        .into_iter()
        .next()
        .ok_or_else(|| DriverError::DataError("No document verification URL configured".into()))?
        .to_string()
        .trim_end_matches('/')
        .to_string();
    let client = client_for(config)?;
    let open = client
        .post(format!(
            "{base_url}/{}/_pit?keep_alive={PIT_KEEP_ALIVE}",
            config.index
        ))
        .send()
        .await
        .map_err(|error| {
            DriverError::DataError(format!("Failed to open verification PIT: {error}"))
        })?;
    if !open.status().is_success() {
        return Err(DriverError::DataError(format!(
            "Failed to open verification PIT for {}: HTTP {}",
            config.index,
            open.status()
        ))
        .into());
    }
    let opened: Value = open.json().await.map_err(|error| {
        DriverError::DataError(format!("Invalid verification PIT response: {error}"))
    })?;
    let mut pit_id = opened["id"]
        .as_str()
        .ok_or_else(|| DriverError::DataError("PIT response is missing id".into()))?
        .to_string();
    let mut search_after: Option<Value> = None;
    let mut count = 0_u64;
    let mut ids = DigestAccumulator::default();
    let mut contents = DigestAccumulator::default();

    let scan = async {
        loop {
            let mut request = json!({
                "size": page_size.clamp(1, 10_000),
                "track_total_hits": false,
                "pit": {"id": pit_id, "keep_alive": PIT_KEEP_ALIVE},
                "sort": ["_shard_doc"],
                "query": config.query.clone().unwrap_or_else(|| json!({"match_all": {}})),
                "_source": true
            });
            if let Some(cursor) = search_after.take() {
                request["search_after"] = cursor;
            }
            let response = client
                .post(format!("{base_url}/_search"))
                .json(&request)
                .send()
                .await
                .map_err(|error| {
                    DriverError::DataError(format!(
                        "Verification search failed for {}: {error}",
                        config.index
                    ))
                })?;
            if !response.status().is_success() {
                return Err(DriverError::DataError(format!(
                    "Verification search failed for {}: HTTP {}",
                    config.index,
                    response.status()
                ))
                .into());
            }
            let body: Value = response.json().await.map_err(|error| {
                DriverError::DataError(format!("Invalid verification search response: {error}"))
            })?;
            if let Some(next_pit) = body["pit_id"].as_str() {
                pit_id = next_pit.to_string();
            }
            let hits = body["hits"]["hits"].as_array().ok_or_else(|| {
                DriverError::DataError("Verification response is missing hits.hits".into())
            })?;
            if hits.is_empty() {
                break;
            }
            for hit in hits {
                let id = hit["_id"].as_str().ok_or_else(|| {
                    DriverError::DataError("Verification hit is missing _id".into())
                })?;
                let mut source = hit["_source"].clone();
                let object = source.as_object_mut().ok_or_else(|| {
                    DriverError::DataError(format!("Document {id} has a non-object _source"))
                })?;
                object.remove("_dataspool");
                ids.push(id.as_bytes());
                let canonical = serde_json::to_vec(&source).map_err(|error| {
                    DriverError::DataError(format!("Unable to canonicalize document {id}: {error}"))
                })?;
                contents.push(&canonical);
                count = count.saturating_add(1);
                search_after = hit["sort"]
                    .as_array()
                    .map(|values| Value::Array(values.clone()));
            }
            if search_after.is_none() {
                return Err(DriverError::DataError(
                    "Verification hit is missing search_after sort values".into(),
                )
                .into());
            }
        }
        Ok::<(), crate::error::DatspoolError>(())
    }
    .await;

    // PIT cleanup is attempted even when scanning fails; preserve the primary scan error.
    let close = client
        .delete(format!("{base_url}/_pit"))
        .json(&json!({"id": pit_id}))
        .send()
        .await;
    scan?;
    if let Ok(response) = close
        && !response.status().is_success()
    {
        tracing::warn!(index = %config.index, status = %response.status(), "Unable to close verification PIT");
    }

    Ok(IndexVerification {
        index: config.index.clone(),
        document_count: count,
        id_set_digest: ids.finish(),
        content_digest: contents.finish(),
    })
}

/// Independently scan both indexes through PIT and compare document count, ID set,
/// and canonical document content. Sink-owned `_dataspool` metadata is excluded.
pub async fn verify_source_target(
    source: &DocumentConfig,
    target: &DocumentConfig,
    page_size: usize,
) -> Result<VerificationReport> {
    let source_result = verify_index(source, page_size).await?;
    let target_result = verify_index(target, page_size).await?;
    Ok(VerificationReport {
        count_matches: source_result.document_count == target_result.document_count,
        id_set_matches: source_result.id_set_digest == target_result.id_set_digest,
        content_matches: source_result.content_digest == target_result.content_digest,
        source: source_result,
        target: target_result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_aggregation_is_order_independent() {
        let mut first = DigestAccumulator::default();
        first.push(b"alpha");
        first.push(b"beta");
        let mut second = DigestAccumulator::default();
        second.push(b"beta");
        second.push(b"alpha");
        assert_eq!(first.finish(), second.finish());
    }

    #[test]
    fn empty_digest_is_stable() {
        assert_eq!(
            DigestAccumulator::default().finish(),
            DigestAccumulator::default().finish()
        );
    }
}
