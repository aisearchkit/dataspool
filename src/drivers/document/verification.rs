// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 Dataspool Contributors

//! Independent, streaming source/target verification for document migrations.

use super::{DocumentConfig, DocumentSource};
use crate::error::Result;
use crate::pipeline::{OffsetValue, Source};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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

async fn verify_index(config: &DocumentConfig, page_size: usize) -> Result<IndexVerification> {
    let mut source = DocumentSource::new(config.clone());
    source.connect().await?;
    let mut offset = OffsetValue::initial();
    let mut count = 0_u64;
    let mut ids = DigestAccumulator::default();
    let mut contents = DigestAccumulator::default();

    let scan = async {
        loop {
            let (batch, next_offset) = source
                .read_batch(&offset, page_size.clamp(1, 10_000))
                .await?;
            if batch.is_empty() {
                break;
            }
            for record in batch.into_records() {
                let id = record.id.ok_or_else(|| {
                    crate::error::DriverError::DataError(
                        "Verification source returned a document without _id".into(),
                    )
                })?;
                let mut payload = record.payload;
                let object = payload.as_object_mut().ok_or_else(|| {
                    crate::error::DriverError::DataError(format!(
                        "Document {id} has a non-object _source"
                    ))
                })?;
                object.remove("_dataspool");
                ids.push(id.as_bytes());
                let canonical = serde_json::to_vec(&payload).map_err(|error| {
                    crate::error::DriverError::DataError(format!(
                        "Unable to canonicalize document {id}: {error}"
                    ))
                })?;
                contents.push(&canonical);
                count = count.saturating_add(1);
            }
            offset = next_offset;
        }
        Ok::<(), crate::error::DatspoolError>(())
    }
    .await;
    let close = source.close().await;
    scan?;
    close?;
    if let OffsetValue::Custom(cursor) = &offset
        && let Some(raw_count) = cursor["docs_read"].as_u64()
        && raw_count != count
    {
        return Err(crate::error::DriverError::DataError(format!(
            "Verification for {} read {count} documents with _source but the reader observed {raw_count} matching hits; refusing an incomplete content digest",
            config.index
        )).into());
    }

    Ok(IndexVerification {
        index: config.index.clone(),
        document_count: count,
        id_set_digest: ids.finish(),
        content_digest: contents.finish(),
    })
}

/// Independently scan both indexes and compare document count, ID set, and
/// canonical content. The reader selects Scroll or PIT/search_after by server version.
/// Sink-owned `_dataspool` metadata is excluded.
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
