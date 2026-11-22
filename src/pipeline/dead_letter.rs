// SPDX-License-Identifier: AGPL-3.0-or-later
//! Durable dead-letter storage for permanent document-level sink failures.

use super::record::RecordBatch;
use super::traits::{FailedRecord, Sink};
use crate::error::{DatspoolError, Result};
use crate::storage::kv::{KvStore, cf};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    pub id: String,
    pub record: super::record::Record,
    pub status: u16,
    pub error_type: String,
    pub reason: String,
    pub attempts: u32,
    pub created_at_ms: u64,
}

pub struct DeadLetterQueue<S: KvStore> {
    store: Arc<S>,
    prefix: Vec<u8>,
}

impl<S: KvStore> Clone for DeadLetterQueue<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            prefix: self.prefix.clone(),
        }
    }
}

impl<S: KvStore> DeadLetterQueue<S> {
    pub fn new(store: Arc<S>, namespace: &str) -> Self {
        let namespace: String = namespace
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || "-_".contains(c) {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        Self {
            store,
            prefix: format!("dlq/{namespace}/").into_bytes(),
        }
    }

    pub async fn enqueue(&self, failures: Vec<FailedRecord>) -> Result<usize> {
        if failures.is_empty() {
            return Ok(0);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mut batch = crate::storage::kv::WriteBatch::new();
        let count = failures.len();
        for failure in failures {
            // Stable identity makes DLQ persistence idempotent across a crash between
            // DLQ write, shard checkpoint update and durable cache acknowledgement.
            let identity = serde_json::json!({
                "record_id": failure.record.id,
                "source_type": failure.record.source_type,
                "payload": failure.record.payload,
                "target_index": failure.record.get_metadata("_target_index"),
                "status": failure.status,
                "error_type": failure.error_type,
            })
            .to_string();
            use sha2::{Digest, Sha256};
            let digest = Sha256::digest(identity.as_bytes());
            let id = digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let entry = DeadLetterEntry {
                id: id.clone(),
                record: failure.record,
                status: failure.status,
                error_type: failure.error_type,
                reason: failure.reason,
                attempts: 0,
                created_at_ms: now,
            };
            let mut key = self.prefix.clone();
            key.extend_from_slice(id.as_bytes());
            let value = serde_json::to_vec(&entry)
                .map_err(|e| DatspoolError::Internal(format!("serialize DLQ entry: {e}")))?;
            batch.put_cf(cf::DEAD_LETTER, key, value);
        }
        self.store.write_batch(batch).await?;
        Ok(count)
    }

    pub async fn list(&self, limit: usize) -> Result<Vec<DeadLetterEntry>> {
        self.list_page(0, limit).await
    }

    /// Read a stable page ordered by RocksDB key (the deterministic entry ID).
    pub async fn list_page(&self, offset: usize, limit: usize) -> Result<Vec<DeadLetterEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = self
            .store
            .prefix_scan_page_cf(cf::DEAD_LETTER, &self.prefix, offset, limit)
            .await?;
        rows.into_iter()
            .map(|(_, value)| {
                serde_json::from_slice(&value)
                    .map_err(|e| DatspoolError::Internal(format!("decode DLQ entry: {e}")))
            })
            .collect()
    }

    pub async fn get(&self, id: &str) -> Result<Option<DeadLetterEntry>> {
        let mut key = self.prefix.clone();
        key.extend_from_slice(id.as_bytes());
        self.store
            .get_cf(cf::DEAD_LETTER, &key)
            .await?
            .map(|value| {
                serde_json::from_slice(&value)
                    .map_err(|e| DatspoolError::Internal(format!("decode DLQ entry: {e}")))
            })
            .transpose()
    }

    pub async fn len(&self) -> Result<usize> {
        self.store
            .count_prefix_cf(cf::DEAD_LETTER, &self.prefix)
            .await
    }

    pub async fn remove(&self, id: &str) -> Result<()> {
        let mut key = self.prefix.clone();
        key.extend_from_slice(id.as_bytes());
        self.store.delete_cf(cf::DEAD_LETTER, &key).await
    }

    async fn update_entry(&self, entry: &DeadLetterEntry) -> Result<()> {
        let mut key = self.prefix.clone();
        key.extend_from_slice(entry.id.as_bytes());
        let value = serde_json::to_vec(entry)
            .map_err(|e| DatspoolError::Internal(format!("serialize DLQ entry: {e}")))?;
        self.store.put_cf(cf::DEAD_LETTER, &key, &value).await
    }

    /// Replay selected entries. Entries are removed only after destination success.
    pub async fn replay<SinkT: Sink + ?Sized>(
        &self,
        sink: &mut SinkT,
        ids: &[String],
    ) -> Result<usize> {
        if ids.is_empty() {
            return Err(DatspoolError::Configuration(
                "DLQ replay requires explicit entry IDs; replay-all is disabled".into(),
            ));
        }
        let mut selected = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(entry) = self.get(id).await? {
                selected.push(entry);
            }
        }
        let mut replayed = 0;
        for entry in selected {
            let result = sink
                .write_batch(RecordBatch::from(vec![entry.record.clone()]))
                .await?;
            if result.is_success() {
                self.remove(&entry.id).await?;
                replayed += 1;
            } else {
                let mut failed = entry;
                failed.attempts = failed.attempts.saturating_add(1);
                failed.reason = result.errors.join("; ");
                self.update_entry(&failed).await?;
            }
        }
        Ok(replayed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::rocksdb::RocksDbStore;
    use tempfile::tempdir;

    struct NoopSink;
    #[async_trait::async_trait]
    impl Sink for NoopSink {
        fn sink_type(&self) -> &str {
            "noop"
        }
        async fn connect(&mut self) -> Result<()> {
            Ok(())
        }
        async fn write_batch(
            &mut self,
            batch: RecordBatch,
        ) -> Result<super::super::traits::WriteResult> {
            Ok(super::super::traits::WriteResult {
                success_count: batch.len(),
                ..Default::default()
            })
        }
        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn persists_and_removes_dead_letters() {
        let dir = tempdir().unwrap();
        let db = Arc::new(RocksDbStore::open(dir.path()).unwrap());
        let dlq = DeadLetterQueue::new(db, "task/shard-1");
        let record = super::super::record::Record::with_id("42", "es", serde_json::json!({"v":1}));
        let count = dlq
            .enqueue(vec![FailedRecord {
                record,
                status: 400,
                error_type: "mapper_parsing_exception".into(),
                reason: "bad value".into(),
            }])
            .await
            .unwrap();
        assert_eq!(count, 1);
        let duplicate = super::super::traits::FailedRecord {
            record: super::super::record::Record::with_id("42", "es", serde_json::json!({"v":1})),
            status: 400,
            error_type: "mapper_parsing_exception".into(),
            reason: "bad value".into(),
        };
        dlq.enqueue(vec![duplicate]).await.unwrap();
        let entries = dlq.list(10).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, 400);
        assert_eq!(dlq.list_page(1, 10).await.unwrap().len(), 0);
        assert_eq!(
            dlq.get(&entries[0].id).await.unwrap().unwrap().id,
            entries[0].id
        );
        assert!(dlq.replay(&mut NoopSink, &[]).await.is_err());
        dlq.remove(&entries[0].id).await.unwrap();
        assert_eq!(dlq.len().await.unwrap(), 0);
    }
}
