// SPDX-License-Identifier: AGPL-3.0-or-later
//! Durable, shard-scoped FIFO batch queue backed by the configured KV store.

use super::record::{Record, RecordBatch};
use super::traits::OffsetValue;
use crate::error::{DatspoolError, Result};
use crate::storage::kv::{KvStore, cf};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct CacheEntry {
    #[serde(default)]
    pub(crate) batch_id: String,
    pub(crate) records: Vec<Record>,
    pub(crate) next_offset: Option<OffsetValue>,
    pub(crate) source_count: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BatchManifestStatus {
    Prepared,
    Delivered,
    Checkpointed,
    Acknowledged,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct BatchManifest {
    pub schema_version: u32,
    pub shard_id: String,
    pub batch_id: String,
    pub fencing_token: u64,
    pub source_offset: OffsetValue,
    pub next_offset: Option<OffsetValue>,
    pub source_count: u64,
    pub payload_digest: String,
    pub target_success_count: Option<u64>,
    pub target_failed_count: Option<u64>,
    pub target_bytes_written: Option<u64>,
    pub status: BatchManifestStatus,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl BatchManifest {
    pub(crate) fn key(shard_id: &str, batch_id: &str) -> Vec<u8> {
        format!("batch_manifest:{shard_id}:{batch_id}").into_bytes()
    }
}

/// A persistent queue scoped to one migration shard. Entries are acknowledged
/// only after the destination confirms successful delivery.
pub struct LocalBatchCache<S: KvStore> {
    store: Arc<S>,
    prefix: Vec<u8>,
    max_entries: usize,
}

impl<S: KvStore> Clone for LocalBatchCache<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            prefix: self.prefix.clone(),
            max_entries: self.max_entries,
        }
    }
}

impl<S: KvStore> LocalBatchCache<S> {
    pub fn new(store: Arc<S>, max_entries: usize) -> Self {
        Self::with_namespace(store, max_entries, "default", "migration-batch")
    }

    pub fn new_for_shard(store: Arc<S>, max_entries: usize, shard_id: &str) -> Self {
        Self::with_namespace(store, max_entries, shard_id, "migration-batch")
    }

    /// Cache namespace used only for batches committed through the Raft log.
    pub fn new_replicated_for_shard(store: Arc<S>, max_entries: usize, shard_id: &str) -> Self {
        Self::with_namespace(store, max_entries, shard_id, "migration-batch-v2")
    }

    fn with_namespace(store: Arc<S>, max_entries: usize, namespace: &str, prefix: &str) -> Self {
        let safe_namespace: String = namespace
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
            prefix: format!("{prefix}/{safe_namespace}/").into_bytes(),
            max_entries: max_entries.max(1),
        }
    }

    pub(crate) fn prefix_for_shard(shard_id: &str) -> Vec<u8> {
        Self::namespace_prefix("migration-batch-v2", shard_id)
    }

    fn namespace_prefix(prefix: &str, shard_id: &str) -> Vec<u8> {
        let safe_namespace: String = shard_id
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || "-_".contains(c) {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        format!("{prefix}/{safe_namespace}/").into_bytes()
    }

    pub(crate) fn key_for_shard(shard_id: &str, batch_id: &str) -> Vec<u8> {
        let mut key = Self::prefix_for_shard(shard_id);
        key.extend_from_slice(batch_id.as_bytes());
        key
    }

    pub async fn ensure_capacity(&self) -> Result<()> {
        let pending = self.pending().await?;
        if pending >= self.max_entries {
            return Err(DatspoolError::Internal(format!(
                "local migration cache is full ({pending} entries; limit {})",
                self.max_entries
            )));
        }
        Ok(())
    }

    pub async fn enqueue(&self, batch: &RecordBatch) -> Result<String> {
        self.enqueue_with_checkpoint(batch, None, batch.len() as u64)
            .await
    }

    /// Persist the batch and the source cursor together. A restart can replay
    /// the batch and advance the shard checkpoint only after delivery succeeds.
    pub async fn enqueue_with_checkpoint(
        &self,
        batch: &RecordBatch,
        next_offset: Option<OffsetValue>,
        source_count: u64,
    ) -> Result<String> {
        if self.pending().await? >= self.max_entries {
            return Err(DatspoolError::Internal(format!(
                "local migration cache is full ({} entries; limit {})",
                self.pending().await?,
                self.max_entries
            )));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seq = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let batch_id = format!("{:032x}-{:016x}", now, seq);
        self.enqueue_with_id(&batch_id, batch, next_offset, source_count)
            .await
    }

    pub(crate) async fn enqueue_with_id(
        &self,
        batch_id: &str,
        batch: &RecordBatch,
        next_offset: Option<OffsetValue>,
        source_count: u64,
    ) -> Result<String> {
        self.ensure_capacity().await?;
        let key = [self.prefix.as_slice(), batch_id.as_bytes()].concat();
        let entry = CacheEntry {
            batch_id: batch_id.to_string(),
            records: batch.records().to_vec(),
            next_offset,
            source_count,
        };
        self.store
            .put_cf(cf::CACHE, &key, &serde_json::to_vec(&entry)?)
            .await?;
        Ok(String::from_utf8_lossy(&key).into_owned())
    }

    /// Wait for a batch committed through Raft to become visible in this node's
    /// local state-machine database before attempting delivery.
    pub async fn wait_for_batch(&self, batch_id: &str, timeout: std::time::Duration) -> Result<()> {
        let key = [self.prefix.as_slice(), batch_id.as_bytes()].concat();
        tokio::time::timeout(timeout, async {
            loop {
                if self.store.get_cf(cf::CACHE, &key).await?.is_some() {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }).await.map_err(|_| DatspoolError::Internal(format!(
            "Raft committed cache batch {batch_id}, but it was not applied locally before timeout"
        )))?
    }

    pub(crate) async fn entries(&self) -> Result<Vec<(String, CacheEntry)>> {
        let rows = self.store.prefix_scan_cf(cf::CACHE, &self.prefix).await?;
        rows.into_iter()
            .map(|(key, value)| {
                let entry = serde_json::from_slice(&value)?;
                Ok((String::from_utf8_lossy(&key).into_owned(), entry))
            })
            .collect()
    }

    pub(crate) async fn peek_entry(&self) -> Result<Option<(String, CacheEntry)>> {
        let entries = self.store.prefix_scan_cf(cf::CACHE, &self.prefix).await?;
        match entries.into_iter().next() {
            Some((key, value)) => {
                let entry: CacheEntry = serde_json::from_slice(&value)?;
                Ok(Some((String::from_utf8_lossy(&key).into_owned(), entry)))
            }
            None => Ok(None),
        }
    }

    pub async fn peek(&self) -> Result<Option<(String, RecordBatch)>> {
        Ok(self
            .peek_entry()
            .await?
            .map(|(key, entry)| (key, entry.records.into())))
    }

    pub async fn ack(&self, key: &str) -> Result<()> {
        self.store.delete_cf(cf::CACHE, key.as_bytes()).await
    }

    pub async fn pending(&self) -> Result<usize> {
        Ok(self
            .store
            .prefix_scan_cf(cf::CACHE, &self.prefix)
            .await?
            .len())
    }

    /// Deliver and acknowledge one batch, returning its checkpoint only after
    /// the destination has accepted the complete batch.
    pub async fn deliver_one_with_checkpoint<F, Fut>(&self, deliver: F) -> Result<bool>
    where
        F: FnOnce(RecordBatch, Option<OffsetValue>, u64, String) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        self.deliver_one_with_checkpoint_and_ack(deliver, |_| async { Ok(()) })
            .await
    }

    pub async fn deliver_one_with_checkpoint_and_ack<F, Fut, A, AFut>(
        &self,
        deliver: F,
        acknowledge: A,
    ) -> Result<bool>
    where
        F: FnOnce(RecordBatch, Option<OffsetValue>, u64, String) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
        A: FnOnce(String) -> AFut,
        AFut: std::future::Future<Output = Result<()>>,
    {
        let Some((key, entry)) = self.peek_entry().await? else {
            return Ok(false);
        };
        let batch_id = if entry.batch_id.is_empty() {
            let prefix = String::from_utf8_lossy(&self.prefix);
            key.strip_prefix(prefix.as_ref())
                .unwrap_or_default()
                .to_string()
        } else {
            entry.batch_id
        };
        deliver(
            entry.records.into(),
            entry.next_offset,
            entry.source_count,
            batch_id,
        )
        .await?;
        // Replicate the acknowledgement before deleting the local cache copy.
        acknowledge(key.clone()).await?;
        self.ack(&key).await?;
        Ok(true)
    }

    pub async fn deliver_one<F, Fut>(&self, deliver: F) -> Result<bool>
    where
        F: FnOnce(RecordBatch) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        self.deliver_one_with_checkpoint(|batch, _, _, _| deliver(batch))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::rocksdb::RocksDbStore;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn persists_checkpoint_until_acknowledged() {
        let dir = tempdir().unwrap();
        let store = Arc::new(RocksDbStore::open(dir.path()).unwrap());
        let cache = LocalBatchCache::new_for_shard(store, 4, "shard-a");
        let batch = RecordBatch::from(vec![Record::with_id("1", "elasticsearch", json!({"v": 1}))]);
        let offset = OffsetValue::Custom(json!({"pit_id":"pit-1","search_after":[1]}));
        cache
            .enqueue_with_checkpoint(&batch, Some(offset.clone()), 1)
            .await
            .unwrap();
        let acknowledged = cache
            .deliver_one_with_checkpoint(
                |restored, next_offset, source_count, _batch_id| async move {
                    assert!(!restored.is_empty());
                    assert_eq!(next_offset, Some(offset));
                    assert_eq!(source_count, 1);
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert!(acknowledged);
        assert_eq!(cache.pending().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn failed_delivery_keeps_batch() {
        let dir = tempdir().unwrap();
        let store = Arc::new(RocksDbStore::open(dir.path()).unwrap());
        let cache = LocalBatchCache::new_for_shard(store, 2, "shard-b");
        cache
            .enqueue(&RecordBatch::from(vec![Record::new("es", json!({"v": 1}))]))
            .await
            .unwrap();
        let result = cache
            .deliver_one(|_| async {
                Err(DatspoolError::Internal("destination unavailable".into()))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(cache.pending().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn queues_are_isolated_by_shard() {
        let dir = tempdir().unwrap();
        let store = Arc::new(RocksDbStore::open(dir.path()).unwrap());
        let a = LocalBatchCache::new_for_shard(store.clone(), 2, "a");
        let b = LocalBatchCache::new_for_shard(store, 2, "b");
        a.enqueue(&RecordBatch::from(vec![Record::new("es", json!({"v": 1}))]))
            .await
            .unwrap();
        assert_eq!(a.pending().await.unwrap(), 1);
        assert_eq!(b.pending().await.unwrap(), 0);
    }
}
