// SPDX-License-Identifier: AGPL-3.0-or-later

//! Tiered index store combining L1/L2/L3

use super::{MemoryCache, S3Store, SsdStore, TieredConfig, TieredStats};
use crate::error::Result;
use crate::storage::KvStore;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct TieredIndexStore<S: KvStore> {
    config: TieredConfig,
    l1: Arc<MemoryCache>,
    l2: Arc<SsdStore<S>>,
    l3: Arc<S3Store>,
    stats: Arc<RwLock<TieredStats>>,
}

impl<S: KvStore> TieredIndexStore<S> {
    pub fn config(&self) -> &TieredConfig {
        &self.config
    }

    pub fn new(config: TieredConfig, metadata_store: Arc<S>) -> Self {
        let l1 = Arc::new(MemoryCache::new(config.memory_capacity));

        // L2: Parquet files on SSD
        let ssd_base_dir = PathBuf::from("/tmp/dataspool/indexes"); // TODO: from config
        let l2 = Arc::new(SsdStore::new(
            ssd_base_dir,
            config.ssd_capacity_bytes,
            metadata_store.clone(),
        ));

        let l3 = Arc::new(S3Store::new(config.s3_bucket.clone()));

        Self {
            config,
            l1,
            l2,
            l3,
            stats: Arc::new(RwLock::new(TieredStats::default())),
        }
    }

    /// Get document IDs for a task chunk
    pub async fn get(&self, task_id: &str, chunk_id: u32) -> Result<Option<Vec<String>>> {
        let cache_key = format!("{}:{}", task_id, chunk_id);

        // L1: Memory
        if let Some(value) = self.l1.get(&cache_key) {
            self.stats.write().await.l1_hits += 1;
            return Ok(Some(value));
        }

        // L2: SSD Parquet
        if let Some(value) = self.l2.get(task_id, chunk_id).await? {
            self.stats.write().await.l2_hits += 1;
            self.l1.insert(cache_key, value.clone());
            return Ok(Some(value));
        }

        // L3: S3 Parquet
        if let Some(value) = self.l3.get(task_id, chunk_id).await? {
            self.stats.write().await.l3_hits += 1;
            self.l1.insert(cache_key.clone(), value.clone());
            // TODO: Optionally write back to L2
            return Ok(Some(value));
        }

        self.stats.write().await.l1_misses += 1;
        Ok(None)
    }

    /// Put document IDs for a task chunk
    pub async fn put(&self, task_id: String, chunk_id: u32, doc_ids: Vec<String>) -> Result<()> {
        let cache_key = format!("{}:{}", task_id, chunk_id);

        // Write to L1 and L2
        self.l1.insert(cache_key, doc_ids.clone());
        self.l2.put(task_id, chunk_id, doc_ids).await?;

        self.stats.write().await.total_writes += 1;

        // Check if L2 is over capacity
        if self.l2.is_over_capacity().await {
            // TODO: Trigger LRU eviction to L3
            tracing::warn!("L2 over capacity, need to evict to S3");
        }

        Ok(())
    }

    /// Get statistics
    pub async fn stats(&self) -> TieredStats {
        let (l1_hits, l1_misses, l1_evictions) = self.l1.stats();
        let mut stats = self.stats.read().await.clone();
        stats.l1_hits = l1_hits;
        stats.l1_misses = l1_misses;
        stats.l1_evictions = l1_evictions;
        stats.l1_entries = self.l1.size();
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_tiered_put_get() {
        let temp_dir = tempdir().unwrap();
        let metadata_store = Arc::new(crate::storage::RocksDbStore::open(temp_dir.path()).unwrap());

        let config = TieredConfig::default();
        let store = TieredIndexStore::new(config, metadata_store);

        let doc_ids = vec!["doc-1".to_string(), "doc-2".to_string()];

        // Write
        store
            .put("task-abc".to_string(), 0, doc_ids.clone())
            .await
            .unwrap();

        // Read (should hit L1)
        let result = store.get("task-abc", 0).await.unwrap().unwrap();
        assert_eq!(result, doc_ids);

        // Check stats
        let stats = store.stats().await;
        assert_eq!(stats.l1_hits, 1);
        assert_eq!(stats.total_writes, 1);
    }
}
