// SPDX-License-Identifier: AGPL-3.0-or-later

//! Tiered index store combining L1/L2/L3

use super::{MemoryCache, S3Store, SsdStore, TieredConfig, TieredStats};
use crate::error::Result;
use crate::storage::KvStore;
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
    pub fn new(config: TieredConfig, store: S) -> Self {
        let l1 = Arc::new(MemoryCache::new(config.memory_capacity));
        let l2 = Arc::new(SsdStore::new(store, config.ssd_capacity_bytes));
        let l3 = Arc::new(S3Store::new(config.s3_bucket.clone()));

        Self {
            config,
            l1,
            l2,
            l3,
            stats: Arc::new(RwLock::new(TieredStats::default())),
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<String>>> {
        // L1: Memory
        if let Some(value) = self.l1.get(key) {
            return Ok(Some(value));
        }

        // L2: SSD
        if let Some(value) = self.l2.get(key).await? {
            self.l1.insert(key.to_string(), value.clone());
            return Ok(Some(value));
        }

        // L3: S3
        if let Some(value) = self.l3.get(key).await? {
            self.l1.insert(key.to_string(), value.clone());
            return Ok(Some(value));
        }

        Ok(None)
    }

    pub async fn put(&self, key: String, value: Vec<String>) -> Result<()> {
        self.l1.insert(key.clone(), value.clone());
        self.l2.put(key, value).await?;
        Ok(())
    }

    pub async fn stats(&self) -> TieredStats {
        let (l1_hits, l1_misses, l1_evictions) = self.l1.stats();
        TieredStats {
            l1_hits,
            l1_misses,
            l1_evictions,
            l1_entries: self.l1.size(),
            ..Default::default()
        }
    }
}
