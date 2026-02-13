// SPDX-License-Identifier: AGPL-3.0-or-later

//! L1: In-memory cache layer with LRU eviction

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tracing::debug;

/// In-memory cache for hot index data
pub struct MemoryCache {
    data: DashMap<String, Vec<String>>,
    capacity: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl MemoryCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: DashMap::new(),
            capacity,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<String>> {
        match self.data.get(key) {
            Some(entry) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn insert(&self, key: String, value: Vec<String>) {
        if self.data.len() >= self.capacity {
            self.evict_one();
        }
        self.data.insert(key, value);
    }

    fn evict_one(&self) {
        if let Some(entry) = self.data.iter().next() {
            let key = entry.key().clone();
            drop(entry);
            self.data.remove(&key);
            self.evictions.fetch_add(1, Ordering::Relaxed);
            debug!("Evicted key from L1: {}", key);
        }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.evictions.load(Ordering::Relaxed),
        )
    }
}
