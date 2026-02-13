// SPDX-License-Identifier: AGPL-3.0-or-later

//! Hybrid tiered index storage (Memory + SSD + S3)

mod eviction;
mod flusher;
mod memory;
mod queue;
mod s3;
mod ssd;
mod tiered;

pub use eviction::{EvictionManager, FileLocation, FileMetadata};
pub use flusher::Flusher;
pub use memory::MemoryCache;
pub use queue::{IndexEntry, IndexQueue};
pub use s3::S3Store;
pub use ssd::SsdStore;
pub use tiered::TieredIndexStore;

/// Configuration for tiered index storage
#[derive(Debug, Clone)]
pub struct TieredConfig {
    pub memory_capacity: usize,
    pub ssd_capacity_bytes: u64,
    pub s3_bucket: String,
    pub s3_endpoint: Option<String>,
    pub s3_region: String,
    pub queue_capacity: usize,
    pub flush_batch_size: usize,
    pub flush_interval_secs: u64,
}

impl Default for TieredConfig {
    fn default() -> Self {
        Self {
            memory_capacity: 100_000,
            ssd_capacity_bytes: 100 * 1024 * 1024 * 1024,
            s3_bucket: "dataspool-indexes".to_string(),
            s3_endpoint: None,
            s3_region: "us-east-1".to_string(),
            queue_capacity: 10_000,
            flush_batch_size: 1_000,
            flush_interval_secs: 10,
        }
    }
}

/// Statistics for tiered storage
#[derive(Debug, Clone, Default)]
pub struct TieredStats {
    pub l1_hits: u64,
    pub l2_hits: u64,
    pub l3_hits: u64,
    pub l1_misses: u64,
    pub l1_entries: usize,
    pub l2_entries: usize,
    pub l3_entries: usize,
    pub l1_evictions: u64,
    pub l2_evictions: u64,
    pub queue_size: usize,
    pub total_writes: u64,
}
