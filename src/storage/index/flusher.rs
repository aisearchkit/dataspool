// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Background flusher thread for async index writes
//!
//! Consumes from MPMC queue and writes batches to Parquet:
//! 1. Receive IndexEntry from queue
//! 2. Accumulate in memory buffer
//! 3. Flush when: batch_size reached OR timeout elapsed
//! 4. Write Parquet file to L2
//! 5. Check capacity and trigger LRU eviction

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time;
use tracing::{debug, info, warn};

use super::{IndexEntry, TieredIndexStore, TieredConfig};
use crate::error::Result;
use crate::storage::KvStore;

/// Background flusher that consumes queue and writes batches
pub struct Flusher<S: KvStore> {
    config: TieredConfig,
    store: Arc<TieredIndexStore<S>>,
    receiver: Receiver<IndexEntry>,
}

impl<S: KvStore + 'static> Flusher<S> {
    pub fn new(
        config: TieredConfig,
        store: Arc<TieredIndexStore<S>>,
        receiver: Receiver<IndexEntry>,
    ) -> Self {
        Self {
            config,
            store,
            receiver,
        }
    }

    /// Start background flush loop
    pub async fn run(mut self) -> Result<()> {
        info!("Starting index flusher (batch_size={}, interval={}s)", 
              self.config.flush_batch_size, self.config.flush_interval_secs);

        let mut buffer: HashMap<String, Vec<String>> = HashMap::new();
        let mut total_count = 0usize;
        let mut interval = time::interval(Duration::from_secs(self.config.flush_interval_secs));

        loop {
            tokio::select! {
                // Receive from queue
                entry = self.receiver.recv() => {
                    match entry {
                        Some(entry) => {
                            let key = format!("{}:{}", entry.task_id, 0); // TODO: chunk_id logic
                            buffer.entry(key).or_default().extend(entry.doc_ids);
                            total_count += 1;

                            // Flush if batch size reached
                            if total_count >= self.config.flush_batch_size {
                                self.flush_buffer(&mut buffer, &mut total_count).await?;
                            }
                        }
                        None => {
                            info!("Queue closed, flushing remaining data");
                            self.flush_buffer(&mut buffer, &mut total_count).await?;
                            break;
                        }
                    }
                }

                // Timeout flush
                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        debug!("Timeout reached, flushing {} entries", total_count);
                        self.flush_buffer(&mut buffer, &mut total_count).await?;
                    }
                }
            }
        }

        info!("Index flusher stopped");
        Ok(())
    }

    async fn flush_buffer(
        &self,
        buffer: &mut HashMap<String, Vec<String>>,
        total_count: &mut usize,
    ) -> Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }

        info!("Flushing {} entries to storage", *total_count);

        for (key, doc_ids) in buffer.drain() {
            // Parse key: "task_id:chunk_id"
            let parts: Vec<&str> = key.split(':').collect();
            if parts.len() != 2 {
                warn!("Invalid key format: {}", key);
                continue;
            }

            let task_id = parts[0].to_string();
            let chunk_id: u32 = parts[1].parse().unwrap_or(0);

            // Write to tiered store
            self.store.put(task_id, chunk_id, doc_ids).await?;
        }

        *total_count = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::IndexQueue;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_flusher_batch() {
        let temp_dir = tempdir().unwrap();
        let metadata_store = Arc::new(
            crate::storage::RocksDbStore::open(temp_dir.path()).unwrap()
        );

        let config = TieredConfig {
            flush_batch_size: 3,
            flush_interval_secs: 1,
            ..Default::default()
        };

        let store = Arc::new(TieredIndexStore::new(config.clone(), metadata_store));
        
        let mut queue = IndexQueue::new(10);
        let receiver = queue.take_receiver().unwrap();
        
        let flusher = Flusher::new(config, store.clone(), receiver);

        // Spawn flusher
        let handle = tokio::spawn(async move {
            flusher.run().await
        });

        // Send data
        for i in 0..5 {
            queue.send(IndexEntry {
                task_id: "task-1".to_string(),
                doc_ids: vec![format!("doc-{}", i)],
            }).await.unwrap();
        }

        // Wait a bit for flush
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Close queue
        drop(queue);

        // Wait for flusher to finish
        handle.await.unwrap().unwrap();

        // Verify data was written
        let result = store.get("task-1", 0).await.unwrap();
        assert!(result.is_some());
    }
}
