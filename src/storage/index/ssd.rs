// SPDX-License-Identifier: AGPL-3.0-or-later

//! L2: SSD layer using RocksDB

use crate::error::Result;
use crate::storage::KvStore;

pub struct SsdStore<S: KvStore> {
    store: S,
    capacity_bytes: u64,
}

impl<S: KvStore> SsdStore<S> {
    pub fn new(store: S, capacity_bytes: u64) -> Self {
        Self {
            store,
            capacity_bytes,
        }
    }

    pub async fn get(&self, _key: &str) -> Result<Option<Vec<String>>> {
        // TODO: Implement RocksDB read
        Ok(None)
    }

    pub async fn put(&self, _key: String, _value: Vec<String>) -> Result<()> {
        // TODO: Implement RocksDB write
        Ok(())
    }
}
