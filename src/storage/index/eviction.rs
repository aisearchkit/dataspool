// SPDX-License-Identifier: AGPL-3.0-or-later

//! LRU eviction mechanism for L2 → L3 migration

use std::path::PathBuf;
use std::time::SystemTime;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::error::Result;
use crate::storage::KvStore;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub task_id: String,
    pub chunk_id: u32,
    pub location: FileLocation,
    pub local_path: Option<PathBuf>,
    pub s3_key: Option<String>,
    pub size_bytes: u64,
    pub doc_count: usize,
    pub last_access: SystemTime,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileLocation {
    L2,
    L3,
}

pub struct EvictionManager<S: KvStore> {
    store: S,
}

impl<S: KvStore> EvictionManager<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn put_metadata(&self, meta: &FileMetadata) -> Result<()> {
        let key = Self::metadata_key(&meta.task_id, meta.chunk_id);
        let value = serde_json::to_string(meta)
            .map_err(|e| crate::error::DatspoolError::Internal(e.to_string()))?;
        
        self.store.put(key.as_bytes(), value.as_bytes()).await?;
        Ok(())
    }

    pub async fn get_metadata(&self, task_id: &str, chunk_id: u32) -> Result<Option<FileMetadata>> {
        let key = Self::metadata_key(task_id, chunk_id);
        
        match self.store.get(key.as_bytes()).await? {
            Some(bytes) => {
                let meta: FileMetadata = serde_json::from_slice(&bytes)
                    .map_err(|e| crate::error::DatspoolError::Internal(e.to_string()))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    pub async fn touch(&self, task_id: &str, chunk_id: u32) -> Result<()> {
        if let Some(mut meta) = self.get_metadata(task_id, chunk_id).await? {
            meta.last_access = SystemTime::now();
            self.put_metadata(&meta).await?;
        }
        Ok(())
    }

    pub async fn get_lru_candidates(&self) -> Result<Vec<FileMetadata>> {
        let candidates = Vec::new();
        warn!("get_lru_candidates: full scan not implemented, returning empty");
        Ok(candidates)
    }

    pub async fn evict_to_l3(&self, task_id: &str, chunk_id: u32, s3_key: String) -> Result<Option<PathBuf>> {
        if let Some(mut meta) = self.get_metadata(task_id, chunk_id).await? {
            let local_path = meta.local_path.clone();
            
            meta.location = FileLocation::L3;
            meta.s3_key = Some(s3_key);
            meta.local_path = None;
            
            self.put_metadata(&meta).await?;
            
            info!("Evicted {}:{} to L3", task_id, chunk_id);
            return Ok(local_path);
        }
        
        Ok(None)
    }

    pub async fn l2_total_size(&self) -> Result<u64> {
        warn!("l2_total_size not implemented yet");
        Ok(0)
    }

    fn metadata_key(task_id: &str, chunk_id: u32) -> String {
        format!("file:{}:{:08}", task_id, chunk_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_metadata_crud() {
        let temp_dir = tempdir().unwrap();
        let store = crate::storage::RocksDbStore::open(temp_dir.path()).unwrap();
        let manager = EvictionManager::new(store);

        let meta = FileMetadata {
            task_id: "task-1".to_string(),
            chunk_id: 0,
            location: FileLocation::L2,
            local_path: Some(PathBuf::from("/tmp/test.parquet")),
            s3_key: None,
            size_bytes: 1024,
            doc_count: 100,
            last_access: SystemTime::now(),
            created_at: SystemTime::now(),
        };

        manager.put_metadata(&meta).await.unwrap();
        let retrieved = manager.get_metadata("task-1", 0).await.unwrap().unwrap();
        assert_eq!(retrieved.task_id, "task-1");
        assert_eq!(retrieved.location, FileLocation::L2);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        manager.touch("task-1", 0).await.unwrap();

        let touched = manager.get_metadata("task-1", 0).await.unwrap().unwrap();
        assert!(touched.last_access > meta.last_access);
    }
}
