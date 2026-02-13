// SPDX-License-Identifier: AGPL-3.0-or-later

//! MPMC queue for async index writes

use crate::error::Result;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub task_id: String,
    pub doc_ids: Vec<String>,
}

pub struct IndexQueue {
    tx: mpsc::Sender<IndexEntry>,
    rx: Option<mpsc::Receiver<IndexEntry>>,
}

impl IndexQueue {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self { tx, rx: Some(rx) }
    }

    pub async fn send(&self, entry: IndexEntry) -> Result<()> {
        self.tx
            .send(entry)
            .await
            .map_err(|_| crate::error::DatspoolError::Internal("Queue send failed".to_string()))?;
        Ok(())
    }

    pub fn take_receiver(&mut self) -> Option<mpsc::Receiver<IndexEntry>> {
        self.rx.take()
    }

    pub fn sender(&self) -> mpsc::Sender<IndexEntry> {
        self.tx.clone()
    }
}
