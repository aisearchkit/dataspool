// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 Dataspool Contributors

//! Snapshot-plus-CDC source for Elasticsearch online migration.
//!
//! The source captures a change-log watermark before opening the snapshot.
//! It copies the snapshot first, then replays events strictly after that
//! watermark. The change-log producer must provide the ordering/visibility
//! guarantees documented in `docs/elasticsearch-online-migration.md`.

use super::{CdcConfig, DocumentConfig, DocumentSource, ElasticsearchCdcSource};
use crate::error::{DatspoolError, DriverError, Result};
use crate::pipeline::{OffsetValue, RecordBatch, Source};
use async_trait::async_trait;
use serde_json::{Value, json};

const DRIVER: &str = "elasticsearch_online_migration";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Snapshot,
    Cdc,
}

/// Reads a consistent snapshot and then tails the append-only CDC feed.
pub struct ElasticsearchOnlineMigrationSource {
    snapshot: DocumentSource,
    cdc: ElasticsearchCdcSource,
    phase: Phase,
    initial_watermark: Option<u64>,
    snapshot_offset: OffsetValue,
    cdc_offset: OffsetValue,
    connected: bool,
}

impl ElasticsearchOnlineMigrationSource {
    pub fn new(
        snapshot: DocumentConfig,
        change_log_connection: DocumentConfig,
        cdc: CdcConfig,
    ) -> Self {
        Self {
            snapshot: DocumentSource::new(snapshot),
            cdc: ElasticsearchCdcSource::new(change_log_connection, cdc),
            phase: Phase::Snapshot,
            initial_watermark: None,
            snapshot_offset: OffsetValue::initial(),
            cdc_offset: OffsetValue::Custom(json!({
                "driver": "elasticsearch_cdc",
                "last_sequence": 0
            })),
            connected: false,
        }
    }

    fn checkpoint(&self) -> OffsetValue {
        OffsetValue::Custom(json!({
            "driver": DRIVER,
            "phase": match self.phase { Phase::Snapshot => "snapshot", Phase::Cdc => "cdc" },
            "initial_watermark": self.initial_watermark.unwrap_or(0),
            "snapshot_offset": serde_json::to_value(&self.snapshot_offset).unwrap_or(Value::Null),
            "cdc_offset": serde_json::to_value(&self.cdc_offset).unwrap_or(Value::Null)
        }))
    }

    fn cdc_offset_sequence(&self) -> Option<u64> {
        match &self.cdc_offset {
            OffsetValue::Custom(value) => value["last_sequence"].as_u64(),
            _ => None,
        }
    }

    fn restore(&mut self, offset: &OffsetValue) -> Result<()> {
        let OffsetValue::Custom(value) = offset else {
            return Ok(());
        };
        if value["driver"].as_str() != Some(DRIVER) {
            return Ok(());
        }
        self.phase = match value["phase"].as_str() {
            Some("snapshot") => Phase::Snapshot,
            Some("cdc") => Phase::Cdc,
            other => return Err(DriverError::DataError(format!(
                "Invalid online migration checkpoint phase: {other:?}"
            )).into()),
        };
        self.initial_watermark = Some(value["initial_watermark"].as_u64().ok_or_else(|| {
            DriverError::DataError("Online migration checkpoint is missing initial_watermark".into())
        })?);
        self.snapshot_offset = serde_json::from_value(value["snapshot_offset"].clone())
            .map_err(|e| DriverError::DataError(format!("Invalid snapshot checkpoint: {e}")))?;
        self.cdc_offset = serde_json::from_value(value["cdc_offset"].clone())
            .map_err(|e| DriverError::DataError(format!("Invalid CDC checkpoint: {e}")))?;
        self.cdc.set_start_sequence(
            self.cdc_offset_sequence().ok_or_else(|| {
                DriverError::DataError("Online migration checkpoint has invalid CDC sequence".into())
            })?,
        );
        Ok(())
    }
}

#[async_trait]
impl Source for ElasticsearchOnlineMigrationSource {
    async fn connect(&mut self) -> Result<()> {
        self.cdc.connect().await?;
        if self.initial_watermark.is_none() {
            let watermark = self.cdc.capture_watermark().await?;
            self.initial_watermark = Some(watermark);
            self.cdc.set_start_sequence(watermark);
            self.cdc_offset = OffsetValue::Custom(json!({
                "driver": "elasticsearch_cdc",
                "last_sequence": watermark
            }));
        }
        self.snapshot.connect().await?;
        self.connected = true;
        Ok(())
    }

    async fn read_batch(
        &mut self,
        offset: &OffsetValue,
        batch_size: usize,
    ) -> Result<(RecordBatch, OffsetValue)> {
        if !self.connected {
            return Err(DatspoolError::Internal("Online migration source is not connected".into()));
        }
        self.restore(offset)?;
        if self.phase == Phase::Snapshot {
            let (batch, next) = self.snapshot.read_batch(&self.snapshot_offset, batch_size).await?;
            self.snapshot_offset = next;
            if !batch.is_empty() {
                return Ok((batch, self.checkpoint()));
            }
            // Snapshot is exhausted. Close its PIT/scroll context before tailing CDC.
            self.snapshot.close().await?;
            self.phase = Phase::Cdc;
        }
        let (batch, next) = self.cdc.read_batch(&self.cdc_offset, batch_size).await?;
        self.cdc_offset = next;
        Ok((batch, self.checkpoint()))
    }

    fn is_continuous(&self) -> bool {
        true
    }

    async fn estimate_count(&self) -> Result<Option<u64>> {
        if self.phase == Phase::Snapshot {
            self.snapshot.estimate_count().await
        } else {
            Ok(None)
        }
    }

    async fn close(&mut self) -> Result<()> {
        let snapshot_result = self.snapshot.close().await;
        let cdc_result = self.cdc.close().await;
        self.connected = false;
        snapshot_result?;
        cdc_result
    }

    fn source_type(&self) -> &str {
        DRIVER
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn online_checkpoint_preserves_snapshot_and_cdc_cursors() {
        let mut source = ElasticsearchOnlineMigrationSource::new(
            DocumentConfig::local("source"),
            DocumentConfig::local("change-log"),
            CdcConfig::default(),
        );
        source.initial_watermark = Some(17);
        source.snapshot_offset = OffsetValue::Custom(json!({
            "driver": "elasticsearch_pit",
            "pit_id": "pit-1",
            "docs_read": 100,
            "search_after": ["x"]
        }));
        source.cdc_offset = OffsetValue::Custom(json!({
            "driver": "elasticsearch_cdc",
            "last_sequence": 17
        }));
        let checkpoint = source.checkpoint();

        let mut resumed = ElasticsearchOnlineMigrationSource::new(
            DocumentConfig::local("source"),
            DocumentConfig::local("change-log"),
            CdcConfig::default(),
        );
        resumed.restore(&checkpoint).unwrap();

        assert_eq!(resumed.phase, Phase::Snapshot);
        assert_eq!(resumed.initial_watermark, Some(17));
        assert_eq!(resumed.snapshot_offset, source.snapshot_offset);
        assert_eq!(resumed.cdc_offset, source.cdc_offset);
        assert_eq!(resumed.cdc_offset_sequence(), Some(17));
    }

    #[test]
    fn online_source_is_continuous() {
        let source = ElasticsearchOnlineMigrationSource::new(
            DocumentConfig::local("source"),
            DocumentConfig::local("change-log"),
            CdcConfig::default(),
        );
        assert!(source.is_continuous());
    }
}
