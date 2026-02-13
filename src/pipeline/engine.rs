// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

//! Pipeline execution engine

use super::record::RecordBatch;
use super::traits::{OffsetValue, Sink, Source, Transform};
use crate::error::Result;
use std::sync::Arc;
use std::time::Instant;

/// Pipeline execution statistics
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    /// Total records read from source
    pub records_read: u64,

    /// Total records written to sink
    pub records_written: u64,

    /// Total bytes read
    pub bytes_read: u64,

    /// Total bytes written
    pub bytes_written: u64,

    /// Number of failed records
    pub records_failed: u64,

    /// Total execution time in milliseconds
    pub duration_ms: u64,

    /// Current offset position
    pub current_offset: Option<OffsetValue>,
}

impl PipelineStats {
    /// Calculate throughput in records per second
    pub fn throughput_records_per_sec(&self) -> f64 {
        if self.duration_ms == 0 {
            return 0.0;
        }
        (self.records_read as f64) / (self.duration_ms as f64 / 1000.0)
    }

    /// Calculate throughput in MB per second
    pub fn throughput_mb_per_sec(&self) -> f64 {
        if self.duration_ms == 0 {
            return 0.0;
        }
        let mb = self.bytes_read as f64 / (1024.0 * 1024.0);
        mb / (self.duration_ms as f64 / 1000.0)
    }

    /// Get progress percentage (if total count is known)
    pub fn progress_percentage(&self, total_records: Option<u64>) -> Option<f64> {
        total_records.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.records_read as f64 / total as f64) * 100.0
            }
        })
    }
}

/// Callback for progress updates
pub trait ProgressCallback: Send + Sync {
    /// Called periodically with current statistics
    fn on_progress(&self, stats: &PipelineStats);
}

/// No-op progress callback
pub struct NoOpCallback;

impl ProgressCallback for NoOpCallback {
    fn on_progress(&self, _stats: &PipelineStats) {}
}

/// Pipeline configuration
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Number of records to process per batch
    pub batch_size: usize,

    /// Checkpoint offset every N records
    pub checkpoint_interval: u64,

    /// Maximum number of consecutive errors before failing
    pub max_consecutive_errors: u32,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            checkpoint_interval: 10000,
            max_consecutive_errors: 10,
        }
    }
}

/// Data pipeline executor
///
/// Orchestrates data flow: Source -> Transforms -> Sink
pub struct Pipeline {
    source: Box<dyn Source>,
    transforms: Vec<Box<dyn Transform>>,
    sink: Box<dyn Sink>,
    config: PipelineConfig,
    progress_callback: Option<Arc<dyn ProgressCallback>>,
}

impl Pipeline {
    /// Create a new pipeline
    pub fn new(source: Box<dyn Source>, sink: Box<dyn Sink>, config: PipelineConfig) -> Self {
        Self {
            source,
            transforms: Vec::new(),
            sink,
            config,
            progress_callback: None,
        }
    }

    /// Add a transformation step
    pub fn add_transform(mut self, transform: Box<dyn Transform>) -> Self {
        self.transforms.push(transform);
        self
    }

    /// Set progress callback
    pub fn with_progress_callback(mut self, callback: Arc<dyn ProgressCallback>) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    /// Execute the pipeline starting from given offset
    pub async fn run(&mut self, initial_offset: OffsetValue) -> Result<PipelineStats> {
        let start_time = Instant::now();
        let mut stats = PipelineStats::default();
        let mut current_offset = initial_offset;
        let mut consecutive_errors = 0;

        // Connect to source and sink
        self.source.connect().await?;
        self.sink.connect().await?;

        tracing::info!(
            source = self.source.source_type(),
            sink = self.sink.sink_type(),
            "Pipeline started"
        );

        // Get estimated total count for progress tracking
        let _total_count = self.source.estimate_count().await?;

        loop {
            // Read batch from source
            let (batch, new_offset) = self
                .source
                .read_batch(&current_offset, self.config.batch_size)
                .await?;

            // Empty batch means end of data
            if batch.is_empty() {
                break;
            }

            let batch_size = batch.len();
            tracing::debug!(
                batch_size,
                offset = ?new_offset,
                "Read batch from source"
            );

            // Apply transformations
            let transformed_batch = self.apply_transforms(batch)?;

            // Write to sink
            match self.sink.write_batch(transformed_batch).await {
                Ok(write_result) => {
                    stats.records_read += batch_size as u64;
                    stats.records_written += write_result.success_count as u64;
                    stats.bytes_written += write_result.bytes_written;
                    stats.records_failed += write_result.failed_count as u64;

                    // Reset error counter on success
                    consecutive_errors = 0;

                    // Update offset
                    current_offset = new_offset.clone();
                    stats.current_offset = Some(new_offset);

                    // Report progress
                    if stats.records_read % self.config.checkpoint_interval == 0 {
                        stats.duration_ms = start_time.elapsed().as_millis() as u64;
                        if let Some(ref callback) = self.progress_callback {
                            callback.on_progress(&stats);
                        }
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;
                    tracing::warn!(
                        error = %e,
                        consecutive_errors,
                        "Write batch failed"
                    );

                    if consecutive_errors >= self.config.max_consecutive_errors {
                        tracing::error!(
                            max_errors = self.config.max_consecutive_errors,
                            "Maximum consecutive errors reached"
                        );
                        return Err(e);
                    }
                }
            }
        }

        // Flush sink
        self.sink.flush().await?;

        // Close connections
        self.sink.close().await?;
        self.source.close().await?;

        stats.duration_ms = start_time.elapsed().as_millis() as u64;

        tracing::info!(
            records_read = stats.records_read,
            records_written = stats.records_written,
            duration_ms = stats.duration_ms,
            throughput_rps = stats.throughput_records_per_sec(),
            "Pipeline completed"
        );

        Ok(stats)
    }

    /// Apply all transformations to a batch
    fn apply_transforms(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut records = batch.into_records();

        for transform in &self.transforms {
            let mut transformed = Vec::with_capacity(records.len());

            for record in records {
                let result = transform.transform(record)?;
                transformed.extend(result);
            }

            records = transformed;
        }

        Ok(RecordBatch::from(records))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::record::Record;
    use crate::pipeline::traits::FilterTransform;
    use crate::pipeline::WriteResult;
    use async_trait::async_trait;
    use serde_json::json;

    // Mock source that generates test records
    struct MockSource {
        records_generated: usize,
        total_records: usize,
    }

    impl MockSource {
        fn new(total_records: usize) -> Self {
            Self {
                records_generated: 0,
                total_records,
            }
        }
    }

    #[async_trait]
    impl Source for MockSource {
        async fn connect(&mut self) -> Result<()> {
            Ok(())
        }

        async fn read_batch(
            &mut self,
            _offset: &OffsetValue,
            batch_size: usize,
        ) -> Result<(RecordBatch, OffsetValue)> {
            let remaining = self.total_records - self.records_generated;
            if remaining == 0 {
                return Ok((
                    RecordBatch::new(),
                    OffsetValue::DocumentCount(self.records_generated as u64),
                ));
            }

            let count = remaining.min(batch_size);
            let mut batch = RecordBatch::with_capacity(count);

            for i in 0..count {
                let record = Record::new("mock", json!({"index": self.records_generated + i}));
                batch.push(record);
            }

            self.records_generated += count;
            let offset = OffsetValue::DocumentCount(self.records_generated as u64);

            Ok((batch, offset))
        }

        async fn estimate_count(&self) -> Result<Option<u64>> {
            Ok(Some(self.total_records as u64))
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }

        fn source_type(&self) -> &str {
            "mock"
        }
    }

    // Mock sink that collects records
    struct MockSink {
        records: Vec<Record>,
    }

    impl MockSink {
        fn new() -> Self {
            Self {
                records: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl Sink for MockSink {
        async fn connect(&mut self) -> Result<()> {
            Ok(())
        }

        async fn write_batch(&mut self, batch: RecordBatch) -> Result<WriteResult> {
            let count = batch.len();
            self.records.extend(batch.into_records());
            Ok(WriteResult::success(count, count as u64 * 100))
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }

        fn sink_type(&self) -> &str {
            "mock"
        }
    }

    #[tokio::test]
    async fn test_pipeline_basic_flow() {
        let source = Box::new(MockSource::new(100));
        let sink = Box::new(MockSink::new());
        let config = PipelineConfig {
            batch_size: 10,
            ..Default::default()
        };

        let mut pipeline = Pipeline::new(source, sink, config);
        let stats = pipeline.run(OffsetValue::initial()).await.unwrap();

        assert_eq!(stats.records_read, 100);
        assert_eq!(stats.records_written, 100);
        // Duration is always >= 0 for u64, just verify it exists
        assert!(stats.duration_ms < u64::MAX);
    }

    #[tokio::test]
    async fn test_pipeline_with_transform() {
        let source = Box::new(MockSource::new(100));
        let sink = Box::new(MockSink::new());
        let config = PipelineConfig::default();

        // Filter: keep only records with even index
        let transform = Box::new(FilterTransform::new(|record: &Record| {
            if let Some(index) = record.payload.get("index").and_then(|v| v.as_u64()) {
                index % 2 == 0
            } else {
                false
            }
        }));

        let mut pipeline = Pipeline::new(source, sink, config).add_transform(transform);

        let stats = pipeline.run(OffsetValue::initial()).await.unwrap();

        assert_eq!(stats.records_read, 100);
        assert_eq!(stats.records_written, 50); // Half filtered out
    }

    #[test]
    fn test_pipeline_stats() {
        let stats = PipelineStats {
            records_read: 1000,
            bytes_read: 1024 * 1024, // 1 MB
            duration_ms: 1000,       // 1 second
            ..Default::default()
        };

        assert_eq!(stats.throughput_records_per_sec(), 1000.0);
        assert_eq!(stats.throughput_mb_per_sec(), 1.0);

        assert_eq!(stats.progress_percentage(Some(2000)), Some(50.0));
        assert_eq!(stats.progress_percentage(None), None);
    }
}
