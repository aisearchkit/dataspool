// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! MySQL sink implementation (placeholder)

use async_trait::async_trait;

use super::config::MysqlConfig;
use crate::error::DatspoolError;
use crate::pipeline::{RecordBatch, Sink};

/// MySQL data sink (placeholder)
pub struct MysqlSink {
    #[allow(dead_code)]
    config: MysqlConfig,
}

impl MysqlSink {
    pub fn new(config: MysqlConfig) -> Result<Self, DatspoolError> {
        Ok(Self { config })
    }
}

#[async_trait]
impl Sink for MysqlSink {
    fn sink_type(&self) -> &str {
        "mysql"
    }

    async fn connect(&mut self) -> Result<(), DatspoolError> {
        // TODO: Implement MySQL sink connection
        Ok(())
    }

    async fn write_batch(&mut self, _batch: RecordBatch) -> Result<crate::pipeline::WriteResult, DatspoolError> {
        // TODO: Implement MySQL sink writes
        Ok(crate::pipeline::WriteResult::success(0, 0))
    }

    async fn flush(&mut self) -> Result<(), DatspoolError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), DatspoolError> {
        Ok(())
    }
}
