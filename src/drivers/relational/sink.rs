// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Relational database sink implementation (placeholder)

use async_trait::async_trait;

use super::config::RelationalConfig;
use crate::error::DatspoolError;
use crate::pipeline::{RecordBatch, Sink};

/// Relational database data sink (placeholder)
pub struct RelationalSink {
    #[allow(dead_code)]
    config: RelationalConfig,
}

impl RelationalSink {
    pub fn new(config: RelationalConfig) -> Result<Self, DatspoolError> {
        Ok(Self { config })
    }
}

#[async_trait]
impl Sink for RelationalSink {
    fn sink_type(&self) -> &str {
        match self.config.db_type {
            super::config::DatabaseType::Mysql => "relational:mysql",
            super::config::DatabaseType::Postgresql => "relational:postgresql",
            super::config::DatabaseType::Sqlite => "relational:sqlite",
        }
    }

    async fn connect(&mut self) -> Result<(), DatspoolError> {
        // TODO: Implement relational database sink connection
        Ok(())
    }

    async fn write_batch(&mut self, _batch: RecordBatch) -> Result<crate::pipeline::WriteResult, DatspoolError> {
        // TODO: Implement relational database sink writes
        Ok(crate::pipeline::WriteResult::success(0, 0))
    }

    async fn flush(&mut self) -> Result<(), DatspoolError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), DatspoolError> {
        Ok(())
    }
}
