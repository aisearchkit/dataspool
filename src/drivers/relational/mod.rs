// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Relational database driver for data migration
//!
//! Supports multiple relational databases through a unified interface:
//! - MySQL (5.7+, 8.0+)
//! - PostgreSQL (12+, 13+, 14+, 15+)
//! - SQLite (future support)

pub mod config;
pub mod outbox;
pub mod sink;
pub mod source;

pub use config::{DatabaseType, RelationalConfig, SslConfig};
pub use outbox::{
    PartitionedCdcWriteBarrier, PostgresOutboxConfig, PostgresOutboxEvent,
    PostgresOutboxEventInput, PostgresOutboxRetentionStats, PostgresOutboxSource,
    PostgresOutboxVectorWatermark, PostgresOutboxWrite, PostgresPartitionedOutbox,
    PostgresPartitionedOutboxConfig, PostgresPartitionedOutboxEvent,
    PostgresPartitionedOutboxSource, PostgresPartitionedOutboxWrite, PostgresTransactionalOutbox,
};
pub use sink::RelationalSink;
pub use source::RelationalSource;
