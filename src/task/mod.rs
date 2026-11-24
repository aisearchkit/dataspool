// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Dataspool Contributors

//! Task management module for distributed data migration tasks
//!
//! This module implements task and shard metadata management following the
//! hybrid all-in-one architecture. Tasks are automatically split into shards
//! for parallel execution across distributed worker pools.

pub mod meta;
pub mod shard;
pub mod store;

pub use meta::{NodeTaskAggregate, ShardCheckpoint, TaskAggregate, TaskMeta, TaskPriority};
pub use shard::{ShardGenerator, ShardMeta, ShardingStrategy};
pub use store::{
    AuditEvent, NodeLoadMetric, OperationalMetrics, SlaAlert, TaskStore, TaskStoreSnapshot,
};
