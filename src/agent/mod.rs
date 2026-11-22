// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Agent worker pool - distributed task execution

mod executor;
mod shard_claimer;
mod worker_pool;

pub use executor::{ExecutionStats, ShardExecutor};
pub use shard_claimer::ShardClaimer;
pub use worker_pool::{ResourceLimits, WorkerPool};
