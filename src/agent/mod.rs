// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Agent worker pool - distributed task execution

mod executor;
mod shard_claimer;
mod worker_pool;

pub use executor::ShardExecutor;
pub use shard_claimer::ShardClaimer;
pub use worker_pool::WorkerPool;

use crate::error::Result;

/// Initialize the agent worker pool
pub async fn init_agent(node_id: String, max_concurrent_tasks: usize) -> Result<WorkerPool> {
    WorkerPool::new(node_id, max_concurrent_tasks)
}
