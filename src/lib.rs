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

//! Dataspool - Hybrid-mode data migration and orchestration platform
//!
//! This crate provides a distributed data migration system with built-in
//! high availability through Raft consensus and horizontal scalability
//! through distributed task execution.

pub mod config;
pub mod error;
pub mod types;

pub use config::DatspoolConfig;
pub use error::{
    ConfigError, DatspoolError, DriverError, RaftError, Result, StorageError,
};
pub use types::{
    NodeId, NodeRole, NodeStatus, ShardId, ShardState, TaskId, TaskState, TaskStats,
};
