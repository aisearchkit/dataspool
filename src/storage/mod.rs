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

//! Storage layer abstraction
//!
//! This module provides storage abstractions for different data types:
//! - `kv`: Key-value storage for system metadata (RocksDB)
//! - `rocksdb`: RocksDB implementation for metadata
//! - `index`: Index storage for migration data (eloqstore)
//!
//! **Note**: eloqstore integration is currently stubbed out with a mock
//! implementation. Real eloqstore requires Ubuntu 24.04+ and C++ build tools.

pub mod index;
pub mod kv;
pub mod rocksdb;

pub use index::{IndexStore, IndexTable};
pub use kv::{ColumnFamily, KvStore, WriteBatch, cf};
pub use rocksdb::RocksDbStore;
