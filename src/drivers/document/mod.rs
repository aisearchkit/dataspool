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

//! Document store driver for data migration
//!
//! Compatible with HTTP-based document stores:
//! - Elasticsearch (5.x - 8.x)
//! - OpenSearch (1.x - 2.x)
//! - Easysearch, Infini, and other compatible engines
//!
//! Features:
//! - Automatic version detection
//! - Scroll API and Point-in-Time support
//! - Bulk API for efficient writes
//! - Configurable batch sizes and timeouts
//! - Local shard reader for zero-network overhead (when colocated)

pub mod cdc;
mod config;
pub mod local_reader;
mod sink;
mod source;

pub use cdc::{CdcConfig, ElasticsearchCdcSource, switch_alias_after_catchup};
pub use config::DocumentConfig;
pub use sink::{ClusterNodePressure, DocumentSink, NodeWritePressure};
pub use source::DocumentSource;
