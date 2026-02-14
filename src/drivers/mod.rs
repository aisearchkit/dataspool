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

//! Data source and sink drivers
//!
//! Pluggable driver architecture for different data sources:
//! - **document**: Document databases (Elasticsearch, OpenSearch)
//! - **relational**: Relational databases (MySQL, PostgreSQL, SQLite)
//! - **nosql**: NoSQL databases (MongoDB, Redis - future)
//! - **streaming**: Streaming platforms (Kafka, Pulsar, AutoMQ - future)
//! - **object_storage**: Object storage (S3, MinIO, NFS - future)

pub mod document;
pub mod factory;
pub mod relational;

pub use document::{DocumentConfig, DocumentSink, DocumentSource};
pub use factory::{SinkConfig, SourceConfig};
pub use relational::{DatabaseType, RelationalConfig, RelationalSink, RelationalSource, SslConfig};
