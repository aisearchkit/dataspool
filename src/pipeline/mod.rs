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

//! Data pipeline module for ETL operations

pub mod backpressure;
pub mod cache;
pub mod engine;
pub mod record;
pub mod traits;

pub use backpressure::{AdaptiveBackpressure, BackpressureConfig, ClusterPressure, FlowWindow};
pub use cache::LocalBatchCache;

pub use engine::{Pipeline, PipelineConfig, PipelineStats, ProgressCallback};
pub use record::{Record, RecordBatch};
pub use traits::{
    FieldRenameTransform, FilterTransform, NoOpTransform, OffsetValue, Sink, Source, Transform,
    WriteResult,
};
