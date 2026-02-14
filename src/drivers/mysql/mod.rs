// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! MySQL driver for data migration

pub mod config;
pub mod source;
pub mod sink;

pub use config::MysqlConfig;
pub use source::MysqlSource;
pub use sink::MysqlSink;
