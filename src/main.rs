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

use dataspool::{DatspoolError, Result};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let admin_token = std::env::var("DATASPOOL_ADMIN_TOKEN").map_err(|_| {
        DatspoolError::Configuration("DATASPOOL_ADMIN_TOKEN must be set (minimum 32 bytes)".into())
    })?;
    let db_path = std::env::var("DATASPOOL_DB_PATH").unwrap_or_else(|_| "./data/dataspool".into());
    let bind = std::env::var("DATASPOOL_HTTP_BIND").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let store = dataspool::RocksDbStore::open(&db_path)?;
    let tasks = std::sync::Arc::new(dataspool::TaskStore::new(store));
    let api = dataspool::DlqApi::new(tasks, admin_token)?;
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| DatspoolError::Internal(format!("HTTP bind {bind} failed: {e}")))?;

    tracing::info!(%bind, %db_path, "Starting Dataspool DLQ admin API");
    if !bind.starts_with("127.0.0.1:") && !bind.starts_with("[::1]:") {
        tracing::warn!(
            "DLQ admin API is bound to a non-loopback address; terminate TLS and restrict network access at a trusted reverse proxy"
        );
    }
    axum::serve(listener, api.router())
        .await
        .map_err(|e| DatspoolError::Internal(format!("HTTP server failed: {e}")))?;
    Ok(())
}
