// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Communication layer - gRPC server and client for cluster communication

mod generated;
mod server;
mod client;

pub use generated::*;
pub use server::GrpcServer;
pub use client::GrpcClient;

use crate::error::Result;

/// Initialize the gRPC communication layer
pub async fn init_comms(bind_addr: &str) -> Result<GrpcServer> {
    GrpcServer::new(bind_addr).await
}
