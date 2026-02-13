// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! Auto-generated Protobuf code
//!
//! This module re-exports the generated gRPC types and services.
//! Code is generated during build from proto/*.proto files.

// Include generated protobuf code
pub mod cluster {
    tonic::include_proto!("dataspool.cluster");
}

pub mod task {
    tonic::include_proto!("dataspool.task");
}

pub mod log {
    tonic::include_proto!("dataspool.log");
}

// Re-export prost_types for generated code
pub use prost_types;
