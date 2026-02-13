// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! gRPC client for inter-node communication

use tonic::transport::Channel;
use tracing::info;

use crate::error::{DatspoolError, Result};
use super::generated::{cluster, task, log};

/// gRPC client for connecting to other Dataspool nodes
pub struct GrpcClient {
    cluster_client: cluster::cluster_service_client::ClusterServiceClient<Channel>,
    task_client: task::task_service_client::TaskServiceClient<Channel>,
    log_client: log::log_service_client::LogServiceClient<Channel>,
}

impl GrpcClient {
    /// Connect to a remote node
    pub async fn connect(addr: &str) -> Result<Self> {
        info!("Connecting to gRPC server at {}", addr);
        
        let endpoint = Channel::from_shared(format!("http://{}", addr))
            .map_err(|e| DatspoolError::Internal(format!("Invalid address: {}", e)))?;
        
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| DatspoolError::Internal(format!("Connection failed: {}", e)))?;

        Ok(Self {
            cluster_client: cluster::cluster_service_client::ClusterServiceClient::new(
                channel.clone()
            ),
            task_client: task::task_service_client::TaskServiceClient::new(
                channel.clone()
            ),
            log_client: log::log_service_client::LogServiceClient::new(channel),
        })
    }

    /// Get cluster service client
    pub fn cluster(&mut self) -> &mut cluster::cluster_service_client::ClusterServiceClient<Channel> {
        &mut self.cluster_client
    }

    /// Get task service client
    pub fn task(&mut self) -> &mut task::task_service_client::TaskServiceClient<Channel> {
        &mut self.task_client
    }

    /// Get log service client
    pub fn log(&mut self) -> &mut log::log_service_client::LogServiceClient<Channel> {
        &mut self.log_client
    }
}
