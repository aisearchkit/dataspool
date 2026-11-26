// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! gRPC client for inter-node communication

use tonic::{
    Request,
    metadata::MetadataValue,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
};
use tracing::info;

use super::generated::{cluster, log, task};
use crate::error::{DatspoolError, Result};

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
                channel.clone(),
            ),
            task_client: task::task_service_client::TaskServiceClient::new(channel.clone()),
            log_client: log::log_service_client::LogServiceClient::new(channel),
        })
    }

    /// Connect to a federated peer using mutual TLS. The endpoint must be an
    /// HTTPS URL whose DNS name matches the peer certificate SAN.
    pub async fn connect_mtls(
        endpoint_url: &str,
        ca_file: &std::path::Path,
        certificate_file: &std::path::Path,
        private_key_file: &std::path::Path,
    ) -> Result<Self> {
        let ca = tokio::fs::read(ca_file)
            .await
            .map_err(|e| DatspoolError::Configuration(format!("Unable to read peer CA: {e}")))?;
        Self::connect_mtls_with_ca_pem(endpoint_url, &ca, certificate_file, private_key_file).await
    }

    /// Connect with a Raft-managed peer trust bundle rather than a static CA file.
    pub async fn connect_mtls_with_ca_pem(
        endpoint_url: &str,
        ca_pem: &[u8],
        certificate_file: &std::path::Path,
        private_key_file: &std::path::Path,
    ) -> Result<Self> {
        let parsed = url::Url::parse(endpoint_url)
            .map_err(|e| DatspoolError::Configuration(format!("Invalid peer endpoint: {e}")))?;
        if parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(DatspoolError::Configuration(
                "Federated peer endpoint must be an HTTPS origin URL".into(),
            ));
        }
        let cert = tokio::fs::read(certificate_file).await.map_err(|e| {
            DatspoolError::Configuration(format!("Unable to read client certificate: {e}"))
        })?;
        let key = tokio::fs::read(private_key_file).await.map_err(|e| {
            DatspoolError::Configuration(format!("Unable to read client private key: {e}"))
        })?;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca_pem))
            .identity(Identity::from_pem(cert, key))
            .domain_name(parsed.host_str().expect("validated host").to_string());
        let endpoint = Endpoint::from_shared(endpoint_url.to_string())
            .map_err(|e| DatspoolError::Configuration(format!("Invalid peer endpoint: {e}")))?
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .tls_config(tls)
            .map_err(|e| {
                DatspoolError::Configuration(format!("Invalid peer TLS configuration: {e}"))
            })?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| DatspoolError::Internal(format!("mTLS peer connection failed: {e}")))?;
        Ok(Self {
            cluster_client: cluster::cluster_service_client::ClusterServiceClient::new(
                channel.clone(),
            ),
            task_client: task::task_service_client::TaskServiceClient::new(channel.clone()),
            log_client: log::log_service_client::LogServiceClient::new(channel),
        })
    }

    /// Stage a peer certificate rotation while authenticating with the currently trusted certificate.
    pub async fn stage_federated_certificate_rotation(
        &mut self,
        peer_cluster_id: &str,
        new_fingerprint: &str,
        request_id: &str,
        grace_period_ms: i64,
    ) -> Result<task::FederatedCertificateRotationResponse> {
        let mut request = Request::new(task::FederatedCertificateRotationRequest {
            new_fingerprint: new_fingerprint.to_string(),
            request_id: request_id.to_string(),
            grace_period_ms,
        });
        request.metadata_mut().insert(
            "x-dataspool-peer-cluster",
            MetadataValue::try_from(peer_cluster_id).map_err(|e| {
                DatspoolError::Configuration(format!("Invalid peer cluster metadata: {e}"))
            })?,
        );
        self.task_client
            .stage_federated_certificate_rotation(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| {
                DatspoolError::Internal(format!("Certificate rotation staging failed: {status}"))
            })
    }

    /// Prove possession of the staged new certificate. Call on a client connected using the new cert/key pair.
    pub async fn prove_federated_certificate_rotation(
        &mut self,
        peer_cluster_id: &str,
        new_fingerprint: &str,
        request_id: &str,
    ) -> Result<task::FederatedCertificateRotationResponse> {
        let mut request = Request::new(task::FederatedCertificateRotationRequest {
            new_fingerprint: new_fingerprint.to_string(),
            request_id: request_id.to_string(),
            grace_period_ms: 0,
        });
        request.metadata_mut().insert(
            "x-dataspool-peer-cluster",
            MetadataValue::try_from(peer_cluster_id).map_err(|e| {
                DatspoolError::Configuration(format!("Invalid peer cluster metadata: {e}"))
            })?,
        );
        self.task_client
            .prove_federated_certificate_rotation(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| {
                DatspoolError::Internal(format!("Certificate rotation proof failed: {status}"))
            })
    }

    /// Create the peer-side half of a bilateral CA trust rotation. The remote
    /// server reverses local/peer material and durably stages trust for this client.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_remote_trust_rotation(
        &mut self,
        peer_cluster_id: &str,
        rotation_id: &str,
        request_id: &str,
        local_ca_bundle_pem: &str,
        peer_ca_bundle_pem: &str,
        local_leaf_fingerprint: &str,
        peer_leaf_fingerprint: &str,
        overlap_ms: i64,
    ) -> Result<task::FederatedTrustRotationResponse> {
        let mut request = Request::new(task::FederatedTrustRotationRequest {
            rotation_id: rotation_id.to_string(),
            request_id: request_id.to_string(),
            local_ca_bundle_pem: local_ca_bundle_pem.to_string(),
            peer_ca_bundle_pem: peer_ca_bundle_pem.to_string(),
            local_leaf_fingerprint: local_leaf_fingerprint.to_string(),
            peer_leaf_fingerprint: peer_leaf_fingerprint.to_string(),
            overlap_ms,
            expected_revision: 0,
            event_json: String::new(),
        });
        request.metadata_mut().insert(
            "x-dataspool-peer-cluster",
            MetadataValue::try_from(peer_cluster_id).map_err(|error| {
                DatspoolError::Configuration(format!("Invalid peer cluster metadata: {error}"))
            })?,
        );
        self.task_client
            .create_federated_trust_rotation(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| {
                DatspoolError::Internal(format!("Remote trust rotation creation failed: {status}"))
            })
    }

    /// Apply a peer-side event; the remote server mirrors local/peer semantics
    /// and performs the corresponding local Activate/Retire/Abort Raft command.
    pub async fn advance_remote_trust_rotation(
        &mut self,
        peer_cluster_id: &str,
        rotation_id: &str,
        request_id: &str,
        event: &crate::federation::FederatedTrustRotationEvent,
    ) -> Result<task::FederatedTrustRotationResponse> {
        let event_json = serde_json::to_string(event)
            .map_err(|error| DatspoolError::Serialization(error.to_string()))?;
        let mut request = Request::new(task::FederatedTrustRotationRequest {
            rotation_id: rotation_id.to_string(),
            request_id: request_id.to_string(),
            local_ca_bundle_pem: String::new(),
            peer_ca_bundle_pem: String::new(),
            local_leaf_fingerprint: String::new(),
            peer_leaf_fingerprint: String::new(),
            overlap_ms: 0,
            expected_revision: 0,
            event_json,
        });
        request.metadata_mut().insert(
            "x-dataspool-peer-cluster",
            MetadataValue::try_from(peer_cluster_id).map_err(|error| {
                DatspoolError::Configuration(format!("Invalid peer cluster metadata: {error}"))
            })?,
        );
        self.task_client
            .advance_federated_trust_rotation(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| {
                DatspoolError::Internal(format!("Remote trust rotation advance failed: {status}"))
            })
    }

    /// Read the peer's durable half of the bilateral rotation state.
    pub async fn get_remote_trust_rotation(
        &mut self,
        peer_cluster_id: &str,
        rotation_id: &str,
    ) -> Result<task::FederatedTrustRotationResponse> {
        let mut request = Request::new(task::FederatedTrustRotationRequest {
            rotation_id: rotation_id.to_string(),
            request_id: String::new(),
            local_ca_bundle_pem: String::new(),
            peer_ca_bundle_pem: String::new(),
            local_leaf_fingerprint: String::new(),
            peer_leaf_fingerprint: String::new(),
            overlap_ms: 0,
            expected_revision: 0,
            event_json: String::new(),
        });
        request.metadata_mut().insert(
            "x-dataspool-peer-cluster",
            MetadataValue::try_from(peer_cluster_id).map_err(|error| {
                DatspoolError::Configuration(format!("Invalid peer cluster metadata: {error}"))
            })?,
        );
        self.task_client
            .get_federated_trust_rotation(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| {
                DatspoolError::Internal(format!("Remote trust rotation read failed: {status}"))
            })
    }

    /// Read a remote task over the authenticated federation channel.
    pub async fn get_remote_task(
        &mut self,
        task_id: &str,
        source_cluster_id: &str,
    ) -> Result<Option<task::GetTaskResponse>> {
        let mut request = Request::new(task::GetTaskRequest {
            task_id: task_id.to_string(),
        });
        let peer = MetadataValue::try_from(source_cluster_id).map_err(|e| {
            DatspoolError::Configuration(format!("Invalid source cluster metadata: {e}"))
        })?;
        request
            .metadata_mut()
            .insert("x-dataspool-peer-cluster", peer);
        match self.task_client.get_task(request).await {
            Ok(response) => Ok(Some(response.into_inner())),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(DatspoolError::Internal(format!(
                "Remote task query failed: {status}"
            ))),
        }
    }

    /// Send an idempotent task control operation to a federated peer.
    pub async fn control_remote_task(
        &mut self,
        task_id: &str,
        request_id: &str,
        source_cluster_id: &str,
        action: &str,
    ) -> Result<task::TaskControlResponse> {
        let request = task::TaskControlRequest {
            task_id: task_id.to_string(),
            request_id: request_id.to_string(),
        };
        let mut request = Request::new(request);
        let peer = MetadataValue::try_from(source_cluster_id).map_err(|e| {
            DatspoolError::Configuration(format!("Invalid source cluster metadata: {e}"))
        })?;
        request
            .metadata_mut()
            .insert("x-dataspool-peer-cluster", peer);
        let response = match action {
            "pause" => self.task_client.pause_task(request).await,
            "resume" => self.task_client.resume_task(request).await,
            "cancel" => self.task_client.cancel_task(request).await,
            "retry" => self.task_client.retry_task(request).await,
            _ => {
                return Err(DatspoolError::Configuration(format!(
                    "Unsupported remote task action: {action}"
                )));
            }
        };
        response
            .map(|value| value.into_inner())
            .map_err(|e| DatspoolError::Internal(format!("Remote task control failed: {e}")))
    }

    /// Prepare the target-side durable commit record after validating all shard checkpoints.
    pub async fn prepare_remote_commit(
        &mut self,
        migration_id: &str,
        generation: u64,
        task_id: &str,
        request_id: &str,
        source_cluster_id: &str,
    ) -> Result<task::FederatedOperationResponse> {
        let request = federated_operation_request(
            migration_id,
            generation,
            task_id,
            request_id,
            source_cluster_id,
        )?;
        self.task_client
            .prepare_federated_commit(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|e| DatspoolError::Internal(format!("Remote commit preparation failed: {e}")))
    }

    /// Commit the prepared target-side operation. Repeated calls are idempotent.
    pub async fn commit_remote_migration(
        &mut self,
        migration_id: &str,
        generation: u64,
        task_id: &str,
        request_id: &str,
        source_cluster_id: &str,
    ) -> Result<task::FederatedOperationResponse> {
        let request = federated_operation_request(
            migration_id,
            generation,
            task_id,
            request_id,
            source_cluster_id,
        )?;
        self.task_client
            .commit_federated_migration(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|e| DatspoolError::Internal(format!("Remote distributed commit failed: {e}")))
    }

    /// Compensate a target operation that has not crossed the commit point.
    pub async fn compensate_remote_migration(
        &mut self,
        migration_id: &str,
        generation: u64,
        task_id: &str,
        request_id: &str,
        source_cluster_id: &str,
    ) -> Result<task::FederatedOperationResponse> {
        let request = federated_operation_request(
            migration_id,
            generation,
            task_id,
            request_id,
            source_cluster_id,
        )?;
        self.task_client
            .compensate_federated_migration(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|e| DatspoolError::Internal(format!("Remote compensation failed: {e}")))
    }

    /// Get cluster service client
    pub fn cluster(
        &mut self,
    ) -> &mut cluster::cluster_service_client::ClusterServiceClient<Channel> {
        &mut self.cluster_client
    }

    /// Provision a task from a registered federated peer. The server binds the
    /// asserted cluster ID to the mTLS certificate fingerprint before accepting it.
    pub async fn create_remote_task(
        &mut self,
        request: task::CreateTaskRequest,
        source_cluster_id: &str,
    ) -> Result<task::CreateTaskResponse> {
        if source_cluster_id.trim().is_empty() || source_cluster_id.len() > 128 {
            return Err(DatspoolError::Configuration(
                "Invalid source cluster ID".into(),
            ));
        }
        let mut request = Request::new(request);
        let peer = MetadataValue::try_from(source_cluster_id).map_err(|e| {
            DatspoolError::Configuration(format!("Invalid source cluster metadata: {e}"))
        })?;
        request
            .metadata_mut()
            .insert("x-dataspool-peer-cluster", peer);
        self.task_client
            .create_task(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|e| DatspoolError::Internal(format!("Remote task provisioning failed: {e}")))
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

fn federated_operation_request(
    migration_id: &str,
    generation: u64,
    task_id: &str,
    request_id: &str,
    source_cluster_id: &str,
) -> Result<Request<task::FederatedOperationRequest>> {
    if migration_id.trim().is_empty() || task_id.trim().is_empty() || request_id.trim().is_empty() {
        return Err(DatspoolError::Configuration(
            "Federated operation identifiers are required".into(),
        ));
    }
    let mut request = Request::new(task::FederatedOperationRequest {
        migration_id: migration_id.to_string(),
        generation,
        task_id: task_id.to_string(),
        request_id: request_id.to_string(),
    });
    let peer = MetadataValue::try_from(source_cluster_id).map_err(|e| {
        DatspoolError::Configuration(format!("Invalid source cluster metadata: {e}"))
    })?;
    request
        .metadata_mut()
        .insert("x-dataspool-peer-cluster", peer);
    Ok(request)
}

#[cfg(test)]
mod federation_transport_tests {
    use super::GrpcClient;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn unreachable_federated_peer_fails_with_bounded_connect_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener); // Inject a peer-side network partition/refused connection.

        let directory = tempfile::tempdir().unwrap();
        let ca = directory.path().join("ca.pem");
        let certificate = directory.path().join("client.pem");
        let private_key = directory.path().join("client.key");
        tokio::fs::write(
            &ca,
            include_bytes!("../../tests/fixtures/federation-test-ca.pem"),
        )
        .await
        .unwrap();
        tokio::fs::write(
            &certificate,
            include_bytes!("../../tests/fixtures/federation-test-client.pem"),
        )
        .await
        .unwrap();
        tokio::fs::write(
            &private_key,
            include_bytes!("../../tests/fixtures/federation-test-client.key.pem"),
        )
        .await
        .unwrap();

        let started = Instant::now();
        let result = GrpcClient::connect_mtls(
            &format!("https://localhost:{port}"),
            &ca,
            &certificate,
            &private_key,
        )
        .await;
        assert!(
            result.is_err(),
            "unreachable peer must not be treated as connected"
        );
        assert!(
            started.elapsed() < Duration::from_secs(6),
            "peer connect exceeded the configured five-second timeout"
        );
    }
}
