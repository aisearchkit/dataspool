// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors

//! OpenRaft HTTP transport. RPC endpoints are internal cluster traffic only.

#[cfg(test)]
use std::collections::HashSet;
use std::io;
use std::sync::Arc;
#[cfg(test)]
use tokio::sync::RwLock;

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
};
use openraft::BasicNode;
use openraft::error::{NetworkError, RPCError, RaftError as OpenRaftError};
use openraft::network::{RPCOption, RaftNetwork as OpenRaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use reqwest::Client;

use super::Raft;
use super::storage::{RocksNodeId, TypeConfig};

#[derive(Clone)]
pub struct NetworkFactory {
    client: Client,
    token: String,
    #[cfg(test)]
    blocked_nodes: Arc<RwLock<HashSet<RocksNodeId>>>,
}

impl NetworkFactory {
    pub fn new(token: String) -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .connect_timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("HTTP client"),
            token,
            #[cfg(test)]
            blocked_nodes: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    #[cfg(test)]
    pub async fn set_peer_blocked(&self, node_id: RocksNodeId, blocked: bool) {
        let mut nodes = self.blocked_nodes.write().await;
        if blocked {
            nodes.insert(node_id);
        } else {
            nodes.remove(&node_id);
        }
    }
}

pub struct NetworkConnection {
    client: Client,
    base_url: String,
    token: String,
    #[cfg(test)]
    target: RocksNodeId,
    #[cfg(test)]
    blocked_nodes: Arc<RwLock<HashSet<RocksNodeId>>>,
}

impl NetworkConnection {
    fn rpc_error<E: std::error::Error + 'static>(
        error: E,
    ) -> RPCError<RocksNodeId, BasicNode, OpenRaftError<RocksNodeId>> {
        RPCError::Network(NetworkError::new(&error))
    }

    async fn post<Req, Resp>(&self, path: &str, request: &Req) -> Result<Resp, io::Error>
    where
        Req: serde::Serialize + ?Sized,
        Resp: serde::de::DeserializeOwned,
    {
        #[cfg(test)]
        if self.blocked_nodes.read().await.contains(&self.target) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("test network partition blocks Raft peer {}", self.target),
            ));
        }
        self.client
            .post(format!("{}{path}", self.base_url))
            .header("x-dataspool-raft-token", &self.token)
            .json(request)
            .send()
            .await
            .map_err(io::Error::other)?
            .error_for_status()
            .map_err(io::Error::other)?
            .json()
            .await
            .map_err(io::Error::other)
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: RocksNodeId, node: &BasicNode) -> Self::Network {
        #[cfg(not(test))]
        let _ = target;
        NetworkConnection {
            client: self.client.clone(),
            base_url: format!("http://{}", node.addr),
            token: self.token.clone(),
            #[cfg(test)]
            target,
            #[cfg(test)]
            blocked_nodes: self.blocked_nodes.clone(),
        }
    }
}

impl OpenRaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<RocksNodeId>,
        RPCError<RocksNodeId, BasicNode, OpenRaftError<RocksNodeId>>,
    > {
        self.post("/raft/append", &rpc)
            .await
            .map_err(Self::rpc_error)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<RocksNodeId>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<RocksNodeId>,
        RPCError<RocksNodeId, BasicNode, OpenRaftError<RocksNodeId>>,
    > {
        self.post("/raft/vote", &rpc).await.map_err(Self::rpc_error)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RocksNodeId>,
        RPCError<
            RocksNodeId,
            BasicNode,
            OpenRaftError<RocksNodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        self.post("/raft/snapshot", &rpc)
            .await
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))
    }
}

#[derive(Clone)]
struct RaftHttpState {
    raft: Arc<Raft>,
    token: Arc<[u8]>,
}

pub fn router(raft: Arc<Raft>, token: String) -> Router {
    let state = RaftHttpState {
        raft,
        token: Arc::from(token.into_bytes()),
    };
    Router::new()
        .route("/raft/vote", post(vote))
        .route("/raft/append", post(append))
        .route("/raft/snapshot", post(snapshot))
        .route("/raft/propose", post(client_write))
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize))
        .with_state(state)
}

async fn authorize(
    State(state): State<RaftHttpState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    use subtle::ConstantTimeEq;
    let supplied = request
        .headers()
        .get("x-dataspool-raft-token")
        .map(|v| v.as_bytes())
        .unwrap_or_default();
    if supplied.len() != state.token.len() || !bool::from(supplied.ct_eq(state.token.as_ref())) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(request).await
}

async fn client_write(
    State(state): State<RaftHttpState>,
    Json(mut command): Json<super::Command>,
) -> Result<Json<super::CommandResponse>, StatusCode> {
    super::node::normalize_command_timestamp(&mut command).map_err(|_| StatusCode::BAD_REQUEST)?;
    state
        .raft
        .client_write(command)
        .await
        .map(|response| Json(response.data))
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

async fn vote(
    State(state): State<RaftHttpState>,
    Json(request): Json<VoteRequest<RocksNodeId>>,
) -> Result<Json<VoteResponse<RocksNodeId>>, StatusCode> {
    state
        .raft
        .vote(request)
        .await
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

async fn append(
    State(state): State<RaftHttpState>,
    Json(request): Json<AppendEntriesRequest<TypeConfig>>,
) -> Result<Json<AppendEntriesResponse<RocksNodeId>>, StatusCode> {
    state
        .raft
        .append_entries(request)
        .await
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

async fn snapshot(
    State(state): State<RaftHttpState>,
    Json(request): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Result<Json<InstallSnapshotResponse<RocksNodeId>>, StatusCode> {
    state
        .raft
        .install_snapshot(request)
        .await
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

#[cfg(test)]
#[derive(Clone)]
struct MutableRaftHttpState {
    raft: Arc<RwLock<Option<Arc<Raft>>>>,
    token: Arc<[u8]>,
}

/// Test-only router whose Raft handle can be replaced while the TCP listener
/// remains stable. This models a process restart without rebinding a socket
/// that may still have accepted connections in flight.
#[cfg(test)]
pub(crate) fn test_router_with_shared_raft(
    raft: Arc<RwLock<Option<Arc<Raft>>>>,
    token: String,
) -> Router {
    let state = MutableRaftHttpState {
        raft,
        token: Arc::from(token.into_bytes()),
    };
    Router::new()
        .route("/raft/vote", post(test_vote))
        .route("/raft/append", post(test_append))
        .route("/raft/snapshot", post(test_snapshot))
        .route("/raft/propose", post(test_client_write))
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            test_authorize,
        ))
        .with_state(state)
}

#[cfg(test)]
async fn test_authorize(
    State(state): State<MutableRaftHttpState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    use subtle::ConstantTimeEq;
    let supplied = request
        .headers()
        .get("x-dataspool-raft-token")
        .map(|value| value.as_bytes())
        .unwrap_or_default();
    if supplied.len() != state.token.len() || !bool::from(supplied.ct_eq(state.token.as_ref())) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(request).await
}

#[cfg(test)]
async fn test_client_write(
    State(state): State<MutableRaftHttpState>,
    Json(mut command): Json<super::Command>,
) -> Result<Json<super::CommandResponse>, StatusCode> {
    super::node::normalize_command_timestamp(&mut command).map_err(|_| StatusCode::BAD_REQUEST)?;
    state
        .raft
        .read()
        .await
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?
        .client_write(command)
        .await
        .map(|response| Json(response.data))
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

#[cfg(test)]
async fn test_vote(
    State(state): State<MutableRaftHttpState>,
    Json(request): Json<VoteRequest<RocksNodeId>>,
) -> Result<Json<VoteResponse<RocksNodeId>>, StatusCode> {
    state
        .raft
        .read()
        .await
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?
        .vote(request)
        .await
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

#[cfg(test)]
async fn test_append(
    State(state): State<MutableRaftHttpState>,
    Json(request): Json<AppendEntriesRequest<TypeConfig>>,
) -> Result<Json<AppendEntriesResponse<RocksNodeId>>, StatusCode> {
    state
        .raft
        .read()
        .await
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?
        .append_entries(request)
        .await
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

#[cfg(test)]
async fn test_snapshot(
    State(state): State<MutableRaftHttpState>,
    Json(request): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Result<Json<InstallSnapshotResponse<RocksNodeId>>, StatusCode> {
    state
        .raft
        .read()
        .await
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?
        .install_snapshot(request)
        .await
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}
