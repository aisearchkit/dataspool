// SPDX-License-Identifier: AGPL-3.0-or-later
//! Secure dead-letter queue management API and operator UI.

use std::sync::Arc;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::{
    FederatedTrustRotationCoordinator, FederatedTrustRotationDiagnostics,
    FederatedTrustRotationRecoveryAction,
    drivers::factory::{SinkConfig, create_sink},
    federation::{
        FederatedCluster, FederatedClusterStatus, FederatedMigration, FederatedTrustRotationPhase,
        MigrationState, MigrationTimeoutPolicy,
    },
    pipeline::{DeadLetterEntry, DeadLetterQueue},
    raft::{Command, RaftNode},
    storage::KvStore,
    task::TaskStore,
    types::{ShardId, TaskId},
};

const UI: &str = include_str!("../../web/dlq.html");
const TRUST_ROTATION_UI: &str = include_str!("../../web/trust-rotations.html");
const MAX_PAGE_SIZE: usize = 200;
const MAX_REPLAY_IDS: usize = 100;

#[derive(Clone)]
struct OperatorCredential {
    token: Arc<[u8]>,
    actor: Arc<str>,
}

#[derive(Clone)]
struct ApiState<S: KvStore + Clone> {
    tasks: Arc<TaskStore<S>>,
    operator_credentials: Arc<Vec<OperatorCredential>>,
    operation_lock: Arc<tokio::sync::Mutex<()>>,
    raft: Option<Arc<RaftNode>>,
    trust_rotation_coordinator: Option<Arc<FederatedTrustRotationCoordinator>>,
}

/// DLQ REST API. The UI shell is public; every data endpoint requires a bearer token.
pub struct DlqApi<S: KvStore + Clone> {
    state: ApiState<S>,
}

impl<S: KvStore + Clone + 'static> DlqApi<S> {
    /// Construct the API with a single administrator credential.
    /// The authenticated principal is `admin`; caller-supplied actor headers are ignored.
    pub fn new(tasks: Arc<TaskStore<S>>, admin_token: impl Into<String>) -> crate::Result<Self> {
        Self::new_with_operators(tasks, vec![("admin".into(), admin_token.into())])
    }

    /// Construct the API with individually attributable operator credentials.
    /// Each actor is derived from the token that authenticated the request.
    pub fn new_with_operators(
        tasks: Arc<TaskStore<S>>,
        credentials: Vec<(String, String)>,
    ) -> crate::Result<Self> {
        if credentials.is_empty() || credentials.len() > 128 {
            return Err(crate::DatspoolError::Configuration(
                "DLQ operator credential count must be between 1 and 128".into(),
            ));
        }
        let mut actors = std::collections::HashSet::new();
        let mut tokens = std::collections::HashSet::new();
        let mut operator_credentials = Vec::with_capacity(credentials.len());
        for (actor, token) in credentials {
            let valid_actor = !actor.is_empty()
                && actor.len() <= 128
                && actor
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._@-".contains(&byte));
            if !valid_actor || !actors.insert(actor.clone()) {
                return Err(crate::DatspoolError::Configuration(
                    "DLQ operator identities must be unique, 1-128 ASCII letters, digits, '.', '_', '@' or '-'".into(),
                ));
            }
            if !(32..=4096).contains(&token.len())
                || !token.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
                || !tokens.insert(token.clone())
            {
                return Err(crate::DatspoolError::Configuration(
                    "DLQ operator tokens must be unique, 32-4096 bytes, and contain visible ASCII only".into(),
                ));
            }
            operator_credentials.push(OperatorCredential {
                token: Arc::from(token.into_bytes()),
                actor: Arc::from(actor),
            });
        }
        Ok(Self {
            state: ApiState {
                tasks,
                operator_credentials: Arc::new(operator_credentials),
                operation_lock: Arc::new(tokio::sync::Mutex::new(())),
                raft: None,
                trust_rotation_coordinator: None,
            },
        })
    }

    pub fn with_raft(mut self, raft: Arc<RaftNode>) -> Self {
        self.state.raft = Some(raft);
        self
    }

    pub fn with_trust_rotation_coordinator(
        mut self,
        coordinator: Arc<FederatedTrustRotationCoordinator>,
    ) -> Self {
        self.state.trust_rotation_coordinator = Some(coordinator);
        self
    }

    pub fn router(self) -> Router {
        let api = Router::new()
            .route(
                "/api/v1/tasks/{task_id}/shards/{shard_id}/dlq",
                get(list_entries),
            )
            .route(
                "/api/v1/tasks/{task_id}/shards/{shard_id}/dlq/replay",
                post(replay_entries),
            )
            .route(
                "/api/v1/tasks/{task_id}/shards/{shard_id}/dlq/{entry_id}",
                delete(delete_entry),
            )
            .route("/metrics", get(metrics))
            .route("/api/v1/audit", get(list_audit_events))
            .route("/api/v1/session", get(current_session))
            .route("/api/v1/alerts/sla", get(list_sla_alerts))
            .route(
                "/api/v1/federation/clusters",
                get(list_federated_clusters).post(register_federated_cluster),
            )
            .route(
                "/api/v1/federation/clusters/{cluster_id}/heartbeat",
                post(heartbeat_federated_cluster),
            )
            .route(
                "/api/v1/federation/clusters/{cluster_id}/trust-bundle/stage",
                post(stage_federated_trust_bundle),
            )
            .route(
                "/api/v1/federation/clusters/{cluster_id}/trust-bundle/activate",
                post(activate_federated_trust_bundle),
            )
            .route(
                "/api/v1/federation/clusters/{cluster_id}/trust-bundle/retire",
                post(retire_federated_trust_bundle),
            )
            .route(
                "/api/v1/federation/trust-rotations",
                get(list_federated_trust_rotations).post(create_federated_trust_rotation),
            )
            .route(
                "/api/v1/federation/trust-rotations/operator-view",
                get(list_trust_rotation_operator_summaries),
            )
            .route(
                "/api/v1/federation/trust-rotations/{rotation_id}",
                get(get_federated_trust_rotation),
            )
            .route(
                "/api/v1/federation/trust-rotations/{rotation_id}/deployment-ready",
                post(mark_trust_rotation_deployment_ready),
            )
            .route(
                "/api/v1/federation/trust-rotations/{rotation_id}/authorize-activation",
                post(authorize_trust_rotation_activation),
            )
            .route(
                "/api/v1/federation/trust-rotations/{rotation_id}/activate",
                post(activate_trust_rotation),
            )
            .route(
                "/api/v1/federation/trust-rotations/{rotation_id}/retire",
                post(retire_trust_rotation),
            )
            .route(
                "/api/v1/federation/trust-rotations/{rotation_id}/rollback",
                post(rollback_trust_rotation),
            )
            .route(
                "/api/v1/federation/trust-rotations/{rotation_id}/diagnostics",
                get(diagnose_trust_rotation),
            )
            .route(
                "/api/v1/federation/trust-rotations/{rotation_id}/audit",
                get(list_trust_rotation_audit),
            )
            .route(
                "/api/v1/federation/trust-rotations/{rotation_id}/recover",
                post(recover_trust_rotation),
            )
            .route(
                "/api/v1/federation/migrations",
                get(list_federated_migrations).post(create_federated_migration),
            )
            .route(
                "/api/v1/federation/migrations/{migration_id}",
                get(get_federated_migration),
            )
            .route(
                "/api/v1/federation/migrations/{migration_id}/transition",
                post(transition_federated_migration),
            )
            .route_layer(middleware::from_fn_with_state(
                self.state.clone(),
                authorize::<S>,
            ))
            .with_state(self.state);
        Router::new()
            .route("/admin/dlq", get(ui))
            .route("/admin/trust-rotations", get(trust_rotation_ui))
            .merge(api)
    }
}

#[derive(Debug, Deserialize)]
struct AuditQuery {
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct AuthenticatedSession {
    actor: String,
}

async fn current_session(headers: axum::http::HeaderMap) -> Json<AuthenticatedSession> {
    Json(AuthenticatedSession {
        actor: request_actor(&headers),
    })
}

async fn list_audit_events<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Query(query): Query<AuditQuery>,
) -> Response {
    match state
        .tasks
        .list_audit_events(query.limit.unwrap_or(100))
        .await
    {
        Ok(events) => Json(events).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct RegisterFederatedClusterRequest {
    cluster_id: String,
    endpoint: String,
    identity_fingerprint: String,
    version: String,
    #[serde(default)]
    capabilities: std::collections::BTreeSet<String>,
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct CreateFederatedTrustRotationRequest {
    rotation_id: String,
    peer_cluster_id: String,
    local_ca_bundle_pem: String,
    peer_ca_bundle_pem: String,
    local_leaf_fingerprint: String,
    peer_leaf_fingerprint: String,
    overlap_ms: i64,
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct TrustRotationActionRequest {
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct TrustRotationRollbackRequest {
    request_id: String,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct TrustRotationRecoveryRequest {
    request_id: String,
    expected_action: FederatedTrustRotationRecoveryAction,
}

#[derive(Debug, Deserialize, Default)]
struct TrustRotationListQuery {
    phase: Option<String>,
    peer_cluster_id: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Serialize)]
struct TrustRotationRecoveryResponse {
    expected_action: FederatedTrustRotationRecoveryAction,
    before: FederatedTrustRotationDiagnostics,
    after: FederatedTrustRotationDiagnostics,
}

/// Sanitized operator projection. CA bundle PEM is deliberately never serialized to the UI.
#[derive(Debug, Serialize)]
struct TrustRotationOperatorSummary {
    rotation_id: String,
    peer_cluster_id: String,
    phase: FederatedTrustRotationPhase,
    local_ca_bundle_fingerprint: String,
    peer_ca_bundle_fingerprint: String,
    local_leaf_fingerprint: String,
    peer_leaf_fingerprint: String,
    overlap_ms: i64,
    activation_barrier_committed: bool,
    local_activation_expires_at_ms: Option<i64>,
    peer_activation_expires_at_ms: Option<i64>,
    local_staged: bool,
    peer_staged: bool,
    local_deployment_ready: bool,
    peer_deployment_ready: bool,
    local_retired: bool,
    peer_retired: bool,
    revision: u64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

async fn create_federated_trust_rotation<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<CreateFederatedTrustRotationRequest>,
) -> Response {
    let Some(coordinator) = state.trust_rotation_coordinator else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match coordinator
        .start_rotation(
            &request.rotation_id,
            &request.peer_cluster_id,
            request.local_ca_bundle_pem,
            request.peer_ca_bundle_pem,
            &request.local_leaf_fingerprint,
            &request.peer_leaf_fingerprint,
            request.overlap_ms,
            &request.request_id,
            &request_actor(&headers),
        )
        .await
    {
        Ok(rotation) => Json(rotation).into_response(),
        Err(error) => {
            audit_rotation_operation_failure(
                &coordinator,
                &request.rotation_id,
                &request.request_id,
                &request_actor(&headers),
                "create",
                "failed",
                &error.to_string(),
            )
            .await;
            (StatusCode::CONFLICT, error.to_string()).into_response()
        }
    }
}

async fn list_federated_trust_rotations<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Query(query): Query<TrustRotationListQuery>,
) -> Response {
    const VALID_PHASES: &[&str] = &[
        "staging",
        "awaiting_deployment",
        "activation_barrier",
        "activating",
        "recovery_required",
        "overlap",
        "retiring",
        "completed",
        "rolling_back",
        "rolled_back",
    ];
    if query
        .phase
        .as_deref()
        .is_some_and(|phase| !VALID_PHASES.contains(&phase))
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new("invalid trust-rotation phase filter")),
        )
            .into_response();
    }
    match state.tasks.list_federated_trust_rotations().await {
        Ok(mut rotations) => {
            if let Some(phase) = query.phase.as_deref() {
                rotations.retain(|rotation| {
                    serde_json::to_value(rotation.phase)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_owned))
                        .as_deref()
                        == Some(phase)
                });
            }
            if let Some(peer_cluster_id) = query.peer_cluster_id.as_deref() {
                rotations.retain(|rotation| rotation.peer_cluster_id == peer_cluster_id);
            }
            rotations.sort_by_key(|rotation| std::cmp::Reverse(rotation.updated_at_ms));
            let offset = query.offset.unwrap_or(0);
            let limit = query.limit.unwrap_or(100).clamp(1, 500);
            Json(
                rotations
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .collect::<Vec<_>>(),
            )
            .into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn list_trust_rotation_operator_summaries<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Query(query): Query<TrustRotationListQuery>,
) -> Response {
    const VALID_PHASES: &[&str] = &[
        "staging",
        "awaiting_deployment",
        "activation_barrier",
        "activating",
        "recovery_required",
        "overlap",
        "retiring",
        "completed",
        "rolling_back",
        "rolled_back",
    ];
    if query
        .phase
        .as_deref()
        .is_some_and(|phase| !VALID_PHASES.contains(&phase))
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new("invalid trust-rotation phase filter")),
        )
            .into_response();
    }
    match state.tasks.list_federated_trust_rotations().await {
        Ok(mut rotations) => {
            if let Some(phase) = query.phase.as_deref() {
                rotations.retain(|rotation| {
                    serde_json::to_value(rotation.phase)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_owned))
                        .as_deref()
                        == Some(phase)
                });
            }
            if let Some(peer_cluster_id) = query.peer_cluster_id.as_deref() {
                rotations.retain(|rotation| rotation.peer_cluster_id == peer_cluster_id);
            }
            rotations.sort_by_key(|rotation| std::cmp::Reverse(rotation.updated_at_ms));
            let offset = query.offset.unwrap_or(0);
            let limit = query.limit.unwrap_or(25).clamp(1, 100);
            let summaries = rotations
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|rotation| TrustRotationOperatorSummary {
                    rotation_id: rotation.rotation_id,
                    peer_cluster_id: rotation.peer_cluster_id,
                    phase: rotation.phase,
                    local_ca_bundle_fingerprint: rotation.local_ca_bundle_fingerprint,
                    peer_ca_bundle_fingerprint: rotation.peer_ca_bundle_fingerprint,
                    local_leaf_fingerprint: rotation.local_leaf_fingerprint,
                    peer_leaf_fingerprint: rotation.peer_leaf_fingerprint,
                    overlap_ms: rotation.overlap_ms,
                    activation_barrier_committed: rotation.activation_barrier_committed,
                    local_activation_expires_at_ms: rotation.local_activation_expires_at_ms,
                    peer_activation_expires_at_ms: rotation.peer_activation_expires_at_ms,
                    local_staged: rotation.local_staged,
                    peer_staged: rotation.peer_staged,
                    local_deployment_ready: rotation.local_deployment_ready,
                    peer_deployment_ready: rotation.peer_deployment_ready,
                    local_retired: rotation.local_retired,
                    peer_retired: rotation.peer_retired,
                    revision: rotation.revision,
                    created_at_ms: rotation.created_at_ms,
                    updated_at_ms: rotation.updated_at_ms,
                })
                .collect::<Vec<_>>();
            Json(summaries).into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn get_federated_trust_rotation<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(rotation_id): Path<String>,
) -> Response {
    match state.tasks.get_federated_trust_rotation(&rotation_id).await {
        Ok(Some(rotation)) => Json(rotation).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn mark_trust_rotation_deployment_ready<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(rotation_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<TrustRotationActionRequest>,
) -> Response {
    let Some(coordinator) = state.trust_rotation_coordinator else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match coordinator
        .mark_local_deployment_ready(&rotation_id, &request.request_id, &request_actor(&headers))
        .await
    {
        Ok(rotation) => Json(rotation).into_response(),
        Err(error) => {
            audit_rotation_operation_failure(
                &coordinator,
                &rotation_id,
                &request.request_id,
                &request_actor(&headers),
                "deployment_ready",
                "failed",
                &error.to_string(),
            )
            .await;
            (StatusCode::CONFLICT, error.to_string()).into_response()
        }
    }
}

async fn authorize_trust_rotation_activation<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(rotation_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<TrustRotationActionRequest>,
) -> Response {
    let Some(coordinator) = state.trust_rotation_coordinator else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match coordinator
        .authorize_activation(&rotation_id, &request.request_id, &request_actor(&headers))
        .await
    {
        Ok(rotation) => Json(rotation).into_response(),
        Err(error) => {
            audit_rotation_operation_failure(
                &coordinator,
                &rotation_id,
                &request.request_id,
                &request_actor(&headers),
                "authorize_activation",
                "failed",
                &error.to_string(),
            )
            .await;
            (StatusCode::CONFLICT, error.to_string()).into_response()
        }
    }
}

async fn activate_trust_rotation<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(rotation_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<TrustRotationActionRequest>,
) -> Response {
    let Some(coordinator) = state.trust_rotation_coordinator else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match coordinator
        .activate_both(&rotation_id, &request.request_id, &request_actor(&headers))
        .await
    {
        Ok(rotation) => Json(rotation).into_response(),
        Err(error) => {
            audit_rotation_operation_failure(
                &coordinator,
                &rotation_id,
                &request.request_id,
                &request_actor(&headers),
                "activate",
                "failed",
                &error.to_string(),
            )
            .await;
            (StatusCode::CONFLICT, error.to_string()).into_response()
        }
    }
}

async fn retire_trust_rotation<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(rotation_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<TrustRotationActionRequest>,
) -> Response {
    let Some(coordinator) = state.trust_rotation_coordinator else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match coordinator
        .retire_both(&rotation_id, &request.request_id, &request_actor(&headers))
        .await
    {
        Ok(rotation) => Json(rotation).into_response(),
        Err(error) => {
            audit_rotation_operation_failure(
                &coordinator,
                &rotation_id,
                &request.request_id,
                &request_actor(&headers),
                "retire",
                "failed",
                &error.to_string(),
            )
            .await;
            (StatusCode::CONFLICT, error.to_string()).into_response()
        }
    }
}

async fn rollback_trust_rotation<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(rotation_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<TrustRotationRollbackRequest>,
) -> Response {
    let Some(coordinator) = state.trust_rotation_coordinator else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match coordinator
        .rollback_before_activation(
            &rotation_id,
            &request.reason,
            &request.request_id,
            &request_actor(&headers),
        )
        .await
    {
        Ok(rotation) => Json(rotation).into_response(),
        Err(error) => {
            audit_rotation_operation_failure(
                &coordinator,
                &rotation_id,
                &request.request_id,
                &request_actor(&headers),
                "rollback",
                "failed",
                &error.to_string(),
            )
            .await;
            (StatusCode::CONFLICT, error.to_string()).into_response()
        }
    }
}

async fn diagnose_trust_rotation<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(rotation_id): Path<String>,
) -> Response {
    let Some(coordinator) = state.trust_rotation_coordinator else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match coordinator.inspect_bilateral(&rotation_id).await {
        Ok(diagnostics) => Json(diagnostics).into_response(),
        Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    }
}

async fn list_trust_rotation_audit<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(rotation_id): Path<String>,
    Query(query): Query<AuditQuery>,
) -> Response {
    let resource_id = format!("federated_trust_rotation:{rotation_id}");
    match state
        .tasks
        .list_audit_events_for_resource(&resource_id, query.limit.unwrap_or(100))
        .await
    {
        Ok(events) => Json(events).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn recover_trust_rotation<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(rotation_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<TrustRotationRecoveryRequest>,
) -> Response {
    let Some(coordinator) = state.trust_rotation_coordinator else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let before = match coordinator.inspect_bilateral(&rotation_id).await {
        Ok(diagnostics) => diagnostics,
        Err(error) => {
            let audit_persisted = audit_rotation_operation_failure(
                &coordinator,
                &rotation_id,
                &request.request_id,
                &request_actor(&headers),
                "recover_diagnostics",
                "failed",
                &error.to_string(),
            )
            .await;
            if !audit_persisted {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ApiError::new("diagnostics failed and the failure audit could not be committed; inspect the Raft audit stream before retrying")),
                ).into_response();
            }
            return (StatusCode::BAD_GATEWAY, error.to_string()).into_response();
        }
    };
    if before.recommended_action != request.expected_action
        || !before.automated_recovery_safe
        || !before.safety_violations.is_empty()
    {
        let audit_persisted = audit_rotation_operation_failure(
            &coordinator,
            &rotation_id,
            &request.request_id,
            &request_actor(&headers),
            "recover",
            "rejected",
            "expected action is stale or bilateral diagnostics report unsafe recovery",
        )
        .await;
        if !audit_persisted {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiError::new("recovery was blocked but its rejection audit could not be committed; inspect the Raft audit stream")),
            ).into_response();
        }
        return (
            StatusCode::CONFLICT,
            Json(ApiError::new(
                "recovery action is stale or unsafe; refresh bilateral diagnostics before retrying",
            )),
        )
            .into_response();
    }
    match coordinator
        .recover_rotation(
            &rotation_id,
            &request.expected_action,
            &request.request_id,
            &request_actor(&headers),
        )
        .await
    {
        Ok(_) => {
            if let Err(error) = coordinator
                .record_operation_outcome(
                    &rotation_id,
                    &request.request_id,
                    &request_actor(&headers),
                    "recover",
                    "committed",
                    &format!("expected_action={:?}", request.expected_action),
                )
                .await
            {
                tracing::error!(rotation_id, %error, request_id = %request.request_id, "recovery may have committed but its operator outcome audit did not");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ApiError::new("recovery may have committed but its operator outcome audit could not be committed; inspect bilateral state and the Raft audit stream before retrying")),
                ).into_response();
            }
            match coordinator.inspect_bilateral(&rotation_id).await {
                Ok(after) => Json(TrustRotationRecoveryResponse {
                    expected_action: request.expected_action,
                    before,
                    after,
                })
                .into_response(),
                Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
            }
        }
        Err(error) => {
            let audit_persisted = audit_rotation_operation_failure(
                &coordinator,
                &rotation_id,
                &request.request_id,
                &request_actor(&headers),
                "recover",
                "failed",
                &error.to_string(),
            )
            .await;
            if !audit_persisted {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ApiError::new("recovery failed and its outcome audit could not be committed; inspect bilateral state and the Raft audit stream before retrying")),
                ).into_response();
            }
            (StatusCode::CONFLICT, error.to_string()).into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateFederatedMigrationRequest {
    migration_id: String,
    source_cluster: String,
    target_cluster: String,
    task_id: String,
    #[serde(default)]
    required_capabilities: std::collections::BTreeSet<String>,
    request_id: String,
    #[serde(default)]
    task_name: Option<String>,
    #[serde(default)]
    source_config: Option<String>,
    #[serde(default)]
    sink_config: Option<String>,
    #[serde(default)]
    transform_config: Option<String>,
    #[serde(default)]
    sharding_strategy: Option<String>,
    #[serde(default)]
    priority: Option<i32>,
    #[serde(default)]
    tags: std::collections::HashMap<String, String>,
    #[serde(default)]
    deadline_at_ms: Option<i64>,
    #[serde(default)]
    execution_budget_ms: Option<u64>,
    #[serde(default)]
    retry_budget: Option<u32>,
    #[serde(default)]
    timeout_policy: Option<MigrationTimeoutPolicy>,
}

#[derive(Debug, Deserialize)]
struct TransitionFederatedMigrationRequest {
    next_state: MigrationState,
    expected_generation: u64,
    request_id: String,
    error: Option<String>,
}

async fn audit_rotation_operation_failure(
    coordinator: &FederatedTrustRotationCoordinator,
    rotation_id: &str,
    request_id: &str,
    actor: &str,
    action: &str,
    outcome: &str,
    details: &str,
) -> bool {
    match coordinator
        .record_operation_outcome(rotation_id, request_id, actor, action, outcome, details)
        .await
    {
        Ok(()) => true,
        Err(audit_error) => {
            tracing::error!(rotation_id, request_id, action, %audit_error, "failed to persist trust rotation operation audit");
            false
        }
    }
}

fn request_actor(headers: &axum::http::HeaderMap) -> String {
    headers
        .get("x-dataspool-actor")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.trim().is_empty() && v.len() <= 128)
        .unwrap_or("anonymous")
        .to_string()
}

async fn register_federated_cluster<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<RegisterFederatedClusterRequest>,
) -> Response {
    let Some(raft) = state.raft else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let now = chrono::Utc::now().timestamp_millis();
    let cluster = FederatedCluster {
        cluster_id: request.cluster_id,
        endpoint: request.endpoint,
        identity_fingerprint: request.identity_fingerprint,
        previous_identity_fingerprint: None,
        previous_identity_expires_at_ms: None,
        pending_identity_fingerprint: None,
        rotation_started_at_ms: None,
        pending_rotation_grace_period_ms: None,
        trust_bundle_pem: None,
        trust_bundle_fingerprint: None,
        pending_trust_bundle_pem: None,
        pending_trust_bundle_fingerprint: None,
        trust_bundle_staged_at_ms: None,
        previous_trust_bundle_pem: None,
        previous_trust_bundle_fingerprint: None,
        previous_trust_bundle_expires_at_ms: None,
        version: request.version,
        capabilities: request.capabilities,
        status: FederatedClusterStatus::Healthy,
        last_seen_at_ms: now,
        updated_at_ms: now,
    };
    match raft
        .propose(Command::UpsertFederatedCluster {
            cluster,
            request_id: request.request_id,
            actor: request_actor(&headers),
        })
        .await
    {
        Ok(response) if response.success => Json(response).into_response(),
        Ok(response) => (StatusCode::CONFLICT, Json(response)).into_response(),
        Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct StageFederatedTrustBundleRequest {
    #[serde(default)]
    current_fingerprint: String,
    bundle_pem: String,
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct ActivateFederatedTrustBundleRequest {
    bundle_fingerprint: String,
    overlap_ms: i64,
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct RetireFederatedTrustBundleRequest {
    request_id: String,
}

async fn stage_federated_trust_bundle<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(cluster_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<StageFederatedTrustBundleRequest>,
) -> Response {
    let Some(raft) = state.raft else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let fingerprint = match crate::federation::validate_trust_bundle(&request.bundle_pem) {
        Ok(value) => value,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    match raft
        .propose(Command::StageFederatedTrustBundle {
            cluster_id,
            current_fingerprint: request.current_fingerprint,
            bundle_pem: request.bundle_pem,
            bundle_fingerprint: fingerprint,
            at_ms: chrono::Utc::now().timestamp_millis(),
            request_id: request.request_id,
            actor: request_actor(&headers),
        })
        .await
    {
        Ok(response) if response.success => Json(response).into_response(),
        Ok(response) => (StatusCode::CONFLICT, Json(response)).into_response(),
        Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    }
}

async fn activate_federated_trust_bundle<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(cluster_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<ActivateFederatedTrustBundleRequest>,
) -> Response {
    let Some(raft) = state.raft else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match raft
        .propose(Command::ActivateFederatedTrustBundle {
            cluster_id,
            bundle_fingerprint: request.bundle_fingerprint,
            at_ms: chrono::Utc::now().timestamp_millis(),
            overlap_ms: request.overlap_ms,
            request_id: request.request_id,
            actor: request_actor(&headers),
        })
        .await
    {
        Ok(response) if response.success => Json(response).into_response(),
        Ok(response) => (StatusCode::CONFLICT, Json(response)).into_response(),
        Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    }
}

async fn retire_federated_trust_bundle<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(cluster_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<RetireFederatedTrustBundleRequest>,
) -> Response {
    let Some(raft) = state.raft else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match raft
        .propose(Command::RetireFederatedTrustBundle {
            cluster_id,
            at_ms: chrono::Utc::now().timestamp_millis(),
            request_id: request.request_id,
            actor: request_actor(&headers),
        })
        .await
    {
        Ok(response) if response.success => Json(response).into_response(),
        Ok(response) => (StatusCode::CONFLICT, Json(response)).into_response(),
        Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct FederatedHeartbeatRequest {
    identity_fingerprint: String,
    status: FederatedClusterStatus,
    request_id: String,
}

async fn heartbeat_federated_cluster<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(cluster_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<FederatedHeartbeatRequest>,
) -> Response {
    let Some(raft) = state.raft else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match raft
        .propose(Command::HeartbeatFederatedCluster {
            cluster_id,
            identity_fingerprint: request.identity_fingerprint,
            status: request.status,
            last_seen_at_ms: chrono::Utc::now().timestamp_millis(),
            request_id: request.request_id,
            actor: request_actor(&headers),
        })
        .await
    {
        Ok(response) if response.success => Json(response).into_response(),
        Ok(response) => (StatusCode::CONFLICT, Json(response)).into_response(),
        Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    }
}

async fn create_federated_migration<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<CreateFederatedMigrationRequest>,
) -> Response {
    let Some(raft) = state.raft else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let now = chrono::Utc::now().timestamp_millis();
    let source_config = match request.source_config.as_deref() {
        Some(value)
            if !value.trim().is_empty()
                && serde_json::from_str::<serde_json::Value>(value)
                    .is_ok_and(|parsed| parsed.is_object()) =>
        {
            value.to_string()
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "source_config must be a valid JSON object".to_string(),
            )
                .into_response();
        }
    };
    let sink_config = match request.sink_config.as_deref() {
        Some(value)
            if !value.trim().is_empty()
                && serde_json::from_str::<serde_json::Value>(value)
                    .is_ok_and(|parsed| parsed.is_object()) =>
        {
            value.to_string()
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "sink_config must be a valid JSON object".to_string(),
            )
                .into_response();
        }
    };
    let priority = request.priority.unwrap_or(1);
    if !(0..=3).contains(&priority) {
        return (
            StatusCode::BAD_REQUEST,
            "priority must be between 0 and 3".to_string(),
        )
            .into_response();
    }
    let task_name = request
        .task_name
        .clone()
        .unwrap_or_else(|| request.migration_id.clone());
    if task_name.trim().is_empty() || task_name.len() > 256 {
        return (
            StatusCode::BAD_REQUEST,
            "task_name must contain 1-256 characters".to_string(),
        )
            .into_response();
    }
    let migration = FederatedMigration {
        task_name,
        migration_id: request.migration_id,
        source_cluster: request.source_cluster,
        target_cluster: request.target_cluster,
        task_id: request.task_id,
        required_capabilities: request.required_capabilities,
        state: MigrationState::Planned,
        generation: 0,
        created_at_ms: now,
        updated_at_ms: now,
        last_error: None,
        source_config,
        sink_config,
        transform_config: request.transform_config,
        sharding_strategy: request.sharding_strategy,
        priority,
        tags: request.tags,
        remote_task_id: None,
        remote_task_state: None,
        remote_checkpoint: None,
        last_reconciled_at_ms: None,
        deadline_at_ms: request.deadline_at_ms,
        execution_budget_ms: request.execution_budget_ms,
        retry_budget: request.retry_budget,
        timeout_policy: request.timeout_policy.unwrap_or_default(),
    };
    match raft
        .propose(Command::CreateFederatedMigration {
            migration: Box::new(migration),
            request_id: request.request_id,
            actor: request_actor(&headers),
        })
        .await
    {
        Ok(response) if response.success => Json(response).into_response(),
        Ok(response) => (StatusCode::CONFLICT, Json(response)).into_response(),
        Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    }
}

async fn transition_federated_migration<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(migration_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<TransitionFederatedMigrationRequest>,
) -> Response {
    let Some(raft) = state.raft else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match raft
        .propose(Command::TransitionFederatedMigration {
            migration_id,
            next_state: request.next_state,
            expected_generation: request.expected_generation,
            at_ms: chrono::Utc::now().timestamp_millis(),
            error: request.error,
            request_id: request.request_id,
            actor: request_actor(&headers),
        })
        .await
    {
        Ok(response) if response.success => Json(response).into_response(),
        Ok(response) => (StatusCode::CONFLICT, Json(response)).into_response(),
        Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    }
}

async fn list_federated_clusters<S: KvStore + Clone>(State(state): State<ApiState<S>>) -> Response {
    if let Some(raft) = state.raft.as_ref()
        && let Err(error) = raft.ensure_linearizable().await
    {
        return (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response();
    }
    match state.tasks.list_federated_clusters().await {
        Ok(clusters) => {
            Json(serde_json::json!({"clusters": clusters, "total": clusters.len()})).into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn list_federated_migrations<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
) -> Response {
    if let Some(raft) = state.raft.as_ref()
        && let Err(error) = raft.ensure_linearizable().await
    {
        return (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response();
    }
    match state.tasks.list_federated_migrations().await {
        Ok(migrations) => {
            Json(serde_json::json!({"migrations": migrations, "total": migrations.len()}))
                .into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn get_federated_migration<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path(migration_id): Path<String>,
) -> Response {
    if let Some(raft) = state.raft.as_ref()
        && let Err(error) = raft.ensure_linearizable().await
    {
        return (StatusCode::SERVICE_UNAVAILABLE, error.to_string()).into_response();
    }
    match state.tasks.get_federated_migration(&migration_id).await {
        Ok(Some(migration)) => Json(migration).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn list_sla_alerts<S: KvStore + Clone>(State(state): State<ApiState<S>>) -> Response {
    match state.tasks.sla_alerts().await {
        Ok(alerts) => {
            Json(serde_json::json!({"alerts": alerts, "total": alerts.len()})).into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

fn trust_rotation_metric_phase(action: &str) -> Option<&'static str> {
    let action = action
        .strip_prefix("federation.trust_rotation.")
        .unwrap_or(action);
    match action {
        "create" | "local_staged" | "peer_staged" => Some("staging"),
        // Older releases emitted a generic action without enough detail to
        // reliably attribute same-phase transitions to a specific operation.
        "advance" => Some("lifecycle"),
        "deployment_ready" | "local_deployment_ready" | "peer_deployment_ready" => {
            Some("deployment")
        }
        "authorize_activation"
        | "activation_authorized"
        | "activate"
        | "local_activated"
        | "peer_activated" => Some("activation"),
        "retire" | "local_retired" | "peer_retired" => Some("retirement"),
        "rollback" | "rollback_started" | "rollback_confirmed" | "local_rolled_back"
        | "peer_rolled_back" => Some("rollback"),
        "recover" | "recover_diagnostics" | "recover_response_diagnostics" | "reconcile" => {
            Some("recovery")
        }
        _ => None,
    }
}

#[derive(Default)]
struct TrustRotationAuditMetricSummary {
    recovery_attempts: std::collections::BTreeSet<(String, String)>,
    recovery_failures: std::collections::BTreeSet<(String, String)>,
    phase_attempts:
        std::collections::BTreeMap<(&'static str, String), std::collections::BTreeSet<String>>,
    phase_failures:
        std::collections::BTreeMap<(&'static str, String), std::collections::BTreeSet<String>>,
}

impl TrustRotationAuditMetricSummary {
    fn observe(&mut self, rotation_id: &str, event: &crate::task::AuditEvent) {
        let Some(phase) = trust_rotation_metric_phase(&event.action) else {
            return;
        };
        let key = (phase, rotation_id.to_string());
        self.phase_attempts
            .entry(key.clone())
            .or_default()
            .insert(event.request_id.clone());
        if phase == "recovery" {
            self.recovery_attempts
                .insert((rotation_id.to_string(), event.request_id.clone()));
        }
        if matches!(event.outcome.as_str(), "failed" | "rejected") {
            self.phase_failures
                .entry(key)
                .or_default()
                .insert(event.request_id.clone());
            if phase == "recovery" {
                self.recovery_failures
                    .insert((rotation_id.to_string(), event.request_id.clone()));
            }
        }
    }
}

fn prometheus_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

async fn metrics<S: KvStore + Clone>(State(state): State<ApiState<S>>) -> Response {
    let metrics = match state.tasks.operational_metrics().await {
        Ok(metrics) => metrics,
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };
    let sla_alerts = match state.tasks.sla_alerts().await {
        Ok(alerts) => alerts,
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };
    let sla_at_risk = sla_alerts.iter().filter(|alert| !alert.breached).count();
    let sla_breached = sla_alerts.iter().filter(|alert| alert.breached).count();
    let rotations = match state.tasks.list_federated_trust_rotations().await {
        Ok(rotations) => rotations,
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut rotation_counts = std::collections::BTreeMap::<&'static str, usize>::new();
    let mut stalled_rotations = 0usize;
    let mut recovery_required_rotations = 0usize;
    let mut rotations_with_errors = 0usize;
    let mut audit_summary = TrustRotationAuditMetricSummary::default();
    let mut terminal_duration_sum_seconds = 0.0f64;
    let mut terminal_duration_count = 0u64;
    let audit_events = match state.tasks.list_all_audit_events().await {
        Ok(events) => events,
        Err(error) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
        }
    };
    for event in &audit_events {
        let Some(rotation_id) = event.resource_id.strip_prefix("federated_trust_rotation:") else {
            continue;
        };
        // Include failed create attempts even when no rotation state record was committed.
        audit_summary.observe(rotation_id, event);
    }
    let mut body = format!(
        "# HELP dataspool_sla_tasks_at_risk Tasks approaching configured SLA limits\n# TYPE dataspool_sla_tasks_at_risk gauge\ndataspool_sla_tasks_at_risk {}\n# HELP dataspool_sla_tasks_breached Tasks breaching configured SLA limits\n# TYPE dataspool_sla_tasks_breached gauge\ndataspool_sla_tasks_breached {}\n# HELP dataspool_tasks_total Number of known tasks\n# TYPE dataspool_tasks_total gauge\ndataspool_tasks_total {}\n# HELP dataspool_tasks Number of tasks by lifecycle state\n# TYPE dataspool_tasks gauge\ndataspool_tasks{{state=\"pending\"}} {}\ndataspool_tasks{{state=\"running\"}} {}\ndataspool_tasks{{state=\"paused\"}} {}\ndataspool_tasks{{state=\"cancelling\"}} {}\ndataspool_tasks{{state=\"cancelled\"}} {}\ndataspool_tasks{{state=\"completed\"}} {}\ndataspool_tasks{{state=\"failed\"}} {}\n# HELP dataspool_shards Number of shards by state\n# TYPE dataspool_shards gauge\ndataspool_shards{{state=\"pending\"}} {}\ndataspool_shards{{state=\"running\"}} {}\ndataspool_shards{{state=\"completed\"}} {}\ndataspool_shards{{state=\"failed\"}} {}\n# HELP dataspool_nodes Number of nodes by state\n# TYPE dataspool_nodes gauge\ndataspool_nodes{{state=\"healthy\"}} {}\ndataspool_nodes{{state=\"degraded\"}} {}\ndataspool_nodes{{state=\"paused\"}} {}\ndataspool_nodes{{state=\"offline\"}} {}\n# HELP dataspool_retries_total Total failed shard attempts beyond the first attempt\n# TYPE dataspool_retries_total counter\ndataspool_retries_total {}\n# HELP dataspool_oldest_pending_seconds Age of the oldest pending shard\n# TYPE dataspool_oldest_pending_seconds gauge\ndataspool_oldest_pending_seconds {}\n",
        sla_at_risk,
        sla_breached,
        metrics.tasks_total,
        metrics.tasks_pending,
        metrics.tasks_running,
        metrics.tasks_paused,
        metrics.tasks_cancelling,
        metrics.tasks_cancelled,
        metrics.tasks_completed,
        metrics.tasks_failed,
        metrics.shards_pending,
        metrics.shards_running,
        metrics.shards_completed,
        metrics.shards_failed,
        metrics.nodes_healthy,
        metrics.nodes_degraded,
        metrics.nodes_paused,
        metrics.nodes_offline,
        metrics.retries_total,
        metrics.oldest_pending_seconds,
    );
    body.push_str(&format!("# HELP dataspool_scheduler_delay_seconds Age of the oldest pending shard waiting for scheduling\n# TYPE dataspool_scheduler_delay_seconds gauge\ndataspool_scheduler_delay_seconds {}\n# HELP dataspool_node_active_shards Number of active shard leases per node\n# TYPE dataspool_node_active_shards gauge\n# HELP dataspool_node_capacity_shards Configured concurrent shard capacity per node\n# TYPE dataspool_node_capacity_shards gauge\n# HELP dataspool_node_memory_capacity_mb Configured memory capacity per node in MiB\n# TYPE dataspool_node_memory_capacity_mb gauge\n# HELP dataspool_node_reserved_memory_mb Memory reserved by active shard leases per node in MiB\n# TYPE dataspool_node_reserved_memory_mb gauge\n", metrics.oldest_pending_seconds));
    body.push_str("# HELP dataspool_federation_trust_rotations Number of federated trust rotations by lifecycle phase\n# TYPE dataspool_federation_trust_rotations gauge\n# HELP dataspool_federation_trust_rotation_stalled Locally persisted recoverable rotations beyond their phase-specific stall threshold\n# TYPE dataspool_federation_trust_rotation_stalled gauge\n# HELP dataspool_federation_trust_rotation_recovery_required Rotations requiring forward recovery\n# TYPE dataspool_federation_trust_rotation_recovery_required gauge\n# HELP dataspool_federation_trust_rotation_errors Rotations with a persisted last error\n# TYPE dataspool_federation_trust_rotation_errors gauge\n# HELP dataspool_federation_trust_rotation_bilateral_diagnostics_available Whether the bilateral Raft diagnostic probe succeeded for a rotation\n# TYPE dataspool_federation_trust_rotation_bilateral_diagnostics_available gauge\n# HELP dataspool_federation_trust_rotation_bilateral_state_aligned Whether the durable bilateral rotation records and trust state are aligned\n# TYPE dataspool_federation_trust_rotation_bilateral_state_aligned gauge\n");
    let mut bilateral_status = std::collections::BTreeMap::<String, Option<bool>>::new();
    if let Some(coordinator) = state.trust_rotation_coordinator.as_ref() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(8));
        let mut checks = tokio::task::JoinSet::new();
        for rotation in &rotations {
            if matches!(
                rotation.phase,
                FederatedTrustRotationPhase::Completed | FederatedTrustRotationPhase::RolledBack
            ) {
                continue;
            }
            let coordinator = Arc::clone(coordinator);
            let semaphore = Arc::clone(&semaphore);
            let rotation_id = rotation.rotation_id.clone();
            checks.spawn(async move {
                let permit = semaphore.acquire_owned().await.ok()?;
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    coordinator.inspect_bilateral_state(&rotation_id),
                )
                .await;
                drop(permit);
                let aligned = match result {
                    Ok(Ok(diagnostics)) => Some(diagnostics.state_aligned),
                    Ok(Err(error)) => {
                        tracing::debug!(rotation_id = %rotation_id, %error, "bilateral trust-rotation metrics probe failed");
                        None
                    }
                    Err(_) => {
                        tracing::debug!(rotation_id = %rotation_id, "bilateral trust-rotation metrics probe timed out");
                        None
                    }
                };
                Some((rotation_id, aligned))
            });
        }
        while let Some(result) = checks.join_next().await {
            if let Ok(Some((rotation_id, aligned))) = result {
                bilateral_status.insert(rotation_id, aligned);
            }
        }
    } else {
        for rotation in &rotations {
            if !matches!(
                rotation.phase,
                FederatedTrustRotationPhase::Completed | FederatedTrustRotationPhase::RolledBack
            ) {
                bilateral_status.insert(rotation.rotation_id.clone(), None);
            }
        }
    }
    for rotation in &rotations {
        if matches!(
            rotation.phase,
            FederatedTrustRotationPhase::Completed | FederatedTrustRotationPhase::RolledBack
        ) {
            let duration = rotation
                .updated_at_ms
                .saturating_sub(rotation.created_at_ms)
                .max(0) as f64
                / 1000.0;
            terminal_duration_sum_seconds += duration;
            terminal_duration_count = terminal_duration_count.saturating_add(1);
        }
        let phase = match rotation.phase {
            FederatedTrustRotationPhase::Staging => "staging",
            FederatedTrustRotationPhase::AwaitingDeployment => "awaiting_deployment",
            FederatedTrustRotationPhase::ActivationBarrier => "activation_barrier",
            FederatedTrustRotationPhase::Activating => "activating",
            FederatedTrustRotationPhase::RecoveryRequired => "recovery_required",
            FederatedTrustRotationPhase::Overlap => "overlap",
            FederatedTrustRotationPhase::Retiring => "retiring",
            FederatedTrustRotationPhase::Completed => "completed",
            FederatedTrustRotationPhase::RollingBack => "rolling_back",
            FederatedTrustRotationPhase::RolledBack => "rolled_back",
        };
        *rotation_counts.entry(phase).or_default() += 1;
        if rotation.phase == FederatedTrustRotationPhase::RecoveryRequired {
            recovery_required_rotations += 1;
        }
        if rotation.last_error.is_some() {
            rotations_with_errors += 1;
        }
        let stall_threshold_ms = match rotation.phase {
            FederatedTrustRotationPhase::Staging => Some(60_000),
            FederatedTrustRotationPhase::ActivationBarrier
                if rotation.activation_barrier_committed =>
            {
                Some(15_000)
            }
            FederatedTrustRotationPhase::Activating
            | FederatedTrustRotationPhase::RecoveryRequired => Some(15_000),
            FederatedTrustRotationPhase::RollingBack => Some(30_000),
            FederatedTrustRotationPhase::Retiring
                if rotation
                    .local_activation_expires_at_ms
                    .zip(rotation.peer_activation_expires_at_ms)
                    .is_some_and(|(local, peer)| now_ms >= local.max(peer)) =>
            {
                Some(60_000)
            }
            _ => None,
        };
        if stall_threshold_ms
            .is_some_and(|threshold| now_ms.saturating_sub(rotation.updated_at_ms) >= threshold)
        {
            stalled_rotations += 1;
        }
    }
    for phase in [
        "staging",
        "awaiting_deployment",
        "activation_barrier",
        "activating",
        "recovery_required",
        "overlap",
        "retiring",
        "completed",
        "rolling_back",
        "rolled_back",
    ] {
        body.push_str(&format!(
            "dataspool_federation_trust_rotations{{phase=\"{phase}\"}} {}\n",
            rotation_counts.get(phase).copied().unwrap_or_default()
        ));
    }
    body.push_str(&format!(
        "dataspool_federation_trust_rotation_stalled {stalled_rotations}\ndataspool_federation_trust_rotation_recovery_required {recovery_required_rotations}\ndataspool_federation_trust_rotation_errors {rotations_with_errors}\n"
    ));
    body.push_str(&format!(
        "# HELP dataspool_federation_trust_rotation_recovery_attempts_total Number of distinct persisted recovery requests\n# TYPE dataspool_federation_trust_rotation_recovery_attempts_total counter\ndataspool_federation_trust_rotation_recovery_attempts_total {}\n# HELP dataspool_federation_trust_rotation_recovery_failures_total Number of distinct recovery requests that failed or were rejected\n# TYPE dataspool_federation_trust_rotation_recovery_failures_total counter\ndataspool_federation_trust_rotation_recovery_failures_total {}\n# HELP dataspool_federation_trust_rotation_terminal_duration_seconds_sum Sum of terminal rotation durations (completed or rolled back)\n# TYPE dataspool_federation_trust_rotation_terminal_duration_seconds_sum gauge\ndataspool_federation_trust_rotation_terminal_duration_seconds_sum {:.3}\n# HELP dataspool_federation_trust_rotation_terminal_duration_seconds_count Number of terminal rotations with a persisted duration\n# TYPE dataspool_federation_trust_rotation_terminal_duration_seconds_count gauge\ndataspool_federation_trust_rotation_terminal_duration_seconds_count {}\n",
        audit_summary.recovery_attempts.len(), audit_summary.recovery_failures.len(), terminal_duration_sum_seconds, terminal_duration_count
    ));
    body.push_str("# HELP dataspool_federation_trust_rotation_recovery_attempts Number of distinct recovery requests per rotation\n# TYPE dataspool_federation_trust_rotation_recovery_attempts gauge\n# HELP dataspool_federation_trust_rotation_recovery_failures Number of failed or rejected recovery requests per rotation\n# TYPE dataspool_federation_trust_rotation_recovery_failures gauge\n# HELP dataspool_federation_trust_rotation_phase_operations_total Distinct audited request IDs observed for a phase across rotations\n# TYPE dataspool_federation_trust_rotation_phase_operations_total counter\n# HELP dataspool_federation_trust_rotation_phase_failures_total Distinct phase request IDs that failed or were rejected across rotations\n# TYPE dataspool_federation_trust_rotation_phase_failures_total counter\n# HELP dataspool_federation_trust_rotation_phase_failure_ratio Fraction of distinct phase requests that failed or were rejected across rotations\n# TYPE dataspool_federation_trust_rotation_phase_failure_ratio gauge\n# HELP dataspool_federation_trust_rotation_phase_operations_by_rotation_total Distinct audited request IDs for a phase per rotation\n# TYPE dataspool_federation_trust_rotation_phase_operations_by_rotation_total counter\n# HELP dataspool_federation_trust_rotation_phase_failures_by_rotation_total Distinct failed or rejected request IDs for a phase per rotation\n# TYPE dataspool_federation_trust_rotation_phase_failures_by_rotation_total counter\n# HELP dataspool_federation_trust_rotation_phase_failure_ratio_by_rotation Failure ratio for a phase within one rotation\n# TYPE dataspool_federation_trust_rotation_phase_failure_ratio_by_rotation gauge\n");
    let mut recovery_counts_by_rotation =
        std::collections::BTreeMap::<String, (usize, usize)>::new();
    for (rotation_id, _request_id) in &audit_summary.recovery_attempts {
        recovery_counts_by_rotation
            .entry(rotation_id.clone())
            .or_default()
            .0 += 1;
    }
    for (rotation_id, _request_id) in &audit_summary.recovery_failures {
        recovery_counts_by_rotation
            .entry(rotation_id.clone())
            .or_default()
            .1 += 1;
    }
    for (rotation_id, (attempts, failures)) in recovery_counts_by_rotation {
        let label = prometheus_label(&rotation_id);
        body.push_str(&format!("dataspool_federation_trust_rotation_recovery_attempts{{rotation_id=\"{label}\"}} {attempts}\ndataspool_federation_trust_rotation_recovery_failures{{rotation_id=\"{label}\"}} {failures}\n"));
    }
    let mut aggregate_phase_counts =
        std::collections::BTreeMap::<&'static str, (usize, usize)>::new();
    for ((phase_name, rotation_id), attempts) in &audit_summary.phase_attempts {
        let failures = audit_summary
            .phase_failures
            .get(&(*phase_name, rotation_id.clone()))
            .map_or(0, |ids| ids.len());
        let counts = aggregate_phase_counts.entry(phase_name).or_default();
        counts.0 = counts.0.saturating_add(attempts.len());
        counts.1 = counts.1.saturating_add(failures);
    }
    for (phase_name, (attempts, failures)) in aggregate_phase_counts {
        let ratio = if attempts == 0 {
            0.0
        } else {
            failures as f64 / attempts as f64
        };
        body.push_str(&format!("dataspool_federation_trust_rotation_phase_operations_total{{phase=\"{phase_name}\"}} {attempts}\ndataspool_federation_trust_rotation_phase_failures_total{{phase=\"{phase_name}\"}} {failures}\ndataspool_federation_trust_rotation_phase_failure_ratio{{phase=\"{phase_name}\"}} {ratio:.6}\n"));
    }
    for ((phase_name, rotation_id), attempts) in &audit_summary.phase_attempts {
        let failures = audit_summary
            .phase_failures
            .get(&(*phase_name, rotation_id.clone()))
            .map_or(0, |ids| ids.len());
        let label = prometheus_label(rotation_id);
        let ratio = if attempts.is_empty() {
            0.0
        } else {
            failures as f64 / attempts.len() as f64
        };
        body.push_str(&format!("dataspool_federation_trust_rotation_phase_operations_by_rotation_total{{rotation_id=\"{label}\",phase=\"{phase_name}\"}} {}\ndataspool_federation_trust_rotation_phase_failures_by_rotation_total{{rotation_id=\"{label}\",phase=\"{phase_name}\"}} {failures}\ndataspool_federation_trust_rotation_phase_failure_ratio_by_rotation{{rotation_id=\"{label}\",phase=\"{phase_name}\"}} {ratio:.6}\n", attempts.len()));
    }
    body.push_str("# HELP dataspool_federation_trust_rotation_overlap_remaining_seconds Remaining CA overlap duration per rotation\n# TYPE dataspool_federation_trust_rotation_overlap_remaining_seconds gauge\n# HELP dataspool_federation_trust_rotation_age_seconds Age of a trust rotation since creation\n# TYPE dataspool_federation_trust_rotation_age_seconds gauge\n# HELP dataspool_federation_trust_rotation_phase_age_seconds Time spent in the current trust rotation phase\n# TYPE dataspool_federation_trust_rotation_phase_age_seconds gauge\n# HELP dataspool_federation_trust_rotation_last_error Whether the rotation has a persisted last error\n# TYPE dataspool_federation_trust_rotation_last_error gauge\n# HELP dataspool_federation_trust_rotation_duration_seconds Total duration for terminal rotations\n# TYPE dataspool_federation_trust_rotation_duration_seconds gauge\n");
    for rotation in &rotations {
        let label = prometheus_label(&rotation.rotation_id);
        let phase = match rotation.phase {
            FederatedTrustRotationPhase::Staging => "staging",
            FederatedTrustRotationPhase::AwaitingDeployment => "awaiting_deployment",
            FederatedTrustRotationPhase::ActivationBarrier => "activation_barrier",
            FederatedTrustRotationPhase::Activating => "activating",
            FederatedTrustRotationPhase::RecoveryRequired => "recovery_required",
            FederatedTrustRotationPhase::Overlap => "overlap",
            FederatedTrustRotationPhase::Retiring => "retiring",
            FederatedTrustRotationPhase::Completed => "completed",
            FederatedTrustRotationPhase::RollingBack => "rolling_back",
            FederatedTrustRotationPhase::RolledBack => "rolled_back",
        };
        let age = now_ms.saturating_sub(rotation.created_at_ms).max(0) as f64 / 1000.0;
        let phase_age = now_ms.saturating_sub(rotation.updated_at_ms).max(0) as f64 / 1000.0;
        let last_error = i32::from(rotation.last_error.is_some());
        body.push_str(&format!(
            "dataspool_federation_trust_rotation_age_seconds{{rotation_id=\"{label}\",phase=\"{phase}\"}} {age:.3}\ndataspool_federation_trust_rotation_phase_age_seconds{{rotation_id=\"{label}\",phase=\"{phase}\"}} {phase_age:.3}\ndataspool_federation_trust_rotation_last_error{{rotation_id=\"{label}\"}} {last_error}\n"
        ));
        if !matches!(
            rotation.phase,
            FederatedTrustRotationPhase::Completed | FederatedTrustRotationPhase::RolledBack
        ) {
            let bilateral_available = i32::from(
                bilateral_status
                    .get(&rotation.rotation_id)
                    .is_some_and(Option::is_some),
            );
            let bilateral_aligned = bilateral_status
                .get(&rotation.rotation_id)
                .and_then(|aligned| *aligned)
                .map(i32::from);
            body.push_str(&format!(
                "dataspool_federation_trust_rotation_bilateral_diagnostics_available{{rotation_id=\"{label}\"}} {bilateral_available}\n"
            ));
            if let Some(aligned) = bilateral_aligned {
                body.push_str(&format!(
                    "dataspool_federation_trust_rotation_bilateral_state_aligned{{rotation_id=\"{label}\"}} {aligned}\n"
                ));
            }
        }
        if matches!(
            rotation.phase,
            FederatedTrustRotationPhase::Completed | FederatedTrustRotationPhase::RolledBack
        ) {
            let duration = rotation
                .updated_at_ms
                .saturating_sub(rotation.created_at_ms)
                .max(0) as f64
                / 1000.0;
            body.push_str(&format!("dataspool_federation_trust_rotation_duration_seconds{{rotation_id=\"{label}\",terminal_state=\"{phase}\"}} {duration:.3}\n"));
        }
        if matches!(
            rotation.phase,
            FederatedTrustRotationPhase::Overlap | FederatedTrustRotationPhase::Retiring
        ) && let Some(deadline) = rotation
            .local_activation_expires_at_ms
            .zip(rotation.peer_activation_expires_at_ms)
            .map(|(local, peer)| local.max(peer))
        {
            let remaining = deadline.saturating_sub(now_ms).max(0) as f64 / 1000.0;
            body.push_str(&format!("dataspool_federation_trust_rotation_overlap_remaining_seconds{{rotation_id=\"{label}\",phase=\"{phase}\"}} {remaining:.3}\n"));
        }
    }
    for node in &metrics.node_load {
        let label = node
            .node_id
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n");
        body.push_str(&format!("dataspool_node_active_shards{{node_id=\"{label}\"}} {}\ndataspool_node_capacity_shards{{node_id=\"{label}\"}} {}\ndataspool_node_memory_capacity_mb{{node_id=\"{label}\"}} {}\ndataspool_node_reserved_memory_mb{{node_id=\"{label}\"}} {}\n", node.active_shards, node.capacity_shards, node.memory_capacity_mb, node.reserved_memory_mb));
    }
    let mut response = body.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "text/plain; version=0.0.4; charset=utf-8"
            .parse()
            .expect("static content type"),
    );
    response
}

async fn ui() -> Html<&'static str> {
    Html(UI)
}

async fn trust_rotation_ui() -> Html<&'static str> {
    Html(TRUST_ROTATION_UI)
}

async fn authorize<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    mut request: axum::http::Request<Body>,
    next: Next,
) -> Response {
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
        .as_bytes();
    let mut authenticated_actor = None;
    // Compare against every configured credential; never accept the caller's actor header.
    for credential in state.operator_credentials.iter() {
        let expected = credential.token.as_ref();
        let matches = supplied.len() == expected.len() && bool::from(supplied.ct_eq(expected));
        if matches {
            authenticated_actor = Some(credential.actor.clone());
        }
    }
    let Some(actor) = authenticated_actor else {
        let mut response = (
            StatusCode::UNAUTHORIZED,
            Json(ApiError::new("unauthorized")),
        )
            .into_response();
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            "no-store".parse().expect("static header"),
        );
        return response;
    };
    let actor_header = match axum::http::HeaderValue::from_str(&actor) {
        Ok(value) => value,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    request
        .headers_mut()
        .insert("x-dataspool-actor", actor_header);
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-store".parse().expect("static header"),
    );
    response
}

#[derive(Debug, Serialize)]
struct ApiError {
    error: String,
}
impl ApiError {
    fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    pub offset: usize,
}
fn default_limit() -> usize {
    50
}

#[derive(Debug, Serialize)]
pub struct DlqListResponse {
    task_id: String,
    shard_id: String,
    total: usize,
    entries: Vec<DeadLetterEntry>,
}

#[allow(clippy::result_large_err)]
async fn resolve_queue<S: KvStore + Clone>(
    state: &ApiState<S>,
    task_text: &str,
    shard_text: &str,
) -> Result<(TaskId, ShardId, DeadLetterQueue<S>, crate::task::TaskMeta), Response> {
    let task_id = task_text.parse::<TaskId>().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new("invalid task_id")),
        )
            .into_response()
    })?;
    let shard_id = shard_text.parse::<ShardId>().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new("invalid shard_id")),
        )
            .into_response()
    })?;
    let shard = state
        .tasks
        .get_shard(&shard_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::new("shard not found")),
            )
                .into_response()
        })?;
    if shard.task_id != task_id {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::new("shard does not belong to task")),
        )
            .into_response());
    }
    let task = state
        .tasks
        .get_task(&task_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (StatusCode::NOT_FOUND, Json(ApiError::new("task not found"))).into_response()
        })?;
    let queue = DeadLetterQueue::new(
        Arc::new(state.tasks.kv_store().clone()),
        &shard_id.to_string(),
    );
    Ok((task_id, shard_id, queue, task))
}

fn internal_error(error: impl std::fmt::Display) -> Response {
    tracing::error!(error = %error, "DLQ admin request failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ApiError::new("internal server error")),
    )
        .into_response()
}

async fn list_entries<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path((task_text, shard_text)): Path<(String, String)>,
    Query(query): Query<ListQuery>,
) -> Response {
    let (_, _, queue, _) = match resolve_queue(&state, &task_text, &shard_text).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let limit = query.limit.clamp(1, MAX_PAGE_SIZE);
    let total = match queue.len().await {
        Ok(v) => v,
        Err(e) => return internal_error(e),
    };
    let entries = match queue.list_page(query.offset, limit).await {
        Ok(v) => v,
        Err(e) => return internal_error(e),
    };
    Json(DlqListResponse {
        task_id: task_text,
        shard_id: shard_text,
        total,
        entries,
    })
    .into_response()
}

#[derive(Debug, Deserialize)]
pub struct ReplayRequest {
    /// Explicit IDs only. Empty lists are rejected to prevent accidental replay-all.
    ids: Vec<String>,
}
#[derive(Debug, Serialize)]
pub struct ReplayResponse {
    selected: usize,
    replayed: usize,
    remaining: usize,
}

async fn replay_entries<S: KvStore + Clone + 'static>(
    State(state): State<ApiState<S>>,
    Path((task_text, shard_text)): Path<(String, String)>,
    Json(request): Json<ReplayRequest>,
) -> Response {
    if request.ids.is_empty() || request.ids.len() > MAX_REPLAY_IDS {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new(format!(
                "ids must contain 1..={MAX_REPLAY_IDS} entries"
            ))),
        )
            .into_response();
    }
    if request
        .ids
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len()
        != request.ids.len()
        || request
            .ids
            .iter()
            .any(|id| id.len() != 64 || !id.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new("invalid DLQ entry id")),
        )
            .into_response();
    }
    let _operation = state.operation_lock.lock().await;
    let (_, _, queue, task) = match resolve_queue(&state, &task_text, &shard_text).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    for id in &request.ids {
        match queue.get(id).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(ApiError::new(format!("DLQ entry {id} not found"))),
                )
                    .into_response();
            }
            Err(error) => return internal_error(error),
        }
    }
    let sink_config: SinkConfig = match serde_json::from_str(&task.sink_config) {
        Ok(v) => v,
        Err(e) => return internal_error(e),
    };
    let mut sink = match create_sink(&sink_config) {
        Ok(v) => v,
        Err(e) => return internal_error(e),
    };
    if let Err(e) = sink.connect().await {
        return internal_error(e);
    }
    let selected = request.ids.len();
    let replayed = queue.replay(sink.as_mut(), &request.ids).await;
    let close_result = sink.close().await;
    let replayed = match replayed {
        Ok(v) => v,
        Err(e) => return internal_error(e),
    };
    if let Err(e) = close_result {
        return internal_error(e);
    }
    let remaining = match queue.len().await {
        Ok(v) => v,
        Err(e) => return internal_error(e),
    };
    tracing::warn!(task_id = %task_text, shard_id = %shard_text, selected, replayed, remaining, "DLQ entries replayed by administrator");
    Json(ReplayResponse {
        selected,
        replayed,
        remaining,
    })
    .into_response()
}

async fn delete_entry<S: KvStore + Clone>(
    State(state): State<ApiState<S>>,
    Path((task_text, shard_text, entry_id)): Path<(String, String, String)>,
) -> Response {
    if entry_id.len() != 64 || !entry_id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new("invalid DLQ entry id")),
        )
            .into_response();
    }
    let _operation = state.operation_lock.lock().await;
    let (_, _, queue, _) = match resolve_queue(&state, &task_text, &shard_text).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let exists = match queue.get(&entry_id).await {
        Ok(entry) => entry.is_some(),
        Err(e) => return internal_error(e),
    };
    if !exists {
        return (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("DLQ entry not found")),
        )
            .into_response();
    }
    match queue.remove(&entry_id).await {
        Ok(()) => {
            tracing::warn!(task_id = %task_text, shard_id = %shard_text, entry_id = %entry_id, "DLQ entry permanently discarded by administrator");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => internal_error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::RocksDbStore, task::TaskStore};
    use tempfile::tempdir;

    #[test]
    fn operator_summary_exposes_fingerprints_but_never_certificate_pem() {
        let summary = TrustRotationOperatorSummary {
            rotation_id: "rotation-safe-view".into(),
            peer_cluster_id: "peer-a".into(),
            phase: FederatedTrustRotationPhase::Overlap,
            local_ca_bundle_fingerprint: "sha256:local-ca".into(),
            peer_ca_bundle_fingerprint: "sha256:peer-ca".into(),
            local_leaf_fingerprint: "sha256:local-leaf".into(),
            peer_leaf_fingerprint: "sha256:peer-leaf".into(),
            overlap_ms: 3_600_000,
            activation_barrier_committed: true,
            local_activation_expires_at_ms: Some(1_800_000_000_000),
            peer_activation_expires_at_ms: Some(1_800_000_000_000),
            local_staged: true,
            peer_staged: true,
            local_deployment_ready: true,
            peer_deployment_ready: true,
            local_retired: false,
            peer_retired: false,
            revision: 7,
            created_at_ms: 1_799_000_000_000,
            updated_at_ms: 1_799_500_000_000,
        };
        let serialized = serde_json::to_string(&summary).unwrap();
        assert!(serialized.contains("sha256:local-ca"));
        assert!(serialized.contains("activation_barrier_committed"));
        assert!(!serialized.contains("_pem"));
        assert!(!serialized.contains("BEGIN CERTIFICATE"));
    }

    #[test]
    fn trust_rotation_audit_metrics_deduplicate_request_replays() {
        let mut summary = TrustRotationAuditMetricSummary::default();
        let event = |request_id: &str, action: &str, outcome: &str| crate::task::AuditEvent {
            log_index: 1,
            actor: "operator".into(),
            request_id: request_id.into(),
            action: action.into(),
            resource_id: "federated_trust_rotation:rotation-1".into(),
            from_state: "staging".into(),
            to_state: "staging".into(),
            occurred_at_ms: 1_700_000_000_000,
            outcome: outcome.into(),
            details: None,
        };
        let failed = event("recover-1", "federation.trust_rotation.recover", "failed");
        summary.observe("rotation-1", &failed);
        summary.observe("rotation-1", &failed);
        let recovered = event(
            "recover-1",
            "federation.trust_rotation.recover",
            "committed",
        );
        summary.observe("rotation-1", &recovered);
        let stage = event(
            "stage-1",
            "federation.trust_rotation.local_staged",
            "committed",
        );
        summary.observe("rotation-1", &stage);

        assert_eq!(summary.recovery_attempts.len(), 1);
        assert_eq!(summary.recovery_failures.len(), 1);
        assert_eq!(
            summary
                .phase_attempts
                .get(&("recovery", "rotation-1".into()))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            summary
                .phase_failures
                .get(&("recovery", "rotation-1".into()))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            summary
                .phase_attempts
                .get(&("staging", "rotation-1".into()))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            trust_rotation_metric_phase("federation.trust_rotation.peer_retired"),
            Some("retirement")
        );
        assert_eq!(
            trust_rotation_metric_phase("federation.trust_rotation.reconcile"),
            Some("recovery")
        );
        assert_eq!(
            trust_rotation_metric_phase("federation.trust_rotation.unknown"),
            None
        );
        assert_eq!(
            trust_rotation_metric_phase("federation.trust_rotation.advance"),
            Some("lifecycle")
        );
    }

    #[tokio::test]
    async fn ui_is_public_but_dlq_api_requires_bearer_token() {
        use axum::{body::Body, http::Request};
        use tower::ServiceExt;

        let dir = tempdir().unwrap();
        let store = RocksDbStore::open(dir.path()).unwrap();
        let tasks = Arc::new(TaskStore::new(store));
        let app = DlqApi::new(tasks.clone(), "a-secure-test-token-with-at-least-32-bytes")
            .unwrap()
            .router();

        let ui = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/dlq")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ui.status(), StatusCode::OK);

        let trust_ui = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/trust-rotations")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(trust_ui.status(), StatusCode::OK);
        let trust_ui_body = axum::body::to_bytes(trust_ui.into_body(), usize::MAX)
            .await
            .unwrap();
        let trust_ui_html = String::from_utf8(trust_ui_body.to_vec()).unwrap();
        assert!(trust_ui_html.contains("Operator confirmation"));
        assert!(trust_ui_html.contains("ActivationBarrier is committed"));

        let session_unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/session")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(session_unauthorized.status(), StatusCode::UNAUTHORIZED);
        let session = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/session")
                    .header(
                        "authorization",
                        "Bearer a-secure-test-token-with-at-least-32-bytes",
                    )
                    .header("x-dataspool-actor", "spoofed-operator")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(session.status(), StatusCode::OK);
        let session_body = axum::body::to_bytes(session.into_body(), usize::MAX)
            .await
            .unwrap();
        let session_json: serde_json::Value = serde_json::from_slice(&session_body).unwrap();
        assert_eq!(session_json["actor"], "admin");

        let operator_app = DlqApi::new_with_operators(
            tasks.clone(),
            vec![(
                "oncall-platform".into(),
                "operator-token-with-at-least-32-bytes-123".into(),
            )],
        )
        .unwrap()
        .router();
        let operator_session = operator_app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/session")
                    .header(
                        "authorization",
                        "Bearer operator-token-with-at-least-32-bytes-123",
                    )
                    .header("x-dataspool-actor", "attacker")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(operator_session.status(), StatusCode::OK);
        let operator_session_body = axum::body::to_bytes(operator_session.into_body(), usize::MAX)
            .await
            .unwrap();
        let operator_session_json: serde_json::Value =
            serde_json::from_slice(&operator_session_body).unwrap();
        assert_eq!(operator_session_json["actor"], "oncall-platform");

        let operator_view_path =
            "/api/v1/federation/trust-rotations/operator-view?limit=20&offset=0";
        let operator_view_unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(operator_view_path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            operator_view_unauthorized.status(),
            StatusCode::UNAUTHORIZED
        );
        let operator_view = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(operator_view_path)
                    .header(
                        "authorization",
                        "Bearer a-secure-test-token-with-at-least-32-bytes",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(operator_view.status(), StatusCode::OK);
        let operator_view_body = axum::body::to_bytes(operator_view.into_body(), usize::MAX)
            .await
            .unwrap();
        let operator_view_json = String::from_utf8(operator_view_body.to_vec()).unwrap();
        assert_eq!(operator_view_json, "[]");
        assert!(!operator_view_json.contains("_pem"));

        let unauthorized = app.clone().oneshot(Request::builder().uri("/api/v1/tasks/00000000-0000-0000-0000-000000000001/shards/00000000-0000-0000-0000-000000000002/dlq").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let authorized = app.clone().oneshot(Request::builder().uri("/api/v1/tasks/00000000-0000-0000-0000-000000000001/shards/00000000-0000-0000-0000-000000000002/dlq")
            .header("authorization", "Bearer a-secure-test-token-with-at-least-32-bytes").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(authorized.status(), StatusCode::NOT_FOUND);

        let diagnostics_path = "/api/v1/federation/trust-rotations/rotation-1/diagnostics";
        let diagnostics_unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(diagnostics_path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(diagnostics_unauthorized.status(), StatusCode::UNAUTHORIZED);
        let diagnostics_unavailable = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(diagnostics_path)
                    .header(
                        "authorization",
                        "Bearer a-secure-test-token-with-at-least-32-bytes",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            diagnostics_unavailable.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let rotation_status_list = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/federation/trust-rotations?phase=overlap&peer_cluster_id=target&limit=25&offset=0")
                    .header(
                        "authorization",
                        "Bearer a-secure-test-token-with-at-least-32-bytes",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rotation_status_list.status(), StatusCode::OK);

        let rotation_audit = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/federation/trust-rotations/rotation-1/audit?limit=10")
                    .header(
                        "authorization",
                        "Bearer a-secure-test-token-with-at-least-32-bytes",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rotation_audit.status(), StatusCode::OK);

        let recovery_unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/federation/trust-rotations/rotation-1/recover")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"request_id":"manual-recovery-1","expected_action":"forward_recovery"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recovery_unauthorized.status(), StatusCode::UNAUTHORIZED);
        let recovery_unavailable = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/federation/trust-rotations/rotation-1/recover")
                    .header(
                        "authorization",
                        "Bearer a-secure-test-token-with-at-least-32-bytes",
                    )
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"request_id":"manual-recovery-1","expected_action":"forward_recovery"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            recovery_unavailable.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        let metrics = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header(
                        "authorization",
                        "Bearer a-secure-test-token-with-at-least-32-bytes",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metrics.status(), StatusCode::OK);
        assert!(
            metrics
                .headers()
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .contains("text/plain")
        );
        let metrics_body = axum::body::to_bytes(metrics.into_body(), 64 * 1024)
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&metrics_body).contains("dataspool_scheduler_delay_seconds")
        );
        assert!(
            String::from_utf8_lossy(&metrics_body).contains("dataspool_federation_trust_rotations")
        );
        let metrics_text = String::from_utf8_lossy(&metrics_body);
        for metric in [
            "dataspool_federation_trust_rotation_recovery_attempts_total",
            "dataspool_federation_trust_rotation_terminal_duration_seconds_sum",
            "dataspool_federation_trust_rotation_phase_failure_ratio",
            "dataspool_federation_trust_rotation_phase_operations_by_rotation_total",
            "dataspool_federation_trust_rotation_recovery_attempts",
            "dataspool_federation_trust_rotation_overlap_remaining_seconds",
            "dataspool_federation_trust_rotation_bilateral_diagnostics_available",
            "dataspool_federation_trust_rotation_bilateral_state_aligned",
        ] {
            assert!(metrics_text.contains(metric), "missing metric {metric}");
        }

        let audit = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/audit?limit=5")
                    .header(
                        "authorization",
                        "Bearer a-secure-test-token-with-at-least-32-bytes",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(audit.status(), StatusCode::OK);
    }

    #[test]
    fn rejects_invalid_operator_credentials() {
        let dir = tempdir().unwrap();
        let store = RocksDbStore::open(dir.path()).unwrap();
        let tasks = Arc::new(TaskStore::new(store));
        assert!(DlqApi::new_with_operators(tasks.clone(), vec![]).is_err());
        assert!(
            DlqApi::new_with_operators(
                tasks.clone(),
                vec![(
                    "bad actor".into(),
                    "operator-token-with-at-least-32-bytes-123".into()
                )],
            )
            .is_err()
        );
        assert!(
            DlqApi::new_with_operators(
                tasks,
                vec![
                    (
                        "oncall".into(),
                        "operator-token-with-at-least-32-bytes-123".into()
                    ),
                    (
                        "backup".into(),
                        "operator-token-with-at-least-32-bytes-123".into()
                    ),
                ],
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_short_admin_tokens() {
        let dir = tempdir().unwrap();
        let store = RocksDbStore::open(dir.path()).unwrap();
        let tasks = Arc::new(TaskStore::new(store));
        assert!(DlqApi::new(tasks, "too-short").is_err());
    }
}
