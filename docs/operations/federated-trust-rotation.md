# Federated trust rotation operations

All endpoints below require the Dataspool admin bearer token and should remain on the loopback interface or behind a trusted TLS-terminating proxy. Prefer `DATASPOOL_ADMIN_OPERATORS` for individually attributable operators; see the Operator recovery UI section. The legacy single-token mode identifies all requests as `admin`.

## Lifecycle safety boundary

The bilateral activation barrier is the irreversible decision point. Once either Raft group commits AuthorizeActivation, rollback is no longer accepted, even if no CA activation expiry has yet been recorded. A concurrent unconfirmed rollback is promoted to RecoveryRequired and must proceed through forward recovery. This avoids a split decision where one cluster rolls back while the other is authorized to activate.

Before the activation barrier, rollback is permitted only after both clusters durably enter RollingBack and persist the peer confirmation. Certificate and trust-bundle cleanup is rejected until that bilateral confirmation exists. A confirmed rollback cannot be changed back into forward recovery.

Activation expiry values are absolute, persisted decisions. Recovery and request replay reuse those values; they never calculate a new overlap deadline.

## Inspect both clusters

    curl -sS \
      -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
      "http://127.0.0.1:8080/api/v1/federation/trust-rotations/$ROTATION_ID/diagnostics"

The response contains sanitized local and peer views, phase/revision differences, cross-cluster CA and leaf fingerprint checks, activation expiry comparisons, rollback confirmation state, safety violations, and a recommended recovery action. It intentionally excludes CA bundle PEM and private-key material.

The recommended_action field is one of:

- continue_staging: resume the already-created staging operation.
- resume_rollback: finish the pre-activation bilateral rollback protocol.
- forward_recovery: finish the irreversible activation path.
- retire_both: retire the previous CA on both clusters after the persisted overlap deadline.
- wait_for_overlap: retain both trust roots until the persisted deadline.
- await_operator: deployment readiness or explicit activation authorization is still required.
- no_action: both sides are terminal.
- manual_intervention_required: a safety invariant is violated; automated recovery is deliberately blocked.

The diagnostics response classifies progress with `progress_status`: `waiting` means a legitimate external/operator prerequisite or an unexpired overlap window; `progressing` means an automated transition is within its phase window; `automatic_recovery` means the durable audit stream shows a reconciler attempt within the last 15 seconds; `stalled` means a recoverable transition exceeded its phase-specific threshold; `manual_intervention_required` means a safety invariant is violated; and `terminal` means both sides are complete or rolled back. The response includes the conservative bilateral `last_progress_at_ms` (the older of the two Raft `updated_at_ms` values), `stalled_for_ms`, `stall_threshold_ms`, the latest audit action/outcome/time, `diagnosis_reason`, and `recovery_guidance`. Failed/rejected audit events explain failures but do not advance the durable progress watermark.

Stall thresholds are 60 seconds for Staging, 15 seconds for ActivationBarrier after the bilateral barrier is committed and for Activating/RecoveryRequired, 30 seconds for RollingBack, and 60 seconds for Retiring after both persisted overlap expiries have elapsed. AwaitingDeployment, an uncommitted ActivationBarrier, and an unexpired Overlap/Retiring window are waits, not stalls. The `stalled` boolean is true only when the current recommended action is automatically recoverable and the applicable threshold has elapsed. `state_aligned` is false when the two durable views differ, including expected partial-commit windows.

## Trigger safe recovery

    curl -sS -X POST \
      -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
      -H 'Content-Type: application/json' \
      -d '{"request_id":"operator-recovery-20261128-01","expected_action":"forward_recovery"}' \
      "http://127.0.0.1:8080/api/v1/federation/trust-rotations/$ROTATION_ID/recover"

The endpoint reads the persisted phase and invokes only the corresponding coordinator operation. It does not directly edit Raft state, skip deployment readiness, bypass the activation barrier, shorten the overlap, or force CA retirement. AwaitingDeployment and an uncommitted ActivationBarrier remain waiting states. If diagnostics report manual_intervention_required, this endpoint refuses to proceed.

Every recovery request must include a unique operator-supplied request ID (1-64 bytes). The operator identity and request ID are included in the replicated audit trail.

## Rotation audit history

    curl -sS \
      -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
      "http://127.0.0.1:8080/api/v1/federation/trust-rotations/$ROTATION_ID/audit?limit=100"

The endpoint returns the newest audit records for the exact federated_trust_rotation:$ROTATION_ID resource. Records include actor, request ID, action, previous/next phase, timestamp, and committed Raft log index. Each record also includes an `outcome` (`committed`, `failed`, or `rejected`) and optional bounded `details`. Successful lifecycle transitions are recorded atomically with their state mutation. Failed or rejected operator API operations are recorded by a separate audit-only Raft command: this preserves the current rotation state while assigning the audit record its actual committed Raft index. The original operator request ID and authenticated actor are retained. If the audit-only Raft proposal itself cannot commit, the recovery API logs the request ID and returns `503` rather than reporting a normal success; it does not fabricate a local record or index. The operator must inspect bilateral state and the Raft audit stream before retrying.

## Prometheus metrics

The authenticated /metrics endpoint exports:

- dataspool_federation_trust_rotations{phase} — rotation count by lifecycle phase.
- dataspool_federation_trust_rotation_stalled — locally persisted recoverable transitions beyond the phase-specific threshold (60s staging, 15s activation/recovery, 30s rollback, 60s post-deadline retirement). This aggregate metric uses the local durable record; the bilateral `/diagnostics` response is authoritative for operator decisions.
- dataspool_federation_trust_rotation_recovery_required — rotations requiring forward recovery.
- dataspool_federation_trust_rotation_errors — rotations with a persisted last error.
- dataspool_federation_trust_rotation_last_error{rotation_id} — per-rotation persisted-error indicator.
- dataspool_federation_trust_rotation_age_seconds{rotation_id,phase} — total time since rotation creation.
- dataspool_federation_trust_rotation_phase_age_seconds{rotation_id,phase} — time spent in the current phase.
- dataspool_federation_trust_rotation_overlap_remaining_seconds{rotation_id,phase} — remaining overlap for each rotation in Overlap or Retiring. The deadline is the later of the two persisted bilateral expiries, so zero means both sides' old-CA overlap windows have elapsed; the metric is omitted if either committed expiry is missing.
- dataspool_federation_trust_rotation_recovery_attempts_total / `...recovery_failures_total` — distinct persisted recovery request IDs and the subset that failed or were rejected. Successful manual recoveries are audited explicitly; background reconciliation successes are audited when they advance the persisted revision, and each failed reconciliation retry is recorded separately.
- dataspool_federation_trust_rotation_duration_seconds{rotation_id,terminal_state} — stable total duration for Completed and RolledBack rotations, calculated from persisted creation and terminal update timestamps. Active rotations continue to use `...age_seconds`.
- dataspool_federation_trust_rotation_terminal_duration_seconds_sum / `..._count` — aggregate duration sum and terminal rotation count, both rebuilt from persisted terminal records.
- dataspool_federation_trust_rotation_phase_operations_total, `...phase_failures_total`, and `...phase_failure_ratio`{phase} — distinct audited request IDs per phase aggregated across rotations. The corresponding `...phase_operations_by_rotation_total`, `...phase_failures_by_rotation_total`, and `...phase_failure_ratio_by_rotation` series carry `{rotation_id,phase}` labels. The failure ratio is `distinct failed/rejected request IDs / distinct observed request IDs` for that operation category. Replayed audit records with the same request ID are counted once; a request that failed and later succeeded still records that a failure occurred.

All history-derived values are recomputed from the replicated audit stream at scrape time. The implementation scans the audit keyspace once per scrape, groups rotation audit events by resource ID (including failed create attempts that never produced a rotation record), and uses request-ID sets to deduplicate replayed outcomes. These metrics survive process restart and do not depend on in-memory counters. Append-only event counts are exported as counters rebuilt from durable data; durations, ratios, and per-rotation snapshots are gauges. Counts include only audit events that successfully committed to Raft; if an audit-only proposal cannot commit, the service records an error log and that event is absent from derived counts. Legacy audit events from releases that only recorded the generic `advance` action are retained in the `lifecycle` phase bucket because their exact operation cannot be reconstructed reliably.

The ActivationBarrier and AwaitingDeployment phases are intentionally reported separately from stalled transitions because they may be waiting for explicit operator action or external certificate deployment.

## Alert rules

Load `deploy/prometheus/federation-trust-rotation-alerts.yml` into the Prometheus rule files and validate it with `promtool check rules` before enabling paging. The rules assume the authenticated Dataspool `/metrics` endpoint is scraped successfully and the scrape interval is no longer than 30 seconds. Alert labels include `service=dataspool` and `component=federation-trust-rotation`; every alert includes a runbook link.

| Alert | Trigger / debounce | Severity | Operator action |
| --- | --- | --- | --- |
| `DataspoolFederationTrustRotationStalled` | Aggregate phase-aware stalled count is nonzero for 1 minute | warning | Identify the rotation, inspect bilateral diagnostics and audit, then use only the recommended constrained recovery action |
| `DataspoolFederationTrustRotationNeedsForwardRecovery` | Persisted `RecoveryRequired` count is nonzero for 30 seconds | critical | Activation is irreversible; restore peer connectivity and complete forward recovery, never roll back |
| `DataspoolFederationTrustRotationRecoveryFailed` | A new durable recovery failure appears in the last 15 minutes, held for 1 minute | warning | Inspect the failed audit event and peer/Raft error before retrying |
| `DataspoolFederationTrustRotationOverlapExpiring` | Later bilateral expiry is between 1 and 6 hours, held for 5 minutes | warning | Verify new certificates and both peers; prepare retirement without shortening overlap |
| `DataspoolFederationTrustRotationOverlapCritical` | Later bilateral expiry is within 1 hour, held for 1 minute | critical | Restore peer reachability and prepare safe retirement; never remove the old CA early |
| `DataspoolFederationTrustRotationRetirementOverdue` | Expiry is zero and phase age exceeds 5 minutes, held for 2 minutes | critical | Inspect both persisted records; use `retire_both` only when bilateral diagnostics recommend it |
| `DataspoolFederationTrustRotationExpiryMissing` | Overlap/Retiring has no bilateral expiry metric for 2 minutes | critical | Treat as unsafe; do not retire either CA and compare both Raft records |
| `DataspoolFederationTrustRotationBilateralMismatch` | Bilateral state alignment is false for 3 minutes while the peer probe succeeds | critical | Inspect phase/revision, fingerprints, expiry and acknowledgements; transient commit windows are debounced |
| `DataspoolFederationTrustRotationBilateralDiagnosticsUnavailable` | Peer diagnostics probe fails for 2 minutes | warning | Check peer gRPC/mTLS, registration and Raft availability; unavailable is not aligned |
| `DataspoolFederationTrustRotationAwaitingOperator` | ActivationBarrier phase age exceeds 15 minutes, held for 5 minutes | warning | Review deployment evidence and explicitly authorize activation or perform safe pre-activation rollback |

Recovery failures use `increase(...recovery_failures_total[15m])`, not a raw `> 0` check: the metric is reconstructed from the append-only Raft audit stream and therefore retains historical failures across restarts. The old `last_error` gauge is intentionally not paged on because a persisted historical error can remain after the underlying condition has recovered.

The bilateral metrics `...bilateral_diagnostics_available` and `...bilateral_state_aligned` are emitted per active rotation. The metrics probe reads both Raft views without scanning audit history, runs with at most eight concurrent peer requests, and applies a two-second timeout per request. Terminal rotations are excluded from peer probes to avoid repeatedly querying immutable historical records. A failed or timed-out probe emits availability `0` and omits the alignment sample; it never reports unavailable state as aligned. If the coordinator is not configured, availability is `0` for each known rotation. The `/diagnostics` endpoint remains the authoritative operator view and includes the audit-derived recovery guidance.

`RetirementOverdue` deliberately waits until the later persisted expiry is zero and the rotation has remained in Overlap/Retiring for at least five minutes, avoiding an alert merely because the overlap window has just elapsed. `ExpiryMissing` is separate because the absence of either persisted expiry must block retirement, not be interpreted as an elapsed deadline. `BilateralMismatch` waits three minutes to absorb ordinary short-lived cross-cluster commit windows; the alert must never trigger an automatic recovery action.

Example validation:

    promtool check rules deploy/prometheus/federation-trust-rotation-alerts.yml

## Query rotation status

The existing collection and single-rotation endpoints remain the canonical status APIs:

    curl -sS -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
      "http://127.0.0.1:8080/api/v1/federation/trust-rotations?phase=overlap&peer_cluster_id=target&limit=50&offset=0"

The collection response remains a JSON array for compatibility. Results are ordered by most recently updated first. Supported phase filters are staging, awaiting_deployment, activation_barrier, activating, recovery_required, overlap, retiring, completed, rolling_back, and rolled_back. The default page size is 100 and the maximum is 500.

    curl -sS -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
      "http://127.0.0.1:8080/api/v1/federation/trust-rotations/$ROTATION_ID"

This single-resource endpoint returns the local durable rotation record. Use the existing /diagnostics endpoint when bilateral Raft state, peer certificate fingerprints, expiry drift, and safety violations are required. A peer read failure is returned as a gateway error; the API does not silently downgrade a bilateral query to local-only data.

## Operator recovery UI

Open `http://127.0.0.1:8080/admin/trust-rotations` from the trusted admin network. The page itself is a static shell; all data requests require a bearer token. Configure individual credentials with `DATASPOOL_ADMIN_OPERATORS` as a JSON object mapping immutable operator IDs to unique high-entropy tokens (each at least 32 bytes), for example `{"oncall-platform":"<token-1>","security-duty":"<token-2>"}`. The UI calls `GET /api/v1/session` after authentication and displays the actor identity resolved by the server. The authenticated actor is derived from the matching bearer credential; any caller-supplied `X-Dataspool-Actor` header is overwritten by the authentication middleware and cannot choose the audit identity. The legacy `DATASPOOL_ADMIN_TOKEN` remains supported as a single `admin` principal for compatibility, but it cannot distinguish individual operators. Prefer per-operator credentials in production and rotate/revoke them through deployment secret management.

The registry uses the dedicated `GET /api/v1/federation/trust-rotations/operator-view` projection. It intentionally omits both CA bundle PEM fields and exposes only public fingerprints, lifecycle flags, revisions and expiry metadata. Each visible row loads bilateral diagnostics with bounded client concurrency. Selecting a rotation loads the authoritative bilateral view and up to 100 audit events, including actor, request ID, outcome and committed Raft index.

The UI enables recovery only when `recommended_action` is automatically recoverable, `automated_recovery_safe` is true, and `safety_violations` is empty. An operator must type the exact rotation ID and confirm the operation. The browser submits only `request_id` and the observed `expected_action` to the existing `POST /recover` endpoint. The server independently re-reads bilateral diagnostics and remains the sole authority; stale or unsafe submissions are rejected and audited. A successful lifecycle transition is not reported as a fully successful operator request unless its explicit `committed` outcome event is also appended to Raft. If that audit append fails, the API returns `503` with an indeterminate-outcome message; inspect bilateral state and the audit stream before retrying.

If either side has committed `ActivationBarrier`, the UI displays an irreversible-boundary warning and never offers rollback. The server-side coordinator remains authoritative even if a browser is stale, modified or bypassed. The UI does not call deployment-ready, authorize-activation, activate, retire or rollback endpoints.

## Live deployment smoke check

After building the deployable binary, run `scripts/smoke_trust_rotation_deployment.sh`. It starts an isolated one-node Raft Dataspool process with a temporary self-signed CA/server certificate, enables the real mTLS gRPC listener and federation coordinator, and verifies the HTTP operator UI, anonymous rejection, per-operator bearer authentication, actor-header spoofing resistance, sanitized registry, and `Cache-Control: no-store`. It then sends a deliberately invalid recovery request and reads back the failure event from the live Raft audit endpoint, checking the authenticated actor, request ID, outcome and nonzero committed log index. The test does not create or mutate a real certificate rotation; bilateral success/failure paths remain covered by the federation process E2E suite.

The legacy `DATASPOOL_ADMIN_TOKEN` mode maps all requests to the `admin` principal and therefore cannot provide individual attribution. Production deployments should use distinct `DATASPOOL_ADMIN_OPERATORS` tokens and protect the UI/API with TLS and network access controls. Tokens are deployment secrets; never commit the JSON value to configuration files.

## Confirm a manual recovery

The existing POST /recover endpoint now requires the action observed in the latest diagnostics:

    curl -sS -X POST \
      -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
      -H 'Content-Type: application/json' \
      -d '{"request_id":"operator-recovery-20261128-02","expected_action":"forward_recovery"}' \
      "http://127.0.0.1:8080/api/v1/federation/trust-rotations/$ROTATION_ID/recover"

The request is rejected with HTTP 409 if the expected action is stale, automated recovery is not safe, or either cluster reports a safety violation. The coordinator independently re-reads bilateral diagnostics and checks the expected action before acting, so a stale operator view cannot authorize a different recovery path. The response contains both the before and after bilateral diagnostics. The operator identity is resolved from the bearer token by the server authentication middleware; `X-Dataspool-Actor` is overwritten and never trusted as an identity source. `GET /api/v1/session` returns the authenticated actor used by the UI. The request ID is included in replicated lifecycle audit events.

Manual recovery cannot select an arbitrary transition. It cannot force rollback after activation, authorize activation while deployment readiness is missing, or retire a CA before the persisted overlap deadline. If diagnostics recommend await_operator or manual_intervention_required, the recovery endpoint refuses the request.
