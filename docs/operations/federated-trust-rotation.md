# Federated trust rotation operations

All endpoints below require the Dataspool admin bearer token and should remain on the loopback interface or behind a trusted TLS-terminating proxy.

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

The stalled field becomes true when a recoverable transitional phase has not been updated for at least 30 seconds. state_aligned is false when the two durable views differ, including expected partial-commit windows.

## Trigger safe recovery

    curl -sS -X POST \
      -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
      -H 'Content-Type: application/json' \
      -d '{"request_id":"operator-recovery-20261128-01"}' \
      "http://127.0.0.1:8080/api/v1/federation/trust-rotations/$ROTATION_ID/recover"

The endpoint reads the persisted phase and invokes only the corresponding coordinator operation. It does not directly edit Raft state, skip deployment readiness, bypass the activation barrier, shorten the overlap, or force CA retirement. AwaitingDeployment and an uncommitted ActivationBarrier remain waiting states. If diagnostics report manual_intervention_required, this endpoint refuses to proceed.

Every recovery request must include a unique operator-supplied request ID (1-64 bytes). The operator identity and request ID are included in the replicated audit trail.

## Rotation audit history

    curl -sS \
      -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
      "http://127.0.0.1:8080/api/v1/federation/trust-rotations/$ROTATION_ID/audit?limit=100"

The endpoint returns the newest audit records for the exact federated_trust_rotation:$ROTATION_ID resource. Records include actor, request ID, action, previous/next phase, timestamp, and committed Raft log index. Each record also includes an `outcome` (`committed`, `failed`, or `rejected`) and optional bounded `details`. Successful lifecycle transitions are recorded atomically with their state mutation. Failed or rejected operator API operations are recorded by a separate audit-only Raft command: this preserves the current rotation state while assigning the audit record its actual committed Raft index. The original operator request ID and authenticated actor are retained. If the audit-only Raft proposal itself cannot commit, the API logs that audit persistence failure; it does not fabricate a local record or index.

## Prometheus metrics

The authenticated /metrics endpoint exports:

- dataspool_federation_trust_rotations{phase} — rotation count by lifecycle phase.
- dataspool_federation_trust_rotation_stalled — stale recoverable transitions (30-second threshold).
- dataspool_federation_trust_rotation_recovery_required — rotations requiring forward recovery.
- dataspool_federation_trust_rotation_errors — rotations with a persisted last error.
- dataspool_federation_trust_rotation_last_error{rotation_id} — per-rotation persisted-error indicator.
- dataspool_federation_trust_rotation_age_seconds{rotation_id,phase} — total time since rotation creation.
- dataspool_federation_trust_rotation_phase_age_seconds{rotation_id,phase} — time spent in the current phase.
- dataspool_federation_trust_rotation_overlap_remaining_seconds{rotation_id,phase} — remaining overlap for each rotation in Overlap or Retiring.

The ActivationBarrier and AwaitingDeployment phases are intentionally reported separately from stalled transitions because they may be waiting for explicit operator action or external certificate deployment.

## Alert rules

The Prometheus rule group in deploy/prometheus/federation-trust-rotation-alerts.yml covers stalled transitions, forward-recovery requirements, persisted errors, overlap expiry, overdue retirement, and long waits at the activation barrier. Review the thresholds against the expected deployment and overlap windows before enabling paging.

## Query rotation status

The existing collection and single-rotation endpoints remain the canonical status APIs:

    curl -sS -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
      "http://127.0.0.1:8080/api/v1/federation/trust-rotations?phase=overlap&peer_cluster_id=target&limit=50&offset=0"

The collection response remains a JSON array for compatibility. Results are ordered by most recently updated first. Supported phase filters are staging, awaiting_deployment, activation_barrier, activating, recovery_required, overlap, retiring, completed, rolling_back, and rolled_back. The default page size is 100 and the maximum is 500.

    curl -sS -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
      "http://127.0.0.1:8080/api/v1/federation/trust-rotations/$ROTATION_ID"

This single-resource endpoint returns the local durable rotation record. Use the existing /diagnostics endpoint when bilateral Raft state, peer certificate fingerprints, expiry drift, and safety violations are required. A peer read failure is returned as a gateway error; the API does not silently downgrade a bilateral query to local-only data.

## Confirm a manual recovery

The existing POST /recover endpoint now requires the action observed in the latest diagnostics:

    curl -sS -X POST \
      -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
      -H 'Content-Type: application/json' \
      -d '{"request_id":"operator-recovery-20261128-02","expected_action":"forward_recovery"}' \
      "http://127.0.0.1:8080/api/v1/federation/trust-rotations/$ROTATION_ID/recover"

The request is rejected with HTTP 409 if the expected action is stale, automated recovery is not safe, or either cluster reports a safety violation. The coordinator independently re-reads bilateral diagnostics and checks the expected action before acting, so a stale operator view cannot authorize a different recovery path. The response contains both the before and after bilateral diagnostics. The operator identity is derived from the authenticated request and the request ID is included in replicated lifecycle audit events.

Manual recovery cannot select an arbitrary transition. It cannot force rollback after activation, authorize activation while deployment readiness is missing, or retire a CA before the persisted overlap deadline. If diagnostics recommend await_operator or manual_intervention_required, the recovery endpoint refuses the request.
