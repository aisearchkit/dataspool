# Elasticsearch migration production operations

## Adaptive Bulk workers

`DocumentSink` splits an incoming batch into independently acknowledged Bulk requests. The worker window is bounded by `BackpressureConfig::max_in_flight`; the AIMD controller increases concurrency after healthy responses and reduces batch size/concurrency on 429/rejection, high write-queue pressure, or excessive request latency.

The sink samples `GET /_nodes/stats/thread_pool` before each batch. `DocumentSink::node_pressure()` exposes the last aggregate node count, write active/queue/rejected counters, per-node pressure samples and queue pressure ratio. The `rejected` value returned by Elasticsearch is a process-lifetime counter, so it is diagnostic only; the controller uses per-request rejection counts to avoid remaining throttled after one historical 429.

Tune `min_batch_size`, `max_batch_size`, `initial_batch_size`, `min_in_flight`, `max_in_flight`, and `target_latency` based on heap, shard count, ingest pipeline cost and network RTT. Do not set a high worker count without testing against the target cluster.

## Dead Letter Queue

Permanent document-level Bulk failures (for example `mapper_parsing_exception`) are written to the RocksDB `dead_letter` column family. The queue is namespaced by shard and stores the original record, status, error type, reason, attempt count and creation timestamp. Its key is a SHA-256 identity derived from record identity/content and failure classification, making enqueue idempotent if a process crashes after DLQ persistence but before checkpoint/cache acknowledgement.

Example operator flow from an agent/admin component:

```rust,ignore
let dlq = DeadLetterQueue::new(Arc::new(task_store.kv_store().clone()), shard_id);
let pending = dlq.list(100).await?;       // inspect mapping failures
let replayed = dlq.replay(&mut sink, &selected_ids).await?;
// replay removes only successfully written entries; failures remain and increment attempts
```

Call `sink.connect()` before replay. The library rejects an empty ID list, so replay is always explicit and bounded to selected entries. `remove(id)` is an explicit operator discard operation; use it only after reviewing the entry.

A secured bearer-token HTTP API and operator page are available through `DlqApi`. See [DLQ administration API](dlq-admin-api.md) for endpoints, authentication, and UI usage. The API requires an administrator token of at least 32 bytes; expose it only behind TLS and network access controls.

## CDC and cutover safety

Elasticsearch does not provide a generic native document CDC stream. `ElasticsearchCdcSource` consumes an append-only change index populated by an application outbox or external CDC connector. The producer must guarantee a globally unique, strictly increasing sequence and retain events until every migration consumer has advanced past them.

Before alias cutover:

1. Establish a source write barrier or equivalent final watermark.
2. Read and apply CDC events through that exact watermark.
3. Verify the target applied watermark is at least the source watermark.
4. Call `cutover_with_raft`; it refuses to switch when the watermark is behind, uses one atomic `_aliases` request, and commits the Raft cutover record.
5. Release the write barrier only after the Raft record succeeds.

For PostgreSQL applications, use the `PostgresTransactionalOutbox` and `PostgresOutboxSource` described in [PostgreSQL Transactional Outbox](../postgres-transactional-outbox.md). `cutover_with_raft` retains the durable pause if the alias has switched but the Raft result has not committed. Without a write barrier/final watermark protocol, a producer can append events between watermark sampling and alias switch; the helper cannot make that external race safe by itself.

## Compatibility test matrix

Run the Docker matrix with:

```bash
docker compose -f deploy/compose/elasticsearch-compat.yml up -d --wait
./tests/run_es_compat.sh
docker compose -f deploy/compose/elasticsearch-compat.yml down -v
```

Or provide `ES_7102_URL`, `ES_717_URL`, `ES_8X_URL`, and `ES_9X_URL` separately. The test validates version detection, per-item mapping failures, PIT/search_after and `DocumentSource` restart recovery, CDC index/delete events, alias cutover/watermark guard, concurrent Bulk delivery and an overload probe that reports observed HTTP 429 responses. A zero 429 count means the cluster did not reject during that probe; it is not evidence that the 429 path was exercised.

The local compose configuration intentionally uses a one-entry write queue to make overload more reproducible and disables disk threshold enforcement only for disposable test clusters. Never copy those settings into production.

## Snapshot plus CDC task configuration

The `elasticsearch_online` source runs a snapshot first and then tails the append-only change index. It captures the change-log maximum sequence before opening the snapshot and persists both phase and cursor in the shard checkpoint. See [online migration design](../elasticsearch-online-migration.md) for the producer ordering contract and the full YAML example. This is a continuous source: the worker remains active when the change-log backlog is empty. It does not imply native Elasticsearch CDC; a transactional outbox or external CDC producer is still required.

## Graceful worker shutdown

The worker pool supports cooperative shutdown through a `CancellationToken`:

```rust,ignore
let shutdown = CancellationToken::new();
let worker_shutdown = shutdown.clone();
let worker = tokio::spawn(async move {
    pool.run_until_cancelled(worker_shutdown).await
});

// On SIGTERM / deployment shutdown:
shutdown.cancel();
worker.await??;
```

Shutdown stops new shard claims, signals active executors, and waits for them to exit. Executors do not cancel an in-flight destination write or its checkpoint/cache-ack sequence. They stop before the next source read (or during the idle CDC poll), close both drivers, and return the shard to `Pending` with its last committed offset so another worker can resume it. The process supervisor should still enforce an overall termination deadline.


## Running the Agent and graceful process shutdown

The application binary can start the Agent worker pool alongside the DLQ admin API. Provide a configuration file with `DATASPOOL_CONFIG`; a one-member Raft cluster is suitable for development, while multi-node deployments use the same membership on every node:

```bash
export DATASPOOL_CONFIG=/etc/dataspool/dataspool-cluster.yaml
export DATASPOOL_ADMIN_TOKEN="$(openssl rand -hex 32)"
export DATASPOOL_DB_PATH=/var/lib/dataspool  # optional; overrides storage.data_dir
export DATASPOOL_HTTP_BIND=127.0.0.1:8080    # optional; overrides server.bind_addr/http_port
./dataspool
```

Without `DATASPOOL_CONFIG`, the binary remains in its backwards-compatible admin-API-only mode. Set `agent.enabled: false` for control/API-only operation. `DATASPOOL_CONFIG` uses the YAML settings for node identity, storage path, HTTP listener, and Agent concurrency.

**Raft cluster configuration:** Agent shard claims, lease renewals, releases, progress checkpoints, and completion transitions are committed through OpenRaft. Configure `cluster.peers` as `node-id@host:port` entries excluding the local node; every node must use the same initial membership and `DATASPOOL_RAFT_TOKEN` (at least 32 bytes). Set `DATASPOOL_RAFT_ADVERTISE_ADDR` to a routable address when `server.bind_addr` is a wildcard. The Raft RPC listener uses HTTP on `server.grpc_port`, authenticates every RPC with the shared token, and must be restricted to the private cluster network. It does not provide TLS; use a private encrypted network or a trusted TLS tunnel. The binary currently supports RocksDB only.

Shard ownership uses a 30-second lease and a monotonically increasing fencing generation. Any Raft member can claim and renew work: followers forward proposals to the current leader, then wait until their local state machine has applied the committed lease before executing. Workers renew every 8 seconds; failure to renew or a stale fencing generation cancels the worker. Checkpoints, cache batches, dead-letter writes, cache acknowledgements and terminal-state transitions are replicated Raft commands checked against the current owner and fencing generation. Source batches are committed to the replicated RocksDB-backed cache before destination delivery; a new owner replays unacknowledged batches before reading further source data. Do not manually reset `Running` shards: an expired lease is reclaimed by a committed claim, which increments the fencing generation. Keep node clocks synchronized (NTP), since lease deadlines are wall-clock timestamps carried in deterministic Raft commands.

The Elasticsearch `DocumentSink` enforces destination fencing with Bulk API `version` and `version_type=external_gte`; leased writes require stable document IDs. The external version is monotonic across lease generations for a shard, so a delayed write from an older owner cannot overwrite a newer owner's write. Other sink implementations fail closed until they implement `write_batch_fenced`; a Raft lease alone cannot fence an arbitrary external destination. Ensure each document ID is owned by one migration shard, and do not mix unrelated writers that use higher Elasticsearch external versions for the same IDs.

On restart, interrupted shards remain `Running` until their lease expires. A new committed claim then increments the fencing token and preserves the last committed source offset. Set a stable, non-empty `node.id` across restarts; an empty ID falls back to a hostname-derived ID.

On SIGINT or SIGTERM, the HTTP server stops accepting new requests and drains active requests. The worker pool stops claiming shards, lets in-flight destination writes and checkpoint acknowledgements finish, closes drivers, and waits for workers to exit. Configure the service manager's termination grace period longer than the maximum expected sink write/flush duration; a forced kill can still interrupt an in-flight operation.

## Federation chaos and recovery validation

The federation integration suite includes a deterministic chaos scheduler on top of two independent three-member Raft groups and a real mTLS gRPC connection routed through a bidirectional TCP fault proxy. `ChaosConfig` bounds the number of rounds and maximum injected delay and assigns weights to partition/heal, commit-ACK loss, independent follower crash/restart, and duplicate-reconcile events. Crash and restart are separate scheduled actions, so a member can remain offline across intervening reconciliation rounds. The xorshift64* scheduler is deterministic across platforms. The scenario accepts `FEDERATION_CHAOS_SEED`, `FEDERATION_CHAOS_ROUNDS` (1-512), and `FEDERATION_CHAOS_MAX_DELAY_MS` (0-5000) so a fixed seed and bounded configuration can be replayed without changing source code. When `FEDERATION_CHAOS_ARTIFACT_DIR` is set, the test continuously writes an atomic JSON artifact containing the seed, full event sequence, scheduler configuration, replay command, and logical state snapshots for all three Source and all three Target Raft members. Each member snapshot includes its running/offline status, OpenRaft metrics, migration record, remote-operation record, and task records. The artifact is refreshed after each injected event and after terminal/fencing checks, so a panic leaves the last completed checkpoint available.

The independent `.github/workflows/federation-chaos-nightly.yml` workflow runs eight deterministic seeds nightly (or a caller-supplied comma-separated seed list on manual dispatch), with 64 rounds per seed by default. It continues through the remaining seeds after a failure and uploads per-seed logs plus JSON state snapshots as a 90-day GitHub Actions artifact. This workload is separate from PR CI. To replay a captured seed locally or in a one-off CI job:

```bash
export FEDERATION_CHAOS_SEED=0xDADA202611240001
export FEDERATION_CHAOS_ROUNDS=64
export FEDERATION_CHAOS_MAX_DELAY_MS=40
export FEDERATION_CHAOS_ARTIFACT_DIR=artifacts/federation-chaos
cargo test --lib --all-features \
  comms::server::federation_e2e_tests::federation_mtls_recovery_survives_two_source_and_target_leader_changes \
  -- --exact --nocapture
```

The in-doubt commit scenario deliberately keeps the source migration in `Committing`: an isolated link cannot reach the target, while an available link drops the Commit response only after the target has durably committed. This exercises retries against the target's idempotent durable operation record rather than treating a successful transport response as the commit point. The suite verifies that source and target Raft members converge, the target provisions exactly one task, and stale fencing tokens cannot mutate shard progress. The scheduler is bounded for normal CI; longer randomized soak runs should use a separate scheduled workflow rather than slowing every pull request.

These tests validate the exercised recovery paths, not every possible distributed execution ordering and not a generic cross-cluster ACID transaction. Elasticsearch writes already acknowledged by a sink are not automatically reversible; compensation remains sink-specific.

## Distributed task aggregation and recovery

The replicated shard record is the source of truth for task progress. Every committed shard claim, checkpoint, completion, failure, lease release, and node-offline requeue refreshes the task aggregate in the same application-store batch as the corresponding shard mutation. The aggregate includes pending/running/completed/failed counts, records and serialized record bytes, elapsed time, document/byte throughput, per-node ownership-state counts and processed-document/byte throughput, and each shard's last committed offset/fencing generation. Per-node document/byte totals are attributed to the shard's last committed owner; task-wide totals remain authoritative across reassignment.

Read a task with a fresh projection through TaskStore::get_task_with_aggregate; the cached TaskMeta.aggregate is also persisted for efficient consumers. TaskStore::rebuild_task_aggregates reconstructs it from shard records after startup or snapshot installation. Do not use task counters as an independent source of truth or increment them from worker-local events.

A shard's last owner is retained in ShardMeta.last_owner_node_id, so completed/failed work remains attributable after its lease is cleared. A later claim transfers current shard attribution to the new owner; the shard checkpoint and total processed counters remain intact.

The Raft recovery integration test exercises a three-member cluster: it isolates a follower from inbound Raft traffic, commits additional task state on the remaining quorum, creates and purges a snapshot, reconnects the follower and verifies snapshot catch-up, then shuts down the leader and verifies that the surviving quorum elects a replacement leader and commits new work. Duplicate completion and failure proposals are checked for idempotency. Snapshot restoration also rebuilds the aggregate from committed shard metadata rather than trusting the serialized projection.

## TaskService gRPC query API

The control-plane TaskService runs independently from the Raft transport. server.grpc_port remains the authenticated internal Raft HTTP RPC listener for compatibility; server.task_api_port is the TaskService gRPC listener (default 50052). server.task_api_bind_addr defaults to 127.0.0.1 because the current TaskService does not yet implement native authentication or TLS. Keep it loopback-only, or expose it only through a trusted TLS/authentication proxy. The corresponding environment overrides are DATASPOOL_SERVER_TASK_API_PORT and DATASPOOL_SERVER_TASK_API_BIND_ADDR.

TaskService.GetTask accepts a UUID task_id and returns:
- The live task state and aggregate shard counts (pending/running/completed/failed).
- Processed documents/bytes, elapsed time, and document/byte throughput.
- Per-node shard counts, processed totals, and throughput.
- Every shard's current state, last owner, checkpoint offset (JSON-encoded), fencing token, lease expiry, processed documents/bytes, and update timestamp.

In Raft mode, the service accepts reads only on the elected leader and runs OpenRaft's quorum-confirmed linearizable read barrier before accessing storage. A follower returns FAILED_PRECONDITION with leader_id; a missing leader or lost quorum returns UNAVAILABLE. In non-Raft admin-only mode, the service reads the local store. The service computes the aggregate from the same shard scan used to construct the returned shard list; it does not trust the cached TaskMeta.aggregate projection. Invalid IDs return INVALID_ARGUMENT, unknown tasks return NOT_FOUND, and storage failures return INTERNAL. ShardInfo.total_records is zero when the source total is unknown; ShardMeta currently stores processed counts but not an independent source cardinality estimate.

Example (on the local node):

```bash
grpcurl -plaintext -d '{"task_id":"<task-uuid>"}' \
  127.0.0.1:50052 dataspool.task.TaskService/GetTask
```

Clients can also use the existing GrpcClient::task() generated client accessor. The gRPC server is started and gracefully stopped by the main Dataspool process alongside the HTTP and Raft listeners.

## Scheduler and Operational Control (Phase 2.7)

The worker-side scheduler evaluates replicated node metadata before proposing a claim. It excludes paused/offline nodes, enforces `capacity.max_concurrent` and `capacity.memory_mb`, matches task placement constraints expressed as `requires.<tag>=<value>` in task tags, reserves `requires.memory_mb` against active leases, and selects the least-utilized eligible node using a deterministic concurrency-plus-memory utilization score. Task candidates are ordered by priority (Critical, High, Normal, Low), then FIFO by creation time. Configure node labels with `agent.tags` (for example `region=us-west`, `storage=ssd`; a bare tag is stored as `tag=true`); Dataspool adds the two capacity tags from `agent.resources` at registration. If no node records exist (standalone compatibility mode), the local worker retains the legacy claim behavior.

`TaskService.ListTasks` provides leader-only linearizable listing with 1-based pagination, state and tag filters, and `created_at`, `priority`, or `name` ordering. Page sizes default to 20 and are capped at 100.

Example:

```bash
grpcurl -plaintext -d '{"page":1,"page_size":20,"state":"running","tag_key":"environment","tag_value":"prod","sort_by":"priority","descending":true}' \
  127.0.0.1:50052 dataspool.task.TaskService/ListTasks
```


Control operations write audit events in the same RocksDB batch as their state changes and idempotency keys. Events include the committed Raft log index, actor, request ID, operation, resource, previous/new state, and timestamp. gRPC clients may supply `x-dataspool-actor`; absent metadata is recorded as `anonymous`. Only trust this value when the Task API is behind an authenticated proxy that strips caller-supplied copies and injects the verified identity. Audit events are available from the authenticated HTTP endpoint `GET /api/v1/audit?limit=100` (maximum 1000), newest Raft index first.

The authenticated Prometheus text endpoint is `GET /metrics`. It exposes task/shard/node state gauges, oldest pending shard age (`dataspool_scheduler_delay_seconds`), per-node active shard and configured capacity gauges, reserved versus configured memory, and cumulative committed retry counts. Scrapers must send the DLQ admin bearer token configured as `DATASPOOL_ADMIN_TOKEN`.

## Distributed Task Control Plane

Task mutations are submitted as OpenRaft client writes. The gRPC handlers do not mutate RocksDB directly: CreateTask, PauseTask, ResumeTask, CancelTask, RetryTask, and ControlNodeExecution all call `RaftNode::propose`; the deterministic state machine applies the committed command to replicated metadata. Mutations are unavailable when Raft is not configured, rather than silently falling back to a local write.

The TaskService exposes these operations:

- `CreateTask`: requires a non-empty `request_id`; its task UUID is deterministically derived from that key, so client retries address the same task. Reusing the key with a different task payload is rejected.
- `PauseTask` / `ResumeTask`: pause transitions to `Pausing` while live leases drain. Workers receive the replicated state change, finish the in-flight sink write plus checkpoint/ack sequence, release their lease, and only then does the task become `Paused`. New source batches are rejected during draining; existing leases continue to renew until the in-flight batch is checkpointed and the worker releases its lease. Resume reopens claims.
- `CancelTask`: marks the task terminal and propagates through worker polling. New claims and source batches stop. Existing leases continue to renew while workers finish an already committed batch. A source batch already committed to the replicated cache may finish its destination write and checkpoint/ack sequence before the worker releases its lease; this preserves the restart checkpoint while preventing additional work.
- `RetryTask`: requeues failed shards and preserves their durable offsets and processed counters. Completed shards are not rerun.
- `ControlNodeExecution`: `paused=true` prevents that node from claiming new shards. Existing leases may finish or expire; heartbeat traffic does not implicitly resume a manually paused node. `paused=false` resumes claims.

All control requests require a caller-generated `request_id` (1-128 characters). The replicated state machine records request IDs and rejects reuse with a different operation/payload; repeated committed requests are acknowledged without repeating the mutation. Task and node control responses indicate whether the command was accepted by the committed state machine. A successful gRPC transport alone is not a substitute for checking the response `success` field.

Example:

```bash
grpcurl -plaintext -d '{"task_id":"<task-uuid>","request_id":"pause-<unique-id>"}' \
  127.0.0.1:50052 dataspool.task.TaskService/PauseTask

grpcurl -plaintext -d '{"node_id":"worker-1","paused":true,"request_id":"node-pause-<unique-id>"}' \
  127.0.0.1:50052 dataspool.task.TaskService/ControlNodeExecution
```


## Task SLA alerts (Phase 2.8)

Task metadata supplied at creation can define an SLA policy. The fields are replicated with the task, so every Raft member evaluates the same policy against committed task/shard state:

- `sla.deadline_at`: absolute RFC3339 deadline, for example `2026-12-01T18:00:00Z`.
- `sla.max_duration_seconds`: maximum elapsed execution duration.
- `sla.max_retries`: retry budget across all task shards.

The authenticated `GET /api/v1/alerts/sla` endpoint returns derived alerts. Deadlines within 15 minutes, duration budgets within 15 minutes, and exhausted retry budgets are reported as `at risk`; exceeded budgets are `breached`. Completed and cancelled tasks are excluded. Invalid policy values are ignored rather than blocking task execution. Prometheus exposes `dataspool_sla_tasks_at_risk` and `dataspool_sla_tasks_breached`; alert delivery (PagerDuty, email, webhook) should be handled by the Prometheus Alertmanager rule path so notification retries and deduplication remain outside the Raft state machine.

### Recovery and multi-cluster deployment boundary

Each Dataspool Raft group is an independent consistency domain. A task can read from and write to Elasticsearch clusters across regions, but Raft membership, task metadata, leases, and checkpoints are not shared between independent Dataspool clusters. Do not configure two Raft groups to write the same task/shard namespace or assume that a remote cluster's task status is covered by the local quorum. For multi-cluster operations, use a single orchestration/control cluster to own the migration task and connect it to remote source/destination data clusters; keep the worker-to-control-plane path routable during leader changes.

During a Raft leader failure, workers continue to hold their last committed leases only until expiry. They retry proposals through the elected leader; a new claim is committed by the new leader and increments the fencing generation before a replacement worker resumes from the last committed checkpoint. This is at-least-once execution with destination fencing, not a promise of uninterrupted writes during election. Keep the 30-second lease longer than the expected election plus transient network recovery window, and alert on `dataspool_scheduler_delay_seconds` and expired running leases. A follower without quorum must not locally reassign work.

Example task tags for a 2-hour execution budget and retry budget of 5:

```json
{
  "sla.max_duration_seconds": "7200",
  "sla.max_retries": "5",
  "sla.deadline_at": "2026-12-01T18:00:00Z"
}
```

Prometheus rule example:

```yaml
groups:
  - name: dataspool-task-sla
    rules:
      - alert: DataspoolTaskSlaBreached
        expr: dataspool_sla_tasks_breached > 0
        for: 1m
        labels:
          severity: critical
      - alert: DataspoolTaskSlaAtRisk
        expr: dataspool_sla_tasks_at_risk > 0
        for: 5m
        labels:
          severity: warning
```

## Federated cluster registry and migration orchestration (Phase 2.8)

The local Raft group is authoritative for registry, migration, remote-operation and audit records. All state changes are Raft commands persisted atomically with their idempotency keys. Snapshot/restore includes these records. Only the current Raft leader runs the reconciliation loop and performs remote side effects; a replacement leader replays generation-scoped operations and reconciles target state before advancing a migration.

### Register a remote cluster

`POST /api/v1/federation/clusters` (administrator bearer token):

```json
{
  "cluster_id": "west-prod",
  "endpoint": "https://west-control.example.com:9443",
  "identity_fingerprint": "<64-character SHA-256 fingerprint of the leaf client certificate DER>",
  "version": "0.1.0",
  "capabilities": ["elasticsearch-source", "elasticsearch-sink"],
  "request_id": "register-west-prod-001"
}
```

Federation peer endpoints must be HTTPS origins. HTTP is accepted only for loopback development. The first registration pins the cluster ID to its SHA-256 certificate fingerprint; subsequent identity changes must use the certificate rotation protocol below. Each cluster must register the other cluster's client certificate fingerprint, because both outbound and inbound peer authentication are mutual. Configure `server.tls.enabled`, `server.tls.cert_file`, `server.tls.key_file`, and `server.tls.ca_file` on each cluster. The CA file is used to require and validate client certificates. Set `DATASPOOL_FEDERATION_CLUSTER_ID` to enable the leader-driven orchestrator. The remote endpoint hostname must match the peer server certificate SAN.

### Federated certificate rotation

Rotation is performed independently in each direction: the receiving cluster stores the fingerprint of the remote cluster's client certificate. The old certificate must remain available until the peer has staged the replacement and the new certificate has proved possession. The protocol is exposed by `TaskService.StageFederatedCertificateRotation` and `TaskService.ProveFederatedCertificateRotation`:

1. Connect to the peer using the currently trusted client certificate and call `StageFederatedCertificateRotation`, supplying the SHA-256 fingerprint of the new leaf certificate DER, a unique request ID, and an overlap grace period from 60,000 ms to 604,800,000 ms (1 minute to 7 days). The peer persists the pending fingerprint and grace period through its Raft group. A staged rotation expires after one hour if proof is not completed.
2. Reconnect to the same peer using the new client certificate and call `ProveFederatedCertificateRotation` with the same cluster ID, new fingerprint and a new unique request ID. The server hashes the certificate actually presented by the TLS connection and requires it to match the staged fingerprint; a metadata claim alone cannot prove possession.
3. The peer commits the new active fingerprint and previous fingerprint expiry in one Raft command. Ordinary peer RPCs accept the new fingerprint immediately and the old fingerprint only until the committed expiry. The pending fingerprint is never accepted for ordinary RPCs. A replay of the proof request returns the already committed result without extending the grace window.

The overlap grace period is fixed when the old identity stages the rotation; the new identity cannot enlarge it during proof. Once the window expires, the old fingerprint is rejected. A new rotation cannot start while the previous trust window remains active. Existing registry records remain readable because all rotation fields are optional during deserialization. If the new certificate cannot complete proof within one hour, stage a new rotation using the still-trusted old certificate and a fresh request ID. Rotate both clusters' outbound client identities separately; rotating only one direction does not change the peer's server certificate or its trust store.

Authenticated TaskService peer RPCs require both a verified mTLS client certificate and `x-dataspool-peer-cluster`. The server hashes the presented leaf certificate DER, checks the pinned fingerprint against the replicated registry, and refreshes the peer heartbeat. A caller cannot establish identity merely by supplying a cluster ID header. The Admin REST registry and migration endpoints remain protected by the administrator bearer token; the actor header is audit metadata, not authentication.

`POST /api/v1/federation/clusters/{cluster_id}/heartbeat` accepts `identity_fingerprint`, `status` (`healthy`, `degraded`, or `offline`) and a unique `request_id`. Read endpoints are `GET /api/v1/federation/clusters`, `GET /api/v1/federation/migrations`, and `GET /api/v1/federation/migrations/{migration_id}`.

### Create and execute a migration

`POST /api/v1/federation/migrations`:

```json
{
  "migration_id": "migration-2026-001",
  "source_cluster": "west-prod",
  "target_cluster": "east-prod",
  "task_id": "source-task-reference",
  "task_name": "orders-copy",
  "source_config": "{\"type\":\"elasticsearch\",\"addresses\":[\"https://source-es.example.com:9200\"]}",
  "sink_config": "{\"type\":\"elasticsearch\",\"addresses\":[\"https://target-es.example.com:9200\"]}",
  "required_capabilities": ["elasticsearch-source", "elasticsearch-sink"],
  "priority": 2,
  "deadline_at_ms": 1790000000000,
  "execution_budget_ms": 7200000,
  "retry_budget": 2,
  "timeout_policy": "pause",
  "request_id": "migration-create-2026-001"
}
```

`source_config` and `sink_config` must be JSON objects. Both clusters must be registered, not offline, and advertise every required capability. Plans start at `planned`, generation `0`; the elected leader advances them automatically through `preparing`, `running`, `committing`, and `completed`. Operators can request a transition with `POST /api/v1/federation/migrations/{migration_id}/transition`, passing `next_state`, `expected_generation`, `request_id`, and optional `error`. A failed migration can be retried to `preparing`; a paused migration can be resumed to `preparing`. A paused resume keeps the same generation because it continues the same remote task; a failed retry increments the generation, fences delayed reports, compensates the previous generation's sink writes, and provisions a new generation-scoped task.

The orchestrator provisions the target task using a deterministic task request ID derived from migration ID and generation. Repeated requests therefore return the same target task instead of creating duplicates. It exchanges the target task state, aggregate counters and per-shard checkpoints/fencing tokens over mTLS, and persists the observed checkpoint on the source Raft group. The target refuses commit preparation until the task and every shard are completed. It then writes a durable `prepared` operation record through its own Raft group. The source advances to `committing` only after that acknowledgement, requests target commit, and advances to `completed` only after the target's durable `committed` acknowledgement. Request IDs and migration generation make prepare/commit replay safe after leader replacement.

Federated target tasks are tagged with the migration ID and generation. For Elasticsearch sinks, the executor uses a create-only compensation mode: documents require stable IDs and object payloads, are written with the Bulk `create` action, and receive a reserved `_dataspool` marker containing the migration ID and generation. This mode is append-only: it rejects updates, deletes, alternate per-record target indices, and payloads that already use `_dataspool`. Existing target documents are never overwritten. Other sink types fail closed when compensation is requested.

The target stores a durable compensation journal in `.dataspool-compensation`. Before cancelling workers or touching Elasticsearch, it first commits the remote operation state `compensating` through target Raft. This is a mutually exclusive fence: `Prepared -> Committed` and `Prepared -> Compensating -> Compensated` are the only terminal paths, so a Commit racing with compensation is rejected once the compensation intent commits. Compensation then persists `started` in `.dataspool-compensation`, runs a generation-scoped `_delete_by_query` against the configured target index, and marks the journal `completed` only after Elasticsearch reports no timeout, version conflicts, or item failures. If the process or response is lost, replay resumes the same journal operation; deleting the same marker set again is safe. The target records the Raft `compensated` operation only after sink compensation completes. A durably `committed` remote operation remains terminal and cannot be rolled back. This is sink-level compensation for migration-owned append-only documents, not an Elasticsearch transaction or a rollback guarantee for arbitrary updates/CDC pipelines.

### Compensation and failure-injection coverage

The unit suite exercises create-only bulk markers, rejects unsafe compensation identities, and simulates a lost compensation response: the replay reads the completed journal and does not issue a second delete-by-query. A peer transport test injects an unreachable mTLS endpoint and verifies bounded connection failure. The Raft cluster integration test partitions a follower, heals it through snapshot recovery, crashes the leader, restarts the failed member from durable state, and forces a second leader replacement while verifying committed task state and subsequent writes.

The cross-cluster fault-proxy integration test runs two independent one-node Raft groups, registers each other's certificate fingerprints, and uses real Tonic gRPC over mutual TLS through a bidirectional TCP fault proxy. It drops all traffic during a partition, then drops the gRPC Prepare and Commit acknowledgements only after the target handlers have durably applied those operations. It crashes and restarts the source Raft process after target commit but before the source records completion, then verifies that reconciliation reaches `completed`, the target remains `committed`, and only one remote task exists.

A second cross-cluster HA integration test runs two independent three-member Raft groups and a real mTLS TaskService behind a bidirectional TCP fault proxy. It combines initial network isolation with provisioning, drops the target Commit acknowledgement after durable target commit, partitions the peer link, and replaces both source and target leaders while their independent Raft quorums remain available. A replacement source leader retries while isolated, then completes the in-doubt commit after the link heals; the former leaders are restarted and caught up, and a second source leader replacement is forced. The test additionally flaps the peer link through repeated failed reconciliation attempts, checks stale migration generations and stale shard fencing tokens, and verifies replicated terminal state, durable target commit, and exactly one provisioned remote task. Both federation integration tests run in the normal library CI suite (`cargo test --lib --bins --all-features`), not the periodic Elasticsearch soak workflow.

The certificate-rotation HA integration test runs two independent three-member Raft groups, each exposing a real mTLS TaskService through its own bidirectional TCP fault proxy. It stages the peer leaf-certificate replacement in both directions, loses a Stage acknowledgement and retries the same request ID, partitions both peer links, replaces each group's leader, and restarts the former leaders from persistent Raft state. It then proves possession with the new certificate, loses the target Proof acknowledgement after the Raft commit, replaces the target leader again, and retries the exact proof request. Assertions cover all six replicas, pending-state cleanup, old/new fingerprint acceptance, exact expiry-boundary rejection, and unchanged overlap expiry after replay. This test validates Raft member restart and failover recovery; it is not an OS-level `SIGKILL` of the Raft process. It runs in the normal library CI suite.

A separate process-crash integration test starts source and target as independent OS processes, each owning its own persistent single-member Raft group and mTLS gRPC endpoint. It sends real `SIGKILL` to both processes after Stage commits but before Proof, restarts both from their original RocksDB directories, commits Proof, and sends `SIGKILL` again at the durable-commit boundary before any client acknowledgement is emitted. Both processes then restart concurrently and verify the committed identity, immutable overlap deadline, and idempotent Proof replay. The recovered mTLS endpoints reject fresh connections using the expired old certificate for both Stage and Proof replay, while the new active certificate can reconnect and replay the completed Proof. A stale Stage proposal from the restarted former leader is rejected. The process test deliberately drives the Raft command at the commit boundary; the companion mTLS HA test separately injects an actual lost gRPC Proof response and retries it. CI runs this test as `cargo test --test federation_rotation_process --all-features -- --nocapture` on Unix runners.

These tests validate the exercised quorum failover and durable cross-cluster operation replay paths; they are not a formal proof of all network schedules or a distributed ACID transaction. The source and target Raft groups commit independently, and Elasticsearch document deletion is a replayable compensation action rather than an atomic rollback.

### SLA and recovery behavior

`deadline_at_ms`, `execution_budget_ms`, `retry_budget`, and `timeout_policy` are persisted with the migration. The timeout policy is `pause` (pause the target and retain its checkpoint), `cancel` (cancel and compensate before marking cancelled), or `fail` (cancel and compensate before marking failed). Commit is treated as a short critical section: after target prepare succeeds, the orchestrator completes commit reconciliation rather than timing out between prepare and commit. Target RPCs have bounded connect/request timeouts; a failed RPC leaves the migration in its current durable stage for retry by the next reconciliation pass. The scheduler is leader-only, and remote task IDs, checkpoints, operation state and generation are persisted rather than held only in memory.

The task SLA alert endpoint remains `GET /api/v1/alerts/sla`; task metadata supports `sla.deadline_at`, `sla.max_duration_seconds`, `sla.execution_budget_ms`, and `sla.max_retries`. The federated migration fields above enforce actions in the orchestrator, while task-level SLA metadata produces operational alerts and retry-budget rejection.
