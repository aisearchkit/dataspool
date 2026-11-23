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
4. Call `switch_alias_after_catchup`; it refuses to switch when the watermark is behind and uses one atomic `_aliases` request.
5. Release the write barrier only after verifying the alias target.

Without a write barrier/final watermark protocol, a producer can append events between watermark sampling and alias switch; the helper cannot make that external race safe by itself.

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
