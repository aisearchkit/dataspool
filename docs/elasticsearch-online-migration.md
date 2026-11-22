# Elasticsearch online migration design

## Current repository capabilities

The document driver detects the cluster version, uses PIT + `search_after` for Elasticsearch 7.12+ (and compatible engines), uses Scroll for Elasticsearch 7.10/7.11 or when configured, and writes through `_bulk`. The generic pipeline supports pluggable transforms and checkpoints an opaque source offset. These are the base for snapshot migration, not yet a complete change-data-capture (CDC) system.

## Target architecture

```text
Source ES (PIT/search_after) -> bounded read -> local durable batch cache
                                      |                    |
                                      v                    v
                               local transforms      retry / replay
                                      |                    |
                                      +------> adaptive bulk writer -> Target ES
                                                   |
                                           cluster pressure signals
```

### Compatibility

- Detect source and target versions independently; do not assume both clusters expose identical features.
- Elasticsearch 7.12+ uses PIT + `search_after`; Elasticsearch 7.10/7.11 and older versions use Scroll.
- Target writes use the Bulk API and preserve `_id`, routing, and selected metadata.
- Treat HTTP 429 and item-level `es_rejected_execution_exception` as backpressure signals, not generic fatal errors.

### Delivery and consistency

- Snapshot phase: PIT provides a stable view while documents are paged. A checkpoint must include PIT/search-after state or a restart-safe deterministic partition cursor; document count alone is not a restart-safe cursor.
- Cache phase: persist transformed batches before acknowledging upstream progress. Delete cache entries only after the destination confirms all corresponding bulk items succeeded.
- Retry phase: retry only failed bulk items; use bounded exponential backoff with jitter. Keep permanent mapping/validation failures in a dead-letter path.
- Cutover phase: a PIT snapshot alone does not capture writes made after the PIT was opened. True online migration requires a CDC source (for example a supported change stream or application dual-write/outbox) and a replay watermark before alias/index cutover. Do not claim zero-downtime consistency without that phase.

## Implemented foundation in this branch

- `pipeline/backpressure.rs`: AIMD controller adjusts batch size and in-flight window from latency, rejection, HTTP status and queue-utilization signals.
- `pipeline/cache.rs`: durable RocksDB-backed FIFO batch cache with explicit acknowledgement; failed delivery leaves the batch queued.
- `pipeline/engine.rs`: optional adaptive batch sizing and pacing can be enabled with `Pipeline::with_backpressure`. The current pipeline is single-writer; the controller exposes an in-flight target, but parallel bulk dispatch is not yet wired into this sequential engine.

## Next implementation stages

1. Make PIT/search-after checkpoints restart-safe and validate PIT renewal/cleanup behavior.
2. Add item-level bulk retry and dead-letter handling; do not count a partially failed batch as fully checkpointed.
3. Wire the durable cache into the executor so offsets advance only after destination acknowledgement.
4. Implement a CDC/change-log adapter and watermark-based catch-up before cutover.
5. Add integration tests against Elasticsearch 7.10.2, 7.17, 8.x, 9.x and compatible Easysearch/OpenSearch versions.
6. Expose throughput, bulk latency, 429 count, cache depth, retry count and adaptive window as cluster metrics.

## CDC contract and cutover

The CDC adapter consumes an append-only Elasticsearch change-log index. Elasticsearch does not provide a generic native document change stream; the change index must be populated by an application transactional outbox, a database-log CDC connector, or an equivalent producer. Each event must contain:

```json
{
  "sequence": 123456,
  "operation": "index",
  "document_id": "stable-id",
  "target_index": "orders-v2",
  "document": {"status": "paid"}
}
```

`operation` is `index` or `delete`. `sequence` must be globally unique and strictly increasing in the consumed feed. The CDC source checkpoints `last_sequence`; delete events are emitted as Bulk `delete` actions and index events use stable document IDs. The change log must be retained until all consumers have passed its watermark.

Cutover is guarded by `switch_alias_after_catchup`: it refuses to update aliases until the applied watermark is at least the final source watermark, then performs the alias remove/add in one `_aliases` request. The caller is responsible for obtaining a final source watermark after pausing/serializing writes or otherwise establishing a consistent write barrier. Without a producer-side barrier, a moving watermark can race the alias switch.

## Operational constraints

- PIT checkpoints contain the current PIT ID, `search_after` values and read count. A PIT is server-side and expires after `keep_alive`; if it expires before recovery, the snapshot phase must be restarted or use a separately materialized immutable snapshot. A PIT cursor is not a permanent bookmark.
- The durable batch queue is shard-scoped. The executor commits the shard cursor after successful destination delivery and before cache acknowledgement. A crash between checkpoint and acknowledgement can replay the same batch; stable IDs make this at-least-once protocol idempotent.
- Permanent mapping/validation failures remain in the durable cache and stop the shard rather than silently advancing its checkpoint. A production dead-letter workflow and operator resolution API are still required.
- The current CDC source assumes an append-only change index with a unique sequence. It is not a direct Elasticsearch translog or `_changes` API integration.

## Compatibility test matrix

`docker-compose.elasticsearch-compat.yml` starts Elasticsearch 7.10.2, 7.17.28 and 8.19.8. Run `docker compose -f docker-compose.elasticsearch-compat.yml up -d`, wait for health checks, then execute `tests/run_es_compat.sh`. The test creates a temporary index, validates Bulk item-level errors, opens a PIT and exercises `search_after`. Existing clusters can be tested by setting `ES_7102_URL`, `ES_717_URL` and `ES_8X_URL` (and optional matching `_USER` / `_PASSWORD` variables).

## Snapshot-plus-CDC source (implemented foundation)

Use `type: elasticsearch_online` to run a snapshot followed by continuous change-log replay in one shard execution. The source captures the maximum change-log sequence first, opens the document snapshot only after that watermark has been captured, reads the snapshot, and then consumes events with sequence greater than the captured value. The composite checkpoint stores the phase, snapshot cursor, initial watermark, and CDC cursor. The executor persists a phase-only transition even when no records are emitted, so a restart does not recapture a newer watermark and skip changes.

Example task source configuration:

```yaml
source:
  type: elasticsearch_online
  snapshot:
    urls: ["https://source-es:9200"]
    index: "orders"
    batch_size: 1000
  change_log_connection:
    urls: ["https://source-es:9200"]
    index: "dataspool-change-log"
  cdc:
    change_index: "dataspool-change-log"
    sequence_field: "sequence"
    operation_field: "operation"
    document_id_field: "document_id"
    target_index_field: "target_index"
    document_field: "document"
    routing_field: "routing"
    batch_size: 1000
```

Configure the sink as a document sink targeting the destination index. CDC events may override the sink's default index using `target_index`; `operation: delete` is translated to a Bulk delete action. Snapshot `_routing` metadata and optional CDC `routing` values are carried into Bulk action metadata. The change-log producer must guarantee that each sequence is globally unique and strictly increasing, that a change event is searchable only after its source mutation is visible to the snapshot/search API, and that events are retained until the migration has consumed them. The snapshot is opened after the initial watermark is captured: changes after that watermark may be present in both snapshot and CDC, which is intentional and safe with stable document IDs and ordered replay. This source is at-least-once, not exactly-once.

The online source does not itself create a transactional outbox, pause application writes, or perform final alias cutover. Those require producer-specific integration and an explicit write-barrier/final-watermark operation. Do not switch production aliases solely because the CDC poller is running.
