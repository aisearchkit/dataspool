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

Call `sink.connect()` before replay. Passing an empty `selected_ids` slice replays all entries in that shard's queue. `remove(id)` is an explicit operator discard operation; use it only after reviewing the entry. The current DLQ API is a storage/library interface; a secured HTTP admin endpoint and UI are not yet implemented.

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
docker compose -f docker-compose.elasticsearch-compat.yml up -d --wait
./tests/run_es_compat.sh
docker compose -f docker-compose.elasticsearch-compat.yml down -v
```

Or provide `ES_7102_URL`, `ES_717_URL`, `ES_8X_URL`, and `ES_9X_URL` separately. The test validates version detection, per-item mapping failures, PIT/search_after and `DocumentSource` restart recovery, CDC index/delete events, alias cutover/watermark guard, concurrent Bulk delivery and an overload probe that reports observed HTTP 429 responses. A zero 429 count means the cluster did not reject during that probe; it is not evidence that the 429 path was exercised.

The local compose configuration intentionally uses a one-entry write queue to make overload more reproducible and disables disk threshold enforcement only for disposable test clusters. Never copy those settings into production.
