# PostgreSQL Transactional Outbox and CDC cutover

Dataspool provides a PostgreSQL-backed Transactional Outbox for application writers that need a durable CDC feed and a final cutover barrier. The producer and consumer use the same `stream_key`. The legacy scalar stream gate is intentionally a serialization point. `connect_with_max_connections()` lets each application process cap its own pool. CI retains a 32-writer baseline with eight pools of four connections, then separately runs 128/256/512 producer tasks with 16/32/64 configured PostgreSQL connections (eight pools of 2/4/8 connections). The benchmark samples peak active PostgreSQL sessions so pool capacity is not mistaken for database concurrency. Treat CI throughput as a contention baseline, not a production capacity figure; benchmark on production-like hardware, PostgreSQL settings, payload sizes, and network latency before setting a capacity target.

## Guarantees and operating contract

- A producer starts with `PostgresTransactionalOutbox::begin_write()`, performs its source-table mutations through `PostgresOutboxWrite::connection()`, appends the matching event(s) with `append_event()` or `append_events()`, and commits once. `append_events()` performs one idempotency lookup, reserves one contiguous sequence range for new events, and inserts them with one multi-row `INSERT`; it preserves input order and returns the sequence for each input event. A fully replayed batch reserves no new sequence values. Mixed batches retain the original sequence for already committed IDs and allocate a contiguous range for new IDs. Duplicate IDs within one batch or an ID reused with different content poison the transaction. `commit()` rejects a transaction with no Outbox event. The source mutations, stream sequence allocation, and Outbox events therefore commit or roll back together. Batch size should be bounded by application latency and transaction/WAL budgets; large batches hold the stream gate longer and delay cutover.
- The per-stream gate row is locked with `FOR UPDATE` for the lifetime of each producer transaction. This intentionally serializes writes within one stream. It makes the stream sequence contiguous and equal to commit order, including under concurrent producers. Use independent stream keys to partition unrelated workloads; do not share a stream key across unrelated source tables unless they share the same ordering contract.
- Cutover acquires the same row lock. It waits for already accepted writes to finish, persists `paused = true`, then reads the final committed sequence. Writers arriving after the barrier wait for the lock and are rejected after they observe the durable pause.
- The pause is stored in PostgreSQL, not process memory. A process restart does not resume writes automatically. This is intentional: after an ambiguous alias/Raft outcome, resuming without reconciliation could reopen a consistency gap.
- `cutover_with_raft()` keeps the producer paused until the alias switch and Raft `RecordCdcCutover` both succeed. If the alias has switched but Raft rejects or its response is lost, the producer remains paused. Retry the idempotent cutover while retaining the same stream; call `resume_writes()` only after confirming the cutover outcome.
- Keep Outbox events until every relevant consumer has committed a checkpoint at or beyond them; the gate's `last_sequence` remains the final watermark even if old event rows are later pruned. `register_consumer()` creates a durable checkpoint at zero, `record_consumer_checkpoint()` advances one checkpoint monotonically, and `record_consumer_checkpoints()` advances several consumers in one transaction. `prune_consumed_events_batch(limit)` deletes at most `limit` rows through the minimum registered consumer checkpoint; the compatibility method `prune_consumed_events()` drains the eligible range in 1,000-row transactions. Bounded deletes limit transaction size and WAL bursts but still create dead tuples. `retention_stats()` reports estimated live/dead tuples, table+index bytes, autovacuum count/time, and (when supported) the cluster-wide `pg_stat_wal.wal_bytes` counter. Retention batches emit deleted-row count, configured limit, and elapsed time. `n_dead_tup` is an estimate, not an exact count. Alert on sustained dead-tuple growth, stale autovacuum, increasing table/index bytes, and WAL generation; consider time/sequence partition detach-and-drop only after every consumer has passed the partition boundary. A consumer must be registered before it starts reading. Do not delete a lagging consumer's registration merely to unblock retention; first retire that consumer operationally and prove it will never resume from its old checkpoint.
- `install_write_guard(schema, table)` installs a table trigger that rejects ordinary direct DML unless the transaction-local stream marker was established by `begin_write()`. Install it as the table owner, and ensure the application role does not own the table or have permission to disable/drop triggers. This is an accidental-bypass guard, not a hostile-SQL security boundary: PostgreSQL custom GUCs are user-settable, so a role with arbitrary SQL and table DML privileges can forge the marker. For a hard privilege boundary, use `restrict_application_dml(schema, table, application_role)` during deployment to revoke direct mutation privileges from the application login while retaining SELECT. The runtime application role must not own protected tables, inherit DML through another role, or have permission to disable/drop triggers. Route mutations through a separately controlled writer service using a distinct database login that retains DML privileges and invokes this library. Keep schema/trigger ownership and migration credentials separate from both runtime roles. The helper cannot revoke privileges inherited through role memberships, so validate effective privileges with `has_table_privilege()` as part of deployment checks.

## Partition sequence design and global ordering

`PostgresPartitionedOutbox` is an opt-in partitioned implementation for one logical business stream. Configure a fixed partition count at migration time; assign each source entity/key deterministically with `partition_for_key()`, then use `begin_write(partition_id)` for its source mutation and event. SHA-256 based assignment is stable for a fixed partition count. The topology is persisted and changing the count in place is rejected; repartitioning requires a dedicated migration procedure. Each partition has its own gate and `local_sequence`, so unrelated partitions can commit concurrently. The current API requires one source transaction to mutate entities belonging to a single partition; cross-partition atomic source transactions are not yet supported and must remain on the scalar stream until a multi-gate transaction API is added. Event IDs remain globally unique within the partitioned table, and retries must reuse the same ID, partition, and content.

Consumers use `read_partition_after()` or `read_vector_after()` and persist a complete `BTreeMap<partition_id, local_sequence>` with `record_consumer_checkpoint_vector()`. Partial vectors, unknown consumers, regressions, and checkpoints beyond committed partition watermarks are rejected. `pause_and_capture_vector_watermark()` locks every partition gate in ascending order, pauses them atomically, and returns the final vector. `cutover_partitioned_with_raft()` waits until every target partition reaches its matching watermark, switches the alias, and commits `RecordCdcPartitionedCutover` to Raft. The Raft completion gate validates the entire vector; it never reduces the vector to a scalar. If alias acknowledgement or Raft outcome is ambiguous, all partitions remain paused for idempotent reconciliation. The partitioned API is intentionally separate from the legacy scalar `PostgresOutboxSource` and its `last_sequence` checkpoint.

The current `stream_key` is the ordering domain. One gate row provides a contiguous, gap-free sequence whose order is the serialization/commit order for all producers in that stream. Splitting that gate into N independent counters removes the single-row bottleneck, but it also changes the contract: `(partition_id, local_sequence)` is only a partial order. A consumer can safely process partitions independently, and a cutover can use a vector watermark `{partition_id: last_sequence}`, only if application semantics do not require a total order across partitions.

Three designs are viable:

1. **Independent ordered partitions:** assign each entity/key deterministically to a partition; serialize only within that partition; checkpoint and retain with a per-partition vector. Cross-partition operations must be commutative or carry an explicit application-level dependency/version. This removes the single gate hotspot without pretending that a total order exists.
2. **Global sequence allocator:** partition event storage and worker execution, but allocate one global commit ordinal from a shared sequencer. This preserves a total order but retains a serialized allocation point; batching can amortize it, but partitioning alone cannot remove the fundamental bottleneck.
3. **WAL/logical-decoding commit order:** consume committed transaction records from PostgreSQL logical decoding and use commit LSN as the global order. This is a different source architecture: polling the Outbox table by a preassigned sequence cannot reconstruct exact transaction commit order from `pg_current_wal_lsn()` sampled before COMMIT. It requires slot lifecycle, replication-origin/LSN checkpointing, failover-slot policy, and retention coordination with replication slots.

Partitioned streams intentionally do not provide a total order across partitions. Cross-partition operations must be commutative or carry application-level dependencies/versions. Partition count and entity-to-partition mapping are part of the stream identity; changing the mapping requires explicit migration and replay planning. Do not configure a partitioned table with the scalar source/checkpoint protocol.

## Throughput measurements

Partitioned producer transactions now acquire their partition Gate lazily on the first Outbox append, not when the transaction is opened. Source mutations performed before append remain uncommitted in that same PostgreSQL transaction. If cutover pauses the Gate first, the later append is rejected and the caller must roll back; if append acquires the Gate first, cutover waits for the transaction commit and captures its sequence. This shortens Gate lock hold time without permitting source-only commits. The scalar Outbox keeps its existing eager lock behavior until separately measured and migrated.

Partitioned producer transactions now acquire their partition Gate lazily on the first Outbox append, not when the transaction is opened. Source mutations performed before append remain uncommitted in that same PostgreSQL transaction. If cutover pauses the Gate first, the later append is rejected and the caller must roll back; if append acquires the Gate first, cutover waits for the transaction commit and captures its sequence. This shortens Gate lock hold time without permitting source-only commits. The scalar Outbox keeps its existing eager lock behavior until separately measured and migrated.

The PostgreSQL integration benchmark records 32 producer tasks with eight pools capped at four connections, then 128/256/512 producer tasks with eight pools capped at 2/4/8 connections (16/32/64 database connections total). It reports configured pool capacity and sampled peak active PostgreSQL sessions separately, plus throughput, P50/P95/P99 transaction latency, sampled maximum lock-wait sessions, deadlock delta, and `pg_stat_wal.wal_bytes` delta. The partitioned benchmark additionally samples `pg_stat_activity.wait_event_type/wait_event` and `pg_locks` by lock type/mode/grant state, and writes four events per source transaction using a single lookup, sequence-range reservation, and multi-row insert. It also compares four independent scalar streams to one shared stream. These are diagnostic CI measurements, not performance assertions or production capacity promises. Compare results on the same PostgreSQL version, hardware, payload distribution, pool sizing, and server settings. `pg_stat_wal` is cluster-wide, so isolate the benchmark database/instance when attributing WAL bytes.

## Event-ID idempotency index layout

Partitioned Outbox event IDs are UUIDs. The first UUID byte selects one of 16 deterministic buckets; a database CHECK constraint rejects a mismatched bucket, and one partial unique B-tree per bucket enforces uniqueness. This makes duplicate IDs globally impossible while splitting index maintenance across independent trees. Existing rows are backfilled before the former global unique constraint is dropped. Keep the bucket derivation stable across releases; changing it requires an explicit index/data migration.

## Native PostgreSQL table partitioning evaluation

The current `partition_id` design is logical/application-level partitioning: it creates independent gate rows and local sequences, while events still reside in one ordinary PostgreSQL table. The global idempotency invariant is enforced by a deterministic 16-way UUID bucket: a CHECK constraint derives the bucket from the UUID's first byte, and 16 independent partial unique indexes enforce uniqueness within each bucket. Since every UUID maps to exactly one bucket, this preserves global event-ID uniqueness while avoiding one shared unique-index B-tree. The vector watermark remains independent of physical storage layout.

Native storage partitioning can be added later, but PostgreSQL requires every unique constraint on a declaratively partitioned table to include the partition key. A native `LIST(partition_id)` or `RANGE(created_at)` event table therefore cannot by itself preserve global uniqueness of `event_id`. The production design would need a small, separately indexed idempotency ledger mapping `event_id` to `(stream_key, partition_id, local_sequence, content_digest)` and a transactionally maintained retention policy for that ledger. A practical hierarchy is LIST by `partition_id`, then RANGE by `local_sequence` or event time within each partition. Detach/drop is preferable to mass DELETE for fully consumed ranges, but only after every registered consumer's vector checkpoint has passed the range boundary and the event-ID retry horizon has expired. `DETACH PARTITION CONCURRENTLY` is version-dependent; keep a regular detach path for supported older PostgreSQL versions. Do not add native partitioning until benchmarks show table/index bloat or retention WAL is a material bottleneck.

## Producer usage

Run `migrate()` during application deployment/startup. Keep the database URL in a process environment variable; only the variable name is stored in the replicated Dataspool task configuration.

```rust
let outbox = PostgresTransactionalOutbox::connect(
    &std::env::var("APP_DATABASE_URL")?,
    "customers",
).await?;
outbox.migrate().await?;

let mut write = outbox.begin_write().await?;
sqlx::query("UPDATE customers SET body = $1 WHERE id = $2")
    .bind(&body)
    .bind(&customer_id)
    .execute(write.connection())
    .await?;
write.append_event(
    event_id,                 // reuse this ID if retrying an ambiguous commit
    "index",                  // or "delete"
    &customer_id,
    "customers-v2",
    &document,
).await?;
write.commit().await?;
```

If the source mutation fails, the transaction is dropped/rolled back and no event or sequence increment becomes visible. Reusing the same event ID and identical event content after an ambiguous commit returns the original sequence instead of appending a duplicate; reusing an ID for different content is rejected. The source mutation itself should also be idempotent (for example, an upsert) when its commit outcome is unknown.

## Dataspool task source

The direct source is configured as `type: postgres_outbox`. `connection_env` names the environment variable containing the URL; the URL/password itself is not placed in Raft task metadata.

```yaml
source:
  type: postgres_outbox
  connection_env: DATASPOOL_POSTGRES_URL
  stream_key: customers
```

The source emits records with stable document IDs and `_cdc_event_id`, `_op`, `_target_index`, and `_sequence` metadata. Its checkpoint is `driver: postgres_outbox` plus `last_sequence`. The target document sink translates `index` and `delete` operations and honors `_target_index`.

For partitioned streams, use `type: postgres_partitioned_outbox` and provide the immutable partition count:

```yaml
source:
  type: postgres_partitioned_outbox
  connection_env: DATASPOOL_POSTGRES_URL
  stream_key: customers
  partition_count: 16
```

This source returns a `partition_watermarks` vector checkpoint and adds `_partition_id` to each record. It fairly interleaves partition batches without asserting a global order. The registered consumer's durable vector must be advanced with `record_consumer_checkpoint_vector()` only after the corresponding target batch is durably committed; never advance it when merely reading a batch. Retention uses the minimum committed checkpoint for each partition.

## Cutover

The final cutover uses the same `PostgresTransactionalOutbox` instance (or another instance configured with the same stream key) as the `CdcWriteBarrier` passed to `cutover_with_raft()`. The applied-watermark callback must read the durable checkpoint committed by the target worker, normally from Raft. Do not substitute an in-memory worker counter.

PostgreSQL integration tests run in GitHub Actions against PostgreSQL 16. Extended Validation also runs the PostgreSQL Outbox source through the process-level Worker Crash + Raft Leader Failover scenario: the first worker is SIGKILLed after the target Bulk succeeds but before its checkpoint commits, a Raft leader is stopped, a replacement worker claims a higher fencing token and replays idempotently, then `cutover_with_raft()` closes the producer gate, switches the alias, commits the Raft cutover record, and releases writes. The final target document and alias are independently checked. They cover source/Outbox atomic commit and rollback, a killed PostgreSQL backend and pool reconnect, application-role DML revocation plus trigger-based direct-write rejection, 32/128/256/512 concurrent writers with measured throughput/P50/P95/P99/deadlock, sampled lock-wait sessions and WAL-byte deltas, batched source event append, batched consumer checkpoint advancement, bounded 7-row retention chunks, checkpoint-coordinated pruning, sequence ordering, a barrier waiting for an in-flight transaction, durable pause across a new connection, write rejection while paused, direct CDC source checkpointing, Raft fencing rejection, and successful Cutover Completion Gate release. Extended Validation also starts a real PostgreSQL 16 streaming standby, commits an Outbox event on the primary, stops the primary, promotes the standby, and verifies the replicated watermark/event plus a new write through the Dataspool API. This proves manual primary promotion and data continuity, not automatic failover-manager behavior or transparent pool endpoint discovery. The suite also kills an active PostgreSQL backend and verifies pool reconnection. The integration suite also uses a PostgreSQL wire-protocol TCP proxy to forward COMMIT to the server, discard the resulting ReadyForQuery response, and close the client connection. It verifies that PostgreSQL committed the source row and Outbox event despite the client receiving a transport error, then retries the same event ID and an idempotent source upsert and proves the sequence and event count remain unchanged.
