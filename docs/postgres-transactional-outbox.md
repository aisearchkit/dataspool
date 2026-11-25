# PostgreSQL Transactional Outbox and CDC cutover

Dataspool provides a PostgreSQL-backed Transactional Outbox for application writers that need a durable CDC feed and a final cutover barrier. The producer and consumer use the same `stream_key`.

## Guarantees and operating contract

- A producer starts with `PostgresTransactionalOutbox::begin_write()`, performs its source-table mutation through `PostgresOutboxWrite::connection()`, appends the matching event with `append_event()`, and commits once. `commit()` rejects a transaction with no Outbox event. The source mutation, stream sequence allocation, and Outbox event therefore commit or roll back together.
- The per-stream gate row is locked with `FOR UPDATE` for the lifetime of each producer transaction. This intentionally serializes writes within one stream. It makes the stream sequence contiguous and equal to commit order, including under concurrent producers. Use independent stream keys to partition unrelated workloads; do not share a stream key across unrelated source tables unless they share the same ordering contract.
- Cutover acquires the same row lock. It waits for already accepted writes to finish, persists `paused = true`, then reads the final committed sequence. Writers arriving after the barrier wait for the lock and are rejected after they observe the durable pause.
- The pause is stored in PostgreSQL, not process memory. A process restart does not resume writes automatically. This is intentional: after an ambiguous alias/Raft outcome, resuming without reconciliation could reopen a consistency gap.
- `cutover_with_raft()` keeps the producer paused until the alias switch and Raft `RecordCdcCutover` both succeed. If the alias has switched but Raft rejects or its response is lost, the producer remains paused. Retry the idempotent cutover while retaining the same stream; call `resume_writes()` only after confirming the cutover outcome.
- Keep Outbox events until every relevant consumer has committed a checkpoint at or beyond them; the gate's `last_sequence` remains the final watermark even if old event rows are later pruned. The guarantee applies only to mutations executed in the returned `PostgresOutboxWrite` transaction. Every application write path for the protected data must use this API (or an equivalent database-enforced trigger/procedure); direct SQL writes that bypass the gate are outside the guarantee. Restrict application-role DML or add table-specific triggers if bypass must be prevented at the database permission boundary.

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

## Cutover

The final cutover uses the same `PostgresTransactionalOutbox` instance (or another instance configured with the same stream key) as the `CdcWriteBarrier` passed to `cutover_with_raft()`. The applied-watermark callback must read the durable checkpoint committed by the target worker, normally from Raft. Do not substitute an in-memory worker counter.

PostgreSQL integration tests run in GitHub Actions against PostgreSQL 16. They cover source/Outbox atomic commit and rollback, concurrent writers, sequence ordering, a barrier waiting for an in-flight transaction, durable pause across a new process-style connection, write rejection while paused, direct CDC source checkpointing, Raft fencing rejection, and successful Cutover Completion Gate release.
