# PostgreSQL Transactional Outbox and CDC cutover

Dataspool provides a PostgreSQL-backed Transactional Outbox for application writers that need a durable CDC feed and a final cutover barrier. The producer and consumer use the same `stream_key`. The per-stream gate is intentionally a serialization point. `connect_with_max_connections()` lets each application process cap its own pool; the CI load test uses eight independent application pools (four connections each), then compares 32 writers on one stream with 32 writers distributed over four independent streams. Treat CI throughput as a contention baseline, not a production capacity figure; benchmark on production-like hardware, PostgreSQL settings, payload sizes, and network latency before setting a capacity target.

## Guarantees and operating contract

- A producer starts with `PostgresTransactionalOutbox::begin_write()`, performs its source-table mutation through `PostgresOutboxWrite::connection()`, appends the matching event with `append_event()`, and commits once. `commit()` rejects a transaction with no Outbox event. The source mutation, stream sequence allocation, and Outbox event therefore commit or roll back together.
- The per-stream gate row is locked with `FOR UPDATE` for the lifetime of each producer transaction. This intentionally serializes writes within one stream. It makes the stream sequence contiguous and equal to commit order, including under concurrent producers. Use independent stream keys to partition unrelated workloads; do not share a stream key across unrelated source tables unless they share the same ordering contract.
- Cutover acquires the same row lock. It waits for already accepted writes to finish, persists `paused = true`, then reads the final committed sequence. Writers arriving after the barrier wait for the lock and are rejected after they observe the durable pause.
- The pause is stored in PostgreSQL, not process memory. A process restart does not resume writes automatically. This is intentional: after an ambiguous alias/Raft outcome, resuming without reconciliation could reopen a consistency gap.
- `cutover_with_raft()` keeps the producer paused until the alias switch and Raft `RecordCdcCutover` both succeed. If the alias has switched but Raft rejects or its response is lost, the producer remains paused. Retry the idempotent cutover while retaining the same stream; call `resume_writes()` only after confirming the cutover outcome.
- Keep Outbox events until every relevant consumer has committed a checkpoint at or beyond them; the gate's `last_sequence` remains the final watermark even if old event rows are later pruned. `register_consumer()` creates a durable checkpoint at zero, `record_consumer_checkpoint()` advances it monotonically, and `prune_consumed_events()` deletes only through the minimum registered consumer checkpoint. A consumer must be registered before it starts reading. Do not delete a lagging consumer's registration merely to unblock retention; first retire that consumer operationally and prove it will never resume from its old checkpoint.
- `install_write_guard(schema, table)` installs a table trigger that rejects ordinary direct DML unless the transaction-local stream marker was established by `begin_write()`. Install it as the table owner, and ensure the application role does not own the table or have permission to disable/drop triggers. This is an accidental-bypass guard, not a hostile-SQL security boundary: PostgreSQL custom GUCs are user-settable, so a role with arbitrary SQL and table DML privileges can forge the marker. For a hard privilege boundary, use `restrict_application_dml(schema, table, application_role)` during deployment to revoke direct mutation privileges from the application login while retaining SELECT. The runtime application role must not own protected tables, inherit DML through another role, or have permission to disable/drop triggers. Route mutations through a separately controlled writer service using a distinct database login that retains DML privileges and invokes this library. Keep schema/trigger ownership and migration credentials separate from both runtime roles. The helper cannot revoke privileges inherited through role memberships, so validate effective privileges with `has_table_privilege()` as part of deployment checks.

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

PostgreSQL integration tests run in GitHub Actions against PostgreSQL 16. Extended Validation also runs the PostgreSQL Outbox source through the process-level Worker Crash + Raft Leader Failover scenario: the first worker is SIGKILLed after the target Bulk succeeds but before its checkpoint commits, a Raft leader is stopped, a replacement worker claims a higher fencing token and replays idempotently, then `cutover_with_raft()` closes the producer gate, switches the alias, commits the Raft cutover record, and releases writes. The final target document and alias are independently checked. They cover source/Outbox atomic commit and rollback, a killed PostgreSQL backend and pool reconnect, application-role DML revocation plus trigger-based direct-write rejection, 32 concurrent writers with measured throughput/P50/P95/P99/deadlock and sampled lock-wait output, checkpoint-coordinated pruning, sequence ordering, a barrier waiting for an in-flight transaction, durable pause across a new connection, write rejection while paused, direct CDC source checkpointing, Raft fencing rejection, and successful Cutover Completion Gate release. Extended Validation also starts a real PostgreSQL 16 streaming standby, commits an Outbox event on the primary, stops the primary, promotes the standby, and verifies the replicated watermark/event plus a new write through the Dataspool API. This proves manual primary promotion and data continuity, not automatic failover-manager behavior or transparent pool endpoint discovery. The suite also kills an active PostgreSQL backend and verifies pool reconnection. The network-level lost-COMMIT-ack ambiguity window still requires a TCP fault proxy; the idempotent event-ID replay test covers application retry semantics but is not a substitute for that wire-level fault injection.
