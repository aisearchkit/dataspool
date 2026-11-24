# Durable Batch Execution

Dataspool's Raft-backed shard executor persists every source batch before it attempts a destination write. The batch payload remains in the replicated cache until the destination result, source checkpoint, and cache acknowledgement have each been committed.

## Batch manifest

Each replicated batch has a durable manifest in the metadata column family. The manifest records:

- schema version, shard ID, deterministic batch ID, and the current fencing generation;
- source offset before the batch and the next source offset;
- source record count and SHA-256 digest of the serialized batch records;
- destination success/failure counts and bytes written;
- creation/update timestamps and the manifest state.

Manifest state transitions are monotonic:

```text
Prepared -> Delivered -> Checkpointed -> Acknowledged
```

- `Prepared`: batch payload and manifest have been persisted by the Raft state machine; no destination result is committed yet.
- `Delivered`: destination response has been observed, permanent document failures have been persisted to the DLQ, and the delivery result is recorded through Raft.
- `Checkpointed`: the shard's absolute progress, task aggregate, and manifest transition are committed in one RocksDB write batch, so recovery cannot observe an advanced cursor without the matching manifest state.
- `Acknowledged`: the replicated payload is removed and the per-shard acknowledgement watermark is advanced in one RocksDB write batch; the manifest is retained as history.

The manifest is stored in the metadata column family, which is included in Raft snapshots. Cache payload and manifest writes are individually replay-safe: if state-machine application is interrupted before the applied-log marker is durable, replay repairs a missing manifest without accepting a conflicting batch payload.

## Recovery semantics

- A crash before destination acknowledgement leaves the replicated payload available for replay.
- A destination write that succeeded but whose response was lost may be replayed. The recovery regression test commits a target document, deliberately omits the source-side delivery acknowledgement, drops and recreates the state machine over the same RocksDB data, then claims the expired lease from a replacement Worker. The replacement replays the same batch identity, records delivery, advances the checkpoint once, and acknowledges the cache. The live Elasticsearch compatibility suite also sends the exact same fenced batch twice to cover a lost Bulk response. Elasticsearch fenced writes use stable document IDs, external versions derived from the lease generation, and a reserved `_dataspool` source marker containing the fencing version and shard identity. A version conflict is considered an idempotent success only when a read-after-conflict `_mget` proves that the stored document has an equal/newer external version from the same shard. A higher version from an unrelated shard or pre-existing document fails closed, so it cannot silently advance the checkpoint.
- Bulk item-level permanent failures are persisted to the deterministic, idempotent DLQ before the manifest becomes `Delivered`.
- A crash after `Delivered` but before checkpoint commit replays the batch and repeats the absolute checkpoint operation. It does not add the batch count twice.
- A crash after checkpoint commit but before cache acknowledgement replays the checkpoint transition idempotently, then acknowledges the cache entry.
- The acknowledgement write atomically deletes the payload, advances the watermark, and retains the manifest as `Acknowledged`.
- A stale lease cannot record delivery, advance progress, or acknowledge the cache because each Raft command validates the current owner and fencing token.

## Important boundaries

The batch manifest records the destination's acknowledged Bulk API result; it is not an independent read-after-write verification of target document contents. Source and target content verification, incremental change capture, and cutover safety are separate follow-up work. Elasticsearch external-version fencing requires stable document IDs, object-shaped document payloads (to persist `_dataspool.fencing_version` and `_dataspool.fencing_shard_id`), and writes that pass through the fenced sink path. This reserved `_dataspool` object is merged with existing `_dataspool` fields; its fencing keys are owned by Dataspool.

Manifest history is retained for audit/recovery. A bounded retention/compaction policy should be added before very long-running deployments accumulate unbounded terminal manifests.
