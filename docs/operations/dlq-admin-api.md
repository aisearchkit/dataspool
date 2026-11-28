# Dead Letter Queue administration

## Start the management API

The binary starts the DLQ management HTTP API. It uses the same RocksDB directory as the agent/task metadata store.

```bash
export DATASPOOL_DB_PATH=/var/lib/dataspool
export DATASPOOL_ADMIN_TOKEN="$(openssl rand -hex 32)"
# Alternative: use distinct tokens, one per operator (this replaces DATASPOOL_ADMIN_TOKEN):
# export DATASPOOL_ADMIN_OPERATORS='{"oncall-platform":"<64-hex-character-token>"}'
export DATASPOOL_HTTP_BIND=127.0.0.1:8080
cargo run --release
```

The bearer token must be at least 32 bytes. Do not expose this service directly to the public network. Keep the default loopback bind or put it behind a TLS-terminating reverse proxy with network allow-listing. The HTML shell at `/admin/dlq` is public but contains no data; all `/api/v1/...` endpoints require `Authorization: Bearer <token>`. Responses are marked `Cache-Control: no-store`.

`GET /api/v1/session` returns the server-resolved operator ID for the authenticated bearer token. With `DATASPOOL_ADMIN_OPERATORS`, each operator has an independent token and audit identity. The caller-supplied `X-Dataspool-Actor` header is overwritten after authentication. Legacy `DATASPOOL_ADMIN_TOKEN` remains supported as the shared `admin` identity.

## API

All routes are scoped by both task and shard. The server verifies that the shard belongs to the requested task before accessing its queue.

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v1/tasks/{task_id}/shards/{shard_id}/dlq?limit=50&offset=0` | List failures (maximum page size 200) |
| POST | `/api/v1/tasks/{task_id}/shards/{shard_id}/dlq/replay` | Replay selected entries |
| DELETE | `/api/v1/tasks/{task_id}/shards/{shard_id}/dlq/{entry_id}` | Permanently discard one entry |

Replay requires an explicit non-empty `ids` array (maximum 100 IDs). There is intentionally no replay-all operation. The server reconstructs the task's configured sink, connects, replays selected entries, removes only successful entries, and increments attempt count while retaining failures. Discard is irreversible and should be used only after operator review.

```bash
curl -sS -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
  "http://127.0.0.1:8080/api/v1/tasks/$TASK_ID/shards/$SHARD_ID/dlq?limit=50"

curl -sS -X POST -H "Authorization: Bearer $DATASPOOL_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"ids":["<64-character-entry-id>"]}' \
  "http://127.0.0.1:8080/api/v1/tasks/$TASK_ID/shards/$SHARD_ID/dlq/replay"
```

Open `http://127.0.0.1:8080/admin/dlq` for the operator page. Enter the token, task UUID and shard UUID. The UI supports pagination, record inspection, explicit selection, replay confirmation and discard confirmation. The token is held only in the current page's memory and is not written to local storage.

## Multi-node Elasticsearch validation

The multi-node Compose file defines isolated three-node clusters for 7.10.2, 7.17.28 and 8.19.8. Run one version at a time (each profile consumes three JVMs):

```bash
ES_SOAK_SECONDS=300 tests/run_es_multinode.sh 7102
tests/run_es_multinode.sh 717
tests/run_es_multinode.sh 8x
```

The script starts the selected cluster, waits for health, runs the compatibility suite with a three-node assertion, exercises high-concurrency Bulk delivery and requires the real `DocumentSink` to report at least one retried document (HTTP 429 path), runs a sustained soak (default 300 seconds), then removes the cluster and volumes. Set `ES_SOAK_SECONDS=0` to omit the soak; keep `ES_REQUIRE_429=1` for acceptance testing. The disposable cluster uses a one-entry write queue and disables disk allocation thresholds to make pressure testing more reproducible; never use those settings in production.


For individually attributable operator sessions, set `DATASPOOL_ADMIN_OPERATORS` to a JSON object mapping operator IDs to unique bearer tokens (minimum 32 bytes each). When configured, this credential set replaces the legacy single `DATASPOOL_ADMIN_TOKEN`; the `/api/v1/session` endpoint returns the authenticated actor ID. Caller-supplied `X-Dataspool-Actor` values are ignored and overwritten after token authentication.
