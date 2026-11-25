#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
NETWORK="dataspool-pg-ha"
PRIMARY="dataspool-pg-ha-primary"
STANDBY="dataspool-pg-ha-standby"
cleanup() {
  docker rm -f "$PRIMARY" "$STANDBY" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$NETWORK" >/dev/null
docker run -d --name "$PRIMARY" --network "$NETWORK" -p 15433:5432 \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=dataspool_test \
  postgres:16 -c wal_level=replica -c max_wal_senders=10 -c hot_standby=on -c listen_addresses='*' >/dev/null
for attempt in $(seq 1 60); do
  if docker exec "$PRIMARY" pg_isready -U postgres -d dataspool_test >/dev/null 2>&1; then break; fi
  if [ "$attempt" -eq 60 ]; then docker logs "$PRIMARY"; exit 1; fi
  sleep 1
done
docker exec "$PRIMARY" psql -U postgres -d dataspool_test -v ON_ERROR_STOP=1 -c "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replica';"
docker exec "$PRIMARY" bash -lc "echo 'host replication replicator 0.0.0.0/0 scram-sha-256' >> \$PGDATA/pg_hba.conf"
docker exec -u postgres "$PRIMARY" /usr/lib/postgresql/16/bin/pg_ctl -D /var/lib/postgresql/data reload >/dev/null

docker run -d --name "$STANDBY" --network "$NETWORK" -p 15434:5432 \
  --entrypoint bash postgres:16 -lc '
    export PGPASSWORD=replica
    mkdir -p "$PGDATA"
    rm -rf "$PGDATA"/*
    chown -R postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"
    export PGPASSFILE=/var/lib/postgresql/.pgpass
    printf "dataspool-pg-ha-primary:5432:replication:replicator:replica\n" > "$PGPASSFILE"
    chown postgres:postgres "$PGPASSFILE"
    chmod 600 "$PGPASSFILE"
    until gosu postgres pg_basebackup -h dataspool-pg-ha-primary -D "$PGDATA" -U replicator -Fp -Xs -P -R; do sleep 1; done
    exec gosu postgres /usr/lib/postgresql/16/bin/postgres -D "$PGDATA" -c hot_standby=on -c listen_addresses="*"
  ' >/dev/null
for attempt in $(seq 1 90); do
  if docker exec "$STANDBY" pg_isready -U postgres -d dataspool_test >/dev/null 2>&1; then break; fi
  if [ "$attempt" -eq 90 ]; then docker logs "$STANDBY"; exit 1; fi
  sleep 1
done

export DATASPOOL_TEST_POSTGRES_HA_URL="postgres://postgres:postgres@127.0.0.1:15433/dataspool_test"
export DATASPOOL_TEST_POSTGRES_HA_PHASE=seed
cargo test --test postgres_ha_failover -- --nocapture

# Wait until the standby has replayed the committed Outbox row and gate watermark.
for attempt in $(seq 1 90); do
  if docker exec "$STANDBY" psql -U postgres -d dataspool_test -Atqc "SELECT last_sequence FROM dataspool_cdc_outbox_gate WHERE stream_key = 'dataspool-primary-promotion-e2e'" 2>/dev/null | grep -qx 1; then break; fi
  if [ "$attempt" -eq 90 ]; then docker logs "$STANDBY"; exit 1; fi
  sleep 1
done

docker stop "$PRIMARY" >/dev/null
docker exec -u postgres "$STANDBY" /usr/lib/postgresql/16/bin/pg_ctl -D /var/lib/postgresql/data promote >/dev/null
for attempt in $(seq 1 60); do
  if docker exec "$STANDBY" psql -U postgres -d dataspool_test -Atqc 'SELECT NOT pg_is_in_recovery()' | grep -qx t; then break; fi
  if [ "$attempt" -eq 60 ]; then docker logs "$STANDBY"; exit 1; fi
  sleep 1
done

export DATASPOOL_TEST_POSTGRES_HA_URL="postgres://postgres:postgres@127.0.0.1:15434/dataspool_test"
export DATASPOOL_TEST_POSTGRES_HA_PHASE=verify
cargo test --test postgres_ha_failover -- --nocapture
