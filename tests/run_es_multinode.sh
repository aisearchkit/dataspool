#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

VERSION="${1:-8x}"
case "$VERSION" in
  7102) PROFILE=es7102; TARGET_IMAGE=docker.elastic.co/elasticsearch/elasticsearch:7.10.2; export ES_7102_URL="${ES_7102_URL:-http://127.0.0.1:19302}" ;;
  717) PROFILE=es717; TARGET_IMAGE=docker.elastic.co/elasticsearch/elasticsearch:7.17.28; export ES_717_URL="${ES_717_URL:-http://127.0.0.1:19312}" ;;
  8x) PROFILE=es8x; TARGET_IMAGE=docker.elastic.co/elasticsearch/elasticsearch:8.19.8; export ES_8X_URL="${ES_8X_URL:-http://127.0.0.1:19322}" ;;
  9x) PROFILE=es9x; TARGET_IMAGE=docker.elastic.co/elasticsearch/elasticsearch:9.5.6; export ES_9X_URL="${ES_9X_URL:-http://127.0.0.1:19332}" ;;
  *) echo "usage: $0 {7102|717|8x|9x}" >&2; exit 2 ;;
esac
export ES_EXPECT_NODES="${ES_EXPECT_NODES:-3}"
export ES_SOAK_SECONDS="${ES_SOAK_SECONDS:-300}"
export ES_REQUIRE_429="${ES_REQUIRE_429:-1}"
export ES_REQUIRE_LIVE=1
COMPOSE=(docker compose -f "$ROOT_DIR/deploy/compose/elasticsearch-multinode.yml" --profile "$PROFILE")
TARGET_CONTAINER="dataspool-e2e-target-${VERSION}"
export ES_WORKER_TARGET_URL="http://127.0.0.1:19402"
export ES_CDC_SOURCE_URL="${ES_CDC_SOURCE_URL:-${ES_7102_URL:-${ES_717_URL:-${ES_8X_URL:-${ES_9X_URL:-}}}}}"
export ES_CDC_TARGET_URL="$ES_WORKER_TARGET_URL"
POSTGRES_CONTAINER="dataspool-e2e-postgres-${VERSION}"
export DATASPOOL_TEST_POSTGRES_URL="${DATASPOOL_TEST_POSTGRES_URL:-postgres://postgres:postgres@127.0.0.1:15432/dataspool_test}"
cleanup() {
  docker rm -f "$TARGET_CONTAINER" "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
  "${COMPOSE[@]}" down -v
}
trap cleanup EXIT
sudo sysctl -w vm.max_map_count=262144
"${COMPOSE[@]}" up -d --wait
docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$POSTGRES_CONTAINER" -p 15432:5432 \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=dataspool_test \
  postgres:16 >/dev/null
for attempt in $(seq 1 60); do
  if docker exec "$POSTGRES_CONTAINER" pg_isready -U postgres -d dataspool_test >/dev/null 2>&1; then break; fi
  if [ "$attempt" -eq 60 ]; then docker logs "$POSTGRES_CONTAINER"; exit 1; fi
  sleep 1
done
docker rm -f "$TARGET_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$TARGET_CONTAINER" -p 19402:9200 \
  -e discovery.type=single-node -e xpack.security.enabled=false \
  -e ES_JAVA_OPTS="-Xms512m -Xmx512m" "$TARGET_IMAGE" >/dev/null
for attempt in $(seq 1 90); do
  if curl -fsS "$ES_WORKER_TARGET_URL/_cluster/health?wait_for_status=yellow&timeout=2s" >/dev/null; then break; fi
  if [ "$attempt" -eq 90 ]; then docker logs "$TARGET_CONTAINER"; exit 1; fi
  sleep 2
done
cargo test --test elasticsearch_compat -- --nocapture
cargo test --test cdc_cutover_e2e -- --nocapture
cargo test --features test-fault-injection --test cdc_worker_failover -- --nocapture
cargo test --features test-fault-injection --test worker_process_crash -- --nocapture
