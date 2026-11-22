#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

VERSION="${1:-8x}"
case "$VERSION" in
  7102) PROFILE=es7102; export ES_7102_URL="${ES_7102_URL:-http://127.0.0.1:19302}" ;;
  717) PROFILE=es717; export ES_717_URL="${ES_717_URL:-http://127.0.0.1:19312}" ;;
  8x) PROFILE=es8x; export ES_8X_URL="${ES_8X_URL:-http://127.0.0.1:19322}" ;;
  *) echo "usage: $0 {7102|717|8x}" >&2; exit 2 ;;
esac
export ES_EXPECT_NODES="${ES_EXPECT_NODES:-3}"
export ES_SOAK_SECONDS="${ES_SOAK_SECONDS:-300}"
export ES_REQUIRE_429="${ES_REQUIRE_429:-1}"
export ES_REQUIRE_LIVE=1
COMPOSE=(docker compose -f docker-compose.elasticsearch-multinode.yml --profile "$PROFILE")
cleanup() { "${COMPOSE[@]}" down -v; }
trap cleanup EXIT
sudo sysctl -w vm.max_map_count=262144
"${COMPOSE[@]}" up -d --wait
cargo test --test elasticsearch_compat -- --nocapture
