#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export ES_7102_URL="${ES_7102_URL:-http://127.0.0.1:19202}"
export ES_717_URL="${ES_717_URL:-http://127.0.0.1:19217}"
export ES_8X_URL="${ES_8X_URL:-http://127.0.0.1:19218}"

cargo test --test elasticsearch_compat -- --nocapture
