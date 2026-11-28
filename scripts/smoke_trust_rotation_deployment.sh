#!/usr/bin/env bash
set -euo pipefail

binary="${DATASPOOL_BINARY:-target/debug/dataspool}"
if [[ ! -x "$binary" ]]; then echo "Dataspool binary not executable: $binary" >&2; exit 1; fi
for tool in openssl curl python3 node google-chrome; do command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }; done

http_port="${DATASPOOL_SMOKE_HTTP_PORT:-58080}"
raft_port="${DATASPOOL_SMOKE_RAFT_PORT:-58081}"
task_port="${DATASPOOL_SMOKE_TASK_PORT:-58082}"
tmp="$(mktemp -d)"
pid=""
chrome_pid=""
cleanup() {
  if [[ -n "$chrome_pid" ]] && kill -0 "$chrome_pid" 2>/dev/null; then
    kill -TERM "$chrome_pid" 2>/dev/null || true
    wait "$chrome_pid" 2>/dev/null || true
  fi
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  if [[ "${DATASPOOL_SMOKE_KEEP:-0}" == "1" ]]; then echo "Smoke artifacts retained at $tmp"; else rm -rf "$tmp"; fi
}
trap cleanup EXIT

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$tmp/server.key" \
  -out "$tmp/server.crt" -days 1 -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign' >/dev/null 2>&1
chmod 600 "$tmp/server.key"
python3 - "$tmp" "$http_port" "$raft_port" "$task_port" <<'PY'
from pathlib import Path
import sys
root=Path(sys.argv[1]); http,raft,task=sys.argv[2:]
s=Path('config/dataspool-cluster.yaml').read_text()
s=s.replace('grpc_port: 50051', f'grpc_port: {raft}', 1)
s=s.replace('task_api_port: 50052', f'task_api_port: {task}', 1)
s=s.replace('http_port: 8080', f'http_port: {http}', 1)
s=s.replace('bind_addr: "0.0.0.0"', 'bind_addr: "127.0.0.1"', 1)
s=s.replace('enabled: false', 'enabled: true', 1)
s=s.replace('/etc/dataspool/certs/server.crt', str(root/'server.crt'))
s=s.replace('/etc/dataspool/certs/server.key', str(root/'server.key'))
s=s.replace('/etc/dataspool/certs/ca.crt', str(root/'server.crt'))
s=s.replace('/var/lib/dataspool', str(root/'data'))
(root/'config.yaml').write_text(s)
PY

operator_token="$(openssl rand -hex 32)"
auditor_token="$(openssl rand -hex 32)"
printf '%s' "$operator_token" >"$tmp/operator-token"
chmod 600 "$tmp/operator-token"
export DATASPOOL_CONFIG="$tmp/config.yaml"
export DATASPOOL_DB_PATH="$tmp/data"
export DATASPOOL_HTTP_BIND="127.0.0.1:$http_port"
export DATASPOOL_RAFT_BIND="127.0.0.1:$raft_port"
export DATASPOOL_RAFT_ADVERTISE_ADDR="127.0.0.1:$raft_port"
export DATASPOOL_RAFT_TOKEN="$(openssl rand -hex 32)"
export DATASPOOL_FEDERATION_CLUSTER_ID="operator-ui-smoke"
export DATASPOOL_ADMIN_OPERATORS="$(python3 -c 'import json,sys; print(json.dumps({"smoke-operator":sys.argv[1],"smoke-auditor":sys.argv[2]}))' "$operator_token" "$auditor_token")"
unset DATASPOOL_ADMIN_TOKEN || true

"$binary" >"$tmp/dataspool.log" 2>&1 & pid=$!
base="http://127.0.0.1:$http_port"
ready=0
for _ in $(seq 1 240); do
  if curl --silent --fail "$base/admin/trust-rotations" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$pid" 2>/dev/null; then cat "$tmp/dataspool.log" >&2; echo "Dataspool exited before HTTP became ready" >&2; exit 1; fi
  sleep 0.25
done
if [[ "$ready" != "1" ]]; then cat "$tmp/dataspool.log" >&2; echo "Timed out waiting for Dataspool HTTP listener" >&2; exit 1; fi

status="$(curl --silent --output /dev/null --write-out '%{http_code}' "$base/api/v1/session")"
[[ "$status" == "401" ]] || { echo "anonymous session returned HTTP $status, expected 401" >&2; exit 1; }
session="$(curl --silent --fail -H "Authorization: Bearer $operator_token" -H 'X-Dataspool-Actor: forged-admin' "$base/api/v1/session")"
python3 - "$session" <<'PY'
import json,sys
value=json.loads(sys.argv[1]); assert value == {'actor':'smoke-operator'}, value
PY
auditor_session="$(curl --silent --fail -H "Authorization: Bearer $auditor_token" -H 'X-Dataspool-Actor: forged-admin' "$base/api/v1/session")"
python3 - "$auditor_session" <<'PY'
import json,sys
value=json.loads(sys.argv[1]); assert value == {'actor':'smoke-auditor'}, value
PY
rotations="$(curl --silent --fail -H "Authorization: Bearer $operator_token" -H 'X-Dataspool-Actor: forged-admin' "$base/api/v1/federation/trust-rotations/operator-view?limit=20&offset=0")"
python3 - "$rotations" <<'PY'
import json,sys
value=json.loads(sys.argv[1]); assert value == [], value
PY

# Exercise the real failed-recovery audit path against the live one-node Raft state machine.
recovery_status="$(curl --silent --output "$tmp/recovery-response.json" --write-out '%{http_code}' \
  -X POST -H "Authorization: Bearer $operator_token" -H 'X-Dataspool-Actor: forged-admin' \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"smoke-request-0001","expected_action":"forward_recovery"}' \
  "$base/api/v1/federation/trust-rotations/nonexistent-smoke-rotation/recover")"
[[ "$recovery_status" == "502" ]] || { cat "$tmp/recovery-response.json" >&2; echo "missing rotation recovery returned HTTP $recovery_status, expected 502" >&2; exit 1; }
replay_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -X POST -H "Authorization: Bearer $operator_token" -H 'X-Dataspool-Actor: forged-admin' \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"smoke-request-0001","expected_action":"forward_recovery"}' \
  "$base/api/v1/federation/trust-rotations/nonexistent-smoke-rotation/recover")"
[[ "$replay_status" == "502" ]] || { echo "replayed recovery returned HTTP $replay_status, expected 502" >&2; exit 1; }
audit="$(curl --silent --fail -H "Authorization: Bearer $operator_token" -H 'X-Dataspool-Actor: forged-admin' \
  "$base/api/v1/federation/trust-rotations/nonexistent-smoke-rotation/audit?limit=10")"
python3 - "$audit" <<'PY'
import json,sys
rows=json.loads(sys.argv[1]); assert len(rows)==1, rows
row=rows[0]
assert row['actor']=='smoke-operator', row
assert row['request_id']=='smoke-request-0001', row
assert row['action']=='federation.trust_rotation.recover_diagnostics', row
assert row['outcome']=='failed', row
assert row['log_index']>0, row
PY

headers="$(curl --silent --show-error --dump-header - --output /dev/null -H "Authorization: Bearer $operator_token" "$base/api/v1/session")"
grep -qi '^cache-control: no-store' <<<"$headers" || { echo "authenticated API response missing Cache-Control: no-store" >&2; exit 1; }
html="$(curl --silent --fail "$base/admin/trust-rotations")"
grep -q 'Operator confirmation' <<<"$html"
grep -q 'ActivationBarrier is committed' <<<"$html"
google-chrome --headless --no-sandbox --disable-gpu --disable-dev-shm-usage \
  --disable-background-networking --no-first-run --no-default-browser-check \
  --remote-debugging-port=0 --user-data-dir="$tmp/chrome" about:blank \
  >"$tmp/browser.log" 2>&1 & chrome_pid=$!
for _ in $(seq 1 100); do
  [[ -s "$tmp/chrome/DevToolsActivePort" ]] && break
  kill -0 "$chrome_pid" 2>/dev/null || { cat "$tmp/browser.log" >&2; echo "Chrome exited before DevTools became ready" >&2; exit 1; }
  sleep 0.1
done
[[ -s "$tmp/chrome/DevToolsActivePort" ]] || { cat "$tmp/browser.log" >&2; echo "Timed out waiting for Chrome DevTools" >&2; exit 1; }
node scripts/smoke_trust_rotation_browser.mjs "$base" "$operator_token" smoke-operator "$tmp/chrome/DevToolsActivePort"

printf 'PASS: real Dataspool process started with single-node Raft and mTLS gRPC\n'
printf 'PASS: anonymous API rejected; two distinct bearer credentials resolve to their configured operator identities\n'
printf 'PASS: forged actor header ignored; replayed failure produces one audit event with authenticated actor, request ID, outcome and real Raft index\n'
printf 'PASS: sanitized operator registry, no-store API response and served operator UI\n'
if [[ "${DATASPOOL_SMOKE_HOLD:-0}" == "1" ]]; then
  printf 'BROWSER_URL=%s/admin/trust-rotations\n' "$base"
  printf 'TOKEN_FILE=%s/operator-token\n' "$tmp"
  while kill -0 "$pid" 2>/dev/null; do sleep 1; done
fi
