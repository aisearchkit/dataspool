#!/usr/bin/env bash
# Run the required quality gates and commit only the explicitly named stage files.
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 'type(scope): summary' path [path ...]" >&2
  exit 2
fi

message="$1"
shift
if [[ ! "$message" =~ ^(feat|fix|docs|test|refactor|perf|build|ci|chore)(\([a-z0-9._-]+\))?:[[:space:]].+ ]]; then
  echo "commit message must follow Conventional Commits" >&2
  exit 2
fi

root="$(git rev-parse --show-toplevel)"
cd "$root"

# Match CI: RocksDB's bundled trace_record.h relies on cstdint being included first.
export CXXFLAGS="${CXXFLAGS:+$CXXFLAGS }-include cstdint"

if ! git diff --cached --quiet; then
  echo "refusing to mix with pre-staged changes; unstage or commit them first" >&2
  exit 1
fi

cargo fmt --all
cargo fmt --all -- --check
cargo check --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
if ! command -v cargo-audit >/dev/null 2>&1; then
  cargo install cargo-audit --locked
fi
cargo audit --deny unsound --ignore RUSTSEC-2023-0071 --ignore RUSTSEC-2026-0459
if [[ -n "${ES_7102_URL:-}${ES_717_URL:-}${ES_8X_URL:-}${ES_9X_URL:-}" ]]; then
  : "${ES_7102_URL:?set ES_7102_URL for the complete compatibility matrix}"
  : "${ES_717_URL:?set ES_717_URL for the complete compatibility matrix}"
  : "${ES_8X_URL:?set ES_8X_URL for the complete compatibility matrix}"
  : "${ES_9X_URL:?set ES_9X_URL for the complete compatibility matrix}"
  cargo test --test elasticsearch_compat -- --nocapture
else
  echo "Skipping live Elasticsearch matrix (set all ES_7102_URL / ES_717_URL / ES_8X_URL / ES_9X_URL to enable)"
fi

git diff --check
git add -- "$@"
git diff --cached --check
if git diff --cached --quiet; then
  echo "no staged changes to commit" >&2
  exit 1
fi

git commit -m "$message"
printf '\nCommitted: '
git rev-parse --short HEAD
branch="$(git branch --show-current)"
if [[ -z "$branch" ]]; then
  echo "cannot auto-push from detached HEAD" >&2
  exit 1
fi
printf '\nPushing %s to origin...\n' "$branch"
git push --set-upstream origin "$branch"
printf '\nPushed commit: '
git rev-parse --short HEAD
printf '\nRemaining worktree changes:\n'
git status --short --branch
