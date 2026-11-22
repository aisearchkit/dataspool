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

if ! git diff --cached --quiet; then
  echo "refusing to mix with pre-staged changes; unstage or commit them first" >&2
  exit 1
fi

# Format only this stage's Rust files; skip child modules to avoid touching unrelated files.
for path in "$@"; do
  if [[ "$path" == *.rs ]]; then
    rustfmt --edition 2024 --config skip_children=true "$path"
  fi
done

cargo check --all-targets
cargo test --lib
if [[ -n "${ES_7102_URL:-}${ES_717_URL:-}${ES_8X_URL:-}" ]]; then
  cargo test --test elasticsearch_compat -- --nocapture
else
  echo "Skipping live Elasticsearch matrix (set ES_7102_URL / ES_717_URL / ES_8X_URL to enable)"
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
printf '\nRemaining worktree changes:\n'
git status --short
