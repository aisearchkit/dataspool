---
name: rust-dataspool
description: Apply Dataspool-specific Rust engineering constraints when modifying this repository: Rust 2024, async Tokio/Axum, OpenRaft, RocksDB, Elasticsearch migration and federated certificate lifecycle. Use with the installed rust-stable and relevant specialized Rust skills.
---

# Dataspool Rust Engineering

This skill layers repository-specific constraints over the upstream Rust skills. Read `DEVELOPMENT.md` and the relevant upstream skill before changing code.

## Repository invariants

- Respect `Cargo.lock`, Rust edition 2024, and the project's declared MSRV in `DEVELOPMENT.md` / CI. Never assume the upstream skill's current-stable baseline is this project's MSRV.
- Preserve compatibility with Elasticsearch 7.10.2 and later supported versions. Keep cluster protocol and persisted Raft state changes backward/forward compatibility explicit.
- Treat Raft committed state as authoritative. Never emulate a distributed commit with local in-memory state or mutate lifecycle state outside its state machine.
- For federation certificate rotation, bilateral state, activation barriers, persisted expiry, request idempotency, and append-only audit are safety invariants. After either side commits ActivationBarrier, rollback is forbidden; use forward recovery only.
- Async fan-out, peer probes, bulk workers, queues, retries, and cancellation must be bounded. Never hold synchronous locks across `.await`.
- Do not leak CA PEM, private keys, bearer tokens, or secret-bearing error strings into API responses, logs, audit details, metrics labels, or browser state.
- Public API DTOs must be explicit sanitized projections; do not serialize internal persistence structs when they contain secrets or implementation-only fields.
- Operator identity must come from authenticated server-side principal mapping, never from a caller-controlled display header or form field. Request IDs are correlation/idempotency identifiers, not authentication.
- Recovery endpoints must revalidate bilateral diagnostics and expected action server-side. A browser confirmation is UX, not an authorization or safety boundary.

## Validation and delivery contract

- Run `cargo fmt --all` and `cargo fmt --all -- --check`.
- Run `CXXFLAGS='-include cstdint' cargo check -j 1 --all-targets --all-features` and `CXXFLAGS='-include cstdint' cargo clippy -j 1 --all-targets --all-features -- -D warnings` when native dependencies require the CI compatibility flag.
- Do not run `cargo test` locally or in Box for this repository. GitHub Actions is the test authority; inspect the run for the exact pushed commit and report the actual result.
- For Elasticsearch multi-node or federation process tests, use CI jobs; do not claim local verification without running a real cluster.
- Run `git diff --check`, inspect staged changes, use `scripts/commit-stage.sh` where applicable, and push the completed stage to `origin`.
- Never claim deployment, Raft, TLS, or browser end-to-end validation based only on compilation or router unit tests. Record which real processes, nodes, certificates, and failure cases were exercised.

## Skill routing

- Language semantics and ownership: `rust-stable`.
- API/domain modeling: `rust-api-design`.
- Tokio, Raft concurrency, bounded workers and cancellation: `rust-concurrency`.
- Axum routes and service lifecycle: `rust-web`; authentication, authorization, TLS and browser threat model: `rust-web-security`.
- Durable storage, SQL and transactions: `rust-database`.
- Metrics, audit correlation and tracing: `rust-observability`.
- Test architecture: `rust-testing`.
- Dependency/MSRV/Cargo changes: `rust-cargo-build` and `rust-dependencies`.
- Final correctness/security review: `rust-code-review` and `rust-style-clippy`.
