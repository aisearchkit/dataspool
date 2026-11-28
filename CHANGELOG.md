# Dataspool Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Extended federated trust-rotation status listing with phase/peer filters and bounded pagination; hardened the existing manual recovery endpoint with expected-action fencing, fresh bilateral safety checks, and before/after diagnostics.
- Added bilateral federated trust-rotation diagnostics, sanitized state-difference reporting, phase-aware safe recovery, per-rotation audit queries, and Prometheus phase/stall/recovery/overlap metrics.; added per-rotation age/error metrics and Prometheus alert rules for stalled recovery, activation recovery, overlap expiry, retirement delay, and operator authorization waits.
- Added a deterministic dynamic shard scheduler that orders tasks by priority/FIFO age, places work on the least-utilized healthy eligible node, enforces per-node concurrency and memory capacity, and honors `requires.*` task tag constraints.
- Added graceful task pause with a Pausing drain phase, worker-side cancellation propagation, in-flight sink/checkpoint/ack completion, and lease release before the Paused state is committed. Cancellation stops new source batches and lease renewals while allowing an already replicated batch to checkpoint safely.
- Added TaskService.ListTasks with leader-linearizable pagination, state/tag filtering, and deterministic sorting.
- Added replicated audit events with actor, request ID, operation, state transition, timestamp, and applied Raft log index; exposed them through the authenticated `/api/v1/audit` endpoint.
- Added an authenticated Prometheus `/metrics` endpoint for task/shard/node counts, scheduler queue age, node load/capacity, memory capacity, and durable retry totals.
- Added the Raft-committed Distributed Task Control Plane: CreateTask, PauseTask, ResumeTask, CancelTask, RetryTask, and per-node execution pause/resume commands. Task creation uses deterministic request-key-derived IDs; control request IDs are replicated for idempotency and conflicting reuse is rejected.
- Added task lifecycle states and claim gating: paused tasks/nodes stop accepting new shard claims, cancellation blocks subsequent progress/terminal mutations, and retry requeues failed shards while preserving checkpoints.
- Implemented TaskService.GetTask over gRPC with live Task/shard aggregation, per-node throughput, checkpoint offsets, fencing generations, lease deadlines, timestamps, and a Raft linearizable read barrier on the leader.
- Added a separate Task API gRPC listener (default loopback 127.0.0.1:50052) while preserving the existing Raft RPC port; wired graceful lifecycle into the main process.
- Added bidirectional Raft network-partition fault injection and worker-process restart coverage that verifies lease reclamation and checkpoint preservation.
- Raft-backed distributed task aggregation derived from committed shard state, including per-node shard counts, records/bytes and throughput, task-wide throughput, durable checkpoints, and failed-shard accounting.
- Task aggregate rebuild after application startup and Raft snapshot installation; task and shard projections are committed atomically for progress and terminal transitions.
- Three-node recovery coverage for follower network isolation, snapshot catch-up, leader shutdown/election, replicated task aggregation, and idempotent completion/failure commands.
- Cooperative worker-pool shutdown using cancellation tokens. Active continuous/CDC shards finish in-flight writes and checkpoint acknowledgements, close their source and sink, and return to Pending for safe reassignment.
- Application startup now optionally loads `DATASPOOL_CONFIG`, starts the configured single-node Agent alongside the DLQ API, recovers interrupted local shards, and coordinates SIGINT/SIGTERM shutdown.

## [0.1.0] - 2026-11-23

### Added
- Initial Rust project structure and foundational migration components.
- Elasticsearch-compatible HTTP source and sink support.
- Elasticsearch compatibility coverage for 7.10.2, 7.17, 8.x, and 9.x using three-node test clusters.
- Retry handling and soak coverage for transient Elasticsearch write rejections, including HTTP 429 responses.
- CI checks for formatting, Clippy, tests, dependency security auditing, and release builds.
- Automated Dependabot updates for GitHub Actions dependencies, with guarded CI-only auto-merge and dependency-update tags.

### Changed
- Improved fixture bulk initialization reliability by retrying transient item-level rejections in compatibility tests.

### Limitations
- This is an early preview focused on Elasticsearch HTTP migration. MySQL/PostgreSQL sink completeness, production Raft consensus, full gRPC cluster/task APIs, local Lucene reading, and a complete Control/Agent production orchestration lifecycle are not claimed as supported in this release.
- The automated release workflow currently publishes a Linux x86_64 artifact as a GitHub pre-release. Other platform binaries and production deployment/upgrade procedures are not included.
