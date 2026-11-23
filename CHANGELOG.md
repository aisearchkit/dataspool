# Dataspool Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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
