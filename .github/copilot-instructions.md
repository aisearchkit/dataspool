# Dataspool - 混合模式数据迁移与编排平台

## Project Overview

Dataspool is a next-generation hybrid-mode data migration and orchestration platform built in Rust. The system uses an "All-in-One" architecture where each node acts as both Control Plane and Agent Plane, enabling simple deployment with high availability through Raft consensus.

**Current Status**: Early stage - skeleton Rust project with comprehensive design documentation in `docs/`.

## Build & Test Commands

### Rust Backend

```bash
# Build the project
cargo build

# Build with optimizations
cargo build --release

# Run the application
cargo run

# Run tests
cargo test

# Run a specific test
cargo test <test_name>

# Check code without building
cargo check

# Format code
cargo fmt

# Run linter
cargo clippy
```

## Architecture

### Hybrid All-in-One Model

Every Dataspool instance simultaneously runs:
- **Control Plane**: Cluster management, task scheduling, Raft consensus, metadata storage
- **Agent Plane**: Data reading, transformation, and writing operations

The Control Plane uses Raft protocol for leader election and log replication, while Agents form a distributed worker pool that claims and executes shards.

### Core Components (See `docs/02_概要设计说明书.md`)

```
dataspool/
├── api/           # API gateway (RESTful + gRPC-Web)
├── control/       # Control plane (Raft, scheduler, cluster mgmt)
├── agent/         # Agent plane (worker pool, task execution)
├── pipeline/      # Data pipeline (source/transform/sink)
├── drivers/       # Pluggable data source drivers
├── comms/         # Communication layer (gRPC, Protobuf)
├── config/        # Configuration management
└── observability/ # Metrics, logging, health checks
```

### Task Execution Flow

1. User submits task → Leader's Scheduler validates and persists via Raft
2. Scheduler auto-shards task (by index name, date range, PK range, or file)
3. Shards written to Raft log and synchronized to all nodes
4. Agent nodes claim shards via distributed lock (CAS on Raft KV)
5. Agents execute shards in parallel, reporting progress to Leader
6. Offset tracking enables resumption after failures

### Data Sources (Pluggable Driver Architecture)

- Elasticsearch (5.x - 8.x): Reindex API, Scroll API, Point-in-Time
- Snapshots: S3/NFS snapshot files
- MySQL (5.7+, 8.0+): Full table scan, incremental via Binlog
- MongoDB (4.x - 6.x): Collection-level migration
- Filesystem: Local FS and NFS mounts

## Key Conventions

### Raft State Machine

All metadata changes (tasks, shards, templates, nodes) must go through Raft for consistency:
- Only Leader can propose changes
- Followers automatically forward writes to Leader
- Use `propose()` for writes, `consistent_read()` for linearizable reads

### Shard State Transitions

```
Pending → Running (via CAS when Agent claims)
Running → Completed | Failed
Failed → Pending (retry if attempts < max)
```

### Offset Tracking

Checkpoints are persisted via Raft after every N records or T seconds:
- ES: Document count + Scroll ID
- MySQL: Max primary key value or Binlog position
- Files: Byte offset

### Resource Isolation

Each Agent enforces:
- `max_concurrent_tasks`: Limits parallel shards (default: CPU cores)
- `memory_limit_mb`: Stops accepting shards when exceeded
- Read rate limiting to prevent source cluster overload

### Communication Patterns

- **Control ↔ Agent**: gRPC bidirectional streaming (mTLS in production)
  - Control → Agent: Task commands (assign, pause, cancel)
  - Agent → Control: Progress reports, logs, resource metrics
- **External API**: REST (JSON) or gRPC-Web (Protobuf)
  - Reads can hit any node (load balanced)
  - Writes forwarded to Leader

### Error Handling

Use structured errors with retry policies:
- `RetryPolicy` specifies `max_attempts` and `BackoffStrategy`
- Transient errors (network, timeout) → exponential backoff
- Permanent errors (auth failure, invalid config) → fail immediately

### Protobuf Definitions

All gRPC interfaces defined in `comms/proto/*.proto`:
- `cluster.proto`: Node registration, health reports
- `task.proto`: Task CRUD, shard allocation, progress
- `log.proto`: Real-time log streaming
- `meta.proto`: Templates, mappings, ILM

### Observability

Export Prometheus metrics from all modules:
- Cluster: `dataspool_cluster_nodes_total`, `dataspool_raft_log_entries_total`
- Tasks: `dataspool_tasks_total{state}`, `dataspool_task_duration_seconds`
- Pipeline: `dataspool_docs_read_total`, `dataspool_bytes_written_total`
- Resources: `dataspool_worker_pool_active_workers`, `dataspool_memory_used_bytes`

Structured JSON logging with fields: `timestamp`, `level`, `target`, `message`, `node_id`, `task_id`, `shard_id`, `trace_id`

### Configuration

System configured via `config/dataspool-cluster.yaml` (YAML format):
- Environment variables can override any config: `DATASPOOL_<PATH_IN_UPPERCASE>`
- Example: `server.http_port` → `DATASPOOL_SERVER_HTTP_PORT`
- Sensitive values (passwords, secrets) must use env vars, never hardcoded

## Design Documentation

Critical references in `docs/` directory (Chinese):

1. **01_需求说明书.md**: Requirements specification
   - User roles, use cases, functional/non-functional requirements
   - Core scenarios: ES cross-cluster migration, snapshot restore, heterogeneous DB migration

2. **02_概要设计说明书.md**: High-level design
   - System architecture, module breakdown, deployment patterns
   - Key decisions: why hybrid mode, why embedded Raft, pluggable drivers

3. **03_详细设计说明书.md**: Detailed design
   - Data structures, API endpoints, gRPC interfaces
   - Scheduler logic, worker pool, pipeline execution engine

4. **04_数据库设计说明书.md**: Storage design
   - RocksDB Column Families: `raft`, `meta`, `offset`, `auth`, `alert`
   - KV key space design, data lifecycle

## Development Guidelines

### When Implementing New Features

1. Check design docs first - most decisions are already documented
2. Follow the pluggable architecture for data sources (extend Driver trait)
3. All state changes through Raft - no local mutations of cluster state
4. Add Prometheus metrics for observable behavior
5. Use structured logging with appropriate context fields

### Testing Strategy

- Unit tests: Individual components (drivers, transforms, utilities)
- Integration tests: Raft consensus, task scheduling, shard claiming
- End-to-end tests: Full migration scenarios with real/mock data sources

### Code Style

- Follow Rust conventions: `cargo fmt` and `cargo clippy`
- Use `async/await` for I/O operations (tokio runtime)
- Prefer `Result<T, E>` over panics for error handling
- Document public APIs with rustdoc comments

## Git Commit Guidelines

Follow [Conventional Commits](https://www.conventionalcommits.org/) specification:

### Commit Message Format

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Types

- `feat`: New feature (triggers minor version bump)
- `fix`: Bug fix (triggers patch version bump)
- `docs`: Documentation only changes
- `style`: Code style changes (formatting, missing semicolons, etc.)
- `refactor`: Code refactoring without changing functionality
- `perf`: Performance improvements
- `test`: Adding or updating tests
- `build`: Changes to build system or dependencies
- `ci`: Changes to CI configuration files
- `chore`: Other changes that don't modify src or test files

### Scope Examples

- `raft`: Raft consensus module
- `scheduler`: Task scheduler
- `agent`: Agent worker pool
- `driver`: Data source drivers (elasticsearch, mysql, etc.)
- `api`: API layer (REST/gRPC)
- `pipeline`: Data pipeline engine
- `config`: Configuration system
- `observability`: Metrics, logging, tracing

### Examples

```bash
# Feature commits
feat(scheduler): implement task auto-sharding by date range
feat(driver): add MongoDB driver with incremental migration support
feat(api): expose real-time log streaming endpoint

# Bug fix commits
fix(raft): prevent split-brain during network partition
fix(agent): correctly handle offset checkpoint on abrupt termination
fix(driver): escape special characters in ES index patterns

# Breaking changes (triggers major version bump)
feat(api)!: change task submission API to use Protobuf

BREAKING CHANGE: TaskRequest now uses Protobuf schema instead of JSON.
Clients must regenerate code from updated .proto files.

# Documentation commits
docs(readme): add deployment guide for hybrid mode
docs(api): document authentication middleware usage

# Chore commits
chore(deps): upgrade tokio to 1.40
chore(build): add release profile optimization flags
```

### Rules

1. **Type is mandatory**: Every commit must start with a valid type
2. **Use lowercase**: Type and scope should be lowercase
3. **Imperative mood**: Description uses imperative present tense ("add" not "added" or "adds")
4. **No period at end**: Description should not end with a period
5. **Max 72 characters**: First line (type + scope + description) should be ≤ 72 chars
6. **Reference issues**: Use footer to reference issues: `Fixes #123` or `Closes #456`
7. **Breaking changes**: Use `!` after type/scope and add `BREAKING CHANGE:` in footer

## License

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.

### Key Requirements

- **Source Disclosure**: If you run a modified version as a network service, you must make the source code available to users
- **Copyleft**: Derivative works must also be licensed under AGPL-3.0
- **Attribution**: Preserve copyright and license notices

### Adding License Headers

Add the following header to all Rust source files:

```rust
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// Dataspool - Hybrid-mode data migration and orchestration platform
// Copyright (C) 2026 Dataspool Contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
```

### Cargo.toml License Field

Ensure `Cargo.toml` includes:

```toml
[package]
license = "AGPL-3.0-or-later"
```

### Third-Party Dependencies

When adding dependencies, verify license compatibility:
- ✅ Compatible: MIT, Apache-2.0, BSD-3-Clause, MPL-2.0
- ⚠️ Review carefully: GPL-2.0, GPL-3.0 (may require AGPL-3.0)
- ❌ Incompatible: Proprietary, non-commercial licenses

Check licenses: `cargo license` or `cargo-deny`

## Code Quality Standards

### Pre-commit Checklist

Before committing code, ensure:

```bash
# Format code
cargo fmt

# Run linter
cargo clippy -- -D warnings

# Run tests
cargo test

# Check for unused dependencies
cargo +nightly udeps

# Security audit (install: cargo install cargo-audit)
cargo audit
```

### Documentation Requirements

1. **Public APIs**: All public functions, structs, enums must have rustdoc comments
2. **Modules**: Each module should have a module-level doc comment explaining its purpose
3. **Examples**: Complex APIs should include usage examples in doc comments
4. **Safety**: Unsafe code must have detailed safety documentation

Example:

```rust
/// Attempts to claim an available shard for execution.
///
/// This operation uses Raft CAS (Compare-And-Swap) to atomically transition
/// a shard from `Pending` to `Running` state. Only one agent can successfully
/// claim a given shard.
///
/// # Returns
///
/// Returns `Some(ShardId)` if a shard was successfully claimed, `None` if no
/// shards are available or all claims failed due to concurrent access.
///
/// # Examples
///
/// ```
/// let worker = Worker::new(node_id, raft_client);
/// if let Some(shard_id) = worker.try_claim_shard().await {
///     worker.execute_shard(shard_id).await?;
/// }
/// ```
pub async fn try_claim_shard(&self) -> Option<ShardId> {
    // Implementation...
}
```

### Error Handling Patterns

Use `thiserror` for error types:

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DriverError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    
    #[error("Authentication failed for user {user}")]
    AuthenticationFailed { user: String },
    
    #[error("Query timeout after {timeout_secs}s")]
    QueryTimeout { timeout_secs: u64 },
    
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}
```

### Naming Conventions

- **Modules**: `snake_case` - `task_scheduler`, `worker_pool`
- **Structs/Enums**: `PascalCase` - `TaskMeta`, `ShardState`
- **Functions/Variables**: `snake_case` - `claim_shard()`, `node_id`
- **Constants**: `SCREAMING_SNAKE_CASE` - `MAX_RETRY_ATTEMPTS`, `DEFAULT_TIMEOUT`
- **Type Parameters**: Single uppercase letter or `PascalCase` - `T`, `TSource`

### Module Organization

```rust
// 1. External crate imports
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

// 2. Internal crate imports  
use crate::control::raft::RaftNode;
use crate::pipeline::Source;

// 3. Module declarations
mod worker_pool;
mod task_runner;

// 4. Re-exports
pub use worker_pool::WorkerPool;

// 5. Type definitions
type ShardId = String;

// 6. Constants
const DEFAULT_BATCH_SIZE: usize = 1000;

// 7. Struct/Enum definitions
pub struct Agent { ... }

// 8. Trait implementations
impl Agent { ... }

// 9. Tests
#[cfg(test)]
mod tests { ... }
```

## Performance Guidelines

### Async Best Practices

- Use `tokio::spawn()` for CPU-bound tasks that need to run concurrently
- Prefer `tokio::select!` over manual polling for waiting on multiple futures
- Use `Arc<RwLock<T>>` for shared state with many readers, few writers
- Use `tokio::sync::Mutex` (not `std::sync::Mutex`) in async code
- Avoid `.await` inside locks - acquire data, release lock, then `.await`

### Memory Management

- Use `bytes::Bytes` for zero-copy buffer sharing
- Stream large datasets instead of loading into memory
- Implement backpressure in data pipelines
- Set reasonable batch sizes (default: 1000 records)
- Monitor memory usage via `dataspool_memory_used_bytes` metric

### Database Access Patterns

- Batch writes to RocksDB (use WriteBatch)
- Use appropriate Column Family for data type
- Checkpoint offsets every N records or T seconds (not every record)
- Leverage bloom filters for point lookups

## Security Guidelines

### Authentication & Authorization

- Never log passwords, API keys, or tokens
- Use environment variables for secrets, not config files
- Validate all user inputs (SQL injection, path traversal, etc.)
- Implement rate limiting on public APIs
- Require mTLS for production inter-node communication

### Dependency Management

- Regularly run `cargo audit` to check for vulnerabilities
- Pin dependencies to specific versions in production
- Review dependencies before adding (license, maintenance, popularity)
- Prefer well-maintained crates with active communities

### Data Protection

- Encrypt sensitive data at rest (use OS keyring or external secret manager)
- Support TLS 1.2+ only for external APIs
- Sanitize logs to prevent information disclosure
- Implement proper RBAC for multi-tenant scenarios

## CI/CD Pipeline

### GitHub Actions Workflow (Recommended)

The project should have the following CI checks:

**On Pull Request:**
- ✅ Code formatting check (`cargo fmt --check`)
- ✅ Linting (`cargo clippy -- -D warnings`)
- ✅ Unit tests (`cargo test`)
- ✅ Integration tests
- ✅ Security audit (`cargo audit`)
- ✅ License compliance check
- ✅ Documentation build (`cargo doc --no-deps`)

**On Main Branch:**
- All PR checks
- Release build (`cargo build --release`)
- Benchmarks (if performance-critical changes)
- Docker image build and push
- Generate changelog from commits

### Local Pre-push Hook

Create `.git/hooks/pre-push`:

```bash
#!/bin/bash
set -e

echo "Running pre-push checks..."

# Format check
cargo fmt --check || {
    echo "❌ Code not formatted. Run: cargo fmt"
    exit 1
}

# Clippy
cargo clippy -- -D warnings || {
    echo "❌ Clippy found issues"
    exit 1
}

# Tests
cargo test || {
    echo "❌ Tests failed"
    exit 1
}

echo "✅ All checks passed"
```

Make it executable: `chmod +x .git/hooks/pre-push`

## Release Process

### Version Bumping

Follow Semantic Versioning (SemVer):
- **MAJOR**: Breaking changes (incompatible API changes)
- **MINOR**: New features (backward-compatible)
- **PATCH**: Bug fixes (backward-compatible)

### Release Checklist

1. Update `CHANGELOG.md` with release notes
2. Bump version in `Cargo.toml`
3. Commit: `chore(release): prepare v0.2.0`
4. Tag: `git tag -a v0.2.0 -m "Release v0.2.0"`
5. Push: `git push origin main --tags`
6. GitHub Actions builds and publishes artifacts
7. Publish to crates.io: `cargo publish`

## Troubleshooting

### Common Development Issues

**Raft election timeouts in local testing:**
- Increase `election_timeout` in config
- Ensure system time is synchronized
- Check network latency between nodes

**Out of memory during large migrations:**
- Reduce `batch_size` in pipeline config
- Lower `max_concurrent_tasks` per agent
- Monitor `dataspool_memory_used_bytes` metric

**Shard starvation (some agents idle):**
- Check agent tags match task requirements
- Verify shard distribution is balanced
- Review `dataspool_worker_pool_idle_workers` metric

**gRPC connection failures:**
- Verify TLS certificates if mTLS enabled
- Check firewall rules for port 50051
- Enable debug logging: `RUST_LOG=dataspool::comms=debug`

### Debug Logging

Enable detailed logging:

```bash
# All modules at debug level
RUST_LOG=debug cargo run

# Specific modules
RUST_LOG=dataspool::raft=trace,dataspool::agent=debug cargo run

# Exclude noisy modules
RUST_LOG=info,hyper=warn,h2=warn cargo run
```

## Deployment Modes

**Hybrid Mode** (recommended): 3-5 nodes, each running Control+Agent
- Simple deployment, built-in HA, linear scaling

**Server Mode**: Separate Control cluster (3 nodes) + Agent pool (N nodes)
- Max performance for large-scale migrations

**CLI Mode**: Single binary, one-shot tasks, no HTTP server
- Script-friendly for ad-hoc migrations

Minimum viable cluster: 3 nodes (tolerates 1 failure)
