# Dataspool Driver Architecture

## Current Drivers

### 1. Document Driver (`drivers/document/`)
**Status**: ✅ Implemented

Supports HTTP-based document databases:
- Elasticsearch (5.x - 8.x)
- OpenSearch (1.x - 2.x)
- Compatible engines (Easysearch, Infini, etc.)

**Features**:
- Scroll API and Point-in-Time (PIT) support
- Bulk API for efficient writes
- Local shard reader for zero-network overhead
- Automatic version detection

**Configuration**:
```yaml
source:
  type: document
  urls:
    - "http://localhost:9200"
  index: "source-index"
  batch_size: 1000
```

### 2. Relational Driver (`drivers/relational/`)
**Status**: ✅ Implemented (Source only, Sink placeholder)

Supports relational databases via unified interface:
- MySQL (5.7+, 8.0+)
- PostgreSQL (12+, 13+, 14+, 15+)
- SQLite (planned)

**Features**:
- DatabaseType enum for multi-DB support
- Automatic port defaults per database type
- Primary key-based pagination
- WHERE clause filtering
- Connection pooling via sqlx

**Configuration**:
```yaml
source:
  type: relational
  db_type: mysql  # or postgresql
  host: "localhost"
  # port: 3306  # optional, defaults based on db_type
  database: "testdb"
  username: "root"
  password: "secret"
  table: "users"
  pk_column: "id"
  batch_size: 1000
  where_clause: "created_at > '2024-01-01'"  # optional
```

---

## Future Drivers

### 3. Streaming Driver (`drivers/streaming/`) 
**Status**: 🔜 Planned

**Purpose**: Real-time data streaming and CDC (Change Data Capture)

**Supported Platforms**:
- **Kafka** (Apache Kafka, Confluent)
- **Pulsar** (Apache Pulsar)
- **AutoMQ** (Kafka-compatible cloud-native service)
- **RedPanda** (Kafka API compatible)
- **RabbitMQ Streams** (future)

**Use Cases**:
1. **Source**: Consume from Kafka/Pulsar topics
   - Subscribe to topics/partitions
   - Consumer group management
   - Offset tracking and checkpointing
   - Schema registry integration (Avro, Protobuf)

2. **Sink**: Produce to Kafka/Pulsar topics
   - Batch writes with configurable flush
   - Partitioning strategies (key-based, round-robin, custom)
   - Exactly-once semantics (Kafka transactions)
   - Compression (gzip, snappy, lz4)

**Configuration Example**:
```yaml
source:
  type: streaming
  platform: kafka  # or pulsar, automq
  brokers:
    - "localhost:9092"
  topics:
    - "events"
  consumer_group: "dataspool-migration"
  offset: "earliest"  # or latest, specific offset
  
sink:
  type: streaming
  platform: kafka
  brokers:
    - "target-kafka:9092"
  topic: "migrated-events"
  partitioning: "key"  # or round_robin
  compression: "snappy"
```

**Implementation Notes**:
- Use `rdkafka` crate for Kafka
- Use `pulsar-rs` for Apache Pulsar
- Offset management via Raft KV store
- Support for both batch and streaming modes
- Schema evolution handling

**AutoMQ Specifics**:
- Cloud-native Kafka with automatic scaling
- S3-based tiered storage
- Compatible with standard Kafka clients
- May need specific configurations for optimal performance

---

### 4. NoSQL Driver (`drivers/nosql/`)
**Status**: 🔜 Planned

**Purpose**: Document and key-value store migrations

**Supported Databases**:
- **MongoDB** (4.x - 6.x)
  - Collection-level migration
  - Change streams for CDC
  - Aggregation pipeline support

- **Redis** (5.x - 7.x)
  - Key scanning with SCAN (not KEYS)
  - Type-aware dumping (String, Hash, List, Set, ZSet)
  - Pipelining for batch operations

- **Cassandra** / **ScyllaDB** (future)
  - Token-range based sharding
  - CQL query support

**Configuration Example**:
```yaml
# MongoDB
source:
  type: nosql
  engine: mongodb
  uri: "mongodb://localhost:27017"
  database: "production"
  collection: "users"
  batch_size: 1000
  filter: '{"status": "active"}'  # optional

# Redis
source:
  type: nosql
  engine: redis
  host: "localhost"
  port: 6379
  db: 0
  key_pattern: "user:*"  # SCAN pattern
  batch_size: 100
```

---

### 5. Object Storage Driver (`drivers/object_storage/`)
**Status**: 🔜 Planned

**Purpose**: Snapshot-based migrations and bulk exports

**Supported Storages**:
- **Amazon S3** / **S3-compatible** (MinIO, Ceph, etc.)
- **NFS** / **Local Filesystem**
- **Azure Blob Storage** (future)
- **Google Cloud Storage** (future)

**Use Cases**:
1. **Elasticsearch Snapshot Restore**:
   - Read Elasticsearch snapshot files from S3/NFS
   - Parse segment files directly
   - Zero-copy when colocated with nodes

2. **Bulk Export/Import**:
   - Export data to Parquet/JSON/CSV
   - Import from file-based datasets
   - Parallel file processing

**Configuration Example**:
```yaml
# S3 Snapshot
source:
  type: object_storage
  storage: s3
  bucket: "es-snapshots"
  prefix: "snapshot-2024-01-01/"
  region: "us-east-1"
  # credentials via AWS SDK defaults or explicit
  
# NFS Snapshot
source:
  type: object_storage
  storage: nfs
  path: "/mnt/es-snapshots/snapshot-2024-01-01"
  
# Parquet Files
source:
  type: object_storage
  storage: s3
  bucket: "data-lake"
  file_pattern: "events/year=2024/month=01/*.parquet"
  format: parquet
```

**Implementation Notes**:
- Use `aws-sdk-s3` for S3 (already in Cargo.toml)
- Use `parquet` crate for Parquet files (already in Cargo.toml)
- Support streaming reads to avoid memory issues
- Parallel file downloads with configurable concurrency

---

## Driver Implementation Checklist

When adding a new driver, ensure:

- [ ] Implement `Source` trait (`src/pipeline/source.rs`)
  - `source_type()` - unique identifier
  - `connect()` - establish connection
  - `read_batch()` - fetch records
  - `estimate_count()` - total record estimation
  - `close()` - cleanup resources

- [ ] Implement `Sink` trait (`src/pipeline/sink.rs`)
  - `sink_type()` - unique identifier
  - `connect()` - establish connection
  - `write_batch()` - write records
  - `flush()` - ensure durability
  - `close()` - cleanup resources

- [ ] Add configuration struct
  - Serde serialization/deserialization
  - Validation method
  - Sensible defaults

- [ ] Update `drivers/factory.rs`
  - Add variant to `SourceConfig` enum
  - Add variant to `SinkConfig` enum
  - Update `create_source()` function
  - Update `create_sink()` function

- [ ] Add comprehensive tests
  - Config serialization/deserialization
  - Unit tests for query builders
  - Integration tests with mock server (if applicable)

- [ ] Update documentation
  - Add example to `examples/task-example.yaml`
  - Update `docs/03_详细设计说明书.md`
  - Add driver-specific README

- [ ] Follow licensing requirements
  - AGPL-3.0 headers in all files
  - Check dependency licenses for compatibility

---

## Priority Order

Based on user requirements and industry adoption:

1. **Streaming (Kafka/AutoMQ)** - High demand for real-time CDC
2. **NoSQL (MongoDB/Redis)** - Common migration scenarios
3. **Object Storage (S3/Parquet)** - Snapshot restore and data lake imports

---

## References

- **Kafka Client**: https://github.com/fede1024/rust-rdkafka
- **Pulsar Client**: https://github.com/streamnative/pulsar-rs
- **MongoDB Driver**: https://github.com/mongodb/mongo-rust-driver
- **Redis Client**: https://github.com/redis-rs/redis-rs
- **AWS SDK**: https://github.com/awslabs/aws-sdk-rust
- **Parquet**: https://docs.rs/parquet/latest/parquet/

---

_Last updated: 2026-02-14_
