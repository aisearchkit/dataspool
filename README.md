# Dataspool

Dataspool 是一个以 Rust 实现的数据迁移与编排项目，当前重点是 Elasticsearch 兼容 HTTP 数据迁移能力。项目处于 early preview 阶段；请以实际实现、测试覆盖和本文的限制说明为准，不要将规划中的集群能力视为已交付功能。

## 当前能力

- Elasticsearch 兼容 HTTP Source / Sink。
- Elasticsearch 7.10.2、7.17、8.x、9.x 三节点兼容性测试。
- Bulk 写入、瞬时拒绝（包括 HTTP 429）重试与背压相关测试。
- 基于 PIT / `search_after` 的快照读取，以及基于 append-only change index 的 Snapshot + CDC 基础流程。
- RocksDB 持久化组件、checkpoint、DLQ 等迁移基础设施。

## 快速开始

### 环境要求

- Rust stable（edition 2024）
- Protobuf compiler（`protoc`）
- Clang / LLVM 与 RocksDB 所需的本机构建依赖
- Docker Compose（仅在本地启动 Elasticsearch 测试集群时需要）

### 构建

```bash
cargo build --locked --release
```

### 连接已有 Elasticsearch 集群

按需设置连接地址，然后运行兼容性测试：

```bash
export ES_7102_URL=http://127.0.0.1:9200
export ES_717_URL=http://127.0.0.1:9201
export ES_8X_URL=http://127.0.0.1:9202
export ES_9X_URL=http://127.0.0.1:9203
cargo test --test elasticsearch_compat -- --nocapture
```

### 使用 Docker Compose 启动测试集群

单节点版本兼容测试集群：

```bash
docker compose -f deploy/compose/elasticsearch-compat.yml up -d --wait
./tests/run_es_compat.sh
docker compose -f deploy/compose/elasticsearch-compat.yml down -v
```

三节点测试集群（每次选择一个版本 profile）：

```bash
./tests/run_es_multinode.sh 7102  # 7.10.2
./tests/run_es_multinode.sh 717   # 7.17
./tests/run_es_multinode.sh 8x    # 8.x
./tests/run_es_multinode.sh 9x    # 9.x
```

测试脚本会启动对应 Compose profile，并在退出时清理容器和卷。测试集群关闭安全认证并使用较小 write queue 来提高拒绝场景的可复现性，只能用于隔离测试环境，不能直接用于生产。

## 项目结构

```text
.
├── config/                 # 示例配置
├── deploy/
│   └── compose/            # Docker Compose 测试与开发环境
├── docs/                   # 架构、设计与运维文档
├── examples/               # 任务与 License 示例
├── proto/                  # gRPC / Protobuf 定义
├── scripts/                # 开发辅助脚本
├── src/
│   ├── admin/              # 管理接口与 DLQ
│   ├── agent/              # Agent 执行与 Worker Pool
│   ├── comms/              # gRPC 通信
│   ├── config/             # 配置加载
│   ├── drivers/              # 数据源与目标端驱动
│   ├── license/              # License 管理
│   ├── pipeline/             # 迁移流水线、背压与 DLQ
│   ├── raft/                # 集群共识相关组件
│   ├── storage/              # KV 与分层存储
│   └── task/                # 任务与 Shard 元数据
├── tests/                  # 集成测试与测试集群脚本
├── web/                    # Web UI 原型资源
├── Cargo.toml
└── README.md
```

### 重要配置和部署文件

- `config/dataspool-cluster.yaml`：集群配置示例。
- `deploy/compose/elasticsearch-compat.yml`：ES 7.10.2、7.17、8.x 单节点兼容测试集群。
- `deploy/compose/elasticsearch-multinode.yml`：ES 7.10.2、7.17、8.x、9.x 三节点测试集群。

## 当前限制

本版本是 Elasticsearch HTTP 迁移方向的 early preview，不代表完整生产集群产品。MySQL/PostgreSQL Sink 完整性、生产级 Raft consensus、完整 gRPC 集群与任务 API、本地 Lucene Reader，以及 Control/Agent 生产级编排生命周期仍未全部交付。CDC 依赖外部生产者维护 append-only change index；它不是 Elasticsearch translog 或原生 CDC 接口。

发布版本、已知限制和变更记录见 [CHANGELOG.md](CHANGELOG.md)；在线迁移约束见 [docs/elasticsearch-online-migration.md](docs/elasticsearch-online-migration.md)。

## 许可证

AGPL-3.0-or-later，详见 [LICENSE](LICENSE)。
