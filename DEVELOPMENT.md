# Dataspool — 下一代混合模式数据迁移与编排平台

## 1. 项目概述

### 1.1 项目背景

在大规模数据平台的运营场景中，Elasticsearch、MySQL、MongoDB等异构数据源之间的数据迁移、同步与编排需求极为普遍，而现有工具存在以下问题：

- **部署复杂**：传统分布式架构需要独立的 Control 和 Agent 节点，操作繁琐
- **单点故障**：单实例部署无高可用，生产环境风险较高
- **扩展性差**：数据源插件化支持不完善，新属性接入成本高
- **监控缺乏**：缺乏现代化 UI 控制台，实时日志和进度不可视

Dataspool 旨在解决以上痛点，提供一个延迟低、扩展性强、部署简单的数据迁移与编排平台。

### 1.2 项目目标

- 构建以 **Rust** 为核心语言的高性能数据迁移引擎
- 实现 Hybrid 混合模式：每个节点同时有 Control Plane 与 Agent Plane 双重身份
- 支持 3 种运行模式：纯 Server 模式、混合模式（推荐）、独立 CLI 模式
- 基于 Raft 协议实现 Leader 选举，消除单点故障
- 通过 gRPC + HTTP/2 实现高效内部通信
- 提供现代化 Web UI 控制台（**React 19**）
- 支持 Elasticsearch、MySQL、MongoDB等多种数据源的插件化接入

### 1.3 项目范围

| 分类          | 包含（In Scope）                           | 排除（Out of Scope）              |
|---------------|--------------------------------------------|-----------------------------------|
| 核心引擎      | Rust 后端进程、Control/Agent 双平面、Raft 共识 | ML/AI 相关功能                   |
| UI 控制台     | 集群视图、实时日志、任务编排、进度监控       | 移动端 App、SaaS 订阅管理        |
| 数据源        | Elasticsearch（主）、MySQL、MongoDB、NFS/S3 快照 | 商业数据库（Oracle/MSSQL）       |
| 配置管理      | 模板管理、YAML 配置编辑器                 |                                   |

## 2. 技术架构设计

### 2.1 整体架构图

每个 Dataspool 进程实例包含双重身份，形成混合模式集群：

| 身份           | 职责                                          | 关键特性                                               |
|----------------|-----------------------------------------------|--------------------------------------------------------|
| **Control Plane（大脑）** | 集群元数据管理、任务调度与分发、API 网关 | Raft 协议实现、内嵌 KV 存储（sled/rocksdb）、gRPC Stream |
| **Agent Plane（四肢）**  | 数据读取、转换、写入、进度上报 | 内建 Worker Pool 支持并发任务、Offset Tracking 实现断点续传  |

### 2.2 核心技术选型

| 层次           | 技术选型                           | 版本要求     | 选型理由                           |
|----------------|------------------------------------|--------------|------------------------------------|
| 后端语言       | **Rust**                           | >= 1.75.0    | 内存安全、高性能、异步并发优秀     |
| 异步运行时     | Tokio                              | >= 1.35      | 成熟的 Rust 异步运行时，社区最大   |
| RPC 通信       | tonic (gRPC) + HTTP/2              | >= 0.11      | 强类型 Protobuf、双向流、与 Rust 完美配合 |
| 元数据存储     | sled / RocksDB                     | >= 0.34      | 嵌入式 KV 存储，避免外部存储依赖   |
| 前端框架       | **React 19**                       | >= 19.0      | 更强的生态系统支持和性能优化       |

### 2.3 数据源插件体系

| Trait           | 方向   | 已支持       | 计划支持               |
|-----------------|--------|--------------|------------------------|
| SourceConnector | Source | Elasticsearch | MySQL, MongoDB, S3 快照 |
| SinkConnector   | Sink   | Elasticsearch | S3, 本地文件           |
| TransformPlugin | Transform | Field Rename, Type Cast | Script (Lua/WASM), Regex Extract |

## 3. 功能模块详细说明

### 3.1 集群管理模块（Control Plane）

#### 3.1.1 Raft 共识层

核心功能包括 Leader 选举、日志复制和成员变更等。

#### 3.1.2 任务调度层

任务切分策略：

- 用户提交任务
- Leader 节点切分任务，Agent 节点通过分布式锁抢占任务

### 3.2 数据源插件体系

插件体系采用 Rust Trait 实现，映射到具体数据源实现，包括 Elasticsearch、MySQL 和 S3 快照。

## 4. 数据流与接口设计

### 4.1 任务生命周期状态机

任务会经过以下状态：PENDING、RUNNING、PAUSED、FAILED、DONE。

## 5. 自动化开发与提交规范（强制）

所有由自动化开发代理或开发者完成的代码阶段，必须在阶段结束时执行质量检查并自动创建 Git commit。不得仅以“代码已写入工作区”作为完成状态。

### 5.1 阶段完成门禁

按顺序执行：

1. 对本阶段修改的 Rust 文件执行 `rustfmt --edition 2024 --config skip_children=true`，避免递归格式化无关模块。
2. `cargo check --all-targets`
3. 运行 `cargo test --lib` 和相关集成测试。
4. `git diff --check`
5. 审查 `git diff`、`git status`，确认没有密钥、凭据、构建产物、临时文件或无关改动。
6. 使用 `scripts/commit-stage.sh 'type(scope): summary' path...` 自动执行上述门禁，并只暂存明确列出的本阶段文件。
7. 检查 `git status --short`，记录 commit SHA 和剩余工作区变更。

仓库现有部分文件尚未满足全仓库 `cargo fmt --all -- --check`；阶段门禁只格式化本阶段改动文件，避免无关格式化噪声。

若检查失败，必须先修复并重跑；不得提交未通过门禁的代码。若失败属于已确认的基线问题，必须记录错误、证明与本阶段变更无关，并在提交说明/开发记录中明确标记；未经验证不得宣称构建或测试通过。禁止使用 `git add .` 盲目暂存。

### 5.2 提交粒度与格式

- 一个独立功能阶段对应一个 commit，便于审查和回滚。
- 使用 `feat(scope): ...`、`fix(scope): ...`、`test(scope): ...`、`docs(scope): ...` 等 Conventional Commits 类型。
- 提交前检查 staged diff；提交后记录 commit SHA 和验证结果。
- 不自动推送远程仓库，除非任务明确要求。

## 6. 配置文件规范

### 5.1 集群配置示例 (dataspool-cluster.yaml)

```yaml
cluster:
  node_name: "dataspool-node-1"
  peers:
    - "192.168.1.10:7000"
    - "192.168.1.11:7000"
    - "192.168.1.12:7000"
  election_timeout: "5s"

server:
  grpc_port: 50051
  http_port: 8080
