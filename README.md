# 🎮 Dataspool UI

<p align="center">
  <img src="https://img.shields.io/badge/React-19-blue.svg" alt="React 19" />
  <img src="https://img.shields.io/badge/TanStack_Query-v5-ff4154.svg" alt="TanStack Query" />
  <img src="https://img.shields.io/badge/Tailwind_CSS-4+-38bdf8.svg" alt="Tailwind CSS 4+" />
  <img src="https://img.shields.io/badge/Radix_UI-Components-8b5cf6.svg" alt="Radix UI" />
  <img src="https://img.shields.io/badge/Vite-5-646cff.svg" alt="Vite 5" />
  <img src="https://img.shields.io/badge/TypeScript-5-blue.svg" alt="TypeScript 5" />
</p>

<p align="center">
  下一代混合模式数据迁移与编排平台 - 现代化控制台
</p>

---

## 📋 目录

- [简介](#简介)
- [功能特性](#功能特性)
- [技术栈](#技术栈)
- [快速开始](#快速开始)
- [项目结构](#项目结构)
- [开发指南](#开发指南)
- [构建与部署](#构建与部署)
- [环境变量](#环境变量)
- [API 集成](#api-集成)
- [贡献指南](#贡献指南)
- [许可证](#许可证)

---

## 简介

Dataspool UI 是一个基于 **React 19** 和 **TanStack Query** 构建的现代化 Web 控制台，用于管理和监控 Dataspool 数据迁移集群。

它提供实时的集群状态可视化、任务编排、进度追踪和日志流功能，帮助用户轻松管理大规模数据迁移任务。

### 核心能力

- 🎯 **集群可视化** - 实时展示所有节点（Control/Agent）的健康状态
- ⚡ **实时日志流** - 基于 TanStack Query Suspense 模式，无缝接收 gRPC 流日志
- 🔄 **任务编排** - 可视化配置迁移任务，支持多数据源选择
- 📊 **性能监控** - 实时图表展示迁移速度、资源使用率
- 🎨 **响应式设计** - 基于 Tailwind CSS 4+ 和 Radix UI，完美适配桌面与移动端

---

## 功能特性

| 功能 | 描述 | 状态 |
|------|------|------|
| **集群概览** | 展示所有节点的健康状态、CPU/内存使用率 | ✅ |
| **实时日志** | 通过 gRPC Stream 接收并展示迁移日志（类似 `tail -f`） | ✅ |
| **任务管理** | 创建、编辑、暂停、恢复、删除迁移任务 | ✅ |
| **任务编排** | 可视化配置数据源、目标端、转换规则 | ✅ |
| **节点管理** | 查看节点详情、标签管理、资源限制配置 | ✅ |
| **监控仪表盘** | 实时图表展示迁移速度、吞吐量、错误率 | ✅ |
| **多数据源支持** | ES/Easysearch、S3 快照、MySQL、MongoDB、文件系统 | ✅ |
| **暗色主题** | 基于 Radix UI 的系统级主题切换 | ✅ |

---

## 技术栈

### 核心框架

- **React 19** - 使用最新的 React 特性（Actions、use）[^1]
- **TanStack Query v5** - 数据获取、缓存、同步和实时流
- **TypeScript 5** - 类型安全的开发体验
- **Vite 5** - 极速的开发构建工具

### UI 组件库

- **Radix UI** - 无样式、可访问的原始组件
- **Tailwind CSS 4+** - 原子化 CSS 框架
- **Lucide React** - 轻量级图标库

### 状态与数据

- **Zustand** - 轻量级全局状态管理
- **gRPC-Web** - 与 Dataspool Control Plane 通信
- **Protobuf** - 严格的数据契约定义

### 工具链

- **ESLint + Prettier** - 代码规范与格式化
- **Playwright** - E2E 测试
- **Vitest** - 单元测试

---

## 快速开始

### 前置要求

- Node.js >= 20.x
- pnpm >= 8.x（推荐）或 npm/yarn

### 安装依赖

```bash
# 使用 pnpm（推荐）
pnpm install

# 或使用 npm
npm install

# 或使用 yarn
yarn install
