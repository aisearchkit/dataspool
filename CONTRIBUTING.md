# Dataspool Contributing Guide

感谢您对 Dataspool 项目的关注！

## 开发流程

### 1. Fork 并克隆仓库

```bash
git clone https://github.com/yourusername/Dataspool.git
cd Dataspool
```

### 2. 创建功能分支

```bash
git checkout -b feat/your-feature-name
# 或
git checkout -b fix/your-bug-fix
```

### 3. 进行开发

- 查看 `docs/` 目录下的设计文档
- 遵循 `.github/copilot-instructions.md` 中的开发规范
- 添加单元测试和集成测试
- 运行 `cargo fmt` 和 `cargo clippy` 确保代码质量

### 4. 提交代码

使用 Conventional Commits 格式：

```bash
git add .
git commit -m "feat(scheduler): add task auto-sharding by date range"
```

### 5. 推送并创建 Pull Request

```bash
git push origin feat/your-feature-name
```

在 GitHub 上创建 Pull Request，并：
- 填写 PR 模板
- 关联相关 Issue
- 等待 CI 检查通过
- 响应 Code Review 意见

## 代码审查标准

- ✅ 通过所有测试
- ✅ 通过 `cargo clippy` 检查
- ✅ 代码已格式化 (`cargo fmt`)
- ✅ 添加必要的文档注释
- ✅ 遵循 Conventional Commits
- ✅ 包含适当的错误处理
- ✅ 添加 Prometheus 指标（如适用）
- ✅ 添加结构化日志（如适用）

## 许可协议

所有贡献将按照 AGPL-3.0 许可证发布。提交 PR 即表示您同意按此许可证贡献代码。

## 问题反馈

- 使用 GitHub Issues 报告 Bug
- 提供详细的复现步骤
- 包含环境信息（OS、Rust 版本等）
- 附上相关日志输出

## 社区准则

- 尊重他人
- 建设性的讨论
- 欢迎新手提问
- 帮助他人成长
