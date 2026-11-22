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

### 5. 阶段验证与自动提交

每个独立开发阶段结束后，必须执行 `DEVELOPMENT.md` 中的自动化质量门禁。推荐直接运行 `scripts/commit-stage.sh 'feat(scope): summary' path...`：脚本会格式化指定 Rust 文件、执行完整编译、单元测试和兼容集成测试、检查 diff，并只暂存列出的文件后自动创建 Conventional Commit。检查失败时先修复，不得提交；确认属于仓库既有基线问题时必须明确记录，不能声称检查通过。禁止盲目执行 `git add .`。

自动提交不代表自动推送。未经明确要求，不得推送远程分支或创建 PR。

### 6. 推送并创建 Pull Request

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
