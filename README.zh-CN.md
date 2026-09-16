# IM Bridge

[English](README.md) · [安全政策](SECURITY.md) · [贡献指南](CONTRIBUTING.md)

IM Bridge 是一个使用 Rust 编写的 Telegram 到 [SillyTavern](https://github.com/SillyTavern/SillyTavern) sidecar。SillyTavern 始终是角色、聊天、模型设置、Provider Secret 和生成的唯一业务来源；IM Bridge 负责 Telegram 传输、可靠更新收件箱、账号绑定、操作恢复、投递账本、本地加密秘密以及小型管理 API。

> **项目状态：** 尚未正式发布。原生角色、Provider 和 Conversation 路由已主动退役。在 [项目状态](docs/project-status.zh-CN.md) 的发布检查完成前，请勿将本项目视为可直接生产部署。

## 安全模型

- SillyTavern 不可用时，不回退到本地聊天引擎。
- 聊天写入通过签名 Connector 协议完成，并使用 operation ID、CAS/integrity fencing、replay 和 commit-state 跟踪。
- Bot Token 使用 XChaCha20-Poly1305 加密存储。
- 密码使用 Argon2id。
- 管理面必须使用 Session 和 CSRF Token；不存在自动登录模式。
- 后端 URL 必须使用 HTTPS；只有本机 loopback sidecar 允许 HTTP。

部署前请阅读[安全模型](docs/security-model.zh-CN.md)。安全问题请按 [SECURITY.md](SECURITY.md) 私下报告。

## 架构

```text
Telegram
   │
   ▼
IM Bridge（Rust）
   ├── durable update inbox 与 poller ownership
   ├── 账号、绑定与 channel context
   ├── operation 与 delivery ledger
   ├── 加密 SecretVault
   └── 管理 API
             │ 签名 Connector + ST API
             ▼
        SillyTavern
        ├── 角色与聊天
        ├── 模型与 Provider 设置
        └── 生成
```

组件及故障状态详见[架构文档](docs/architecture.zh-CN.md)。

## 环境要求

- Rust 1.98.0（由 `rust-toolchain.toml` 固定）
- SQLite
- 兼容的 SillyTavern 与 IM Bridge Connector
- 用于 Telegram transport 的 Telegram Bot Token
- 主要部署目标为 Linux

## 本机只读快速开始

```bash
cargo build --locked
printf '%s\n' 'replace-with-a-long-local-password' | \
  cargo run --locked -- bootstrap-admin --username admin --password-file -
IMBRIDGE_COOKIE_SECURE=false \
IMBRIDGE_ST_MODE=disabled \
cargo run --locked -- serve
```

服务默认监听 `127.0.0.1:8787`。`IMBRIDGE_COOKIE_SECURE=false` 只适用于直接访问 loopback 的本地开发；任何非 loopback 部署都应使用 TLS 和 Secure Cookie。

完整配置见[配置参考](docs/configuration.zh-CN.md)，systemd 与反向代理指导见[部署文档](docs/deployment.zh-CN.md)。

## 命令

```text
im-bridge serve
im-bridge doctor
im-bridge bootstrap-admin --username <name> --password-file <path-or->
im-bridge import-st --data-root <path> [--plugin-db <path>] --dry-run
im-bridge export-st --workspace <id> --output <path>
im-bridge backup --output <path>
im-bridge rotate-master-key --new-key <path> --dry-run
```

在实现可崩溃恢复的两阶段轮换协议前，`rotate-master-key` 仅允许 dry-run 验证。

## 开发门禁

```bash
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all --locked
# 隔离 sidecar E2E 需启用 --features e2e-control 并显式配置 IMBRIDGE_E2E_* 端点。
cargo build --release --locked
```

Pull Request 还必须通过依赖、许可证、CodeQL 和秘密扫描。

## 仓库边界

本仓库不得包含真实 Bot Token、HMAC Key、Master Key、Cookie、数据库、备份、生产日志、内部主机名或私有部署证据。所有 fixture 必须为合成数据。

## 许可证

使用 [MIT License](LICENSE) 发布。
