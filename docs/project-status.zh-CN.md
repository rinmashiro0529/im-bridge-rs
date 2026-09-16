# 项目状态

[English](project-status.md)

IM Bridge 尚未正式发布。Sidecar 主路径、Telegram 可靠传输、operation recovery 与 no-fallback 负向测试已经实现；native chat 路由已退役，但 legacy 模块仍为迁移兼容保留。

## 发布阻断项

- 完整 Linux CI 矩阵必须通过。
- RustSec、许可证、CodeQL 与秘密扫描必须通过。
- SillyTavern Connector 必须单独完成安全审计。
- 尚未实现可崩溃恢复的在线 Master Key 轮换，实际 mutation 已禁用。
- 进入 production write 前必须完成受控 read-only 与测试 Bot canary。

在这些门禁关闭前，不承诺生产支持。
