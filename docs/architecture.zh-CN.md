# 架构

[English](architecture.md)

IM Bridge 使用 ports-and-adapters 边界隔离 Telegram transport、SillyTavern 访问、加密秘密和持久化操作。

## 所有权

| 组件 | 负责内容 |
| --- | --- |
| SillyTavern | 角色、聊天、设置、Provider、生成 |
| Connector | 认证的 typed mutation、CAS/integrity 检查、operation replay |
| IM Bridge | Telegram update、绑定、channel context、operation、delivery ledger、本地集成秘密 |

## 读取路径

Telegram 与管理查询进入 `StBridgeEngine`，再委托给 `StBackend`。SillyTavern 读取失败会显式返回错误，不读取 native 业务表。

## 写入路径

1. 校验 actor、Bot ownership、channel locator 和写入就绪状态。
2. 领取持久化 operation ID 和执行 lease。
3. 读取包含 integrity 的 SillyTavern snapshot。
4. 生成或准备 typed mutation。
5. 通过认证 Connector 使用 CAS/integrity fencing 提交。
6. 响应不明确时使用相同 operation ID reconcile。
7. 向 Telegram 投递已确认结果并保存 terminal evidence。

## 故障模型

Commit state 区分 `not_started`、`not_applied`、`applied` 和 `unknown`。`unknown` 必须 reconcile，不能盲目重试。Telegram delivery 使用独立账本，因为 ST 已确认提交和渠道投递不确定可以同时存在。

## 旧代码

Native character、conversation、provider 和 direct-LLM 模块仅为迁移兼容与测试保留；相关 HTTP 路由已退役，`tests/no_local_fallback.rs` 保护生产不回退约束。
