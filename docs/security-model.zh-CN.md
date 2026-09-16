# 安全模型

[English](security-model.md)

## 资产

Bot Token、Connector HMAC key、Master Key、Session Cookie、operation payload key、聊天内容和数据库副本均属于敏感资产。

## 信任边界

- 管理浏览器已认证，但其输入不可信。
- Telegram 和 Provider 响应属于外部输入。
- SillyTavern 与 Connector 只有在 Session 和机器认证通过后才可信。
- SQLite 是本地持久化状态，不是 SillyTavern 业务数据的替代来源。

## 控制

密码使用 Argon2id；秘密使用带随机 nonce 和 AAD 的 XChaCha20-Poly1305；Connector 请求使用包含时间戳和 nonce replay 防护的 HMAC-SHA256；mutation 使用 integrity fencing，operation ID 持久化；外部错误在保存或展示前应完成脱敏。

## 部署假设

服务使用无特权账号运行；Master Key 与环境文件仅 owner 可读；loopback 外的管理端使用 TLS；每个 Telegram Bot 同时只有一个 poller owner。

## 明确非目标

IM Bridge 无法防护完全失陷的主机、恶意 SillyTavern/Connector 进程、被盗 Master Key 或泄漏的 Telegram Token，也不在相互敌对的系统管理员之间提供多租户隔离。
