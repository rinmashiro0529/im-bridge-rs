# 部署

[English](deployment.md)

## 推荐拓扑

使用无特权账号运行 IM Bridge，并默认绑定 loopback。仅在确实需要远程管理时通过 TLS 反向代理开放管理 API，并使用私网或显式防火墙策略限制访问。

1. 执行 `cargo build --release --locked`。
2. 创建专用 `imbridge` 用户和 `/var/lib/im-bridge` 数据目录。
3. 直接生成 32 字节 Master Key 到 root 管理、权限 `0600` 的文件。
4. 把 Connector HMAC key 写入权限 `0600` 的 `/etc/im-bridge/im-bridge.env`。
5. 安装并按环境调整 `deploy/im-bridge.service`。
6. 从受保护来源把密码通过 stdin 传给 `--password-file -`，创建管理员。
7. 先以 `read_only` 启动，验证 `/health/ready`，使用隔离测试 Bot，最后再启用写入。

不要在 loopback 之外直接暴露明文 HTTP。确认 poller ownership 前不要启动 Bot。数据库备份不包含 Master Key，必须通过独立受限渠道备份。
