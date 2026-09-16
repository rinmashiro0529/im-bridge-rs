# 配置参考

[English](configuration.md)

默认使用环境变量配置。也可通过 `--config` 提供 JSON 文件，其字段参见 `config.example.json`。

## 核心配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `IMBRIDGE_LISTEN` | `127.0.0.1:8787` | 除非有 TLS 和网络访问控制，否则保持 loopback。 |
| `IMBRIDGE_DATA_DIR` | `./data` | 保存 SQLite 数据和 assets。 |
| `IMBRIDGE_MASTER_KEY_PATH` | `./master.key` | 必须是 32 字节普通文件；Unix 上仅 owner 可读写。 |
| `IMBRIDGE_SESSION_TTL_HOURS` | `12` | 范围 1–168。 |
| `IMBRIDGE_COOKIE_SECURE` | `true` | 仅直接访问 loopback HTTP 的开发环境可设为 false。 |
| `IMBRIDGE_TELEGRAM_API_BASE` | `https://api.telegram.org` | 必须 HTTPS；本机测试 API 可用 loopback HTTP。 |

## SillyTavern 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `IMBRIDGE_ST_BASE_URL` | 未设置 | 必须 HTTPS，loopback HTTP 除外。 |
| `IMBRIDGE_ST_HANDLE` | `default-user` | SillyTavern 用户 handle。 |
| `IMBRIDGE_ST_HOST_HEADER` | 未设置 | 仅用于可信本机反向代理。 |
| `IMBRIDGE_ST_MODE` | `disabled` | `disabled`、`read_only`、`test_write`、`production_write`。 |
| `IMBRIDGE_ST_CONNECTOR_HMAC_KEY` | 未设置 | 写入模式至少 32 字节，通过受保护环境文件或 secret manager 注入。 |
| `IMBRIDGE_ST_TIMEOUT_MS` | `15000` | 100–120000。 |
| `IMBRIDGE_ST_GENERATE_HARD_TIMEOUT_MS` | `900000` | 1000–3600000。 |
| `IMBRIDGE_ST_GENERATE_IDLE_TIMEOUT_MS` | `90000` | 1000–600000。 |

非法值会导致启动失败，不会静默回退。不要把真实秘密写入 `config.example.json`、`.env.example`、命令示例、日志或 issue。
