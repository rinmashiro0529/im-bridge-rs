# Configuration

[简体中文](configuration.zh-CN.md)

Environment variables are the default configuration mechanism. A JSON file can be passed with `--config`; it must contain the fields shown in `config.example.json`.

## Core settings

| Variable | Default | Notes |
| --- | --- | --- |
| `IMBRIDGE_LISTEN` | `127.0.0.1:8787` | Keep loopback unless protected by TLS and network controls. |
| `IMBRIDGE_DATA_DIR` | `./data` | Contains SQLite data and assets. |
| `IMBRIDGE_MASTER_KEY_PATH` | `./master.key` | Must be a 32-byte regular file with owner-only permissions on Unix. |
| `IMBRIDGE_SESSION_TTL_HOURS` | `12` | Allowed range: 1–168. |
| `IMBRIDGE_COOKIE_SECURE` | `true` | Set false only for direct loopback HTTP development. |
| `IMBRIDGE_TELEGRAM_API_BASE` | `https://api.telegram.org` | HTTPS required; loopback HTTP is allowed for a local test API only. |

## SillyTavern settings

| Variable | Default | Notes |
| --- | --- | --- |
| `IMBRIDGE_ST_BASE_URL` | unset | HTTPS required, except loopback HTTP. |
| `IMBRIDGE_ST_HANDLE` | `default-user` | SillyTavern user handle. |
| `IMBRIDGE_ST_HOST_HEADER` | unset | Optional explicit Host header for a trusted local reverse proxy. |
| `IMBRIDGE_ST_MODE` | `disabled` | `disabled`, `read_only`, `test_write`, or `production_write`. |
| `IMBRIDGE_ST_CONNECTOR_HMAC_KEY` | unset | At least 32 bytes in write modes; inject through a protected environment file or secret manager. |
| `IMBRIDGE_ST_TIMEOUT_MS` | `15000` | 100–120000. |
| `IMBRIDGE_ST_GENERATE_HARD_TIMEOUT_MS` | `900000` | 1000–3600000. |
| `IMBRIDGE_ST_GENERATE_IDLE_TIMEOUT_MS` | `90000` | 1000–600000. |

Invalid values fail startup instead of silently falling back. Never put real secrets in `config.example.json`, `.env.example`, command examples, logs, or issue reports.
