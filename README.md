# IM Bridge

[简体中文](README.zh-CN.md) · [Security](SECURITY.md) · [Contributing](CONTRIBUTING.md)

IM Bridge is a Telegram-to-[SillyTavern](https://github.com/SillyTavern/SillyTavern) sidecar written in Rust. SillyTavern remains the source of truth for characters, chats, model settings, provider secrets, and generation. IM Bridge owns Telegram transport, durable updates, bindings, operation recovery, delivery tracking, encrypted local secrets, and a small administration API.

> **Project status:** pre-release. Native character, provider, and conversation routes are intentionally retired. Do not treat this repository as production-ready until the release checklist in [Project status](docs/project-status.md) is complete.

## Security model

- No fallback to a local chat engine when SillyTavern is unavailable.
- Chat mutations use a signed Connector protocol with operation IDs, CAS/integrity fencing, replay, and commit-state tracking.
- Bot tokens are encrypted at rest with XChaCha20-Poly1305.
- Passwords use Argon2id.
- Administration requires a session and CSRF token. There is no automatic-login mode.
- HTTP backend URLs are accepted only over HTTPS, except loopback HTTP for local sidecars.

Read [Security model](docs/security-model.md) before deployment. Report vulnerabilities according to [SECURITY.md](SECURITY.md).

## Architecture

```text
Telegram
   │
   ▼
IM Bridge (Rust)
   ├── durable update inbox and poller ownership
   ├── account/binding/channel context
   ├── operation and delivery ledgers
   ├── encrypted SecretVault
   └── administration API
             │ signed Connector + ST API
             ▼
        SillyTavern
        ├── characters and chats
        ├── model/provider settings
        └── generation
```

See [Architecture](docs/architecture.md) for component and failure-state details.

## Requirements

- Rust 1.98.0 (pinned by `rust-toolchain.toml`)
- SQLite
- A compatible SillyTavern instance and IM Bridge Connector for sidecar operations
- A Telegram Bot token for Telegram transport
- Linux is the primary deployment target

## Quick start (local, read-only)

```bash
cargo build --locked
printf '%s\n' 'replace-with-a-long-local-password' | \
  cargo run --locked -- bootstrap-admin --username admin --password-file -
IMBRIDGE_COOKIE_SECURE=false \
IMBRIDGE_ST_MODE=disabled \
cargo run --locked -- serve
```

The service listens on `127.0.0.1:8787` by default. `IMBRIDGE_COOKIE_SECURE=false` is only appropriate for direct loopback development. Use TLS and secure cookies for every non-loopback deployment.

For a complete configuration reference, see [Configuration](docs/configuration.md). For systemd and reverse-proxy guidance, see [Deployment](docs/deployment.md).

## Commands

```text
im-bridge serve
im-bridge doctor
im-bridge bootstrap-admin --username <name> --password-file <path-or->
im-bridge import-st --data-root <path> [--plugin-db <path>] --dry-run
im-bridge export-st --workspace <id> --output <path>
im-bridge backup --output <path>
im-bridge rotate-master-key --new-key <path> --dry-run
```

`rotate-master-key` is deliberately dry-run only until a crash-recoverable two-phase rotation protocol is implemented.

## Development checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all --locked
# Isolated sidecar E2E tests require --features e2e-control and explicit IMBRIDGE_E2E_* endpoints.
cargo build --release --locked
```

Pull requests must also pass dependency, license, CodeQL, and secret-scanning workflows.

## Repository boundaries

This repository must never contain real bot tokens, HMAC keys, master keys, cookies, databases, backups, production logs, internal hostnames, or private deployment evidence. Fixtures must be synthetic.

## License

Licensed under the [MIT License](LICENSE).
