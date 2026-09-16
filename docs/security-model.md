# Security model

[简体中文](security-model.zh-CN.md)

## Assets

Bot tokens, Connector HMAC keys, the master key, session cookies, operation payload keys, chat contents, and database copies are sensitive.

## Trust boundaries

- The administration browser is authenticated but untrusted input.
- Telegram and provider responses are external input.
- SillyTavern and the Connector are trusted only after session and machine-authentication checks.
- SQLite is durable local state, not a substitute for SillyTavern business data.

## Controls

Passwords use Argon2id. Secrets use XChaCha20-Poly1305 with random nonces and AAD. Connector requests use HMAC-SHA256 with timestamp and nonce replay protection. Mutations are integrity-fenced and operation IDs are durable. External errors are expected to be redacted before persistence or display.

## Deployment assumptions

The service runs as an unprivileged user, the master key and environment file are owner-only, the administration endpoint uses TLS outside loopback, and only one poller owns each Telegram bot.

## Explicit non-goals

IM Bridge does not protect a fully compromised host, malicious SillyTavern/Connector process, stolen master key, or leaked Telegram token. It does not provide multi-tenant isolation across hostile system administrators.
