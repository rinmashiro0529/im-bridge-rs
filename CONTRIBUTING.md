# Contributing

Thank you for helping improve IM Bridge.

## Before opening a pull request

1. Keep SillyTavern as the only source of truth for characters, chats, provider settings, and generation.
2. Do not introduce a local-chat fallback.
3. Do not commit credentials, databases, logs, internal addresses, real bot identities, or private deployment evidence.
4. Add or update tests for behavior changes.
5. Run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all --locked
# E2E tests are compiled with --all-features but run only in an approved isolated harness.
cargo build --release --locked
```

## Security-sensitive changes

Changes to authentication, authorization, cryptography, key lifecycle, Connector signing, CAS semantics, operation recovery, database schema, or production deployment defaults require a focused design note and negative tests.

## Commit and review scope

Keep changes focused. Generated files, local data, `.env` files, keys, and `target/` are not accepted. Use synthetic names and RFC-reserved examples in fixtures and docs.

By contributing, you agree that your contribution is licensed under the repository's MIT License.
