## Summary

## Security and privacy checklist
- [ ] No credentials, personal data, private hosts, production logs, databases, or internal evidence are included.
- [ ] Authentication/cryptography/schema changes include a focused design note and negative tests, or are not applicable.

## Validation
- [ ] `cargo fmt --check`
- [ ] `cargo clippy --all-targets --all-features --locked -- -D warnings`
- [ ] `cargo test --all --locked`
- [ ] `cargo build --release --locked`
