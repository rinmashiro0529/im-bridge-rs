# Remove the unused Telegram framework without changing transport capabilities

Audit mapping: lightweight D01. This is dependency-closure reduction, not removal of Telegram functionality.

## Scope and evidence

The application's checked-in Rust sources do not reference the third-party `teloxide` crate as an identifier. The public `adapters::telegram::teloxide_runtime::TeloxideRuntime::start` wrapper delegates to `TelegramModule::start_bot`; it is retained unchanged. Production polling, durable inbox handling, delivery, ownership checks and shutdown remain application code. No production Rust source, public wrapper, schema, CLI or release-profile setting changes in this PR.

A source scan alone is insufficient. The candidate must also compile all targets/features, pass the complete default-feature suite, and retain the resolved network capabilities used by the executable. The added transport contract tests run on the original dependency graph before the manifest change and on the candidate afterward.

## Why this is not a one-line deletion

The previously unused framework still enabled reqwest features transitively through Cargo feature unification: `rustls-tls` (WebPKI root certificates) and `multipart`. Merely deleting the dependency would remove those features while retaining native roots. That is a change in transport capability, not a safe no-behavior-change cleanup.

The manifest now owns those two reqwest features explicitly, alongside the existing `json`, `stream` and `rustls-tls-native-roots`. Both certificate sources, the Rustls provider, HTTP protocol features, Tokio signal/runtime support and middleware features are preserved. This is not an endorsement of removing certificate validation or switching trust stores for size savings.

The validation compares resolved normal/build network-feature unions for default and all project features on Linux x86-64, Linux AArch64, Windows MSVC and macOS AArch64. Graph resolution is not cross-platform compilation or runtime certification; actual compilation and smoke execution use Linux x86-64.

## Lockfile and advisory policy

Cargo regenerates the lockfile from the existing resolution after the manifest edit. Retained package names, versions, sources and checksums must not change; the validation rejects additions or upgrades. Unneeded packages are pruned rather than manually editing package records.

Once `proc-macro-error2` is absent from the resulting lockfile, its obsolete `RUSTSEC-2026-0173` exception is removed from `deny.toml`. No advisory is newly ignored. Other existing advisories, including an RSA finding if it remains in the lockfile, must still be reported and analyzed separately. Removing an unused framework is not a claim that all security checks pass.

## Validation contract

`tests/transport_dependency_contract.rs` preserves two concrete contracts:

- The reqwest client can be built with both native and WebPKI certificate features, and retains multipart request construction without making an external request.
- The historical public runtime entry point still returns `BOT_TOKEN_MISSING` and does not start a bot when no token is configured.

The isolated validation also exercises both baseline and candidate release executables: root help and all seven subcommand help pages, loopback health endpoints and static assets, unauthenticated API rejection, and clean SIGTERM/SIGINT shutdown with no active bots. Help output and local response bytes/headers are compared before and after. This does not certify in-flight bot draining or a live Telegram/ST deployment.

Required checks remain:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all --locked
cargo build --release --locked
python3 scripts/publication_gate.py
```

Real Telegram/ST/Connector E2E and deployment-specific TLS tests remain release acceptance work; they are not replaced by feature inspection or request construction.

## Measurement and rollback

Report lockfile package count separately from compiled normal/build dependencies, executable bytes and fixed-parameter gzip bytes. A package removed from Cargo.lock was not necessarily linked into the executable. Compare the two release binaries on the same runner, worktree path, toolchain and unchanged release profile. Do not sum dependency source sizes and call the result binary savings.

The PR contains no generated binaries or raw runner logs. Measurements and exact command outcomes belong in the PR validation record. No broad benchmark claim is made from a cache-warm build.

Rollback restores the original manifest, lockfile and obsolete exception as one commit; no database or secret migration is needed. Keep compatibility tests independent of the framework so they remain useful in either configuration.
