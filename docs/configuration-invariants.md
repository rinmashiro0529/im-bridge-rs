# Configuration validation invariants

This change addresses full audit A16 and lightweight C16 without introducing a
new configuration framework. Inclusive numeric ranges are declared once and
shared by environment parsing and final validation. The two former signed and
unsigned parsers share one small generic parser and bounded-value helper.

ST mode uses the same explicit default function for Rust Default and serde field
omission. Explicit invalid modes remain errors. JSON/direct construction now
rejects the out-of-range timeouts that environment parsing already rejected.
That rejection and accepting omitted mode as disabled are intentional bug fixes;
this is not described as a completely behavior-neutral refactor.

The existing file-over-environment source policy, field names, CLI, public
structs, HTTPS/loopback restrictions, cookie constraints, HMAC minimum length,
and unknown-field rejection are unchanged. No environment overlay is introduced
that could accidentally enable production writes or replace protected secrets.
Numeric diagnostics do not echo supplied values, including invalid UTF-8.

`tests/config_contract.rs` checks direct/JSON final validation, real CLI child
processes for both sources and inclusive boundaries, negative and overflowing
numbers, invalid UTF-8 on Unix, explicit file precedence and security guards.
Each process receives an isolated environment and temporary paths; tests do not
mutate process-global environment variables and never contact Telegram or ST.
Invalid configuration must not create a database directory or master key.

Run normal contribution gates and `cargo test --locked --test config_contract`.
The existing JSON examples remain compatible. Before reverting, note that
removing the fix would again permit out-of-range JSON timeouts and reject partial
ST objects without mode. No database or wire-format migration is required.
