# Recovery payload validation: one core, unchanged entry contracts

## Scope

Lightweight audit C03, following the full audit's requirement to retain negative
security tests. This change only factors repeated code in
`OperationCoordinator`; it does not change cryptography, persisted formats,
public signatures, dependencies, schema, replay/CAS behavior or release options.
The reviewed baseline is `01d4fb24503adc74508c8a6e3b35facf05cc204b`.

## Design and observable ordering

Both public entry points retain their own key source and scope checks. Two
private helpers implement the identical payload/record precheck and the
AAD/decrypt/JSON/domain-validation/digest sequence. Neither helper obtains keys,
accesses storage, starts generation, commits an operation or delivers a message.

| Order | Synchronous entry | Asynchronous entry |
| --- | --- | --- |
| 1 | Require a fixed encryptor | Restore the historical operation key |
| 2 | Require a payload, then nonblank record id and bot id | Same |
| 3 | Validate fence and explicit internal_bot_id | Validate fence/bot match |
| 4 | Shared AAD, authenticated decryption, JSON, domain validation, digest | Same |

The two scope-error messages remain different, as before. Key errors still take
precedence over payload and scope errors. Invalid JSON/domain payloads are
rejected before a mismatched digest. The async path never calls key creation
while recovering; a missing key reference is an error, not a rotation request.
The sync path still fails for a managed-only coordinator.

The AAD field order, length encoding and fence contents are untouched. Mutation
payloads retain their compact-mutation digest, while Create retains the digest
of the original plaintext bytes, including whitespace. Records without a digest
retain their existing compatibility behavior; all other validation still runs.

## Regression evidence to require

`tests/recovery_payload_contract.rs` runs in the ordinary default-feature suite.
It exercises both fixed-key entry points, all five payload variants, optional
digests, error code/status/message and precedence, AAD retargeting, ciphertext,
nonce, wrong key/version, invalid JSON and domain payloads. Pretty-encoded Create
payloads detect accidental digest re-serialization. Managed-key cases use real
SQLite migrations and an encrypted vault, reload a persisted generated record,
restore its historical key version, assert no new secrets/references or changed
operation rows, and reject missing key references. No test accesses real ST or
Telegram; the scripted backend's call log must remain empty.

Before accepting the refactor, run the new tests against the old implementation,
then the new one. Deliberately removing digest/domain validation or replacing
restore with key creation must compile but fail the targeted regression test.
Compilation failure is not evidence that a mutation was caught.

```sh
cargo test --locked --test recovery_payload_contract
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all --locked
cargo build --release --locked
python3 scripts/publication_gate.py
```

## Review, accounting and limits

Review key acquisition and scope predicates before reviewing the shared core.
Count source, tests and this note separately after formatting; helper lines count
against source savings. No binary-size improvement is claimed without a measured
comparison. Whole-stack E2E, cancellation during commit, hardware power failure
and dependency advisories are separate gates, not validated by these tests.

No data migration is needed to revert the refactor. Keep the regression tests
when reverting if possible. Validation scripts and temporary workflows are not
part of the product PR; execution records belong in its linked progress issue
and GitHub Actions logs.
