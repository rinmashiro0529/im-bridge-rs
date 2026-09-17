# Identity lifecycle invariants

This note implements audit findings A02/A06 and lightweight recommendation C15.
Public Rust method signatures, CLI commands, schema, password parameters and wire
formats remain unchanged. It intentionally fixes disabled-account authorization
and partial bootstrap rather than treating those defects as compatibility.

## Authorization boundary

`actor_in_workspace` reloads the account from SQLite before checking enabled
status and administrator privileges. A caller's `Account` is a snapshot, not an
authorization grant. This covers HTTP and bound Telegram resolution, stale
account values and administrator demotion. `create_account` uses the same fresh
account check. Missing accounts are unauthorized; disabled accounts are forbidden.
The existing HTTP guard remains. The extra lookup is an intentional correctness
cost, not a performance optimization.

This does not retroactively cancel an operation already executing when an account
is disabled. Commit-time reauthorization and session/binding revocation policy
remain separate work. No binding is deleted or re-enabled by this change.

## Atomic provisioning

A private AccountSeed and one transaction now create account, workspace, owner
membership and workspace settings. New imported accounts store their legacy
handle in the same transaction. Hashing occurs before acquiring a write
transaction. Public validation stays at its previous entry points: bootstrap and
admin-created users retain strict field validation; legacy handles retain their
existing acceptance rules.

| Entry | Workspace name | Prompt user name | Administrator |
| --- | --- | --- | --- |
| bootstrap-admin | Default | User | yes |
| create_account | display name | display name | requested role |
| new legacy account | display name | display name | no |

All retain legacy_bridge_v1. The result is returned after commit, without a
fallible post-commit lookup. New imported Account results now include the same
legacy handle as their stored row. SQLx transaction rollback protects all earlier
rows on failure. Database uniqueness arbitrates concurrent bootstrap attempts:
a loser may return an error but must leave no partial rows; explicit retry is safe.

## Repeated bootstrap is not repair or password reset

An existing enabled administrator is accepted only if its selected default
workspace has the account as creator and owner, and has a settings row. Existing
non-administrators or disabled accounts return BOOTSTRAP_ACCOUNT_CONFLICT;
incomplete setup returns BOOTSTRAP_INCOMPLETE. A successful repeat does not change
password, display name, role, settings or row counts. This does not silently
promote an account, re-enable it or invent missing data. Restore a verified
consistent backup for incomplete prior initialization.

## Negative tests and review

`tests/identity_lifecycle.rs` covers repeated bootstrap, invalid existing roles,
three incomplete-workspace cases, four insert-failure points through all three
creation paths, retry after rollback, concurrent creation, stale administrator
values, existing HTTP sessions and Telegram commands/callbacks for disabled
administrators and members, re-enabling, revoked bindings and legacy defaults.
Fixtures use temporary databases, synthetic identities and no real Telegram/ST.

Passwords are generated independently for each fixture, held in a fixture-owned
`Zeroizing<String>`, and passed explicitly to the in-process login helper. The
repeated-bootstrap test uses a guaranteed-distinct alternate value and still
asserts that the original password works while the alternate password does not.
There is no shared hard-coded password. This test-data correction retains all
eight test cases and their assertions; it does not suppress CodeQL rules, exclude
the tests from analysis, or change production authentication behavior.

Run the normal contribution gates and:

```sh
cargo test --locked --test identity_lifecycle
```

Review the transaction boundary, preserved per-entry validation/defaults, and
fresh authorization lookup separately. This change neither introduces local-chat
fallback nor weakens CSRF, dependency checks or publication safety.
