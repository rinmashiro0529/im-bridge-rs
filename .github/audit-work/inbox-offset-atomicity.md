# Telegram inbox offset publication

Audit mapping: full audit A01. This change intentionally precedes lightweight C09; it does not combine the single-update and batch persistence paths.

## Invariant

A running poller may publish a new in-memory offset only after the transaction containing the inbox rows and durable offset succeeds. An error must leave the caller's offset unchanged. The caller currently continues polling after an inbox error, so publishing a candidate early could acknowledge an update that was rolled back.

The Telegram [getUpdates contract](https://core.telegram.org/bots/api#getupdates) confirms an update when a subsequent request supplies a greater offset. This makes the offset a data-safety boundary, not just a cursor optimization.

## Minimal change

`persist_update_batch` accumulates `candidate_offset`, binds that value to the existing offset UPSERT, commits the transaction, and only then copies the candidate to the caller. There is no further await between successful commit and publication. No schema, public API, hash, write-mode, ownership, generation or delivery policy changes are introduced.

On commit error or cancellation before successful completion, the unpublished candidate is discarded. A durable commit whose acknowledgement was lost may be observed again with a conservative offset; existing identity checks and durable processing claims remain responsible for replay handling. This patch does not attempt to solve distributed exactly-once delivery.

## Preserved behavior

- Negative IDs and `i64::MAX` overflow are rejected with the existing error code.
- Missing or non-integer IDs retain the existing skip behavior.
- Raw JSON and SHA identity comparisons are unchanged.
- `received` and `failed` rows retain their retry selection; `processing` and `processed` rows are not returned.
- Batch output order and duplicate entries in a single batch are unchanged. Durable processing claims still suppress repeated execution.
- Offsets advance monotonically for out-of-order input. Startup still reads the committed database offset.
- A batch uses one transaction; it is not replaced with independently committed per-update calls.

## Regression evidence

The private `inbox_offset_tests` module runs in the normal default-feature unit suite, using the real SQLx pool, repository migrations and temporary SQLite files. It never contacts Telegram or SillyTavern and uses no real credentials.

Eight tests cover:

1. Identity conflict after an earlier valid update, rollback and corrected retry.
2. Offset write failure after inbox insertion, rollback and retry after removing the fault.
3. A deferred foreign-key violation at COMMIT, including rollback of trigger effects and retry after satisfying the constraint. A separate explicit transaction proves the constraint permits statement execution before commit.
4. Invalid/overflowing second IDs, with no partial batch or early offset publication.
5. Out-of-order input and all four existing inbox statuses during replay.
6. Same-body duplicate entries without changing the existing dispatch contract.
7. Reopening the database, loading the durable offset and skipping a processed replay.
8. Empty batches and missing/non-integer IDs.

Targeted command:

```sh
cargo test --locked --lib modules::telegram::inbox_offset_tests
```

Before applying the production fix, the four rollback tests must fail at the offset assertion while the four compatibility tests pass. After the fix, all eight must pass. Full formatting, Clippy, default tests, publication checks and release build remain required; a green targeted suite is not a substitute for those gates.

## Limits and rollback

These tests exercise SQL statement and transaction failures, not hardware power loss, real Bot API delivery or the external Connector. WAL synchronization policy and independent recovery scheduling are separate audit items. No dependency or security check is disabled.

Reverting the production change requires no migration but reintroduces the early-acknowledgement risk. Keep this safety fix separate from subsequent inbox de-duplication so each change can be reviewed against a stable regression suite.
