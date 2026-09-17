# Shared lock implementation, independent lock domains

Implements lightweight audit C01. Both existing public manager types, module
paths, constructors, methods and capacity-error strings are preserved. Delivery
continues accepting String; locator continues accepting &str. Each manager owns
its own concrete String-keyed LockTable, capacity, TTL and entries. No global
state, dependency, arbitrary-key framework or backend abstraction is added.

## Invariants

The table mutex protects eviction, lookup, timestamp update and Arc cloning.
It is released before awaiting a per-key mutex. Both holders and queued waiters
retain an Arc, so neither can be evicted. Dropping a cancelled waiter releases
its Arc. TTL and oldest-idle eviction retain their prior semantics, including
updating last_used on acquisition requests rather than on guard release. Zero
capacity still fails closed with the manager-specific error. Capacity failure
never replaces an active key's mutex.

The only source macro generates mechanical forwarding methods to preserve both
public APIs without repeating every forwarding declaration. It generates no
locking algorithm or business policy. Acquire and its manager-specific error
mapping remain explicit in each wrapper. String inputs are moved, not cloned.
The &str wrapper owns its key before invoking the shared core.

## Review and verification

The existing tests remain. `tests/lock_manager_contract.rs` runs six identical
contract cases through each public manager and one cross-domain case. Waiters
are explicitly polled to Pending, avoiding assumptions about task scheduling;
TTL-zero tests avoid wall-clock sleeps. Only the oldest-idle ordering test uses
a short elapsed-time interval, since the original contract uses std::Instant.

```sh
cargo test --locked --test lock_manager_contract
cargo test --locked --test delivery_lock_manager --test st_locator_concurrency
```

Run all normal contribution gates as well. This is a source-maintenance change;
no binary-size or performance improvement is claimed without measurement.
Count the shared core, wrapper macro and both call sites together when reporting
source reduction. Tests and this design note are additional quality coverage,
not part of a claimed repository-wide line reduction. Revert the source change
and its new tests together; no schema migration or protocol migration is needed.
