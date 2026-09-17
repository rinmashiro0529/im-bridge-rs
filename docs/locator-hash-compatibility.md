# Canonical locator hashing compatibility

Audit mapping: lightweight C02. This is a maintenance refactor, not a new hashing scheme or cryptographic primitive.

## One implementation, unchanged public paths

The canonical implementation lives in the internal `domain::locator` module. Both existing public paths remain callable with the original signature through re-exports:

```rust
im_bridge::modules::bridge::st_ops::locator_hash
im_bridge::modules::bridge::operation_payload::locator_hash
```

Callers are not required to migrate. The internal domain module is not added as a new public API. There are no new dependencies, feature flags, schema changes, migrations, release-profile changes or security-policy exceptions.

## Protocol invariants

For each of `handle`, `avatar`, and `chat_file`, in that order, append an unsigned 64-bit big-endian UTF-8 byte length followed by the exact UTF-8 bytes. Return lowercase hexadecimal SHA-256 of the concatenation.

Do not substitute character counts, native/little-endian integers, delimiter concatenation, JSON serialization or Unicode normalization. This hash participates in persisted identities and authenticated recovery data; changing its bytes would not be an acceptable code-size optimization.

The implementation deliberately retains the original allocation/encoding structure. Streaming hash updates or other performance changes should be measured and reviewed separately rather than mixed into this duplicate-removal change.

## Validation

`tests/locator_hash_contract.rs` tests both public entry points against 11 fixed vectors across three independently discovered tests. Coverage includes empty fields, ambiguous concatenations, colons, embedded NUL/newline bytes, Chinese characters, emoji, composed/decomposed Unicode, and lengths beyond 8-bit and 16-bit ranges. The expected strings were computed with Python's standard-library SHA-256, not by either implementation under test.

An independent reproduction of the encoding is:

```python
import hashlib
import struct

def expected(parts):
    raw = [part.encode("utf-8") for part in parts]
    framed = b"".join(struct.pack(">Q", len(part)) + part for part in raw)
    return hashlib.sha256(framed).hexdigest()
```

Targeted checks:

```sh
cargo test --locked --test locator_hash_contract --test st_hash_parity --test operation_payload
```

Run these on the original implementations before refactoring and on the shared implementation afterward. The new suite must also detect deliberate code-point-length and little-endian mutations. These are focused mutation checks, not a comprehensive formal proof. Full default tests, Clippy, formatting, publication checks and release builds still apply.

## Size accounting and rollback

Count the neutral module and re-exports when reporting production source changes; additional tests and this design note are not hidden as code savings. Final executable bytes and fixed-parameter compressed bytes must be measured on the same toolchain, target, path and release profile. A smaller source tree does not imply a smaller executable.

Reverting this refactor requires no data migration because the persisted encoding and public call paths are unchanged. Retain the compatibility vectors even if the refactor is reverted.
