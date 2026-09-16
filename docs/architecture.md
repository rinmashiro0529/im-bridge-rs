# Architecture

[简体中文](architecture.zh-CN.md)

IM Bridge uses ports-and-adapters boundaries around Telegram transport, SillyTavern access, encrypted secrets, and durable operations.

## Ownership

| Component | Owns |
| --- | --- |
| SillyTavern | Characters, chats, settings, providers, generation |
| Connector | Authenticated typed mutations, CAS/integrity checks, operation replay |
| IM Bridge | Telegram updates, bindings, channel context, operations, delivery ledger, local integration secrets |

## Read path

Telegram and administration queries call `StBridgeEngine`, which delegates to `StBackend`. A failed SillyTavern read is returned as an explicit error; native business tables are not consulted.

## Write path

1. Validate actor, bot ownership, channel locator, and write readiness.
2. Claim a durable operation ID and execution lease.
3. Read an integrity-bearing SillyTavern snapshot.
4. Generate or prepare a typed mutation.
5. Commit through the authenticated Connector with CAS/integrity fencing.
6. Reconcile ambiguous responses with the same operation ID.
7. Deliver the confirmed result to Telegram and persist terminal evidence.

## Failure model

Commit state distinguishes `not_started`, `not_applied`, `applied`, and `unknown`. `unknown` must be reconciled, not blindly retried. Telegram delivery has a separate ledger because a confirmed SillyTavern commit can coexist with an uncertain channel delivery.

## Legacy code

Native character, conversation, provider, and direct-LLM modules remain only for migration compatibility and tests. Their HTTP routes are retired, and `tests/no_local_fallback.rs` guards the production invariant.
