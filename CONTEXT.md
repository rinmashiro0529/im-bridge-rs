# Public project context

IM Bridge is a pre-release Telegram-to-SillyTavern sidecar. SillyTavern owns all role-play business data and generation. Rust owns transport, durable coordination, encrypted local integration secrets, and administration.

## Non-negotiable invariants

1. Rust does not directly read or write a SillyTavern production data directory.
2. Native Rust character/conversation/provider tables are not a runtime fallback.
3. Mutations require Connector authentication, CAS/integrity checks, and durable operation IDs.
4. A lost commit response is reconciled with the same operation ID; the business mutation is not blindly repeated.
5. Poller ownership must be exclusive for each Telegram bot.
6. Production credentials and deployment evidence do not belong in this repository.

Private deployment names, network topology, bot identities, and canary evidence are intentionally excluded from this public context.
