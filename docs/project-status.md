# Project status

[简体中文](project-status.zh-CN.md)

IM Bridge is pre-release. The sidecar path, durable Telegram transport, operation recovery, and negative no-fallback tests are implemented. Native chat routes are retired but legacy modules remain for migration compatibility.

## Release blockers

- The complete Linux CI matrix must pass.
- RustSec, license, CodeQL, and secret scans must pass.
- The SillyTavern Connector must receive an independent security review.
- Crash-recoverable online master-key rotation is not implemented; mutation is disabled.
- A controlled read-only and test-bot canary must complete before production write mode.

No production-support promise is made before these gates are closed.
