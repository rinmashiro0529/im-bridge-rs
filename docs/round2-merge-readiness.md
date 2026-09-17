# Round 2: integrated remediation and merge readiness

## Scope and invariant boundaries

This candidate integrates the reviewed heads of PRs #9, #10, #11, #12, #13,
#14 and #16 from main `01d4fb24503adc74508c8a6e3b35facf05cc204b`.
Those original contributions remain attributable in history. It adds live inbox
retry, SQLx 0.9 compatibility, executed local transport contracts and CI changes.
It does not blindly restore closed HMAC/SHA/rand upgrade PRs. Those upgrades need
separate compatibility work; they are not prerequisites to merge these fixes.

No production fallback, schema migration, cryptographic format change or advisory
exception is introduced. SillyTavern remains the source of truth.

## Live inbox retry

The poller retries durable received/failed rows between drained batches, before
calling getUpdates. It also does so during idle polling and upstream outages;
no new incoming update or process restart is required. A 2-second scheduling
interval is a minimum, not a hard response SLA: a long poll and an active batch
can delay the next pass. This intentionally adds no concurrent reset worker.

Only startup resets interrupted processing rows. Live recovery never resets
processing, processed or exhausted rows. The existing atomic processing claim now
also enforces fewer than three total attempts, including the live dispatch path.
Due selection uses durable updated_at with 2s/4s backoff and a maximum 100-row
page. Malformed JSON is quarantined as failed with an explicit code and exhausted
budget so one damaged row cannot prevent later rows from progressing. Persisted
rows and error codes remain inspectable; no automatic destructive cleanup occurs.

Ownership is checked before each replay and revalidated after online recovery,
immediately before the next getUpdates request. Per-chat locks, operation identity,
external delivery ledgers, CAS and the commit-before-offset publication order are
retained. This is not a claim of distributed exactly-once delivery. Panicked
processing rows and indeterminate external effects still require the existing
restart/operation-recovery protocol; they are not stolen on a timer.

The ordinary default unit suite covers exact due-time boundaries, page limits,
bot isolation, live processing/exhausted protection, poison-row isolation, atomic
claim competition and same-process polling recovery without a second update.
The full polling case is an unsupported/no-chat update with a database fault,
not proof of a successful business turn against real Telegram or SillyTavern.

## SQLx and dependency security

SQLx 0.9 is paired with source adaptations rather than blanket AssertSqlSafe
wrappers. Operation queries use a private literal-only concatenation macro and
continue binding values. Legacy imports use QueryBuilder with static column and
fallback choices; database values never become query text. Fault-injection tests
use the same explicit, fixed-identifier construction. Existing migrations are
unchanged and the complete default suite remains mandatory.

Cargo must regenerate the lockfile, not manually delete packages. Validation must
confirm rsa and proc-macro-error2 are absent and run cargo-audit against the full
lockfile, with no ignore additions. A clean dependency graph is not evidence of
absence of application vulnerabilities.

## Transport and feature execution

The new HTTP test sends and captures a real multipart body on loopback. Linux TLS
tests generate an ephemeral certificate at runtime and start an OpenSSL server;
they exercise successful explicit trust, rejection of an unknown root and a
wrong DNS name. They do not disable certificate/hostname verification, and never
contact the public network. This does not certify every platform's native trust
store or a real deployment's certificates.

CI executes default tests and all-feature local unit/integration contracts. The
external e2e_sidecar suite is compiled and its scenarios listed, NOT reported as
executed. Running those scenarios still needs the separately provisioned approved
isolated sidecar stack. Release builds continue to use production default features.

CodeQL init/analyze are updated together to one pinned revision; future updates
are grouped. RustCrypto HMAC/SHA upgrades are grouped but still require migration
review. The pinned cargo-audit CLI runs read-only even for fork/Dependabot PRs;
no issues/checks write permission, continue-on-error, or advisory ignore is used.

## Merge controls and validation record

Read actual final-SHA checks before merging: Rust quality gate, CodeQL (Rust),
RustSec audit, Dependency and license policy and Secret scan. Require updated
base-branch checks and an independent reviewer. GitHub branch protection/rulesets
are administrative settings, not enabled merely by committing workflow YAML.
Enable protection on main, restrict bypass and force pushes, require these checks
and PR review. An integration PR must not approve itself.

Execution outcomes belong in the PR and Actions logs and must distinguish the
candidate SHA, baseline tests, mutation tests, skipped external E2E, and native
platforms actually tested. Do not describe a running job or an external stack
that was not provisioned as passed. No production deployment or main merge is
part of this change.

## Rollback

Revert the candidate code/manifest/lockfile together; there is no new schema.
Keep regression tests where compatible. Reverting original #9 or #12 restores
known authorization/transaction defects. Do not roll back by deleting durable
inbox rows, historical keys, or operation state.
