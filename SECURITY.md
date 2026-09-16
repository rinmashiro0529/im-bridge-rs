# Security Policy

## Supported versions

IM Bridge is pre-release software. Security fixes are currently provided only on the latest development branch. No release is considered production-supported yet.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Contact the repository owner through GitHub's private security-advisory feature after the repository is created. Include the affected revision, impact, reproduction steps, and any suggested mitigation. Do not include production credentials or personal data.

We aim to acknowledge complete reports within 7 days. Timelines for validation, remediation, and disclosure depend on severity and reproducibility.

## Security boundaries

- SillyTavern and the Connector are separate trust components and require independent review.
- A Telegram Bot token, Connector HMAC key, master key, session cookie, or database copy is sensitive.
- The administration API should be exposed only through TLS and an authenticated reverse proxy or a private network boundary.
- Production credentials must never be used in tests or GitHub Actions.

## Known pre-release limitations

- Online master-key rotation is disabled. The command supports dry-run decryption validation only until a crash-recoverable two-phase rotation protocol is implemented.
- `proc-macro-error2` is an unmaintained transitive build dependency through `teloxide -> aquamarine` (RUSTSEC-2026-0173). No safe upgrade is currently available; it is explicitly tracked in `deny.toml` and must be revisited when the upstream dependency graph changes.
