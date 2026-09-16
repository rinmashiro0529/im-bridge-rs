# Changelog

All notable changes will be documented here. This project follows Keep a Changelog conventions and intends to use semantic versioning after its first public release.

## [Unreleased]

### Security
- Removed administration auto-login behavior.
- Added strict service URL and configuration validation.
- Replaced the source-controlled Telegram binding-code pepper with a per-installation derived key.
- Hardened master-key file creation and disabled unsafe online key rotation.
- Added request limits, concurrency budgets, timeouts, and browser security headers.
- Added bounded account, password, Bot Token, and service URL validation.
- Removed redundant in-memory copies of Connector authentication material.
- Updated `rustls` to 0.23.45 to remediate RUSTSEC-2026-0285.

### Documentation
- Added English and Simplified Chinese public documentation.
- Added security, contributing, conduct, deployment, and configuration guidance.
