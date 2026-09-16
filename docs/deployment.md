# Deployment

[简体中文](deployment.zh-CN.md)

## Recommended topology

Run IM Bridge as an unprivileged service bound to loopback. Put a TLS reverse proxy in front of the administration API only when remote administration is necessary. Restrict access with a private network or explicit firewall policy.

1. Build with `cargo build --release --locked`.
2. Create a dedicated `imbridge` user and `/var/lib/im-bridge` data directory.
3. Generate a 32-byte master key directly into a root-managed file with mode `0600`.
4. Store the Connector HMAC key in `/etc/im-bridge/im-bridge.env` with mode `0600`.
5. Install and customize `deploy/im-bridge.service`.
6. Bootstrap an administrator by piping a password from a protected source to `--password-file -`.
7. Start in `read_only`, validate `/health/ready`, and use an isolated test bot before enabling writes.

Do not expose the service directly on plain HTTP outside loopback. Do not enable a bot until poller ownership has been verified. Database backups do not contain the master key; back it up separately through a restricted channel.
