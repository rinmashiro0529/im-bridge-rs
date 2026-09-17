from pathlib import Path
import re
import shutil
import sys

root = Path.cwd()
inputs = Path(sys.argv[1])

def edit(path, old, new, count=1):
    p = root / path
    text = p.read_text()
    assert text.count(old) == count, (path, text.count(old), old[:60])
    p.write_text(text.replace(old, new))

edit('Cargo.toml', 'sqlx = { version = "0.8"', 'sqlx = { version = "0.9"')
p = root / 'src/modules/bridge/operation_store.rs'
s = p.read_text()
m = re.search(r'const SELECT_COLUMNS: &str =\s*"(.*?)";', s, re.S)
assert m
macro = '// Only literal clauses are accepted; values continue to use bind parameters.\nmacro_rules! select_operations {\n    ($tail:literal) => {\n        concat!("SELECT ' + m.group(1) + ' FROM bridge_operations", $tail)\n    };\n}'
s = s[:m.start()] + macro + s[m.end():]
s, n = re.subn(r'&format!\(\s*"SELECT \{SELECT_COLUMNS\} FROM bridge_operations(.*?)"\s*\)', lambda m: 'select_operations!("' + m.group(1) + '")', s, flags=re.S)
assert n == 5
p.write_text(s)
edit('src/modules/migration/mod.rs', '''        let account_sql = format!(
            "SELECT account_id, display_name, {st_handle_expr} FROM accounts ORDER BY created_at"
        );
        let account_rows = sqlx::query(&account_sql).fetch_all(&legacy).await?;''', '''        // Both fragments are compile-time choices, never legacy database contents.
        let account_rows = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT account_id, display_name, ",
        )
        .push(st_handle_expr)
        .push(" FROM accounts ORDER BY created_at")
        .build()
        .fetch_all(&legacy)
        .await?;''')
edit('src/modules/migration/mod.rs', '''            let config_sql = format!(
                "SELECT account_id, telegram_bot_token, telegram_allowed_user_ids,
                        {inter_delay}, {stream_interval}, {stream_delta}, {advanced}
                 FROM account_configs"
            );
            sqlx::query(&config_sql).fetch_all(&legacy).await?''', '''            // optional_column only accepts static identifiers and fallback expressions.
            sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "SELECT account_id, telegram_bot_token, telegram_allowed_user_ids, ",
            )
            .push(inter_delay)
            .push(", ")
            .push(stream_interval)
            .push(", ")
            .push(stream_delta)
            .push(", ")
            .push(advanced)
            .push(" FROM account_configs")
            .build()
            .fetch_all(&legacy)
            .await?''')
edit('src/modules/migration/mod.rs', '''    column: &str,
    fallback: &str,
) -> AppResult<String> {
    Ok(if column_exists(pool, table, column).await? {
        column.to_string()
    } else {
        fallback.to_string()
    })''', '''    column: &'static str,
    fallback: &'static str,
) -> AppResult<&'static str> {
    Ok(if column_exists(pool, table, column).await? {
        column
    } else {
        fallback
    })''')
edit('tests/identity_lifecycle.rs', '''sqlx::query(&format!("UPDATE accounts SET {change} WHERE id = ?"))
            .bind(&fixture.admin.account.id)''', '''sqlx::QueryBuilder::<sqlx::Sqlite>::new("UPDATE accounts SET ")
            .push(change)
            .push(" WHERE id = ")
            .push_bind(&fixture.admin.account.id)
            .build()''', 2)
edit('tests/identity_lifecycle.rs', '''sqlx::query(&format!(
                "CREATE TRIGGER fail_provision AFTER INSERT ON {table}
                 BEGIN SELECT RAISE(ABORT, 'synthetic provisioning failure'); END"
            ))''', '''sqlx::QueryBuilder::<sqlx::Sqlite>::new("CREATE TRIGGER fail_provision AFTER INSERT ON ")
            .push(table)
            .push(" BEGIN SELECT RAISE(ABORT, 'synthetic provisioning failure'); END")
            .build()''')
p = root / 'src/modules/telegram/mod.rs'
s = p.read_text().replace('mod inbox_offset_tests;', 'mod inbox_offset_tests;\n\n#[cfg(test)]\nmod inbox_recovery_tests;', 1)
old = '    let client = match telegram_http_client() {'
assert s.count(old) == 1
s = s.replace(old, '''    let api_base = telegram_api_base()?;
    poll_telegram_bot_at(module, pool, bot_id, token, cancel, ownership_guard, api_base).await
}

async fn poll_telegram_bot_at(
    module: TelegramModule,
    pool: SqlitePool,
    bot_id: String,
    token: String,
    cancel: CancellationToken,
    ownership_guard: Option<PollerOwnershipGuard>,
    api_base: String,
) -> AppResult<()> {
    let client = match telegram_http_client() {''', 1)
s = s.replace('    let api_base = telegram_api_base()?;\n    if let Err(err) =', '    if let Err(err) =', 1)
reset = '''    sqlx::query(
        "UPDATE telegram_updates SET status = 'received', updated_at = ? WHERE bot_id = ? AND status = 'processing'",
    )
    .bind(now_rfc3339())
    .bind(bot_id)
    .execute(pool)
    .await?;
'''
assert s.count(reset) == 1
s = s.replace(reset, '', 1)
s = s.replace('    if let Err(err) = recover_pending_updates(', '''    // Startup only: the owning runtime has not spawned any delivery tasks yet.
    sqlx::query(
        "UPDATE telegram_updates SET status = 'received', updated_at = ? WHERE bot_id = ? AND status = 'processing'",
    )
    .bind(now_rfc3339())
    .bind(&bot_id)
    .execute(&pool)
    .await?;
    if let Err(err) = recover_pending_updates(''', 1)
s = s.replace('    let mut next_recovery_at = tokio::time::Instant::now() + std::time::Duration::from_secs(30);', '    let mut next_recovery_at = tokio::time::Instant::now() + std::time::Duration::from_secs(30);\n    let mut next_inbox_recovery_at = tokio::time::Instant::now();', 1)
s = s.replace('        let url = format!("{api_base}/bot{token}/getUpdates");', '''        // Every previous batch is drained before this point. Do not reset
        // processing rows here: a live or uncertain effect must not be stolen.
        if tokio::time::Instant::now() >= next_inbox_recovery_at {
            if let Err(err) = recover_pending_updates(
                &module, &pool, &client, &token, &bot_id, ownership_guard.as_ref(),
            ).await {
                tracing::error!(bot_id, code = %err.code, "telegram online inbox recovery failed");
            }
            next_inbox_recovery_at = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        }
        let url = format!("{api_base}/bot{token}/getUpdates");''', 1)
s = s.replace('''    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT update_id, raw_update_json FROM telegram_updates
         WHERE bot_id = ? AND status IN ('received', 'failed') AND attempt_count < 3
         ORDER BY update_id",
    )
    .bind(bot_id)
    .fetch_all(pool)
    .await?;''', '''    let rows = pending_inbox_batch(pool, bot_id, time::OffsetDateTime::now_utc().unix_timestamp()).await?;''', 1)
s = s.replace('''        let update: Value = serde_json::from_str(&raw).map_err(|err| {
            AppError::internal(format!("invalid persisted telegram update: {err}"))
        })?;''', '''        if let Some(guard) = ownership_guard {
            guard.assert_valid().await.map_err(|_| {
                AppError::conflict("POLLER_OWNERSHIP_LOST", "inbox recovery ownership is no longer valid")
            })?;
        }
        let update: Value = match serde_json::from_str(&raw) {
            Ok(update) => update,
            Err(_) => {
                // Quarantine one poison row without blocking every later update.
                sqlx::query(
                    "UPDATE telegram_updates SET status = 'failed', attempt_count = 3,
                     error_summary = 'TELEGRAM_INBOX_JSON_INVALID', updated_at = ?
                     WHERE bot_id = ? AND update_id = ? AND status IN ('received', 'failed')",
                )
                .bind(now_rfc3339()).bind(bot_id).bind(update_id).execute(pool).await?;
                tracing::warn!(bot_id, update_id, "invalid inbox JSON exhausted; manual review required");
                continue;
            }
        };''', 1)
s = s.replace('''SET status = 'processing', attempt_count = attempt_count + 1, updated_at = ?
         WHERE bot_id = ? AND update_id = ? AND status IN ('received', 'failed')''', '''SET status = 'processing', attempt_count = attempt_count + 1, updated_at = ?
         WHERE bot_id = ? AND update_id = ? AND status IN ('received', 'failed')
           AND attempt_count < 3''', 1)
helper = '''// Bounded due-work selection; the caller owns the bot and drains live batches.
// last failure time is durable; retries back off by 2s then 4s, with three
// total attempts. Exhausted rows stay visible as failed for manual inspection.
async fn pending_inbox_batch(
    pool: &SqlitePool,
    bot_id: &str,
    now_unix: i64,
) -> AppResult<Vec<(i64, String)>> {
    Ok(sqlx::query_as(
        "SELECT update_id, raw_update_json FROM telegram_updates
         WHERE bot_id = ? AND status IN ('received', 'failed') AND attempt_count < 3
           AND (attempt_count = 0 OR COALESCE(unixepoch(updated_at), 0)
                + CASE WHEN attempt_count <= 1 THEN 2 ELSE 4 END <= ?)
         ORDER BY update_id LIMIT 100",
    )
    .bind(bot_id)
    .bind(now_unix)
    .fetch_all(pool)
    .await?)
}

'''
s = s.replace('async fn recover_pending_updates(', helper + 'async fn recover_pending_updates(', 1)
p.write_text(s)
edit('src/modules/telegram/inbox_offset_tests.rs', 'async fn fixture()', 'pub(super) async fn fixture()')
for name in ['ci.yml', 'codeql.yml', 'security.yml']:
    p = root / '.github/workflows' / name
    s = p.read_text().replace('d23441a48e516b6c34aea4fa41551a30e30af803 # v6.1.0', '3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1')
    if name == 'codeql.yml':
        s = s.replace('faaca9a8f6edddba5725ffe5adefdab6669a2eca # v3', 'b96794f015dfd88f77b49b1c93e0fa7110f94c63 # v4.38.0')
    if name == 'ci.yml':
        s = s.replace('      - name: Release build', '      - name: All-feature local contracts (external E2E compiled only)\n        run: python3 scripts/test_feature_contracts.py\n      - name: Release build')
    if name == 'security.yml':
        s = s.replace('    permissions:\n      contents: read\n      checks: write\n      issues: write\n', '')
        a = s.index('      - uses: rustsec/audit-check@')
        b = s.index('\n  policy:', a)
        s = s[:a] + '''      - name: Install pinned auditor
        run: cargo install cargo-audit --version 0.22.2 --locked
      - name: Audit the complete lockfile without exceptions
        run: cargo audit --deny warnings
''' + s[b:]
    p.write_text(s)
p = root / '.github/dependabot.yml'
s = p.read_text().replace('    groups:\n      cargo-minor-and-patch:', '    groups:\n      rustcrypto-compatibility:\n        patterns: [hmac, sha2]\n      cargo-minor-and-patch:')
s += '    groups:\n      codeql-family:\n        patterns: ["github/codeql-action*"]\n'
p.write_text(s)
for source, destination in [
    ('inbox_recovery_tests.rs', 'src/modules/telegram/inbox_recovery_tests.rs'),
    ('transport_runtime_contract.rs', 'tests/transport_runtime_contract.rs'),
    ('test_feature_contracts.py', 'scripts/test_feature_contracts.py'),
    ('round2-merge-readiness.md', 'docs/round2-merge-readiness.md'),
]:
    shutil.copyfile(inputs / source, root / destination)
