from pathlib import Path

p = Path('src/modules/telegram/mod.rs')
s = p.read_text()
reset = '''    // Startup only: the owning runtime has not spawned any delivery tasks yet.
    sqlx::query(
        "UPDATE telegram_updates SET status = 'received', updated_at = ? WHERE bot_id = ? AND status = 'processing'",
    )
    .bind(now_rfc3339())
    .bind(&bot_id)
    .execute(&pool)
    .await?;
    if let Err(err) = recover_pending_updates('''
assert s.count(reset) == 1
s = s.replace(reset, '    if let Err(err) = recover_startup_updates(', 1)
helper = '''// Run once before spawning delivery tasks. Keep reset failures inside the
// existing recoverable startup boundary; do not exit without poller cleanup.
async fn recover_startup_updates(
    module: &TelegramModule,
    pool: &SqlitePool,
    client: &reqwest::Client,
    token: &str,
    bot_id: &str,
    ownership_guard: Option<&PollerOwnershipGuard>,
) -> AppResult<()> {
    sqlx::query(
        "UPDATE telegram_updates SET status = 'received', updated_at = ? WHERE bot_id = ? AND status = 'processing'",
    )
    .bind(now_rfc3339())
    .bind(bot_id)
    .execute(pool)
    .await?;
    recover_pending_updates(module, pool, client, token, bot_id, ownership_guard).await
}

'''
marker = '// Bounded due-work selection; the caller owns the bot and drains live batches.'
assert s.count(marker) == 1
s = s.replace(marker, helper + marker, 1)
p.write_text(s)
p = Path('src/modules/telegram/inbox_recovery_tests.rs')
s = p.read_text()
marker = '''async fn failed_inbox_recovers_in_the_same_poller_with_no_new_updates() {
    let (_dir, pool) = super::inbox_offset_tests::fixture().await;
'''
assert s.count(marker) == 1
s = s.replace(marker, marker + '''    // A startup reset failure must be logged, not terminate the poller and
    // leave its ownership entry behind. Live recovery must not reset this row.
    seed(&pool, 99, "processing", 1, "2000-01-01T00:00:00Z").await;
    sqlx::query("CREATE TRIGGER reject_startup_reset BEFORE UPDATE OF status ON telegram_updates
        WHEN OLD.status = 'processing' AND NEW.status = 'received'
        BEGIN SELECT RAISE(ABORT, 'synthetic startup reset failure'); END")
        .execute(&pool).await.unwrap();
''', 1)
s = s.replace('    assert_eq!(offset, 101);', '    assert_eq!(offset, 101);\n    assert_eq!(state(&pool, 99).await, ("processing".into(), 1));', 1)
p.write_text(s)
