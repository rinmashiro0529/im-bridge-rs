use super::persist_update_batch;
use crate::adapters::sqlite::{connect_pool, migrate};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use tempfile::TempDir;

const BOT: &str = "inbox-test-bot";
const NOW: &str = "2000-01-01T00:00:00Z";

async fn fixture() -> (TempDir, SqlitePool) {
    let dir = tempfile::tempdir().unwrap();
    let pool = connect_pool(&dir.path().join("app.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    for statement in [
        "INSERT INTO accounts (id, username, display_name, password_hash, created_at, updated_at)
         VALUES ('inbox-account', 'inbox-test', 'Inbox test', 'unused-test-hash', '', '')",
        "INSERT INTO workspaces (id, name, created_by, created_at, updated_at)
         VALUES ('inbox-workspace', 'Inbox test', 'inbox-account', '', '')",
        "INSERT INTO telegram_bots (id, workspace_id, owner_account_id, created_at, updated_at)
         VALUES ('inbox-test-bot', 'inbox-workspace', 'inbox-account', '', '')",
    ] {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO telegram_bot_offsets (bot_id, next_offset, updated_at) VALUES (?, 100, ?)")
        .bind(BOT)
        .bind(NOW)
        .execute(&pool)
        .await
        .unwrap();
    (dir, pool)
}

fn update(id: i64, text: &str) -> Value {
    json!({"update_id": id, "message": {"text": text}})
}

async fn stored_offset(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar("SELECT next_offset FROM telegram_bot_offsets WHERE bot_id = ?")
        .bind(BOT)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn assert_state(pool: &SqlitePool, memory: i64, offset: i64, ids: &[i64]) {
    assert_eq!(memory, offset, "in-memory offset must reflect a committed batch");
    assert_eq!(stored_offset(pool).await, offset);
    let actual: Vec<i64> = sqlx::query_scalar(
        "SELECT update_id FROM telegram_updates WHERE bot_id = ? ORDER BY update_id",
    )
    .bind(BOT)
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(actual, ids, "a failed batch must not leave partial inbox rows");
}

async fn seed(pool: &SqlitePool, value: &Value) {
    let raw = value.to_string();
    sqlx::query(
        "INSERT INTO telegram_updates
         (bot_id, update_id, raw_update_json, raw_update_sha256, status, received_at, updated_at)
         VALUES (?, ?, ?, ?, 'received', ?, ?)",
    )
    .bind(BOT)
    .bind(value["update_id"].as_i64().unwrap())
    .bind(&raw)
    .bind(hex::encode(Sha256::digest(raw.as_bytes())))
    .bind(NOW)
    .bind(NOW)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn rollback_on_identity_conflict() {
    let (_dir, pool) = fixture().await;
    let original = update(101, "original");
    seed(&pool, &original).await;
    let mut offset = 100;
    let error = persist_update_batch(
        &pool,
        BOT,
        vec![update(100, "new"), update(101, "changed")],
        &mut offset,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "TELEGRAM_UPDATE_IDENTITY_REUSED");
    assert_state(&pool, offset, 100, &[101]).await;
    let replies = persist_update_batch(
        &pool,
        BOT,
        vec![update(100, "new"), original.clone()],
        &mut offset,
    )
    .await
    .unwrap();
    assert_eq!(replies, vec![(100, update(100, "new")), (101, original)]);
    assert_state(&pool, offset, 102, &[100, 101]).await;
}

#[tokio::test]
async fn rollback_on_offset_write_failure() {
    let (_dir, pool) = fixture().await;
    sqlx::query(
        "CREATE TRIGGER reject_test_offset BEFORE UPDATE ON telegram_bot_offsets
         BEGIN SELECT RAISE(ABORT, 'injected offset write failure'); END",
    )
    .execute(&pool)
    .await
    .unwrap();
    let batch = vec![update(100, "first"), update(101, "second")];
    let mut offset = 100;
    assert!(persist_update_batch(&pool, BOT, batch.clone(), &mut offset).await.is_err());
    assert_state(&pool, offset, 100, &[]).await;
    sqlx::query("DROP TRIGGER reject_test_offset").execute(&pool).await.unwrap();
    assert_eq!(persist_update_batch(&pool, BOT, batch, &mut offset).await.unwrap().len(), 2);
    assert_state(&pool, offset, 102, &[100, 101]).await;
}

#[tokio::test]
async fn rollback_on_deferred_commit_failure() {
    let (_dir, pool) = fixture().await;
    for statement in [
        "CREATE TABLE test_commit_parent (id INTEGER PRIMARY KEY)",
        "CREATE TABLE test_commit_guard (
             parent_id INTEGER REFERENCES test_commit_parent(id) DEFERRABLE INITIALLY DEFERRED
         )",
        "CREATE TRIGGER defer_test_commit AFTER UPDATE ON telegram_bot_offsets
         BEGIN INSERT INTO test_commit_guard (parent_id) VALUES (1); END",
    ] {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }
    // Prove the injected constraint is deferred, not a statement-time failure.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO test_commit_guard (parent_id) VALUES (1)")
        .execute(&mut *tx)
        .await
        .expect("a deferred violation must allow the statement to complete");
    tx.rollback().await.unwrap();

    let batch = vec![update(100, "first"), update(101, "second")];
    let mut offset = 100;
    assert!(persist_update_batch(&pool, BOT, batch.clone(), &mut offset).await.is_err());
    assert_state(&pool, offset, 100, &[]).await;
    let guards: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_commit_guard")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(guards, 0, "failed COMMIT must roll back its trigger effects too");

    // Keep the trigger; satisfying the FK makes the exact same batch commit.
    sqlx::query("INSERT INTO test_commit_parent (id) VALUES (1)")
        .execute(&pool).await.unwrap();
    assert_eq!(persist_update_batch(&pool, BOT, batch, &mut offset).await.unwrap().len(), 2);
    assert_state(&pool, offset, 102, &[100, 101]).await;
}

#[tokio::test]
async fn rollback_on_invalid_update_id() {
    for invalid in [-1, i64::MAX] {
        let (_dir, pool) = fixture().await;
        let mut offset = 100;
        let error = persist_update_batch(
            &pool,
            BOT,
            vec![update(100, "valid first"), update(invalid, "invalid second")],
            &mut offset,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "TELEGRAM_UPDATE_ID_INVALID");
        assert_state(&pool, offset, 100, &[]).await;
        persist_update_batch(&pool, BOT, vec![update(100, "valid first")], &mut offset)
            .await.unwrap();
        assert_state(&pool, offset, 101, &[100]).await;
    }
}

#[tokio::test]
async fn successful_batch_and_replay_keep_status_filtering() {
    let (_dir, pool) = fixture().await;
    let batch = vec![update(103, "failed"), update(100, "received"),
                     update(102, "processed"), update(101, "processing")];
    let mut offset = 100;
    let first = persist_update_batch(&pool, BOT, batch.clone(), &mut offset).await.unwrap();
    assert_eq!(first.iter().map(|(id, _)| *id).collect::<Vec<_>>(), [103, 100, 102, 101]);
    assert_state(&pool, offset, 104, &[100, 101, 102, 103]).await;
    for (id, status) in [(101, "processing"), (102, "processed"), (103, "failed")] {
        sqlx::query("UPDATE telegram_updates SET status = ? WHERE bot_id = ? AND update_id = ?")
            .bind(status).bind(BOT).bind(id).execute(&pool).await.unwrap();
    }
    let replay = persist_update_batch(&pool, BOT, batch, &mut offset).await.unwrap();
    assert_eq!(replay, vec![(103, update(103, "failed")), (100, update(100, "received"))]);
    assert_state(&pool, offset, 104, &[100, 101, 102, 103]).await;
}

#[tokio::test]
async fn duplicate_updates_in_one_batch_keep_existing_dispatch_contract() {
    let (_dir, pool) = fixture().await;
    let value = update(100, "same");
    let mut offset = 100;
    let result = persist_update_batch(&pool, BOT, vec![value.clone(), value.clone()], &mut offset)
        .await.unwrap();
    // Durable processing claims, not this function, suppress duplicate dispatch.
    assert_eq!(result, vec![(100, value.clone()), (100, value)]);
    assert_state(&pool, offset, 101, &[100]).await;
}

#[tokio::test]
async fn restart_keeps_offset_and_processed_update_deduplication() {
    let (dir, pool) = fixture().await;
    let mut offset = 100;
    persist_update_batch(&pool, BOT, vec![update(100, "committed")], &mut offset)
        .await.unwrap();
    sqlx::query("UPDATE telegram_updates SET status = 'processed' WHERE bot_id = ?")
        .bind(BOT).execute(&pool).await.unwrap();
    pool.close().await;
    let reopened = connect_pool(&dir.path().join("app.db")).await.unwrap();
    let mut offset = stored_offset(&reopened).await;
    let replay = persist_update_batch(&reopened, BOT, vec![update(100, "committed")], &mut offset)
        .await.unwrap();
    assert!(replay.is_empty());
    assert_state(&reopened, offset, 101, &[100]).await;
    persist_update_batch(&reopened, BOT, vec![update(101, "next")], &mut offset)
        .await.unwrap();
    assert_state(&reopened, offset, 102, &[100, 101]).await;
}

#[tokio::test]
async fn empty_and_missing_ids_keep_existing_skip_contract() {
    let (_dir, pool) = fixture().await;
    let mut offset = 100;
    for batch in [vec![], vec![json!({}), json!({"update_id": null}), json!({"update_id": "text"})]] {
        assert!(persist_update_batch(&pool, BOT, batch, &mut offset).await.unwrap().is_empty());
        assert_state(&pool, offset, 100, &[]).await;
    }
}
