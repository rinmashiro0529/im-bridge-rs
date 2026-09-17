use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use sqlx::SqlitePool;
use tokio_util::sync::CancellationToken;
use wiremock::{matchers::method, matchers::path, Mock, MockServer, ResponseTemplate};

use super::{pending_inbox_batch, persist_update_batch, poll_telegram_bot_at, process_and_deliver,
    recover_pending_updates, TelegramModule};

const BOT: &str = "inbox-test-bot";

async fn state(pool: &SqlitePool, id: i64) -> (String, i64) {
    sqlx::query_as("SELECT status, attempt_count FROM telegram_updates WHERE bot_id = ? AND update_id = ?")
        .bind(BOT).bind(id).fetch_one(pool).await.unwrap()
}

async fn seed(pool: &SqlitePool, id: i64, status: &str, attempts: i64, timestamp: &str) {
    sqlx::query("INSERT INTO telegram_updates
        (bot_id, update_id, raw_update_json, status, attempt_count, received_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(BOT).bind(id).bind(json!({"update_id": id}).to_string())
        .bind(status).bind(attempts).bind(timestamp).bind(timestamp)
        .execute(pool).await.unwrap();
}

#[tokio::test]
async fn due_selection_has_exact_backoff_boundaries_and_a_bounded_page() {
    let (_dir, pool) = super::inbox_offset_tests::fixture().await;
    let at = "2000-01-01T00:00:00Z";
    seed(&pool, 1, "failed", 1, at).await;
    seed(&pool, 2, "failed", 2, at).await;
    seed(&pool, 3, "failed", 3, at).await;
    seed(&pool, 4, "processing", 1, at).await;
    seed(&pool, 5, "processed", 1, at).await;
    let ids = |rows: Vec<(i64, String)>| rows.into_iter().map(|r| r.0).collect::<Vec<_>>();
    assert!(pending_inbox_batch(&pool, BOT, 946684801).await.unwrap().is_empty());
    assert_eq!(ids(pending_inbox_batch(&pool, BOT, 946684802).await.unwrap()), [1]);
    assert_eq!(ids(pending_inbox_batch(&pool, BOT, 946684804).await.unwrap()), [1, 2]);
    assert!(pending_inbox_batch(&pool, "other-bot", i64::MAX).await.unwrap().is_empty());
    for id in 10..115 {
        seed(&pool, id, "received", 0, at).await;
    }
    let rows = pending_inbox_batch(&pool, BOT, 946684900).await.unwrap();
    assert_eq!(rows.len(), 100);
    assert_eq!(rows.first().unwrap().0, 1);
    assert_eq!(rows.last().unwrap().0, 107);
}

#[tokio::test]
async fn live_recovery_never_resets_processing_or_retries_exhausted_rows() {
    let (_dir, pool) = super::inbox_offset_tests::fixture().await;
    let at = "2000-01-01T00:00:00Z";
    seed(&pool, 1, "processing", 1, at).await;
    seed(&pool, 2, "failed", 3, at).await;
    seed(&pool, 3, "received", 0, at).await;
    let module = TelegramModule::new(pool.clone());
    let client = reqwest::Client::new();
    recover_pending_updates(&module, &pool, &client, "synthetic", BOT, None).await.unwrap();
    assert_eq!(state(&pool, 1).await, ("processing".into(), 1));
    assert_eq!(state(&pool, 2).await, ("failed".into(), 3));
    assert_eq!(state(&pool, 3).await, ("processed".into(), 1));
    // Even a direct/live dispatch cannot bypass the same durable budget.
    process_and_deliver(&module, &pool, &client, "synthetic", BOT, 2,
        &json!({"update_id": 2}), None).await.unwrap();
    assert_eq!(state(&pool, 2).await, ("failed".into(), 3));
}

#[tokio::test]
async fn malformed_row_does_not_starve_later_valid_updates() {
    let (_dir, pool) = super::inbox_offset_tests::fixture().await;
    let at = "2000-01-01T00:00:00Z";
    seed(&pool, 1, "failed", 1, at).await;
    seed(&pool, 2, "received", 0, at).await;
    sqlx::query("UPDATE telegram_updates SET raw_update_json = '{broken' WHERE update_id = 1")
        .execute(&pool).await.unwrap();
    let module = TelegramModule::new(pool.clone());
    recover_pending_updates(&module, &pool, &reqwest::Client::new(), "synthetic", BOT, None)
        .await.unwrap();
    assert_eq!(state(&pool, 1).await, ("failed".into(), 3));
    assert_eq!(state(&pool, 2).await, ("processed".into(), 1));
    let code: String = sqlx::query_scalar("SELECT error_summary FROM telegram_updates WHERE update_id = 1")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(code, "TELEGRAM_INBOX_JSON_INVALID");
}

#[tokio::test]
async fn concurrent_recovery_uses_the_existing_atomic_claim() {
    let (_dir, pool) = super::inbox_offset_tests::fixture().await;
    let mut offset = 100;
    persist_update_batch(&pool, BOT, vec![json!({"update_id": 100})], &mut offset)
        .await.unwrap();
    let module = TelegramModule::new(pool.clone());
    let client = reqwest::Client::new();
    let (left, right) = tokio::join!(
        recover_pending_updates(&module, &pool, &client, "synthetic", BOT, None),
        recover_pending_updates(&module, &pool, &client, "synthetic", BOT, None),
    );
    left.unwrap(); right.unwrap();
    assert_eq!(state(&pool, 100).await, ("processed".into(), 1));
}

#[tokio::test]
async fn failed_inbox_recovers_in_the_same_poller_with_no_new_updates() {
    let (_dir, pool) = super::inbox_offset_tests::fixture().await;
    // A local database fault, before any bridge operation or external delivery.
    sqlx::query("CREATE TRIGGER reject_inbox_completion BEFORE UPDATE OF status ON telegram_updates
        WHEN NEW.status = 'processed'
        BEGIN SELECT RAISE(ABORT, 'synthetic transient failure'); END")
        .execute(&pool).await.unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200)
        .set_body_json(json!({"ok": true, "result": {"id": 123, "username": "synthetic_bot"}})))
        .mount(&server).await;
    let offered = Arc::new(AtomicBool::new(false));
    let flag = offered.clone();
    Mock::given(method("GET")).and(path("/botsynthetic/getUpdates"))
        .respond_with(move |_: &wiremock::Request| {
            let updates: Vec<Value> = if flag.swap(true, Ordering::SeqCst) {
                vec![]
            } else {
                // Unsupported/no-chat updates take the real claim/ack path without
                // requiring a live bot, account, provider, or test-only fallback.
                vec![json!({"update_id": 100})]
            };
            ResponseTemplate::new(200).set_delay(Duration::from_millis(50))
                .set_body_json(json!({"ok": true, "result": updates}))
        }).mount(&server).await;
    let cancel = CancellationToken::new();
    let task = tokio::spawn(poll_telegram_bot_at(TelegramModule::new(pool.clone()),
        pool.clone(), BOT.into(), "synthetic".into(), cancel.clone(), None, server.uri()));
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let failed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telegram_updates WHERE update_id = 100 AND status = 'failed'")
                .fetch_one(&pool).await.unwrap();
            if failed == 1 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        sqlx::query("DROP TRIGGER reject_inbox_completion").execute(&pool).await.unwrap();
        loop {
            if state(&pool, 100).await.0 == "processed" { break; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await;
    cancel.cancel();
    let joined = tokio::time::timeout(Duration::from_secs(5), task).await;
    result.expect("online recovery requires neither restart nor another update");
    joined.unwrap().unwrap().unwrap();
    assert_eq!(state(&pool, 100).await, ("processed".into(), 2));
    let offset: i64 = sqlx::query_scalar("SELECT next_offset FROM telegram_bot_offsets WHERE bot_id = ?")
        .bind(BOT).fetch_one(&pool).await.unwrap();
    assert_eq!(offset, 101);
    let operations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bridge_operations")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(operations, 0);
    assert!(offered.load(Ordering::SeqCst));
    let requests = server.received_requests().await.unwrap();
    assert!(!requests.iter().any(|r| r.url.path().ends_with("/sendMessage")));
}
