use std::sync::Arc;

use im_bridge::adapters::secrets::encrypted_sqlite::EncryptedSqliteVault;
use im_bridge::domain::st::{
    StCapabilities, StChatLocator, StChatSnapshot, StCommitResult, StCommitStatus, StMutation,
    StStatus, StWriteMode,
};
use im_bridge::modules::bridge::channel_context::ChannelContextStore;
use im_bridge::modules::bridge::engine::SidecarBridgeEngine;
use im_bridge::modules::bridge::operation_coordinator::OperationCoordinator;
use im_bridge::modules::bridge::operation_payload::OperationPayloadKeyProvider;
use im_bridge::modules::bridge::operation_store::OperationStore;
use im_bridge::modules::bridge::poller_ownership::{PollerOwner, SharedMemoryPollerRegistry};
use im_bridge::modules::telegram::delivery::{TelegramDelivery, TurnScope};
use im_bridge::modules::telegram::dispatch::dispatch_update_with_delivery;
use im_bridge::modules::telegram::{TelegramModule, TelegramServices};
use im_bridge::seams::secret_vault::SecretVault;
use im_bridge::seams::st_operation_journal::MemoryStOperationJournal;
use serde_json::json;
use sha2::Digest;
use sqlx::SqlitePool;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::st_backend::{ScriptedStBackend, StBackendCall};

fn status() -> StStatus {
    StStatus {
        available: true,
        version: Some("1.16.0-synthetic".into()),
        handle: "default-user".into(),
        capabilities: StCapabilities {
            mode: StWriteMode::TestWrite,
            snapshot: true,
            typed_mutations: true,
            integrity_rotation: true,
            operation_replay: true,
        },
    }
}

fn locator() -> StChatLocator {
    StChatLocator {
        handle: "default-user".into(),
        avatar: "Synthetic.png".into(),
        character_name: "TestChar".into(),
        chat_file: "IMBridge-Test-TestChar-1.jsonl".into(),
    }
}

fn dialogue_snapshot() -> StChatSnapshot {
    StChatSnapshot {
        locator: locator(),
        parsed_chat: vec![
            json!({"chat_metadata": {"integrity": "int-1"}}),
            json!({"is_user": true, "name": "User", "mes": "hello"}),
            json!({"is_user": false, "name": "TestChar", "mes": "hi there"}),
        ],
        source_sha256: "a".repeat(64),
        source_integrity: "int-1".into(),
        source_byte_length: 64,
        source_message_count: 3,
    }
}

fn two_turn_snapshot() -> StChatSnapshot {
    StChatSnapshot {
        locator: locator(),
        parsed_chat: vec![
            json!({"chat_metadata": {"integrity": "int-1"}}),
            json!({"is_user": true, "name": "User", "mes": "first"}),
            json!({"is_user": false, "name": "TestChar", "mes": "first-reply"}),
            json!({"is_user": true, "name": "User", "mes": "second"}),
            json!({"is_user": false, "name": "TestChar", "mes": "second-reply"}),
        ],
        source_sha256: "c".repeat(64),
        source_integrity: "int-2".into(),
        source_byte_length: 128,
        source_message_count: 5,
    }
}

fn applied() -> StCommitResult {
    StCommitResult {
        status: StCommitStatus::Applied,
        new_sha256: Some("b".repeat(64)),
        new_integrity: Some("int-applied".into()),
        byte_length: Some(80),
        message_count: Some(1),
    }
}

fn not_applied() -> StCommitResult {
    StCommitResult {
        status: StCommitStatus::NotApplied,
        new_sha256: None,
        new_integrity: None,
        byte_length: None,
        message_count: None,
    }
}

fn count_methods(requests: &[wiremock::Request], suffix: &str) -> usize {
    requests
        .iter()
        .filter(|request| request.url.path().ends_with(suffix))
        .count()
}

async fn seed_turn_deliveries(pool: &SqlitePool, scope: &TurnScope, bot_id: &str, ids: &[i64]) {
    let now = im_bridge::clock::now_rfc3339();
    for (index, message_id) in ids.iter().enumerate() {
        sqlx::query(
            "INSERT INTO channel_deliveries
                (id, bot_id, chat_id, message_kind, turn_id, external_message_id, attempt_count, status, created_at, updated_at)
             VALUES (?, ?, ?, 'generation', 'turn-last', ?, 1, 'sent', ?, ?)",
        )
        .bind(format!("delivery-{index}"))
        .bind(bot_id)
        .bind(scope.chat_id.to_string())
        .bind(message_id.to_string())
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO telegram_turn_messages
                (account_id, internal_bot_id, numeric_bot_id, chat_id, locator_hash,
                 turn_id, operation_id, chunk_index, message_id, content_hash,
                 render_version, status, lifecycle, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, 'turn-last', 'turn-last', ?, ?, ?, 1, 'sent', 'active', ?, ?)",
        )
        .bind(&scope.account_id)
        .bind(&scope.internal_bot_id)
        .bind(scope.numeric_bot_id)
        .bind(scope.chat_id)
        .bind(&scope.locator_hash)
        .bind(i64::try_from(index).unwrap_or(i64::MAX))
        .bind(*message_id)
        .bind(hex::encode(sha2::Sha256::digest(
            format!("message-{message_id}").as_bytes(),
        )))
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
    }
}

struct Fixture {
    _app: common::TestApp,
    tg: TelegramModule,
    bot_id: String,
    backend: Arc<ScriptedStBackend>,
    delivery: TelegramDelivery,
    server: MockServer,
}

async fn setup_fixture() -> Fixture {
    let app = common::setup().await;
    let tg = TelegramModule::new(app.pool.clone());
    let backend = Arc::new(ScriptedStBackend::new());
    let channel = ChannelContextStore::new(app.pool.clone());
    let registry = Arc::new(SharedMemoryPollerRegistry::new());
    registry.bootstrap_legacy(1).await.unwrap();
    registry
        .transfer(1, PollerOwner::LegacyPlugin, 1, PollerOwner::RustBridge)
        .await
        .unwrap();
    tg.attach_ownership_registry(registry.clone()).await;
    let bot = tg.upsert_bot(&app.actor, None, false).await.unwrap();
    let guard = tg.claim_numeric_bot(1, &bot.id).await.unwrap();
    let context_key = format!("{}:9", bot.id);
    let store = Arc::new(OperationStore::new(app.pool.clone()));
    let vault: Arc<dyn SecretVault> =
        Arc::new(EncryptedSqliteVault::new(app.pool.clone(), [7_u8; 32]));
    let provider = Arc::new(OperationPayloadKeyProvider::new(
        vault.clone(),
        store.clone(),
    ));
    let coordinator = Arc::new(OperationCoordinator::new_with_key_provider(
        backend.clone(),
        store,
        provider,
        Arc::new(MemoryStOperationJournal::new()),
    ));
    let engine = SidecarBridgeEngine::new(backend.clone(), channel.clone())
        .with_context_key(&context_key)
        .with_required_coordinator(coordinator)
        .with_ownership_guard(Arc::new(guard));
    tg.attach_services(TelegramServices {
        identity: app.identity.clone(),
        vault,
        bridge: Some(engine),
        channel: channel.clone(),
        ownership_registry: Some(registry.clone()),
    })
    .await;
    let code = tg
        .generate_bind_code(&bot.id, &app.actor.account.id)
        .await
        .unwrap();
    dispatch_update_with_delivery(
        &tg,
        &bot.id,
        &json!({"message": {"chat": {"id": 9}, "from": {"id": 4242}, "text": format!("/bind {}", code.code)}}),
        None,
    )
    .await
    .unwrap();
    channel
        .select_chat(
            &app.actor.account.id,
            app.actor.workspace_id.as_deref().unwrap(),
            &context_key,
            &locator(),
        )
        .await
        .unwrap();
    let selected_locator = locator();
    let scope = TurnScope::new(
        app.actor.account.id.clone(),
        bot.id.clone(),
        1,
        9,
        im_bridge::modules::bridge::st_ops::locator_hash(
            &selected_locator.handle,
            &selected_locator.avatar,
            &selected_locator.chat_file,
        ),
    )
    .unwrap();

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/sendMessage"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"ok": true, "result": {"message_id": 900}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/editMessageText"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true, "result": true})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/deleteMessage"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true, "result": true})))
        .mount(&server)
        .await;

    let delivery = TelegramDelivery::new(
        reqwest::Client::new(),
        app.pool.clone(),
        "test-token".into(),
        server.uri(),
        bot.id.clone(),
        1,
        9,
        0,
        0,
        1,
        1,
        3200,
    );
    seed_turn_deliveries(&app.pool, &scope, &bot.id, &[501, 502, 503]).await;
    Fixture {
        _app: app,
        tg,
        bot_id: bot.id.clone(),
        backend,
        delivery,
        server,
    }
}

fn command_update(text: &str, update_id: i64) -> serde_json::Value {
    json!({
        "update_id": update_id,
        "message": {
            "chat": {"id": 9},
            "from": {"id": 4242},
            "text": text
        }
    })
}

#[tokio::test]
async fn undo_does_not_edit_or_delete_telegram_messages() {
    let fixture = setup_fixture().await;
    fixture.backend.push_probe(Ok(status()));
    fixture.backend.push_snapshot(Ok(dialogue_snapshot()));
    fixture.backend.push_snapshot(Ok(dialogue_snapshot()));
    fixture.backend.push_commit(Ok(applied()));

    let replies = dispatch_update_with_delivery(
        &fixture.tg,
        &fixture.bot_id,
        &command_update("/undo", 201),
        Some(&fixture.delivery),
    )
    .await
    .unwrap();
    assert!(replies[0].text.contains("Telegram 消息保持保留"));
    assert!(replies[0].text.contains("hello"));

    let requests = fixture.server.received_requests().await.unwrap();
    assert_eq!(count_methods(&requests, "/editMessageText"), 0);
    assert_eq!(count_methods(&requests, "/deleteMessage"), 0);
    let commits: Vec<_> = fixture
        .backend
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            StBackendCall::Commit { command } => Some(command.mutation),
            _ => None,
        })
        .collect();
    assert_eq!(commits.len(), 1);
    assert!(matches!(commits[0], StMutation::UndoLastTurn { .. }));
}

#[tokio::test]
async fn revoke_edits_tombstone_and_deletes_extras_strictly_after_commit() {
    let fixture = setup_fixture().await;
    fixture.backend.push_probe(Ok(status()));
    fixture.backend.push_snapshot(Ok(dialogue_snapshot()));
    fixture.backend.push_snapshot(Ok(dialogue_snapshot()));
    fixture.backend.push_commit(Ok(applied()));

    let replies = dispatch_update_with_delivery(
        &fixture.tg,
        &fixture.bot_id,
        &command_update("/revoke", 202),
        Some(&fixture.delivery),
    )
    .await
    .unwrap();
    assert!(replies[0].text.contains("SillyTavern 已回退"));
    assert!(replies[0].text.contains("消息已清理"));

    let requests = fixture.server.received_requests().await.unwrap();
    let edits: Vec<_> = requests
        .iter()
        .filter(|request| request.url.path().ends_with("/editMessageText"))
        .collect();
    let deletes: Vec<_> = requests
        .iter()
        .filter(|request| request.url.path().ends_with("/deleteMessage"))
        .collect();
    assert_eq!(edits.len(), 1);
    assert_eq!(deletes.len(), 2);
    let edit_body: serde_json::Value = edits[0].body_json().unwrap();
    assert_eq!(edit_body["message_id"], 501);
    assert_eq!(edit_body["text"], "[已撤回]");
    let deleted_ids: Vec<i64> = deletes
        .iter()
        .map(|request| {
            let body: serde_json::Value = request.body_json().unwrap();
            body["message_id"].as_i64().unwrap()
        })
        .collect();
    assert_eq!(deleted_ids, vec![502, 503]);

    let commits: Vec<_> = fixture
        .backend
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            StBackendCall::Commit { command } => Some(command.mutation),
            _ => None,
        })
        .collect();
    assert_eq!(commits.len(), 1);
    assert!(matches!(commits[0], StMutation::UndoLastTurn { .. }));
}

#[tokio::test]
async fn revoke_with_failed_commit_makes_zero_physical_calls() {
    let fixture = setup_fixture().await;
    fixture.backend.push_probe(Ok(status()));
    fixture.backend.push_snapshot(Ok(dialogue_snapshot()));
    fixture.backend.push_snapshot(Ok(dialogue_snapshot()));
    fixture.backend.push_commit(Ok(not_applied()));

    let error = dispatch_update_with_delivery(
        &fixture.tg,
        &fixture.bot_id,
        &command_update("/revoke", 203),
        Some(&fixture.delivery),
    )
    .await
    .expect_err("failed commit must not run telegram cleanup");
    assert_eq!(error.code, "ST_COMMIT_FAILED");

    let requests = fixture.server.received_requests().await.unwrap();
    assert_eq!(count_methods(&requests, "/editMessageText"), 0);
    assert_eq!(count_methods(&requests, "/deleteMessage"), 0);
}

#[tokio::test]
async fn duplicate_update_does_not_undo_second_turn() {
    let fixture = setup_fixture().await;
    fixture.backend.push_probe(Ok(status()));
    fixture.backend.push_snapshot(Ok(two_turn_snapshot()));
    fixture.backend.push_snapshot(Ok(two_turn_snapshot()));
    fixture.backend.push_commit(Ok(applied()));
    fixture.backend.push_probe(Ok(status()));

    let first = dispatch_update_with_delivery(
        &fixture.tg,
        &fixture.bot_id,
        &command_update("/revoke", 204),
        Some(&fixture.delivery),
    )
    .await
    .unwrap();
    assert!(first[0].text.contains("SillyTavern 已回退"));

    let second = dispatch_update_with_delivery(
        &fixture.tg,
        &fixture.bot_id,
        &command_update("/revoke", 204),
        Some(&fixture.delivery),
    )
    .await
    .unwrap();
    assert!(
        second.is_empty(),
        "duplicate revoke must not emit a second reply"
    );

    let commit_count = fixture
        .backend
        .calls()
        .into_iter()
        .filter(|call| matches!(call, StBackendCall::Commit { .. }))
        .count();
    assert_eq!(commit_count, 1);
}

#[tokio::test]
async fn partial_telegram_failure_does_not_corrupt_commit() {
    let app = common::setup().await;
    let tg = TelegramModule::new(app.pool.clone());
    let backend = Arc::new(ScriptedStBackend::new());
    backend.push_probe(Ok(status()));
    backend.push_snapshot(Ok(dialogue_snapshot()));
    backend.push_snapshot(Ok(dialogue_snapshot()));
    backend.push_commit(Ok(applied()));
    let channel = ChannelContextStore::new(app.pool.clone());
    let registry = Arc::new(SharedMemoryPollerRegistry::new());
    registry.bootstrap_legacy(1).await.unwrap();
    registry
        .transfer(1, PollerOwner::LegacyPlugin, 1, PollerOwner::RustBridge)
        .await
        .unwrap();
    tg.attach_ownership_registry(registry.clone()).await;
    let bot = tg.upsert_bot(&app.actor, None, false).await.unwrap();
    let guard = tg.claim_numeric_bot(1, &bot.id).await.unwrap();
    let context_key = format!("{}:9", bot.id);
    let store = Arc::new(OperationStore::new(app.pool.clone()));
    let vault: Arc<dyn SecretVault> =
        Arc::new(EncryptedSqliteVault::new(app.pool.clone(), [7_u8; 32]));
    let provider = Arc::new(OperationPayloadKeyProvider::new(
        vault.clone(),
        store.clone(),
    ));
    let coordinator = Arc::new(OperationCoordinator::new_with_key_provider(
        backend.clone(),
        store,
        provider,
        Arc::new(MemoryStOperationJournal::new()),
    ));
    let engine = SidecarBridgeEngine::new(backend.clone(), channel.clone())
        .with_context_key(&context_key)
        .with_required_coordinator(coordinator)
        .with_ownership_guard(Arc::new(guard));
    tg.attach_services(TelegramServices {
        identity: app.identity.clone(),
        vault,
        bridge: Some(engine),
        channel: channel.clone(),
        ownership_registry: Some(registry.clone()),
    })
    .await;
    let code = tg
        .generate_bind_code(&bot.id, &app.actor.account.id)
        .await
        .unwrap();
    dispatch_update_with_delivery(
        &tg,
        &bot.id,
        &json!({"message": {"chat": {"id": 9}, "from": {"id": 4242}, "text": format!("/bind {}", code.code)}}),
        None,
    )
    .await
    .unwrap();
    channel
        .select_chat(
            &app.actor.account.id,
            app.actor.workspace_id.as_deref().unwrap(),
            &context_key,
            &locator(),
        )
        .await
        .unwrap();
    let selected_locator = locator();
    let scope = TurnScope::new(
        app.actor.account.id.clone(),
        bot.id.clone(),
        1,
        9,
        im_bridge::modules::bridge::st_ops::locator_hash(
            &selected_locator.handle,
            &selected_locator.avatar,
            &selected_locator.chat_file,
        ),
    )
    .unwrap();

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/editMessageText"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true, "result": true})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/deleteMessage"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({
            "ok": false,
            "description": "synthetic delete failure"
        })))
        .mount(&server)
        .await;
    let delivery = TelegramDelivery::new(
        reqwest::Client::new(),
        app.pool.clone(),
        "test-token".into(),
        server.uri(),
        bot.id.clone(),
        1,
        9,
        0,
        0,
        1,
        1,
        3200,
    );
    seed_turn_deliveries(&app.pool, &scope, &bot.id, &[501, 502, 503]).await;

    let error = dispatch_update_with_delivery(
        &tg,
        &bot.id,
        &command_update("/revoke", 205),
        Some(&delivery),
    )
    .await
    .expect_err("partial Telegram cleanup must remain unknown");
    assert_eq!(error.code, "TELEGRAM_REVOKE_CLEANUP_UNKNOWN");

    let commits: Vec<_> = backend
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            StBackendCall::Commit { command } => Some(command),
            _ => None,
        })
        .collect();
    assert_eq!(commits.len(), 1);
    assert!(matches!(
        commits[0].mutation,
        StMutation::UndoLastTurn { .. }
    ));

    let requests = server.received_requests().await.unwrap();
    assert_eq!(count_methods(&requests, "/editMessageText"), 1);
    assert_eq!(count_methods(&requests, "/deleteMessage"), 2);
}

#[tokio::test]
async fn unknown_commit_does_not_run_telegram_cleanup() {
    let fixture = setup_fixture().await;
    fixture.backend.push_probe(Ok(status()));
    fixture.backend.push_snapshot(Ok(dialogue_snapshot()));
    fixture.backend.push_snapshot(Ok(dialogue_snapshot()));
    fixture.backend.push_commit(Ok(StCommitResult {
        status: StCommitStatus::Unknown,
        new_sha256: None,
        new_integrity: None,
        byte_length: None,
        message_count: None,
    }));

    let error = dispatch_update_with_delivery(
        &fixture.tg,
        &fixture.bot_id,
        &command_update("/revoke", 206),
        Some(&fixture.delivery),
    )
    .await
    .expect_err("unknown commit must not claim telegram cleanup");
    assert_eq!(error.code, "ST_COMMIT_STATE_UNKNOWN");
    let requests = fixture.server.received_requests().await.unwrap();
    assert_eq!(count_methods(&requests, "/editMessageText"), 0);
    assert_eq!(count_methods(&requests, "/deleteMessage"), 0);
}

#[tokio::test]
async fn redo_cleanup_deletes_shrunk_tail_chunks_in_reverse_order() {
    let app = common::setup().await;
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/bottest-token/deleteMessage"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ok": true,
                "result": true
            })),
        )
        .mount(&server)
        .await;
    let scope = TurnScope::new(
        app.actor.account.id.clone(),
        "bot-1",
        1,
        9,
        im_bridge::modules::bridge::st_ops::locator_hash(
            &locator().handle,
            &locator().avatar,
            &locator().chat_file,
        ),
    )
    .unwrap();
    let delivery = TelegramDelivery::new(
        reqwest::Client::new(),
        app.pool.clone(),
        "test-token".into(),
        server.uri(),
        "bot-1".into(),
        1,
        9,
        0,
        0,
        1,
        1,
        3200,
    );
    sqlx::query(
        "INSERT INTO telegram_bots (id, workspace_id, owner_account_id, desired_enabled, created_at, updated_at)
         VALUES (?, ?, ?, 0, '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .bind("bot-1")
    .bind(app.actor.workspace_id.as_deref().unwrap())
    .bind(&app.actor.account.id)
    .execute(&app.pool)
    .await
    .unwrap();
    seed_turn_deliveries(&app.pool, &scope, "bot-1", &[501, 502, 503]).await;
    delivery
        .execute_redo_cleanup_scoped(&scope, &[501, 502, 503], &[501])
        .await
        .unwrap();
    let requests = server.received_requests().await.unwrap();
    let ids = requests
        .iter()
        .map(|request| {
            request.body_json::<serde_json::Value>().unwrap()["message_id"]
                .as_i64()
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![503, 502]);
}
