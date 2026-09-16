use im_bridge::modules::telegram::dispatch::dispatch_update;
use im_bridge::modules::telegram::{TelegramModule, TelegramServices};
use serde_json::json;
use std::sync::Arc;

mod common;

#[tokio::test]
async fn bind_code_and_durable_inbox() {
    let app = common::setup().await;
    let tg = TelegramModule::new(app.pool.clone());
    let bot = tg.upsert_bot(&app.actor, None, false).await.unwrap();
    let code = tg
        .generate_bind_code(&bot.id, &app.actor.account.id)
        .await
        .unwrap();
    assert_eq!(code.code.len(), 6);
    let outcome = tg
        .redeem_bind_code(&bot.id, &app.actor.account.id, &code.code, "4242")
        .await
        .unwrap();
    assert_eq!(outcome, "ok");
    let inserted = tg
        .ingest_update(
            &bot.id,
            17,
            &json!({"update_id": 17, "message": {"text": "/help"}}),
        )
        .await
        .unwrap();
    assert!(inserted);
    let duplicate = tg
        .ingest_update(
            &bot.id,
            17,
            &json!({"update_id": 17, "message": {"text": "/help"}}),
        )
        .await
        .unwrap();
    assert!(!duplicate);
    let offset: i64 =
        sqlx::query_scalar("SELECT next_offset FROM telegram_bot_offsets WHERE bot_id = ?")
            .bind(&bot.id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(offset, 18);
}

#[tokio::test]
async fn telegram_commands_bind_help_and_chat() {
    let app = common::setup().await;
    let _ = common::seed_character_and_provider(&app).await;
    let tg = TelegramModule::new(app.pool.clone());
    let st = Arc::new(common::st_backend::ScriptedStBackend::new());
    for _ in 0..4 {
        st.push_characters(Ok(vec![im_bridge::domain::st::StCharacterSummary {
            avatar: "Synthetic.png".into(),
            name: "TestChar".into(),
            ..Default::default()
        }]));
    }
    for _ in 0..8 {
        st.push_probe(Ok(im_bridge::domain::st::StStatus {
            available: true,
            version: Some("1.16.0-synthetic".into()),
            handle: "default-user".into(),
            capabilities: im_bridge::domain::st::StCapabilities {
                mode: im_bridge::domain::st::StWriteMode::ReadOnly,
                snapshot: false,
                typed_mutations: false,
                integrity_rotation: false,
                operation_replay: false,
            },
        }));
    }
    for _ in 0..2 {
        st.push_chats(Ok(Vec::new()));
    }
    for _ in 0..4 {
        st.push_models(Ok(im_bridge::domain::st::StModelCatalog {
            models: vec![
                im_bridge::domain::st::StModelSummary {
                    id: "synthetic-model".into(),
                    owned_by: Some("synthetic".into()),
                },
                im_bridge::domain::st::StModelSummary {
                    id: "synthetic-compress".into(),
                    owned_by: Some("synthetic".into()),
                },
            ],
            current_model: Some("synthetic-model".into()),
        }));
    }
    let channel =
        im_bridge::modules::bridge::channel_context::ChannelContextStore::new(app.pool.clone());
    let bridge =
        im_bridge::modules::bridge::engine::SidecarBridgeEngine::new(st.clone(), channel.clone());
    tg.attach_services(TelegramServices {
        identity: app.identity.clone(),
        vault: Arc::new(DummyVault),
        bridge: Some(bridge),
        channel,
        ownership_registry: None,
    })
    .await;
    let bot = tg.upsert_bot(&app.actor, None, false).await.unwrap();
    let code = tg
        .generate_bind_code(&bot.id, &app.actor.account.id)
        .await
        .unwrap();
    let unauthorized = dispatch_update(
        &tg,
        &bot.id,
        &json!({"message": {"chat": {"id": 9}, "from": {"id": 4242}, "text": "/help"}}),
    )
    .await
    .unwrap();
    assert!(unauthorized[0].text.contains("未授权"));
    let bound = dispatch_update(
        &tg,
        &bot.id,
        &json!({"message": {"chat": {"id": 9}, "from": {"id": 4242}, "text": format!("/bind@test_bot {}", code.code)}}),
    )
    .await
    .unwrap();
    assert!(bound[0].text.contains("绑定成功"));
    let empty_error = dispatch_update(
        &tg,
        &bot.id,
        &json!({"message": {"chat": {"id": 9}, "from": {"id": 4242}, "text": "/error"}}),
    )
    .await
    .unwrap();
    assert!(empty_error[0].text.contains("没有可查看的错误记录"));
    let help = dispatch_update(
        &tg,
        &bot.id,
        &json!({"message": {"chat": {"id": 9}, "from": {"id": 4242}, "text": "/help"}}),
    )
    .await
    .unwrap();
    assert!(help[0].text.contains("/undo"));
    assert!(help[0].text.contains("/revoke"));
    assert!(help[0].text.contains("/error"));
    sqlx::query(
        "INSERT INTO bridge_error_events
            (actor_id, bot_id, chat_id, stage, code, safe_message, retryable, commit_state, attempt, request_id, trace_id, created_at)
         VALUES (?, ?, '9', 'generation', 'ST_GENERATE_UPSTREAM_5XX', 'synthetic safe message', 1, 'not_started', 1, ?, 'trace-short', ?)",
    )
    .bind(&app.actor.account.id)
    .bind(&bot.id)
    .bind("request-private-synthetic-id-do-not-leak")
    .bind("2026-09-04T00:00:00Z")
    .execute(&app.pool)
    .await
    .unwrap();
    let last_error = dispatch_update(
        &tg,
        &bot.id,
        &json!({"message": {"chat": {"id": 9}, "from": {"id": 4242}, "text": "/error"}}),
    )
    .await
    .unwrap();
    assert!(last_error[0].text.contains("ST_GENERATE_UPSTREAM_5XX"));
    assert!(!last_error[0]
        .text
        .contains("request-private-synthetic-id-do-not-leak"));
    let chars = dispatch_update(
        &tg,
        &bot.id,
        &json!({"message": {"chat": {"id": 9}, "from": {"id": 4242}, "text": "/chars"}}),
    )
    .await
    .unwrap();
    assert!(chars[0].text.contains("TestChar") || chars[0].markup.is_some());
    let selected = dispatch_update(
        &tg,
        &bot.id,
        &json!({"callback_query": {"id": "cbq-select-char", "from": {"id": 4242}, "message": {"chat": {"id": 9}, "message_id": 201}, "data": "cs:0:0"}}),
    )
    .await
    .unwrap();
    assert!(selected[0].text.contains("选择已失效，请重新打开面板"));
    let before_counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM conversations),
            (SELECT COUNT(*) FROM messages),
            (SELECT COUNT(*) FROM generation_runs)",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    let frozen_new = dispatch_update(
        &tg,
        &bot.id,
        &json!({"update_id": 100, "message": {"chat": {"id": 9}, "from": {"id": 4242}, "text": "/new"}}),
    )
    .await
    .expect_err("writes without a selected character must fail closed");
    assert_eq!(frozen_new.code, "CHARACTER_NOT_SELECTED");
    for (update_id, text) in [
        (101, "hello"),
        (102, "/undo"),
        (103, "/revoke"),
        (104, "/redo"),
        (105, "/compress"),
    ] {
        let error = dispatch_update(
            &tg,
            &bot.id,
            &json!({"update_id": update_id, "message": {"chat": {"id": 9}, "from": {"id": 4242}, "text": text}}),
        )
        .await
        .expect_err("writes without a selected character must fail closed");
        assert_eq!(error.code, "CHARACTER_NOT_SELECTED");
    }
    let after_counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM conversations),
            (SELECT COUNT(*) FROM messages),
            (SELECT COUNT(*) FROM generation_runs)",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(after_counts, before_counts);
    let model = dispatch_update(
        &tg,
        &bot.id,
        &json!({"callback_query": {"id": "cbq-model-chat", "from": {"id": 4242}, "message": {"chat": {"id": 9}, "message_id": 202}, "data": "md:chat:0"}}),
    )
    .await
    .unwrap();
    assert!(model[0].text.contains("选择已失效，请重新打开面板"));
    let compression_model = dispatch_update(
        &tg,
        &bot.id,
        &json!({"callback_query": {"id": "cbq-model-comp", "from": {"id": 4242}, "message": {"chat": {"id": 9}, "message_id": 203}, "data": "md:compression:0"}}),
    )
    .await
    .unwrap();
    assert!(compression_model[0]
        .text
        .contains("选择已失效，请重新打开面板"));
    let second_code = tg
        .generate_bind_code(&bot.id, &app.actor.account.id)
        .await
        .unwrap();
    assert_eq!(
        tg.redeem_bind_code(&bot.id, &app.actor.account.id, &second_code.code, "4242")
            .await
            .unwrap(),
        "ok"
    );
    let active_bindings: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM telegram_bindings WHERE bot_id = ? AND revoked_at IS NULL",
    )
    .bind(&bot.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(active_bindings, 1);
}

#[tokio::test]
async fn bind_rate_limit_and_identity_conflict_are_enforced() {
    let app = common::setup().await;
    let tg = TelegramModule::new(app.pool.clone());
    let bot = tg.upsert_bot(&app.actor, None, false).await.unwrap();
    let code = tg
        .generate_bind_code(&bot.id, &app.actor.account.id)
        .await
        .unwrap();
    for _ in 0..10 {
        assert_eq!(
            tg.redeem_bind_code(&bot.id, &app.actor.account.id, "WRONG1", "9999")
                .await
                .unwrap(),
            "invalid"
        );
    }
    assert_eq!(
        tg.redeem_bind_code(&bot.id, &app.actor.account.id, &code.code, "9999")
            .await
            .unwrap(),
        "rate_limited"
    );
    let locked_until: String = sqlx::query_scalar(
        "SELECT locked_until FROM bind_attempts WHERE account_id = ? AND telegram_user_id = '9999'",
    )
    .bind(&app.actor.account.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert!(locked_until > im_bridge::clock::now_rfc3339());

    let first_code = tg
        .generate_bind_code(&bot.id, &app.actor.account.id)
        .await
        .unwrap();
    assert_eq!(
        tg.redeem_bind_code(&bot.id, &app.actor.account.id, &first_code.code, "7777")
            .await
            .unwrap(),
        "ok"
    );
    let (other_account, other_workspace) = app
        .identity
        .ensure_legacy_account("other-user", "Other user")
        .await
        .unwrap();
    let other_actor = app
        .identity
        .actor_in_workspace(other_account, &other_workspace)
        .await
        .unwrap();
    let other_bot = tg.upsert_bot(&other_actor, None, false).await.unwrap();
    let other_code = tg
        .generate_bind_code(&other_bot.id, &other_actor.account.id)
        .await
        .unwrap();
    assert_eq!(
        tg.redeem_bind_code(
            &other_bot.id,
            &other_actor.account.id,
            &other_code.code,
            "7777",
        )
        .await
        .unwrap(),
        "identity_conflict"
    );
}

#[tokio::test]
async fn legacy_history_callback_expires_before_snapshot() {
    let app = common::setup().await;
    let tg = TelegramModule::new(app.pool.clone());
    let st = Arc::new(common::st_backend::ScriptedStBackend::new());
    for _ in 0..2 {
        st.push_characters(Ok(vec![im_bridge::domain::st::StCharacterSummary {
            avatar: "Tokyo_Womens_Life_Narrative_GM.png".into(),
            name: "东京女子生活叙事GM".into(),
            ..Default::default()
        }]));
    }
    st.push_probe(Ok(im_bridge::domain::st::StStatus {
        available: true,
        version: Some("1.16.0-synthetic".into()),
        handle: "default-user".into(),
        capabilities: im_bridge::domain::st::StCapabilities {
            mode: im_bridge::domain::st::StWriteMode::TestWrite,
            snapshot: true,
            typed_mutations: true,
            integrity_rotation: true,
            operation_replay: true,
        },
    }));
    for _ in 0..2 {
        st.push_chats(Ok(vec![im_bridge::domain::st::StChatSummary {
            chat_file: "东京女子生活叙事GM - 2026-08-28@17h16m04s631ms.jsonl".into(),
            title: Some("东京女子生活叙事GM - 2026-08-28@17h16m04s631ms".into()),
            updated_at: None,
            message_count: None,
        }]));
    }
    st.push_snapshot(Err(
        im_bridge::modules::bridge::errors::StBridgeError::boxed(
            im_bridge::modules::bridge::errors::StErrorCode::StChatNotFound,
            im_bridge::modules::bridge::errors::StErrorStage::Snapshot,
            "找不到对应的 SillyTavern 聊天，本次没有写入。",
            false,
            im_bridge::modules::bridge::errors::CommitState::NotStarted,
        ),
    ));
    let channel =
        im_bridge::modules::bridge::channel_context::ChannelContextStore::new(app.pool.clone());
    let bridge =
        im_bridge::modules::bridge::engine::SidecarBridgeEngine::new(st.clone(), channel.clone());
    tg.attach_services(TelegramServices {
        identity: app.identity.clone(),
        vault: Arc::new(DummyVault),
        bridge: Some(bridge),
        channel,
        ownership_registry: None,
    })
    .await;
    let bot = tg.upsert_bot(&app.actor, None, false).await.unwrap();
    let code = tg
        .generate_bind_code(&bot.id, &app.actor.account.id)
        .await
        .unwrap();
    dispatch_update(
        &tg,
        &bot.id,
        &json!({"message": {"chat": {"id": 9}, "from": {"id": 4242}, "text": format!("/bind {}", code.code)}}),
    )
    .await
    .unwrap();
    dispatch_update(
        &tg,
        &bot.id,
        &json!({"callback_query": {"id": "cbq-hist-char", "from": {"id": 4242}, "message": {"chat": {"id": 9}, "message_id": 301}, "data": "cs:0:0"}}),
    )
    .await
    .unwrap();
    let expired = dispatch_update(
        &tg,
        &bot.id,
        &json!({"callback_query": {"id": "cbq-hist-select", "from": {"id": 4242}, "message": {"chat": {"id": 9}, "message_id": 302}, "data": "ho:0:0"}}),
    )
    .await
    .unwrap();
    assert!(expired[0].text.contains("选择已失效，请重新打开面板"));
    assert!(
        !st.calls()
            .iter()
            .any(|call| matches!(call, common::st_backend::StBackendCall::Snapshot { .. })),
        "legacy history callback must expire before snapshot"
    );
}

struct DummyVault;

#[async_trait::async_trait]
impl im_bridge::seams::secret_vault::SecretVault for DummyVault {
    async fn put(
        &self,
        _owner_scope: &str,
        kind: &str,
        _plaintext: &[u8],
    ) -> im_bridge::error::AppResult<im_bridge::seams::secret_vault::SecretMeta> {
        Ok(im_bridge::seams::secret_vault::SecretMeta {
            id: "dummy".into(),
            kind: kind.into(),
            fingerprint: "fp_dummy".into(),
            configured: true,
        })
    }
    async fn get(&self, _secret_id: &str) -> im_bridge::error::AppResult<Vec<u8>> {
        Ok(b"dummy".to_vec())
    }
    async fn delete(&self, _secret_id: &str) -> im_bridge::error::AppResult<()> {
        Ok(())
    }
    async fn fingerprint(&self, _secret_id: &str) -> im_bridge::error::AppResult<String> {
        Ok("fp_dummy".into())
    }
    async fn rotate_master_key(
        &self,
        _new_key: &[u8],
        _dry_run: bool,
    ) -> im_bridge::error::AppResult<u32> {
        Ok(0)
    }
}

#[test]
fn split_text_keeps_paragraphs() {
    let text = "a\n\nb\n\n".to_string() + &"c".repeat(80);
    let parts = TelegramModule::split_text(&text, 20);
    assert!(parts.len() >= 2);
    let unicode = TelegramModule::split_text(&"你好，世界。".repeat(30), 20);
    assert!(unicode.len() > 1);
    assert!(unicode.iter().all(|item| item.encode_utf16().count() <= 20));
    let supplementary = TelegramModule::split_text(&"𠀀".repeat(20), 10);
    assert_eq!(supplementary.len(), 4);
    assert!(supplementary
        .iter()
        .all(|item| item.encode_utf16().count() <= 10));
    assert!(TelegramModule::help_text().contains("/undo"));
    assert!(TelegramModule::help_text().contains("/revoke"));
    assert!(TelegramModule::help_text().contains("Telegram 消息保持保留"));
}
