use im_bridge::modules::chat::{ChatCommand, ChatEngine, ChatQuery};
use im_bridge::seams::llm_gateway::LlmGateway;

mod common;

#[tokio::test]
async fn send_undo_redo_and_idempotency() {
    let app = common::setup().await;
    let (character_id, _) = common::seed_character_and_provider(&app).await;
    let started = app
        .chat
        .execute(
            &app.actor,
            ChatCommand::StartConversation {
                character_id,
                channel: "web".into(),
                external_context_key: "web-default".into(),
                client_turn_id: None,
            },
            None,
        )
        .await
        .unwrap();
    let conversation_id = started.conversation.unwrap().id;

    let first = app
        .chat
        .execute(
            &app.actor,
            ChatCommand::SendMessage {
                conversation_id: conversation_id.clone(),
                text: "The harbor is quiet tonight.".into(),
                channel: "web".into(),
                external_context_key: "web-default".into(),
                client_turn_id: Some("turn-1".into()),
                expected_revision: None,
                model_preset_id: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        first.reply_text.as_deref(),
        Some("The lighthouse is still on.")
    );

    let replay = app
        .chat
        .execute(
            &app.actor,
            ChatCommand::SendMessage {
                conversation_id: conversation_id.clone(),
                text: "The harbor is quiet tonight.".into(),
                channel: "web".into(),
                external_context_key: "web-default".into(),
                client_turn_id: Some("turn-1".into()),
                expected_revision: None,
                model_preset_id: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(replay.operation_id, first.operation_id);
    assert_eq!(replay.reply_text, first.reply_text);

    let history = app
        .chat
        .query(
            &app.actor,
            ChatQuery::GetHistory {
                conversation_id: conversation_id.clone(),
                known_revision: None,
            },
        )
        .await
        .unwrap();
    let items = history.history.unwrap().items;
    assert!(items
        .iter()
        .any(|item| item.is_user && item.text.contains("harbor")));
    assert!(items
        .iter()
        .any(|item| !item.is_user && item.text.contains("lighthouse")));

    app.llm.push_reply("The tide receded an inch.");
    let _ = app
        .chat
        .execute(
            &app.actor,
            ChatCommand::RegenerateReply {
                conversation_id: conversation_id.clone(),
                channel: "web".into(),
                external_context_key: "web-default".into(),
                client_turn_id: Some("redo-1".into()),
                expected_revision: None,
                model_preset_id: None,
            },
            None,
        )
        .await
        .unwrap();
    let after_redo = app
        .chat
        .query(
            &app.actor,
            ChatQuery::GetHistory {
                conversation_id: conversation_id.clone(),
                known_revision: None,
            },
        )
        .await
        .unwrap()
        .history
        .unwrap()
        .items;
    assert!(after_redo
        .iter()
        .any(|item| item.text.contains("tide receded")));
    assert!(!after_redo
        .iter()
        .any(|item| item.text.contains("lighthouse is still on")));

    let undo = app
        .chat
        .execute(
            &app.actor,
            ChatCommand::UndoLastTurn {
                conversation_id: conversation_id.clone(),
                channel: "web".into(),
                external_context_key: "web-default".into(),
                client_turn_id: Some("undo-1".into()),
                expected_revision: None,
            },
            None,
        )
        .await
        .unwrap();
    let undo_replay = app
        .chat
        .execute(
            &app.actor,
            ChatCommand::UndoLastTurn {
                conversation_id: conversation_id.clone(),
                channel: "web".into(),
                external_context_key: "web-default".into(),
                client_turn_id: Some("undo-1".into()),
                expected_revision: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(undo_replay.operation_id, undo.operation_id);
    let after_undo = app
        .chat
        .query(
            &app.actor,
            ChatQuery::GetHistory {
                conversation_id: conversation_id.clone(),
                known_revision: None,
            },
        )
        .await
        .unwrap()
        .history
        .unwrap()
        .items;
    assert!(!after_undo
        .iter()
        .any(|item| item.text.contains("tide receded")));
}

#[tokio::test]
async fn failed_regeneration_restores_previous_assistant() {
    let app = common::setup().await;
    let (character_id, _) = common::seed_character_and_provider(&app).await;
    let conversation_id = app
        .chat
        .execute(
            &app.actor,
            ChatCommand::StartConversation {
                character_id,
                channel: "telegram".into(),
                external_context_key: "bot:chat".into(),
                client_turn_id: None,
            },
            None,
        )
        .await
        .unwrap()
        .conversation
        .unwrap()
        .id;
    app.chat
        .execute(
            &app.actor,
            ChatCommand::SendMessage {
                conversation_id: conversation_id.clone(),
                text: "hello".into(),
                channel: "telegram".into(),
                external_context_key: "bot:chat".into(),
                client_turn_id: Some("send-before-failed-redo".into()),
                expected_revision: None,
                model_preset_id: None,
            },
            None,
        )
        .await
        .unwrap();
    app.llm.fail_next();
    assert!(app
        .chat
        .execute(
            &app.actor,
            ChatCommand::RegenerateReply {
                conversation_id: conversation_id.clone(),
                channel: "telegram".into(),
                external_context_key: "bot:chat".into(),
                client_turn_id: Some("failed-redo".into()),
                expected_revision: None,
                model_preset_id: None,
            },
            None,
        )
        .await
        .is_err());
    let history = app
        .chat
        .query(
            &app.actor,
            ChatQuery::GetHistory {
                conversation_id,
                known_revision: None,
            },
        )
        .await
        .unwrap()
        .history
        .unwrap()
        .items;
    assert!(history
        .iter()
        .any(|item| item.text.contains("lighthouse is still on")));
}

#[tokio::test]
async fn compression_keeps_original() {
    let app = common::setup().await;
    let (character_id, _) = common::seed_character_and_provider(&app).await;
    let started = app
        .chat
        .execute(
            &app.actor,
            ChatCommand::StartConversation {
                character_id,
                channel: "web".into(),
                external_context_key: "web-default".into(),
                client_turn_id: None,
            },
            None,
        )
        .await
        .unwrap();
    let conversation_id = started.conversation.unwrap().id;
    let sent = app
        .chat
        .execute(
            &app.actor,
            ChatCommand::SendMessage {
                conversation_id: conversation_id.clone(),
                text: "continue".into(),
                channel: "web".into(),
                external_context_key: "web-default".into(),
                client_turn_id: Some("c1".into()),
                expected_revision: None,
                model_preset_id: None,
            },
            None,
        )
        .await
        .unwrap();
    let original_reply = sent.reply_text.clone().unwrap();
    app.llm.push_reply("short");
    let _ = app
        .chat
        .execute(
            &app.actor,
            ChatCommand::CompressHistory {
                conversation_id: conversation_id.clone(),
                channel: "web".into(),
                external_context_key: "web-default".into(),
                client_turn_id: None,
                keep_recent: 0,
                model_preset_id: None,
            },
            None,
        )
        .await
        .unwrap();
    let originals: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT content_original, prompt_content FROM messages WHERE conversation_id = ? AND role = 'assistant' ORDER BY sequence",
    )
    .bind(&conversation_id)
    .fetch_all(&app.pool)
    .await
    .unwrap();
    assert!(originals
        .iter()
        .any(|(original, _)| original == &original_reply));
}

#[tokio::test]
async fn channel_model_override_is_used_for_generation() {
    let app = common::setup().await;
    let (character_id, _) = common::seed_character_and_provider(&app).await;
    let provider_id: String = sqlx::query_scalar("SELECT id FROM provider_profiles LIMIT 1")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    let default_preset_id: String =
        sqlx::query_scalar("SELECT id FROM model_presets WHERE label = 'fake' LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let alt = app
        .models
        .upsert_preset(
            &provider_id,
            "alt-model",
            "alternate",
            "chat",
            0.7,
            0.9,
            512,
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workspace_settings SET default_chat_model_preset_id = ? WHERE workspace_id = ?",
    )
    .bind(default_preset_id)
    .bind(app.actor.require_workspace().unwrap())
    .execute(&app.pool)
    .await
    .unwrap();
    app.chat
        .execute(
            &app.actor,
            ChatCommand::SetChannelModel {
                channel: "telegram".into(),
                external_context_key: "bot:chat".into(),
                purpose: "chat".into(),
                model_preset_id: Some(alt.id),
            },
            None,
        )
        .await
        .unwrap();
    let conversation_id = app
        .chat
        .execute(
            &app.actor,
            ChatCommand::StartConversation {
                character_id,
                channel: "telegram".into(),
                external_context_key: "bot:chat".into(),
                client_turn_id: None,
            },
            None,
        )
        .await
        .unwrap()
        .conversation
        .unwrap()
        .id;
    app.chat
        .execute(
            &app.actor,
            ChatCommand::SendMessage {
                conversation_id,
                text: "hello".into(),
                channel: "telegram".into(),
                external_context_key: "bot:chat".into(),
                client_turn_id: Some("override-1".into()),
                expected_revision: None,
                model_preset_id: None,
            },
            None,
        )
        .await
        .unwrap();
    let snapshot: String = sqlx::query_scalar(
        "SELECT provider_snapshot_json FROM generation_runs WHERE client_turn_id = 'override-1'",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert!(snapshot.contains("alt-model"));
}

#[tokio::test]
async fn fake_llm_lists_model() {
    let app = common::setup().await;
    let models = app
        .llm
        .list_models(&im_bridge::seams::llm_gateway::ResolvedProvider {
            id: "p".into(),
            base_url: "http://127.0.0.1".into(),
            api_key: None,
            custom_headers: vec![],
            custom_prompt_post_processing: String::new(),
            model: "fake-model".into(),
            temperature: 1.0,
            top_p: 1.0,
            max_tokens: 16,
            hard_timeout_ms: 1000,
            idle_timeout_ms: 1000,
        })
        .await
        .unwrap();
    assert_eq!(models[0].id, "fake-model");
}
