use std::sync::Arc;

use im_bridge::domain::st::{
    PollerRuntimeFence, StCapabilities, StChatLocator, StChatMessage, StChatSnapshot,
    StCommitStatus, StCompressionExtraPatch, StGenerationRequest, StGenerationResult,
    StMessagePatch, StMutation, StPromptMessage, StStatus, StWriteMode,
};
use im_bridge::modules::bridge::errors::{CommitState, StBridgeError, StErrorCode, StErrorStage};
use im_bridge::seams::st_backend::StBackend;
use serde_json::json;

mod common;

use common::st_backend::{ScriptedStBackend, StBackendCall};

fn locator(avatar: &str, chat_file: &str, character_name: &str) -> StChatLocator {
    StChatLocator {
        handle: "synthetic-handle".into(),
        avatar: avatar.into(),
        character_name: character_name.into(),
        chat_file: chat_file.into(),
    }
}

fn fence() -> PollerRuntimeFence {
    PollerRuntimeFence {
        telegram_bot_id: 1,
        owner: "rust_bridge".into(),
        epoch: 1,
        internal_bot_id: "synthetic-bot".into(),
        runtime_instance_id: "synthetic-runtime".into(),
    }
}

fn snapshot(locator: StChatLocator) -> StChatSnapshot {
    StChatSnapshot {
        locator,
        parsed_chat: vec![json!({"fixture": "synthetic"})],
        source_sha256: "sha256-source-fixture".into(),
        source_integrity: "integrity-source-fixture".into(),
        source_byte_length: 123,
        source_message_count: 1,
    }
}

#[tokio::test]
async fn locator_lookup_uses_avatar_and_chat_file_not_display_name() {
    let backend = Arc::new(ScriptedStBackend::new());
    let first = locator("First.png", "first.jsonl", "Same Display Name");
    backend.push_snapshot(Ok(snapshot(first.clone())));
    let result = backend.snapshot(&first).await.unwrap();
    assert_eq!(result.locator.avatar, "First.png");
    assert_eq!(result.locator.chat_file, "first.jsonl");
    assert_eq!(result.locator.character_name, "Same Display Name");

    let calls = backend.calls();
    assert!(matches!(
        calls.as_slice(),
        [StBackendCall::Snapshot { locator }]
            if locator.avatar == "First.png" && locator.chat_file == "first.jsonl"
    ));
}

#[tokio::test]
async fn snapshot_revision_is_adapter_supplied_and_separate_from_parsed_chat() {
    let backend = Arc::new(ScriptedStBackend::new());
    let locator = locator("Synthetic.png", "synthetic.jsonl", "Synthetic Character");
    let mut expected = snapshot(locator.clone());
    expected.parsed_chat = vec![json!({
        "chat_metadata": {"integrity": "payload-value-only"},
        "fixture": "synthetic",
    })];
    expected.source_sha256 = "adapter-raw-sha256".into();
    expected.source_integrity = "adapter-integrity".into();
    expected.source_byte_length = 4096;
    expected.source_message_count = 7;
    backend.push_snapshot(Ok(expected.clone()));

    let actual = backend.snapshot(&locator).await.unwrap();
    assert_eq!(actual.parsed_chat, expected.parsed_chat);
    assert_eq!(actual.source_revision(), expected.source_revision());
    assert_ne!(actual.source_sha256, "payload-value-only");
}

#[test]
fn typed_mutations_have_only_the_approved_shapes() {
    let append = StMutation::AppendTurn {
        user_message: StChatMessage {
            name: "Synthetic User".into(),
            is_user: true,
            mes: "synthetic user".into(),
            send_date: None,
            extra: Default::default(),
        },
        assistant_message: StChatMessage {
            name: "Synthetic Character".into(),
            is_user: false,
            mes: "synthetic assistant".into(),
            send_date: None,
            extra: Default::default(),
        },
    };
    let undo = StMutation::UndoLastTurn {
        expected_user_sha256: "user-sha".into(),
        expected_assistant_sha256: "assistant-sha".into(),
    };
    let replace = StMutation::ReplaceLastAssistant {
        expected_assistant_sha256: "assistant-sha".into(),
        assistant_message: StChatMessage {
            name: "Synthetic Character".into(),
            is_user: false,
            mes: "replacement".into(),
            send_date: None,
            extra: Default::default(),
        },
    };
    let compress = StMutation::CompressMessages {
        patches: vec![StMessagePatch {
            index: 1,
            expected_message_sha256: "message-sha".into(),
            mes: Some("compressed synthetic text".into()),
            extra: Some(StCompressionExtraPatch {
                display_text: Some("display text".into()),
                compressed: Some(true),
            }),
        }],
    };

    let encoded_append = serde_json::to_value(&append).unwrap();
    assert_eq!(encoded_append["kind"], "append_turn");
    assert!(encoded_append.get("userMessage").is_some());
    assert!(encoded_append.get("assistantMessage").is_some());
    assert!(encoded_append.get("user_message").is_none());
    assert!(encoded_append.get("assistant_message").is_none());
    let encoded_user = &encoded_append["userMessage"];
    assert!(encoded_user.get("isUser").is_some());
    assert!(encoded_user.get("is_user").is_none());
    assert!(encoded_user.get("sendDate").is_some());
    assert!(encoded_user.get("send_date").is_none());

    let encoded_undo = serde_json::to_value(&undo).unwrap();
    assert_eq!(encoded_undo["kind"], "undo_last_turn");
    assert_eq!(encoded_undo["expectedUserSha256"], "user-sha");
    assert_eq!(encoded_undo["expectedAssistantSha256"], "assistant-sha");
    assert!(encoded_undo.get("expected_user_sha256").is_none());
    assert!(encoded_undo.get("expected_assistant_sha256").is_none());
    let legacy_tail_key = ["expected", "tail", "sha256"].join("_");
    assert!(encoded_undo.get(&legacy_tail_key).is_none());

    let encoded_replace = serde_json::to_value(&replace).unwrap();
    assert_eq!(encoded_replace["kind"], "replace_last_assistant");
    assert!(encoded_replace.get("assistantMessage").is_some());
    assert_eq!(encoded_replace["expectedAssistantSha256"], "assistant-sha");
    assert!(encoded_replace.get("assistant_message").is_none());
    assert!(encoded_replace.get("expected_assistant_sha256").is_none());

    let encoded_compress = serde_json::to_value(&compress).unwrap();
    assert_eq!(encoded_compress["kind"], "compress_messages");
    let encoded_patch = &encoded_compress["patches"][0];
    assert_eq!(encoded_patch["expectedMessageSha256"], "message-sha");
    assert!(encoded_patch.get("expected_message_sha256").is_none());
    let encoded_extra = &encoded_patch["extra"];
    assert_eq!(encoded_extra["displayText"], "display text");
    assert_eq!(encoded_extra["compressed"], true);
    assert!(encoded_extra.get("metadata").is_none());

    for encoded in [
        encoded_append,
        encoded_undo,
        encoded_replace,
        encoded_compress,
    ] {
        for key in [
            "target_chat",
            "targetChat",
            "full_chat",
            "fullChat",
            "path",
            "force",
            "metadata",
            "deleteIndex",
            "removeIndex",
            "deleteIndices",
        ] {
            assert!(encoded.get(key).is_none(), "unexpected key: {key}");
        }
    }

    let command = im_bridge::domain::st::CommitStChat {
        operation_id: "operation-synthetic".into(),
        locator: locator("Synthetic.png", "synthetic.jsonl", "Synthetic Character"),
        expected_sha256: "before-sha".into(),
        expected_integrity: "before-integrity".into(),
        mutation: append,
        fence: fence(),
    };
    let encoded_command = serde_json::to_value(command).unwrap();
    assert_eq!(encoded_command["operationId"], "operation-synthetic");
    assert_eq!(encoded_command["expectedSha256"], "before-sha");
    assert_eq!(encoded_command["expectedIntegrity"], "before-integrity");
    assert!(encoded_command.get("operation_id").is_none());
    assert!(encoded_command.get("expected_sha256").is_none());
    assert!(encoded_command.get("expected_integrity").is_none());
    let encoded_locator = &encoded_command["locator"];
    assert_eq!(encoded_locator["characterName"], "Synthetic Character");
    assert_eq!(encoded_locator["chatFile"], "synthetic.jsonl");
    assert!(encoded_locator.get("character_name").is_none());
    assert!(encoded_locator.get("chat_file").is_none());
}

#[test]
fn synthetic_fixtures_match_the_declared_st_response_shapes() {
    let characters: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/st/characters-list.json")).unwrap();
    assert!(characters.as_array().is_some_and(|items| !items.is_empty()));

    let character_card: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/st/character-card.json")).unwrap();
    assert_eq!(character_card["spec"], "chara_card_v2");

    let chats: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/st/chat-search.json")).unwrap();
    assert!(chats.as_array().is_some_and(|items| !items.is_empty()));
    assert!(chats[0].get("file_name").is_some());

    let settings: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/st/settings.json")).unwrap();
    let settings_text = settings["settings"].as_str().unwrap();
    let settings_value: serde_json::Value = serde_json::from_str(settings_text).unwrap();
    assert_eq!(
        settings_value["oai_settings"]["custom_model"],
        "synthetic-model"
    );

    let models: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/st/model-catalog.json")).unwrap();
    assert!(models["data"]
        .as_array()
        .is_some_and(|items| !items.is_empty()));

    let raw_lines = include_str!("fixtures/st/raw-chat.jsonl");
    assert_eq!(raw_lines.lines().count(), 3);
    for line in raw_lines.lines() {
        serde_json::from_str::<serde_json::Value>(line).unwrap();
    }
    assert!(include_str!("fixtures/st/sse-normal.txt").contains("[DONE]"));
    assert!(include_str!("fixtures/st/sse-malformed.txt").contains("not-json"));
    serde_json::from_str::<serde_json::Value>(include_str!("fixtures/st/error-http-json.json"))
        .unwrap();
    assert!(include_str!("fixtures/st/error-html.html").contains("Synthetic Error"));
    assert!(include_str!("fixtures/st/error-plaintext.txt").contains("fixture only"));
    let invalid: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/st/chat-invalid-payload.json")).unwrap();
    assert!(invalid.is_object());
}

#[tokio::test]
async fn scripted_input_is_stable_and_errors_are_returned_unchanged() {
    let first = Arc::new(ScriptedStBackend::new());
    let second = Arc::new(ScriptedStBackend::new());
    let status = StStatus {
        available: true,
        version: Some("1.16.0-synthetic".into()),
        handle: "synthetic-handle".into(),
        capabilities: StCapabilities {
            mode: StWriteMode::ReadOnly,
            snapshot: true,
            typed_mutations: true,
            integrity_rotation: true,
            operation_replay: true,
        },
    };
    first.push_probe(Ok(status.clone()));
    second.push_probe(Ok(status.clone()));
    assert_eq!(first.probe().await.unwrap(), second.probe().await.unwrap());

    let request = StGenerationRequest {
        model_id: Some("synthetic-model".into()),
        messages: vec![StPromptMessage {
            role: "user".into(),
            content: "synthetic prompt".into(),
            name: Some("Synthetic User".into()),
        }],
        temperature: Some(0.7),
        top_p: Some(0.9),
        max_tokens: Some(64),
    };
    let result = StGenerationResult {
        text: "synthetic reply".into(),
        finish_reason: Some("stop".into()),
        usage: None,
    };
    first.push_generation(Ok(result.clone()));
    assert_eq!(
        first
            .stream_generate(request, None, tokio_util::sync::CancellationToken::new())
            .await
            .unwrap(),
        result
    );

    let failed = Arc::new(ScriptedStBackend::new());
    failed.push_probe(Err(StBridgeError::boxed(
        StErrorCode::StConnectFailed,
        StErrorStage::Connect,
        "synthetic connection failure",
        true,
        CommitState::NotStarted,
    )));
    let error = failed.probe().await.expect_err("scripted error expected");
    assert_eq!(error.code, StErrorCode::StConnectFailed);
    assert_eq!(error.stage, StErrorStage::Connect);
    assert_eq!(error.commit_state, CommitState::NotStarted);
    assert!(error.request_id.len() >= 8);
    assert!(error.trace_id.len() >= 8);

    let commit = im_bridge::domain::st::StCommitResult {
        status: StCommitStatus::AlreadyApplied,
        new_sha256: Some("after-sha".into()),
        new_integrity: Some("after-integrity".into()),
        byte_length: Some(128),
        message_count: Some(4),
    };
    first.push_commit(Ok(commit.clone()));
    let command = im_bridge::domain::st::CommitStChat {
        operation_id: "operation-synthetic".into(),
        locator: locator("Synthetic.png", "synthetic.jsonl", "Synthetic Character"),
        expected_sha256: "before-sha".into(),
        expected_integrity: "before-integrity".into(),
        mutation: StMutation::UndoLastTurn {
            expected_user_sha256: "user-sha".into(),
            expected_assistant_sha256: "assistant-sha".into(),
        },
        fence: fence(),
    };
    assert_eq!(first.commit(command).await.unwrap(), commit);
}
