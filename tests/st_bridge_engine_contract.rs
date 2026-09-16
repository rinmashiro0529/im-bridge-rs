use std::sync::Arc;

use im_bridge::adapters::secrets::encrypted_sqlite::EncryptedSqliteVault;
use im_bridge::domain::identity::{Account, Actor, WorkspaceRole};
use im_bridge::domain::st::{
    StCapabilities, StChatLocator, StChatSnapshot, StCommitResult, StCommitStatus,
    StGenerationResult, StGenerationSettings, StMutation, StStatus, StWriteMode,
};
use im_bridge::modules::bridge::channel_context::ChannelContextStore;
use im_bridge::modules::bridge::engine::SidecarBridgeEngine;
use im_bridge::modules::bridge::errors::{CommitState, StBridgeError, StErrorCode, StErrorStage};
use im_bridge::modules::bridge::operation_coordinator::OperationCoordinator;
use im_bridge::modules::bridge::operation_payload::OperationPayloadKeyProvider;
use im_bridge::modules::bridge::operation_store::OperationStore;
use im_bridge::modules::bridge::poller_ownership::{
    PollerOwner, PollerOwnershipGuard, PollerRuntimeBinding, SharedMemoryPollerRegistry,
};
use im_bridge::seams::st_bridge_engine::{
    BridgeOperationOrigin, StBridgeCommand, StBridgeEngine, StBridgeOutcome, StBridgeQuery,
    StBridgeView,
};
use im_bridge::seams::st_operation_journal::MemoryStOperationJournal;

mod common;

use common::st_backend::{ScriptedStBackend, StBackendCall};
use common::st_bridge_engine::{ScriptedStBridgeEngine, StBridgeEngineCall};

fn actor() -> Actor {
    Actor {
        account: Account {
            id: "account-synthetic".into(),
            username: "synthetic".into(),
            display_name: "Synthetic".into(),
            is_system_admin: true,
            disabled_at: None,
            legacy_st_handle: None,
        },
        workspace_id: Some("workspace-synthetic".into()),
        workspace_role: Some(WorkspaceRole::Owner),
    }
}

fn locator() -> StChatLocator {
    StChatLocator {
        handle: "synthetic-handle".into(),
        avatar: "Synthetic.png".into(),
        character_name: "Synthetic Character".into(),
        chat_file: "synthetic.jsonl".into(),
    }
}

fn origin(update_id: i64) -> BridgeOperationOrigin {
    BridgeOperationOrigin {
        internal_bot_id: "contract-bot".into(),
        telegram_update_id: update_id,
        channel_context_key: "tg:9".into(),
    }
}

async fn configured_write_engine(
    app: &common::TestApp,
    backend: Arc<ScriptedStBackend>,
) -> SidecarBridgeEngine {
    let workspace_id = app.actor.workspace_id.as_deref().unwrap();
    sqlx::query(
        "INSERT INTO telegram_bots (id, workspace_id, owner_account_id, desired_enabled, created_at, updated_at)
         VALUES (?, ?, ?, 0, '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .bind("contract-bot")
    .bind(workspace_id)
    .bind(&app.actor.account.id)
    .execute(&app.pool)
    .await
    .unwrap();
    let channels = ChannelContextStore::new(app.pool.clone());
    channels
        .select_chat(&app.actor.account.id, workspace_id, "tg:9", &test_locator())
        .await
        .unwrap();
    let store = Arc::new(OperationStore::new(app.pool.clone()));
    let vault = Arc::new(EncryptedSqliteVault::new(app.pool.clone(), [7_u8; 32]));
    let provider = Arc::new(OperationPayloadKeyProvider::new(vault, store.clone()));
    let coordinator = Arc::new(OperationCoordinator::new_with_key_provider(
        backend.clone(),
        store,
        provider,
        Arc::new(MemoryStOperationJournal::new()),
    ));
    let registry = Arc::new(SharedMemoryPollerRegistry::new());
    registry.bootstrap_legacy(9002).await.unwrap();
    let ownership = registry
        .transfer(9002, PollerOwner::LegacyPlugin, 1, PollerOwner::RustBridge)
        .await
        .unwrap();
    let binding =
        PollerRuntimeBinding::new("contract-bot", "contract-runtime", ownership.epoch).running();
    registry.claim_runtime(9002, binding.clone()).await.unwrap();
    let guard = Arc::new(PollerOwnershipGuard::new_with_binding(
        9002,
        PollerOwner::RustBridge,
        binding,
        registry,
    ));
    SidecarBridgeEngine::new(backend.clone(), channels)
        .with_required_coordinator(coordinator)
        .with_ownership_guard(guard)
        .with_context_key("tg:9")
}

#[tokio::test]
async fn trait_object_can_be_constructed() {
    let engine: Arc<dyn StBridgeEngine> = Arc::new(ScriptedStBridgeEngine::new());
    let _ = engine;
}

#[tokio::test]
async fn send_message_is_recorded_without_native_table_side_effects() {
    let engine = ScriptedStBridgeEngine::new();
    engine.push_execute(Ok(StBridgeOutcome {
        operation_id: Some("operation-synthetic".into()),
        reply_text: Some("synthetic reply".into()),
        write_committed: false,
        confirmed_commit: false,
        removed_safe_content: None,
        st_tail_fingerprint: None,
    }));
    let command = StBridgeCommand::SendMessage {
        locator: locator(),
        text: "hello".into(),
        client_operation_id: "client-op-synthetic".into(),
        model_override: None,
    };
    let outcome = engine.execute(&actor(), command.clone()).await.unwrap();
    assert_eq!(outcome.reply_text.as_deref(), Some("synthetic reply"));
    assert!(!outcome.write_committed);
    assert_eq!(
        engine.calls(),
        vec![StBridgeEngineCall::Execute {
            command: Box::new(command),
        }]
    );
}

#[tokio::test]
async fn list_characters_uses_query_not_execute() {
    let engine = ScriptedStBridgeEngine::new();
    engine.push_query(Ok(StBridgeView::Characters(Vec::new())));
    let view = engine
        .query(&actor(), StBridgeQuery::ListCharacters)
        .await
        .unwrap();
    assert!(matches!(view, StBridgeView::Characters(_)));
    assert_eq!(
        engine.calls(),
        vec![StBridgeEngineCall::Query {
            query: StBridgeQuery::ListCharacters
        }]
    );
}

#[tokio::test]
async fn execute_error_path_returns_write_not_ready() {
    let engine = ScriptedStBridgeEngine::new();
    engine.push_execute(Err(StBridgeError::boxed(
        StErrorCode::StWriteNotReady,
        StErrorStage::Control,
        im_bridge::st_readiness::ST_WRITE_NOT_READY_MESSAGE,
        false,
        CommitState::NotStarted,
    )));
    let error = engine
        .execute(
            &actor(),
            StBridgeCommand::StartChat {
                locator: locator(),
                client_operation_id: "client-op-synthetic".into(),
            },
        )
        .await
        .expect_err("write must stay frozen");
    assert_eq!(error.code, StErrorCode::StWriteNotReady);
}

fn character_locator() -> StChatLocator {
    StChatLocator {
        handle: "default-user".into(),
        avatar: "Synthetic.png".into(),
        character_name: "TestChar".into(),
        chat_file: String::new(),
    }
}

fn status(mode: StWriteMode) -> StStatus {
    StStatus {
        available: true,
        version: Some("1.16.0-synthetic".into()),
        handle: "default-user".into(),
        capabilities: StCapabilities {
            mode,
            snapshot: true,
            typed_mutations: true,
            integrity_rotation: true,
            operation_replay: true,
        },
    }
}

#[tokio::test]
async fn start_chat_stays_frozen_when_backend_is_read_only() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.push_probe(Ok(status(StWriteMode::ReadOnly)));
    let channels = ChannelContextStore::new(app.pool.clone());
    let engine = SidecarBridgeEngine::new(backend.clone(), channels).with_context_key("tg:9");
    let error = engine
        .execute_with_origin(
            &app.actor,
            StBridgeCommand::StartChat {
                locator: character_locator(),
                client_operation_id: "client-op-new".into(),
            },
            origin(1),
        )
        .await
        .expect_err("read_only must not create chats");
    assert_eq!(error.code, StErrorCode::StWriteNotReady);
    assert_eq!(backend.calls(), vec![StBackendCall::Probe]);
}

#[tokio::test]
async fn start_chat_creates_test_chat_and_selects_it() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.push_probe(Ok(status(StWriteMode::TestWrite)));
    backend.push_create(Ok(StCommitResult {
        status: StCommitStatus::Applied,
        new_sha256: Some("a".repeat(64)),
        new_integrity: Some("integrity-synthetic".into()),
        byte_length: Some(128),
        message_count: Some(2),
    }));
    let engine = configured_write_engine(&app, backend.clone()).await;
    let outcome = engine
        .execute_with_origin(
            &app.actor,
            StBridgeCommand::StartChat {
                locator: character_locator(),
                client_operation_id: "client-op-new".into(),
            },
            origin(1),
        )
        .await
        .expect("test_write may create a dedicated chat");
    assert!(outcome.write_committed);
    assert_eq!(outcome.operation_id.as_deref(), Some("client-op-new"));
    let reply = outcome.reply_text.expect("created chat reply");
    assert!(reply.contains("IMBridge-Test-"));
    let context = ChannelContextStore::new(app.pool.clone())
        .load(&app.actor.account.id, "tg:9")
        .await
        .unwrap();
    let chat_file = context.chat_file.expect("selected chat file");
    assert!(chat_file.starts_with("IMBridge-Test-"));
    assert!(chat_file.ends_with(".jsonl"));
    let native_counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM conversations),
            (SELECT COUNT(*) FROM messages),
            (SELECT COUNT(*) FROM generation_runs)",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(native_counts, (0, 0, 0));
    match backend.calls().as_slice() {
        [StBackendCall::Probe, StBackendCall::CreateChat { command }] => {
            assert_eq!(command.operation_id, "client-op-new");
            assert_eq!(command.locator.avatar, "Synthetic.png");
            assert_eq!(command.locator.chat_file, chat_file);
            assert!(command.locator.chat_file.starts_with("IMBridge-Test-"));
            assert_eq!(command.scope, im_bridge::domain::st::StWriteScope::TestChat);
            assert_eq!(
                command
                    .opening_message
                    .get("is_user")
                    .and_then(|value| value.as_bool()),
                Some(false)
            );
            assert!(command
                .opening_message
                .get("mes")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .contains("TestChar"));
        }
        other => panic!("unexpected backend calls: {other:?}"),
    }
}

fn test_locator() -> StChatLocator {
    StChatLocator {
        handle: "default-user".into(),
        avatar: "Synthetic.png".into(),
        character_name: "TestChar".into(),
        chat_file: "IMBridge-Test-TestChar-1.jsonl".into(),
    }
}

fn long_snapshot() -> StChatSnapshot {
    let mut parsed_chat = vec![serde_json::json!({"chat_metadata": {"integrity": "int-1"}})];
    for index in 0..20 {
        parsed_chat.push(serde_json::json!({
            "is_user": true,
            "name": "User",
            "mes": format!("user-{index}")
        }));
        parsed_chat.push(serde_json::json!({
            "is_user": false,
            "name": "TestChar",
            "mes": format!("assistant-{index}")
        }));
    }
    StChatSnapshot {
        locator: test_locator(),
        parsed_chat,
        source_sha256: "sha-source".into(),
        source_integrity: "int-1".into(),
        source_byte_length: 1024,
        source_message_count: 41,
    }
}

fn dialogue_snapshot() -> StChatSnapshot {
    StChatSnapshot {
        locator: test_locator(),
        parsed_chat: vec![
            serde_json::json!({"chat_metadata": {"integrity": "int-1"}}),
            serde_json::json!({"is_user": true, "name": "User", "mes": "hello"}),
            serde_json::json!({"is_user": false, "name": "TestChar", "mes": "hi"}),
        ],
        source_sha256: "sha-source".into(),
        source_integrity: "int-1".into(),
        source_byte_length: 64,
        source_message_count: 3,
    }
}

fn settings() -> StGenerationSettings {
    StGenerationSettings {
        username: "User".into(),
        chat_completion_source: "custom".into(),
        model: "synthetic-model".into(),
        custom_url: "https://synthetic.invalid/provider".into(),
        custom_prompt_post_processing: "merge_tools".into(),
        temperature: 0.7,
        top_p: 0.9,
        max_tokens: 256,
    }
}

fn character() -> im_bridge::domain::st::StCharacterSummary {
    im_bridge::domain::st::StCharacterSummary {
        avatar: "Synthetic.png".into(),
        name: "TestChar".into(),
        description: "A fixture-only character description.".into(),
        ..Default::default()
    }
}

fn applied() -> StCommitResult {
    StCommitResult {
        status: StCommitStatus::Applied,
        new_sha256: Some("b".repeat(64)),
        new_integrity: Some("int-2".into()),
        byte_length: Some(80),
        message_count: Some(3),
    }
}

#[tokio::test]
async fn send_undo_redo_and_compress_stay_frozen_in_read_only() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    for _ in 0..5 {
        backend.push_probe(Ok(status(StWriteMode::ReadOnly)));
    }
    let engine =
        SidecarBridgeEngine::new(backend.clone(), ChannelContextStore::new(app.pool.clone()))
            .with_context_key("tg:9");
    for command in [
        StBridgeCommand::SendMessage {
            locator: test_locator(),
            text: "hello".into(),
            client_operation_id: "op-send".into(),
            model_override: None,
        },
        StBridgeCommand::UndoLastTurn {
            locator: test_locator(),
            client_operation_id: "op-undo".into(),
        },
        StBridgeCommand::RevokeLastTurn {
            locator: test_locator(),
            client_operation_id: "op-revoke".into(),
        },
        StBridgeCommand::RegenerateReply {
            locator: test_locator(),
            client_operation_id: "op-redo".into(),
            model_override: None,
        },
        StBridgeCommand::CompressChat {
            locator: test_locator(),
            client_operation_id: "op-compress".into(),
        },
    ] {
        let error = engine
            .execute_with_origin(&app.actor, command, origin(1))
            .await
            .expect_err("read_only writes stay frozen");
        assert_eq!(error.code, StErrorCode::StWriteNotReady);
    }
    assert_eq!(backend.calls(), vec![StBackendCall::Probe; 5]);
}

#[tokio::test]
async fn send_commits_append_turn_without_native_tables() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.push_probe(Ok(status(StWriteMode::TestWrite)));
    backend.push_snapshot(Ok(dialogue_snapshot()));
    backend.push_characters(Ok(vec![character()]));
    backend.push_settings(Ok(settings()));
    backend.push_generation(Ok(StGenerationResult {
        text: "synthetic reply".into(),
        finish_reason: Some("stop".into()),
        usage: None,
    }));
    backend.push_commit(Ok(applied()));
    let engine = configured_write_engine(&app, backend.clone()).await;
    let outcome = engine
        .execute_with_origin(
            &app.actor,
            StBridgeCommand::SendMessage {
                locator: test_locator(),
                text: "hello".into(),
                client_operation_id: "op-send".into(),
                model_override: None,
            },
            origin(1),
        )
        .await
        .unwrap();
    assert!(outcome.write_committed);
    assert_eq!(outcome.reply_text.as_deref(), Some("synthetic reply"));
    match backend.calls().last() {
        Some(StBackendCall::Commit { command }) => {
            assert_eq!(command.operation_id, "op-send");
            assert_eq!(command.expected_sha256, "sha-source");
            assert_eq!(command.expected_integrity, "int-1");
            assert!(matches!(command.mutation, StMutation::AppendTurn { .. }));
        }
        other => panic!("expected commit, got {other:?}"),
    }
    let native_counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM conversations),
            (SELECT COUNT(*) FROM messages),
            (SELECT COUNT(*) FROM generation_runs)",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(native_counts, (0, 0, 0));
}

#[tokio::test]
async fn generation_rejection_settles_bridge_operation_as_failed() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.push_probe(Ok(status(StWriteMode::TestWrite)));
    backend.push_snapshot(Ok(dialogue_snapshot()));
    backend.push_characters(Ok(vec![character()]));
    backend.push_settings(Ok(settings()));
    backend.push_generation(Err(StBridgeError::boxed(
        StErrorCode::StGenerateRejected,
        StErrorStage::Generation,
        "synthetic generation rejection",
        false,
        CommitState::NotStarted,
    )));
    let engine = configured_write_engine(&app, backend).await;

    let error = engine
        .execute_with_origin(
            &app.actor,
            StBridgeCommand::SendMessage {
                locator: test_locator(),
                text: "hello".into(),
                client_operation_id: "op-send-rejected".into(),
                model_override: None,
            },
            origin(4),
        )
        .await
        .expect_err("generation rejection must be returned");

    assert_eq!(error.code, StErrorCode::StGenerateRejected);
    let row: (String, String, String, String, i64) = sqlx::query_as(
        "SELECT status, commit_state, error_stage, error_code, retryable
         FROM bridge_operations WHERE id = ?",
    )
    .bind("op-send-rejected")
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(
        row,
        (
            "failed".into(),
            "not_started".into(),
            "generation".into(),
            "ST_GENERATE_REJECTED".into(),
            0,
        )
    );
}

#[tokio::test]
async fn undo_redo_and_compress_use_typed_mutations() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.push_probe(Ok(status(StWriteMode::TestWrite)));
    backend.push_snapshot(Ok(dialogue_snapshot()));
    backend.push_commit(Ok(applied()));
    backend.push_probe(Ok(status(StWriteMode::TestWrite)));
    backend.push_snapshot(Ok(dialogue_snapshot()));
    backend.push_characters(Ok(vec![character()]));
    backend.push_settings(Ok(settings()));
    backend.push_generation(Ok(StGenerationResult {
        text: "regenerated".into(),
        finish_reason: Some("stop".into()),
        usage: None,
    }));
    backend.push_commit(Ok(applied()));
    backend.push_probe(Ok(status(StWriteMode::TestWrite)));
    backend.push_snapshot(Ok(long_snapshot()));
    backend.push_settings(Ok(settings()));
    for _ in 0..12 {
        backend.push_generation(Ok(StGenerationResult {
            text: "compressed".into(),
            finish_reason: Some("stop".into()),
            usage: None,
        }));
    }
    backend.push_commit(Ok(applied()));
    let engine = configured_write_engine(&app, backend.clone()).await;
    engine
        .execute_with_origin(
            &app.actor,
            StBridgeCommand::UndoLastTurn {
                locator: test_locator(),
                client_operation_id: "op-undo".into(),
            },
            origin(1),
        )
        .await
        .unwrap();
    engine
        .execute_with_origin(
            &app.actor,
            StBridgeCommand::RegenerateReply {
                locator: test_locator(),
                client_operation_id: "op-redo".into(),
                model_override: None,
            },
            origin(2),
        )
        .await
        .unwrap();
    engine
        .execute_with_origin(
            &app.actor,
            StBridgeCommand::CompressChat {
                locator: test_locator(),
                client_operation_id: "op-compress".into(),
            },
            origin(3),
        )
        .await
        .unwrap();
    let commits: Vec<_> = backend
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            StBackendCall::Commit { command } => Some(command.mutation),
            _ => None,
        })
        .collect();
    assert!(matches!(commits[0], StMutation::UndoLastTurn { .. }));
    assert!(matches!(
        commits[1],
        StMutation::ReplaceLastAssistant { .. }
    ));
    assert!(matches!(commits[2], StMutation::CompressMessages { .. }));
}

#[tokio::test]
async fn undo_and_revoke_share_typed_mutation_but_differ_in_outcome_policy() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.push_probe(Ok(status(StWriteMode::TestWrite)));
    backend.push_snapshot(Ok(dialogue_snapshot()));
    backend.push_commit(Ok(applied()));
    backend.push_probe(Ok(status(StWriteMode::TestWrite)));
    backend.push_snapshot(Ok(dialogue_snapshot()));
    backend.push_commit(Ok(applied()));
    let engine = configured_write_engine(&app, backend.clone()).await;
    let undo = engine
        .execute_with_origin(
            &app.actor,
            StBridgeCommand::UndoLastTurn {
                locator: test_locator(),
                client_operation_id: "op-undo-policy".into(),
            },
            origin(1),
        )
        .await
        .unwrap();
    let revoke = engine
        .execute_with_origin(
            &app.actor,
            StBridgeCommand::RevokeLastTurn {
                locator: test_locator(),
                client_operation_id: "op-revoke-policy".into(),
            },
            origin(2),
        )
        .await
        .unwrap();
    assert!(undo.write_committed);
    assert!(undo.confirmed_commit);
    assert!(undo
        .reply_text
        .as_deref()
        .unwrap_or_default()
        .contains("Telegram 消息保持保留"));
    assert!(undo
        .removed_safe_content
        .as_deref()
        .unwrap_or_default()
        .contains("hello"));
    assert!(undo.st_tail_fingerprint.is_some());
    assert!(revoke.write_committed);
    assert!(revoke.confirmed_commit);
    assert!(revoke
        .reply_text
        .as_deref()
        .unwrap_or_default()
        .contains("SillyTavern 已回退"));
    assert!(!revoke
        .reply_text
        .as_deref()
        .unwrap_or_default()
        .contains("Telegram 消息保持保留"));
    let mutations: Vec<_> = backend
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            StBackendCall::Commit { command } => Some(command.mutation),
            _ => None,
        })
        .collect();
    assert_eq!(mutations.len(), 2);
    assert!(matches!(mutations[0], StMutation::UndoLastTurn { .. }));
    assert!(matches!(mutations[1], StMutation::UndoLastTurn { .. }));
}

#[tokio::test]
async fn undo_and_revoke_commands_are_distinct_enum_variants() {
    let undo = StBridgeCommand::UndoLastTurn {
        locator: locator(),
        client_operation_id: "op-undo".into(),
    };
    let revoke = StBridgeCommand::RevokeLastTurn {
        locator: locator(),
        client_operation_id: "op-revoke".into(),
    };
    assert_ne!(undo, revoke);
}
