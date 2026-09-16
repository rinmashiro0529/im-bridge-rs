use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use im_bridge::adapters::secrets::encrypted_sqlite::EncryptedSqliteVault;
use im_bridge::domain::st::{
    StCapabilities, StCharacterSummary, StChatLocator, StCommitResult, StCommitStatus,
    StGenerationResult, StGenerationSettings, StStatus, StWriteMode,
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
use im_bridge::seams::bridge_progress::{
    BridgeExecutionContext, BridgeProgressEvent, BridgeProgressSink, ConfirmedCommit,
};
use im_bridge::seams::st_bridge_engine::{BridgeOperationOrigin, StBridgeCommand, StBridgeEngine};
use im_bridge::seams::st_operation_journal::MemoryStOperationJournal;

mod common;
use common::st_backend::ScriptedStBackend;

#[derive(Default)]
struct RecordingProgressSink {
    events: Mutex<Vec<BridgeProgressEvent>>,
    delta_count: AtomicUsize,
}

#[async_trait::async_trait]
impl BridgeProgressSink for RecordingProgressSink {
    async fn emit(
        &self,
        event: BridgeProgressEvent,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let BridgeProgressEvent::Delta { .. } = &event {
            self.delta_count.fetch_add(1, Ordering::SeqCst);
        }
        self.events.lock().await.push(event);
        Ok(())
    }
}

fn test_status() -> StStatus {
    StStatus {
        available: true,
        version: Some("1.16.0-stream".into()),
        handle: "default-user".into(),
        capabilities: StCapabilities {
            mode: StWriteMode::ProductionWrite,
            snapshot: true,
            typed_mutations: true,
            integrity_rotation: true,
            operation_replay: true,
        },
    }
}

fn test_locator() -> StChatLocator {
    StChatLocator {
        handle: "default-user".into(),
        avatar: "StreamChar.png".into(),
        character_name: "StreamChar".into(),
        chat_file: "StreamChar-chat.jsonl".into(),
    }
}

#[tokio::test]
async fn execute_with_context_emits_done_only_after_commit() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.push_probe(Ok(test_status()));
    backend.push_snapshot(Ok(im_bridge::domain::st::StChatSnapshot {
        locator: test_locator(),
        parsed_chat: vec![serde_json::json!({"chat_metadata": {"integrity": "int-1"}})],
        source_sha256: "sha-1".into(),
        source_integrity: "int-1".into(),
        source_byte_length: 100,
        source_message_count: 1,
    }));
    backend.push_characters(Ok(vec![StCharacterSummary {
        avatar: "StreamChar.png".into(),
        name: "StreamChar".into(),
        ..StCharacterSummary::default()
    }]));
    backend.push_settings(Ok(StGenerationSettings {
        username: "User".into(),
        chat_completion_source: "custom".into(),
        model: "stream-model".into(),
        custom_url: "".into(),
        custom_prompt_post_processing: "".into(),
        temperature: 0.7,
        top_p: 1.0,
        max_tokens: 100,
    }));
    backend.push_generation(Ok(StGenerationResult {
        text: "Streamed answer to user.".into(),
        finish_reason: Some("stop".into()),
        usage: None,
    }));
    backend.push_commit(Ok(StCommitResult {
        status: StCommitStatus::Applied,
        new_sha256: Some("sha-2".into()),
        new_integrity: Some("int-2".into()),
        byte_length: Some(300),
        message_count: Some(3),
    }));

    let actor = app.actor.clone();
    let workspace_id = actor.workspace_id.as_deref().unwrap();
    sqlx::query(
        "INSERT INTO telegram_bots (id, workspace_id, owner_account_id, desired_enabled, created_at, updated_at)
         VALUES (?, ?, ?, 0, '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .bind("stream-bot")
    .bind(workspace_id)
    .bind(&actor.account.id)
    .execute(&app.pool)
    .await
    .unwrap();
    let channels = ChannelContextStore::new(app.pool.clone());
    channels
        .select_chat(
            &actor.account.id,
            workspace_id,
            "tg:stream:1",
            &test_locator(),
        )
        .await
        .unwrap();
    let store = Arc::new(OperationStore::new(app.pool.clone()));
    let vault = Arc::new(EncryptedSqliteVault::new(app.pool.clone(), [8_u8; 32]));
    let provider = Arc::new(OperationPayloadKeyProvider::new(vault, store.clone()));
    let coordinator = Arc::new(OperationCoordinator::new_with_key_provider(
        backend.clone(),
        store,
        provider,
        Arc::new(MemoryStOperationJournal::new()),
    ));
    let registry = Arc::new(SharedMemoryPollerRegistry::new());
    registry.bootstrap_legacy(9001).await.unwrap();
    let ownership = registry
        .transfer(9001, PollerOwner::LegacyPlugin, 1, PollerOwner::RustBridge)
        .await
        .unwrap();
    let binding =
        PollerRuntimeBinding::new("stream-bot", "stream-runtime", ownership.epoch).running();
    registry.claim_runtime(9001, binding.clone()).await.unwrap();
    let guard = Arc::new(PollerOwnershipGuard::new_with_binding(
        9001,
        PollerOwner::RustBridge,
        binding,
        registry,
    ));
    let engine = SidecarBridgeEngine::new(backend.clone(), channels)
        .with_required_coordinator(coordinator)
        .with_ownership_guard(guard)
        .with_context_key("tg:stream:1");

    let sink = Arc::new(RecordingProgressSink::default());
    let ctx = BridgeExecutionContext::new("op-stream-1", Some(sink.clone()));

    let outcome = engine
        .execute_with_context_and_origin(
            &actor,
            StBridgeCommand::SendMessage {
                locator: test_locator(),
                text: "Tell me a story.".into(),
                client_operation_id: "op-stream-1".into(),
                model_override: None,
            },
            Some(ctx),
            BridgeOperationOrigin {
                internal_bot_id: "stream-bot".into(),
                telegram_update_id: 1,
                channel_context_key: "tg:stream:1".into(),
            },
        )
        .await
        .expect("send message with context must succeed");

    assert!(outcome.write_committed);
    assert_eq!(
        outcome.reply_text.as_deref(),
        Some("Streamed answer to user.")
    );

    let events = sink.events.lock().await;
    assert_eq!(events.len(), 1);
    match &events[0] {
        BridgeProgressEvent::Done { reply_text, commit } => {
            assert_eq!(reply_text, "Streamed answer to user.");
            assert_eq!(*commit, ConfirmedCommit::Applied);
        }
        other => panic!("expected Done event, got: {:?}", other),
    }
}

#[tokio::test]
async fn execute_with_context_does_not_emit_done_on_commit_failure() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.push_probe(Ok(test_status()));
    backend.push_snapshot(Ok(im_bridge::domain::st::StChatSnapshot {
        locator: test_locator(),
        parsed_chat: vec![serde_json::json!({"chat_metadata": {"integrity": "int-1"}})],
        source_sha256: "sha-1".into(),
        source_integrity: "int-1".into(),
        source_byte_length: 100,
        source_message_count: 1,
    }));
    backend.push_characters(Ok(vec![StCharacterSummary {
        avatar: "StreamChar.png".into(),
        name: "StreamChar".into(),
        ..StCharacterSummary::default()
    }]));
    backend.push_settings(Ok(StGenerationSettings {
        username: "User".into(),
        chat_completion_source: "custom".into(),
        model: "stream-model".into(),
        custom_url: "".into(),
        custom_prompt_post_processing: "".into(),
        temperature: 0.7,
        top_p: 1.0,
        max_tokens: 100,
    }));
    backend.push_generation(Ok(StGenerationResult {
        text: "Uncommitted generated text.".into(),
        finish_reason: Some("stop".into()),
        usage: None,
    }));
    // Commit 阶段模拟冲突失败！
    backend.push_commit(Err(StBridgeError::boxed(
        StErrorCode::StChatConflict,
        StErrorStage::Commit,
        "chat conflict during CAS commit",
        false,
        CommitState::NotApplied,
    )));

    let actor = app.actor.clone();
    let workspace_id = actor.workspace_id.as_deref().unwrap();
    sqlx::query(
        "INSERT INTO telegram_bots (id, workspace_id, owner_account_id, desired_enabled, created_at, updated_at)
         VALUES (?, ?, ?, 0, '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .bind("stream-bot")
    .bind(workspace_id)
    .bind(&actor.account.id)
    .execute(&app.pool)
    .await
    .unwrap();
    let channels = ChannelContextStore::new(app.pool.clone());
    channels
        .select_chat(
            &actor.account.id,
            workspace_id,
            "tg:stream:1",
            &test_locator(),
        )
        .await
        .unwrap();
    let store = Arc::new(OperationStore::new(app.pool.clone()));
    let vault = Arc::new(EncryptedSqliteVault::new(app.pool.clone(), [8_u8; 32]));
    let provider = Arc::new(OperationPayloadKeyProvider::new(vault, store.clone()));
    let coordinator = Arc::new(OperationCoordinator::new_with_key_provider(
        backend.clone(),
        store,
        provider,
        Arc::new(MemoryStOperationJournal::new()),
    ));
    let registry = Arc::new(SharedMemoryPollerRegistry::new());
    registry.bootstrap_legacy(9001).await.unwrap();
    let ownership = registry
        .transfer(9001, PollerOwner::LegacyPlugin, 1, PollerOwner::RustBridge)
        .await
        .unwrap();
    let binding =
        PollerRuntimeBinding::new("stream-bot", "stream-runtime", ownership.epoch).running();
    registry.claim_runtime(9001, binding.clone()).await.unwrap();
    let guard = Arc::new(PollerOwnershipGuard::new_with_binding(
        9001,
        PollerOwner::RustBridge,
        binding,
        registry,
    ));
    let engine = SidecarBridgeEngine::new(backend.clone(), channels)
        .with_required_coordinator(coordinator)
        .with_ownership_guard(guard)
        .with_context_key("tg:stream:1");

    let sink = Arc::new(RecordingProgressSink::default());
    let ctx = BridgeExecutionContext::new("op-stream-conflict", Some(sink.clone()));

    let err = engine
        .execute_with_context_and_origin(
            &actor,
            StBridgeCommand::SendMessage {
                locator: test_locator(),
                text: "This will conflict.".into(),
                client_operation_id: "op-stream-conflict".into(),
                model_override: None,
            },
            Some(ctx),
            BridgeOperationOrigin {
                internal_bot_id: "stream-bot".into(),
                telegram_update_id: 2,
                channel_context_key: "tg:stream:1".into(),
            },
        )
        .await
        .expect_err("commit failure must return error");

    assert_eq!(err.code, StErrorCode::StChatConflict);

    // 关键门禁：发生冲突失败时，绝对不能向 sink 误发射 Done 事件！
    let events = sink.events.lock().await;
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, BridgeProgressEvent::Done { .. })),
        "Done event must NOT be emitted if commit failed!"
    );
}
