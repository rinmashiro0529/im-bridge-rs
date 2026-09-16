use std::sync::Arc;
use std::time::Duration;

use im_bridge::adapters::secrets::encrypted_sqlite::EncryptedSqliteVault;
use im_bridge::domain::st::{
    StCapabilities, StCharacterSummary, StChatLocator, StChatSnapshot, StCommitResult,
    StCommitStatus, StGenerationResult, StGenerationSettings, StStatus, StWriteMode,
};
use im_bridge::modules::bridge::channel_context::ChannelContextStore;
use im_bridge::modules::bridge::engine::SidecarBridgeEngine;
use im_bridge::modules::bridge::operation_coordinator::OperationCoordinator;
use im_bridge::modules::bridge::operation_payload::OperationPayloadKeyProvider;
use im_bridge::modules::bridge::operation_store::OperationStore;
use im_bridge::modules::bridge::poller_ownership::{
    PollerOwner, PollerOwnershipGuard, PollerRuntimeBinding, SharedMemoryPollerRegistry,
};
use im_bridge::seams::st_bridge_engine::{BridgeOperationOrigin, StBridgeCommand, StBridgeEngine};
use im_bridge::seams::st_operation_journal::MemoryStOperationJournal;

mod common;
use common::st_backend::ScriptedStBackend;

fn origin(context_key: &str, update_id: i64) -> BridgeOperationOrigin {
    BridgeOperationOrigin {
        internal_bot_id: "locator-bot".into(),
        telegram_update_id: update_id,
        channel_context_key: context_key.to_string(),
    }
}

async fn configured_write_engine(
    app: &common::TestApp,
    backend: Arc<ScriptedStBackend>,
    context_key: &str,
) -> SidecarBridgeEngine {
    let workspace_id = app.actor.workspace_id.as_deref().unwrap();
    sqlx::query(
        "INSERT INTO telegram_bots (id, workspace_id, owner_account_id, desired_enabled, created_at, updated_at)
         VALUES (?, ?, ?, 0, '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .bind("locator-bot")
    .bind(workspace_id)
    .bind(&app.actor.account.id)
    .execute(&app.pool)
    .await
    .unwrap();
    let channels = ChannelContextStore::new(app.pool.clone());
    let store = Arc::new(OperationStore::new(app.pool.clone()));
    let vault = Arc::new(EncryptedSqliteVault::new(app.pool.clone(), [10_u8; 32]));
    let provider = Arc::new(OperationPayloadKeyProvider::new(vault, store.clone()));
    let coordinator = Arc::new(OperationCoordinator::new_with_key_provider(
        backend.clone(),
        store,
        provider,
        Arc::new(MemoryStOperationJournal::new()),
    ));
    let registry = Arc::new(SharedMemoryPollerRegistry::new());
    registry.bootstrap_legacy(9004).await.unwrap();
    let ownership = registry
        .transfer(9004, PollerOwner::LegacyPlugin, 1, PollerOwner::RustBridge)
        .await
        .unwrap();
    let binding =
        PollerRuntimeBinding::new("locator-bot", "locator-runtime", ownership.epoch).running();
    registry.claim_runtime(9004, binding.clone()).await.unwrap();
    let guard = Arc::new(PollerOwnershipGuard::new_with_binding(
        9004,
        PollerOwner::RustBridge,
        binding,
        registry,
    ));
    SidecarBridgeEngine::new(backend.clone(), channels)
        .with_required_coordinator(coordinator)
        .with_ownership_guard(guard)
        .with_context_key(context_key)
}

fn locator(chat_file: &str) -> StChatLocator {
    StChatLocator {
        handle: "default-user".into(),
        avatar: "Synthetic.png".into(),
        character_name: "TestChar".into(),
        chat_file: chat_file.to_string(),
    }
}

fn status() -> StStatus {
    StStatus {
        available: true,
        version: Some("1.16.0-synthetic".into()),
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

fn snapshot_for(chat_file: &str) -> StChatSnapshot {
    StChatSnapshot {
        locator: locator(chat_file),
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

fn character() -> StCharacterSummary {
    StCharacterSummary {
        avatar: "Synthetic.png".into(),
        name: "TestChar".into(),
        ..Default::default()
    }
}

fn applied() -> StCommitResult {
    StCommitResult {
        status: StCommitStatus::Applied,
        new_sha256: Some("a".repeat(64)),
        new_integrity: Some("integrity-synthetic".into()),
        byte_length: Some(128),
        message_count: Some(4),
    }
}

fn generated(text: &str) -> StGenerationResult {
    StGenerationResult {
        text: text.to_string(),
        finish_reason: Some("stop".into()),
        usage: None,
    }
}

fn send_command(chat_file: &str, op: &str) -> StBridgeCommand {
    StBridgeCommand::SendMessage {
        locator: locator(chat_file),
        text: "hello".into(),
        client_operation_id: op.to_string(),
        model_override: None,
    }
}

fn script_send(backend: &ScriptedStBackend, chat_file: &str, reply: &str) {
    backend.push_probe(Ok(status()));
    backend.push_snapshot(Ok(snapshot_for(chat_file)));
    backend.push_characters(Ok(vec![character()]));
    backend.push_settings(Ok(settings()));
    backend.push_generation(Ok(generated(reply)));
    backend.push_commit(Ok(applied()));
}

#[tokio::test]
async fn same_locator_different_contexts_are_strictly_serialized() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.set_snapshot_delay(Duration::from_millis(80));
    script_send(&backend, "shared.jsonl", "one");
    script_send(&backend, "shared.jsonl", "two");
    let base = configured_write_engine(&app, backend.clone(), "tg:left").await;
    let left = base.with_context_key("tg:left");
    let right = base.with_context_key("tg:right");
    let workspace_id = app.actor.workspace_id.as_deref().unwrap();
    let channels = ChannelContextStore::new(app.pool.clone());
    channels
        .select_chat(
            &app.actor.account.id,
            workspace_id,
            "tg:left",
            &locator("shared.jsonl"),
        )
        .await
        .unwrap();
    channels
        .select_chat(
            &app.actor.account.id,
            workspace_id,
            "tg:right",
            &locator("shared.jsonl"),
        )
        .await
        .unwrap();
    let actor = app.actor.clone();
    let first = tokio::spawn({
        let left = left.clone();
        let actor = actor.clone();
        async move {
            left.execute_with_origin(
                &actor,
                send_command("shared.jsonl", "op-left"),
                origin("tg:left", 1),
            )
            .await
            .expect("left send")
        }
    });
    let second = tokio::spawn({
        let right = right.clone();
        let actor = actor.clone();
        async move {
            right
                .execute_with_origin(
                    &actor,
                    send_command("shared.jsonl", "op-right"),
                    origin("tg:right", 2),
                )
                .await
                .expect("right send")
        }
    });
    first.await.expect("join left");
    second.await.expect("join right");
    assert_eq!(backend.snapshot_peak(), 1);
}

#[tokio::test]
async fn different_locators_execute_concurrently() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.set_snapshot_delay(Duration::from_millis(80));
    script_send(&backend, "chat-a.jsonl", "a");
    script_send(&backend, "chat-b.jsonl", "b");
    let base = configured_write_engine(&app, backend.clone(), "tg:mix:a").await;
    let engine_a = base.with_context_key("tg:mix:a");
    let engine_b = base.with_context_key("tg:mix:b");
    let workspace_id = app.actor.workspace_id.as_deref().unwrap();
    let channels = ChannelContextStore::new(app.pool.clone());
    channels
        .select_chat(
            &app.actor.account.id,
            workspace_id,
            "tg:mix:a",
            &locator("chat-a.jsonl"),
        )
        .await
        .unwrap();
    channels
        .select_chat(
            &app.actor.account.id,
            workspace_id,
            "tg:mix:b",
            &locator("chat-b.jsonl"),
        )
        .await
        .unwrap();
    let actor = app.actor.clone();
    let first = tokio::spawn({
        let engine = engine_a;
        let actor = actor.clone();
        async move {
            engine
                .execute_with_origin(
                    &actor,
                    send_command("chat-a.jsonl", "op-a"),
                    origin("tg:mix:a", 3),
                )
                .await
                .expect("locator a")
        }
    });
    let second = tokio::spawn({
        let engine = engine_b;
        let actor = actor.clone();
        async move {
            engine
                .execute_with_origin(
                    &actor,
                    send_command("chat-b.jsonl", "op-b"),
                    origin("tg:mix:b", 4),
                )
                .await
                .expect("locator b")
        }
    });
    first.await.expect("join a");
    second.await.expect("join b");
    assert!(
        backend.snapshot_peak() >= 2,
        "distinct locators must overlap in snapshot"
    );
}

#[tokio::test]
async fn generation_budget_limits_max_concurrent_calls() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.set_generation_delay(Duration::from_millis(80));
    for index in 0..4 {
        script_send(
            &backend,
            &format!("chat-{index}.jsonl"),
            &format!("reply-{index}"),
        );
    }
    let base = configured_write_engine(&app, backend.clone(), "tg:budget")
        .await
        .with_generation_budget(2);
    let workspace_id = app.actor.workspace_id.as_deref().unwrap();
    let channels = ChannelContextStore::new(app.pool.clone());
    for index in 0..4 {
        channels
            .select_chat(
                &app.actor.account.id,
                workspace_id,
                &format!("tg:budget:{index}"),
                &locator(&format!("chat-{index}.jsonl")),
            )
            .await
            .unwrap();
    }
    let actor = app.actor.clone();
    let mut handles = Vec::new();
    for index in 0..4 {
        let engine = base.with_context_key(&format!("tg:budget:{index}"));
        let actor = actor.clone();
        handles.push(tokio::spawn(async move {
            engine
                .execute_with_origin(
                    &actor,
                    send_command(&format!("chat-{index}.jsonl"), &format!("op-{index}")),
                    origin(&format!("tg:budget:{index}"), 10 + index as i64),
                )
                .await
                .expect("budgeted send")
        }));
    }
    for handle in handles {
        handle.await.expect("join budgeted send");
    }
    assert!(
        backend.generation_peak() <= 2,
        "generation peak {} exceeded budget 2",
        backend.generation_peak()
    );
}
