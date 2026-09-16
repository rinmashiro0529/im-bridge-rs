use std::sync::Arc;

use im_bridge::domain::st::{StCapabilities, StChatLocator, StStatus, StWriteMode};
use im_bridge::modules::bridge::channel_context::ChannelContextStore;
use im_bridge::modules::bridge::engine::SidecarBridgeEngine;
use im_bridge::modules::bridge::errors::{CommitState, StBridgeError, StErrorCode, StErrorStage};
use im_bridge::seams::st_bridge_engine::{StBridgeCommand, StBridgeEngine};

mod common;
use common::st_backend::ScriptedStBackend;

fn locator() -> StChatLocator {
    StChatLocator {
        handle: "default-user".into(),
        avatar: "Synthetic.png".into(),
        character_name: "TestChar".into(),
        chat_file: "IMBridge-Test-TestChar-1.jsonl".into(),
    }
}

fn connect_failed() -> Box<StBridgeError> {
    StBridgeError::boxed(
        StErrorCode::StConnectFailed,
        StErrorStage::Connect,
        "SillyTavern 连接失败",
        true,
        CommitState::NotStarted,
    )
}

fn write_status() -> StStatus {
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

async fn native_counts(pool: &sqlx::SqlitePool) -> (i64, i64, i64) {
    sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM conversations),
            (SELECT COUNT(*) FROM messages),
            (SELECT COUNT(*) FROM generation_runs)",
    )
    .fetch_one(pool)
    .await
    .expect("native table counts")
}

async fn install_forbid_triggers(pool: &sqlx::SqlitePool) {
    sqlx::query(
        "CREATE TRIGGER forbid_native_conversation_insert BEFORE INSERT ON conversations
         BEGIN SELECT RAISE(ABORT, 'FORBIDDEN_NATIVE_CONVERSATION_INSERT'); END;",
    )
    .execute(pool)
    .await
    .expect("conversation trigger");
    sqlx::query(
        "CREATE TRIGGER forbid_native_message_insert BEFORE INSERT ON messages
         BEGIN SELECT RAISE(ABORT, 'FORBIDDEN_NATIVE_MESSAGE_INSERT'); END;",
    )
    .execute(pool)
    .await
    .expect("message trigger");
    sqlx::query(
        "CREATE TRIGGER forbid_native_generation_insert BEFORE INSERT ON generation_runs
         BEGIN SELECT RAISE(ABORT, 'FORBIDDEN_NATIVE_GENERATION_INSERT'); END;",
    )
    .execute(pool)
    .await
    .expect("generation trigger");
}

fn script_failing_backend(backend: &ScriptedStBackend) {
    backend.push_probe(Ok(write_status()));
    backend.push_snapshot(Err(connect_failed()));
    backend.push_characters(Err(connect_failed()));
    backend.push_settings(Err(connect_failed()));
    backend.push_generation(Err(connect_failed()));
    backend.push_create(Err(connect_failed()));
    backend.push_commit(Err(connect_failed()));
}

#[tokio::test]
async fn st_failures_never_write_native_conversation_tables() {
    let app = common::setup().await;
    install_forbid_triggers(&app.pool).await;
    let backend = Arc::new(ScriptedStBackend::new());
    for _ in 0..8 {
        script_failing_backend(&backend);
    }
    let channels = ChannelContextStore::new(app.pool.clone());
    let engine = SidecarBridgeEngine::new(backend.clone(), channels).with_context_key("tg:fail");
    let commands = [
        StBridgeCommand::SendMessage {
            locator: locator(),
            text: "hello".into(),
            client_operation_id: "op-send".into(),
            model_override: None,
        },
        StBridgeCommand::StartChat {
            locator: locator(),
            client_operation_id: "op-start".into(),
        },
        StBridgeCommand::UndoLastTurn {
            locator: locator(),
            client_operation_id: "op-undo".into(),
        },
        StBridgeCommand::RevokeLastTurn {
            locator: locator(),
            client_operation_id: "op-revoke".into(),
        },
        StBridgeCommand::RegenerateReply {
            locator: locator(),
            client_operation_id: "op-redo".into(),
            model_override: None,
        },
        StBridgeCommand::CompressChat {
            locator: locator(),
            client_operation_id: "op-compress".into(),
        },
    ];
    for command in commands {
        let error = engine
            .execute(&app.actor, command)
            .await
            .expect_err("ST failure must surface as StBridgeError");
        assert!(
            error.code == StErrorCode::StConnectFailed
                || error.code == StErrorCode::StWriteNotReady
                || error.code == StErrorCode::StChatLocatorRejected
                || error.code == StErrorCode::StCommitFailed
                || error.code == StErrorCode::StCommitValidationFailed,
            "unexpected fallback-shaped code {:?}",
            error.code
        );
        assert_eq!(native_counts(&app.pool).await, (0, 0, 0));
    }
    assert_eq!(native_counts(&app.pool).await, (0, 0, 0));
}
