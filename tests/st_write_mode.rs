use std::sync::Arc;

use im_bridge::adapters::secrets::encrypted_sqlite::EncryptedSqliteVault;
use im_bridge::domain::st::{
    StCapabilities, StCharacterSummary, StChatLocator, StCommitResult, StCommitStatus,
    StGenerationResult, StGenerationSettings, StStatus, StWriteMode, WriteFacts,
};
use im_bridge::modules::bridge::channel_context::ChannelContextStore;
use im_bridge::modules::bridge::engine::SidecarBridgeEngine;
use im_bridge::modules::bridge::errors::StErrorCode;
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

#[test]
fn st_write_mode_ordering_and_predicates() {
    assert!(StWriteMode::Disabled < StWriteMode::ReadOnly);
    assert!(StWriteMode::ReadOnly < StWriteMode::TestWrite);
    assert!(StWriteMode::TestWrite < StWriteMode::ProductionWrite);

    assert!(!StWriteMode::Disabled.allows_read());
    assert!(!StWriteMode::Disabled.allows_write());

    assert!(StWriteMode::ReadOnly.allows_read());
    assert!(!StWriteMode::ReadOnly.allows_write());

    assert!(StWriteMode::TestWrite.allows_read());
    assert!(StWriteMode::TestWrite.allows_write());

    assert!(StWriteMode::ProductionWrite.allows_read());
    assert!(StWriteMode::ProductionWrite.allows_write());
}

#[test]
fn st_write_mode_effective_4x4_matrix() {
    let modes = [
        StWriteMode::Disabled,
        StWriteMode::ReadOnly,
        StWriteMode::TestWrite,
        StWriteMode::ProductionWrite,
    ];

    for &rust in &modes {
        for &conn in &modes {
            let eff = rust.effective(conn);
            let expected = std::cmp::min(rust, conn);
            assert_eq!(
                eff, expected,
                "effective({:?}, {:?}) must be {:?}",
                rust, conn, expected
            );
        }
    }
}

#[test]
fn st_write_mode_check_write_facts_matrix() {
    let valid_test_facts = WriteFacts {
        handshake_valid: true,
        profile_matches: true,
        integrity_enabled: true,
        ownership_valid: true,
        approved_locator: true,
        connector_created_test: true,
        test_name_prefix: true,
    };

    // Disabled and ReadOnly must fail with ST_WRITE_NOT_READY
    assert_eq!(
        StWriteMode::Disabled.check_write(&valid_test_facts),
        Err("ST_WRITE_NOT_READY")
    );
    assert_eq!(
        StWriteMode::ReadOnly.check_write(&valid_test_facts),
        Err("ST_WRITE_NOT_READY")
    );

    // TestWrite with all valid facts must pass
    assert_eq!(
        StWriteMode::TestWrite.check_write(&valid_test_facts),
        Ok(())
    );

    // ProductionWrite with all valid facts must pass
    assert_eq!(
        StWriteMode::ProductionWrite.check_write(&valid_test_facts),
        Ok(())
    );

    // Missing preflight conditions must fail with ST_WRITE_PREFLIGHT_REJECTED
    let invalid_handshake = WriteFacts {
        handshake_valid: false,
        ..valid_test_facts
    };
    assert_eq!(
        StWriteMode::TestWrite.check_write(&invalid_handshake),
        Err("ST_WRITE_PREFLIGHT_REJECTED")
    );
    assert_eq!(
        StWriteMode::ProductionWrite.check_write(&invalid_handshake),
        Err("ST_WRITE_PREFLIGHT_REJECTED")
    );

    let invalid_profile = WriteFacts {
        profile_matches: false,
        ..valid_test_facts
    };
    assert_eq!(
        StWriteMode::TestWrite.check_write(&invalid_profile),
        Err("ST_WRITE_PREFLIGHT_REJECTED")
    );

    let invalid_integrity = WriteFacts {
        integrity_enabled: false,
        ..valid_test_facts
    };
    assert_eq!(
        StWriteMode::TestWrite.check_write(&invalid_integrity),
        Err("ST_WRITE_PREFLIGHT_REJECTED")
    );

    let invalid_ownership = WriteFacts {
        ownership_valid: false,
        ..valid_test_facts
    };
    assert_eq!(
        StWriteMode::TestWrite.check_write(&invalid_ownership),
        Err("ST_WRITE_PREFLIGHT_REJECTED")
    );

    let invalid_locator = WriteFacts {
        approved_locator: false,
        ..valid_test_facts
    };
    assert_eq!(
        StWriteMode::TestWrite.check_write(&invalid_locator),
        Err("ST_WRITE_PREFLIGHT_REJECTED")
    );

    // TestWrite without test prefix or marker must fail with ST_TEST_SCOPE_REQUIRED
    let missing_prefix = WriteFacts {
        test_name_prefix: false,
        ..valid_test_facts
    };
    assert_eq!(
        StWriteMode::TestWrite.check_write(&missing_prefix),
        Err("ST_TEST_SCOPE_REQUIRED")
    );

    let missing_marker = WriteFacts {
        connector_created_test: false,
        ..valid_test_facts
    };
    assert_eq!(
        StWriteMode::TestWrite.check_write(&missing_marker),
        Err("ST_TEST_SCOPE_REQUIRED")
    );

    // But ProductionWrite does NOT require test_name_prefix or connector_created_test!
    let prod_facts = WriteFacts {
        handshake_valid: true,
        profile_matches: true,
        integrity_enabled: true,
        ownership_valid: true,
        approved_locator: true,
        connector_created_test: false,
        test_name_prefix: false,
    };
    assert_eq!(
        StWriteMode::ProductionWrite.check_write(&prod_facts),
        Ok(())
    );
}

fn origin(update_id: i64) -> BridgeOperationOrigin {
    BridgeOperationOrigin {
        internal_bot_id: "write-mode-bot".into(),
        telegram_update_id: update_id,
        channel_context_key: "tg:1".into(),
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
    .bind("write-mode-bot")
    .bind(workspace_id)
    .bind(&app.actor.account.id)
    .execute(&app.pool)
    .await
    .unwrap();
    let channels = ChannelContextStore::new(app.pool.clone());
    channels
        .select_chat(&app.actor.account.id, workspace_id, "tg:1", &prod_locator())
        .await
        .unwrap();
    let store = Arc::new(OperationStore::new(app.pool.clone()));
    let vault = Arc::new(EncryptedSqliteVault::new(app.pool.clone(), [9_u8; 32]));
    let provider = Arc::new(OperationPayloadKeyProvider::new(vault, store.clone()));
    let coordinator = Arc::new(OperationCoordinator::new_with_key_provider(
        backend.clone(),
        store,
        provider,
        Arc::new(MemoryStOperationJournal::new()),
    ));
    let registry = Arc::new(SharedMemoryPollerRegistry::new());
    registry.bootstrap_legacy(9003).await.unwrap();
    let ownership = registry
        .transfer(9003, PollerOwner::LegacyPlugin, 1, PollerOwner::RustBridge)
        .await
        .unwrap();
    let binding =
        PollerRuntimeBinding::new("write-mode-bot", "write-mode-runtime", ownership.epoch)
            .running();
    registry.claim_runtime(9003, binding.clone()).await.unwrap();
    let guard = Arc::new(PollerOwnershipGuard::new_with_binding(
        9003,
        PollerOwner::RustBridge,
        binding,
        registry,
    ));
    SidecarBridgeEngine::new(backend.clone(), channels)
        .with_required_coordinator(coordinator)
        .with_ownership_guard(guard)
        .with_context_key("tg:1")
}

fn status(mode: StWriteMode) -> StStatus {
    StStatus {
        available: true,
        version: Some("1.16.0-test".into()),
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

fn prod_locator() -> StChatLocator {
    StChatLocator {
        handle: "default-user".into(),
        avatar: "Hero.png".into(),
        character_name: "Hero".into(),
        chat_file: "Hero - 2026-09-05.jsonl".into(),
    }
}

fn test_locator() -> StChatLocator {
    StChatLocator {
        handle: "default-user".into(),
        avatar: "Hero.png".into(),
        character_name: "Hero".into(),
        chat_file: "IMBridge-Test-Hero-1.jsonl".into(),
    }
}

#[tokio::test]
async fn engine_rejects_send_when_backend_is_read_only() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.push_probe(Ok(status(StWriteMode::ReadOnly)));
    let engine = configured_write_engine(&app, backend.clone()).await;

    let err = engine
        .execute_with_origin(
            &app.actor,
            StBridgeCommand::SendMessage {
                locator: test_locator(),
                text: "hello".into(),
                client_operation_id: "op-1".into(),
                model_override: None,
            },
            origin(1),
        )
        .await
        .expect_err("read only backend must reject write");
    assert_eq!(err.code, StErrorCode::StWriteNotReady);
}

#[tokio::test]
async fn engine_test_write_rejects_non_test_chat_file() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.push_probe(Ok(status(StWriteMode::TestWrite)));
    let engine = configured_write_engine(&app, backend.clone()).await;

    let err = engine
        .execute_with_origin(
            &app.actor,
            StBridgeCommand::SendMessage {
                locator: prod_locator(),
                text: "hello".into(),
                client_operation_id: "op-2".into(),
                model_override: None,
            },
            origin(1),
        )
        .await
        .expect_err("test write mode must reject non-test locator");
    assert_eq!(err.code, StErrorCode::StTestScopeRequired);
}

#[tokio::test]
async fn engine_production_write_allows_production_chat_file() {
    let app = common::setup().await;
    let backend = Arc::new(ScriptedStBackend::new());
    backend.push_probe(Ok(status(StWriteMode::ProductionWrite)));
    backend.push_snapshot(Ok(im_bridge::domain::st::StChatSnapshot {
        locator: prod_locator(),
        parsed_chat: vec![serde_json::json!({"chat_metadata": {"integrity": "int-1"}})],
        source_sha256: "sha-1".into(),
        source_integrity: "int-1".into(),
        source_byte_length: 100,
        source_message_count: 1,
    }));
    backend.push_characters(Ok(vec![StCharacterSummary {
        avatar: "Hero.png".into(),
        name: "Hero".into(),
        ..StCharacterSummary::default()
    }]));
    backend.push_settings(Ok(StGenerationSettings {
        username: "User".into(),
        chat_completion_source: "mock".into(),
        model: "mock-model".into(),
        custom_url: "".into(),
        custom_prompt_post_processing: "".into(),
        temperature: 0.7,
        top_p: 1.0,
        max_tokens: 100,
    }));
    backend.push_generation(Ok(StGenerationResult {
        text: "Greetings, traveler!".into(),
        finish_reason: Some("stop".into()),
        usage: None,
    }));
    backend.push_commit(Ok(StCommitResult {
        status: StCommitStatus::Applied,
        new_sha256: Some("sha-2".into()),
        new_integrity: Some("int-2".into()),
        byte_length: Some(250),
        message_count: Some(3),
    }));

    let engine = configured_write_engine(&app, backend.clone()).await;

    let outcome = engine
        .execute_with_origin(
            &app.actor,
            StBridgeCommand::SendMessage {
                locator: prod_locator(),
                text: "hello".into(),
                client_operation_id: "op-3".into(),
                model_override: None,
            },
            origin(1),
        )
        .await
        .expect("production write mode must allow production chat file");

    assert!(outcome.write_committed);
    assert_eq!(outcome.reply_text.as_deref(), Some("Greetings, traveler!"));
}

#[tokio::test]
async fn bootstrap_does_not_retain_connector_hmac_key_in_public_config() {
    let dir = tempfile::tempdir().unwrap();
    let config = im_bridge::config::AppConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        data_dir: dir.path().to_path_buf(),
        database_path: dir.path().join("app.db"),
        master_key_path: dir.path().join("master.key"),
        session_ttl_hours: 12,
        cookie_secure: false,
        st: im_bridge::config::StClientConfig {
            base_url: Some("http://127.0.0.1:18000".into()),
            mode: "test_write".into(),
            connector_hmac_key: Some("0123456789abcdef0123456789abcdef".into()),
            ..Default::default()
        },
    };
    let state = im_bridge::AppState::bootstrap(config, true).await.unwrap();
    assert!(state.config.st.connector_hmac_key.is_none());
}
