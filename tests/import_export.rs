use std::path::PathBuf;

use im_bridge::adapters::secrets::encrypted_sqlite::EncryptedSqliteVault;
use im_bridge::adapters::sqlite::connect_pool;
use im_bridge::modules::chat::{ChatCommand, ChatEngine};
use im_bridge::modules::migration::{FilesystemLegacySource, LegacyImporter};
use im_bridge::modules::telegram::TelegramModule;
use im_bridge::seams::legacy_source::LegacySource;
use im_bridge::seams::secret_vault::SecretVault;

mod common;

#[tokio::test]
async fn jsonl_import_preserves_raw_and_export_patches() {
    let app = common::setup().await;
    let (character_id, workspace_id) = common::seed_character_and_provider(&app).await;
    let _ = character_id;
    let source_dir = app._dir.path().join("st-data/default-user/chats/TestChar");
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::copy(
        "fixtures/st_chats/sample.jsonl",
        source_dir.join("sample.jsonl"),
    )
    .unwrap();
    std::fs::create_dir_all(app._dir.path().join("st-data/default-user/characters")).unwrap();
    std::fs::copy(
        "fixtures/character_cards/v2.json",
        app._dir
            .path()
            .join("st-data/default-user/characters/TestChar.json"),
    )
    .unwrap();
    std::fs::write(
        app._dir.path().join("st-data/default-user/settings.json"),
        r#"{"username":"Alice"}"#,
    )
    .unwrap();

    let importer = LegacyImporter::new(
        app.pool.clone(),
        app.identity.clone(),
        app.characters.clone(),
        app.models.clone(),
    );
    let source = FilesystemLegacySource::new(app._dir.path().join("st-data"));
    assert_eq!(
        source.list_handles().await.unwrap(),
        vec!["default-user".to_string()]
    );
    let dry_run = importer.import_source(&source, true).await.unwrap();
    assert!(dry_run.characters >= 1);
    assert!(app
        .identity
        .get_by_username("default-user")
        .await
        .unwrap()
        .is_none());
    let report = importer.import_source(&source, false).await.unwrap();
    assert!(report.conversations >= 1);
    assert!(report.messages >= 3);
    let conversation_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM conversations WHERE legacy_locator IS NOT NULL")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let replay = importer.import_source(&source, false).await.unwrap();
    assert_eq!(replay.characters, 0);
    assert_eq!(replay.conversations, 0);
    let conversation_count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM conversations WHERE legacy_locator IS NOT NULL")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(conversation_count_after, conversation_count);

    let raw: Option<String> = sqlx::query_scalar(
        "SELECT source_raw_json FROM messages WHERE source_raw_json LIKE '%custom_client%' LIMIT 1",
    )
    .fetch_optional(&app.pool)
    .await
    .unwrap();
    assert!(raw.unwrap().contains("custom_client"));

    let imported_account = app
        .identity
        .get_by_username("default-user")
        .await
        .unwrap()
        .expect("imported account");
    let imported_workspace = app
        .identity
        .default_workspace_id(&imported_account.id)
        .await
        .unwrap();
    let out = app._dir.path().join("export");
    let exported = importer
        .export_workspace(&imported_workspace, &out)
        .await
        .unwrap();
    assert!(exported >= 1);
    assert!(out.join("TestChar/sample.jsonl").exists());
    let imported_conversation: String = sqlx::query_scalar(
        "SELECT id FROM conversations WHERE workspace_id = ? AND legacy_locator IS NOT NULL LIMIT 1",
    )
    .bind(&imported_workspace)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    let imported_actor = app
        .identity
        .actor_in_workspace(imported_account, &imported_workspace)
        .await
        .unwrap();
    app.chat
        .execute(
            &imported_actor,
            ChatCommand::UndoLastTurn {
                conversation_id: imported_conversation.clone(),
                channel: "telegram".into(),
                external_context_key: "import-test".into(),
                client_turn_id: Some("import-undo".into()),
                expected_revision: None,
            },
            None,
        )
        .await
        .unwrap();
    let revoked: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM messages WHERE conversation_id = ? AND status = 'revoked'",
    )
    .bind(imported_conversation)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(revoked, 2);
    let _ = workspace_id;
    let _ = PathBuf::from("ok");
}

#[tokio::test]
async fn plugin_db_import_encrypts_tokens_and_keeps_bots_disabled() {
    let app = common::setup().await;
    let plugin_db = app._dir.path().join("legacy-plugin.db");
    let legacy = connect_pool(&plugin_db).await.unwrap();
    sqlx::query(
        "CREATE TABLE accounts (
            account_id TEXT PRIMARY KEY,
            display_name TEXT,
            created_at TEXT NOT NULL
        )",
    )
    .execute(&legacy)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE account_configs (
            account_id TEXT PRIMARY KEY,
            telegram_bot_token TEXT,
            telegram_allowed_user_ids TEXT NOT NULL
        )",
    )
    .execute(&legacy)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE external_identities (
            account_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            external_user_id TEXT NOT NULL
        )",
    )
    .execute(&legacy)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO accounts (account_id, display_name, created_at)
         VALUES ('old-account', 'Legacy Alice', '2026-01-01T00:00:00Z')",
    )
    .execute(&legacy)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO account_configs
            (account_id, telegram_bot_token, telegram_allowed_user_ids)
         VALUES ('old-account', '123456:legacy-secret', '[4242]')",
    )
    .execute(&legacy)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO external_identities (account_id, channel, external_user_id)
         VALUES ('old-account', 'telegram', '4242')",
    )
    .execute(&legacy)
    .await
    .unwrap();
    legacy.close().await;

    let importer = LegacyImporter::new(
        app.pool.clone(),
        app.identity.clone(),
        app.characters.clone(),
        app.models.clone(),
    );
    let telegram = TelegramModule::new(app.pool.clone());
    let vault = EncryptedSqliteVault::new(app.pool.clone(), [5u8; 32]);
    let dry_run = importer
        .import_plugin_db(&plugin_db, &telegram, &vault, true)
        .await
        .unwrap();
    assert_eq!(dry_run.bots, 1);
    let bots_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telegram_bots")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(bots_before, 0);
    assert!(app
        .identity
        .get_by_username("old-account")
        .await
        .unwrap()
        .is_none());
    let report = importer
        .import_plugin_db(&plugin_db, &telegram, &vault, false)
        .await
        .unwrap();
    assert_eq!(report.bots, 1);
    assert_eq!(report.bindings, 1);
    let bot: (String, i64, String) =
        sqlx::query_as("SELECT token_secret_id, desired_enabled, id FROM telegram_bots LIMIT 1")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(bot.1, 0);
    assert_eq!(vault.get(&bot.0).await.unwrap(), b"123456:legacy-secret");
    let ciphertext: Vec<u8> = sqlx::query_scalar("SELECT ciphertext FROM secrets WHERE id = ?")
        .bind(&bot.0)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert!(!String::from_utf8_lossy(&ciphertext).contains("legacy-secret"));
    let bindings: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM telegram_bindings WHERE bot_id = ?")
            .bind(&bot.2)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(bindings, 1);
    let secrets_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM secrets")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    importer
        .import_plugin_db(&plugin_db, &telegram, &vault, false)
        .await
        .unwrap();
    let secrets_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM secrets")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    let bots_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telegram_bots")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    let bindings_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telegram_bindings")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(secrets_after, secrets_before);
    assert_eq!(bots_after, 1);
    assert_eq!(bindings_after, 1);

    let legacy = connect_pool(&plugin_db).await.unwrap();
    sqlx::query(
        "INSERT INTO accounts (account_id, display_name, created_at)
         VALUES ('other-account', 'Other owner', '2026-01-02T00:00:00Z')",
    )
    .execute(&legacy)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO account_configs
            (account_id, telegram_bot_token, telegram_allowed_user_ids)
         VALUES ('other-account', '654321:other-secret', '[4242]')",
    )
    .execute(&legacy)
    .await
    .unwrap();
    legacy.close().await;
    let conflict = importer
        .import_plugin_db(&plugin_db, &telegram, &vault, true)
        .await
        .unwrap();
    assert_eq!(conflict.bindings, 0);
    assert!(conflict
        .warnings
        .iter()
        .any(|warning| warning.contains("multiple legacy owners")));
}
