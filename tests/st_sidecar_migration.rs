use im_bridge::adapters::sqlite::connect_pool;

#[tokio::test]
async fn additive_sidecar_migration_preserves_existing_rows_and_is_append_only() {
    let dir = tempfile::tempdir().unwrap();
    let pool = connect_pool(&dir.path().join("legacy.db")).await.unwrap();
    for migration in [
        include_str!("../migrations/0001_init.sql"),
        include_str!("../migrations/0002_channel_command_idempotency.sql"),
        include_str!("../migrations/0003_legacy_source_keys.sql"),
    ] {
        sqlx::raw_sql(migration).execute(&pool).await.unwrap();
    }

    sqlx::query(
        "INSERT INTO accounts (id, username, display_name, password_hash, is_system_admin, created_at, updated_at)
         VALUES ('account-synthetic', 'synthetic-user', 'Synthetic User', 'hash', 1, '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO workspaces (id, name, created_by, created_at, updated_at)
         VALUES ('workspace-synthetic', 'Synthetic Workspace', 'account-synthetic', '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO telegram_bots (id, workspace_id, owner_account_id, desired_enabled, created_at, updated_at)
         VALUES ('bot-synthetic', 'workspace-synthetic', 'account-synthetic', 0, '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO external_identities (id, account_id, channel, external_user_id, verified_at, created_at)
         VALUES ('identity-synthetic', 'account-synthetic', 'telegram', 'user-synthetic', '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO telegram_bindings (id, bot_id, external_identity_id, workspace_id, bound_at)
         VALUES ('binding-synthetic', 'bot-synthetic', 'identity-synthetic', 'workspace-synthetic', '2026-09-04T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO telegram_updates (bot_id, update_id, raw_update_json, status, received_at, updated_at)
         VALUES ('bot-synthetic', 7, '{\"fixture\":true}', 'received', '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO channel_deliveries (id, bot_id, chat_id, message_kind, status, created_at, updated_at)
         VALUES ('delivery-synthetic', 'bot-synthetic', 'chat-synthetic', 'reply', 'sent', '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::raw_sql(include_str!("../migrations/0004_st_sidecar.sql"))
        .execute(&pool)
        .await
        .unwrap();

    let preserved: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM telegram_bots WHERE id = 'bot-synthetic'),
            (SELECT COUNT(*) FROM telegram_bindings WHERE id = 'binding-synthetic' AND revoked_at IS NULL),
            (SELECT COUNT(*) FROM telegram_updates WHERE bot_id = 'bot-synthetic' AND update_id = 7),
            (SELECT COUNT(*) FROM channel_deliveries WHERE id = 'delivery-synthetic')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(preserved, (1, 1, 1, 1));
    for table in [
        "characters",
        "conversations",
        "messages",
        "provider_profiles",
    ] {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(exists, 1, "native table was removed: {table}");
    }

    let locator_columns: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('channel_contexts')
         WHERE name IN ('st_handle', 'st_character_avatar', 'st_character_name', 'st_chat_file', 'chat_model_id', 'compression_model_id')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(locator_columns, 6);
    for table in ["bridge_operations", "bridge_error_events"] {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(exists, 1, "missing table {table}");
    }

    sqlx::query(
        "INSERT INTO bridge_error_events
            (stage, code, safe_message, retryable, commit_state, attempt, created_at)
         VALUES ('connect', 'ST_CONNECT_FAILED', 'synthetic failure', 1, 'not_started', 1, '2026-09-04T00:00:01Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        sqlx::query("UPDATE bridge_error_events SET safe_message = 'changed'")
            .execute(&pool)
            .await
            .is_err()
    );
    assert!(sqlx::query("DELETE FROM bridge_error_events")
        .execute(&pool)
        .await
        .is_err());
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bridge_error_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(event_count, 1);
}
