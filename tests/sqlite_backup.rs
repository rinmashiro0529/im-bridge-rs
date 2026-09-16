use im_bridge::adapters::sqlite::{backup_database, connect_pool};

mod common;

#[tokio::test]
async fn vacuum_into_backup_contains_wal_commits_and_restores() {
    let app = common::setup().await;
    sqlx::query(
        "INSERT INTO audit_events
            (actor_id, operation, result, metadata_json, created_at)
         VALUES (?, 'backup-test', 'ok', '{}', '2026-01-01T00:00:00Z')",
    )
    .bind(&app.actor.account.id)
    .execute(&app.pool)
    .await
    .unwrap();
    let output = app._dir.path().join("backups/app.db");
    backup_database(&app.pool, &output).await.unwrap();
    assert!(output.exists());
    assert!(backup_database(&app.pool, &output).await.is_err());

    let restored = connect_pool(&output).await.unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE operation = 'backup-test'")
            .fetch_one(&restored)
            .await
            .unwrap();
    assert_eq!(count, 1);
}
