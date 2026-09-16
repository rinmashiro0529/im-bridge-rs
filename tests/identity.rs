mod common;

#[tokio::test]
async fn created_account_has_a_default_workspace_and_can_authenticate() {
    let app = common::setup().await;
    let account = app
        .identity
        .create_account(&app.actor, "bob", "strong-password", "Bob", false)
        .await
        .unwrap();
    let workspace_id = app
        .identity
        .default_workspace_id(&account.id)
        .await
        .unwrap();
    let authenticated = app
        .identity
        .authenticate("bob", "strong-password")
        .await
        .unwrap();
    let actor = app
        .identity
        .actor_in_workspace(authenticated, &workspace_id)
        .await
        .unwrap();
    assert_eq!(actor.require_workspace().unwrap(), workspace_id);
    assert!(actor.can_manage_workspace());
}

#[tokio::test]
async fn login_failures_lock_the_account_key_for_fifteen_minutes() {
    let app = common::setup().await;
    for _ in 0..10 {
        assert!(app
            .identity
            .authenticate("alice", "wrong-password")
            .await
            .is_err());
    }
    let error = app
        .identity
        .authenticate("alice", "secret-password")
        .await
        .unwrap_err();
    assert_eq!(error.code, "RATE_LIMITED");
    let locked_until: String =
        sqlx::query_scalar("SELECT locked_until FROM login_rate_limits WHERE key = 'alice'")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert!(locked_until > im_bridge::clock::now_rfc3339());
}

#[tokio::test]
async fn unknown_usernames_do_not_grow_the_rate_limit_table() {
    let app = common::setup().await;
    assert!(app
        .identity
        .authenticate("unknown-user", "synthetic-password")
        .await
        .is_err());
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM login_rate_limits WHERE key = 'unknown-user'")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(count, 0);
}
