use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use im_bridge::adapters::http::router::router;
use im_bridge::bootstrap::AppState;
use im_bridge::config::{AppConfig, StClientConfig};
use im_bridge::domain::identity::{Account, Actor};
use im_bridge::modules::telegram::dispatch::dispatch_update;
use serde_json::{json, Value};
use tower::ServiceExt;
use zeroize::Zeroizing;

fn test_password() -> Zeroizing<String> {
    Zeroizing::new(uuid::Uuid::new_v4().to_string())
}

struct Fixture {
    _dir: tempfile::TempDir,
    state: Arc<AppState>,
    admin: Actor,
    password: Zeroizing<String>,
}

async fn setup() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let config = AppConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        data_dir: dir.path().to_path_buf(),
        database_path: dir.path().join("app.db"),
        master_key_path: dir.path().join("master.key"),
        session_ttl_hours: 12,
        cookie_secure: false,
        st: StClientConfig::default(),
    };
    let state = Arc::new(AppState::bootstrap(config, true).await.unwrap());
    let password = test_password();
    let account = state
        .identity
        .bootstrap_admin("fixture-admin", &password, "Fixture Admin")
        .await
        .unwrap();
    let workspace = state
        .identity
        .default_workspace_id(&account.id)
        .await
        .unwrap();
    let admin = state
        .identity
        .actor_in_workspace(account, &workspace)
        .await
        .unwrap();
    Fixture {
        _dir: dir,
        state,
        admin,
        password,
    }
}

async fn row_counts(state: &AppState) -> (i64, i64, i64, i64) {
    sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM accounts), (SELECT COUNT(*) FROM workspaces),
                (SELECT COUNT(*) FROM workspace_members), (SELECT COUNT(*) FROM workspace_settings)",
    )
    .fetch_one(&state.pool)
    .await
    .unwrap()
}

async fn workspace_defaults(state: &AppState, account: &Account) -> (String, String, String) {
    let workspace = state
        .identity
        .default_workspace_id(&account.id)
        .await
        .unwrap();
    sqlx::query_as(
        "SELECT w.name, s.prompt_user_name, s.default_prompt_profile
         FROM workspaces w JOIN workspace_settings s ON s.workspace_id = w.id WHERE w.id = ?",
    )
    .bind(workspace)
    .fetch_one(&state.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn bootstrap_is_idempotent_without_resetting_password_or_defaults() {
    let fixture = setup().await;
    let state = &fixture.state;
    let before = row_counts(state).await;
    let other_password = Zeroizing::new(format!("{}-different", fixture.password.as_str()));
    let repeated = state
        .identity
        .bootstrap_admin("fixture-admin", &other_password, "Different Name")
        .await
        .unwrap();
    assert_eq!(repeated.id, fixture.admin.account.id);
    assert_eq!(repeated.display_name, "Fixture Admin");
    assert_eq!(row_counts(state).await, before);
    assert_eq!(
        workspace_defaults(state, &repeated).await,
        ("Default".into(), "User".into(), "legacy_bridge_v1".into())
    );
    assert!(state
        .identity
        .authenticate("fixture-admin", &fixture.password)
        .await
        .is_ok());
    assert!(state
        .identity
        .authenticate("fixture-admin", &other_password)
        .await
        .is_err());
}

#[tokio::test]
async fn bootstrap_rejects_disabled_or_non_admin_accounts_without_promoting_them() {
    for change in ["disabled_at = 'disabled'", "is_system_admin = 0"] {
        let fixture = setup().await;
        let state = &fixture.state;
        sqlx::QueryBuilder::<sqlx::Sqlite>::new("UPDATE accounts SET ")
            .push(change)
            .push(" WHERE id = ")
            .push_bind(&fixture.admin.account.id)
            .build()
            .execute(&state.pool)
            .await
            .unwrap();
        let before = row_counts(state).await;
        let error = state
            .identity
            .bootstrap_admin("fixture-admin", &fixture.password, "Replacement")
            .await
            .unwrap_err();
        assert_eq!(error.code, "BOOTSTRAP_ACCOUNT_CONFLICT", "{change}");
        assert_eq!(row_counts(state).await, before);
        let stored = state
            .identity
            .get_by_id(&fixture.admin.account.id)
            .await
            .unwrap()
            .unwrap();
        assert!(!stored.is_system_admin || stored.disabled_at.is_some());
    }
}

#[tokio::test]
async fn bootstrap_detects_incomplete_workspaces_without_silent_repair() {
    for damage in [
        "DELETE FROM workspace_settings",
        "DELETE FROM workspace_members",
        "UPDATE workspace_members SET role = 'member'",
    ] {
        let fixture = setup().await;
        let state = &fixture.state;
        sqlx::query(damage).execute(&state.pool).await.unwrap();
        let before = row_counts(state).await;
        let error = state
            .identity
            .bootstrap_admin("fixture-admin", &fixture.password, "Fixture Admin")
            .await
            .unwrap_err();
        assert_eq!(error.code, "BOOTSTRAP_INCOMPLETE", "{damage}");
        assert_eq!(row_counts(state).await, before);
    }
}

#[derive(Clone, Copy, Debug)]
enum Provisioning {
    Bootstrap,
    Create,
    Import,
}

async fn provision(fixture: &Fixture, mode: Provisioning) -> im_bridge::AppResult<Account> {
    let identity = &fixture.state.identity;
    match mode {
        Provisioning::Bootstrap => {
            identity
                .bootstrap_admin("new-user", &fixture.password, "New User")
                .await
        }
        Provisioning::Create => {
            identity
                .create_account(
                    &fixture.admin,
                    "new-user",
                    &fixture.password,
                    "New User",
                    false,
                )
                .await
        }
        Provisioning::Import => identity
            .ensure_legacy_account("new-user", "New User")
            .await
            .map(|(account, _)| account),
    }
}

#[tokio::test]
async fn every_provisioning_entry_rolls_back_each_insert_failure_and_can_retry() {
    for mode in [
        Provisioning::Bootstrap,
        Provisioning::Create,
        Provisioning::Import,
    ] {
        for table in [
            "accounts",
            "workspaces",
            "workspace_members",
            "workspace_settings",
        ] {
            let fixture = setup().await;
            let state = &fixture.state;
            let before = row_counts(state).await;
            // All identifiers come from the fixed list above, never from user input.
            sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "CREATE TRIGGER fail_provision AFTER INSERT ON ",
            )
            .push(table)
            .push(" BEGIN SELECT RAISE(ABORT, 'synthetic provisioning failure'); END")
            .build()
            .execute(&state.pool)
            .await
            .unwrap();
            assert!(provision(&fixture, mode).await.is_err(), "{mode:?}/{table}");
            assert_eq!(row_counts(state).await, before, "{mode:?}/{table}");
            assert!(state
                .identity
                .get_by_username("new-user")
                .await
                .unwrap()
                .is_none());
            sqlx::query("DROP TRIGGER fail_provision")
                .execute(&state.pool)
                .await
                .unwrap();
            let account = provision(&fixture, mode).await.unwrap();
            assert_eq!(row_counts(state).await, (2, 2, 2, 2));
            let defaults = workspace_defaults(state, &account).await;
            match mode {
                Provisioning::Bootstrap => {
                    assert!(account.is_system_admin);
                    assert_eq!(
                        defaults,
                        ("Default".into(), "User".into(), "legacy_bridge_v1".into())
                    );
                }
                Provisioning::Create | Provisioning::Import => {
                    assert!(!account.is_system_admin);
                    assert_eq!(
                        defaults,
                        (
                            "New User".into(),
                            "New User".into(),
                            "legacy_bridge_v1".into()
                        )
                    );
                }
            }
            if matches!(mode, Provisioning::Import) {
                assert_eq!(account.legacy_st_handle.as_deref(), Some("new-user"));
                let repeated = state
                    .identity
                    .ensure_legacy_account("new-user", "Ignored")
                    .await
                    .unwrap();
                assert_eq!(repeated.0.id, account.id);
                assert_eq!(row_counts(state).await, (2, 2, 2, 2));
            }
        }
    }
}

#[tokio::test]
async fn concurrent_bootstrap_never_leaves_partial_or_duplicate_accounts() {
    let fixture = setup().await;
    let identity = &fixture.state.identity;
    let (left, right) = tokio::join!(
        identity.bootstrap_admin("concurrent-user", &fixture.password, "Concurrent"),
        identity.bootstrap_admin("concurrent-user", &fixture.password, "Concurrent"),
    );
    assert!(left.is_ok() || right.is_ok());
    let account = identity
        .bootstrap_admin("concurrent-user", &fixture.password, "Concurrent")
        .await
        .unwrap();
    for result in [left, right].into_iter().flatten() {
        assert_eq!(result.id, account.id);
    }
    assert_eq!(row_counts(&fixture.state).await, (2, 2, 2, 2));
}

#[tokio::test]
async fn authorization_reloads_account_status_and_admin_role_from_storage() {
    for change in ["disabled_at = 'disabled'", "is_system_admin = 0"] {
        let fixture = setup().await;
        let state = &fixture.state;
        sqlx::QueryBuilder::<sqlx::Sqlite>::new("UPDATE accounts SET ")
            .push(change)
            .push(" WHERE id = ")
            .push_bind(&fixture.admin.account.id)
            .build()
            .execute(&state.pool)
            .await
            .unwrap();
        let error = state
            .identity
            .create_account(
                &fixture.admin,
                "forbidden-user",
                &fixture.password,
                "Forbidden",
                false,
            )
            .await
            .unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
        assert_eq!(row_counts(state).await, (1, 1, 1, 1));
        // The stale admin must not bypass membership checks in another workspace.
        let error = state
            .identity
            .actor_in_workspace(fixture.admin.account.clone(), "not-a-membership")
            .await
            .unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
    }
}

async fn login_cookie(app: &axum::Router, username: &str, password: &str) -> String {
    let csrf = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/csrf")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(csrf.status(), StatusCode::OK);
    let cookie = csrf.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let body = csrf.into_body().collect().await.unwrap().to_bytes();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    let login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/auth/login")
                .header("cookie", cookie)
                .header("x-csrf-token", payload["token"].as_str().unwrap())
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"username": username, "password": password}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    login.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn disabled_admins_and_members_are_denied_on_http_and_bound_telegram_paths() {
    for is_admin in [true, false] {
        let fixture = setup().await;
        let state = &fixture.state;
        let actor = if is_admin {
            fixture.admin.clone()
        } else {
            let account = state
                .identity
                .create_account(&fixture.admin, "reader", &fixture.password, "Reader", false)
                .await
                .unwrap();
            let workspace = state
                .identity
                .default_workspace_id(&account.id)
                .await
                .unwrap();
            state
                .identity
                .actor_in_workspace(account, &workspace)
                .await
                .unwrap()
        };
        let bot = state
            .telegram
            .upsert_bot(&actor, None, false)
            .await
            .unwrap();
        let code = state
            .telegram
            .generate_bind_code(&bot.id, &actor.account.id)
            .await
            .unwrap();
        assert_eq!(
            state
                .telegram
                .redeem_bind_code(&bot.id, &actor.account.id, &code.code, "700")
                .await
                .unwrap(),
            "ok"
        );
        if !is_admin {
            sqlx::query("UPDATE workspace_members SET role = 'member' WHERE account_id = ?")
                .bind(&actor.account.id)
                .execute(&state.pool)
                .await
                .unwrap();
        }
        assert!(state
            .telegram
            .resolve_bound_actor(&bot.id, "700")
            .await
            .unwrap()
            .is_some());
        let app = router(state.clone());
        let cookie = login_cookie(&app, &actor.account.username, &fixture.password).await;
        sqlx::query("UPDATE accounts SET disabled_at = 'disabled' WHERE id = ?")
            .bind(&actor.account.id)
            .execute(&state.pool)
            .await
            .unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/auth/me")
                    .header("cookie", cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let error = state
            .identity
            .actor_in_workspace(actor.account.clone(), actor.require_workspace().unwrap())
            .await
            .unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
        for text in ["/chars", "/new", "synthetic message"] {
            let update = json!({"update_id": 1, "message": {
                "from": {"id": 700}, "chat": {"id": 700}, "text": text
            }});
            let error = dispatch_update(&state.telegram, &bot.id, &update)
                .await
                .unwrap_err();
            assert_eq!(error.status, StatusCode::FORBIDDEN, "{is_admin}/{text}");
            assert_eq!(error.message, "account disabled");
        }
        let callback = json!({"update_id": 2, "callback_query": {
            "id": "synthetic-callback", "from": {"id": 700}, "data": "cb:stale",
            "message": {"message_id": 1, "chat": {"id": 700}}
        }});
        let error = dispatch_update(&state.telegram, &bot.id, &callback)
            .await
            .unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
        assert_eq!(error.message, "account disabled");
        sqlx::query("UPDATE accounts SET disabled_at = NULL WHERE id = ?")
            .bind(&actor.account.id)
            .execute(&state.pool)
            .await
            .unwrap();
        assert!(state
            .telegram
            .resolve_bound_actor(&bot.id, "700")
            .await
            .unwrap()
            .is_some());
        sqlx::query("UPDATE telegram_bindings SET revoked_at = 'revoked' WHERE bot_id = ?")
            .bind(&bot.id)
            .execute(&state.pool)
            .await
            .unwrap();
        assert!(state
            .telegram
            .resolve_bound_actor(&bot.id, "700")
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn legacy_creation_keeps_legacy_handle_acceptance_and_named_defaults() {
    let fixture = setup().await;
    let (account, workspace) = fixture
        .state
        .identity
        .ensure_legacy_account("legacy user", "Legacy User")
        .await
        .unwrap();
    assert_eq!(account.username, "legacy user");
    assert_eq!(account.legacy_st_handle.as_deref(), Some("legacy user"));
    assert_eq!(
        fixture
            .state
            .identity
            .default_workspace_id(&account.id)
            .await
            .unwrap(),
        workspace
    );
    assert_eq!(
        workspace_defaults(&fixture.state, &account).await,
        (
            "Legacy User".into(),
            "Legacy User".into(),
            "legacy_bridge_v1".into()
        )
    );
}
