use std::sync::Arc;

use http_body_util::BodyExt;
use im_bridge::adapters::http::router::router;
use im_bridge::bootstrap::AppState;
use im_bridge::config::AppConfig;
use tower::ServiceExt;

#[tokio::test]
async fn health_and_static_ui() {
    let dir = tempfile::tempdir().unwrap();
    let config = AppConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        data_dir: dir.path().to_path_buf(),
        database_path: dir.path().join("app.db"),
        master_key_path: dir.path().join("master.key"),
        session_ttl_hours: 12,
        cookie_secure: false,
        st: im_bridge::config::StClientConfig::default(),
    };
    let state = AppState::bootstrap(config, true).await.unwrap();
    let app = router(Arc::new(state));
    let live = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/health/live")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(live.status(), 200);
    let ready = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/health/ready")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ready.status(), 200);
    let body = ready.into_body().collect().await.unwrap().to_bytes();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload
            .pointer("/st/writeReady")
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
    let ui = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ui.status(), 200);
    assert_eq!(
        ui.headers()
            .get("x-content-type-options")
            .and_then(|value| value.to_str().ok()),
        Some("nosniff")
    );
    assert_eq!(
        ui.headers()
            .get("x-frame-options")
            .and_then(|value| value.to_str().ok()),
        Some("DENY")
    );
}

#[tokio::test]
async fn anonymous_requests_are_not_automatically_authenticated() {
    let dir = tempfile::tempdir().unwrap();
    let config = AppConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        data_dir: dir.path().to_path_buf(),
        database_path: dir.path().join("app.db"),
        master_key_path: dir.path().join("master.key"),
        session_ttl_hours: 12,
        cookie_secure: false,
        st: im_bridge::config::StClientConfig::default(),
    };
    let state = AppState::bootstrap(config, true).await.unwrap();
    state
        .identity
        .bootstrap_admin("alice", "unused-password", "Alice")
        .await
        .unwrap();
    let app = router(Arc::new(state));
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/v1/auth/me")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn native_character_and_conversation_routes_are_retired() {
    let dir = tempfile::tempdir().unwrap();
    let config = AppConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        data_dir: dir.path().to_path_buf(),
        database_path: dir.path().join("app.db"),
        master_key_path: dir.path().join("master.key"),
        session_ttl_hours: 12,
        cookie_secure: false,
        st: im_bridge::config::StClientConfig::default(),
    };
    let state = AppState::bootstrap(config, true).await.unwrap();
    state
        .identity
        .bootstrap_admin("alice", "unused-password", "Alice")
        .await
        .unwrap();
    let app = router(Arc::new(state));
    let characters = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/v1/characters")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(characters.status(), 410);
    let conversations = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/v1/conversations")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(conversations.status(), 410);
}

#[tokio::test]
async fn non_loopback_plain_cookie_configuration_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let config = AppConfig {
        listen: "0.0.0.0:0".parse().unwrap(),
        data_dir: dir.path().to_path_buf(),
        database_path: dir.path().join("app.db"),
        master_key_path: dir.path().join("master.key"),
        session_ttl_hours: 12,
        cookie_secure: false,
        st: im_bridge::config::StClientConfig::default(),
    };
    assert!(AppState::bootstrap(config, true).await.is_err());
}
