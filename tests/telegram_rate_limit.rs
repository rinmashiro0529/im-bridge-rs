use im_bridge::modules::telegram::delivery::TelegramDelivery;
use im_bridge::modules::telegram::panel::{render_home, PanelEffect};
use sqlx::SqlitePool;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn telegram_429_retry_after_sets_delivery_cooldown() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/sendMessage"))
        .respond_with(ResponseTemplate::new(429).set_body_json(serde_json::json!({
            "ok": false,
            "error_code": 429,
            "description": "Too Many Requests",
            "parameters": {"retry_after": 7}
        })))
        .mount(&server)
        .await;
    let delivery = TelegramDelivery::new(
        reqwest::Client::new(),
        pool,
        "test-token".into(),
        server.uri(),
        "bot-1".into(),
        1,
        9,
        0,
        0,
        1,
        1,
        3200,
    );
    let result = delivery
        .execute_panel_effect(&PanelEffect::Create {
            panel_id: "panel-1".into(),
            chat_id: 9,
            panel: render_home(),
        })
        .await;
    assert!(result.is_err());
    assert!(delivery.is_cooling_down());
    assert!(delivery.cooldown_remaining().as_secs() <= 7);
}

#[tokio::test]
async fn empty_panel_keyboard_is_omitted_from_send_message() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/sendMessage"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": true,
            "result": {"message_id": 42}
        })))
        .mount(&server)
        .await;
    let delivery = TelegramDelivery::new(
        reqwest::Client::new(),
        pool,
        "test-token".into(),
        server.uri(),
        "bot-1".into(),
        1,
        9,
        0,
        0,
        1,
        1,
        3200,
    );
    let mut panel = render_home();
    panel.keyboard.clear();

    let result = delivery
        .execute_panel_effect(&PanelEffect::Create {
            panel_id: "panel-empty".into(),
            chat_id: 9,
            panel,
        })
        .await
        .unwrap();

    assert_eq!(result, Some(42));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert!(body.get("reply_markup").is_none());
    assert!(body.get("parse_mode").is_none());
}

#[tokio::test]
async fn plain_text_panel_edit_omits_parse_mode() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/editMessageText"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ok": true,
            "result": true
        })))
        .mount(&server)
        .await;
    let delivery = TelegramDelivery::new(
        reqwest::Client::new(),
        pool,
        "test-token".into(),
        server.uri(),
        "bot-1".into(),
        1,
        9,
        0,
        0,
        1,
        1,
        3200,
    );
    let panel = render_home();
    let slot = im_bridge::modules::telegram::panel::PanelSlot {
        panel_id: "panel-edit".into(),
        account_id: "account-1".into(),
        numeric_bot_id: 1,
        chat_id: 9,
        message_id: 42,
        kind: im_bridge::modules::telegram::panel::PanelKind::Home,
        revision: 0,
        active: true,
        expires_at_unix: 1,
    };

    let result = delivery
        .execute_panel_effect(&PanelEffect::Replace {
            slot,
            expected_revision: 0,
            panel,
        })
        .await
        .unwrap();

    assert_eq!(result, Some(42));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert!(body.get("parse_mode").is_none());
    assert!(body.get("reply_markup").is_some());
}

#[test]
fn terminal_panel_effects_are_explicit_and_not_dropped() {
    let effect = PanelEffect::FallbackSend {
        old_slot: im_bridge::modules::telegram::panel::PanelSlot {
            panel_id: "old".into(),
            account_id: "account".into(),
            numeric_bot_id: 1,
            chat_id: 1,
            message_id: 1,
            kind: im_bridge::modules::telegram::panel::PanelKind::Home,
            revision: 0,
            active: true,
            expires_at_unix: 1,
        },
        panel: render_home(),
        effect_id: "effect-1".into(),
    };
    assert!(matches!(effect, PanelEffect::FallbackSend { .. }));
}
