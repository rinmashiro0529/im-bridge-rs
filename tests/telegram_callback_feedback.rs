use std::time::Duration;

use im_bridge::modules::telegram::callback::{
    CallbackAction, CallbackBinding, CallbackDedup, CallbackStore, OPAQUE_CALLBACK_DATA_BYTES,
};
use im_bridge::modules::telegram::panel::{
    InlineButton, PanelEffect, PanelKind, PanelParseMode, TelegramPanel,
};

mod common;

#[test]
fn repeated_callback_nonce_is_deduplicated() {
    let dedup = CallbackDedup::new(Duration::from_secs(10));
    assert!(dedup.accept_with_nonce("query-1", "nonce-1"));
    assert!(!dedup.accept_with_nonce("query-1", "nonce-1"));
    assert!(dedup.accept_with_nonce("query-1", "nonce-2"));
}

#[test]
fn callback_actions_have_fast_ack_compatible_tokens() {
    let action = CallbackAction::Status;
    let token = action.to_callback_data();
    assert_eq!(token, "cb:status");
    assert_eq!(CallbackAction::parse(&token), Some(CallbackAction::Status));
}

#[tokio::test]
async fn callback_store_issues_opaque_tokens_of_fixed_length() {
    let app = common::setup().await;
    let store = CallbackStore::new(app.pool.clone());
    let token = store
        .issue(&CallbackBinding {
            account_id: app.actor.account.id.clone(),
            internal_bot_id: "callback-bot".into(),
            numeric_bot_id: 1,
            chat_id: 9,
            authorized_user_id: "4242".into(),
            panel_id: "panel-1".into(),
            message_id: 101,
            panel_revision: 1,
            catalog_revision: None,
            expires_at_unix: time::OffsetDateTime::now_utc().unix_timestamp() + 60,
            action: CallbackAction::Status,
            action_nonce: "nonce-opaque-1".into(),
        })
        .await
        .unwrap();
    assert_eq!(token.len(), OPAQUE_CALLBACK_DATA_BYTES);
    assert!(token.starts_with("cb:"));
    assert!(token[3..]
        .chars()
        .all(|character| character.is_ascii_hexdigit()));
    assert!(token.len() <= 64);
}

#[test]
fn errors_can_be_represented_as_alert_toasts() {
    let effect = PanelEffect::Toast {
        callback_query_id: "query-1".into(),
        text: "操作失败，请稍后重试".into(),
        show_alert: true,
    };
    assert!(matches!(
        effect,
        PanelEffect::Toast {
            show_alert: true,
            ..
        }
    ));
}

#[test]
fn panel_keyboard_serialization_is_data_only() {
    let panel = TelegramPanel {
        kind: PanelKind::Help,
        revision: 0,
        catalog_revision: None,
        text: "help".into(),
        keyboard: vec![vec![InlineButton {
            label: "home".into(),
            callback_data: "cb:home".into(),
        }]],
        parse_mode: PanelParseMode::PlainText,
    };
    assert_eq!(panel.keyboard[0][0].callback_data, "cb:home");
}
