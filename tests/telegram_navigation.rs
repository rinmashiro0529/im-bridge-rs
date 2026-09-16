use im_bridge::modules::telegram::callback::{parse_callback_data, CallbackAction};
use im_bridge::modules::telegram::panel::{
    render_help, render_home, render_settings, render_status, split_current_preview, ModelPurpose,
    PanelKind,
};

#[test]
fn home_help_settings_status_form_a_returnable_navigation_loop() {
    let home = render_home();
    assert!(home.keyboard.iter().flatten().any(|button| matches!(
        parse_callback_data(&button.callback_data),
        Ok(CallbackAction::Help)
    )));
    assert!(home.keyboard.iter().flatten().any(|button| matches!(
        parse_callback_data(&button.callback_data),
        Ok(CallbackAction::Settings)
    )));
    assert!(home.keyboard.iter().flatten().any(|button| matches!(
        parse_callback_data(&button.callback_data),
        Ok(CallbackAction::Status)
    )));
    assert!(render_help()
        .keyboard
        .iter()
        .flatten()
        .any(|button| matches!(
            parse_callback_data(&button.callback_data),
            Ok(CallbackAction::Home)
        )));
    assert!(render_settings(None, None)
        .keyboard
        .iter()
        .flatten()
        .any(|button| matches!(
            parse_callback_data(&button.callback_data),
            Ok(CallbackAction::Home)
        )));
    assert!(render_status("ready", "telegram", 42, None)
        .keyboard
        .iter()
        .flatten()
        .any(|button| matches!(
            parse_callback_data(&button.callback_data),
            Ok(CallbackAction::Home)
        )));
}

#[test]
fn breadcrumbs_are_generated_from_panel_kind() {
    assert_eq!(PanelKind::Home.breadcrumb(), "首页");
    assert_eq!(PanelKind::Help.breadcrumb(), "首页 > 帮助");
    assert_eq!(PanelKind::Settings.breadcrumb(), "首页 > 设置");
    assert_eq!(PanelKind::Status.breadcrumb(), "首页 > 状态");
    assert_eq!(
        PanelKind::Providers {
            purpose: ModelPurpose::Chat,
            page: 0
        }
        .breadcrumb(),
        "首页 > 聊天模型 > 厂商选择"
    );
    assert_eq!(
        PanelKind::ProviderModels {
            purpose: ModelPurpose::Compression,
            provider_key: "local".into(),
            page: 0,
        }
        .breadcrumb(),
        "首页 > 压缩模型 > local"
    );
}

#[test]
fn malformed_and_unknown_callbacks_are_rejected() {
    assert!(parse_callback_data("cb:does_not_exist").is_err());
    assert!(parse_callback_data("not-callback").is_err());
    let token = CallbackAction::Home.to_callback_data();
    assert_eq!(token, "cb:home");
    assert!(token.len() <= 4096);
    assert_eq!(parse_callback_data(&token).unwrap(), CallbackAction::Home);
}

#[test]
fn navigation_tokens_are_compact_and_round_trip() {
    let action = CallbackAction::SelectHistory { page: 3, index: 7 };
    let encoded = action.to_callback_data();
    assert!(encoded.len() <= 4096);
    assert_eq!(parse_callback_data(&encoded).unwrap(), action);
}

#[test]
fn current_preview_is_fully_split_by_utf16_limit() {
    let text = format!("剧情：{}{}", "界".repeat(3200), "𠀀".repeat(900));
    let chunks = split_current_preview(&text);
    assert!(chunks.len() > 1);
    assert!(chunks
        .iter()
        .all(|chunk| chunk.encode_utf16().count() <= 3200));
    assert_eq!(chunks.concat(), text);
}

#[test]
fn current_preview_keeps_sensitive_markers_protected() {
    assert_eq!(
        split_current_preview("剧情 token=do-not-show"),
        vec!["受保护对象"]
    );
}
