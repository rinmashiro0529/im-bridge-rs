use im_bridge::domain::character::NormalizedCardFields;
use im_bridge::domain::conversation::{Message, MessageRole, MessageStatus};
use im_bridge::modules::chat::prompt::{
    build_system_prompt, normalize_assistant_reply, normalize_model_input_text, sanitize_card_text,
    select_recent_messages, to_openai_messages,
};
use serde_json::json;

fn message(role: MessageRole, text: &str, name: &str) -> Message {
    Message {
        id: text.to_string(),
        conversation_id: "c".into(),
        turn_id: None,
        sequence: 0,
        role,
        content_original: text.into(),
        prompt_content: None,
        status: MessageStatus::Active,
        supersedes_id: None,
        metadata: json!({"name": name}),
        source_raw_json: None,
    }
}

#[test]
fn system_prompt_matches_legacy_bridge() {
    let card = NormalizedCardFields {
        name: "TestChar".into(),
        description: "一位冷静的叙事者。{{user}} 是旅人。".into(),
        personality: "克制、观察细致".into(),
        scenario: "雨夜的港口".into(),
        first_mes: "你好".into(),
        mes_example: "{{user}}: 你好\n{{char}}: 嗯。".into(),
        system_prompt: "ignored".into(),
        post_history_instructions: "ignored".into(),
    };
    let prompt = build_system_prompt(&card, "Alice");
    assert!(prompt.starts_with("你是 TestChar。你必须严格保持角色设定"));
    assert!(prompt.contains("一位冷静的叙事者。Alice 是旅人。"));
    assert!(prompt.contains("克制、观察细致"));
    assert!(prompt.contains("雨夜的港口"));
    assert!(prompt.contains("示例对话："));
    assert!(prompt.contains("Alice: 你好"));
    assert!(prompt.contains("TestChar: 嗯。"));
    assert!(!prompt.contains("ignored"));
}

#[test]
fn stop_markers_trim_card_text() {
    let text = sanitize_card_text("设定开始 *重点 后面都不要", "A", "B", 2200);
    assert_eq!(text, "设定开始");
}

#[test]
fn recent_messages_keep_digest_and_last_24() {
    let digest = Message {
        prompt_content: None,
        content_original: "[CompressionDigest] summary".into(),
        role: MessageRole::System,
        ..message(MessageRole::System, "[CompressionDigest] summary", "sys")
    };
    let mut messages = vec![digest];
    for index in 0..30 {
        messages.push(message(
            if index % 2 == 0 {
                MessageRole::User
            } else {
                MessageRole::Assistant
            },
            &format!("m{index}"),
            "n",
        ));
    }
    let selected = select_recent_messages(&messages);
    assert_eq!(selected[0].content_original, "[CompressionDigest] summary");
    assert_eq!(selected.len(), 25);
    assert_eq!(selected.last().unwrap().content_original, "m29");
}

#[test]
fn openai_messages_use_display_text_and_name() {
    let mut assistant = message(MessageRole::Assistant, "long original", "TestChar");
    assistant.prompt_content = Some("灯塔还亮着。".into());
    let history = [
        message(MessageRole::User, "今晚港口很安静。", "Alice"),
        assistant,
    ];
    let refs: Vec<&Message> = history.iter().collect();
    let messages = to_openai_messages("SYS", &refs, "Alice", Some("继续"));
    assert_eq!(messages[0].role, "system");
    assert_eq!(messages[1].name.as_deref(), Some("Alice"));
    assert_eq!(messages[2].content, "灯塔还亮着。");
    assert_eq!(messages[3].name.as_deref(), Some("Alice"));
    assert_eq!(messages[3].content, "继续");
}

#[test]
fn assistant_reply_normalization_strips_name_prefix() {
    let text = normalize_assistant_reply("TestChar", "TestChar：灯塔还亮着。");
    assert_eq!(text, "灯塔还亮着。");
    assert_eq!(normalize_model_input_text("a  \r\n\r\n\r\nb\t"), "a\n\nb");
}
