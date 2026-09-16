use crate::domain::character::NormalizedCardFields;
use crate::domain::conversation::{Message, MessageRole};
use crate::seams::llm_gateway::LlmMessage;

const RECENT_LIMIT: usize = 24;
const STOP_MARKERS: &[&str] = &["*重点", "nsfw", "NSFW", "18岁", "18周岁", "无内容限制"];

pub fn substitute_placeholders(input: &str, character_name: &str, user_name: &str) -> String {
    input
        .replace("{{char}}", character_name)
        .replace("{{user}}", user_name)
}

pub fn sanitize_card_text(
    input: &str,
    character_name: &str,
    user_name: &str,
    max_length: usize,
) -> String {
    let substituted = substitute_placeholders(input, character_name, user_name);
    let mut trimmed = substituted;
    for marker in STOP_MARKERS {
        if let Some(index) = trimmed.find(marker) {
            trimmed.truncate(index);
        }
    }
    trimmed
        .chars()
        .take(max_length)
        .collect::<String>()
        .trim()
        .to_string()
}

pub fn build_system_prompt(card: &NormalizedCardFields, user_name: &str) -> String {
    let mut parts = vec![format!(
        "你是 {}。你必须严格保持角色设定，继续当前剧情，不要跳出角色，不要写元说明。",
        card.name
    )];
    let description = sanitize_card_text(&card.description, &card.name, user_name, 2200);
    if !description.is_empty() {
        parts.push(description);
    }
    let personality = sanitize_card_text(&card.personality, &card.name, user_name, 600);
    if !personality.is_empty() {
        parts.push(personality);
    }
    let scenario = sanitize_card_text(&card.scenario, &card.name, user_name, 1200);
    if !scenario.is_empty() {
        parts.push(scenario);
    }
    if !card.mes_example.trim().is_empty() {
        let example = sanitize_card_text(&card.mes_example, &card.name, user_name, 1200);
        if !example.is_empty() {
            parts.push(format!("示例对话：\n{example}"));
        }
    }
    parts.join("\n\n")
}

pub fn normalize_model_input_text(input: &str) -> String {
    let normalized = input.replace("\r\n", "\n").replace('\r', "\n");
    let joined = normalized
        .split('\n')
        .map(|line| line.trim_end_matches([' ', '\t']).to_string())
        .collect::<Vec<_>>()
        .join("\n");
    collapse_newlines(&joined).trim().to_string()
}

fn collapse_newlines(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut newline_run = 0usize;
    for ch in input.chars() {
        if ch == '\n' {
            newline_run += 1;
            if newline_run <= 2 {
                out.push('\n');
            }
        } else {
            newline_run = 0;
            out.push(ch);
        }
    }
    out
}

fn should_reformat(text: &str) -> bool {
    let newline_count = text.chars().filter(|ch| *ch == '\n').count();
    if newline_count >= 8 {
        return false;
    }
    text.len() >= 120
        || text.contains("━━━━━━")
        || text.contains("──────")
        || "①②③④⑤⑥⑦⑧".chars().any(|ch| text.contains(ch))
        || text.contains('「')
        || text.contains('」')
        || text.contains('『')
        || text.contains('』')
}

pub fn normalize_assistant_reply(character_name: &str, input: &str) -> String {
    let mut text = normalize_model_input_text(input);
    if text.is_empty() {
        return text;
    }
    let prefixes = [
        format!("{character_name}："),
        format!("{character_name}:"),
        format!("{character_name} :"),
        format!("{character_name} ："),
    ];
    for prefix in prefixes {
        if let Some(stripped) = text.strip_prefix(&prefix) {
            text = stripped.trim_start().to_string();
            break;
        }
    }
    if !should_reformat(&text) {
        return text;
    }
    text = regex_lite_replace(&text);
    text.lines()
        .map(str::trim)
        .collect::<Vec<_>>()
        .join("\n")
        .split("\n\n\n")
        .collect::<Vec<_>>()
        .join("\n\n")
        .trim()
        .to_string()
}

fn regex_lite_replace(input: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = input.chars().collect();
    let mut idx = 0;
    while idx < chars.len() {
        let ch = chars[idx];
        if ch == '━' || ch == '─' {
            let mut end = idx;
            while end < chars.len() && chars[end] == ch {
                end += 1;
            }
            let run: String = chars[idx..end].iter().collect();
            if end - idx >= 6 {
                out.push('\n');
                out.push_str(&run);
                out.push('\n');
            } else {
                out.push_str(&run);
            }
            idx = end;
            continue;
        }
        out.push(ch);
        idx += 1;
    }
    for marker in ["①", "②", "③", "④", "⑤", "⑥", "⑦", "⑧"] {
        out = out.replace(marker, &format!("\n{marker}"));
    }
    out
}

pub fn select_recent_messages(messages: &[Message]) -> Vec<&Message> {
    let digest = messages
        .iter()
        .find(|message| message.prompt_text().starts_with("[CompressionDigest]"));
    let mut normal: Vec<&Message> = messages
        .iter()
        .filter(|message| message.role != MessageRole::System)
        .collect();
    if normal.len() > RECENT_LIMIT {
        normal = normal.split_off(normal.len() - RECENT_LIMIT);
    }
    let mut out = Vec::new();
    if let Some(digest) = digest {
        out.push(digest);
    }
    out.extend(normal);
    out
}

pub fn to_openai_messages(
    system_prompt: &str,
    history: &[&Message],
    user_name: &str,
    extra_user: Option<&str>,
) -> Vec<LlmMessage> {
    let mut messages = vec![LlmMessage {
        role: "system".into(),
        content: system_prompt.to_string(),
        name: None,
    }];
    for message in history {
        if message.prompt_text().trim().is_empty() {
            continue;
        }
        match message.role {
            MessageRole::System => messages.push(LlmMessage {
                role: "system".into(),
                content: normalize_model_input_text(message.prompt_text()),
                name: None,
            }),
            MessageRole::User => messages.push(LlmMessage {
                role: "user".into(),
                content: normalize_model_input_text(message.prompt_text()),
                name: Some(speaker_name(message, user_name, true)),
            }),
            MessageRole::Assistant => messages.push(LlmMessage {
                role: "assistant".into(),
                content: normalize_model_input_text(message.prompt_text()),
                name: Some(speaker_name(message, "Assistant", false)),
            }),
        }
    }
    if let Some(text) = extra_user {
        messages.push(LlmMessage {
            role: "user".into(),
            content: normalize_model_input_text(text),
            name: Some(user_name.to_string()),
        });
    }
    messages
}

fn speaker_name(message: &Message, fallback: &str, _is_user: bool) -> String {
    message
        .metadata
        .get("name")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback)
        .to_string()
}

pub const COMPRESSION_SYSTEM_PROMPT: &str = "你是一个文本压缩助手，专门为角色扮演/叙事类对话压缩 AI 回复。\n\n压缩规则：\n1. 保留：关键情节推进、角色行为动作、情感变化、重要对话内容、剧情转折点。\n2. 删除：重复的环境描写、冗余的内心独白、过渡性文字、重复的形容词堆砌。\n3. 目标长度：原文的 30%-50%。\n4. 保持原文的人物视角和叙事风格（第一人称保持第一人称，第三人称保持第三人称）。\n5. 保留原文中的关键引语（带引号的对话）的核心内容，可以缩短但不要删除。\n\n输出要求：\n- 直接输出压缩后的文本，不要添加任何前缀、标记、解释、JSON 包装。\n- 不要回答用户的问题或继续对话，只是压缩给定的文本。\n- 不要使用 \"【压缩版】\"、\"摘要：\" 等任何说明性前缀。";

pub fn sanitize_compression(text: &str) -> String {
    let mut cleaned = text.trim().to_string();
    if let Some(stripped) = cleaned.strip_prefix("```") {
        cleaned = stripped
            .trim_start_matches(|ch: char| ch.is_ascii_alphanumeric() || ch == '-')
            .trim_start_matches('\n')
            .to_string();
        if let Some(end) = cleaned.rfind("```") {
            cleaned.truncate(end);
        }
    }
    for prefix in [
        "【压缩版】",
        "压缩版：",
        "压缩后：",
        "摘要：",
        "Summary:",
        "Compressed:",
    ] {
        if let Some(stripped) = cleaned.strip_prefix(prefix) {
            cleaned = stripped.trim().to_string();
        }
    }
    cleaned.trim().to_string()
}
