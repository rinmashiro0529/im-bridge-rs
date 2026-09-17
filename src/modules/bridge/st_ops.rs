use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::clock::now_rfc3339;
use crate::domain::character::NormalizedCardFields;
use crate::domain::st::{
    StCharacterSummary, StChatMessage, StChatSnapshot, StCompressionExtraPatch, StMessagePatch,
    StMutation, StPromptMessage,
};
use crate::modules::chat::prompt::{
    build_system_prompt, normalize_assistant_reply, normalize_model_input_text,
    sanitize_compression, COMPRESSION_SYSTEM_PROMPT,
};

pub use crate::domain::locator::locator_hash;

const RECENT_LIMIT: usize = 24;
pub const COMPRESS_KEEP_RECENT: usize = 15;

pub fn message_sha256(value: &Value) -> String {
    let encoded = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    hex::encode(Sha256::digest(encoded))
}

pub fn mutation_digest(mutation: &StMutation) -> String {
    let encoded = serde_json::to_vec(mutation).unwrap_or_else(|_| b"{}".to_vec());
    hex::encode(Sha256::digest(encoded))
}

pub fn last_dialogue_indexes(messages: &[Value]) -> Option<(usize, usize)> {
    let mut assistant = None;
    let mut user = None;
    for index in (1..messages.len()).rev() {
        let item = &messages[index];
        if item.get("is_system").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        if assistant.is_none() && item.get("is_user").and_then(Value::as_bool) != Some(true) {
            assistant = Some(index);
            continue;
        }
        if assistant.is_some() && item.get("is_user").and_then(Value::as_bool) == Some(true) {
            user = Some(index);
            break;
        }
    }
    Some((user?, assistant?))
}

pub fn last_turn_hashes(messages: &[Value]) -> Option<(String, String)> {
    let (user, assistant) = last_dialogue_indexes(messages)?;
    Some((
        message_sha256(&messages[user]),
        message_sha256(&messages[assistant]),
    ))
}

pub fn visible_text(item: &Value) -> String {
    item.get("extra")
        .and_then(|extra| extra.get("display_text"))
        .and_then(Value::as_str)
        .or_else(|| item.get("mes").and_then(Value::as_str))
        .unwrap_or("")
        .trim()
        .to_string()
}

pub fn chat_message(name: &str, is_user: bool, text: &str) -> StChatMessage {
    StChatMessage {
        name: name.to_string(),
        is_user,
        mes: text.to_string(),
        send_date: Some(now_rfc3339()),
        extra: Default::default(),
    }
}

pub fn card_fields(character: &StCharacterSummary) -> NormalizedCardFields {
    NormalizedCardFields {
        name: character.name.clone(),
        description: character.description.clone(),
        personality: character.personality.clone(),
        scenario: character.scenario.clone(),
        first_mes: character.first_mes.clone(),
        mes_example: character.mes_example.clone(),
        system_prompt: String::new(),
        post_history_instructions: String::new(),
    }
}

pub fn prompt_messages(
    character: &StCharacterSummary,
    user_name: &str,
    parsed_chat: &[Value],
    extra_user: Option<&str>,
    drop_last_assistant: bool,
) -> Vec<StPromptMessage> {
    let system = build_system_prompt(&card_fields(character), user_name);
    let mut history = Vec::new();
    let mut digest = None;
    for item in parsed_chat.iter().skip(1) {
        let text = visible_text(item);
        if text.is_empty() {
            continue;
        }
        if text.starts_with("[CompressionDigest]") {
            digest = Some(StPromptMessage {
                role: "system".into(),
                content: normalize_model_input_text(&text),
                name: None,
            });
            continue;
        }
        if item.get("is_system").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        let is_user = item.get("is_user").and_then(Value::as_bool) == Some(true);
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or(if is_user {
                user_name
            } else {
                character.name.as_str()
            })
            .to_string();
        history.push(StPromptMessage {
            role: if is_user {
                "user".into()
            } else {
                "assistant".into()
            },
            content: normalize_model_input_text(&text),
            name: Some(name),
        });
    }
    if drop_last_assistant {
        if let Some(index) = history.iter().rposition(|item| item.role == "assistant") {
            history.remove(index);
        }
    }
    if history.len() > RECENT_LIMIT {
        history = history.split_off(history.len() - RECENT_LIMIT);
    }
    let mut messages = vec![StPromptMessage {
        role: "system".into(),
        content: system,
        name: None,
    }];
    if let Some(digest) = digest {
        messages.push(digest);
    }
    messages.extend(history);
    if let Some(text) = extra_user {
        messages.push(StPromptMessage {
            role: "user".into(),
            content: normalize_model_input_text(text),
            name: Some(user_name.to_string()),
        });
    }
    messages
}

pub fn compression_messages(text: &str) -> Vec<StPromptMessage> {
    vec![
        StPromptMessage {
            role: "system".into(),
            content: COMPRESSION_SYSTEM_PROMPT.to_string(),
            name: None,
        },
        StPromptMessage {
            role: "user".into(),
            content: text.to_string(),
            name: None,
        },
    ]
}

pub fn undo_mutation(snapshot: &StChatSnapshot) -> Option<StMutation> {
    let (expected_user_sha256, expected_assistant_sha256) =
        last_turn_hashes(&snapshot.parsed_chat)?;
    Some(StMutation::UndoLastTurn {
        expected_user_sha256,
        expected_assistant_sha256,
    })
}

pub fn append_mutation(
    user_name: &str,
    character_name: &str,
    user_text: &str,
    assistant_text: &str,
) -> StMutation {
    StMutation::AppendTurn {
        user_message: chat_message(user_name, true, user_text),
        assistant_message: chat_message(
            character_name,
            false,
            &normalize_assistant_reply(character_name, assistant_text),
        ),
    }
}

pub fn replace_assistant_mutation(
    snapshot: &StChatSnapshot,
    character_name: &str,
    assistant_text: &str,
) -> Option<StMutation> {
    let (_, assistant) = last_dialogue_indexes(&snapshot.parsed_chat)?;
    Some(StMutation::ReplaceLastAssistant {
        expected_assistant_sha256: message_sha256(&snapshot.parsed_chat[assistant]),
        assistant_message: chat_message(
            character_name,
            false,
            &normalize_assistant_reply(character_name, assistant_text),
        ),
    })
}

pub fn compress_targets(parsed_chat: &[Value]) -> Vec<(u32, String, String)> {
    let keep_from = parsed_chat
        .iter()
        .enumerate()
        .rev()
        .filter(|(_, item)| {
            item.get("is_system").and_then(Value::as_bool) != Some(true)
                && !visible_text(item).is_empty()
        })
        .take(COMPRESS_KEEP_RECENT)
        .last()
        .map(|(index, _)| index)
        .unwrap_or(usize::MAX);
    parsed_chat
        .iter()
        .enumerate()
        .filter(|(index, item)| {
            *index >= 1
                && *index < keep_from
                && item.get("is_user").and_then(Value::as_bool) != Some(true)
                && item.get("is_system").and_then(Value::as_bool) != Some(true)
        })
        .filter_map(|(index, item)| {
            let text = visible_text(item);
            if text.is_empty() || text.starts_with("[CompressionDigest]") {
                return None;
            }
            if item
                .get("extra")
                .and_then(|extra| extra.get("compressed"))
                .and_then(Value::as_bool)
                == Some(true)
            {
                return None;
            }
            Some((index as u32, message_sha256(item), text))
        })
        .collect()
}

pub fn compress_patch(index: u32, expected_sha256: String, compressed: &str) -> StMessagePatch {
    let cleaned = sanitize_compression(compressed);
    StMessagePatch {
        index,
        expected_message_sha256: expected_sha256,
        mes: Some(cleaned.clone()),
        extra: Some(StCompressionExtraPatch {
            display_text: Some(cleaned),
            compressed: Some(true),
        }),
    }
}

pub fn generate_text_from_payload(payload: &Value) -> Option<String> {
    payload
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)
        .or_else(|| {
            payload
                .pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
        })
        .or_else(|| payload.get("content").and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub fn generate_payload(
    settings: &crate::domain::st::StGenerationSettings,
    request: &crate::domain::st::StGenerationRequest,
) -> Value {
    let model = request
        .model_id
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| settings.model.clone());
    let mut payload = json!({
        "chat_completion_source": settings.chat_completion_source,
        "model": model,
        "messages": request.messages,
        "temperature": request.temperature.unwrap_or(settings.temperature),
        "top_p": request.top_p.unwrap_or(settings.top_p),
        "max_tokens": request.max_tokens.unwrap_or(settings.max_tokens),
        "stream": false,
    });
    if settings.chat_completion_source == "custom" {
        payload["custom_url"] = json!(settings.custom_url);
        payload["custom_prompt_post_processing"] = json!(settings.custom_prompt_post_processing);
        payload["custom_include_body"] = json!("");
        payload["custom_include_headers"] = json!("");
        payload["custom_exclude_body"] = json!("");
    }
    payload
}

pub fn generate_stream_payload(
    settings: &crate::domain::st::StGenerationSettings,
    request: &crate::domain::st::StGenerationRequest,
) -> Value {
    let mut payload = generate_payload(settings, request);
    payload["stream"] = json!(true);
    payload
}
