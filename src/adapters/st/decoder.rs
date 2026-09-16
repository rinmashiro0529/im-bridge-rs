use serde_json::Value;

use crate::domain::st::{
    StCharacterSummary, StChatSummary, StGenerationSettings, StModelCatalog, StModelSummary,
};
use crate::modules::bridge::error_mapper::{map_st_error, StErrorFacts, StFailureFacts};
use crate::modules::bridge::errors::{CommitState, StBridgeError, StErrorStage, StResult};

pub fn decode_character_summaries(payload: &Value) -> StResult<Vec<StCharacterSummary>> {
    let Some(items) = payload.as_array() else {
        return Err(decode_error(
            StErrorStage::Catalog,
            Some("characters"),
            StFailureFacts::DecodeInvalidPayload,
        ));
    };
    let mut characters = Vec::new();
    for item in items {
        let avatar = item
            .get("avatar")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if avatar.is_empty() {
            continue;
        }
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                item.get("data")
                    .and_then(|data| data.get("name"))
                    .and_then(Value::as_str)
            })
            .unwrap_or("Unknown");
        characters.push(StCharacterSummary {
            avatar: avatar.to_string(),
            name: name.to_string(),
            description: string_field(item, "description"),
            personality: string_field(item, "personality"),
            scenario: string_field(item, "scenario"),
            first_mes: string_field(item, "first_mes"),
            mes_example: string_field(item, "mes_example"),
        });
    }
    Ok(characters)
}

pub fn decode_chat_summaries(payload: &Value) -> StResult<Vec<StChatSummary>> {
    let Some(items) = payload.as_array() else {
        return Err(decode_error(
            StErrorStage::Catalog,
            Some("chats"),
            StFailureFacts::DecodeInvalidPayload,
        ));
    };
    let mut chats = Vec::new();
    for item in items {
        let file_name = item
            .get("file_name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if file_name.is_empty() {
            continue;
        }
        if is_hidden_chat_file(file_name) {
            continue;
        }
        chats.push(StChatSummary {
            chat_file: normalize_chat_file_name(file_name),
            title: Some(file_name.trim_end_matches(".jsonl").to_string()),
            updated_at: item
                .get("last_mes")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            message_count: item.get("message_count").and_then(Value::as_u64),
        });
    }
    Ok(chats)
}

pub fn decode_generation_settings(payload: &Value) -> StResult<StGenerationSettings> {
    let settings = parse_settings_object(payload)?;
    let oai = settings.get("oai_settings").cloned().unwrap_or(Value::Null);
    let source = oai
        .get("chat_completion_source")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or("custom")
        .to_string();
    let custom = oai
        .get("custom_model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let openai = oai
        .get("openai_model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let model = custom.or(openai).unwrap_or_default().to_string();
    if model.is_empty() {
        return Err(decode_error(
            StErrorStage::Snapshot,
            Some("settings"),
            StFailureFacts::DecodeSettings,
        ));
    }
    Ok(StGenerationSettings {
        username: settings
            .get("username")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("User")
            .to_string(),
        chat_completion_source: source,
        model,
        custom_url: oai
            .get("custom_url")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        custom_prompt_post_processing: oai
            .get("custom_prompt_post_processing")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        temperature: oai
            .get("temp_openai")
            .and_then(Value::as_f64)
            .unwrap_or(1.0),
        top_p: oai
            .get("top_p_openai")
            .and_then(Value::as_f64)
            .unwrap_or(1.0),
        max_tokens: oai
            .get("openai_max_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(1024) as u32,
    })
}

pub fn decode_settings_model(payload: &Value) -> StResult<Option<String>> {
    let settings = parse_settings_object(payload)?;
    let oai = settings.get("oai_settings").cloned().unwrap_or(Value::Null);
    let custom = oai
        .get("custom_model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let openai = oai
        .get("openai_model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    Ok(custom.or(openai).map(ToOwned::to_owned))
}

pub fn decode_model_catalog(payload: &Value, current_model: Option<String>) -> StModelCatalog {
    let data = payload
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let models = data
        .into_iter()
        .filter_map(|item| {
            let id = item.get("id").and_then(Value::as_str)?.trim();
            if id.is_empty() {
                return None;
            }
            Some(StModelSummary {
                id: id.to_string(),
                owned_by: item
                    .get("owned_by")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            })
        })
        .collect();
    StModelCatalog {
        models,
        current_model,
    }
}

pub fn decode_chat_messages(payload: &Value) -> StResult<Vec<Value>> {
    payload.as_array().cloned().ok_or_else(|| {
        decode_error(
            StErrorStage::Snapshot,
            Some("chats/get"),
            StFailureFacts::DecodeInvalidPayload,
        )
    })
}

pub fn normalize_chat_file_name(file_name: &str) -> String {
    let trimmed = file_name.trim();
    if trimmed.ends_with(".jsonl") || trimmed.is_empty() {
        trimmed.to_string()
    } else {
        format!("{trimmed}.jsonl")
    }
}

pub fn is_hidden_chat_file(file_name: &str) -> bool {
    let lowered = file_name.to_ascii_lowercase();
    lowered.contains(".pre_compress_") || lowered.contains("backup")
}

fn parse_settings_object(payload: &Value) -> StResult<Value> {
    let settings_text = payload
        .get("settings")
        .and_then(Value::as_str)
        .unwrap_or("");
    if settings_text.is_empty() {
        return Err(decode_error(
            StErrorStage::Snapshot,
            Some("settings"),
            StFailureFacts::DecodeSettings,
        ));
    }
    serde_json::from_str(settings_text).map_err(|_| {
        decode_error(
            StErrorStage::Snapshot,
            Some("settings"),
            StFailureFacts::DecodeSettings,
        )
    })
}

fn string_field(item: &Value, key: &str) -> String {
    item.get(key)
        .and_then(Value::as_str)
        .or_else(|| {
            item.get("data")
                .and_then(|data| data.get(key))
                .and_then(Value::as_str)
        })
        .unwrap_or("")
        .to_string()
}

fn decode_error(
    stage: StErrorStage,
    endpoint_class: Option<&str>,
    failure: StFailureFacts,
) -> Box<StBridgeError> {
    map_st_error(StErrorFacts {
        stage,
        endpoint_class: endpoint_class.map(ToOwned::to_owned),
        operation_id: None,
        commit_state: CommitState::NotStarted,
        attempt: 1,
        duration_ms: None,
        failure,
    })
}
