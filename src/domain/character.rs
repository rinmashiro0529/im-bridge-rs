use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Character {
    pub id: String,
    pub workspace_id: String,
    pub display_name: String,
    pub current_revision_id: Option<String>,
    pub archived: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CharacterRevision {
    pub id: String,
    pub character_id: String,
    pub source_format: String,
    pub spec: Option<String>,
    pub spec_version: Option<String>,
    pub normalized: NormalizedCardFields,
    pub raw_card_json: Value,
    pub checksum: String,
    pub compatibility_warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NormalizedCardFields {
    pub name: String,
    pub description: String,
    pub personality: String,
    pub scenario: String,
    pub first_mes: String,
    pub mes_example: String,
    pub system_prompt: String,
    pub post_history_instructions: String,
}

impl NormalizedCardFields {
    pub fn from_raw(raw: &Value) -> Self {
        let data = raw.get("data").unwrap_or(raw);
        Self {
            name: string_field(data, &["name"]).unwrap_or_default(),
            description: string_field(data, &["description"]).unwrap_or_default(),
            personality: string_field(data, &["personality"]).unwrap_or_default(),
            scenario: string_field(data, &["scenario"]).unwrap_or_default(),
            first_mes: string_field(data, &["first_mes", "firstMes"]).unwrap_or_default(),
            mes_example: string_field(data, &["mes_example", "mesExample"]).unwrap_or_default(),
            system_prompt: string_field(data, &["system_prompt"]).unwrap_or_default(),
            post_history_instructions: string_field(data, &["post_history_instructions"])
                .unwrap_or_default(),
        }
    }
}

fn string_field(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text.to_string());
        }
    }
    None
}
