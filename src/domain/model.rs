use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderProfile {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub kind: String,
    pub base_url: String,
    pub api_key_secret_id: Option<String>,
    pub custom_headers_secret_id: Option<String>,
    pub custom_prompt_post_processing: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelPreset {
    pub id: String,
    pub workspace_id: String,
    pub provider_profile_id: String,
    pub model_id: String,
    pub label: String,
    pub temperature: f64,
    pub top_p: f64,
    pub max_tokens: i64,
    pub hard_timeout_ms: i64,
    pub idle_timeout_ms: i64,
    pub purpose: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDescriptor {
    pub id: String,
    pub owned_by: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceSettings {
    pub workspace_id: String,
    pub prompt_user_name: String,
    pub default_prompt_profile: String,
    pub default_chat_model_preset_id: Option<String>,
    pub default_compression_model_preset_id: Option<String>,
}
