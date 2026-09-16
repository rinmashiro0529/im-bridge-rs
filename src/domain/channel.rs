use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelContext {
    pub account_id: String,
    pub channel: String,
    pub external_context_key: String,
    pub workspace_id: Option<String>,
    pub character_id: Option<String>,
    pub conversation_id: Option<String>,
    pub chat_model_preset_id: Option<String>,
    pub compression_model_preset_id: Option<String>,
    pub st_handle: Option<String>,
    pub st_character_avatar: Option<String>,
    pub st_character_name: Option<String>,
    pub st_chat_file: Option<String>,
    pub chat_model_id: Option<String>,
    pub compression_model_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StChannelLocator {
    pub handle: Option<String>,
    pub avatar: Option<String>,
    pub character_name: Option<String>,
    pub chat_file: Option<String>,
    pub chat_model_id: Option<String>,
    pub compression_model_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramBot {
    pub id: String,
    pub workspace_id: String,
    pub owner_account_id: String,
    pub token_secret_id: Option<String>,
    pub desired_enabled: bool,
    pub observed_username: Option<String>,
    pub last_error: Option<String>,
    pub inter_message_delay_ms: i64,
    pub stream_min_interval_ms: i64,
    pub stream_min_delta_chars: i64,
    pub stream_first_render_chars: i64,
    pub stream_chunk_size: i64,
}
