use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    System,
    User,
    Assistant,
}

impl MessageRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "system" => Some(Self::System),
            "user" => Some(Self::User),
            "assistant" => Some(Self::Assistant),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageStatus {
    Active,
    Revoked,
    Superseded,
}

impl MessageStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Revoked => "revoked",
            Self::Superseded => "superseded",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Pending,
    Complete,
    Failed,
    Revoked,
}

impl TurnStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Revoked => "revoked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationStatus {
    Started,
    Streaming,
    Completed,
    Failed,
    Interrupted,
}

impl GenerationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Streaming => "streaming",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "started" => Some(Self::Started),
            "streaming" => Some(Self::Streaming),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "interrupted" => Some(Self::Interrupted),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversation {
    pub id: String,
    pub workspace_id: String,
    pub character_id: String,
    pub pinned_character_revision_id: Option<String>,
    pub title: String,
    pub prompt_profile: String,
    pub revision: i64,
    pub default_model_preset_id: Option<String>,
    pub archived: bool,
    pub legacy_locator: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Turn {
    pub id: String,
    pub conversation_id: String,
    pub ordinal: i64,
    pub status: TurnStatus,
    pub created_by: String,
    pub active_assistant_message_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub conversation_id: String,
    pub turn_id: Option<String>,
    pub sequence: i64,
    pub role: MessageRole,
    pub content_original: String,
    pub prompt_content: Option<String>,
    pub status: MessageStatus,
    pub supersedes_id: Option<String>,
    pub metadata: Value,
    pub source_raw_json: Option<Value>,
}

impl Message {
    pub fn prompt_text(&self) -> &str {
        self.prompt_content
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&self.content_original)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationRun {
    pub id: String,
    pub conversation_id: String,
    pub turn_id: Option<String>,
    pub actor_id: String,
    pub channel: String,
    pub external_context_key: String,
    pub operation_kind: String,
    pub client_turn_id: Option<String>,
    pub status: GenerationStatus,
    pub effective_character_revision_id: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub partial_content: Option<String>,
    pub result_json: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryItem {
    pub message_id: String,
    pub turn_id: Option<String>,
    pub speaker: String,
    pub text: String,
    pub send_date: Option<String>,
    pub is_user: bool,
    pub sort_index: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryView {
    pub conversation_id: String,
    pub revision: i64,
    pub mode: String,
    pub items: Vec<HistoryItem>,
}
