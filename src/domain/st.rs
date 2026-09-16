use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HandshakeResponse {
    pub version: String,
    pub handle: String,
    pub profile: String,
    pub connector_mode: StWriteMode,
    pub scopes: Vec<StWriteScope>,
    pub snapshot: bool,
    pub integrity_enabled: bool,
    pub typed_mutations: bool,
    pub integrity_rotation: bool,
    pub operation_replay: bool,
    pub durable_write: bool,
    pub handshake_at_unix: i64,
    pub expires_at_unix: i64,
}

impl HandshakeResponse {
    pub fn is_valid_at(&self, now_unix: i64) -> bool {
        !self.version.trim().is_empty()
            && !self.handle.trim().is_empty()
            && !self.profile.trim().is_empty()
            && self.handshake_at_unix > 0
            && self.expires_at_unix > self.handshake_at_unix
            && now_unix >= self.handshake_at_unix
            && now_unix < self.expires_at_unix
    }

    pub fn supports_scope(&self, scope: StWriteScope) -> bool {
        self.scopes.contains(&scope)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StStatus {
    pub available: bool,
    pub version: Option<String>,
    pub handle: String,
    pub capabilities: StCapabilities,
}

impl StStatus {
    pub fn write_ready(&self) -> bool {
        self.capabilities.mode.allows_write()
            && self.capabilities.snapshot
            && self.capabilities.typed_mutations
            && self.capabilities.integrity_rotation
            && self.capabilities.operation_replay
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct StCharacterSummary {
    pub avatar: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub personality: String,
    #[serde(default)]
    pub scenario: String,
    #[serde(default)]
    pub first_mes: String,
    #[serde(default)]
    pub mes_example: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StChatSummary {
    pub chat_file: String,
    pub title: Option<String>,
    pub updated_at: Option<String>,
    pub message_count: Option<u64>,
}

pub type StChatFileSummary = StChatSummary;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct StChatLocator {
    pub handle: String,
    pub avatar: String,
    pub character_name: String,
    pub chat_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StChatSnapshot {
    pub locator: StChatLocator,
    pub parsed_chat: Vec<Value>,
    pub source_sha256: String,
    pub source_integrity: String,
    pub source_byte_length: u64,
    pub source_message_count: u64,
}

impl StChatSnapshot {
    pub fn source_revision(&self) -> (&str, &str, u64, u64) {
        (
            &self.source_sha256,
            &self.source_integrity,
            self.source_byte_length,
            self.source_message_count,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StModelCatalog {
    pub models: Vec<StModelSummary>,
    pub current_model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StModelSummary {
    pub id: String,
    pub owned_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StGenerationSettings {
    pub username: String,
    pub chat_completion_source: String,
    pub model: String,
    pub custom_url: String,
    pub custom_prompt_post_processing: String,
    pub temperature: f64,
    pub top_p: f64,
    pub max_tokens: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StGenerationRequest {
    pub model_id: Option<String>,
    pub messages: Vec<StPromptMessage>,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub max_tokens: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StPromptMessage {
    pub role: String,
    pub content: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StGenerationResult {
    pub text: String,
    pub finish_reason: Option<String>,
    pub usage: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PollerRuntimeFence {
    pub telegram_bot_id: i64,
    pub owner: String,
    pub epoch: u64,
    pub internal_bot_id: String,
    pub runtime_instance_id: String,
}

impl PollerRuntimeFence {
    pub fn is_valid(&self) -> bool {
        self.telegram_bot_id > 0
            && self.owner == "rust_bridge"
            && self.epoch > 0
            && !self.internal_bot_id.trim().is_empty()
            && !self.runtime_instance_id.trim().is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreateStChat {
    pub operation_id: String,
    pub locator: StChatLocator,
    pub opening_message: Value,
    pub scope: StWriteScope,
    pub fence: PollerRuntimeFence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CommitStChat {
    pub operation_id: String,
    pub locator: StChatLocator,
    pub expected_sha256: String,
    pub expected_integrity: String,
    pub mutation: StMutation,
    pub fence: PollerRuntimeFence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StCommitResult {
    pub status: StCommitStatus,
    pub new_sha256: Option<String>,
    pub new_integrity: Option<String>,
    pub byte_length: Option<u64>,
    pub message_count: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StCommitStatus {
    Applied,
    AlreadyApplied,
    NotApplied,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum StWriteMode {
    Disabled = 0,
    ReadOnly = 1,
    TestWrite = 2,
    ProductionWrite = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WriteFacts {
    pub handshake_valid: bool,
    pub profile_matches: bool,
    pub integrity_enabled: bool,
    pub ownership_valid: bool,
    pub approved_locator: bool,
    pub connector_created_test: bool,
    pub test_name_prefix: bool,
}

impl StWriteMode {
    pub const fn allows_write(self) -> bool {
        matches!(self, Self::TestWrite | Self::ProductionWrite)
    }

    pub const fn allows_read(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "disabled" => Some(Self::Disabled),
            "read_only" => Some(Self::ReadOnly),
            "test_write" => Some(Self::TestWrite),
            "production_write" => Some(Self::ProductionWrite),
            _ => None,
        }
    }

    pub fn effective(self, connector: Self) -> Self {
        self.min(connector)
    }

    pub fn check_write(self, facts: &WriteFacts) -> Result<(), &'static str> {
        if self < Self::TestWrite {
            return Err("ST_WRITE_NOT_READY");
        }
        if !(facts.handshake_valid
            && facts.profile_matches
            && facts.integrity_enabled
            && facts.ownership_valid
            && facts.approved_locator)
        {
            return Err("ST_WRITE_PREFLIGHT_REJECTED");
        }
        if self == Self::TestWrite && !(facts.connector_created_test && facts.test_name_prefix) {
            return Err("ST_TEST_SCOPE_REQUIRED");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StWriteScope {
    TestChat,
    ProductionChat,
}

impl StWriteScope {
    pub const fn mode(self) -> StWriteMode {
        match self {
            Self::TestChat => StWriteMode::TestWrite,
            Self::ProductionChat => StWriteMode::ProductionWrite,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StCapabilities {
    pub mode: StWriteMode,
    pub snapshot: bool,
    pub typed_mutations: bool,
    pub integrity_rotation: bool,
    pub operation_replay: bool,
}

impl Default for StCapabilities {
    fn default() -> Self {
        Self {
            mode: StWriteMode::Disabled,
            snapshot: false,
            typed_mutations: false,
            integrity_rotation: false,
            operation_replay: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StChatMessage {
    pub name: String,
    pub is_user: bool,
    pub mes: String,
    pub send_date: Option<String>,
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StMutation {
    AppendTurn {
        #[serde(rename = "userMessage")]
        user_message: StChatMessage,
        #[serde(rename = "assistantMessage")]
        assistant_message: StChatMessage,
    },
    UndoLastTurn {
        #[serde(rename = "expectedUserSha256")]
        expected_user_sha256: String,
        #[serde(rename = "expectedAssistantSha256")]
        expected_assistant_sha256: String,
    },
    ReplaceLastAssistant {
        #[serde(rename = "expectedAssistantSha256")]
        expected_assistant_sha256: String,
        #[serde(rename = "assistantMessage")]
        assistant_message: StChatMessage,
    },
    CompressMessages {
        patches: Vec<StMessagePatch>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StMessagePatch {
    pub index: u32,
    pub expected_message_sha256: String,
    pub mes: Option<String>,
    pub extra: Option<StCompressionExtraPatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct StCompressionExtraPatch {
    pub display_text: Option<String>,
    pub compressed: Option<bool>,
}
