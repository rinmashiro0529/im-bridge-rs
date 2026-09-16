use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::domain::identity::Actor;
use crate::domain::st::{StCharacterSummary, StChatLocator, StChatSummary, StModelCatalog};
use crate::modules::bridge::errors::StResult;

pub type ModelInfo = crate::domain::st::StModelSummary;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeOperationOrigin {
    pub internal_bot_id: String,
    pub telegram_update_id: i64,
    pub channel_context_key: String,
}

impl BridgeOperationOrigin {
    pub fn is_valid(&self) -> bool {
        !self.internal_bot_id.trim().is_empty()
            && self.telegram_update_id > 0
            && !self.channel_context_key.trim().is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StBridgeCommandEnvelope {
    pub command: StBridgeCommand,
    pub origin: BridgeOperationOrigin,
}

#[async_trait]
pub trait StBridgeEngine: Send + Sync {
    async fn execute(&self, actor: &Actor, command: StBridgeCommand) -> StResult<StBridgeOutcome>;

    async fn execute_enveloped(
        &self,
        actor: &Actor,
        request: StBridgeCommandEnvelope,
    ) -> StResult<StBridgeOutcome> {
        self.execute_with_origin(actor, request.command, request.origin)
            .await
    }

    async fn execute_enveloped_with_context(
        &self,
        actor: &Actor,
        request: StBridgeCommandEnvelope,
        context: Option<crate::seams::bridge_progress::BridgeExecutionContext>,
    ) -> StResult<StBridgeOutcome> {
        self.execute_with_context_and_origin(actor, request.command, context, request.origin)
            .await
    }

    async fn execute_with_origin(
        &self,
        actor: &Actor,
        command: StBridgeCommand,
        origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome>;

    async fn execute_with_context_and_origin(
        &self,
        actor: &Actor,
        command: StBridgeCommand,
        context: Option<crate::seams::bridge_progress::BridgeExecutionContext>,
        origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome>;

    async fn execute_with_context(
        &self,
        actor: &Actor,
        command: StBridgeCommand,
        context: Option<crate::seams::bridge_progress::BridgeExecutionContext>,
    ) -> StResult<StBridgeOutcome> {
        if context.is_some() {
            return Err(crate::modules::bridge::errors::StBridgeError::boxed(
                crate::modules::bridge::errors::StErrorCode::StWriteNotReady,
                crate::modules::bridge::errors::StErrorStage::Control,
                "execution context is not supported by this bridge engine implementation",
                false,
                crate::modules::bridge::errors::CommitState::NotStarted,
            ));
        }
        self.execute(actor, command).await
    }

    async fn query(&self, actor: &Actor, query: StBridgeQuery) -> StResult<StBridgeView>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StBridgeCommand {
    SelectCharacter {
        avatar: String,
        #[serde(rename = "characterName")]
        character_name: String,
    },
    SelectChat {
        locator: StChatLocator,
    },
    StartChat {
        locator: StChatLocator,
        #[serde(rename = "clientOperationId")]
        client_operation_id: String,
    },
    SendMessage {
        locator: StChatLocator,
        text: String,
        #[serde(rename = "clientOperationId")]
        client_operation_id: String,
        #[serde(rename = "modelOverride")]
        model_override: Option<String>,
    },
    UndoLastTurn {
        locator: StChatLocator,
        #[serde(rename = "clientOperationId")]
        client_operation_id: String,
    },
    RevokeLastTurn {
        locator: StChatLocator,
        #[serde(rename = "clientOperationId")]
        client_operation_id: String,
    },
    RegenerateReply {
        locator: StChatLocator,
        #[serde(rename = "clientOperationId")]
        client_operation_id: String,
        #[serde(rename = "modelOverride")]
        model_override: Option<String>,
    },
    CompressChat {
        locator: StChatLocator,
        #[serde(rename = "clientOperationId")]
        client_operation_id: String,
    },
    SetModelOverride {
        kind: ModelOverrideKind,
        #[serde(rename = "modelId")]
        model_id: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelOverrideKind {
    Chat,
    Compression,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StBridgeQuery {
    ListCharacters,
    ListCharacterChats {
        avatar: String,
    },
    ListRecentChats,
    GetChannelContext,
    GetHistory {
        locator: StChatLocator,
    },
    GetLastTurn {
        locator: StChatLocator,
    },
    GetModels,
    GetOperation {
        #[serde(rename = "operationId")]
        operation_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct StBridgeOutcome {
    pub operation_id: Option<String>,
    pub reply_text: Option<String>,
    pub write_committed: bool,
    #[serde(default)]
    pub confirmed_commit: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed_safe_content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub st_tail_fingerprint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StBridgeView {
    Characters(Vec<StCharacterSummary>),
    Chats(Vec<StChatSummary>),
    Context {
        locator: Option<StChatLocator>,
        #[serde(rename = "chatModelId")]
        chat_model_id: Option<String>,
        #[serde(rename = "compressionModelId")]
        compression_model_id: Option<String>,
    },
    History {
        preview: String,
    },
    LastTurn {
        user: Option<String>,
        assistant: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tail_fingerprint: Option<String>,
    },
    Models {
        catalog: StModelCatalog,
        #[serde(rename = "overrideChat")]
        override_chat: Option<String>,
        #[serde(rename = "overrideCompression")]
        override_compression: Option<String>,
    },
    Operation {
        id: String,
        status: String,
    },
    Empty,
}
