use async_trait::async_trait;

use crate::domain::st::{
    CommitStChat, CreateStChat, StCharacterSummary, StChatLocator, StChatSnapshot, StChatSummary,
    StCommitResult, StGenerationRequest, StGenerationResult, StGenerationSettings, StModelCatalog,
    StStatus,
};
use crate::modules::bridge::errors::StResult;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait StBackend: Send + Sync {
    async fn probe(&self) -> StResult<StStatus>;

    async fn list_characters(&self) -> StResult<Vec<StCharacterSummary>>;

    async fn list_chats(&self, avatar: &str) -> StResult<Vec<StChatSummary>>;

    async fn snapshot(&self, locator: &StChatLocator) -> StResult<StChatSnapshot>;

    async fn list_models(&self) -> StResult<StModelCatalog>;

    async fn generation_settings(&self) -> StResult<StGenerationSettings>;

    async fn stream_generate(
        &self,
        request: StGenerationRequest,
        progress: Option<std::sync::Arc<dyn crate::seams::bridge_progress::BridgeProgressSink>>,
        cancel: CancellationToken,
    ) -> StResult<StGenerationResult>;

    async fn create_chat(&self, command: CreateStChat) -> StResult<StCommitResult>;

    async fn commit(&self, command: CommitStChat) -> StResult<StCommitResult>;
}
