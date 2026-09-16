use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::AppResult;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPriority {
    Critical,
    Normal,
    Ephemeral,
}

#[derive(Debug, Clone)]
pub struct OutboundText {
    pub chat_id: String,
    pub text: String,
    pub priority: DeliveryPriority,
    pub reply_markup: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct ExternalMessageId {
    pub chat_id: String,
    pub message_id: String,
}

#[async_trait]
pub trait ChannelGateway: Send + Sync {
    async fn send_text(&self, message: OutboundText) -> AppResult<ExternalMessageId>;
    async fn edit_text(&self, id: &ExternalMessageId, text: &str) -> AppResult<()>;
    async fn delete_message(&self, id: &ExternalMessageId) -> AppResult<()>;
}
