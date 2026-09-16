use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::AppResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmMessage {
    pub role: String,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedProvider {
    pub id: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub custom_headers: Vec<(String, String)>,
    pub custom_prompt_post_processing: String,
    pub model: String,
    pub temperature: f64,
    pub top_p: f64,
    pub max_tokens: i64,
    pub hard_timeout_ms: u64,
    pub idle_timeout_ms: u64,
}

#[derive(Debug, Clone)]
pub struct LlmRequest {
    pub messages: Vec<LlmMessage>,
    pub stream: bool,
}

#[derive(Debug, Clone, Default)]
pub struct LlmCompletion {
    pub text: String,
    pub finish_reason: Option<String>,
    pub usage_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct ModelDescriptor {
    pub id: String,
    pub owned_by: Option<String>,
    pub description: Option<String>,
}

#[async_trait]
pub trait ProgressSink: Send + Sync {
    async fn emit(&self, event: ProgressEvent) -> AppResult<()>;
}

#[derive(Debug, Clone)]
pub enum ProgressEvent {
    Started {
        operation_id: String,
        conversation_id: String,
        revision: i64,
    },
    Delta {
        text: String,
        full_text: String,
    },
    Progress {
        completed: u32,
        total: u32,
    },
    Done {
        reply_text: String,
        conversation_revision: i64,
    },
    Error {
        message: String,
    },
}

#[async_trait]
pub trait LlmGateway: Send + Sync {
    async fn list_models(&self, provider: &ResolvedProvider) -> AppResult<Vec<ModelDescriptor>>;
    async fn stream_chat(
        &self,
        provider: &ResolvedProvider,
        request: LlmRequest,
        progress: Option<&dyn ProgressSink>,
    ) -> AppResult<LlmCompletion>;
}

pub struct NoopProgress;

#[async_trait]
impl ProgressSink for NoopProgress {
    async fn emit(&self, _event: ProgressEvent) -> AppResult<()> {
        Ok(())
    }
}
