use std::collections::VecDeque;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::{AppError, AppResult};
use crate::seams::llm_gateway::{
    LlmCompletion, LlmGateway, LlmRequest, ModelDescriptor, ProgressEvent, ProgressSink,
    ResolvedProvider,
};

pub struct FakeLlm {
    replies: Mutex<VecDeque<String>>,
    fail_next: Mutex<bool>,
}

impl FakeLlm {
    pub fn new(replies: Vec<String>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            fail_next: Mutex::new(false),
        }
    }

    pub fn push_reply(&self, reply: impl Into<String>) {
        self.replies.lock().expect("lock").push_back(reply.into());
    }

    pub fn fail_next(&self) {
        *self.fail_next.lock().expect("lock") = true;
    }
}

#[async_trait]
impl LlmGateway for FakeLlm {
    async fn list_models(&self, _provider: &ResolvedProvider) -> AppResult<Vec<ModelDescriptor>> {
        Ok(vec![ModelDescriptor {
            id: "fake-model".into(),
            owned_by: Some("fake".into()),
            description: Some("deterministic test model".into()),
        }])
    }

    async fn stream_chat(
        &self,
        _provider: &ResolvedProvider,
        _request: LlmRequest,
        progress: Option<&dyn ProgressSink>,
    ) -> AppResult<LlmCompletion> {
        if *self.fail_next.lock().expect("lock") {
            *self.fail_next.lock().expect("lock") = false;
            return Err(AppError::bad_gateway(
                "GENERATE_FAILED",
                "fake provider failed",
            ));
        }
        let text = self
            .replies
            .lock()
            .expect("lock")
            .pop_front()
            .unwrap_or_else(|| "角色继续说话。".to_string());
        if let Some(progress) = progress {
            let mut full = String::new();
            for ch in text.chars() {
                full.push(ch);
                progress
                    .emit(ProgressEvent::Delta {
                        text: ch.to_string(),
                        full_text: full.clone(),
                    })
                    .await?;
            }
        }
        Ok(LlmCompletion {
            text,
            finish_reason: Some("stop".into()),
            usage_json: None,
        })
    }
}
