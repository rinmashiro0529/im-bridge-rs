use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmedCommit {
    Applied,
    AlreadyApplied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeProgressEvent {
    Started {
        total: Option<u32>,
    },
    Delta {
        sequence: u64,
        text: String,
        full_text: String,
    },
    Progress {
        completed: u32,
        total: u32,
    },
    Done {
        reply_text: String,
        commit: ConfirmedCommit,
    },
    Error {
        safe_code: String,
        safe_message: String,
    },
}

#[async_trait::async_trait]
pub trait BridgeProgressSink: Send + Sync {
    async fn emit(
        &self,
        event: BridgeProgressEvent,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

pub struct BridgeExecutionContext {
    pub operation_id: String,
    pub progress: Option<Arc<dyn BridgeProgressSink>>,
    pub cancel: CancellationToken,
}

impl BridgeExecutionContext {
    pub fn new(
        operation_id: impl Into<String>,
        progress: Option<Arc<dyn BridgeProgressSink>>,
    ) -> Self {
        Self::with_cancel(operation_id, progress, CancellationToken::new())
    }

    pub fn with_cancel(
        operation_id: impl Into<String>,
        progress: Option<Arc<dyn BridgeProgressSink>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            operation_id: operation_id.into(),
            progress,
            cancel,
        }
    }
}
