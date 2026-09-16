use serde::{Deserialize, Serialize};

use crate::domain::st::{CreateStChat, PollerRuntimeFence, StChatLocator, StMutation};
use crate::error::{AppError, AppResult};

pub use BridgeOperationStatus as OperationStage;

pub const KIND_SEND: &str = "send";
pub const KIND_UNDO: &str = "undo";
pub const KIND_REVOKE: &str = "revoke";
pub const KIND_START: &str = "start";
pub const KIND_REGENERATE: &str = "regenerate";
pub const KIND_COMPRESS: &str = "compress";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeOperationStatus {
    Received,
    SnapshotReady,
    Generating,
    Generated,
    Committing,
    Committed,
    Delivered,
    Conflict,
    Failed,
    Interrupted,
}

impl BridgeOperationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::SnapshotReady => "snapshot_ready",
            Self::Generating => "generating",
            Self::Generated => "generated",
            Self::Committing => "committing",
            Self::Committed => "committed",
            Self::Delivered => "delivered",
            Self::Conflict => "conflict",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "received" => Some(Self::Received),
            "snapshot_ready" => Some(Self::SnapshotReady),
            "generating" => Some(Self::Generating),
            "generated" => Some(Self::Generated),
            "committing" => Some(Self::Committing),
            "committed" => Some(Self::Committed),
            "delivered" => Some(Self::Delivered),
            "conflict" => Some(Self::Conflict),
            "failed" => Some(Self::Failed),
            "interrupted" => Some(Self::Interrupted),
            _ => None,
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Delivered | Self::Conflict | Self::Failed | Self::Interrupted
        )
    }

    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Received, Self::SnapshotReady)
                | (Self::SnapshotReady, Self::Generating)
                | (Self::SnapshotReady, Self::Generated)
                | (Self::Generating, Self::Generated)
                | (Self::Generated, Self::Committing)
                | (Self::Committing, Self::Committed)
                | (Self::Committed, Self::Delivered)
                | (Self::Received, Self::Failed)
                | (Self::SnapshotReady, Self::Failed)
                | (Self::Generating, Self::Failed)
                | (Self::Generating, Self::Interrupted)
                | (Self::Generated, Self::Failed)
                | (Self::Committing, Self::Conflict)
                | (Self::Committing, Self::Failed)
                | (Self::Committing, Self::Interrupted)
        )
    }

    pub fn transition(self, next: Self) -> AppResult<Self> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(AppError::conflict(
                "BRIDGE_OPERATION_INVALID_TRANSITION",
                format!(
                    "cannot transition from {} to {}",
                    self.as_str(),
                    next.as_str()
                ),
            ))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationCommitState {
    NotStarted,
    NotApplied,
    Applied,
    Unknown,
}

impl OperationCommitState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::NotApplied => "not_applied",
            Self::Applied => "applied",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "not_started" => Some(Self::NotStarted),
            "not_applied" => Some(Self::NotApplied),
            "applied" => Some(Self::Applied),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

/// Minimal, typed recovery payload.  Raw prompt/snapshot data is intentionally
/// not represented here; only the mutation needed to replay the same operation
/// is retained.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OperationRecoveryPayload {
    Append {
        mutation: StMutation,
        expected_sha256: String,
        expected_integrity: String,
        fence: PollerRuntimeFence,
    },
    Replace {
        mutation: StMutation,
        expected_sha256: String,
        expected_integrity: String,
        fence: PollerRuntimeFence,
    },
    Undo {
        mutation: StMutation,
        expected_sha256: String,
        expected_integrity: String,
        fence: PollerRuntimeFence,
    },
    Create {
        command: CreateStChat,
    },
    Compress {
        mutation: StMutation,
        expected_sha256: String,
        expected_integrity: String,
        fence: PollerRuntimeFence,
    },
}

impl OperationRecoveryPayload {
    pub fn mutation(&self) -> Option<&StMutation> {
        match self {
            Self::Append { mutation, .. }
            | Self::Replace { mutation, .. }
            | Self::Undo { mutation, .. }
            | Self::Compress { mutation, .. } => Some(mutation),
            Self::Create { .. } => None,
        }
    }

    pub fn validate(&self) -> AppResult<()> {
        let invalid = || {
            Err(AppError::bad_request(
                "PAYLOAD_RECOVERY_INVALID",
                "recovery payload mutation kind is not allowed",
            ))
        };
        let fence = match self {
            Self::Create { command } => &command.fence,
            Self::Append { fence, .. }
            | Self::Replace { fence, .. }
            | Self::Undo { fence, .. }
            | Self::Compress { fence, .. } => fence,
        };
        if !fence.is_valid() {
            return Err(AppError::bad_request(
                "PAYLOAD_FENCE_INVALID",
                "operation recovery payload runtime fence is invalid",
            ));
        }
        match self {
            Self::Create { command } if command.operation_id.trim().is_empty() => {
                Err(AppError::bad_request(
                    "PAYLOAD_RECOVERY_INVALID",
                    "create operation id is required",
                ))
            }
            Self::Append {
                mutation: StMutation::AppendTurn { .. },
                expected_sha256,
                expected_integrity,
                ..
            }
            | Self::Replace {
                mutation: StMutation::ReplaceLastAssistant { .. },
                expected_sha256,
                expected_integrity,
                ..
            }
            | Self::Undo {
                mutation: StMutation::UndoLastTurn { .. },
                expected_sha256,
                expected_integrity,
                ..
            }
            | Self::Compress {
                mutation: StMutation::CompressMessages { .. },
                expected_sha256,
                expected_integrity,
                ..
            } if expected_sha256.trim().is_empty() || expected_integrity.trim().is_empty() => {
                Err(AppError::bad_request(
                    "PAYLOAD_RECOVERY_INVALID",
                    "recovery snapshot identity is required",
                ))
            }
            Self::Append {
                mutation: StMutation::AppendTurn { .. },
                expected_sha256,
                expected_integrity,
                ..
            }
            | Self::Replace {
                mutation: StMutation::ReplaceLastAssistant { .. },
                expected_sha256,
                expected_integrity,
                ..
            }
            | Self::Undo {
                mutation: StMutation::UndoLastTurn { .. },
                expected_sha256,
                expected_integrity,
                ..
            }
            | Self::Compress {
                mutation: StMutation::CompressMessages { .. },
                expected_sha256,
                expected_integrity,
                ..
            } if !expected_sha256.trim().is_empty() && !expected_integrity.trim().is_empty() => {
                Ok(())
            }
            Self::Create { .. } => Ok(()),
            _ => invalid(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationAad {
    pub version: u16,
    pub operation_id: String,
    pub locator_hash: String,
    pub operation_kind: String,
    pub actor_id: String,
    pub bot_id: String,
    pub runtime_fence: Option<PollerRuntimeFence>,
}

impl OperationAad {
    pub fn new(
        operation_id: impl Into<String>,
        locator_hash: impl Into<String>,
        operation_kind: impl Into<String>,
        actor_id: impl Into<String>,
        bot_id: impl Into<String>,
    ) -> Self {
        Self {
            version: 1,
            operation_id: operation_id.into(),
            locator_hash: locator_hash.into(),
            operation_kind: operation_kind.into(),
            actor_id: actor_id.into(),
            bot_id: bot_id.into(),
            runtime_fence: None,
        }
    }

    pub fn new_with_fence(
        operation_id: impl Into<String>,
        locator_hash: impl Into<String>,
        operation_kind: impl Into<String>,
        actor_id: impl Into<String>,
        fence: PollerRuntimeFence,
    ) -> Self {
        let mut aad = Self::new(
            operation_id,
            locator_hash,
            operation_kind,
            actor_id,
            fence.internal_bot_id.clone(),
        );
        aad.runtime_fence = Some(fence);
        aad
    }

    /// Alias naming the bot identity as it is used by the operation protocol.
    /// The wire encoding remains five length-prefixed immutable components for
    /// compatibility with existing records.
    pub fn internal_bot_id(&self) -> &str {
        &self.bot_id
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = b"im-bridge/operation-payload\0".to_vec();
        out.extend_from_slice(&self.version.to_be_bytes());
        for part in [
            &self.operation_id,
            &self.locator_hash,
            &self.operation_kind,
            &self.actor_id,
            &self.bot_id,
        ] {
            out.extend_from_slice(&(part.len() as u64).to_be_bytes());
            out.extend_from_slice(part.as_bytes());
        }
        if let Some(fence) = &self.runtime_fence {
            for part in [
                fence.telegram_bot_id.to_string(),
                fence.owner.clone(),
                fence.epoch.to_string(),
                fence.internal_bot_id.clone(),
                fence.runtime_instance_id.clone(),
            ] {
                out.extend_from_slice(&(part.len() as u64).to_be_bytes());
                out.extend_from_slice(part.as_bytes());
            }
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptedOperationPayload {
    pub ciphertext: Vec<u8>,
    pub nonce: [u8; 24],
    pub key_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeOperationRecord {
    pub id: String,
    pub actor_id: String,
    pub bot_id: String,
    pub telegram_update_id: i64,
    pub channel_context_key: String,
    pub operation_kind: String,
    pub locator: StChatLocator,
    pub status: BridgeOperationStatus,
    pub commit_state: OperationCommitState,
    pub source_sha256: Option<String>,
    pub source_integrity: Option<String>,
    pub source_size: Option<i64>,
    pub message_count: Option<i64>,
    pub mutation_digest: Option<String>,
    pub payload: Option<EncryptedOperationPayload>,
    pub connector_result_json: Option<String>,
    pub error_stage: Option<String>,
    pub error_code: Option<String>,
    pub error_summary: Option<String>,
    pub retryable: bool,
    pub attempt_count: u32,
    pub request_id: String,
    pub trace_id: String,
    pub created_at: String,
    pub updated_at: String,
}

impl BridgeOperationRecord {
    pub fn aad(&self, locator_hash: impl Into<String>) -> OperationAad {
        OperationAad::new(
            self.id.clone(),
            locator_hash,
            self.operation_kind.clone(),
            self.actor_id.clone(),
            self.bot_id.clone(),
        )
    }
}
