use serde::{Deserialize, Serialize};
use std::fmt;

use super::redaction::redact_detail;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StErrorStage {
    Control,
    Connect,
    Session,
    Catalog,
    Snapshot,
    Prompt,
    Generation,
    Commit,
    Delivery,
}

impl StErrorStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Connect => "connect",
            Self::Session => "session",
            Self::Catalog => "catalog",
            Self::Snapshot => "snapshot",
            Self::Prompt => "prompt",
            Self::Generation => "generation",
            Self::Commit => "commit",
            Self::Delivery => "delivery",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "control" => Some(Self::Control),
            "connect" => Some(Self::Connect),
            "session" => Some(Self::Session),
            "catalog" => Some(Self::Catalog),
            "snapshot" => Some(Self::Snapshot),
            "prompt" => Some(Self::Prompt),
            "generation" => Some(Self::Generation),
            "commit" => Some(Self::Commit),
            "delivery" => Some(Self::Delivery),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitState {
    NotStarted,
    NotApplied,
    Applied,
    Unknown,
}

impl CommitState {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryAction {
    RetryDirect,
    RetryExplicit,
    RetryFromLatestSnapshot,
    ReconcileSameOperation,
    RetryDeliveryOnly,
    NoRetry,
}

impl RetryAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RetryDirect => "retry_direct",
            Self::RetryExplicit => "retry_explicit",
            Self::RetryFromLatestSnapshot => "retry_from_latest_snapshot",
            Self::ReconcileSameOperation => "reconcile_same_operation",
            Self::RetryDeliveryOnly => "retry_delivery_only",
            Self::NoRetry => "no_retry",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StErrorCode {
    StConnectFailed,
    StRequestTimeout,
    StCsrfFetchFailed,
    StCsrfMissing,
    StSessionRejected,
    StCharacterListFailed,
    StChatListFailed,
    StChatNotFound,
    StChatPayloadInvalid,
    StSettingsInvalid,
    StGenerateRejected,
    StGenerateRateLimited,
    #[serde(rename = "ST_GENERATE_UPSTREAM_5XX")]
    StGenerateUpstream5xx,
    StGenerateIdleTimeout,
    StGenerateHardTimeout,
    StGenerateStreamInvalid,
    StGenerateEmpty,
    StGenerateStateUnknown,
    StConnectorUnavailable,
    StConnectorAuthFailed,
    StConnectorVersionMismatch,
    StConnectorMediaTypeRejected,
    StWriteNotReady,
    StTestScopeRequired,
    StPollerNotOwner,
    StPollerEpochMismatch,
    StChatLocatorRejected,
    StChatIntegrityMissing,
    StChatConflict,
    StOperationIdReused,
    StBackupFailed,
    StBackupQuotaExceeded,
    StWriteVerifyFailed,
    StCommitPayloadTooLarge,
    StCommitValidationFailed,
    StCommitFailed,
    StCommitStateUnknown,
    StDeliveryFailed,
}

impl StErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StConnectFailed => "ST_CONNECT_FAILED",
            Self::StRequestTimeout => "ST_REQUEST_TIMEOUT",
            Self::StCsrfFetchFailed => "ST_CSRF_FETCH_FAILED",
            Self::StCsrfMissing => "ST_CSRF_MISSING",
            Self::StSessionRejected => "ST_SESSION_REJECTED",
            Self::StCharacterListFailed => "ST_CHARACTER_LIST_FAILED",
            Self::StChatListFailed => "ST_CHAT_LIST_FAILED",
            Self::StChatNotFound => "ST_CHAT_NOT_FOUND",
            Self::StChatPayloadInvalid => "ST_CHAT_PAYLOAD_INVALID",
            Self::StSettingsInvalid => "ST_SETTINGS_INVALID",
            Self::StGenerateRejected => "ST_GENERATE_REJECTED",
            Self::StGenerateRateLimited => "ST_GENERATE_RATE_LIMITED",
            Self::StGenerateUpstream5xx => "ST_GENERATE_UPSTREAM_5XX",
            Self::StGenerateIdleTimeout => "ST_GENERATE_IDLE_TIMEOUT",
            Self::StGenerateHardTimeout => "ST_GENERATE_HARD_TIMEOUT",
            Self::StGenerateStreamInvalid => "ST_GENERATE_STREAM_INVALID",
            Self::StGenerateEmpty => "ST_GENERATE_EMPTY",
            Self::StGenerateStateUnknown => "ST_GENERATE_STATE_UNKNOWN",
            Self::StConnectorUnavailable => "ST_CONNECTOR_UNAVAILABLE",
            Self::StConnectorAuthFailed => "ST_CONNECTOR_AUTH_FAILED",
            Self::StConnectorVersionMismatch => "ST_CONNECTOR_VERSION_MISMATCH",
            Self::StConnectorMediaTypeRejected => "ST_CONNECTOR_MEDIA_TYPE_REJECTED",
            Self::StWriteNotReady => "ST_WRITE_NOT_READY",
            Self::StTestScopeRequired => "ST_TEST_SCOPE_REQUIRED",
            Self::StPollerNotOwner => "ST_POLLER_NOT_OWNER",
            Self::StPollerEpochMismatch => "ST_POLLER_EPOCH_MISMATCH",
            Self::StChatLocatorRejected => "ST_CHAT_LOCATOR_REJECTED",
            Self::StChatIntegrityMissing => "ST_CHAT_INTEGRITY_MISSING",
            Self::StChatConflict => "ST_CHAT_CONFLICT",
            Self::StOperationIdReused => "ST_OPERATION_ID_REUSED",
            Self::StBackupFailed => "ST_BACKUP_FAILED",
            Self::StBackupQuotaExceeded => "ST_BACKUP_QUOTA_EXCEEDED",
            Self::StWriteVerifyFailed => "ST_WRITE_VERIFY_FAILED",
            Self::StCommitPayloadTooLarge => "ST_COMMIT_PAYLOAD_TOO_LARGE",
            Self::StCommitValidationFailed => "ST_COMMIT_VALIDATION_FAILED",
            Self::StCommitFailed => "ST_COMMIT_FAILED",
            Self::StCommitStateUnknown => "ST_COMMIT_STATE_UNKNOWN",
            Self::StDeliveryFailed => "ST_DELIVERY_FAILED",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "ST_CONNECT_FAILED" => Some(Self::StConnectFailed),
            "ST_REQUEST_TIMEOUT" => Some(Self::StRequestTimeout),
            "ST_CSRF_FETCH_FAILED" => Some(Self::StCsrfFetchFailed),
            "ST_CSRF_MISSING" => Some(Self::StCsrfMissing),
            "ST_SESSION_REJECTED" => Some(Self::StSessionRejected),
            "ST_CHARACTER_LIST_FAILED" => Some(Self::StCharacterListFailed),
            "ST_CHAT_LIST_FAILED" => Some(Self::StChatListFailed),
            "ST_CHAT_NOT_FOUND" => Some(Self::StChatNotFound),
            "ST_CHAT_PAYLOAD_INVALID" => Some(Self::StChatPayloadInvalid),
            "ST_SETTINGS_INVALID" => Some(Self::StSettingsInvalid),
            "ST_GENERATE_REJECTED" => Some(Self::StGenerateRejected),
            "ST_GENERATE_RATE_LIMITED" => Some(Self::StGenerateRateLimited),
            "ST_GENERATE_UPSTREAM_5XX" => Some(Self::StGenerateUpstream5xx),
            "ST_GENERATE_IDLE_TIMEOUT" => Some(Self::StGenerateIdleTimeout),
            "ST_GENERATE_HARD_TIMEOUT" => Some(Self::StGenerateHardTimeout),
            "ST_GENERATE_STREAM_INVALID" => Some(Self::StGenerateStreamInvalid),
            "ST_GENERATE_EMPTY" => Some(Self::StGenerateEmpty),
            "ST_GENERATE_STATE_UNKNOWN" => Some(Self::StGenerateStateUnknown),
            "ST_CONNECTOR_UNAVAILABLE" => Some(Self::StConnectorUnavailable),
            "ST_CONNECTOR_AUTH_FAILED" => Some(Self::StConnectorAuthFailed),
            "ST_CONNECTOR_VERSION_MISMATCH" => Some(Self::StConnectorVersionMismatch),
            "ST_CONNECTOR_MEDIA_TYPE_REJECTED" => Some(Self::StConnectorMediaTypeRejected),
            "ST_WRITE_NOT_READY" => Some(Self::StWriteNotReady),
            "ST_TEST_SCOPE_REQUIRED" => Some(Self::StTestScopeRequired),
            "ST_POLLER_NOT_OWNER" => Some(Self::StPollerNotOwner),
            "ST_POLLER_EPOCH_MISMATCH" => Some(Self::StPollerEpochMismatch),
            "ST_CHAT_LOCATOR_REJECTED" => Some(Self::StChatLocatorRejected),
            "ST_CHAT_INTEGRITY_MISSING" => Some(Self::StChatIntegrityMissing),
            "ST_CHAT_CONFLICT" => Some(Self::StChatConflict),
            "ST_OPERATION_ID_REUSED" => Some(Self::StOperationIdReused),
            "ST_BACKUP_FAILED" => Some(Self::StBackupFailed),
            "ST_BACKUP_QUOTA_EXCEEDED" => Some(Self::StBackupQuotaExceeded),
            "ST_WRITE_VERIFY_FAILED" => Some(Self::StWriteVerifyFailed),
            "ST_COMMIT_PAYLOAD_TOO_LARGE" => Some(Self::StCommitPayloadTooLarge),
            "ST_COMMIT_VALIDATION_FAILED" => Some(Self::StCommitValidationFailed),
            "ST_COMMIT_FAILED" => Some(Self::StCommitFailed),
            "ST_COMMIT_STATE_UNKNOWN" => Some(Self::StCommitStateUnknown),
            "ST_DELIVERY_FAILED" => Some(Self::StDeliveryFailed),
            _ => None,
        }
    }
}

impl fmt::Display for StErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StBridgeError {
    pub code: StErrorCode,
    pub stage: StErrorStage,
    pub safe_message: String,
    pub safe_detail: Option<String>,
    pub upstream_http_status: Option<u16>,
    pub upstream_code: Option<String>,
    pub endpoint_class: Option<String>,
    pub retryable: bool,
    pub commit_state: CommitState,
    pub operation_id: Option<String>,
    pub request_id: String,
    pub trace_id: String,
    pub duration_ms: Option<u64>,
    pub attempt: u32,
}

impl StBridgeError {
    pub fn new(
        code: StErrorCode,
        stage: StErrorStage,
        safe_message: impl Into<String>,
        retryable: bool,
        commit_state: CommitState,
    ) -> Self {
        Self {
            code,
            stage,
            safe_message: safe_message.into(),
            safe_detail: None,
            upstream_http_status: None,
            upstream_code: None,
            endpoint_class: None,
            retryable,
            commit_state,
            operation_id: None,
            request_id: crate::ids::request_id(),
            trace_id: crate::ids::new_id(),
            duration_ms: None,
            attempt: 1,
        }
    }

    pub fn boxed(
        code: StErrorCode,
        stage: StErrorStage,
        safe_message: impl Into<String>,
        retryable: bool,
        commit_state: CommitState,
    ) -> Box<Self> {
        Box::new(Self::new(
            code,
            stage,
            safe_message,
            retryable,
            commit_state,
        ))
    }

    pub fn with_safe_detail(mut self, detail: &str) -> Self {
        self.safe_detail = redact_detail(detail);
        self
    }

    pub fn code_str(&self) -> &'static str {
        self.code.as_str()
    }

    pub fn retry_action(&self) -> RetryAction {
        match self.code {
            StErrorCode::StDeliveryFailed if self.commit_state == CommitState::Applied => {
                RetryAction::RetryDeliveryOnly
            }
            StErrorCode::StChatConflict => RetryAction::RetryFromLatestSnapshot,
            StErrorCode::StCommitStateUnknown | StErrorCode::StGenerateStateUnknown => {
                RetryAction::ReconcileSameOperation
            }
            _ if !self.retryable => RetryAction::NoRetry,
            StErrorCode::StConnectFailed
            | StErrorCode::StRequestTimeout
            | StErrorCode::StCharacterListFailed
            | StErrorCode::StChatListFailed => RetryAction::RetryDirect,
            StErrorCode::StGenerateUpstream5xx
            | StErrorCode::StGenerateRateLimited
            | StErrorCode::StGenerateIdleTimeout
            | StErrorCode::StGenerateHardTimeout
            | StErrorCode::StGenerateEmpty
            | StErrorCode::StGenerateStreamInvalid
            | StErrorCode::StCommitFailed
            | StErrorCode::StBackupFailed => RetryAction::RetryExplicit,
            _ => RetryAction::NoRetry,
        }
    }
}

impl fmt::Display for StBridgeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.safe_message)
    }
}

impl std::error::Error for StBridgeError {}

pub type StResult<T> = Result<T, Box<StBridgeError>>;
