use serde_json::Value;
use sha2::{Digest, Sha256};

use super::errors::{CommitState, StBridgeError, StErrorCode, StErrorStage};
use super::redaction::redact_detail;
use crate::st_readiness::ST_WRITE_NOT_READY_MESSAGE;

pub struct StErrorFacts {
    pub stage: StErrorStage,
    pub endpoint_class: Option<String>,
    pub operation_id: Option<String>,
    pub commit_state: CommitState,
    pub attempt: u32,
    pub duration_ms: Option<u64>,
    pub failure: StFailureFacts,
}

pub enum StFailureFacts {
    Connect,
    Timeout,
    Http {
        status: u16,
        content_type: Option<String>,
        body: Option<Vec<u8>>,
    },
    DecodeInvalidPayload,
    DecodeSettings,
    ConnectorUnavailable,
    ConnectorAuth,
    ConnectorVersion,
    ConnectorMediaType,
    Control {
        code: StErrorCode,
    },
}

pub fn map_st_error(facts: StErrorFacts) -> Box<StBridgeError> {
    match &facts.failure {
        StFailureFacts::Connect => mapped_plain(
            &facts,
            StErrorCode::StConnectFailed,
            StErrorStage::Connect,
            true,
            facts.commit_state,
        ),
        StFailureFacts::Timeout => mapped_plain(
            &facts,
            StErrorCode::StRequestTimeout,
            facts.stage,
            true,
            facts.commit_state,
        ),
        StFailureFacts::Http {
            status,
            content_type,
            body,
        } => map_http(&facts, *status, content_type.as_deref(), body.as_deref()),
        StFailureFacts::DecodeInvalidPayload => mapped_plain(
            &facts,
            StErrorCode::StChatPayloadInvalid,
            facts.stage,
            false,
            facts.commit_state,
        ),
        StFailureFacts::DecodeSettings => mapped_plain(
            &facts,
            StErrorCode::StSettingsInvalid,
            facts.stage,
            false,
            facts.commit_state,
        ),
        StFailureFacts::ConnectorUnavailable => mapped_plain(
            &facts,
            StErrorCode::StConnectorUnavailable,
            facts.stage,
            true,
            facts.commit_state,
        ),
        StFailureFacts::ConnectorAuth => mapped_plain(
            &facts,
            StErrorCode::StConnectorAuthFailed,
            facts.stage,
            false,
            facts.commit_state,
        ),
        StFailureFacts::ConnectorVersion => mapped_plain(
            &facts,
            StErrorCode::StConnectorVersionMismatch,
            facts.stage,
            false,
            facts.commit_state,
        ),
        StFailureFacts::ConnectorMediaType => mapped_plain(
            &facts,
            StErrorCode::StConnectorMediaTypeRejected,
            facts.stage,
            false,
            facts.commit_state,
        ),
        StFailureFacts::Control { code } => mapped_plain(
            &facts,
            *code,
            facts.stage,
            default_retryable(*code),
            facts.commit_state,
        ),
    }
}

fn default_retryable(code: StErrorCode) -> bool {
    matches!(
        code,
        StErrorCode::StConnectFailed
            | StErrorCode::StRequestTimeout
            | StErrorCode::StCsrfFetchFailed
            | StErrorCode::StCharacterListFailed
            | StErrorCode::StChatListFailed
            | StErrorCode::StGenerateRateLimited
            | StErrorCode::StGenerateUpstream5xx
            | StErrorCode::StGenerateIdleTimeout
            | StErrorCode::StGenerateHardTimeout
            | StErrorCode::StGenerateStreamInvalid
            | StErrorCode::StGenerateEmpty
            | StErrorCode::StConnectorUnavailable
            | StErrorCode::StChatConflict
            | StErrorCode::StBackupFailed
            | StErrorCode::StCommitFailed
            | StErrorCode::StDeliveryFailed
    )
}

fn map_http(
    facts: &StErrorFacts,
    status: u16,
    content_type: Option<&str>,
    body: Option<&[u8]>,
) -> Box<StBridgeError> {
    if status == 0 {
        return mapped(
            facts,
            StErrorCode::StConnectFailed,
            StErrorStage::Connect,
            true,
            facts.commit_state,
            MappedExtras {
                upstream_http_status: Some(status),
                upstream_code: json_upstream_code(content_type, body),
                safe_detail: http_safe_detail(status, content_type, body),
            },
        );
    }

    if let Some(mapped) = map_upstream_st_error(facts, status, content_type, body) {
        return mapped;
    }

    let (code, stage, retryable, commit_state) = match status {
        403 => (
            StErrorCode::StSessionRejected,
            StErrorStage::Session,
            false,
            facts.commit_state,
        ),
        401 => (
            StErrorCode::StSessionRejected,
            StErrorStage::Session,
            false,
            facts.commit_state,
        ),
        404 => (
            StErrorCode::StChatNotFound,
            StErrorStage::Snapshot,
            false,
            facts.commit_state,
        ),
        409 if facts.stage == StErrorStage::Commit => (
            StErrorCode::StChatConflict,
            StErrorStage::Commit,
            true,
            CommitState::NotApplied,
        ),
        413 => (
            StErrorCode::StCommitPayloadTooLarge,
            StErrorStage::Commit,
            false,
            facts.commit_state,
        ),
        429 => (
            StErrorCode::StGenerateRateLimited,
            facts.stage,
            true,
            facts.commit_state,
        ),
        200..=299 => (
            StErrorCode::StChatPayloadInvalid,
            facts.stage,
            false,
            facts.commit_state,
        ),
        500..=599 => map_5xx(facts),
        400..=499 => map_other_4xx(facts),
        _ => map_other_4xx(facts),
    };

    mapped(
        facts,
        code,
        stage,
        retryable,
        commit_state,
        MappedExtras {
            upstream_http_status: Some(status),
            upstream_code: json_upstream_code(content_type, body),
            safe_detail: http_safe_detail(status, content_type, body),
        },
    )
}

fn map_5xx(facts: &StErrorFacts) -> (StErrorCode, StErrorStage, bool, CommitState) {
    match facts.stage {
        StErrorStage::Generation => (
            StErrorCode::StGenerateUpstream5xx,
            facts.stage,
            true,
            facts.commit_state,
        ),
        StErrorStage::Commit => {
            let commit_state = if facts.commit_state == CommitState::Unknown {
                CommitState::Unknown
            } else {
                CommitState::NotApplied
            };
            (StErrorCode::StCommitFailed, facts.stage, true, commit_state)
        }
        StErrorStage::Session => (
            StErrorCode::StCsrfFetchFailed,
            facts.stage,
            true,
            facts.commit_state,
        ),
        StErrorStage::Connect => (
            StErrorCode::StConnectFailed,
            StErrorStage::Connect,
            true,
            facts.commit_state,
        ),
        _ => (
            catalog_code(facts.endpoint_class.as_deref()),
            facts.stage,
            true,
            facts.commit_state,
        ),
    }
}

fn map_other_4xx(facts: &StErrorFacts) -> (StErrorCode, StErrorStage, bool, CommitState) {
    match facts.stage {
        StErrorStage::Generation => (
            StErrorCode::StGenerateRejected,
            facts.stage,
            false,
            facts.commit_state,
        ),
        StErrorStage::Commit => (
            StErrorCode::StCommitValidationFailed,
            facts.stage,
            false,
            facts.commit_state,
        ),
        StErrorStage::Session => (
            StErrorCode::StCsrfMissing,
            StErrorStage::Session,
            false,
            facts.commit_state,
        ),
        StErrorStage::Catalog => (
            catalog_code(facts.endpoint_class.as_deref()),
            facts.stage,
            true,
            facts.commit_state,
        ),
        StErrorStage::Snapshot => (
            StErrorCode::StChatPayloadInvalid,
            facts.stage,
            false,
            facts.commit_state,
        ),
        StErrorStage::Connect => (
            StErrorCode::StConnectFailed,
            StErrorStage::Connect,
            true,
            facts.commit_state,
        ),
        _ => (
            StErrorCode::StSessionRejected,
            facts.stage,
            false,
            facts.commit_state,
        ),
    }
}

fn map_upstream_st_error(
    facts: &StErrorFacts,
    status: u16,
    content_type: Option<&str>,
    body: Option<&[u8]>,
) -> Option<Box<StBridgeError>> {
    let body = body?;
    if !is_json_body(content_type, body) {
        return None;
    }
    let value: Value = serde_json::from_slice(body).ok()?;
    let error = value.get("error")?;
    let code = error.get("code").and_then(Value::as_str)?;
    let parsed = StErrorCode::parse(code)?;
    let stage = error
        .get("stage")
        .and_then(Value::as_str)
        .and_then(StErrorStage::parse)
        .unwrap_or(facts.stage);
    let retryable = error
        .get("retryable")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| default_retryable(parsed));
    let commit_state = error
        .get("commitState")
        .and_then(Value::as_str)
        .and_then(CommitState::parse)
        .unwrap_or(facts.commit_state);
    Some(mapped(
        facts,
        parsed,
        stage,
        retryable,
        commit_state,
        MappedExtras {
            upstream_http_status: Some(status),
            upstream_code: Some(code.to_string()),
            safe_detail: http_safe_detail(status, content_type, Some(body)),
        },
    ))
}

fn catalog_code(endpoint_class: Option<&str>) -> StErrorCode {
    let class = endpoint_class.unwrap_or("").to_ascii_lowercase();
    if class.contains("chat") {
        StErrorCode::StChatListFailed
    } else {
        StErrorCode::StCharacterListFailed
    }
}

fn mapped(
    facts: &StErrorFacts,
    code: StErrorCode,
    stage: StErrorStage,
    retryable: bool,
    commit_state: CommitState,
    extras: MappedExtras,
) -> Box<StBridgeError> {
    let mut error = StBridgeError::new(code, stage, safe_message(code), retryable, commit_state);
    error.endpoint_class = facts.endpoint_class.clone();
    error.operation_id = facts.operation_id.clone();
    error.attempt = facts.attempt;
    error.duration_ms = facts.duration_ms;
    error.upstream_http_status = extras.upstream_http_status;
    error.upstream_code = extras.upstream_code;
    error.safe_detail = extras.safe_detail;
    Box::new(error)
}

struct MappedExtras {
    upstream_http_status: Option<u16>,
    upstream_code: Option<String>,
    safe_detail: Option<String>,
}

fn mapped_plain(
    facts: &StErrorFacts,
    code: StErrorCode,
    stage: StErrorStage,
    retryable: bool,
    commit_state: CommitState,
) -> Box<StBridgeError> {
    mapped(
        facts,
        code,
        stage,
        retryable,
        commit_state,
        MappedExtras {
            upstream_http_status: None,
            upstream_code: None,
            safe_detail: None,
        },
    )
}

fn safe_message(code: StErrorCode) -> String {
    match code {
        StErrorCode::StConnectFailed => "无法连接 SillyTavern，本次没有写入聊天。",
        StErrorCode::StRequestTimeout => "请求 SillyTavern 超时，本次没有写入聊天。",
        StErrorCode::StCsrfFetchFailed => "无法获取 SillyTavern 会话凭证，本次没有写入聊天。",
        StErrorCode::StCsrfMissing => "SillyTavern 会话缺少必要凭证，本次没有写入聊天。",
        StErrorCode::StSessionRejected => "SillyTavern 拒绝了本次会话，本次没有写入聊天。",
        StErrorCode::StCharacterListFailed => "无法读取 SillyTavern 角色列表，本次没有写入聊天。",
        StErrorCode::StChatListFailed => "无法读取 SillyTavern 聊天列表，本次没有写入聊天。",
        StErrorCode::StChatNotFound => "找不到对应的 SillyTavern 聊天，本次没有写入。",
        StErrorCode::StChatPayloadInvalid => {
            "SillyTavern 返回的聊天内容无法解析，本次没有写入聊天。"
        }
        StErrorCode::StSettingsInvalid => "SillyTavern 设置无效或无法解析，本次没有写入聊天。",
        StErrorCode::StGenerateRejected => "SillyTavern 拒绝了本次生成请求，内容没有写入聊天。",
        StErrorCode::StGenerateRateLimited => {
            "SillyTavern 生成请求过于频繁，本次内容没有写入聊天。"
        }
        StErrorCode::StGenerateUpstream5xx => "ST 的模型后端返回错误，本次内容没有写入聊天。",
        StErrorCode::StGenerateIdleTimeout => {
            "SillyTavern 生成长时间没有输出，本次内容没有写入聊天。"
        }
        StErrorCode::StGenerateHardTimeout => {
            "SillyTavern 生成达到时间上限，本次内容没有写入聊天。"
        }
        StErrorCode::StGenerateStreamInvalid => {
            "SillyTavern 生成流格式无效，本次内容没有写入聊天。"
        }
        StErrorCode::StGenerateEmpty => "SillyTavern 生成结束但没有文本，本次内容没有写入聊天。",
        StErrorCode::StGenerateStateUnknown => {
            "无法确认 SillyTavern 是否已生成，请先对账后再处理。"
        }
        StErrorCode::StConnectorUnavailable => "无法连接写入服务，本次没有写入聊天。",
        StErrorCode::StConnectorAuthFailed => "写入服务鉴权失败，本次没有写入聊天。",
        StErrorCode::StConnectorVersionMismatch => "写入服务版本不兼容，本次没有写入聊天。",
        StErrorCode::StConnectorMediaTypeRejected => "写入请求格式被拒绝，本次没有写入聊天。",
        StErrorCode::StWriteNotReady => ST_WRITE_NOT_READY_MESSAGE,
        StErrorCode::StTestScopeRequired => "当前只能写入测试聊天，本次没有写入。",
        StErrorCode::StPollerNotOwner => "当前运行实例不是该 Bot 的所有者，本次没有写入。",
        StErrorCode::StPollerEpochMismatch => "当前运行实例持有过期所有权，本次没有写入。",
        StErrorCode::StChatLocatorRejected => "聊天定位信息不合法，本次没有写入。",
        StErrorCode::StChatIntegrityMissing => "聊天缺少完整性标记，本次没有写入。",
        StErrorCode::StChatConflict => "生成期间该聊天已被更新，本次没有覆盖新内容。",
        StErrorCode::StOperationIdReused => "操作标识被重复使用，本次没有写入。",
        StErrorCode::StBackupFailed => "写入前备份失败，本次没有写入聊天。",
        StErrorCode::StBackupQuotaExceeded => "备份空间不足，本次没有写入聊天。",
        StErrorCode::StWriteVerifyFailed => "写入后校验失败，尚未确认聊天是否已更新。",
        StErrorCode::StCommitPayloadTooLarge => "写入内容过大，本次没有写入聊天。",
        StErrorCode::StCommitValidationFailed => "写入内容未通过校验，本次没有写入聊天。",
        StErrorCode::StCommitFailed => "保存聊天失败，本次没有写入。",
        StErrorCode::StCommitStateUnknown => "无法确认聊天是否已保存，请先对账后再处理。",
        StErrorCode::StDeliveryFailed => "ST 内容已写入；Telegram 投递失败。",
    }
    .to_string()
}

fn http_safe_detail(
    status: u16,
    content_type: Option<&str>,
    body: Option<&[u8]>,
) -> Option<String> {
    let body = body.unwrap_or(&[]);
    let sha256 = hex::encode(Sha256::digest(body));
    let metadata = match content_type {
        Some(content_type) => format!(
            "status={status}; len={}; sha256={sha256}; content-type={content_type}",
            body.len()
        ),
        None => format!("status={status}; len={}; sha256={sha256}", body.len()),
    };
    let mut detail = metadata.clone();
    if is_json_body(content_type, body) {
        if let Some((code, message)) = json_error_fields(body) {
            if let Some(code) = code {
                detail.push_str("; upstream_code=");
                detail.push_str(&code);
            }
            if let Some(message) = message {
                detail.push_str("; message=");
                detail.push_str(&message);
            }
        }
    } else if let Ok(text) = std::str::from_utf8(body) {
        if let Some(excerpt) = redact_detail(text) {
            detail.push_str("; excerpt=");
            detail.push_str(&excerpt);
        }
    }
    redact_detail(&detail).or_else(|| redact_detail(&metadata))
}

fn json_upstream_code(content_type: Option<&str>, body: Option<&[u8]>) -> Option<String> {
    let body = body?;
    if !is_json_body(content_type, body) {
        return None;
    }
    json_error_fields(body).and_then(|(code, _)| code)
}

fn json_error_fields(body: &[u8]) -> Option<(Option<String>, Option<String>)> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let error = value.get("error")?;
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let kind = error
        .get("type")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    Some((code.or(kind), message))
}

fn is_json_body(content_type: Option<&str>, body: &[u8]) -> bool {
    let content_type = content_type.unwrap_or("").to_ascii_lowercase();
    if content_type.contains("json") {
        return true;
    }
    if !content_type.is_empty() {
        return false;
    }
    let trimmed = std::str::from_utf8(body)
        .ok()
        .map(str::trim)
        .unwrap_or_default();
    trimmed.starts_with('{') || trimmed.starts_with('[')
}
