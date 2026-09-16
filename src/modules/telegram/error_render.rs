use crate::modules::bridge::errors::{
    CommitState, RetryAction, StBridgeError, StErrorCode, StErrorStage,
};

pub fn render(error: &StBridgeError) -> String {
    let heading = heading(error);
    let write_state = match error.commit_state {
        CommitState::NotStarted => "没有进入 ST 写入",
        CommitState::NotApplied => "未写入 ST",
        CommitState::Applied => "已写入 ST",
        CommitState::Unknown => "状态未知，尚未确认是否写入 ST",
    };
    let retry = match error.retry_action() {
        RetryAction::RetryDirect => "可以安全重试",
        RetryAction::RetryExplicit => "可以，请显式重试",
        RetryAction::RetryFromLatestSnapshot => "可以，请基于最新会话重试",
        RetryAction::ReconcileSameOperation => "不可自动重试",
        RetryAction::RetryDeliveryOnly => "只重试投递",
        RetryAction::NoRetry => "不可自动重试",
    };
    let advice = advice(error);
    let safe_message = redact_message(&error.safe_message);
    format!(
        "{heading}\n\n阶段：{}\n错误码：{}\n本次写入 ST：{write_state}\n说明：{}\n可安全重试：{retry}\n建议：{advice}\n追踪号：{}",
        stage_label(error.stage),
        error.code,
        safe_message,
        short_trace_id(error),
    )
}

fn redact_message(message: &str) -> String {
    let mut output = String::with_capacity(message.len().min(256));
    let mut previous_space = false;
    for character in message.chars().take(256) {
        let replacement = if character.is_control() {
            ' '
        } else {
            character
        };
        if replacement.is_whitespace() {
            if !previous_space {
                output.push(' ');
            }
            previous_space = true;
        } else {
            output.push(replacement);
            previous_space = false;
        }
    }
    let lowered = output.to_ascii_lowercase();
    if lowered.contains("http://")
        || lowered.contains("https://")
        || lowered.contains("bearer ")
        || lowered.contains("token=")
        || lowered.contains("api_key=")
        || lowered.contains("/bot")
    {
        return "上游返回了受保护的错误详情。".into();
    }
    output
}

fn heading(error: &StBridgeError) -> &'static str {
    if error.stage == StErrorStage::Delivery {
        return "Telegram 投递失败";
    }
    match error.code {
        StErrorCode::StWriteNotReady => "ST 聊天写入未就绪",
        StErrorCode::StChatConflict => "ST 保存冲突",
        StErrorCode::StChatNotFound
        | StErrorCode::StChatPayloadInvalid
        | StErrorCode::StChatLocatorRejected
        | StErrorCode::StChatIntegrityMissing
        | StErrorCode::StSettingsInvalid => "ST 读取聊天失败",
        StErrorCode::StGenerateRejected
        | StErrorCode::StGenerateRateLimited
        | StErrorCode::StGenerateUpstream5xx
        | StErrorCode::StGenerateIdleTimeout
        | StErrorCode::StGenerateHardTimeout
        | StErrorCode::StGenerateStreamInvalid
        | StErrorCode::StGenerateEmpty
        | StErrorCode::StGenerateStateUnknown => "ST 生成失败",
        StErrorCode::StConnectorUnavailable
        | StErrorCode::StConnectorAuthFailed
        | StErrorCode::StConnectorVersionMismatch
        | StErrorCode::StConnectorMediaTypeRejected
        | StErrorCode::StBackupFailed
        | StErrorCode::StBackupQuotaExceeded
        | StErrorCode::StWriteVerifyFailed
        | StErrorCode::StCommitPayloadTooLarge
        | StErrorCode::StCommitValidationFailed
        | StErrorCode::StCommitFailed
        | StErrorCode::StCommitStateUnknown => "ST 保存失败",
        StErrorCode::StConnectFailed
        | StErrorCode::StRequestTimeout
        | StErrorCode::StCsrfFetchFailed
        | StErrorCode::StCsrfMissing
        | StErrorCode::StSessionRejected => "ST 连接失败",
        StErrorCode::StCharacterListFailed => "ST 读取角色失败",
        StErrorCode::StChatListFailed => "ST 读取聊天列表失败",
        _ => "ST 请求失败",
    }
}

fn stage_label(stage: StErrorStage) -> &'static str {
    match stage {
        StErrorStage::Control => "控制与写入资格",
        StErrorStage::Connect => "连接 ST",
        StErrorStage::Session => "ST 会话",
        StErrorStage::Catalog => "读取 ST 目录",
        StErrorStage::Snapshot => "读取 ST 聊天快照",
        StErrorStage::Prompt => "构建请求",
        StErrorStage::Generation => "生成响应",
        StErrorStage::Commit => "保存聊天",
        StErrorStage::Delivery => "发送 Telegram 回复",
    }
}

fn advice(error: &StBridgeError) -> &'static str {
    match error.code {
        StErrorCode::StWriteNotReady => "等待 ST backend 接入并完成写入能力校验后再试。",
        StErrorCode::StChatConflict => "查看 ST 最新内容后，重新发送本次消息。",
        StErrorCode::StCommitStateUnknown => "不要重复执行；先使用相同 operation ID 对账。",
        StErrorCode::StGenerateRateLimited => "遵循 ST 返回的等待时间，再显式重试。",
        StErrorCode::StGenerateStateUnknown => "先查询操作状态，不要自动重复生成。",
        StErrorCode::StDeliveryFailed => "ST 内容已写入；只重试 Telegram 投递，不要重复生成。",
        _ if error.retryable => "稍后再次显式重试。",
        _ => "查看管理页中的脱敏诊断信息后再处理。",
    }
}

fn short_trace_id(error: &StBridgeError) -> String {
    let mut value: String = error
        .trace_id
        .chars()
        .chain(error.request_id.chars())
        .chain(error.operation_id.as_deref().unwrap_or_default().chars())
        .filter(|character| character.is_ascii_alphanumeric())
        .map(|character| character.to_ascii_uppercase())
        .take(12)
        .collect();
    while value.len() < 8 {
        value.push('0');
    }
    value
}

#[cfg(test)]
mod tests {
    use super::render;
    use crate::modules::bridge::errors::{CommitState, StBridgeError, StErrorCode, StErrorStage};

    #[test]
    fn renders_actionable_summary_without_full_ids() {
        let error = StBridgeError {
            code: StErrorCode::StGenerateUpstream5xx,
            stage: StErrorStage::Generation,
            safe_message: "ST backend returned a safe synthetic error.".into(),
            safe_detail: Some("status=500".into()),
            upstream_http_status: Some(500),
            upstream_code: None,
            endpoint_class: Some("generation".into()),
            retryable: true,
            commit_state: CommitState::NotStarted,
            operation_id: Some("operation-full-synthetic-id".into()),
            request_id: "request-full-synthetic-id".into(),
            trace_id: "trace-full-synthetic-id".into(),
            duration_ms: Some(12),
            attempt: 1,
        };
        let rendered = render(&error);
        assert!(rendered.starts_with("ST 生成失败"));
        assert!(rendered.contains("ST_GENERATE_UPSTREAM_5XX"));
        assert!(rendered.contains("没有进入 ST 写入"));
        assert!(rendered.contains("可以，请显式重试"));
        assert!(rendered.contains("追踪号：TRACEFULLSYN"));
        assert!(!rendered.contains("trace-full-synthetic-id"));
        assert!(!rendered.contains("request-full-synthetic-id"));
    }
}
