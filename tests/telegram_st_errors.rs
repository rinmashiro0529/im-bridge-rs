use im_bridge::modules::bridge::errors::{
    CommitState, RetryAction, StBridgeError, StErrorCode, StErrorStage,
};
use im_bridge::modules::telegram::error_render::render;

fn error(
    code: StErrorCode,
    stage: StErrorStage,
    retryable: bool,
    state: CommitState,
) -> StBridgeError {
    StBridgeError {
        code,
        stage,
        safe_message: "synthetic safe message".into(),
        safe_detail: None,
        upstream_http_status: None,
        upstream_code: None,
        endpoint_class: None,
        retryable,
        commit_state: state,
        operation_id: Some("operation-private-synthetic".into()),
        request_id: "request-private-synthetic".into(),
        trace_id: "trace-short".into(),
        duration_ms: Some(4),
        attempt: 1,
    }
}

#[test]
fn conflict_template_explains_write_state_and_action() {
    let rendered = render(&error(
        StErrorCode::StChatConflict,
        StErrorStage::Commit,
        true,
        CommitState::NotApplied,
    ));
    assert!(rendered.starts_with("ST 保存冲突"));
    assert!(rendered.contains("ST_CHAT_CONFLICT"));
    assert!(rendered.contains("未写入 ST"));
    assert!(rendered.contains("查看 ST 最新内容后，重新发送本次消息"));
    assert!(rendered.contains("可以，请基于最新会话重试"));
    assert_eq!(
        error(
            StErrorCode::StChatConflict,
            StErrorStage::Commit,
            true,
            CommitState::NotApplied,
        )
        .retry_action(),
        RetryAction::RetryFromLatestSnapshot
    );
    assert!(rendered.contains("追踪号：TRACESHORT"));
    assert!(!rendered.contains("operation-private-synthetic"));
    assert!(!rendered.contains("request-private-synthetic"));
}

#[test]
fn delivery_failure_after_commit_is_not_reported_as_st_failure() {
    let rendered = render(&error(
        StErrorCode::StDeliveryFailed,
        StErrorStage::Delivery,
        true,
        CommitState::Applied,
    ));
    assert!(rendered.starts_with("Telegram 投递失败"));
    assert!(rendered.contains("ST_DELIVERY_FAILED"));
    assert!(rendered.contains("已写入 ST"));
    assert!(rendered.contains("ST 内容已写入；只重试 Telegram 投递，不要重复生成"));
    assert!(rendered.contains("只重试投递"));
    assert_eq!(
        error(
            StErrorCode::StDeliveryFailed,
            StErrorStage::Delivery,
            true,
            CommitState::Applied,
        )
        .retry_action(),
        RetryAction::RetryDeliveryOnly
    );
    assert!(!rendered.starts_with("ST 保存失败"));
}

#[test]
fn default_error_ids_are_nonempty_and_renderer_uses_short_trace() {
    let first = StBridgeError::new(
        StErrorCode::StGenerateStateUnknown,
        StErrorStage::Generation,
        "synthetic safe message",
        false,
        CommitState::Unknown,
    );
    let second = StBridgeError::new(
        StErrorCode::StGenerateStateUnknown,
        StErrorStage::Generation,
        "synthetic safe message",
        false,
        CommitState::Unknown,
    );
    assert!(!first.request_id.is_empty());
    assert!(!first.trace_id.is_empty());
    assert_ne!(first.request_id, second.request_id);
    assert_ne!(first.trace_id, second.trace_id);
    let rendered = render(&first);
    let trace = rendered
        .lines()
        .find_map(|line| line.strip_prefix("追踪号："))
        .expect("short trace line");
    assert!((8..=12).contains(&trace.len()));
    assert_ne!(trace, "00000000");
    assert!(!rendered.contains(&first.request_id));
    assert!(!rendered.contains(&first.trace_id));
}

#[test]
fn retry_action_maps_conflict_unknown_and_delivery() {
    assert_eq!(
        error(
            StErrorCode::StChatConflict,
            StErrorStage::Commit,
            true,
            CommitState::NotApplied,
        )
        .retry_action(),
        RetryAction::RetryFromLatestSnapshot
    );
    assert_eq!(
        error(
            StErrorCode::StCommitStateUnknown,
            StErrorStage::Commit,
            false,
            CommitState::Unknown,
        )
        .retry_action(),
        RetryAction::ReconcileSameOperation
    );
    assert_eq!(
        error(
            StErrorCode::StDeliveryFailed,
            StErrorStage::Delivery,
            true,
            CommitState::Applied,
        )
        .retry_action(),
        RetryAction::RetryDeliveryOnly
    );
}

#[test]
fn unknown_commit_template_forbids_unsafe_retry() {
    let rendered = render(&error(
        StErrorCode::StCommitStateUnknown,
        StErrorStage::Commit,
        false,
        CommitState::Unknown,
    ));
    assert!(rendered.starts_with("ST 保存失败"));
    assert!(rendered.contains("状态未知，尚未确认是否写入 ST"));
    assert!(rendered.contains("不可自动重试"));
    assert!(rendered.contains("不要重复执行；先使用相同 operation ID 对账"));
    assert_eq!(
        error(
            StErrorCode::StCommitStateUnknown,
            StErrorStage::Commit,
            false,
            CommitState::Unknown,
        )
        .retry_action(),
        RetryAction::ReconcileSameOperation
    );
}
