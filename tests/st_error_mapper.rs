use im_bridge::modules::bridge::error_mapper::{map_st_error, StErrorFacts, StFailureFacts};
use im_bridge::modules::bridge::errors::{
    CommitState, RetryAction, StBridgeError, StErrorCode, StErrorStage,
};

fn facts(stage: StErrorStage, failure: StFailureFacts) -> StErrorFacts {
    StErrorFacts {
        stage,
        endpoint_class: None,
        operation_id: Some("operation-synthetic".into()),
        commit_state: CommitState::NotStarted,
        attempt: 1,
        duration_ms: Some(12),
        failure,
    }
}

fn http(
    stage: StErrorStage,
    status: u16,
    content_type: Option<&str>,
    body: Option<&[u8]>,
) -> Box<StBridgeError> {
    let mut facts = facts(
        stage,
        StFailureFacts::Http {
            status,
            content_type: content_type.map(ToOwned::to_owned),
            body: body.map(ToOwned::to_owned),
        },
    );
    if stage == StErrorStage::Catalog {
        facts.endpoint_class = Some("characters".into());
    }
    map_st_error(facts)
}

#[test]
fn retry_action_table_covers_each_class() {
    let cases = [
        (
            StErrorCode::StConnectFailed,
            true,
            CommitState::NotStarted,
            RetryAction::RetryDirect,
        ),
        (
            StErrorCode::StChatListFailed,
            true,
            CommitState::NotStarted,
            RetryAction::RetryDirect,
        ),
        (
            StErrorCode::StGenerateUpstream5xx,
            true,
            CommitState::NotStarted,
            RetryAction::RetryExplicit,
        ),
        (
            StErrorCode::StCommitFailed,
            true,
            CommitState::NotApplied,
            RetryAction::RetryExplicit,
        ),
        (
            StErrorCode::StChatConflict,
            true,
            CommitState::NotApplied,
            RetryAction::RetryFromLatestSnapshot,
        ),
        (
            StErrorCode::StCommitStateUnknown,
            false,
            CommitState::Unknown,
            RetryAction::ReconcileSameOperation,
        ),
        (
            StErrorCode::StGenerateStateUnknown,
            false,
            CommitState::Unknown,
            RetryAction::ReconcileSameOperation,
        ),
        (
            StErrorCode::StDeliveryFailed,
            true,
            CommitState::Applied,
            RetryAction::RetryDeliveryOnly,
        ),
        (
            StErrorCode::StWriteNotReady,
            false,
            CommitState::NotStarted,
            RetryAction::NoRetry,
        ),
        (
            StErrorCode::StConnectFailed,
            false,
            CommitState::NotStarted,
            RetryAction::NoRetry,
        ),
    ];
    for (code, retryable, commit_state, expected) in cases {
        let error = StBridgeError::new(
            code,
            StErrorStage::Control,
            "synthetic",
            retryable,
            commit_state,
        );
        assert_eq!(error.retry_action(), expected, "{}", code.as_str());
        assert_eq!(
            serde_json::to_value(error.retry_action()).unwrap(),
            expected.as_str()
        );
    }
}

#[test]
fn html_fixture_does_not_promote_body_text_to_error_code() {
    let body = include_bytes!("fixtures/st/error-html.html");
    let error = http(
        StErrorStage::Generation,
        500,
        Some("text/html"),
        Some(body.as_slice()),
    );
    assert_eq!(error.code, StErrorCode::StGenerateUpstream5xx);
    assert_ne!(error.code.as_str(), std::str::from_utf8(body).unwrap());
    let detail = error.safe_detail.expect("safe detail");
    assert!(!detail.contains("Cookie"));
    assert!(detail.contains("sha256=") || detail.contains("len="));
    assert!(detail.contains("content-type=text/html"));

    let catalog = http(
        StErrorStage::Catalog,
        500,
        Some("text/html"),
        Some(body.as_slice()),
    );
    assert_eq!(catalog.code, StErrorCode::StCharacterListFailed);
}

#[test]
fn plaintext_fixture_does_not_use_body_as_code() {
    let body = include_bytes!("fixtures/st/error-plaintext.txt");
    let error = http(
        StErrorStage::Generation,
        500,
        Some("text/plain"),
        Some(body.as_slice()),
    );
    assert_eq!(error.code, StErrorCode::StGenerateUpstream5xx);
    assert_ne!(
        error.code.as_str(),
        std::str::from_utf8(body).unwrap().trim()
    );
}

#[test]
fn json_fixture_keeps_upstream_code_separate_from_st_error_code() {
    let body = include_bytes!("fixtures/st/error-http-json.json");
    let error = http(
        StErrorStage::Generation,
        500,
        Some("application/json"),
        Some(body.as_slice()),
    );
    assert_eq!(error.code, StErrorCode::StGenerateUpstream5xx);
    assert_eq!(
        error.upstream_code.as_deref(),
        Some("SYNTHETIC_UPSTREAM_ERROR")
    );
    assert_ne!(error.code.as_str(), "SYNTHETIC_UPSTREAM_ERROR");
}

#[test]
fn snapshot_404_uses_upstream_chat_not_found_not_commit_validation() {
    let body =
        br#"{"error":{"code":"ST_CHAT_NOT_FOUND","message":"chat not found","stage":"snapshot"}}"#;
    let error = http(
        StErrorStage::Snapshot,
        404,
        Some("application/json"),
        Some(body.as_slice()),
    );
    assert_eq!(error.code, StErrorCode::StChatNotFound);
    assert_eq!(error.stage, StErrorStage::Snapshot);
    assert_ne!(error.code, StErrorCode::StCommitValidationFailed);
}

#[test]
fn commit_labeled_404_still_keeps_upstream_chat_not_found() {
    let body =
        br#"{"error":{"code":"ST_CHAT_NOT_FOUND","message":"chat not found","stage":"snapshot"}}"#;
    let error = http(
        StErrorStage::Commit,
        404,
        Some("application/json"),
        Some(body.as_slice()),
    );
    assert_eq!(error.code, StErrorCode::StChatNotFound);
    assert_eq!(error.stage, StErrorStage::Snapshot);
    assert_ne!(error.code, StErrorCode::StCommitValidationFailed);
}

#[test]
fn http_200_object_payload_is_invalid_chat() {
    let body = include_bytes!("fixtures/st/chat-invalid-payload.json");
    let error = http(
        StErrorStage::Snapshot,
        200,
        Some("application/json"),
        Some(body.as_slice()),
    );
    assert_eq!(error.code, StErrorCode::StChatPayloadInvalid);
    assert!(!error.retryable);
}

#[test]
fn forbidden_is_session_rejected_and_not_retryable() {
    let error = http(StErrorStage::Session, 403, None, None);
    assert_eq!(error.code, StErrorCode::StSessionRejected);
    assert!(!error.retryable);
    assert_eq!(error.retry_action(), RetryAction::NoRetry);
}

#[test]
fn commit_conflict_uses_latest_snapshot_retry() {
    let error = http(StErrorStage::Commit, 409, None, None);
    assert_eq!(error.code, StErrorCode::StChatConflict);
    assert!(error.retryable);
    assert_eq!(error.commit_state, CommitState::NotApplied);
    assert_eq!(error.retry_action(), RetryAction::RetryFromLatestSnapshot);
}

#[test]
fn write_not_ready_control_message_matches_readiness_copy() {
    let error = map_st_error(facts(
        StErrorStage::Control,
        StFailureFacts::Control {
            code: StErrorCode::StWriteNotReady,
        },
    ));
    assert_eq!(error.code, StErrorCode::StWriteNotReady);
    assert!(error.safe_message.contains("未写入 SillyTavern"));
    assert!(!error.retryable);
    assert_eq!(error.retry_action(), RetryAction::NoRetry);
}

#[test]
fn display_and_detail_redact_secrets() {
    let body = b"Authorization: Bearer secret-token-value\nCookie: secret-cookie-value\n";
    let error = http(
        StErrorStage::Generation,
        500,
        Some("text/plain"),
        Some(body.as_slice()),
    );
    let rendered = error.to_string();
    assert!(!rendered.contains("Authorization"));
    assert!(!rendered.contains("Cookie"));
    assert!(!rendered.contains("secret-token-value"));
    assert!(!rendered.contains("secret-cookie-value"));
    match error.safe_detail {
        None => {}
        Some(detail) => {
            assert!(!detail.contains("secret-cookie-value"));
            assert!(detail.contains("[REDACTED]"));
        }
    }
}
