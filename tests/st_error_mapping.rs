use im_bridge::modules::bridge::errors::{CommitState, StBridgeError, StErrorCode, StErrorStage};

#[test]
fn all_required_stages_have_stable_wire_names() {
    let stages = [
        (StErrorStage::Control, "control"),
        (StErrorStage::Connect, "connect"),
        (StErrorStage::Session, "session"),
        (StErrorStage::Catalog, "catalog"),
        (StErrorStage::Snapshot, "snapshot"),
        (StErrorStage::Prompt, "prompt"),
        (StErrorStage::Generation, "generation"),
        (StErrorStage::Commit, "commit"),
        (StErrorStage::Delivery, "delivery"),
    ];
    for (stage, expected) in stages {
        assert_eq!(stage.as_str(), expected);
        assert_eq!(serde_json::to_value(stage).unwrap(), expected);
    }
}

#[test]
fn stable_error_codes_have_complete_wire_names() {
    let codes = [
        (StErrorCode::StConnectFailed, "ST_CONNECT_FAILED"),
        (StErrorCode::StRequestTimeout, "ST_REQUEST_TIMEOUT"),
        (StErrorCode::StCsrfFetchFailed, "ST_CSRF_FETCH_FAILED"),
        (StErrorCode::StCsrfMissing, "ST_CSRF_MISSING"),
        (StErrorCode::StSessionRejected, "ST_SESSION_REJECTED"),
        (
            StErrorCode::StCharacterListFailed,
            "ST_CHARACTER_LIST_FAILED",
        ),
        (StErrorCode::StChatListFailed, "ST_CHAT_LIST_FAILED"),
        (StErrorCode::StChatNotFound, "ST_CHAT_NOT_FOUND"),
        (StErrorCode::StChatPayloadInvalid, "ST_CHAT_PAYLOAD_INVALID"),
        (StErrorCode::StSettingsInvalid, "ST_SETTINGS_INVALID"),
        (StErrorCode::StGenerateRejected, "ST_GENERATE_REJECTED"),
        (
            StErrorCode::StGenerateRateLimited,
            "ST_GENERATE_RATE_LIMITED",
        ),
        (
            StErrorCode::StGenerateUpstream5xx,
            "ST_GENERATE_UPSTREAM_5XX",
        ),
        (
            StErrorCode::StGenerateIdleTimeout,
            "ST_GENERATE_IDLE_TIMEOUT",
        ),
        (
            StErrorCode::StGenerateHardTimeout,
            "ST_GENERATE_HARD_TIMEOUT",
        ),
        (
            StErrorCode::StGenerateStreamInvalid,
            "ST_GENERATE_STREAM_INVALID",
        ),
        (StErrorCode::StGenerateEmpty, "ST_GENERATE_EMPTY"),
        (
            StErrorCode::StGenerateStateUnknown,
            "ST_GENERATE_STATE_UNKNOWN",
        ),
        (
            StErrorCode::StConnectorUnavailable,
            "ST_CONNECTOR_UNAVAILABLE",
        ),
        (
            StErrorCode::StConnectorAuthFailed,
            "ST_CONNECTOR_AUTH_FAILED",
        ),
        (
            StErrorCode::StConnectorVersionMismatch,
            "ST_CONNECTOR_VERSION_MISMATCH",
        ),
        (
            StErrorCode::StConnectorMediaTypeRejected,
            "ST_CONNECTOR_MEDIA_TYPE_REJECTED",
        ),
        (StErrorCode::StWriteNotReady, "ST_WRITE_NOT_READY"),
        (StErrorCode::StTestScopeRequired, "ST_TEST_SCOPE_REQUIRED"),
        (StErrorCode::StPollerNotOwner, "ST_POLLER_NOT_OWNER"),
        (
            StErrorCode::StPollerEpochMismatch,
            "ST_POLLER_EPOCH_MISMATCH",
        ),
        (
            StErrorCode::StChatLocatorRejected,
            "ST_CHAT_LOCATOR_REJECTED",
        ),
        (
            StErrorCode::StChatIntegrityMissing,
            "ST_CHAT_INTEGRITY_MISSING",
        ),
        (StErrorCode::StChatConflict, "ST_CHAT_CONFLICT"),
        (StErrorCode::StOperationIdReused, "ST_OPERATION_ID_REUSED"),
        (StErrorCode::StBackupFailed, "ST_BACKUP_FAILED"),
        (
            StErrorCode::StBackupQuotaExceeded,
            "ST_BACKUP_QUOTA_EXCEEDED",
        ),
        (StErrorCode::StWriteVerifyFailed, "ST_WRITE_VERIFY_FAILED"),
        (
            StErrorCode::StCommitPayloadTooLarge,
            "ST_COMMIT_PAYLOAD_TOO_LARGE",
        ),
        (
            StErrorCode::StCommitValidationFailed,
            "ST_COMMIT_VALIDATION_FAILED",
        ),
        (StErrorCode::StCommitFailed, "ST_COMMIT_FAILED"),
        (StErrorCode::StCommitStateUnknown, "ST_COMMIT_STATE_UNKNOWN"),
        (StErrorCode::StDeliveryFailed, "ST_DELIVERY_FAILED"),
    ];
    for (code, expected) in codes {
        assert_eq!(code.as_str(), expected);
        assert_eq!(
            serde_json::to_value(code).unwrap(),
            serde_json::Value::String(expected.to_owned())
        );
    }
}

#[test]
fn bridge_error_keeps_commit_state_and_diagnostic_fields_typed() {
    let error = StBridgeError::new(
        StErrorCode::StChatConflict,
        StErrorStage::Commit,
        "synthetic chat conflict",
        true,
        CommitState::NotApplied,
    );
    assert_eq!(error.code_str(), "ST_CHAT_CONFLICT");
    assert_eq!(error.stage, StErrorStage::Commit);
    assert_eq!(error.commit_state, CommitState::NotApplied);
    assert!(error.safe_detail.is_none());
    let encoded = serde_json::to_value(error).unwrap();
    assert_eq!(encoded["commit_state"], "not_applied");
    assert_eq!(encoded["code"], "ST_CHAT_CONFLICT");
}

#[test]
fn failure_matrix_preserves_stage_retry_and_commit_state() {
    let cases = [
        (
            StErrorCode::StConnectFailed,
            StErrorStage::Connect,
            true,
            CommitState::NotStarted,
        ),
        (
            StErrorCode::StChatPayloadInvalid,
            StErrorStage::Snapshot,
            false,
            CommitState::NotStarted,
        ),
        (
            StErrorCode::StGenerateRateLimited,
            StErrorStage::Generation,
            true,
            CommitState::NotStarted,
        ),
        (
            StErrorCode::StGenerateUpstream5xx,
            StErrorStage::Generation,
            true,
            CommitState::NotStarted,
        ),
        (
            StErrorCode::StGenerateIdleTimeout,
            StErrorStage::Generation,
            true,
            CommitState::Unknown,
        ),
        (
            StErrorCode::StGenerateHardTimeout,
            StErrorStage::Generation,
            true,
            CommitState::Unknown,
        ),
        (
            StErrorCode::StChatConflict,
            StErrorStage::Commit,
            true,
            CommitState::NotApplied,
        ),
        (
            StErrorCode::StCommitStateUnknown,
            StErrorStage::Commit,
            false,
            CommitState::Unknown,
        ),
        (
            StErrorCode::StDeliveryFailed,
            StErrorStage::Delivery,
            true,
            CommitState::Applied,
        ),
    ];
    for (code, stage, retryable, commit_state) in cases {
        let error = StBridgeError::new(
            code,
            stage,
            "synthetic safe message",
            retryable,
            commit_state,
        );
        assert_eq!(error.code_str(), code.as_str());
        assert_eq!(error.stage, stage);
        assert_eq!(error.retryable, retryable);
        assert_eq!(error.commit_state, commit_state);
    }
}
