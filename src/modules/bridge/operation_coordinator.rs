use std::sync::Arc;

use sha2::Digest;

use crate::domain::st::{
    CommitStChat, PollerRuntimeFence, StChatLocator, StCommitResult, StCommitStatus,
};
use crate::error::AppResult;
use crate::modules::bridge::error_mapper::{map_st_error, StErrorFacts, StFailureFacts};
use crate::modules::bridge::errors::{
    CommitState, StBridgeError, StErrorCode, StErrorStage, StResult,
};
use crate::modules::bridge::operation_payload::{
    locator_hash, OperationPayloadEncryptor, OperationPayloadKeyProvider,
};
use crate::modules::bridge::operation_store::{
    ClaimOperationRequest, OperationClaimError, OperationStore,
};
use crate::modules::bridge::operations::{
    BridgeOperationRecord, BridgeOperationStatus, EncryptedOperationPayload, OperationAad,
    OperationRecoveryPayload,
};
use crate::seams::st_backend::StBackend;
use crate::seams::st_operation_journal::{StOperationJournal, StOperationJournalStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimDisposition {
    Fresh,
    Resume,
    AlreadyDelivered,
    Rejected,
}

#[derive(Debug, Clone)]
pub struct ClaimedOperation {
    pub disposition: ClaimDisposition,
    pub record: BridgeOperationRecord,
}

/// Coordinates durable operation state with the ST mutation boundary.
///
/// Every mutating command must use this object for claim, state transitions,
/// commit reconciliation, and terminal delivery.  The backend is intentionally
/// kept behind the existing StBackend seam; Telegram delivery is not part of
/// this type and must only run after `mark_committed` succeeds.
pub struct OperationCoordinator {
    backend: Arc<dyn StBackend>,
    store: Arc<OperationStore>,
    payload_encryptor: Option<Arc<OperationPayloadEncryptor>>,
    payload_key_provider: Option<Arc<OperationPayloadKeyProvider>>,
    journal: Arc<dyn StOperationJournal>,
}

impl OperationCoordinator {
    pub fn new(
        backend: Arc<dyn StBackend>,
        store: Arc<OperationStore>,
        payload_encryptor: Option<Arc<OperationPayloadEncryptor>>,
        journal: Arc<dyn StOperationJournal>,
    ) -> Self {
        Self {
            backend,
            store,
            payload_encryptor,
            payload_key_provider: None,
            journal,
        }
    }

    pub fn new_with_key_provider(
        backend: Arc<dyn StBackend>,
        store: Arc<OperationStore>,
        key_provider: Arc<OperationPayloadKeyProvider>,
        journal: Arc<dyn StOperationJournal>,
    ) -> Self {
        Self {
            backend,
            store,
            payload_encryptor: None,
            payload_key_provider: Some(key_provider),
            journal,
        }
    }

    pub fn backend(&self) -> &Arc<dyn StBackend> {
        &self.backend
    }

    pub fn store(&self) -> &Arc<OperationStore> {
        &self.store
    }

    pub fn payload_encryptor(&self) -> Option<&Arc<OperationPayloadEncryptor>> {
        self.payload_encryptor.as_ref()
    }

    pub async fn payload_encryptor_for_operation(
        &self,
        operation_id: &str,
    ) -> AppResult<Arc<OperationPayloadEncryptor>> {
        if let Some(provider) = self.payload_key_provider.as_ref() {
            return provider.for_operation(operation_id).await;
        }
        self.payload_encryptor.clone().ok_or_else(|| {
            crate::error::AppError::service_unavailable(
                "PAYLOAD_KEY_UNAVAILABLE",
                "operation payload key is unavailable",
            )
        })
    }

    async fn restore_payload_encryptor_for_operation(
        &self,
        operation_id: &str,
    ) -> AppResult<Arc<OperationPayloadEncryptor>> {
        if let Some(provider) = self.payload_key_provider.as_ref() {
            return provider.restore_for_operation(operation_id).await;
        }
        self.payload_encryptor.clone().ok_or_else(|| {
            crate::error::AppError::service_unavailable(
                "PAYLOAD_KEY_UNAVAILABLE",
                "operation payload key is unavailable",
            )
        })
    }

    pub fn decrypt_recovery_payload(
        &self,
        record: &BridgeOperationRecord,
        internal_bot_id: &str,
        fence: &PollerRuntimeFence,
    ) -> AppResult<OperationRecoveryPayload> {
        let Some(encryptor) = self.payload_encryptor.as_ref() else {
            return Err(crate::error::AppError::service_unavailable(
                "PAYLOAD_KEY_UNAVAILABLE",
                "operation payload key is unavailable",
            ));
        };
        let Some(encrypted) = record.payload.as_ref() else {
            return Err(crate::error::AppError::bad_request(
                "PAYLOAD_MISSING",
                "operation recovery payload is missing",
            ));
        };
        if record.id.trim().is_empty() || record.bot_id.trim().is_empty() {
            return Err(crate::error::AppError::bad_request(
                "PAYLOAD_AAD_INVALID",
                "operation payload identity is incomplete",
            ));
        }
        if !fence.is_valid()
            || internal_bot_id.trim().is_empty()
            || internal_bot_id != record.bot_id
            || fence.internal_bot_id != record.bot_id
        {
            return Err(crate::error::AppError::conflict(
                "PAYLOAD_AAD_INVALID",
                "operation payload runtime fence does not match the historical operation",
            ));
        }
        let aad = OperationAad::new_with_fence(
            record.id.clone(),
            locator_hash(
                &record.locator.handle,
                &record.locator.avatar,
                &record.locator.chat_file,
            ),
            record.operation_kind.clone(),
            record.actor_id.clone(),
            fence.clone(),
        );
        let plaintext = encryptor.decrypt(encrypted, &aad)?;
        let payload: OperationRecoveryPayload =
            serde_json::from_slice(&plaintext).map_err(|_| {
                crate::error::AppError::bad_request(
                    "PAYLOAD_RECOVERY_INVALID",
                    "operation recovery payload is invalid",
                )
            })?;
        payload.validate()?;
        if let Some(expected_digest) = record.mutation_digest.as_deref() {
            let actual = payload
                .mutation()
                .map(crate::modules::bridge::st_ops::mutation_digest)
                .unwrap_or_else(|| hex::encode(sha2::Sha256::digest(&plaintext)));
            if actual != expected_digest {
                return Err(crate::error::AppError::conflict(
                    "PAYLOAD_DIGEST_MISMATCH",
                    "operation recovery payload digest mismatch",
                ));
            }
        }
        Ok(payload)
    }

    pub async fn decrypt_recovery_payload_for_operation(
        &self,
        record: &BridgeOperationRecord,
        fence: &PollerRuntimeFence,
    ) -> AppResult<OperationRecoveryPayload> {
        let encryptor = self
            .restore_payload_encryptor_for_operation(&record.id)
            .await?;
        let Some(encrypted) = record.payload.as_ref() else {
            return Err(crate::error::AppError::bad_request(
                "PAYLOAD_MISSING",
                "operation recovery payload is missing",
            ));
        };
        if record.id.trim().is_empty() || record.bot_id.trim().is_empty() {
            return Err(crate::error::AppError::bad_request(
                "PAYLOAD_AAD_INVALID",
                "operation payload identity is incomplete",
            ));
        }
        if !fence.is_valid() || fence.internal_bot_id != record.bot_id {
            return Err(crate::error::AppError::conflict(
                "PAYLOAD_AAD_INVALID",
                "operation recovery fence does not match the historical operation",
            ));
        }
        let aad = OperationAad::new_with_fence(
            record.id.clone(),
            locator_hash(
                &record.locator.handle,
                &record.locator.avatar,
                &record.locator.chat_file,
            ),
            record.operation_kind.clone(),
            record.actor_id.clone(),
            fence.clone(),
        );
        let plaintext = encryptor.decrypt(encrypted, &aad)?;
        let payload: OperationRecoveryPayload =
            serde_json::from_slice(&plaintext).map_err(|_| {
                crate::error::AppError::bad_request(
                    "PAYLOAD_RECOVERY_INVALID",
                    "operation recovery payload is invalid",
                )
            })?;
        payload.validate()?;
        if let Some(expected_digest) = record.mutation_digest.as_deref() {
            let actual = payload
                .mutation()
                .map(crate::modules::bridge::st_ops::mutation_digest)
                .unwrap_or_else(|| hex::encode(sha2::Sha256::digest(&plaintext)));
            if actual != expected_digest {
                return Err(crate::error::AppError::conflict(
                    "PAYLOAD_DIGEST_MISMATCH",
                    "operation recovery payload digest mismatch",
                ));
            }
        }
        Ok(payload)
    }

    pub async fn replay_generated(
        &self,
        record: &BridgeOperationRecord,
        internal_bot_id: &str,
        fence: &PollerRuntimeFence,
    ) -> StResult<StCommitResult> {
        if !fence.is_valid()
            || internal_bot_id.trim().is_empty()
            || internal_bot_id != record.bot_id
            || fence.internal_bot_id != record.bot_id
        {
            return Err(bridge_error_from_app(
                crate::error::AppError::conflict(
                    "PAYLOAD_AAD_INVALID",
                    "operation replay bot identity does not match the historical operation",
                ),
                &record.id,
            ));
        }
        if record.status != BridgeOperationStatus::Generated {
            return Err(unknown_commit_error(
                &record.id,
                "only a generated operation can be replayed",
            ));
        }
        let payload = self
            .decrypt_recovery_payload_for_operation(record, fence)
            .await
            .map_err(|error| bridge_error_from_app(error, &record.id))?;
        self.store
            .mark_committing(&record.id)
            .await
            .map_err(|error| bridge_error_from_app(error, &record.id))?;
        let result = match payload {
            OperationRecoveryPayload::Create { command } => self.backend.create_chat(command).await,
            OperationRecoveryPayload::Append {
                mutation,
                expected_sha256,
                expected_integrity,
                fence,
            }
            | OperationRecoveryPayload::Replace {
                mutation,
                expected_sha256,
                expected_integrity,
                fence,
            }
            | OperationRecoveryPayload::Undo {
                mutation,
                expected_sha256,
                expected_integrity,
                fence,
            }
            | OperationRecoveryPayload::Compress {
                mutation,
                expected_sha256,
                expected_integrity,
                fence,
            } => {
                self.backend
                    .commit(CommitStChat {
                        operation_id: record.id.clone(),
                        locator: record.locator.clone(),
                        expected_sha256,
                        expected_integrity,
                        mutation,
                        fence,
                    })
                    .await
            }
        };
        match result {
            Ok(
                result @ StCommitResult {
                    status: StCommitStatus::Applied | StCommitStatus::AlreadyApplied,
                    ..
                },
            ) => {
                let encoded = serde_json::to_string(&result).map_err(|_| {
                    unknown_commit_error(&record.id, "connector result serialization failed")
                })?;
                self.store
                    .mark_committed_with_state(
                        &record.id,
                        &encoded,
                        crate::modules::bridge::operations::OperationCommitState::NotStarted,
                    )
                    .await
                    .map_err(|error| unknown_commit_error(&record.id, error.to_string()))?;
                Ok(result)
            }
            Ok(StCommitResult {
                status: StCommitStatus::NotApplied,
                ..
            }) => {
                let original = map_st_error(StErrorFacts {
                    stage: StErrorStage::Commit,
                    endpoint_class: Some("connector".into()),
                    operation_id: Some(record.id.clone()),
                    commit_state: CommitState::NotApplied,
                    attempt: 1,
                    duration_ms: None,
                    failure: StFailureFacts::Control {
                        code: StErrorCode::StCommitFailed,
                    },
                });
                let original_code = original.code;
                match self
                    .store
                    .mark_failed_with_state(
                        &record.id,
                        "commit",
                        StErrorCode::StCommitFailed.as_str(),
                        "replayed commit was not applied",
                        false,
                        crate::modules::bridge::operations::OperationCommitState::NotApplied,
                    )
                    .await
                {
                    Ok(_) => Err(original),
                    Err(persistence_error) => Err(operation_state_persistence_error(
                        &record.id,
                        original_code,
                        &persistence_error,
                        CommitState::NotApplied,
                    )),
                }
            }
            Ok(StCommitResult {
                status: StCommitStatus::Unknown,
                ..
            }) => {
                let original =
                    unknown_commit_error(&record.id, "replayed commit outcome is unknown");
                let original_code = original.code;
                match self.store.mark_commit_unknown(&record.id).await {
                    Ok(_) => Err(original),
                    Err(persistence_error) => Err(operation_state_persistence_error(
                        &record.id,
                        original_code,
                        &persistence_error,
                        CommitState::Unknown,
                    )),
                }
            }
            Err(error) if error.commit_state == CommitState::NotApplied => {
                let original_code = error.code;
                match self
                    .store
                    .mark_failed_with_state(
                        &record.id,
                        "commit",
                        StErrorCode::StCommitFailed.as_str(),
                        "replayed commit was rejected before mutation",
                        false,
                        crate::modules::bridge::operations::OperationCommitState::NotApplied,
                    )
                    .await
                {
                    Ok(_) => Err(error),
                    Err(persistence_error) => Err(operation_state_persistence_error(
                        &record.id,
                        original_code,
                        &persistence_error,
                        CommitState::NotApplied,
                    )),
                }
            }
            Err(_) => {
                let original =
                    unknown_commit_error(&record.id, "replayed commit outcome is unknown");
                let original_code = original.code;
                match self.store.mark_commit_unknown(&record.id).await {
                    Ok(_) => Err(original),
                    Err(persistence_error) => Err(operation_state_persistence_error(
                        &record.id,
                        original_code,
                        &persistence_error,
                        CommitState::Unknown,
                    )),
                }
            }
        }
    }

    pub async fn claim(
        &self,
        request: ClaimOperationRequest,
    ) -> Result<ClaimedOperation, OperationClaimError> {
        match self.store.claim_operation(request).await {
            Ok(record) => Ok(ClaimedOperation {
                disposition: ClaimDisposition::Fresh,
                record,
            }),
            Err(OperationClaimError::DuplicateClaim(existing)) => {
                let disposition = match (existing.status, existing.commit_state) {
                    (
                        BridgeOperationStatus::Delivered,
                        crate::modules::bridge::operations::OperationCommitState::Applied,
                    ) => ClaimDisposition::AlreadyDelivered,
                    (
                        BridgeOperationStatus::Committed,
                        crate::modules::bridge::operations::OperationCommitState::Applied,
                    ) => ClaimDisposition::Resume,
                    (
                        BridgeOperationStatus::Committed | BridgeOperationStatus::Delivered,
                        crate::modules::bridge::operations::OperationCommitState::NotApplied,
                    ) => ClaimDisposition::Rejected,
                    (
                        BridgeOperationStatus::Committed | BridgeOperationStatus::Delivered,
                        crate::modules::bridge::operations::OperationCommitState::Unknown,
                    ) => ClaimDisposition::Resume,
                    (_, crate::modules::bridge::operations::OperationCommitState::NotApplied)
                    | (BridgeOperationStatus::Conflict, _)
                    | (BridgeOperationStatus::Failed, _)
                    | (BridgeOperationStatus::Interrupted, _) => ClaimDisposition::Rejected,
                    // Pending, generating, generated, and committing rows must be
                    // resumed/reconciled under the original operation id.  A
                    // duplicate never confirms a mutation by itself.
                    _ => ClaimDisposition::Resume,
                };
                Ok(ClaimedOperation {
                    disposition,
                    record: *existing,
                })
            }
            Err(error) => Err(error),
        }
    }

    pub async fn mark_snapshot_ready(
        &self,
        operation_id: &str,
        source_sha256: Option<&str>,
        source_integrity: Option<&str>,
        source_size: Option<i64>,
        message_count: Option<i64>,
    ) -> StResult<BridgeOperationRecord> {
        self.store
            .update_snapshot_ready(
                operation_id,
                source_sha256,
                source_integrity,
                source_size,
                message_count,
            )
            .await
            .map_err(|error| bridge_error_from_app(error, operation_id))
    }

    pub async fn mark_generating(&self, operation_id: &str) -> StResult<BridgeOperationRecord> {
        self.store
            .update_generating(operation_id)
            .await
            .map_err(|error| bridge_error_from_app(error, operation_id))
    }

    pub async fn settle_generation_failure(
        &self,
        operation_id: &str,
        error: Box<StBridgeError>,
    ) -> Box<StBridgeError> {
        let original_code = error.code;
        match self
            .store
            .mark_failed(
                operation_id,
                error.stage.as_str(),
                error.code.as_str(),
                &error.safe_message,
                error.retryable,
            )
            .await
        {
            Ok(_) => error,
            Err(persistence_error) => operation_state_persistence_error(
                operation_id,
                original_code,
                &persistence_error,
                CommitState::NotStarted,
            ),
        }
    }

    pub async fn mark_committing(&self, operation_id: &str) -> StResult<BridgeOperationRecord> {
        self.store
            .mark_committing(operation_id)
            .await
            .map_err(|error| bridge_error_from_app(error, operation_id))
    }

    pub async fn mark_committed_result(
        &self,
        operation_id: &str,
        result: &StCommitResult,
        expected_commit_state: crate::modules::bridge::operations::OperationCommitState,
    ) -> StResult<BridgeOperationRecord> {
        let encoded = serde_json::to_string(result).map_err(|_| {
            unknown_commit_error(operation_id, "connector result serialization failed")
        })?;
        self.store
            .mark_committed_with_state(operation_id, &encoded, expected_commit_state)
            .await
            .map_err(|error| bridge_error_from_app(error, operation_id))
    }

    pub async fn mark_unknown(&self, operation_id: &str) -> StResult<BridgeOperationRecord> {
        self.store
            .mark_commit_unknown(operation_id)
            .await
            .map_err(|error| bridge_error_from_app(error, operation_id))
    }

    pub async fn mark_generated(
        &self,
        operation_id: &str,
        payload: &EncryptedOperationPayload,
        mutation_digest: &str,
    ) -> StResult<BridgeOperationRecord> {
        self.store
            .update_generated(operation_id, payload, mutation_digest)
            .await
            .map_err(|error| bridge_error_from_app(error, operation_id))
    }

    pub async fn create(
        &self,
        command: crate::domain::st::CreateStChat,
    ) -> StResult<StCommitResult> {
        let operation_id = command.operation_id.clone();
        self.store
            .mark_committing(&operation_id)
            .await
            .map_err(|error| bridge_error_from_app(error, &operation_id))?;

        let result = self.backend.create_chat(command).await;
        match result {
            Ok(
                result @ StCommitResult {
                    status: StCommitStatus::Applied | StCommitStatus::AlreadyApplied,
                    ..
                },
            ) => {
                let encoded = serde_json::to_string(&result).map_err(|_| {
                    unknown_commit_error(&operation_id, "connector result serialization failed")
                })?;
                if let Err(error) = self
                    .store
                    .mark_committed_with_state(
                        &operation_id,
                        &encoded,
                        crate::modules::bridge::operations::OperationCommitState::NotStarted,
                    )
                    .await
                {
                    match self.store.mark_commit_unknown(&operation_id).await {
                        Ok(_) => {
                            return Err(operation_state_persistence_error(
                                &operation_id,
                                StErrorCode::StCommitStateUnknown,
                                &error,
                                CommitState::Unknown,
                            ));
                        }
                        Err(persistence_error) => {
                            return Err(operation_state_persistence_error(
                                &operation_id,
                                StErrorCode::StCommitStateUnknown,
                                &persistence_error,
                                CommitState::Unknown,
                            ));
                        }
                    }
                }
                Ok(result)
            }
            Ok(StCommitResult {
                status: StCommitStatus::NotApplied,
                ..
            }) => {
                let original = map_st_error(StErrorFacts {
                    stage: StErrorStage::Commit,
                    endpoint_class: Some("connector".into()),
                    operation_id: Some(operation_id.clone()),
                    commit_state: CommitState::NotApplied,
                    attempt: 1,
                    duration_ms: None,
                    failure: StFailureFacts::Control {
                        code: StErrorCode::StCommitFailed,
                    },
                });
                let original_code = original.code;
                match self
                    .store
                    .mark_failed_with_state(
                        &operation_id,
                        "commit",
                        StErrorCode::StCommitFailed.as_str(),
                        "create was not applied",
                        false,
                        crate::modules::bridge::operations::OperationCommitState::NotApplied,
                    )
                    .await
                {
                    Ok(_) => Err(original),
                    Err(persistence_error) => Err(operation_state_persistence_error(
                        &operation_id,
                        original_code,
                        &persistence_error,
                        CommitState::NotApplied,
                    )),
                }
            }
            Ok(StCommitResult {
                status: StCommitStatus::Unknown,
                ..
            }) => {
                let original =
                    unknown_commit_error(&operation_id, "connector create outcome is not known");
                let original_code = original.code;
                match self.store.mark_commit_unknown(&operation_id).await {
                    Ok(_) => Err(original),
                    Err(persistence_error) => Err(operation_state_persistence_error(
                        &operation_id,
                        original_code,
                        &persistence_error,
                        CommitState::Unknown,
                    )),
                }
            }
            Err(error) if error.commit_state == CommitState::NotApplied => {
                let original_code = error.code;
                match self
                    .store
                    .mark_failed_with_state(
                        &operation_id,
                        "commit",
                        StErrorCode::StCommitFailed.as_str(),
                        "create was rejected before mutation",
                        false,
                        crate::modules::bridge::operations::OperationCommitState::NotApplied,
                    )
                    .await
                {
                    Ok(_) => Err(error),
                    Err(persistence_error) => Err(operation_state_persistence_error(
                        &operation_id,
                        original_code,
                        &persistence_error,
                        CommitState::NotApplied,
                    )),
                }
            }
            Err(_) => {
                let original =
                    unknown_commit_error(&operation_id, "connector create outcome is not known");
                let original_code = original.code;
                match self.store.mark_commit_unknown(&operation_id).await {
                    Ok(_) => Err(original),
                    Err(persistence_error) => Err(operation_state_persistence_error(
                        &operation_id,
                        original_code,
                        &persistence_error,
                        CommitState::Unknown,
                    )),
                }
            }
        }
    }

    pub async fn commit(
        &self,
        operation_id: &str,
        locator: StChatLocator,
        expected_sha256: String,
        expected_integrity: String,
        mutation: crate::domain::st::StMutation,
        fence: PollerRuntimeFence,
    ) -> StResult<StCommitResult> {
        if !fence.is_valid() {
            return Err(unknown_commit_error(
                operation_id,
                "runtime fence is invalid",
            ));
        }
        self.store
            .mark_committing(operation_id)
            .await
            .map_err(|error| bridge_error_from_app(error, operation_id))?;

        let result = self
            .backend
            .commit(CommitStChat {
                operation_id: operation_id.to_string(),
                locator,
                expected_sha256,
                expected_integrity,
                mutation,
                fence,
            })
            .await;

        match result {
            Ok(
                result @ StCommitResult {
                    status: StCommitStatus::Applied | StCommitStatus::AlreadyApplied,
                    ..
                },
            ) => {
                let encoded = serde_json::to_string(&result).map_err(|_| {
                    unknown_commit_error(operation_id, "connector result serialization failed")
                })?;
                if let Err(error) = self
                    .store
                    .mark_committed_with_state(
                        operation_id,
                        &encoded,
                        crate::modules::bridge::operations::OperationCommitState::NotStarted,
                    )
                    .await
                {
                    match self.store.mark_commit_unknown(operation_id).await {
                        Ok(_) => {
                            return Err(operation_state_persistence_error(
                                operation_id,
                                StErrorCode::StCommitStateUnknown,
                                &error,
                                CommitState::Unknown,
                            ));
                        }
                        Err(persistence_error) => {
                            return Err(operation_state_persistence_error(
                                operation_id,
                                StErrorCode::StCommitStateUnknown,
                                &persistence_error,
                                CommitState::Unknown,
                            ));
                        }
                    }
                }
                Ok(result)
            }
            Ok(StCommitResult {
                status: StCommitStatus::NotApplied,
                ..
            }) => {
                let original = map_st_error(StErrorFacts {
                    stage: StErrorStage::Commit,
                    endpoint_class: Some("connector".into()),
                    operation_id: Some(operation_id.to_string()),
                    commit_state: CommitState::NotApplied,
                    attempt: 1,
                    duration_ms: None,
                    failure: StFailureFacts::Control {
                        code: StErrorCode::StCommitFailed,
                    },
                });
                let original_code = original.code;
                match self
                    .store
                    .mark_failed_with_state(
                        operation_id,
                        "commit",
                        StErrorCode::StCommitFailed.as_str(),
                        "commit was not applied",
                        false,
                        crate::modules::bridge::operations::OperationCommitState::NotApplied,
                    )
                    .await
                {
                    Ok(_) => Err(original),
                    Err(persistence_error) => Err(operation_state_persistence_error(
                        operation_id,
                        original_code,
                        &persistence_error,
                        CommitState::NotApplied,
                    )),
                }
            }
            Ok(StCommitResult {
                status: StCommitStatus::Unknown,
                ..
            }) => {
                let original =
                    unknown_commit_error(operation_id, "connector commit outcome is not known");
                let original_code = original.code;
                match self.store.mark_commit_unknown(operation_id).await {
                    Ok(_) => Err(original),
                    Err(persistence_error) => Err(operation_state_persistence_error(
                        operation_id,
                        original_code,
                        &persistence_error,
                        CommitState::Unknown,
                    )),
                }
            }
            Err(error) if error.commit_state == CommitState::NotApplied => {
                let original_code = error.code;
                match self
                    .store
                    .mark_failed_with_state(
                        operation_id,
                        "commit",
                        StErrorCode::StCommitFailed.as_str(),
                        "commit was rejected before mutation",
                        false,
                        crate::modules::bridge::operations::OperationCommitState::NotApplied,
                    )
                    .await
                {
                    Ok(_) => Err(error),
                    Err(persistence_error) => Err(operation_state_persistence_error(
                        operation_id,
                        original_code,
                        &persistence_error,
                        CommitState::NotApplied,
                    )),
                }
            }
            Err(_) => {
                let original =
                    unknown_commit_error(operation_id, "connector commit outcome is not known");
                let original_code = original.code;
                match self.store.mark_commit_unknown(operation_id).await {
                    Ok(_) => Err(original),
                    Err(persistence_error) => Err(operation_state_persistence_error(
                        operation_id,
                        original_code,
                        &persistence_error,
                        CommitState::Unknown,
                    )),
                }
            }
        }
    }

    pub async fn mark_delivered(&self, operation_id: &str) -> StResult<BridgeOperationRecord> {
        self.store
            .mark_delivered(operation_id)
            .await
            .map_err(|error| bridge_error_from_app(error, operation_id))
    }

    /// Reconcile a committing/unknown operation using the authenticated
    /// connector journal.  A missing entry or a prepared/unknown entry never
    /// becomes a confirmed commit.
    pub async fn reconcile_commit(
        &self,
        record: &BridgeOperationRecord,
        locator_hash: &str,
    ) -> AppResult<Option<BridgeOperationRecord>> {
        if matches!(
            record.status,
            BridgeOperationStatus::Committed | BridgeOperationStatus::Delivered
        ) && record.commit_state
            == crate::modules::bridge::operations::OperationCommitState::Applied
        {
            return Ok(Some(record.clone()));
        }
        if record.status != BridgeOperationStatus::Committing {
            return Ok(None);
        }
        let Some(mutation_digest) = record.mutation_digest.as_deref() else {
            return Ok(None);
        };
        let Some(journal) = self.journal.lookup(&record.id).await? else {
            return Ok(None);
        };
        if !journal.identity_matches(&record.id, locator_hash, mutation_digest) {
            return Err(crate::error::AppError::conflict(
                "ST_OPERATION_ID_REUSED",
                "operation journal identity does not match",
            ));
        }
        if journal.status != StOperationJournalStatus::Applied {
            return Ok(None);
        }
        let result = serde_json::json!({
            "status": "already_applied",
            "newSha256": journal.after_sha256,
            "newIntegrity": journal.after_integrity,
            "byteLength": journal.after_byte_length,
            "messageCount": journal.after_message_count,
        })
        .to_string();
        let expected = record.commit_state;
        self.store
            .mark_committed_with_state(&record.id, &result, expected)
            .await
            .map(Some)
    }
}

fn bridge_error_from_app(
    error: crate::error::AppError,
    operation_id: &str,
) -> Box<crate::modules::bridge::errors::StBridgeError> {
    let mut mapped = crate::modules::bridge::errors::StBridgeError::new(
        StErrorCode::StCommitFailed,
        StErrorStage::Control,
        error.message,
        false,
        CommitState::NotStarted,
    );
    mapped.operation_id = Some(operation_id.to_string());
    Box::new(mapped)
}

fn operation_state_persistence_error(
    operation_id: &str,
    original_code: StErrorCode,
    persistence_error: &crate::error::AppError,
    commit_state: CommitState,
) -> Box<crate::modules::bridge::errors::StBridgeError> {
    tracing::error!(
        operation_id = %operation_id,
        original_code = %original_code,
        persistence_code = %persistence_error.code,
        "operation state persistence failed"
    );
    let mut error = crate::modules::bridge::errors::StBridgeError::new(
        StErrorCode::StCommitStateUnknown,
        StErrorStage::Control,
        format!(
            "operation state persistence failed (original error code {}; persistence error code {})",
            original_code.as_str(),
            persistence_error.code,
        ),
        false,
        commit_state,
    );
    error.operation_id = Some(operation_id.to_string());
    error.safe_detail = Some(format!(
        "original_code={}; persistence_code={}",
        original_code.as_str(),
        persistence_error.code,
    ));
    Box::new(error)
}

fn unknown_commit_error(
    operation_id: &str,
    detail: impl Into<String>,
) -> Box<crate::modules::bridge::errors::StBridgeError> {
    let mut error = crate::modules::bridge::errors::StBridgeError::new(
        StErrorCode::StCommitStateUnknown,
        StErrorStage::Commit,
        "SillyTavern commit state is unknown; reconciliation is required",
        false,
        CommitState::Unknown,
    );
    error.operation_id = Some(operation_id.to_string());
    error.safe_detail = crate::modules::bridge::redaction::redact_detail(&detail.into());
    Box::new(error)
}
