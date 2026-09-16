use async_trait::async_trait;
use sha2::Digest;

use crate::clock::{now_rfc3339, now_unix_ms};
use crate::domain::identity::Actor;
use crate::domain::st::{
    CreateStChat, PollerRuntimeFence, StCharacterSummary, StChatLocator, StChatSnapshot,
    StCommitStatus, StGenerationRequest, StMutation, StWriteMode, StWriteScope,
};
use crate::modules::bridge::channel_context::ChannelContextStore;
use crate::modules::bridge::error_mapper::{map_st_error, StErrorFacts, StFailureFacts};
use crate::modules::bridge::errors::{CommitState, StErrorCode, StErrorStage, StResult};
use crate::modules::bridge::locator_locks::LocatorLockManager;
use crate::modules::bridge::operation_coordinator::{
    ClaimDisposition, ClaimedOperation, OperationCoordinator,
};
use crate::modules::bridge::operation_payload::locator_hash as operation_locator_hash;
use crate::modules::bridge::operation_store::{
    ClaimOperationRequest, OperationClaimError, OperationStore,
};
use crate::modules::bridge::operations;
use crate::modules::bridge::poller_ownership::PollerOwnershipGuard;
use crate::modules::bridge::st_ops;
use crate::seams::st_backend::StBackend;
use crate::seams::st_bridge_engine::{
    BridgeOperationOrigin, StBridgeCommand, StBridgeEngine, StBridgeOutcome, StBridgeQuery,
    StBridgeView,
};

#[derive(Clone)]
pub struct SidecarBridgeEngine {
    backend: std::sync::Arc<dyn StBackend>,
    channels: ChannelContextStore,
    context_key: String,
    operation_store: Option<std::sync::Arc<OperationStore>>,
    operation_coordinator: Option<std::sync::Arc<OperationCoordinator>>,
    ownership_guard: Option<std::sync::Arc<PollerOwnershipGuard>>,
    locator_locks: std::sync::Arc<LocatorLockManager>,
    generation_semaphore: std::sync::Arc<tokio::sync::Semaphore>,
    generation_budget_valid: bool,
}

impl SidecarBridgeEngine {
    pub fn new(backend: std::sync::Arc<dyn StBackend>, channels: ChannelContextStore) -> Self {
        Self {
            backend,
            channels,
            context_key: "telegram".into(),
            operation_store: None,
            operation_coordinator: None,
            ownership_guard: None,
            locator_locks: std::sync::Arc::new(LocatorLockManager::new(
                2048,
                std::time::Duration::from_secs(900),
            )),
            generation_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(8)),
            generation_budget_valid: true,
        }
    }

    pub fn with_context_key(&self, context_key: &str) -> Self {
        Self {
            backend: self.backend.clone(),
            channels: self.channels.clone(),
            context_key: context_key.to_string(),
            operation_store: self.operation_store.clone(),
            operation_coordinator: self.operation_coordinator.clone(),
            ownership_guard: self.ownership_guard.clone(),
            locator_locks: self.locator_locks.clone(),
            generation_semaphore: self.generation_semaphore.clone(),
            generation_budget_valid: self.generation_budget_valid,
        }
    }

    pub fn with_operation_store(self, store: std::sync::Arc<OperationStore>) -> Self {
        Self {
            operation_store: Some(store),
            ..self
        }
    }

    pub fn with_required_coordinator(
        self,
        coordinator: std::sync::Arc<OperationCoordinator>,
    ) -> Self {
        Self {
            operation_store: Some(coordinator.store().clone()),
            operation_coordinator: Some(coordinator),
            ..self
        }
    }

    pub fn with_ownership_guard(self, guard: std::sync::Arc<PollerOwnershipGuard>) -> Self {
        Self {
            ownership_guard: Some(guard),
            ..self
        }
    }

    pub fn operation_coordinator(&self) -> Option<&std::sync::Arc<OperationCoordinator>> {
        self.operation_coordinator.as_ref()
    }

    /// Claim an operation only from a durable, explicit origin.  This is the
    /// sole engine entry point that accepts Telegram identity; no operation id
    /// or context string is parsed to infer bot/update identity.
    pub async fn claim_operation_with_origin(
        &self,
        actor: &Actor,
        operation_id: &str,
        operation_kind: &str,
        locator: &StChatLocator,
        origin: BridgeOperationOrigin,
    ) -> StResult<ClaimedOperation> {
        if operation_id.trim().is_empty() || !origin.is_valid() {
            return Err(self.control_error(
                StErrorCode::StChatLocatorRejected,
                Some(operation_id.to_string()),
            ));
        }
        let Some(coordinator) = self.operation_coordinator.as_ref() else {
            return Err(
                self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.to_string()))
            );
        };
        coordinator
            .claim(ClaimOperationRequest {
                id: operation_id.to_string(),
                actor_id: actor.account.id.clone(),
                bot_id: origin.internal_bot_id,
                telegram_update_id: origin.telegram_update_id,
                channel_context_key: origin.channel_context_key,
                operation_kind: operation_kind.to_string(),
                locator: locator.clone(),
                request_id: operation_id.to_string(),
                trace_id: operation_id.to_string(),
            })
            .await
            .map_err(|error| match error {
                OperationClaimError::DuplicateClaim(record) => {
                    let state = match (record.status, record.commit_state) {
                        (
                            operations::BridgeOperationStatus::Committed,
                            operations::OperationCommitState::Applied,
                        )
                        | (
                            operations::BridgeOperationStatus::Delivered,
                            operations::OperationCommitState::Applied,
                        ) => CommitState::Applied,
                        (_, operations::OperationCommitState::Unknown) => CommitState::Unknown,
                        (_, operations::OperationCommitState::NotApplied) => {
                            CommitState::NotApplied
                        }
                        _ => CommitState::NotStarted,
                    };
                    let code = match state {
                        CommitState::Applied => StErrorCode::StCommitFailed,
                        CommitState::Unknown => StErrorCode::StCommitStateUnknown,
                        CommitState::NotApplied => StErrorCode::StCommitFailed,
                        CommitState::NotStarted => StErrorCode::StOperationIdReused,
                    };
                    let mut mapped = map_st_error(StErrorFacts {
                        stage: StErrorStage::Control,
                        endpoint_class: Some("bridge".into()),
                        operation_id: Some(operation_id.to_string()),
                        commit_state: state,
                        attempt: 1,
                        duration_ms: None,
                        failure: StFailureFacts::Control { code },
                    });
                    mapped.safe_detail = None;
                    mapped
                }
                OperationClaimError::Failed(error) => {
                    Box::new(crate::modules::bridge::errors::StBridgeError::new(
                        StErrorCode::StOperationIdReused,
                        StErrorStage::Control,
                        error.message,
                        false,
                        CommitState::NotStarted,
                    ))
                }
            })
    }

    pub fn with_generation_budget(self, max_concurrent: usize) -> Self {
        Self {
            generation_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(
                max_concurrent.max(1),
            )),
            generation_budget_valid: max_concurrent > 0,
            ..self
        }
    }

    pub fn operation_store(&self) -> Option<&std::sync::Arc<OperationStore>> {
        self.operation_store.as_ref()
    }

    fn locator_lock_key(locator: &StChatLocator) -> String {
        format!(
            "{}:{}:{}",
            locator.handle, locator.avatar, locator.chat_file
        )
    }

    async fn lock_locator(
        &self,
        locator: &StChatLocator,
    ) -> StResult<tokio::sync::OwnedMutexGuard<()>> {
        self.locator_locks
            .acquire(&Self::locator_lock_key(locator))
            .await
            .map_err(|_| {
                map_st_error(StErrorFacts {
                    stage: StErrorStage::Control,
                    endpoint_class: Some("bridge".into()),
                    operation_id: None,
                    commit_state: CommitState::NotStarted,
                    attempt: 1,
                    duration_ms: None,
                    failure: StFailureFacts::Control {
                        code: StErrorCode::StChatConflict,
                    },
                })
            })
    }

    async fn acquire_generation_permit(&self) -> StResult<tokio::sync::SemaphorePermit<'_>> {
        if !self.generation_budget_valid {
            return Err(map_st_error(StErrorFacts {
                stage: StErrorStage::Generation,
                endpoint_class: Some("bridge".into()),
                operation_id: None,
                commit_state: CommitState::NotStarted,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::Control {
                    code: StErrorCode::StGenerateRateLimited,
                },
            }));
        }
        self.generation_semaphore.acquire().await.map_err(|_| {
            map_st_error(StErrorFacts {
                stage: StErrorStage::Generation,
                endpoint_class: Some("bridge".into()),
                operation_id: None,
                commit_state: CommitState::NotStarted,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::Control {
                    code: StErrorCode::StGenerateRateLimited,
                },
            })
        })
    }

    async fn claim_for_origin(
        &self,
        actor: &Actor,
        operation_id: &str,
        operation_kind: &str,
        locator: &StChatLocator,
        origin: BridgeOperationOrigin,
    ) -> StResult<ClaimedOperation> {
        self.claim_operation_with_origin(actor, operation_id, operation_kind, locator, origin)
            .await
    }

    async fn require_generation_fence(&self, operation_id: &str) -> StResult<()> {
        let guard = self.ownership_guard.as_ref().ok_or_else(|| {
            self.control_error(
                StErrorCode::StPollerEpochMismatch,
                Some(operation_id.to_string()),
            )
        })?;
        guard.assert_generation_fence().await.map_err(|_| {
            self.control_error(
                StErrorCode::StPollerEpochMismatch,
                Some(operation_id.to_string()),
            )
        })
    }

    fn runtime_fence(&self, operation_id: &str) -> StResult<PollerRuntimeFence> {
        self.ownership_guard
            .as_ref()
            .and_then(|guard| guard.runtime_fence())
            .ok_or_else(|| {
                self.control_error(
                    StErrorCode::StPollerEpochMismatch,
                    Some(operation_id.to_string()),
                )
            })
    }

    async fn require_commit_fence(&self, operation_id: &str) -> StResult<()> {
        let guard = self.ownership_guard.as_ref().ok_or_else(|| {
            self.control_error(
                StErrorCode::StPollerEpochMismatch,
                Some(operation_id.to_string()),
            )
        })?;
        guard.assert_commit_fence().await.map_err(|_| {
            self.control_error(
                StErrorCode::StPollerEpochMismatch,
                Some(operation_id.to_string()),
            )
        })
    }

    fn resume_or_replay(
        &self,
        operation_id: &str,
        _operation_kind: &str,
        claimed: &ClaimedOperation,
    ) -> StResult<bool> {
        match claimed.disposition {
            ClaimDisposition::Fresh => Ok(false),
            ClaimDisposition::AlreadyDelivered => Ok(true),
            ClaimDisposition::Rejected => Err(self.control_error(
                StErrorCode::StOperationIdReused,
                Some(operation_id.to_string()),
            )),
            ClaimDisposition::Resume => {
                if matches!(
                    (claimed.record.status, claimed.record.commit_state),
                    (
                        operations::BridgeOperationStatus::Committed
                            | operations::BridgeOperationStatus::Delivered,
                        operations::OperationCommitState::Applied
                    )
                ) {
                    return Ok(true);
                }
                if matches!(
                    claimed.record.commit_state,
                    operations::OperationCommitState::Unknown
                ) {
                    return Err(self.control_error(
                        StErrorCode::StCommitStateUnknown,
                        Some(operation_id.to_string()),
                    ));
                }
                if matches!(
                    claimed.record.status,
                    operations::BridgeOperationStatus::Generated
                        | operations::BridgeOperationStatus::Committing
                ) {
                    return Err(self.control_error(
                        StErrorCode::StCommitStateUnknown,
                        Some(operation_id.to_string()),
                    ));
                }
                Ok(false)
            }
        }
    }

    async fn persist_create_payload(
        &self,
        actor_id: &str,
        operation_id: &str,
        locator: &StChatLocator,
        command: &CreateStChat,
        _origin: &BridgeOperationOrigin,
    ) -> StResult<()> {
        let coordinator = self.operation_coordinator.as_ref().ok_or_else(|| {
            self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.to_string()))
        })?;
        let fence = self.runtime_fence(operation_id)?;
        if command.fence != fence {
            return Err(self.control_error(
                StErrorCode::StPollerEpochMismatch,
                Some(operation_id.to_string()),
            ));
        }
        let payload = operations::OperationRecoveryPayload::Create {
            command: command.clone(),
        };
        payload.validate().map_err(|_| {
            self.control_error(
                StErrorCode::StCommitValidationFailed,
                Some(operation_id.to_string()),
            )
        })?;
        let plaintext = serde_json::to_vec(&payload).map_err(|_| {
            self.control_error(
                StErrorCode::StCommitValidationFailed,
                Some(operation_id.to_string()),
            )
        })?;
        let aad = operations::OperationAad::new_with_fence(
            operation_id,
            operation_locator_hash(&locator.handle, &locator.avatar, &locator.chat_file),
            operations::KIND_START,
            actor_id,
            fence.clone(),
        );
        let encryptor = coordinator
            .payload_encryptor_for_operation(operation_id)
            .await
            .map_err(|_| {
                self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.to_string()))
            })?;
        let encrypted = encryptor.encrypt(&plaintext, &aad).map_err(|_| {
            self.control_error(
                StErrorCode::StCommitValidationFailed,
                Some(operation_id.to_string()),
            )
        })?;
        let digest = hex::encode(sha2::Sha256::digest(&plaintext));
        coordinator
            .mark_generated(operation_id, &encrypted, &digest)
            .await
            .map(|_| ())
    }

    // Recovery persistence needs the full actor, operation, locator, snapshot, and mutation context.
    #[allow(clippy::too_many_arguments)]
    async fn persist_generated_payload(
        &self,
        actor_id: &str,
        operation_id: &str,
        operation_kind: &str,
        locator: &StChatLocator,
        snapshot: &StChatSnapshot,
        mutation: &StMutation,
        _origin: &BridgeOperationOrigin,
    ) -> StResult<()> {
        let coordinator = self.operation_coordinator.as_ref().ok_or_else(|| {
            self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.to_string()))
        })?;
        let fence = self.runtime_fence(operation_id)?;
        let payload = match operation_kind {
            operations::KIND_SEND => operations::OperationRecoveryPayload::Append {
                mutation: mutation.clone(),
                expected_sha256: snapshot.source_sha256.clone(),
                expected_integrity: snapshot.source_integrity.clone(),
                fence: fence.clone(),
            },
            operations::KIND_REGENERATE => operations::OperationRecoveryPayload::Replace {
                mutation: mutation.clone(),
                expected_sha256: snapshot.source_sha256.clone(),
                expected_integrity: snapshot.source_integrity.clone(),
                fence: fence.clone(),
            },
            operations::KIND_UNDO | operations::KIND_REVOKE => {
                operations::OperationRecoveryPayload::Undo {
                    mutation: mutation.clone(),
                    expected_sha256: snapshot.source_sha256.clone(),
                    expected_integrity: snapshot.source_integrity.clone(),
                    fence: fence.clone(),
                }
            }
            operations::KIND_COMPRESS => operations::OperationRecoveryPayload::Compress {
                mutation: mutation.clone(),
                expected_sha256: snapshot.source_sha256.clone(),
                expected_integrity: snapshot.source_integrity.clone(),
                fence: fence.clone(),
            },
            _ => {
                return Err(self.control_error(
                    StErrorCode::StCommitValidationFailed,
                    Some(operation_id.to_string()),
                ))
            }
        };
        payload.validate().map_err(|_| {
            self.control_error(
                StErrorCode::StCommitValidationFailed,
                Some(operation_id.to_string()),
            )
        })?;
        let plaintext = serde_json::to_vec(&payload).map_err(|_| {
            self.control_error(
                StErrorCode::StCommitValidationFailed,
                Some(operation_id.to_string()),
            )
        })?;
        let aad = operations::OperationAad::new_with_fence(
            operation_id,
            operation_locator_hash(&locator.handle, &locator.avatar, &locator.chat_file),
            operation_kind,
            actor_id,
            fence.clone(),
        );
        let encryptor = coordinator
            .payload_encryptor_for_operation(operation_id)
            .await
            .map_err(|_| {
                self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.to_string()))
            })?;
        let encrypted = encryptor.encrypt(&plaintext, &aad).map_err(|_| {
            self.control_error(
                StErrorCode::StCommitValidationFailed,
                Some(operation_id.to_string()),
            )
        })?;
        coordinator
            .mark_generated(operation_id, &encrypted, &st_ops::mutation_digest(mutation))
            .await
            .map(|_| ())
    }

    async fn start_chat(
        &self,
        actor: &Actor,
        locator: StChatLocator,
        client_operation_id: String,
        origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome> {
        let _locator_guard = self.lock_locator(&locator).await?;
        let workspace_id = actor.require_workspace().map_err(|_| {
            map_st_error(StErrorFacts {
                stage: StErrorStage::Control,
                endpoint_class: Some("bridge".into()),
                operation_id: None,
                commit_state: CommitState::NotStarted,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::Control {
                    code: StErrorCode::StChatLocatorRejected,
                },
            })
        })?;
        let status = self.backend.probe().await?;
        if !status.capabilities.mode.allows_write() {
            return self.write_frozen();
        }
        if locator.avatar.trim().is_empty() || locator.character_name.trim().is_empty() {
            return Err(map_st_error(StErrorFacts {
                stage: StErrorStage::Control,
                endpoint_class: Some("bridge".into()),
                operation_id: None,
                commit_state: CommitState::NotStarted,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::Control {
                    code: StErrorCode::StChatLocatorRejected,
                },
            }));
        }
        let operation_id = if client_operation_id.trim().is_empty() {
            crate::ids::new_id()
        } else {
            client_operation_id
        };
        let chat_file = test_chat_file(&locator.character_name);
        let created_locator = StChatLocator {
            handle: if locator.handle.trim().is_empty() {
                status.handle.clone()
            } else {
                locator.handle
            },
            avatar: locator.avatar,
            character_name: locator.character_name.clone(),
            chat_file,
        };
        let claimed = self
            .claim_for_origin(
                actor,
                &operation_id,
                operations::KIND_START,
                &created_locator,
                origin.clone(),
            )
            .await?;
        if self.resume_or_replay(&operation_id, operations::KIND_START, &claimed)? {
            return Ok(committed_outcome(
                operation_id,
                "该新建会话已完成，未重复创建文件。",
            ));
        }
        let create_command = CreateStChat {
            operation_id: operation_id.clone(),
            locator: created_locator.clone(),
            opening_message: opening_message(&locator.character_name),
            scope: StWriteScope::TestChat,
            fence: self.runtime_fence(&operation_id)?,
        };
        let coordinator = self.operation_coordinator.as_ref().ok_or_else(|| {
            self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.clone()))
        })?;
        coordinator
            .mark_snapshot_ready(&operation_id, None, None, None, None)
            .await?;
        self.persist_create_payload(
            &actor.account.id,
            &operation_id,
            &created_locator,
            &create_command,
            &origin,
        )
        .await?;
        self.require_commit_fence(&operation_id).await?;
        coordinator.create(create_command).await?;
        self.channels
            .select_chat(
                &actor.account.id,
                workspace_id,
                &self.context_key,
                &created_locator,
            )
            .await
            .map_err(|_| {
                map_st_error(StErrorFacts {
                    stage: StErrorStage::Control,
                    endpoint_class: Some("bridge".into()),
                    operation_id: Some(operation_id.clone()),
                    commit_state: CommitState::Applied,
                    attempt: 1,
                    duration_ms: None,
                    failure: StFailureFacts::Control {
                        code: StErrorCode::StChatLocatorRejected,
                    },
                })
            })?;
        Ok(committed_outcome(
            operation_id,
            format!("已新建测试会话：{}", created_locator.chat_file),
        ))
    }

    fn control_error(
        &self,
        code: StErrorCode,
        operation_id: Option<String>,
    ) -> Box<crate::modules::bridge::errors::StBridgeError> {
        map_st_error(StErrorFacts {
            stage: StErrorStage::Control,
            endpoint_class: Some("bridge".into()),
            operation_id,
            commit_state: CommitState::NotStarted,
            attempt: 1,
            duration_ms: None,
            failure: StFailureFacts::Control { code },
        })
    }

    async fn require_write_for_locator(
        &self,
        locator: &StChatLocator,
    ) -> StResult<crate::domain::st::StStatus> {
        let status = self.backend.probe().await?;
        match status.capabilities.mode {
            StWriteMode::Disabled | StWriteMode::ReadOnly => {
                Err(self.control_error(StErrorCode::StWriteNotReady, None))
            }
            StWriteMode::TestWrite => {
                if locator.chat_file.starts_with("IMBridge-Test-") {
                    Ok(status)
                } else {
                    Err(self.control_error(StErrorCode::StTestScopeRequired, None))
                }
            }
            StWriteMode::ProductionWrite => {
                if locator.chat_file.trim().is_empty() {
                    Err(self.control_error(StErrorCode::StChatLocatorRejected, None))
                } else {
                    Ok(status)
                }
            }
        }
    }

    async fn load_character(&self, locator: &StChatLocator) -> StResult<StCharacterSummary> {
        let characters = self.backend.list_characters().await?;
        characters
            .into_iter()
            .find(|item| item.avatar == locator.avatar)
            .ok_or_else(|| self.control_error(StErrorCode::StChatLocatorRejected, None))
    }

    async fn commit_mutation(
        &self,
        operation_id: String,
        locator: StChatLocator,
        snapshot: &StChatSnapshot,
        mutation: StMutation,
        reply_text: String,
    ) -> StResult<StBridgeOutcome> {
        self.require_commit_fence(&operation_id).await?;
        let fence = self.runtime_fence(&operation_id)?;
        let coordinator = self.operation_coordinator.as_ref().ok_or_else(|| {
            self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.clone()))
        })?;
        let result = coordinator
            .commit(
                &operation_id,
                locator,
                snapshot.source_sha256.clone(),
                snapshot.source_integrity.clone(),
                mutation,
                fence,
            )
            .await?;
        if !matches!(
            result.status,
            StCommitStatus::Applied | StCommitStatus::AlreadyApplied
        ) {
            return Err(map_st_error(StErrorFacts {
                stage: StErrorStage::Commit,
                endpoint_class: Some("connector".into()),
                operation_id: Some(operation_id),
                commit_state: CommitState::NotApplied,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::Control {
                    code: StErrorCode::StCommitFailed,
                },
            }));
        }
        Ok(committed_outcome(operation_id, reply_text))
    }

    async fn send_message(
        &self,
        actor: &Actor,
        locator: StChatLocator,
        text: String,
        client_operation_id: String,
        model_override: Option<String>,
        origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome> {
        self.send_message_with_context(
            actor,
            locator,
            text,
            client_operation_id,
            model_override,
            None,
            origin,
        )
        .await
    }

    // The sidecar command path keeps transport, operation, and progress context explicit.
    #[allow(clippy::too_many_arguments)]
    async fn send_message_with_context(
        &self,
        actor: &Actor,
        locator: StChatLocator,
        text: String,
        client_operation_id: String,
        model_override: Option<String>,
        context: Option<crate::seams::bridge_progress::BridgeExecutionContext>,
        origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome> {
        let _locator_guard = self.lock_locator(&locator).await?;
        self.require_write_for_locator(&locator).await?;
        let operation_id = if client_operation_id.trim().is_empty() {
            context
                .as_ref()
                .map(|c| c.operation_id.clone())
                .unwrap_or_else(crate::ids::new_id)
        } else {
            client_operation_id
        };
        if let Some(command_context) = context.as_ref() {
            if command_context.operation_id != operation_id {
                return Err(
                    self.control_error(StErrorCode::StOperationIdReused, Some(operation_id))
                );
            }
        }
        let claimed = self
            .claim_for_origin(
                actor,
                &operation_id,
                operations::KIND_SEND,
                &locator,
                origin.clone(),
            )
            .await?;
        if self.resume_or_replay(&operation_id, operations::KIND_SEND, &claimed)? {
            return Ok(committed_outcome(
                operation_id,
                "该发送操作已完成，未再次写入 SillyTavern。",
            ));
        }
        let snapshot = self.backend.snapshot(&locator).await?;
        let coordinator = self.operation_coordinator.as_ref().ok_or_else(|| {
            self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.clone()))
        })?;
        coordinator
            .mark_snapshot_ready(
                &operation_id,
                Some(&snapshot.source_sha256),
                Some(&snapshot.source_integrity),
                Some(snapshot.source_byte_length as i64),
                Some(snapshot.source_message_count as i64),
            )
            .await?;
        coordinator.mark_generating(&operation_id).await?;
        let cancel = context
            .as_ref()
            .map(|item| item.cancel.clone())
            .unwrap_or_default();
        let progress = context.and_then(|c| c.progress);
        let generation = async {
            let character = self.load_character(&locator).await?;
            let settings = self.backend.generation_settings().await?;
            let channel_context = self
                .channels
                .load(&actor.account.id, &self.context_key)
                .await
                .map_err(|_| {
                    self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.clone()))
                })?;
            let model_id = model_override.or(channel_context.chat_model_id);
            self.require_generation_fence(&operation_id).await?;
            let _gen_permit = self.acquire_generation_permit().await?;
            let generated = self
                .backend
                .stream_generate(
                    StGenerationRequest {
                        model_id,
                        messages: st_ops::prompt_messages(
                            &character,
                            &settings.username,
                            &snapshot.parsed_chat,
                            Some(&text),
                            false,
                        ),
                        temperature: Some(settings.temperature),
                        top_p: Some(settings.top_p),
                        max_tokens: Some(settings.max_tokens),
                    },
                    progress.clone(),
                    cancel.clone(),
                )
                .await?;
            let mutation = st_ops::append_mutation(
                &settings.username,
                &character.name,
                &text,
                &generated.text,
            );
            Ok((mutation, generated.text))
        }
        .await;
        let (mutation, generated_text) = match generation {
            Ok(result) => result,
            Err(error) => {
                return Err(coordinator
                    .settle_generation_failure(&operation_id, error)
                    .await)
            }
        };
        self.persist_generated_payload(
            &actor.account.id,
            &operation_id,
            operations::KIND_SEND,
            &locator,
            &snapshot,
            &mutation,
            &origin,
        )
        .await?;
        let outcome = self
            .commit_mutation(operation_id, locator, &snapshot, mutation, generated_text)
            .await?;
        if let Some(sink) = &progress {
            if let Err(error) = sink
                .emit(crate::seams::bridge_progress::BridgeProgressEvent::Done {
                    reply_text: outcome.reply_text.clone().unwrap_or_default(),
                    commit: crate::seams::bridge_progress::ConfirmedCommit::Applied,
                })
                .await
            {
                tracing::warn!(error = %error, "bridge terminal progress sink failed");
            }
        }
        Ok(outcome)
    }

    async fn undo_last_turn(
        &self,
        actor: &Actor,
        locator: StChatLocator,
        client_operation_id: String,
        operation_kind: &str,
        origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome> {
        let _locator_guard = self.lock_locator(&locator).await?;
        self.require_write_for_locator(&locator).await?;
        let operation_id = if client_operation_id.trim().is_empty() {
            crate::ids::new_id()
        } else {
            client_operation_id
        };
        let claimed = self
            .claim_for_origin(
                actor,
                &operation_id,
                operation_kind,
                &locator,
                origin.clone(),
            )
            .await?;
        if self.resume_or_replay(&operation_id, operation_kind, &claimed)? {
            return Ok(committed_outcome(
                operation_id,
                "该撤回操作已完成，未再次回退 SillyTavern。",
            ));
        }
        let snapshot = self.backend.snapshot(&locator).await?;
        let Some((user_idx, assistant_idx)) = st_ops::last_dialogue_indexes(&snapshot.parsed_chat)
        else {
            return Err(
                self.control_error(StErrorCode::StCommitValidationFailed, Some(operation_id))
            );
        };
        let user_msg = &snapshot.parsed_chat[user_idx];
        let assistant_msg = &snapshot.parsed_chat[assistant_idx];
        let user_sha = st_ops::message_sha256(user_msg);
        let assistant_sha = st_ops::message_sha256(assistant_msg);
        let tail_fingerprint = format!("{user_sha}:{assistant_sha}");

        let user_text = st_ops::visible_text(user_msg);
        let assistant_text = st_ops::visible_text(assistant_msg);
        let truncate = |s: &str, max: usize| -> String {
            if s.chars().count() <= max {
                s.to_string()
            } else {
                let prefix: String = s.chars().take(max).collect();
                format!("{prefix}…")
            }
        };
        let removed_safe_content = format!(
            "用户: \"{}\" | 助手: \"{}\"",
            truncate(&user_text, 40),
            truncate(&assistant_text, 40)
        );

        let mutation = StMutation::UndoLastTurn {
            expected_user_sha256: user_sha,
            expected_assistant_sha256: assistant_sha,
        };

        let coordinator = self.operation_coordinator.as_ref().ok_or_else(|| {
            self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.clone()))
        })?;
        coordinator
            .mark_snapshot_ready(
                &operation_id,
                Some(&snapshot.source_sha256),
                Some(&snapshot.source_integrity),
                Some(snapshot.source_byte_length as i64),
                Some(snapshot.source_message_count as i64),
            )
            .await?;
        self.persist_generated_payload(
            &actor.account.id,
            &operation_id,
            operation_kind,
            &locator,
            &snapshot,
            &mutation,
            &origin,
        )
        .await?;
        let reply_text = if operation_kind == operations::KIND_REVOKE {
            format!("已撤回最后一轮（SillyTavern 已回退）：{removed_safe_content}")
        } else {
            format!(
                "已在 SillyTavern 撤回最后一轮（Telegram 消息保持保留）：{removed_safe_content}"
            )
        };
        self.commit_mutation(
            operation_id.clone(),
            locator,
            &snapshot,
            mutation,
            reply_text.clone(),
        )
        .await?;

        Ok(StBridgeOutcome {
            operation_id: Some(operation_id),
            reply_text: Some(reply_text),
            write_committed: true,
            confirmed_commit: true,
            removed_safe_content: Some(removed_safe_content),
            st_tail_fingerprint: Some(tail_fingerprint),
        })
    }

    // Reserved for the durable undo replay path; kept next to the operation recovery code.
    #[allow(dead_code)]
    async fn replay_committed_undo(
        &self,
        operation_id: &str,
        operation_kind: &str,
    ) -> Option<StBridgeOutcome> {
        let store = self.operation_store.as_ref()?;
        let record = match store.get_operation(operation_id).await {
            Ok(Some(record)) => record,
            Ok(None) => return None,
            Err(error) => {
                tracing::warn!(operation_id, code = %error.code, "operation replay lookup failed");
                return None;
            }
        };
        replay_outcome_from_record(operation_id, operation_kind, &record)
    }

    async fn regenerate_reply(
        &self,
        actor: &Actor,
        locator: StChatLocator,
        client_operation_id: String,
        model_override: Option<String>,
        origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome> {
        self.regenerate_reply_with_context(
            actor,
            locator,
            client_operation_id,
            model_override,
            None,
            origin,
        )
        .await
    }

    async fn regenerate_reply_with_context(
        &self,
        actor: &Actor,
        locator: StChatLocator,
        client_operation_id: String,
        model_override: Option<String>,
        context: Option<crate::seams::bridge_progress::BridgeExecutionContext>,
        origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome> {
        let _locator_guard = self.lock_locator(&locator).await?;
        self.require_write_for_locator(&locator).await?;
        let operation_id = if client_operation_id.trim().is_empty() {
            context
                .as_ref()
                .map(|c| c.operation_id.clone())
                .unwrap_or_else(crate::ids::new_id)
        } else {
            client_operation_id
        };
        if let Some(command_context) = context.as_ref() {
            if command_context.operation_id != operation_id {
                return Err(
                    self.control_error(StErrorCode::StOperationIdReused, Some(operation_id))
                );
            }
        }
        let claimed = self
            .claim_for_origin(
                actor,
                &operation_id,
                operations::KIND_REGENERATE,
                &locator,
                origin.clone(),
            )
            .await?;
        if self.resume_or_replay(&operation_id, operations::KIND_REGENERATE, &claimed)? {
            return Ok(committed_outcome(
                operation_id,
                "该重生成操作已完成，未再次写入 SillyTavern。",
            ));
        }
        let snapshot = self.backend.snapshot(&locator).await?;
        let coordinator = self.operation_coordinator.as_ref().ok_or_else(|| {
            self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.clone()))
        })?;
        coordinator
            .mark_snapshot_ready(
                &operation_id,
                Some(&snapshot.source_sha256),
                Some(&snapshot.source_integrity),
                Some(snapshot.source_byte_length as i64),
                Some(snapshot.source_message_count as i64),
            )
            .await?;
        coordinator.mark_generating(&operation_id).await?;
        let cancel = context
            .as_ref()
            .map(|item| item.cancel.clone())
            .unwrap_or_default();
        let progress = context.and_then(|c| c.progress);
        let generation = async {
            let character = self.load_character(&locator).await?;
            let settings = self.backend.generation_settings().await?;
            let channel_context = self
                .channels
                .load(&actor.account.id, &self.context_key)
                .await
                .map_err(|_| {
                    self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.clone()))
                })?;
            let model_id = model_override.or(channel_context.chat_model_id);
            self.require_generation_fence(&operation_id).await?;
            let _gen_permit = self.acquire_generation_permit().await?;
            let generated = self
                .backend
                .stream_generate(
                    StGenerationRequest {
                        model_id,
                        messages: st_ops::prompt_messages(
                            &character,
                            &settings.username,
                            &snapshot.parsed_chat,
                            None,
                            true,
                        ),
                        temperature: Some(settings.temperature),
                        top_p: Some(settings.top_p),
                        max_tokens: Some(settings.max_tokens),
                    },
                    progress.clone(),
                    cancel.clone(),
                )
                .await?;
            let Some(mutation) =
                st_ops::replace_assistant_mutation(&snapshot, &character.name, &generated.text)
            else {
                return Err(self.control_error(
                    StErrorCode::StCommitValidationFailed,
                    Some(operation_id.clone()),
                ));
            };
            Ok((mutation, generated.text))
        }
        .await;
        let (mutation, generated_text) = match generation {
            Ok(result) => result,
            Err(error) => {
                return Err(coordinator
                    .settle_generation_failure(&operation_id, error)
                    .await)
            }
        };
        self.persist_generated_payload(
            &actor.account.id,
            &operation_id,
            operations::KIND_REGENERATE,
            &locator,
            &snapshot,
            &mutation,
            &origin,
        )
        .await?;
        let outcome = self
            .commit_mutation(operation_id, locator, &snapshot, mutation, generated_text)
            .await?;
        if let Some(sink) = &progress {
            if let Err(error) = sink
                .emit(crate::seams::bridge_progress::BridgeProgressEvent::Done {
                    reply_text: outcome.reply_text.clone().unwrap_or_default(),
                    commit: crate::seams::bridge_progress::ConfirmedCommit::Applied,
                })
                .await
            {
                tracing::warn!(error = %error, "bridge terminal progress sink failed");
            }
        }
        Ok(outcome)
    }

    async fn compress_chat(
        &self,
        actor: &Actor,
        locator: StChatLocator,
        client_operation_id: String,
        origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome> {
        let _locator_guard = self.lock_locator(&locator).await?;
        self.require_write_for_locator(&locator).await?;
        let operation_id = if client_operation_id.trim().is_empty() {
            crate::ids::new_id()
        } else {
            client_operation_id
        };
        let claimed = self
            .claim_for_origin(
                actor,
                &operation_id,
                operations::KIND_COMPRESS,
                &locator,
                origin.clone(),
            )
            .await?;
        if self.resume_or_replay(&operation_id, operations::KIND_COMPRESS, &claimed)? {
            return Ok(committed_outcome(
                operation_id,
                "该压缩操作已完成，未再次写入 SillyTavern。",
            ));
        }
        let snapshot = self.backend.snapshot(&locator).await?;
        let targets = st_ops::compress_targets(&snapshot.parsed_chat);
        if targets.is_empty() {
            return Ok(StBridgeOutcome {
                operation_id: Some(operation_id),
                reply_text: Some("没有需要压缩的历史消息。".into()),
                write_committed: false,
                confirmed_commit: false,
                removed_safe_content: None,
                st_tail_fingerprint: None,
            });
        }
        let coordinator = self.operation_coordinator.as_ref().ok_or_else(|| {
            self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.clone()))
        })?;
        coordinator
            .mark_snapshot_ready(
                &operation_id,
                Some(&snapshot.source_sha256),
                Some(&snapshot.source_integrity),
                Some(snapshot.source_byte_length as i64),
                Some(snapshot.source_message_count as i64),
            )
            .await?;
        coordinator.mark_generating(&operation_id).await?;
        let generation = async {
            self.require_generation_fence(&operation_id).await?;
            let _gen_permit = self.acquire_generation_permit().await?;
            let context = self
                .channels
                .load(&actor.account.id, &self.context_key)
                .await
                .map_err(|_| {
                    self.control_error(StErrorCode::StWriteNotReady, Some(operation_id.clone()))
                })?;
            let settings = self.backend.generation_settings().await?;
            let mut patches = Vec::new();
            for (index, expected_sha, text) in targets {
                let generated = self
                    .backend
                    .stream_generate(
                        StGenerationRequest {
                            model_id: context.compression_model_id.clone(),
                            messages: st_ops::compression_messages(&text),
                            temperature: Some(settings.temperature),
                            top_p: Some(settings.top_p),
                            max_tokens: Some(settings.max_tokens),
                        },
                        None,
                        tokio_util::sync::CancellationToken::new(),
                    )
                    .await?;
                patches.push(st_ops::compress_patch(index, expected_sha, &generated.text));
            }
            Ok(StMutation::CompressMessages { patches })
        }
        .await;
        let mutation = match generation {
            Ok(mutation) => mutation,
            Err(error) => {
                return Err(coordinator
                    .settle_generation_failure(&operation_id, error)
                    .await)
            }
        };
        self.persist_generated_payload(
            &actor.account.id,
            &operation_id,
            operations::KIND_COMPRESS,
            &locator,
            &snapshot,
            &mutation,
            &origin,
        )
        .await?;
        self.commit_mutation(
            operation_id,
            locator,
            &snapshot,
            mutation,
            "已压缩较早的历史消息。".into(),
        )
        .await
    }

    fn write_frozen(&self) -> StResult<StBridgeOutcome> {
        Err(map_st_error(StErrorFacts {
            stage: StErrorStage::Control,
            endpoint_class: Some("bridge".into()),
            operation_id: None,
            commit_state: CommitState::NotStarted,
            attempt: 1,
            duration_ms: None,
            failure: StFailureFacts::Control {
                code: StErrorCode::StWriteNotReady,
            },
        }))
    }
}

#[async_trait]
impl StBridgeEngine for SidecarBridgeEngine {
    async fn execute(&self, actor: &Actor, command: StBridgeCommand) -> StResult<StBridgeOutcome> {
        match command {
            StBridgeCommand::SelectCharacter {
                avatar,
                character_name,
            } => {
                let workspace_id = actor.require_workspace().map_err(|_| {
                    map_st_error(StErrorFacts {
                        stage: StErrorStage::Control,
                        endpoint_class: Some("bridge".into()),
                        operation_id: None,
                        commit_state: CommitState::NotStarted,
                        attempt: 1,
                        duration_ms: None,
                        failure: StFailureFacts::Control {
                            code: StErrorCode::StChatLocatorRejected,
                        },
                    })
                })?;
                let handle = self.backend.probe().await?.handle;
                self.channels
                    .select_character(
                        &actor.account.id,
                        workspace_id,
                        &self.context_key,
                        &handle,
                        &avatar,
                        &character_name,
                    )
                    .await
                    .map_err(|_| {
                        map_st_error(StErrorFacts {
                            stage: StErrorStage::Control,
                            endpoint_class: Some("bridge".into()),
                            operation_id: None,
                            commit_state: CommitState::NotStarted,
                            attempt: 1,
                            duration_ms: None,
                            failure: StFailureFacts::Control {
                                code: StErrorCode::StChatLocatorRejected,
                            },
                        })
                    })?;
                Ok(StBridgeOutcome {
                    operation_id: None,
                    reply_text: Some(format!("已选择角色：{character_name}")),
                    write_committed: false,
                    confirmed_commit: false,
                    removed_safe_content: None,
                    st_tail_fingerprint: None,
                })
            }
            StBridgeCommand::SelectChat { locator } => {
                let workspace_id = actor.require_workspace().map_err(|_| {
                    map_st_error(StErrorFacts {
                        stage: StErrorStage::Control,
                        endpoint_class: Some("bridge".into()),
                        operation_id: None,
                        commit_state: CommitState::NotStarted,
                        attempt: 1,
                        duration_ms: None,
                        failure: StFailureFacts::Control {
                            code: StErrorCode::StChatLocatorRejected,
                        },
                    })
                })?;
                self.channels
                    .select_chat(&actor.account.id, workspace_id, &self.context_key, &locator)
                    .await
                    .map_err(|_| {
                        map_st_error(StErrorFacts {
                            stage: StErrorStage::Control,
                            endpoint_class: Some("bridge".into()),
                            operation_id: None,
                            commit_state: CommitState::NotStarted,
                            attempt: 1,
                            duration_ms: None,
                            failure: StFailureFacts::Control {
                                code: StErrorCode::StChatLocatorRejected,
                            },
                        })
                    })?;
                Ok(StBridgeOutcome {
                    operation_id: None,
                    reply_text: Some(format!("已选择会话：{}", locator.chat_file)),
                    write_committed: false,
                    confirmed_commit: false,
                    removed_safe_content: None,
                    st_tail_fingerprint: None,
                })
            }
            StBridgeCommand::SetModelOverride { kind, model_id } => {
                let workspace_id = actor.require_workspace().map_err(|_| {
                    map_st_error(StErrorFacts {
                        stage: StErrorStage::Control,
                        endpoint_class: Some("bridge".into()),
                        operation_id: None,
                        commit_state: CommitState::NotStarted,
                        attempt: 1,
                        duration_ms: None,
                        failure: StFailureFacts::Control {
                            code: StErrorCode::StChatLocatorRejected,
                        },
                    })
                })?;
                let purpose = match kind {
                    crate::seams::st_bridge_engine::ModelOverrideKind::Chat => "chat",
                    crate::seams::st_bridge_engine::ModelOverrideKind::Compression => "compression",
                };
                self.channels
                    .set_model_override(
                        &actor.account.id,
                        workspace_id,
                        &self.context_key,
                        purpose,
                        model_id.as_deref(),
                    )
                    .await
                    .map_err(|_| {
                        map_st_error(StErrorFacts {
                            stage: StErrorStage::Control,
                            endpoint_class: Some("bridge".into()),
                            operation_id: None,
                            commit_state: CommitState::NotStarted,
                            attempt: 1,
                            duration_ms: None,
                            failure: StFailureFacts::Control {
                                code: StErrorCode::StChatLocatorRejected,
                            },
                        })
                    })?;
                Ok(StBridgeOutcome {
                    operation_id: None,
                    reply_text: Some("已更新模型覆盖".into()),
                    write_committed: false,
                    confirmed_commit: false,
                    removed_safe_content: None,
                    st_tail_fingerprint: None,
                })
            }
            StBridgeCommand::StartChat {
                client_operation_id,
                ..
            }
            | StBridgeCommand::SendMessage {
                client_operation_id,
                ..
            }
            | StBridgeCommand::UndoLastTurn {
                client_operation_id,
                ..
            }
            | StBridgeCommand::RevokeLastTurn {
                client_operation_id,
                ..
            }
            | StBridgeCommand::RegenerateReply {
                client_operation_id,
                ..
            }
            | StBridgeCommand::CompressChat {
                client_operation_id,
                ..
            } => Err(self.control_error(StErrorCode::StWriteNotReady, Some(client_operation_id))),
        }
    }

    async fn execute_with_origin(
        &self,
        actor: &Actor,
        command: StBridgeCommand,
        origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome> {
        match command {
            StBridgeCommand::StartChat {
                locator,
                client_operation_id,
            } => {
                self.start_chat(actor, locator, client_operation_id, origin)
                    .await
            }
            StBridgeCommand::SendMessage {
                locator,
                text,
                client_operation_id,
                model_override,
            } => {
                self.send_message(
                    actor,
                    locator,
                    text,
                    client_operation_id,
                    model_override,
                    origin,
                )
                .await
            }
            StBridgeCommand::UndoLastTurn {
                locator,
                client_operation_id,
            } => {
                self.undo_last_turn(
                    actor,
                    locator,
                    client_operation_id,
                    operations::KIND_UNDO,
                    origin,
                )
                .await
            }
            StBridgeCommand::RevokeLastTurn {
                locator,
                client_operation_id,
            } => {
                self.undo_last_turn(
                    actor,
                    locator,
                    client_operation_id,
                    operations::KIND_REVOKE,
                    origin,
                )
                .await
            }
            StBridgeCommand::RegenerateReply {
                locator,
                client_operation_id,
                model_override,
            } => {
                self.regenerate_reply(actor, locator, client_operation_id, model_override, origin)
                    .await
            }
            StBridgeCommand::CompressChat {
                locator,
                client_operation_id,
            } => {
                self.compress_chat(actor, locator, client_operation_id, origin)
                    .await
            }
            other => self.execute(actor, other).await,
        }
    }

    async fn execute_with_context_and_origin(
        &self,
        actor: &Actor,
        command: StBridgeCommand,
        context: Option<crate::seams::bridge_progress::BridgeExecutionContext>,
        origin: BridgeOperationOrigin,
    ) -> StResult<StBridgeOutcome> {
        match command {
            StBridgeCommand::SendMessage {
                locator,
                text,
                client_operation_id,
                model_override,
            } => {
                self.send_message_with_context(
                    actor,
                    locator,
                    text,
                    client_operation_id,
                    model_override,
                    context,
                    origin,
                )
                .await
            }
            StBridgeCommand::RegenerateReply {
                locator,
                client_operation_id,
                model_override,
            } => {
                self.regenerate_reply_with_context(
                    actor,
                    locator,
                    client_operation_id,
                    model_override,
                    context,
                    origin,
                )
                .await
            }
            other => self.execute_with_origin(actor, other, origin).await,
        }
    }

    async fn execute_with_context(
        &self,
        actor: &Actor,
        command: StBridgeCommand,
        _context: Option<crate::seams::bridge_progress::BridgeExecutionContext>,
    ) -> StResult<StBridgeOutcome> {
        match command {
            StBridgeCommand::SendMessage {
                client_operation_id,
                ..
            }
            | StBridgeCommand::RegenerateReply {
                client_operation_id,
                ..
            } => Err(self.control_error(StErrorCode::StWriteNotReady, Some(client_operation_id))),
            other => self.execute(actor, other).await,
        }
    }

    async fn query(&self, actor: &Actor, query: StBridgeQuery) -> StResult<StBridgeView> {
        match query {
            StBridgeQuery::ListCharacters => Ok(StBridgeView::Characters(
                self.backend.list_characters().await?,
            )),
            StBridgeQuery::ListCharacterChats { avatar } => {
                Ok(StBridgeView::Chats(self.backend.list_chats(&avatar).await?))
            }
            StBridgeQuery::GetModels => {
                let catalog = self.backend.list_models().await?;
                let context = self
                    .channels
                    .load(&actor.account.id, &self.context_key)
                    .await
                    .map_err(|_| self.control_error(StErrorCode::StChatPayloadInvalid, None))?;
                Ok(StBridgeView::Models {
                    catalog,
                    override_chat: context.chat_model_id,
                    override_compression: context.compression_model_id,
                })
            }
            StBridgeQuery::GetChannelContext => {
                let context = self
                    .channels
                    .load(&actor.account.id, &self.context_key)
                    .await
                    .unwrap_or_default();
                Ok(StBridgeView::Context {
                    locator: match (
                        context.handle.clone(),
                        context.avatar.clone(),
                        context.chat_file.clone(),
                    ) {
                        (Some(handle), Some(avatar), Some(chat_file)) => Some(StChatLocator {
                            handle,
                            avatar,
                            character_name: context.character_name.unwrap_or_default(),
                            chat_file,
                        }),
                        _ => None,
                    },
                    chat_model_id: context.chat_model_id,
                    compression_model_id: context.compression_model_id,
                })
            }
            StBridgeQuery::GetHistory { locator } => {
                let snapshot = self.backend.snapshot(&locator).await?;
                let preview = last_visible_message(&snapshot.parsed_chat)
                    .map(|(_, speaker, text)| format!("{speaker}：\n{text}"))
                    .unwrap_or_default();
                Ok(StBridgeView::History { preview })
            }
            StBridgeQuery::GetLastTurn { locator } => {
                let snapshot = self.backend.snapshot(&locator).await?;
                let mut last_user = None;
                let mut last_assistant = None;
                for item in snapshot.parsed_chat.iter().rev() {
                    if item.get("is_system").and_then(serde_json::Value::as_bool) == Some(true) {
                        continue;
                    }
                    let text = item
                        .get("extra")
                        .and_then(|extra| extra.get("display_text"))
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| item.get("mes").and_then(serde_json::Value::as_str))
                        .unwrap_or("")
                        .trim();
                    if text.is_empty() {
                        continue;
                    }
                    let speaker = item
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    if item.get("is_user").and_then(serde_json::Value::as_bool) == Some(true) {
                        if last_user.is_none() {
                            last_user = Some(format!("{speaker}\n{text}"));
                        }
                    } else if last_assistant.is_none() {
                        last_assistant = Some(format!("{speaker}\n{text}"));
                    }
                    if last_user.is_some() && last_assistant.is_some() {
                        break;
                    }
                }
                Ok(StBridgeView::LastTurn {
                    user: last_user,
                    assistant: last_assistant,
                    tail_fingerprint: Some(snapshot.source_sha256),
                })
            }
            StBridgeQuery::ListRecentChats => {
                let recent = self
                    .channels
                    .list_recent(&actor.account.id, 8)
                    .await
                    .unwrap_or_default();
                Ok(StBridgeView::Chats(
                    recent
                        .into_iter()
                        .filter_map(|item| {
                            Some(crate::domain::st::StChatSummary {
                                chat_file: item.chat_file?,
                                title: item.character_name,
                                updated_at: None,
                                message_count: None,
                            })
                        })
                        .collect(),
                ))
            }
            StBridgeQuery::GetOperation { operation_id } => Ok(StBridgeView::Operation {
                id: operation_id,
                status: "unknown".into(),
            }),
        }
    }
}

fn committed_outcome(operation_id: String, reply_text: impl Into<String>) -> StBridgeOutcome {
    StBridgeOutcome {
        operation_id: Some(operation_id),
        reply_text: Some(reply_text.into()),
        write_committed: true,
        confirmed_commit: true,
        removed_safe_content: None,
        st_tail_fingerprint: None,
    }
}

// Reserved for the durable undo replay path; see `replay_committed_undo`.
#[allow(dead_code)]
fn replay_outcome_from_record(
    operation_id: &str,
    operation_kind: &str,
    record: &crate::modules::bridge::operations::BridgeOperationRecord,
) -> Option<StBridgeOutcome> {
    if record.operation_kind != operation_kind {
        return None;
    }
    if !matches!(
        record.status,
        crate::modules::bridge::operations::BridgeOperationStatus::Committed
            | crate::modules::bridge::operations::BridgeOperationStatus::Delivered
    ) || record.commit_state != crate::modules::bridge::operations::OperationCommitState::Applied
    {
        return None;
    }
    Some(StBridgeOutcome {
        operation_id: Some(operation_id.to_string()),
        reply_text: Some("该撤回已完成，未再次回退 SillyTavern。".into()),
        write_committed: true,
        confirmed_commit: true,
        removed_safe_content: Some("最后一轮".into()),
        st_tail_fingerprint: None,
    })
}

fn last_visible_message(parsed_chat: &[serde_json::Value]) -> Option<(bool, String, String)> {
    parsed_chat.iter().rev().find_map(|item| {
        if item.get("is_system").and_then(serde_json::Value::as_bool) == Some(true) {
            return None;
        }
        let text = item
            .get("extra")
            .and_then(|extra| extra.get("display_text"))
            .and_then(serde_json::Value::as_str)
            .or_else(|| item.get("mes").and_then(serde_json::Value::as_str))
            .unwrap_or("")
            .trim();
        if text.is_empty() {
            return None;
        }
        let is_user = item
            .get("is_user")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let speaker = item
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(if is_user { "用户" } else { "角色" })
            .to_string();
        Some((is_user, speaker, text.to_string()))
    })
}

fn test_chat_file(character_name: &str) -> String {
    let safe_name: String = character_name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect();
    let safe_name = safe_name.trim_matches('-');
    let safe_name = if safe_name.is_empty() {
        "Chat"
    } else {
        safe_name
    };
    format!("IMBridge-Test-{safe_name}-{}.jsonl", now_unix_ms())
}

fn opening_message(character_name: &str) -> serde_json::Value {
    serde_json::json!({
        "name": character_name,
        "is_user": false,
        "is_system": false,
        "send_date": now_rfc3339(),
        "mes": format!("你好，我是{character_name}。我们开始新的故事吧。"),
        "extra": {}
    })
}
