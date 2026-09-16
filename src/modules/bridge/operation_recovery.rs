use std::sync::Arc;

use crate::error::AppResult;
use crate::modules::bridge::operation_coordinator::OperationCoordinator;
use crate::modules::bridge::operation_payload::locator_hash;
use crate::modules::bridge::operation_store::{OperationExecutionLease, OperationStore};
use crate::modules::bridge::operations::{BridgeOperationRecord, BridgeOperationStatus};
use crate::modules::bridge::poller_ownership::PollerOwnershipGuard;

pub struct OperationRecoveryCoordinator {
    store: Arc<OperationStore>,
}

impl OperationRecoveryCoordinator {
    pub fn new(store: Arc<OperationStore>) -> Self {
        Self { store }
    }

    pub async fn recover_for_bot(
        &self,
        coordinator: &OperationCoordinator,
        guard: &PollerOwnershipGuard,
        internal_bot_id: &str,
    ) -> AppResult<Vec<String>> {
        if internal_bot_id.trim().is_empty() || guard.internal_bot_id != internal_bot_id {
            return Err(crate::error::AppError::bad_request(
                "OPERATION_RECOVERY_SCOPE_INVALID",
                "operation recovery bot scope is invalid",
            ));
        }
        let fence = guard.runtime_fence().ok_or_else(|| {
            crate::error::AppError::service_unavailable(
                "ST_POLLER_EPOCH_MISMATCH",
                "operation recovery requires a running runtime fence",
            )
        })?;
        let lease_until = (time::OffsetDateTime::now_utc() + time::Duration::seconds(60))
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|_| {
                crate::error::AppError::internal("operation lease deadline format failed")
            })?;
        let records = self
            .store
            .list_recovery_candidates_for_bot(internal_bot_id, 128)
            .await?;
        let mut recovered = Vec::new();
        for record in records {
            let Some(lease) = self
                .store
                .try_acquire_execution_lease(
                    &record.id,
                    internal_bot_id,
                    &guard.runtime_instance_id,
                    &lease_until,
                )
                .await?
            else {
                continue;
            };
            let result = async {
                guard.assert_commit_fence().await.map_err(|error| {
                    crate::error::AppError::conflict(
                        "ST_POLLER_EPOCH_MISMATCH",
                        format!("operation recovery fence rejected: {error}"),
                    )
                })?;
                match record.status {
                    BridgeOperationStatus::Generated => {
                        coordinator
                            .replay_generated(&record, internal_bot_id, &fence)
                            .await
                            .map_err(|error| {
                                crate::error::AppError::bad_gateway(
                                    "OPERATION_RECOVERY_REPLAY_FAILED",
                                    format!(
                                        "operation {} replay failed: {}",
                                        record.id, error.code
                                    ),
                                )
                            })?;
                        Ok(true)
                    }
                    BridgeOperationStatus::Committing => {
                        let locator = locator_hash(
                            &record.locator.handle,
                            &record.locator.avatar,
                            &record.locator.chat_file,
                        );
                        coordinator
                            .reconcile_commit(&record, &locator)
                            .await
                            .map(|outcome| outcome.is_some())
                            .map_err(|error| {
                                crate::error::AppError::bad_gateway(
                                    "OPERATION_RECOVERY_RECONCILE_FAILED",
                                    format!(
                                        "operation {} reconciliation failed: {}",
                                        record.id, error.code
                                    ),
                                )
                            })
                    }
                    BridgeOperationStatus::Committed => Ok(false),
                    _ => Ok(false),
                }
            }
            .await;
            if matches!(result, Ok(true)) {
                recovered.push(record.id.clone());
            } else if let Err(error) = &result {
                tracing::warn!(
                    operation_id = %record.id,
                    code = %error.code,
                    "operation recovery attempt failed"
                );
            }
            self.release_lease(&lease).await?;
        }
        Ok(recovered)
    }

    async fn release_lease(&self, lease: &OperationExecutionLease) -> AppResult<()> {
        if !self.store.release_execution_lease(lease).await? {
            return Err(crate::error::AppError::conflict(
                "OPERATION_LEASE_LOST",
                "operation execution lease changed before release",
            ));
        }
        Ok(())
    }

    pub async fn find_dangling_committing_operations(
        &self,
    ) -> AppResult<Vec<BridgeOperationRecord>> {
        self.store
            .list_by_status(BridgeOperationStatus::Committing)
            .await
    }

    pub async fn find_generated_operations(&self) -> AppResult<Vec<BridgeOperationRecord>> {
        self.store
            .list_by_status(BridgeOperationStatus::Generated)
            .await
    }

    pub async fn recover_generated_operations(
        &self,
        coordinator: &OperationCoordinator,
        guard: &PollerOwnershipGuard,
    ) -> AppResult<Vec<String>> {
        let fence = guard.runtime_fence().ok_or_else(|| {
            crate::error::AppError::service_unavailable(
                "ST_POLLER_EPOCH_MISMATCH",
                "generated recovery requires a running runtime fence",
            )
        })?;
        let records = self.find_generated_operations().await?;
        let mut recovered = Vec::new();
        for record in records {
            coordinator
                .replay_generated(&record, &record.bot_id, &fence)
                .await
                .map_err(|error| {
                    crate::error::AppError::internal(format!(
                        "generated operation {} recovery failed: {error}",
                        record.id
                    ))
                })?;
            recovered.push(record.id);
        }
        Ok(recovered)
    }

    pub async fn reconcile_committing_operations(
        &self,
        coordinator: &OperationCoordinator,
    ) -> AppResult<Vec<String>> {
        let records = self.find_dangling_committing_operations().await?;
        let mut reconciled = Vec::new();
        for record in records {
            let locator = locator_hash(
                &record.locator.handle,
                &record.locator.avatar,
                &record.locator.chat_file,
            );
            if coordinator
                .reconcile_commit(&record, &locator)
                .await?
                .is_some()
            {
                reconciled.push(record.id);
            }
        }
        Ok(reconciled)
    }

    pub async fn find_undelivered_committed_operations(
        &self,
    ) -> AppResult<Vec<BridgeOperationRecord>> {
        self.store
            .list_by_status(BridgeOperationStatus::Committed)
            .await
    }

    pub async fn find_unknown_commit_operations(&self) -> AppResult<Vec<BridgeOperationRecord>> {
        self.store.list_unknown_commits().await
    }

    pub async fn mark_stale_in_flight_as_interrupted(
        &self,
        cutoff_rfc3339: &str,
    ) -> AppResult<Vec<String>> {
        self.store.interrupt_stale_generating(cutoff_rfc3339).await
    }
}
