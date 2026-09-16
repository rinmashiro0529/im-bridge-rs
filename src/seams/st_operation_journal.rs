use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{AppError, AppResult};

/// Durable connector knowledge for one bridge operation.
///
/// The journal deliberately contains identity and revision metadata only.  It
/// must never be used as a place to persist chat text, prompts, or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StOperationJournalStatus {
    Prepared,
    Applied,
    NotApplied,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StOperationJournalRecord {
    pub operation_id: String,
    pub locator_hash: String,
    pub mutation_digest: String,
    pub status: StOperationJournalStatus,
    pub before_sha256: Option<String>,
    pub before_integrity: Option<String>,
    pub after_sha256: Option<String>,
    pub after_integrity: Option<String>,
    pub after_byte_length: Option<u64>,
    pub after_message_count: Option<u64>,
}

impl StOperationJournalRecord {
    pub fn identity_matches(
        &self,
        operation_id: &str,
        locator_hash: &str,
        mutation_digest: &str,
    ) -> bool {
        self.operation_id == operation_id
            && self.locator_hash == locator_hash
            && self.mutation_digest == mutation_digest
    }
}

/// Read-only seam used by recovery and the operation coordinator.
///
/// Implementations are expected to use the same authenticated ConnectorClient
/// as the write backend.  In particular, a missing journal entry is not proof
/// that a mutation was not applied.
#[async_trait]
pub trait StOperationJournal: Send + Sync {
    async fn lookup(&self, operation_id: &str) -> AppResult<Option<StOperationJournalRecord>>;
}

/// Fail-closed journal used when Connector journal wiring is unavailable.
/// This is preferable to treating an empty in-memory journal as durable
/// evidence in a production bootstrap.
pub struct UnavailableStOperationJournal;

#[async_trait]
impl StOperationJournal for UnavailableStOperationJournal {
    async fn lookup(&self, _operation_id: &str) -> AppResult<Option<StOperationJournalRecord>> {
        Err(AppError::service_unavailable(
            "ST_OPERATION_JOURNAL_UNAVAILABLE",
            "connector operation journal is unavailable",
        ))
    }
}

/// Small deterministic journal implementation for component tests and local
/// composition.  Production wiring should provide the ConnectorClient-backed
/// implementation instead.
#[derive(Clone, Default)]
pub struct MemoryStOperationJournal {
    records: Arc<RwLock<HashMap<String, StOperationJournalRecord>>>,
}

impl MemoryStOperationJournal {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(&self, record: StOperationJournalRecord) -> AppResult<()> {
        if record.operation_id.trim().is_empty()
            || record.locator_hash.trim().is_empty()
            || record.mutation_digest.trim().is_empty()
        {
            return Err(AppError::bad_request(
                "ST_OPERATION_JOURNAL_INVALID",
                "operation journal identity is incomplete",
            ));
        }
        self.records
            .write()
            .await
            .insert(record.operation_id.clone(), record);
        Ok(())
    }

    pub async fn mark_applied(
        &self,
        operation_id: &str,
        after_sha256: impl Into<String>,
        after_integrity: impl Into<String>,
    ) -> AppResult<()> {
        let after_sha256 = after_sha256.into();
        let after_integrity = after_integrity.into();
        let mut records = self.records.write().await;
        let record = records.get_mut(operation_id).ok_or_else(|| {
            AppError::not_found(
                "ST_OPERATION_JOURNAL_MISSING",
                "operation journal record missing",
            )
        })?;
        if matches!(record.status, StOperationJournalStatus::Applied)
            && (record.after_sha256.as_deref() != Some(after_sha256.as_str())
                || record.after_integrity.as_deref() != Some(after_integrity.as_str()))
        {
            return Err(AppError::conflict(
                "ST_OPERATION_JOURNAL_CONFLICT",
                "operation journal applied result conflicts",
            ));
        }
        // Keep this transition conservative: an applied record may only be
        // enriched with the same result; all other states may converge once.
        record.after_sha256 = Some(after_sha256);
        record.after_integrity = Some(after_integrity);
        record.status = StOperationJournalStatus::Applied;
        Ok(())
    }
}

#[async_trait]
impl StOperationJournal for MemoryStOperationJournal {
    async fn lookup(&self, operation_id: &str) -> AppResult<Option<StOperationJournalRecord>> {
        Ok(self.records.read().await.get(operation_id).cloned())
    }
}
