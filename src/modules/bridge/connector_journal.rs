use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorJournalStatus {
    Prepared,
    Applied,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorJournalRecord {
    pub operation_id: String,
    pub locator_hash: String,
    pub mutation_digest: String,
    pub status: ConnectorJournalStatus,
    pub before_sha256: Option<String>,
    pub after_sha256: Option<String>,
    pub before_integrity: Option<String>,
    pub after_integrity: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalError {
    DuplicatePrepare,
    InvalidTransition,
    OperationIdReused,
    Missing,
    Conflict,
}

impl JournalError {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DuplicatePrepare => "duplicate prepare",
            Self::InvalidTransition => "invalid journal transition",
            Self::OperationIdReused => "operation id reused",
            Self::Missing => "journal record missing",
            Self::Conflict => "journal conflict",
        }
    }
}

impl fmt::Display for JournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for JournalError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayOutcome {
    AlreadyApplied,
    Continue { status: ConnectorJournalStatus },
    FailedTerminal,
    NotFound,
}

#[derive(Debug, Default)]
pub struct MemoryConnectorJournal {
    records: HashMap<String, ConnectorJournalRecord>,
}

impl MemoryConnectorJournal {
    pub fn prepare(&mut self, record: ConnectorJournalRecord) -> Result<(), JournalError> {
        if self.records.contains_key(&record.operation_id) {
            return Err(JournalError::DuplicatePrepare);
        }
        if record.status != ConnectorJournalStatus::Prepared
            || record.locator_hash.is_empty()
            || record.mutation_digest.is_empty()
        {
            return Err(JournalError::InvalidTransition);
        }
        self.records.insert(record.operation_id.clone(), record);
        Ok(())
    }

    pub fn mark_applied(
        &mut self,
        operation_id: &str,
        after_sha256: String,
        after_integrity: String,
    ) -> Result<ConnectorJournalStatus, JournalError> {
        let record = self
            .records
            .get_mut(operation_id)
            .ok_or(JournalError::Missing)?;
        match record.status {
            ConnectorJournalStatus::Applied => {
                if record.after_sha256.as_deref() == Some(after_sha256.as_str())
                    && record.after_integrity.as_deref() == Some(after_integrity.as_str())
                {
                    Ok(ConnectorJournalStatus::Applied)
                } else {
                    Err(JournalError::Conflict)
                }
            }
            ConnectorJournalStatus::Prepared | ConnectorJournalStatus::Unknown => {
                record.after_sha256 = Some(after_sha256);
                record.after_integrity = Some(after_integrity);
                record.status = ConnectorJournalStatus::Applied;
                Ok(ConnectorJournalStatus::Applied)
            }
            ConnectorJournalStatus::Failed => Err(JournalError::InvalidTransition),
        }
    }

    pub fn mark_failed(&mut self, operation_id: &str) -> Result<(), JournalError> {
        let record = self
            .records
            .get_mut(operation_id)
            .ok_or(JournalError::Missing)?;
        match record.status {
            ConnectorJournalStatus::Prepared | ConnectorJournalStatus::Unknown => {
                record.status = ConnectorJournalStatus::Failed;
                Ok(())
            }
            ConnectorJournalStatus::Applied | ConnectorJournalStatus::Failed => {
                Err(JournalError::InvalidTransition)
            }
        }
    }

    pub fn mark_unknown(&mut self, operation_id: &str) -> Result<(), JournalError> {
        let record = self
            .records
            .get_mut(operation_id)
            .ok_or(JournalError::Missing)?;
        match record.status {
            ConnectorJournalStatus::Prepared => {
                record.status = ConnectorJournalStatus::Unknown;
                Ok(())
            }
            ConnectorJournalStatus::Unknown
            | ConnectorJournalStatus::Applied
            | ConnectorJournalStatus::Failed => Err(JournalError::InvalidTransition),
        }
    }

    pub fn replay(
        &mut self,
        operation_id: &str,
        locator_hash: &str,
        mutation_digest: &str,
    ) -> Result<ReplayOutcome, JournalError> {
        let Some(record) = self.records.get(operation_id) else {
            return Ok(ReplayOutcome::NotFound);
        };
        if record.locator_hash != locator_hash || record.mutation_digest != mutation_digest {
            return Err(JournalError::OperationIdReused);
        }
        match record.status {
            ConnectorJournalStatus::Applied => Ok(ReplayOutcome::AlreadyApplied),
            ConnectorJournalStatus::Prepared | ConnectorJournalStatus::Unknown => {
                Ok(ReplayOutcome::Continue {
                    status: record.status,
                })
            }
            ConnectorJournalStatus::Failed => Ok(ReplayOutcome::FailedTerminal),
        }
    }

    pub fn get(&self, operation_id: &str) -> Option<&ConnectorJournalRecord> {
        self.records.get(operation_id)
    }
}
