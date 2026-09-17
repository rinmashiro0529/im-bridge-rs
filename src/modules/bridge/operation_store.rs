use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use sqlx::SqlitePool;

use crate::clock::now_rfc3339;
use crate::domain::st::StChatLocator;
use crate::error::{AppError, AppResult};
use crate::ids::new_id;
use crate::modules::bridge::operations::{
    BridgeOperationRecord, BridgeOperationStatus, EncryptedOperationPayload, OperationCommitState,
};

// Only literal clauses are accepted; values continue to use bind parameters.
macro_rules! select_operations {
    ($tail:literal) => {
        concat!(
            "SELECT id, actor_id, bot_id, telegram_update_id, channel_context_key, operation_kind,
        st_handle, st_character_avatar, st_chat_file, status,
        source_sha256, source_integrity, source_size, message_count,
        operation_payload_ciphertext, operation_payload_nonce, operation_payload_key_version,
        mutation_digest, connector_result_json,
        error_stage, error_code, error_summary, retryable, commit_state, attempt_count,
        request_id, trace_id, created_at, updated_at FROM bridge_operations",
            $tail
        )
    };
}

#[derive(Clone)]
pub struct OperationStore {
    pool: SqlitePool,
    in_process_claims: Arc<Mutex<HashSet<String>>>,
}

#[derive(Debug, Clone)]
pub struct ClaimOperationRequest {
    pub id: String,
    pub actor_id: String,
    pub bot_id: String,
    pub telegram_update_id: i64,
    pub channel_context_key: String,
    pub operation_kind: String,
    pub locator: StChatLocator,
    pub request_id: String,
    pub trace_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationKeyReference {
    pub wrapped_secret_id: String,
    pub key_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationExecutionLease {
    pub operation_id: String,
    pub instance_id: String,
    pub lease_generation: u64,
    pub lease_until: String,
}

#[derive(Debug)]
pub enum OperationClaimError {
    DuplicateClaim(Box<BridgeOperationRecord>),
    Failed(AppError),
}

impl OperationClaimError {
    pub fn confirmed_commit(&self) -> bool {
        matches!(
            self,
            Self::DuplicateClaim(record)
                if matches!(
                    (record.status, record.commit_state),
                    (
                        BridgeOperationStatus::Committed | BridgeOperationStatus::Delivered,
                        OperationCommitState::Applied
                    )
                )
        )
    }
}

impl From<AppError> for OperationClaimError {
    fn from(value: AppError) -> Self {
        Self::Failed(value)
    }
}

impl From<sqlx::Error> for OperationClaimError {
    fn from(value: sqlx::Error) -> Self {
        Self::Failed(AppError::from(value))
    }
}

struct InProcessClaimGuard {
    claims: Arc<Mutex<HashSet<String>>>,
    key: String,
}

impl Drop for InProcessClaimGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.claims.lock() {
            set.remove(&self.key);
        }
    }
}

impl OperationStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            in_process_claims: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    fn claim_key(bot_id: &str, update_id: i64, kind: &str) -> String {
        format!("{bot_id}:{update_id}:{kind}")
    }

    async fn acquire_in_process(&self, key: String) -> InProcessClaimGuard {
        loop {
            {
                let mut set = self
                    .in_process_claims
                    .lock()
                    .unwrap_or_else(|err| err.into_inner());
                if set.insert(key.clone()) {
                    return InProcessClaimGuard {
                        claims: self.in_process_claims.clone(),
                        key,
                    };
                }
            }
            tokio::task::yield_now().await;
        }
    }

    pub async fn claim_operation(
        &self,
        request: ClaimOperationRequest,
    ) -> Result<BridgeOperationRecord, OperationClaimError> {
        let id = if request.id.trim().is_empty() {
            new_id()
        } else {
            request.id.clone()
        };
        let key = Self::claim_key(
            &request.bot_id,
            request.telegram_update_id,
            &request.operation_kind,
        );
        let _guard = self.acquire_in_process(key).await;
        let now = now_rfc3339();
        let result = sqlx::query(
            "INSERT INTO bridge_operations (
                id, actor_id, bot_id, telegram_update_id, channel_context_key, operation_kind,
                st_handle, st_character_avatar, st_chat_file,
                status, commit_state, attempt_count, retryable,
                request_id, trace_id, request_meta_json, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'received', 'not_started', 1, 0, ?, ?, '{}', ?, ?)",
        )
        .bind(&id)
        .bind(&request.actor_id)
        .bind(&request.bot_id)
        .bind(request.telegram_update_id)
        .bind(&request.channel_context_key)
        .bind(&request.operation_kind)
        .bind(&request.locator.handle)
        .bind(&request.locator.avatar)
        .bind(&request.locator.chat_file)
        .bind(&request.request_id)
        .bind(&request.trace_id)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => self
                .get_operation(&id)
                .await?
                .ok_or_else(|| AppError::internal("claimed operation missing after insert").into()),
            Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
                let existing = self
                    .get_by_claim(
                        &request.bot_id,
                        request.telegram_update_id,
                        &request.operation_kind,
                    )
                    .await?
                    .ok_or_else(|| {
                        AppError::conflict(
                            "BRIDGE_OPERATION_DUPLICATE_CLAIM",
                            "duplicate claim unique constraint fired but existing row was not found",
                        )
                    })?;
                if !existing.matches_claim(&request) {
                    return Err(OperationClaimError::Failed(AppError::conflict(
                        "ST_OPERATION_ID_REUSED",
                        "operation identity does not match the existing claim",
                    )));
                }
                Err(OperationClaimError::DuplicateClaim(Box::new(existing)))
            }
            Err(err) => Err(OperationClaimError::from(err)),
        }
    }

    pub async fn update_snapshot_ready(
        &self,
        id: &str,
        source_sha256: Option<&str>,
        source_integrity: Option<&str>,
        source_size: Option<i64>,
        message_count: Option<i64>,
    ) -> AppResult<BridgeOperationRecord> {
        let now = now_rfc3339();
        let affected = sqlx::query(
            "UPDATE bridge_operations
             SET status = 'snapshot_ready',
                 source_sha256 = ?,
                 source_integrity = ?,
                 source_size = ?,
                 message_count = ?,
                 updated_at = ?
             WHERE id = ? AND status = 'received' AND commit_state = 'not_started'",
        )
        .bind(source_sha256)
        .bind(source_integrity)
        .bind(source_size)
        .bind(message_count)
        .bind(&now)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        self.assert_cas(affected, id, "received", "snapshot_ready")
            .await
    }

    pub async fn update_generating(&self, id: &str) -> AppResult<BridgeOperationRecord> {
        let now = now_rfc3339();
        let affected = sqlx::query(
            "UPDATE bridge_operations
             SET status = 'generating', updated_at = ?
             WHERE id = ? AND status = 'snapshot_ready' AND commit_state = 'not_started'",
        )
        .bind(&now)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        self.assert_cas(affected, id, "snapshot_ready", "generating")
            .await
    }

    pub async fn update_generated(
        &self,
        id: &str,
        payload: &EncryptedOperationPayload,
        mutation_digest: &str,
    ) -> AppResult<BridgeOperationRecord> {
        let now = now_rfc3339();
        let affected = sqlx::query(
            "UPDATE bridge_operations
             SET status = 'generated',
                 operation_payload_ciphertext = ?,
                 operation_payload_nonce = ?,
                 operation_payload_key_version = ?,
                 mutation_digest = ?,
                 updated_at = ?
             WHERE id = ?
               AND status IN ('snapshot_ready', 'generating')
               AND commit_state = 'not_started'
               AND EXISTS (
                   SELECT 1 FROM operation_key_refs
                   WHERE operation_id = bridge_operations.id AND key_version = ?
               )",
        )
        .bind(&payload.ciphertext)
        .bind(&payload.nonce[..])
        .bind(i64::from(payload.key_version))
        .bind(mutation_digest)
        .bind(&now)
        .bind(id)
        .bind(i64::from(payload.key_version))
        .execute(&self.pool)
        .await?
        .rows_affected();
        self.assert_cas(affected, id, "snapshot_ready|generating", "generated")
            .await
    }

    pub async fn mark_committing(&self, id: &str) -> AppResult<BridgeOperationRecord> {
        let now = now_rfc3339();
        let affected = sqlx::query(
            "UPDATE bridge_operations
             SET status = 'committing', updated_at = ?
             WHERE id = ? AND status = 'generated' AND commit_state = 'not_started'",
        )
        .bind(&now)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        self.assert_cas(affected, id, "generated", "committing")
            .await
    }

    pub async fn mark_committed(
        &self,
        id: &str,
        connector_result_json: &str,
    ) -> AppResult<BridgeOperationRecord> {
        self.mark_committed_with_state(id, connector_result_json, OperationCommitState::NotStarted)
            .await
    }

    pub async fn mark_committed_with_state(
        &self,
        id: &str,
        connector_result_json: &str,
        expected_commit_state: OperationCommitState,
    ) -> AppResult<BridgeOperationRecord> {
        let now = now_rfc3339();
        let affected = sqlx::query(
            "UPDATE bridge_operations
             SET status = 'committed',
                 commit_state = 'applied',
                 connector_result_json = ?,
                 updated_at = ?
             WHERE id = ? AND status = 'committing' AND commit_state = ?",
        )
        .bind(connector_result_json)
        .bind(&now)
        .bind(id)
        .bind(expected_commit_state.as_str())
        .execute(&self.pool)
        .await?
        .rows_affected();
        self.assert_cas(
            affected,
            id,
            &format!("committing/{}", expected_commit_state.as_str()),
            "committed/applied",
        )
        .await
    }

    pub async fn mark_delivered(&self, id: &str) -> AppResult<BridgeOperationRecord> {
        let now = now_rfc3339();
        let affected = sqlx::query(
            "UPDATE bridge_operations
             SET status = 'delivered', updated_at = ?
             WHERE id = ? AND status = 'committed' AND commit_state = 'applied'",
        )
        .bind(&now)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        self.assert_cas(affected, id, "committed", "delivered")
            .await
    }

    pub async fn mark_conflict(
        &self,
        id: &str,
        error_summary: &str,
    ) -> AppResult<BridgeOperationRecord> {
        let now = now_rfc3339();
        let affected = sqlx::query(
            "UPDATE bridge_operations
             SET status = 'conflict',
                 error_stage = 'commit',
                 error_code = 'ST_CHAT_CONFLICT',
                 error_summary = ?,
                 commit_state = 'not_applied',
                 updated_at = ?
             WHERE id = ? AND status = 'committing' AND commit_state = 'not_started'",
        )
        .bind(error_summary)
        .bind(&now)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        self.assert_cas(affected, id, "committing", "conflict")
            .await
    }

    pub async fn mark_failed(
        &self,
        id: &str,
        error_stage: &str,
        error_code: &str,
        error_summary: &str,
        retryable: bool,
    ) -> AppResult<BridgeOperationRecord> {
        self.mark_failed_with_state(
            id,
            error_stage,
            error_code,
            error_summary,
            retryable,
            OperationCommitState::NotStarted,
        )
        .await
    }

    pub async fn mark_failed_with_state(
        &self,
        id: &str,
        error_stage: &str,
        error_code: &str,
        error_summary: &str,
        retryable: bool,
        commit_state: OperationCommitState,
    ) -> AppResult<BridgeOperationRecord> {
        let now = now_rfc3339();
        let affected = sqlx::query(
            "UPDATE bridge_operations
             SET status = 'failed',
                 error_stage = ?,
                 error_code = ?,
                 error_summary = ?,
                 retryable = ?,
                 commit_state = ?,
                 updated_at = ?
             WHERE id = ?
               AND status IN ('received', 'snapshot_ready', 'generating', 'generated', 'committing')
               AND commit_state = 'not_started'",
        )
        .bind(error_stage)
        .bind(error_code)
        .bind(error_summary)
        .bind(i64::from(retryable))
        .bind(commit_state.as_str())
        .bind(&now)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        self.assert_cas(affected, id, "pre-commit", "failed").await
    }

    pub async fn mark_interrupted(&self, id: &str) -> AppResult<BridgeOperationRecord> {
        let now = now_rfc3339();
        let affected = sqlx::query(
            "UPDATE bridge_operations
             SET status = 'interrupted', updated_at = ?
             WHERE id = ? AND status = 'generating' AND commit_state = 'not_started'",
        )
        .bind(&now)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        self.assert_cas(affected, id, "generating", "interrupted")
            .await
    }

    pub async fn mark_commit_unknown(&self, id: &str) -> AppResult<BridgeOperationRecord> {
        let now = now_rfc3339();
        let affected = sqlx::query(
            "UPDATE bridge_operations
             SET commit_state = 'unknown', updated_at = ?
             WHERE id = ? AND status = 'committing' AND commit_state = 'not_started'",
        )
        .bind(&now)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        self.assert_cas(affected, id, "committing", "committing(unknown)")
            .await
    }

    pub async fn get_operation_key_reference(
        &self,
        id: &str,
    ) -> AppResult<Option<OperationKeyReference>> {
        let row: Option<(String, i64)> = sqlx::query_as(
            "SELECT wrapped_secret_id, key_version FROM operation_key_refs WHERE operation_id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        let Some((wrapped_secret_id, key_version)) = row else {
            return Ok(None);
        };
        if wrapped_secret_id.trim().is_empty() {
            return Err(AppError::internal(
                "operation wrapped secret reference is invalid",
            ));
        }
        let key_version = u32::try_from(key_version)
            .map_err(|_| AppError::internal("operation key version is invalid"))?;
        if key_version == 0 {
            return Err(AppError::internal("operation key version is invalid"));
        }
        Ok(Some(OperationKeyReference {
            wrapped_secret_id,
            key_version,
        }))
    }

    pub async fn put_operation_key_reference(
        &self,
        id: &str,
        wrapped_secret_id: &str,
        key_version: u32,
    ) -> AppResult<()> {
        if id.trim().is_empty() || wrapped_secret_id.trim().is_empty() || key_version == 0 {
            return Err(AppError::bad_request(
                "PAYLOAD_KEY_REFERENCE_INVALID",
                "operation wrapped secret reference is invalid",
            ));
        }
        let now = now_rfc3339();
        let inserted = sqlx::query(
            "INSERT INTO operation_key_refs
                (operation_id, wrapped_secret_id, key_version, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(operation_id) DO NOTHING",
        )
        .bind(id)
        .bind(wrapped_secret_id)
        .bind(i64::from(key_version))
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() == 1 {
            return Ok(());
        }
        let existing = self.get_operation_key_reference(id).await?.ok_or_else(|| {
            AppError::internal("operation key reference disappeared after conflict")
        })?;
        if existing.wrapped_secret_id != wrapped_secret_id || existing.key_version != key_version {
            return Err(AppError::conflict(
                "PAYLOAD_KEY_REFERENCE_CONFLICT",
                "operation already has a different wrapped key reference",
            ));
        }
        Ok(())
    }

    pub async fn try_acquire_execution_lease(
        &self,
        operation_id: &str,
        bot_id: &str,
        instance_id: &str,
        lease_until: &str,
    ) -> AppResult<Option<OperationExecutionLease>> {
        if operation_id.trim().is_empty()
            || bot_id.trim().is_empty()
            || instance_id.trim().is_empty()
            || lease_until.trim().is_empty()
        {
            return Err(AppError::bad_request(
                "OPERATION_LEASE_INVALID",
                "operation execution lease identity is incomplete",
            ));
        }
        let now = now_rfc3339();
        let row: Option<(String, String, i64, String)> = sqlx::query_as(
            "INSERT INTO operation_execution_leases (
                operation_id, instance_id, lease_generation, lease_until, created_at, updated_at
             )
             SELECT ?, ?, 1, ?, ?, ?
             WHERE EXISTS (
                 SELECT 1 FROM bridge_operations WHERE id = ? AND bot_id = ?
             )
             ON CONFLICT(operation_id) DO UPDATE SET
                 instance_id = excluded.instance_id,
                 lease_generation = operation_execution_leases.lease_generation + 1,
                 lease_until = excluded.lease_until,
                 updated_at = excluded.updated_at
             WHERE (operation_execution_leases.instance_id = excluded.instance_id
                    OR operation_execution_leases.lease_until <= ?)
               AND operation_execution_leases.lease_generation < 9223372036854775807
             RETURNING operation_id, instance_id, lease_generation, lease_until",
        )
        .bind(operation_id)
        .bind(instance_id)
        .bind(lease_until)
        .bind(&now)
        .bind(&now)
        .bind(operation_id)
        .bind(bot_id)
        .bind(&now)
        .fetch_optional(&self.pool)
        .await?;
        row.map(
            |(operation_id, instance_id, lease_generation, lease_until)| {
                let lease_generation = u64::try_from(lease_generation)
                    .map_err(|_| AppError::internal("operation lease generation is invalid"))?;
                if lease_generation == 0 {
                    return Err(AppError::internal("operation lease generation is invalid"));
                }
                Ok(OperationExecutionLease {
                    operation_id,
                    instance_id,
                    lease_generation,
                    lease_until,
                })
            },
        )
        .transpose()
    }

    pub async fn release_execution_lease(
        &self,
        lease: &OperationExecutionLease,
    ) -> AppResult<bool> {
        let generation = i64::try_from(lease.lease_generation)
            .map_err(|_| AppError::internal("operation lease generation is invalid"))?;
        let result = sqlx::query(
            "DELETE FROM operation_execution_leases
             WHERE operation_id = ? AND instance_id = ? AND lease_generation = ?",
        )
        .bind(&lease.operation_id)
        .bind(&lease.instance_id)
        .bind(generation)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn list_recovery_candidates_for_bot(
        &self,
        bot_id: &str,
        limit: usize,
    ) -> AppResult<Vec<BridgeOperationRecord>> {
        if bot_id.trim().is_empty() {
            return Err(AppError::bad_request(
                "OPERATION_BOT_ID_REQUIRED",
                "operation recovery bot id is required",
            ));
        }
        let limit = i64::try_from(limit.clamp(1, 256))
            .map_err(|_| AppError::internal("operation recovery limit is invalid"))?;
        let rows = sqlx::query_as::<_, OperationRow>(select_operations!(
            "
             WHERE bot_id = ?
               AND (
                   status IN ('generated', 'committing')
                   OR (status = 'committed' AND commit_state = 'applied')
               )
             ORDER BY updated_at ASC, id ASC
             LIMIT ?"
        ))
        .bind(bot_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(OperationRow::into_record).collect()
    }

    pub async fn get_operation(&self, id: &str) -> AppResult<Option<BridgeOperationRecord>> {
        let row = sqlx::query_as::<_, OperationRow>(select_operations!(" WHERE id = ?"))
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(OperationRow::into_record).transpose()
    }

    pub async fn get_by_claim(
        &self,
        bot_id: &str,
        update_id: i64,
        kind: &str,
    ) -> AppResult<Option<BridgeOperationRecord>> {
        let row = sqlx::query_as::<_, OperationRow>(select_operations!(
            "
             WHERE bot_id = ? AND telegram_update_id = ? AND operation_kind = ?"
        ))
        .bind(bot_id)
        .bind(update_id)
        .bind(kind)
        .fetch_optional(&self.pool)
        .await?;
        row.map(OperationRow::into_record).transpose()
    }

    pub async fn list_by_status(
        &self,
        status: BridgeOperationStatus,
    ) -> AppResult<Vec<BridgeOperationRecord>> {
        let rows = sqlx::query_as::<_, OperationRow>(select_operations!(
            "
             WHERE status = ? ORDER BY created_at ASC, id ASC"
        ))
        .bind(status.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(OperationRow::into_record).collect()
    }

    pub async fn list_unknown_commits(&self) -> AppResult<Vec<BridgeOperationRecord>> {
        let rows = sqlx::query_as::<_, OperationRow>(select_operations!(
            "
             WHERE status = 'committing' AND commit_state = 'unknown'
             ORDER BY created_at ASC, id ASC"
        ))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(OperationRow::into_record).collect()
    }

    pub async fn interrupt_stale_generating(&self, cutoff_rfc3339: &str) -> AppResult<Vec<String>> {
        let now = now_rfc3339();
        let rows: Vec<(String,)> = sqlx::query_as(
            "UPDATE bridge_operations
             SET status = 'interrupted', updated_at = ?
             WHERE status = 'generating' AND updated_at < ?
             RETURNING id",
        )
        .bind(&now)
        .bind(cutoff_rfc3339)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|row| row.0).collect())
    }

    pub async fn dry_run_retention_cleanup(&self, cutoff_rfc3339: &str) -> AppResult<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT id FROM bridge_operations
             WHERE status = 'delivered'
               AND commit_state = 'applied'
               AND created_at < ?
             ORDER BY created_at ASC, id ASC",
        )
        .bind(cutoff_rfc3339)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|row| row.0).collect())
    }

    async fn assert_cas(
        &self,
        affected: u64,
        id: &str,
        expected_from: &str,
        expected_to: &str,
    ) -> AppResult<BridgeOperationRecord> {
        if affected != 1 {
            return Err(AppError::conflict(
                "BRIDGE_OPERATION_CAS_CONFLICT",
                format!("operation {id} rejected transition {expected_from} -> {expected_to}"),
            ));
        }
        self.get_operation(id)
            .await?
            .ok_or_else(|| AppError::internal("operation missing after cas update"))
    }
}

impl BridgeOperationRecord {
    fn matches_claim(&self, request: &ClaimOperationRequest) -> bool {
        (request.id.trim().is_empty() || self.id == request.id)
            && self.actor_id == request.actor_id
            && self.bot_id == request.bot_id
            && self.telegram_update_id == request.telegram_update_id
            && self.channel_context_key == request.channel_context_key
            && self.operation_kind == request.operation_kind
            && self.locator.handle == request.locator.handle
            && self.locator.avatar == request.locator.avatar
            && self.locator.chat_file == request.locator.chat_file
    }
}

#[derive(sqlx::FromRow)]
struct OperationRow {
    id: String,
    actor_id: String,
    bot_id: String,
    telegram_update_id: i64,
    channel_context_key: String,
    operation_kind: String,
    st_handle: Option<String>,
    st_character_avatar: Option<String>,
    st_chat_file: Option<String>,
    status: String,
    source_sha256: Option<String>,
    source_integrity: Option<String>,
    source_size: Option<i64>,
    message_count: Option<i64>,
    operation_payload_ciphertext: Option<Vec<u8>>,
    operation_payload_nonce: Option<Vec<u8>>,
    operation_payload_key_version: Option<i64>,
    mutation_digest: Option<String>,
    connector_result_json: Option<String>,
    error_stage: Option<String>,
    error_code: Option<String>,
    error_summary: Option<String>,
    retryable: i64,
    commit_state: String,
    attempt_count: i64,
    request_id: Option<String>,
    trace_id: Option<String>,
    created_at: String,
    updated_at: String,
}

impl OperationRow {
    fn into_record(self) -> AppResult<BridgeOperationRecord> {
        let status = BridgeOperationStatus::parse(&self.status).ok_or_else(|| {
            AppError::internal(format!("unknown bridge operation status {}", self.status))
        })?;
        let commit_state = OperationCommitState::parse(&self.commit_state).ok_or_else(|| {
            AppError::internal(format!(
                "unknown bridge operation commit_state {}",
                self.commit_state
            ))
        })?;
        Ok(BridgeOperationRecord {
            id: self.id,
            actor_id: self.actor_id,
            bot_id: self.bot_id,
            telegram_update_id: self.telegram_update_id,
            channel_context_key: self.channel_context_key,
            operation_kind: self.operation_kind,
            locator: StChatLocator {
                handle: self.st_handle.unwrap_or_default(),
                avatar: self.st_character_avatar.unwrap_or_default(),
                character_name: String::new(),
                chat_file: self.st_chat_file.unwrap_or_default(),
            },
            status,
            commit_state,
            source_sha256: self.source_sha256,
            source_integrity: self.source_integrity,
            source_size: self.source_size,
            message_count: self.message_count,
            mutation_digest: self.mutation_digest,
            payload: map_payload(
                self.operation_payload_ciphertext,
                self.operation_payload_nonce,
                self.operation_payload_key_version,
            )?,
            connector_result_json: self.connector_result_json,
            error_stage: self.error_stage,
            error_code: self.error_code,
            error_summary: self.error_summary,
            retryable: self.retryable != 0,
            attempt_count: u32::try_from(self.attempt_count)
                .map_err(|_| AppError::internal("operation attempt count is invalid"))?,
            request_id: self.request_id.unwrap_or_default(),
            trace_id: self.trace_id.unwrap_or_default(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

fn map_payload(
    ciphertext: Option<Vec<u8>>,
    nonce: Option<Vec<u8>>,
    key_version: Option<i64>,
) -> AppResult<Option<EncryptedOperationPayload>> {
    match (ciphertext, nonce, key_version) {
        (None, None, None) => Ok(None),
        (Some(ciphertext), Some(nonce), Some(key_version)) => {
            if nonce.len() != 24 {
                return Err(AppError::internal(
                    "operation payload nonce has invalid length",
                ));
            }
            let mut nonce_bytes = [0u8; 24];
            nonce_bytes.copy_from_slice(&nonce);
            let key_version = u32::try_from(key_version)
                .map_err(|_| AppError::internal("operation payload key_version is invalid"))?;
            Ok(Some(EncryptedOperationPayload {
                ciphertext,
                nonce: nonce_bytes,
                key_version,
            }))
        }
        _ => Err(AppError::internal(
            "operation payload ciphertext/nonce/key_version are inconsistent",
        )),
    }
}
