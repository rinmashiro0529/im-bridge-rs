use serde_json::json;
use sqlx::SqlitePool;

use crate::clock::now_rfc3339;
use crate::error::{AppError, AppResult};
use crate::ids::new_id;
use crate::modules::telegram::panel::{PanelKind, PanelSlot, TelegramPanel};

type PanelSlotRow = (String, String, i64, i64, i64, String, i64, i64, i64);

#[derive(Clone)]
pub struct PanelStore {
    pool: SqlitePool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnMessageRow {
    pub message_id: i64,
    pub chunk_index: usize,
    pub status: String,
}

impl PanelStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn save_slot(&self, slot: &PanelSlot) -> AppResult<()> {
        let kind = encode_kind(&slot.kind)?;
        let now = now_rfc3339();
        let mut tx = self.pool.begin().await?;
        if slot.active {
            sqlx::query(
                "UPDATE telegram_panel_slots
                 SET active = 0, updated_at = ?
                 WHERE account_id = ? AND numeric_bot_id = ? AND chat_id = ? AND active = 1 AND panel_id <> ?",
            )
            .bind(&now)
            .bind(&slot.account_id)
            .bind(slot.numeric_bot_id)
            .bind(slot.chat_id)
            .bind(&slot.panel_id)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "INSERT INTO telegram_panel_slots
                (panel_id, account_id, numeric_bot_id, chat_id, message_id, kind, revision,
                 active, expires_at_unix, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(panel_id) DO UPDATE SET
                account_id = excluded.account_id,
                numeric_bot_id = excluded.numeric_bot_id,
                chat_id = excluded.chat_id,
                message_id = excluded.message_id,
                kind = excluded.kind,
                revision = excluded.revision,
                active = excluded.active,
                expires_at_unix = excluded.expires_at_unix,
                updated_at = excluded.updated_at",
        )
        .bind(&slot.panel_id)
        .bind(&slot.account_id)
        .bind(slot.numeric_bot_id)
        .bind(slot.chat_id)
        .bind(slot.message_id)
        .bind(kind)
        .bind(encode_revision(slot.revision)?)
        .bind(if slot.active { 1_i64 } else { 0_i64 })
        .bind(slot.expires_at_unix)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn find_active_slot(
        &self,
        numeric_bot_id: i64,
        chat_id: i64,
    ) -> AppResult<Option<PanelSlot>> {
        let row: Option<PanelSlotRow> = sqlx::query_as(
            "SELECT panel_id, account_id, numeric_bot_id, chat_id, message_id, kind,
                    revision, active, expires_at_unix
             FROM telegram_panel_slots
             WHERE numeric_bot_id = ? AND chat_id = ? AND active = 1
             ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(numeric_bot_id)
        .bind(chat_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(decode_slot).transpose()
    }

    pub async fn find_active_slot_for_account_chat(
        &self,
        account_id: &str,
        chat_id: i64,
    ) -> AppResult<Option<PanelSlot>> {
        let row: Option<PanelSlotRow> = sqlx::query_as(
            "SELECT panel_id, account_id, numeric_bot_id, chat_id, message_id, kind,
                    revision, active, expires_at_unix
             FROM telegram_panel_slots
             WHERE account_id = ? AND chat_id = ? AND active = 1
             ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(account_id)
        .bind(chat_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(decode_slot).transpose()
    }

    pub async fn find_active_slot_for_scope(
        &self,
        account_id: &str,
        numeric_bot_id: i64,
        chat_id: i64,
    ) -> AppResult<Option<PanelSlot>> {
        let row: Option<PanelSlotRow> = sqlx::query_as(
            "SELECT panel_id, account_id, numeric_bot_id, chat_id, message_id, kind,
                    revision, active, expires_at_unix
             FROM telegram_panel_slots
             WHERE account_id = ? AND numeric_bot_id = ? AND chat_id = ? AND active = 1
             ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(account_id)
        .bind(numeric_bot_id)
        .bind(chat_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(decode_slot).transpose()
    }

    pub async fn claim_panel_revision(
        &self,
        slot: &PanelSlot,
        expected_revision: u64,
        panel: &TelegramPanel,
        next_slot: &PanelSlot,
    ) -> AppResult<Option<String>> {
        let now = now_rfc3339();
        let payload = serde_json::to_string(&json!({
            "expected_revision": expected_revision,
            "slot": slot,
            "next_slot": next_slot,
            "panel": panel,
        }))
        .map_err(|_| AppError::internal("panel effect payload serialization failed"))?;
        let effect_id = new_id();
        let result = sqlx::query(
            "INSERT INTO telegram_panel_effects
                (effect_id, panel_id, chat_id, effect_type, payload, status, attempt_token, expected_revision, created_at, updated_at)
             SELECT ?, ?, ?, 'replace', ?, 'pending', ?, ?, ?, ?
             WHERE EXISTS (
                 SELECT 1 FROM telegram_panel_slots
                 WHERE panel_id = ? AND account_id = ? AND numeric_bot_id = ? AND chat_id = ?
                   AND revision = ? AND active = 1
             )
             AND NOT EXISTS (
                 SELECT 1 FROM telegram_panel_effects
                 WHERE panel_id = ? AND effect_type = 'replace' AND status IN ('pending', 'sending') AND payload = ?
             )",
        )
        .bind(&effect_id)
        .bind(&slot.panel_id)
        .bind(slot.chat_id)
        .bind(&payload)
        .bind(&effect_id)
        .bind(encode_revision(expected_revision)?)
        .bind(&now)
        .bind(&now)
        .bind(&slot.panel_id)
        .bind(&slot.account_id)
        .bind(slot.numeric_bot_id)
        .bind(slot.chat_id)
        .bind(encode_revision(expected_revision)?)
        .bind(&slot.panel_id)
        .bind(&payload)
        .execute(&self.pool)
        .await?;
        Ok((result.rows_affected() == 1).then_some(effect_id))
    }

    pub async fn mark_panel_revision_sending(
        &self,
        effect_id: &str,
        slot: &PanelSlot,
        expected_revision: u64,
    ) -> AppResult<()> {
        self.mark_panel_revision_status(effect_id, slot, expected_revision, "sending")
            .await
    }
    pub async fn mark_panel_revision_sent(
        &self,
        effect_id: &str,
        slot: &PanelSlot,
        expected_revision: u64,
    ) -> AppResult<()> {
        self.mark_panel_revision_status(effect_id, slot, expected_revision, "sent")
            .await
    }

    pub async fn mark_panel_revision_failed(
        &self,
        effect_id: &str,
        slot: &PanelSlot,
        expected_revision: u64,
    ) -> AppResult<()> {
        self.mark_panel_revision_status(effect_id, slot, expected_revision, "failed")
            .await
    }

    async fn mark_panel_revision_status(
        &self,
        effect_id: &str,
        slot: &PanelSlot,
        expected_revision: u64,
        status: &str,
    ) -> AppResult<()> {
        let expected_status = if status == "sending" {
            "pending"
        } else {
            "sending"
        };
        let changed = sqlx::query(
            "UPDATE telegram_panel_effects
             SET status = ?, terminal_at = CASE WHEN ? IN ('sent', 'failed') THEN ? ELSE terminal_at END, updated_at = ?
             WHERE effect_id = ? AND attempt_token = ? AND panel_id = ? AND chat_id = ?
               AND expected_revision = ? AND effect_type = 'replace' AND status = ?",
        )
        .bind(status)
        .bind(status)
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .bind(effect_id)
        .bind(effect_id)
        .bind(&slot.panel_id)
        .bind(slot.chat_id)
        .bind(encode_revision(expected_revision)?)
        .bind(expected_status)
        .execute(&self.pool)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_PANEL_EFFECT_CAS_CONFLICT",
                "panel effect status changed concurrently",
            ));
        }
        Ok(())
    }

    pub async fn complete_panel_replace(
        &self,
        effect_id: &str,
        slot: &PanelSlot,
        expected_revision: u64,
        next_slot: &PanelSlot,
    ) -> AppResult<()> {
        let kind = encode_kind(&next_slot.kind)?;
        let mut tx = self.pool.begin().await?;
        let slot_changed = sqlx::query(
            "UPDATE telegram_panel_slots
             SET revision = ?, kind = ?, message_id = ?, expires_at_unix = ?, updated_at = ?
             WHERE panel_id = ? AND account_id = ? AND numeric_bot_id = ? AND chat_id = ?
               AND revision = ? AND active = 1",
        )
        .bind(encode_revision(next_slot.revision)?)
        .bind(kind)
        .bind(next_slot.message_id)
        .bind(next_slot.expires_at_unix)
        .bind(now_rfc3339())
        .bind(&slot.panel_id)
        .bind(&slot.account_id)
        .bind(slot.numeric_bot_id)
        .bind(slot.chat_id)
        .bind(encode_revision(expected_revision)?)
        .execute(&mut *tx)
        .await?;
        if slot_changed.rows_affected() != 1 {
            if let Err(rollback_error) = tx.rollback().await {
                tracing::error!(error = %rollback_error, "panel slot transaction rollback failed");
            }
            return Err(AppError::conflict(
                "TELEGRAM_PANEL_STALE",
                "panel slot revision changed before effect confirmation",
            ));
        }
        let effect_changed = sqlx::query(
            "UPDATE telegram_panel_effects
             SET status = 'sent', terminal_at = ?, updated_at = ?
             WHERE effect_id = ? AND attempt_token = ? AND panel_id = ? AND chat_id = ?
               AND expected_revision = ? AND effect_type = 'replace' AND status = 'sending'",
        )
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .bind(effect_id)
        .bind(effect_id)
        .bind(&slot.panel_id)
        .bind(slot.chat_id)
        .bind(encode_revision(expected_revision)?)
        .execute(&mut *tx)
        .await?;
        if effect_changed.rows_affected() != 1 {
            if let Err(rollback_error) = tx.rollback().await {
                tracing::error!(error = %rollback_error, "panel slot transaction rollback failed");
            }
            return Err(AppError::conflict(
                "TELEGRAM_PANEL_EFFECT_CAS_CONFLICT",
                "panel effect confirmation changed concurrently",
            ));
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn cas_update_slot_scoped(
        &self,
        slot: &PanelSlot,
        expected_revision: u64,
        next_slot: &PanelSlot,
    ) -> AppResult<bool> {
        let kind = encode_kind(&next_slot.kind)?;
        let result = sqlx::query(
            "UPDATE telegram_panel_slots
             SET revision = ?, kind = ?, message_id = ?, expires_at_unix = ?, updated_at = ?
             WHERE panel_id = ? AND account_id = ? AND numeric_bot_id = ? AND chat_id = ?
               AND revision = ? AND active = 1",
        )
        .bind(encode_revision(next_slot.revision)?)
        .bind(kind)
        .bind(next_slot.message_id)
        .bind(next_slot.expires_at_unix)
        .bind(now_rfc3339())
        .bind(&slot.panel_id)
        .bind(&slot.account_id)
        .bind(slot.numeric_bot_id)
        .bind(slot.chat_id)
        .bind(encode_revision(expected_revision)?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn cas_update_slot(
        &self,
        panel_id: &str,
        expected_revision: u64,
        new_kind: &PanelKind,
        expires_at_unix: i64,
    ) -> AppResult<bool> {
        let kind = encode_kind(new_kind)?;
        let result = sqlx::query(
            "UPDATE telegram_panel_slots
             SET revision = revision + 1, kind = ?, expires_at_unix = ?, updated_at = ?
             WHERE panel_id = ? AND revision = ? AND active = 1",
        )
        .bind(kind)
        .bind(expires_at_unix)
        .bind(now_rfc3339())
        .bind(panel_id)
        .bind(encode_revision(expected_revision)?)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn retire_slot(&self, panel_id: &str) -> AppResult<()> {
        sqlx::query(
            "UPDATE telegram_panel_slots SET active = 0, updated_at = ? WHERE panel_id = ?",
        )
        .bind(now_rfc3339())
        .bind(panel_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_slot_message_id(&self, panel_id: &str, message_id: i64) -> AppResult<()> {
        sqlx::query(
            "UPDATE telegram_panel_slots SET message_id = ?, updated_at = ? WHERE panel_id = ?",
        )
        .bind(message_id)
        .bind(now_rfc3339())
        .bind(panel_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // This compatibility guard mirrors the legacy call shape while rejecting incomplete scope.
    #[allow(clippy::too_many_arguments)]
    pub async fn record_turn_message(
        &self,
        _chat_id: i64,
        _turn_id: &str,
        _operation_id: &str,
        _chunk_index: usize,
        _message_id: i64,
        _content_hash: &str,
        _render_version: i64,
        _status: &str,
    ) -> AppResult<()> {
        Err(AppError::conflict(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "Telegram turn ledger writes require a complete scope",
        ))
    }

    pub async fn update_turn_message(
        &self,
        _chat_id: i64,
        _turn_id: &str,
        _chunk_index: usize,
        _message_id: i64,
        _content_hash: &str,
        _status: &str,
    ) -> AppResult<bool> {
        Err(AppError::conflict(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "Telegram turn ledger updates require a complete scope",
        ))
    }

    pub async fn find_turn_messages(
        &self,
        _chat_id: i64,
        _turn_id: &str,
    ) -> AppResult<Vec<TurnMessageRow>> {
        Err(AppError::conflict(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "Telegram turn ledger queries require a complete scope",
        ))
    }

    pub async fn mark_turn_message_status(&self, _message_id: i64, _status: &str) -> AppResult<()> {
        Err(AppError::conflict(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "Telegram turn status updates require a complete scope",
        ))
    }
}

fn encode_revision(revision: u64) -> AppResult<i64> {
    i64::try_from(revision).map_err(|_| {
        AppError::bad_request(
            "TELEGRAM_PANEL_REVISION_INVALID",
            "panel revision exceeds SQLite integer range",
        )
    })
}

// Retained for compatibility with persisted legacy panel rows.
#[allow(dead_code)]
fn encode_chunk_index(chunk_index: usize) -> AppResult<i64> {
    i64::try_from(chunk_index).map_err(|_| {
        AppError::bad_request(
            "TELEGRAM_PANEL_CHUNK_INDEX_INVALID",
            "panel chunk index exceeds SQLite integer range",
        )
    })
}

fn decode_revision(revision: i64) -> AppResult<u64> {
    u64::try_from(revision).map_err(|_| AppError::internal("stored panel revision is invalid"))
}

// Retained for compatibility with persisted legacy panel rows.
#[allow(dead_code)]
fn decode_chunk_index(chunk_index: i64) -> AppResult<usize> {
    usize::try_from(chunk_index)
        .map_err(|_| AppError::internal("stored turn message chunk index is invalid"))
}

fn encode_kind(kind: &PanelKind) -> AppResult<String> {
    serde_json::to_string(kind)
        .map_err(|err| AppError::internal(format!("panel kind encode failed: {err}")))
}

fn decode_slot(
    (
        panel_id,
        account_id,
        numeric_bot_id,
        chat_id,
        message_id,
        kind,
        revision,
        active,
        expires_at_unix,
    ): (String, String, i64, i64, i64, String, i64, i64, i64),
) -> AppResult<PanelSlot> {
    let kind = serde_json::from_str(&kind)
        .map_err(|err| AppError::internal(format!("panel kind decode failed: {err}")))?;
    Ok(PanelSlot {
        panel_id,
        account_id,
        numeric_bot_id,
        chat_id,
        message_id,
        kind,
        revision: decode_revision(revision)?,
        active: active != 0,
        expires_at_unix,
    })
}
