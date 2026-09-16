use sqlx::SqlitePool;

use crate::clock::now_rfc3339;
use crate::error::AppResult;
use crate::modules::bridge::errors::StBridgeError;

#[derive(Clone)]
pub struct ErrorEventStore {
    pool: SqlitePool,
}

impl ErrorEventStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn record(
        &self,
        error: &StBridgeError,
        actor_id: Option<&str>,
        bot_id: Option<&str>,
        chat_id: Option<&str>,
        telegram_update_id: Option<i64>,
    ) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO bridge_error_events
                (operation_id, actor_id, bot_id, chat_id, telegram_update_id, stage, code,
                 safe_message, safe_detail, upstream_http_status, upstream_code, endpoint_class,
                 retryable, commit_state, attempt, duration_ms, request_id, trace_id, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(error.operation_id.as_deref())
        .bind(actor_id)
        .bind(bot_id)
        .bind(chat_id)
        .bind(telegram_update_id)
        .bind(error.stage.as_str())
        .bind(error.code.as_str())
        .bind(&error.safe_message)
        .bind(error.safe_detail.as_deref())
        .bind(error.upstream_http_status.map(i64::from))
        .bind(error.upstream_code.as_deref())
        .bind(error.endpoint_class.as_deref())
        .bind(i64::from(error.retryable))
        .bind(error.commit_state.as_str())
        .bind(i64::from(error.attempt))
        .bind(error.duration_ms.map(|value| value as i64))
        .bind(&error.request_id)
        .bind(&error.trace_id)
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
