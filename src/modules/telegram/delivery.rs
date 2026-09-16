use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::{json, Value};
use sha2::Digest;
use sqlx::SqlitePool;
use tokio::sync::Mutex;

use crate::clock::now_rfc3339;
use crate::error::{AppError, AppResult};
use crate::ids::new_id;
use crate::modules::telegram::delivery_queue::DeliveryCoordinator;
use crate::modules::telegram::panel::{InlineButton, PanelEffect, PanelParseMode, TelegramPanel};
use crate::modules::telegram::stream::{chunk_text, StreamPolicy, StreamState};
use crate::seams::llm_gateway::{ProgressEvent, ProgressSink};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryOutcome {
    Applied {
        message_id: Option<i64>,
    },
    RetryAt {
        retry_at_unix_ms: i64,
        safe_code: String,
    },
    Rejected {
        safe_code: String,
    },
    Unknown {
        effect_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnScope {
    pub account_id: String,
    pub internal_bot_id: String,
    pub numeric_bot_id: i64,
    pub chat_id: i64,
    pub locator_hash: String,
}

impl TurnScope {
    pub fn new(
        account_id: impl Into<String>,
        internal_bot_id: impl Into<String>,
        numeric_bot_id: i64,
        chat_id: i64,
        locator_hash: impl Into<String>,
    ) -> AppResult<Self> {
        let scope = Self {
            account_id: account_id.into(),
            internal_bot_id: internal_bot_id.into(),
            numeric_bot_id,
            chat_id,
            locator_hash: locator_hash.into(),
        };
        if scope.account_id.trim().is_empty()
            || scope.internal_bot_id.trim().is_empty()
            || scope.numeric_bot_id <= 0
            || scope.chat_id == 0
            || !scope
                .locator_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || scope.locator_hash.len() != 64
            || scope.locator_hash != scope.locator_hash.to_ascii_lowercase()
        {
            return Err(AppError::bad_request(
                "TELEGRAM_TURN_SCOPE_INVALID",
                "Telegram turn scope is incomplete or invalid",
            ));
        }
        Ok(scope)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnMetadata {
    pub scope: TurnScope,
    pub turn_id: String,
    pub operation_id: String,
}

impl TurnMetadata {
    pub fn new_scoped(
        scope: TurnScope,
        turn_id: impl Into<String>,
        operation_id: impl Into<String>,
    ) -> AppResult<Self> {
        let turn_id = turn_id.into();
        let operation_id = operation_id.into();
        if turn_id.trim().is_empty() || operation_id.trim().is_empty() {
            return Err(AppError::bad_request(
                "TELEGRAM_TURN_ID_INVALID",
                "Telegram turn and operation IDs are required",
            ));
        }
        Ok(Self {
            scope,
            turn_id,
            operation_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenTurnTarget {
    pub operation_id: String,
    pub scope: TurnScope,
    pub target_turn_id: String,
    pub tail_fingerprint: String,
    pub target_revision: u64,
    pub old_message_ids: Vec<i64>,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RevokeEffectResult {
    pub edited_tombstone_message_id: Option<i64>,
    pub deleted_extra_message_ids: Vec<i64>,
    pub failed_message_ids: Vec<i64>,
}

fn retry_after_seconds(error: &AppError) -> u64 {
    error
        .message
        .split("retry_after=")
        .nth(1)
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1)
}

fn retry_at_rfc3339(retry_after_secs: u64) -> AppResult<String> {
    let seconds = i64::try_from(retry_after_secs).map_err(|_| {
        AppError::bad_gateway(
            "TELEGRAM_RETRY_AFTER_INVALID",
            "Telegram retry_after exceeds the supported range",
        )
    })?;
    time::OffsetDateTime::now_utc()
        .checked_add(time::Duration::seconds(seconds))
        .ok_or_else(|| {
            AppError::bad_gateway(
                "TELEGRAM_RETRY_AFTER_INVALID",
                "Telegram retry_after produces an out-of-range timestamp",
            )
        })?
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|_| {
            AppError::bad_gateway(
                "TELEGRAM_RETRY_AFTER_INVALID",
                "Telegram retry_after produces an invalid timestamp",
            )
        })
}

fn encode_chunk_index(chunk_index: usize) -> AppResult<i64> {
    i64::try_from(chunk_index).map_err(|_| {
        AppError::bad_request(
            "TELEGRAM_TURN_CHUNK_INDEX_INVALID",
            "Telegram turn chunk index exceeds SQLite integer range",
        )
    })
}

#[derive(Clone)]
pub struct TelegramDelivery {
    client: reqwest::Client,
    pool: SqlitePool,
    token: String,
    api_base: String,
    bot_id: String,
    numeric_bot_id: i64,
    chat_id: i64,
    inter_message_delay: Duration,
    policy: StreamPolicy,
    rate_limiter: Arc<DeliveryCoordinator>,
}

impl TelegramDelivery {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: reqwest::Client,
        pool: SqlitePool,
        token: String,
        api_base: String,
        bot_id: String,
        numeric_bot_id: i64,
        chat_id: i64,
        inter_message_delay_ms: i64,
        stream_min_interval_ms: i64,
        stream_min_delta_chars: i64,
        stream_first_render_chars: i64,
        stream_chunk_size: i64,
    ) -> Self {
        Self::new_with_coordinator(
            client,
            pool,
            token,
            api_base,
            bot_id,
            numeric_bot_id,
            chat_id,
            inter_message_delay_ms,
            stream_min_interval_ms,
            stream_min_delta_chars,
            stream_first_render_chars,
            stream_chunk_size,
            DeliveryCoordinator::global(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_coordinator(
        client: reqwest::Client,
        pool: SqlitePool,
        token: String,
        api_base: String,
        bot_id: String,
        numeric_bot_id: i64,
        chat_id: i64,
        inter_message_delay_ms: i64,
        stream_min_interval_ms: i64,
        stream_min_delta_chars: i64,
        stream_first_render_chars: i64,
        stream_chunk_size: i64,
        rate_limiter: Arc<DeliveryCoordinator>,
    ) -> Self {
        Self {
            client,
            pool,
            token,
            api_base: api_base.trim_end_matches('/').to_string(),
            bot_id,
            numeric_bot_id,
            chat_id,
            inter_message_delay: Duration::from_millis(inter_message_delay_ms.max(0) as u64),
            policy: StreamPolicy {
                min_interval_ms: stream_min_interval_ms.max(0) as u64,
                min_delta_chars: stream_min_delta_chars.max(1) as usize,
                first_render_chars: stream_first_render_chars.max(1) as usize,
                chunk_size: stream_chunk_size.clamp(1, 4000) as usize,
            },
            rate_limiter,
        }
    }

    pub async fn send_reply(&self, text: &str, markup: Option<Value>) -> AppResult<()> {
        let chunks = chunk_text(text, self.policy.chunk_size);
        let mut markup = markup;
        for (index, chunk) in chunks.into_iter().enumerate() {
            if index > 0 {
                tokio::time::sleep(self.inter_message_delay).await;
            }
            self.send_single_reply_result(None, index, &chunk, markup.take())
                .await?;
        }
        Ok(())
    }

    pub async fn send_operation_reply(
        &self,
        operation_id: &str,
        text: &str,
        markup: Option<Value>,
    ) -> AppResult<()> {
        if operation_id.trim().is_empty() {
            return Err(AppError::bad_request(
                "TELEGRAM_OPERATION_ID_REQUIRED",
                "operation reply requires an operation id",
            ));
        }
        let chunks = chunk_text(text, self.policy.chunk_size);
        let mut markup = markup;
        for (index, chunk) in chunks.into_iter().enumerate() {
            if index > 0 {
                tokio::time::sleep(self.inter_message_delay).await;
            }
            self.send_single_reply_result(Some(operation_id), index, &chunk, markup.take())
                .await?;
        }
        Ok(())
    }
    pub async fn send_business_reply(
        &self,
        metadata: &TurnMetadata,
        text: &str,
        markup: Option<Value>,
    ) -> AppResult<Vec<i64>> {
        let chunks = chunk_text(text, self.policy.chunk_size);
        let mut ids = Vec::with_capacity(chunks.len());
        let mut markup = markup;
        for (index, chunk) in chunks.into_iter().enumerate() {
            if index > 0 {
                tokio::time::sleep(self.inter_message_delay).await;
            }
            ids.push(
                self.send_single_scoped_result(metadata, index, &chunk, markup.take())
                    .await?,
            );
        }
        Ok(ids)
    }

    async fn send_single_scoped_result(
        &self,
        metadata: &TurnMetadata,
        chunk_index: usize,
        text: &str,
        markup: Option<Value>,
    ) -> AppResult<i64> {
        self.send_single_scoped_result_with_revision(metadata, chunk_index, text, markup, 1)
            .await
    }

    async fn send_single_scoped_result_with_revision(
        &self,
        metadata: &TurnMetadata,
        chunk_index: usize,
        text: &str,
        markup: Option<Value>,
        render_version: u64,
    ) -> AppResult<i64> {
        let delivery_id = self
            .create_delivery("reply", Some(&metadata.operation_id))
            .await?;
        self.mark_sending(&delivery_id).await?;
        match self.send_message(text, markup).await {
            Ok(message_id) => {
                if let Err(error) = self
                    .record_scoped_turn_message_with_revision(
                        metadata,
                        chunk_index,
                        message_id,
                        text,
                        "sent",
                        render_version,
                    )
                    .await
                {
                    return Err(self
                        .mark_unknown_after_persistence_failure(&delivery_id, &error)
                        .await);
                }
                if let Err(error) = self.mark_sent_result(&delivery_id, message_id).await {
                    return Err(self
                        .mark_unknown_after_persistence_failure(&delivery_id, &error)
                        .await);
                }
                Ok(message_id)
            }
            Err(err) => {
                if err.code == "TELEGRAM_RATE_LIMITED" {
                    self.mark_retry_pending(&delivery_id, &err).await?;
                } else if err.code == "TELEGRAM_REQUEST_FAILED"
                    || err.code == "TELEGRAM_RESPONSE_INVALID"
                {
                    self.mark_unknown(&delivery_id, &err).await?;
                } else {
                    self.mark_failed(&delivery_id, &err).await?;
                }
                Err(err)
            }
        }
    }

    pub async fn send_reply_with_turn(
        &self,
        turn_id: Option<&str>,
        text: &str,
        markup: Option<Value>,
    ) {
        let chunks = chunk_text(text, self.policy.chunk_size);
        let mut markup = markup;
        for (index, chunk) in chunks.into_iter().enumerate() {
            if index > 0 {
                tokio::time::sleep(self.inter_message_delay).await;
            }
            self.send_single_reply(turn_id, index, &chunk, markup.take())
                .await;
        }
    }

    pub async fn send_turn_chunk(
        &self,
        _turn_id: &str,
        _chunk_index: usize,
        _text: &str,
    ) -> AppResult<i64> {
        Err(AppError::conflict(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "Telegram turn chunk sends require explicit turn metadata",
        ))
    }

    pub async fn send_turn_chunk_scoped(
        &self,
        metadata: &TurnMetadata,
        target_revision: u64,
        chunk_index: usize,
        text: &str,
    ) -> AppResult<i64> {
        let next_revision = target_revision.checked_add(1).ok_or_else(|| {
            AppError::conflict(
                "TELEGRAM_TURN_REVISION_INVALID",
                "Telegram turn revision overflow",
            )
        })?;
        self.send_single_scoped_result_with_revision(
            metadata,
            chunk_index,
            text,
            None,
            next_revision,
        )
        .await
    }

    pub async fn send_reply_with_turn_ids(
        &self,
        turn_id: &str,
        text: &str,
        markup: Option<Value>,
    ) -> AppResult<Vec<i64>> {
        let chunks = chunk_text(text, self.policy.chunk_size);
        let mut ids = Vec::with_capacity(chunks.len());
        let mut markup = markup;
        for (index, chunk) in chunks.into_iter().enumerate() {
            if index > 0 {
                tokio::time::sleep(self.inter_message_delay).await;
            }
            ids.push(
                self.send_single_reply_result(Some(turn_id), index, &chunk, markup.take())
                    .await?,
            );
        }
        Ok(ids)
    }

    async fn send_single_reply_result(
        &self,
        turn_id: Option<&str>,
        _chunk_index: usize,
        text: &str,
        markup: Option<Value>,
    ) -> AppResult<i64> {
        let delivery_id = self.create_delivery("reply", turn_id).await?;
        self.mark_sending(&delivery_id).await?;
        match self.send_message(text, markup).await {
            Ok(message_id) => {
                if let Err(error) = self.mark_sent_result(&delivery_id, message_id).await {
                    return Err(self
                        .mark_unknown_after_persistence_failure(&delivery_id, &error)
                        .await);
                }
                Ok(message_id)
            }
            Err(err) => {
                if err.code == "TELEGRAM_RATE_LIMITED" {
                    self.mark_retry_pending(&delivery_id, &err).await?;
                } else if err.code == "TELEGRAM_REQUEST_FAILED"
                    || err.code == "TELEGRAM_RESPONSE_INVALID"
                {
                    self.mark_unknown(&delivery_id, &err).await?;
                } else {
                    self.mark_failed(&delivery_id, &err).await?;
                }
                Err(err)
            }
        }
    }

    async fn send_single_reply(
        &self,
        turn_id: Option<&str>,
        _chunk_index: usize,
        text: &str,
        markup: Option<Value>,
    ) {
        let delivery_id = match self.create_delivery("reply", turn_id).await {
            Ok(id) => Some(id),
            Err(error) => {
                tracing::error!(code = %error.code, "telegram delivery creation failed");
                None
            }
        };
        if let Some(id) = delivery_id.as_deref() {
            if let Err(err) = self.mark_sending(id).await {
                tracing::error!(code = %err.code, "telegram delivery state unavailable before send");
                return;
            }
        }
        match self.send_message(text, markup).await {
            Ok(message_id) => {
                if let Some(id) = delivery_id.as_deref() {
                    self.mark_sent_best_effort(id, message_id).await;
                }
            }
            Err(err) => {
                tracing::error!(bot_id = %self.bot_id, chat_id = self.chat_id, code = %err.code, "telegram send failed");
                if let Some(id) = delivery_id.as_deref() {
                    if err.code == "TELEGRAM_RATE_LIMITED" {
                        if let Err(mark_err) = self.mark_retry_pending(id, &err).await {
                            tracing::error!(code = %mark_err.code, "telegram retry state persist failed");
                        }
                    } else if err.code == "TELEGRAM_REQUEST_FAILED"
                        || err.code == "TELEGRAM_RESPONSE_INVALID"
                    {
                        if let Err(state_error) = self.mark_unknown(id, &err).await {
                            tracing::error!(delivery_id = id, code = %state_error.code, "telegram unknown state persist failed");
                        }
                    } else {
                        if let Err(state_error) = self.mark_failed(id, &err).await {
                            tracing::error!(delivery_id = id, code = %state_error.code, "telegram failed state persist failed");
                        }
                    }
                }
            }
        }
    }

    pub async fn stream_sink(
        &self,
        metadata: TurnMetadata,
        placeholder: &str,
    ) -> AppResult<TelegramProgressSink> {
        let existing: Option<(String, String)> = sqlx::query_as(
            "SELECT id, external_message_id FROM channel_deliveries
             WHERE bot_id = ? AND chat_id = ? AND message_kind = 'generation'
               AND turn_id = ? AND status = 'sent' AND external_message_id IS NOT NULL
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(&self.bot_id)
        .bind(self.chat_id.to_string())
        .bind(&metadata.operation_id)
        .fetch_optional(&self.pool)
        .await?;
        let (delivery_id, message_id) = if let Some((delivery_id, message_id)) = existing {
            let message_id = message_id.parse::<i64>().map_err(|_| {
                AppError::bad_gateway(
                    "TELEGRAM_RESPONSE_INVALID",
                    "stored generation message_id is invalid",
                )
            })?;
            if message_id <= 0 {
                return Err(AppError::bad_gateway(
                    "TELEGRAM_RESPONSE_INVALID",
                    "stored generation message_id is invalid",
                ));
            }
            (delivery_id, message_id)
        } else {
            let delivery_id = self
                .create_delivery("generation", Some(&metadata.operation_id))
                .await?;
            self.mark_sending(&delivery_id).await?;
            let message_id = match self.send_message(placeholder, None).await {
                Ok(message_id) => message_id,
                Err(err) => {
                    if err.code == "TELEGRAM_RATE_LIMITED" {
                        self.mark_retry_pending(&delivery_id, &err).await?;
                    } else if err.code == "TELEGRAM_REQUEST_FAILED"
                        || err.code == "TELEGRAM_RESPONSE_INVALID"
                    {
                        self.mark_unknown(&delivery_id, &err).await?;
                    } else {
                        self.mark_failed(&delivery_id, &err).await?;
                    }
                    return Err(err);
                }
            };
            if let Err(error) = self
                .record_scoped_turn_message_with_revision(
                    &metadata,
                    0,
                    message_id,
                    placeholder,
                    "sent",
                    1,
                )
                .await
            {
                return Err(self
                    .mark_unknown_after_persistence_failure(&delivery_id, &error)
                    .await);
            }
            if let Err(error) = self.mark_sent_result(&delivery_id, message_id).await {
                return Err(self
                    .mark_unknown_after_persistence_failure(&delivery_id, &error)
                    .await);
            }
            (delivery_id, message_id)
        };
        Ok(TelegramProgressSink {
            delivery: self.clone(),
            delivery_id: Some(delivery_id),
            message_id: Some(message_id),
            metadata,
            render_version: Arc::new(Mutex::new(1)),
            state: Arc::new(Mutex::new(ProgressState {
                stream: StreamState::new(),
                accumulated_text: String::new(),
                finalized: false,
            })),
        })
    }

    async fn create_delivery(&self, kind: &str, turn_id: Option<&str>) -> AppResult<String> {
        let id = new_id();
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO channel_deliveries
                (id, bot_id, chat_id, message_kind, turn_id, attempt_count, status, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, 1, 'pending', ?, ?)",
        )
        .bind(&id)
        .bind(&self.bot_id)
        .bind(self.chat_id.to_string())
        .bind(kind)
        .bind(turn_id)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    // Compatibility wrapper used by older delivery fixtures and pending migration work.
    #[allow(dead_code)]
    async fn record_scoped_turn_message(
        &self,
        metadata: &TurnMetadata,
        chunk_index: usize,
        message_id: i64,
        text: &str,
        status: &str,
    ) -> AppResult<()> {
        self.record_scoped_turn_message_with_revision(
            metadata,
            chunk_index,
            message_id,
            text,
            status,
            1,
        )
        .await
    }

    async fn record_scoped_turn_message_with_revision(
        &self,
        metadata: &TurnMetadata,
        chunk_index: usize,
        message_id: i64,
        text: &str,
        status: &str,
        render_version: u64,
    ) -> AppResult<()> {
        let render_version = i64::try_from(render_version).map_err(|_| {
            AppError::bad_request(
                "TELEGRAM_TURN_REVISION_INVALID",
                "Telegram turn revision is invalid",
            )
        })?;
        let content_hash = hex::encode(sha2::Sha256::digest(text.as_bytes()));
        let now = now_rfc3339();
        let result = sqlx::query(
            "INSERT INTO telegram_turn_messages
                (account_id, internal_bot_id, numeric_bot_id, chat_id, locator_hash,
                 turn_id, operation_id, chunk_index, message_id, content_hash,
                 render_version, status, lifecycle, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)",
        )
        .bind(&metadata.scope.account_id)
        .bind(&metadata.scope.internal_bot_id)
        .bind(metadata.scope.numeric_bot_id)
        .bind(metadata.scope.chat_id)
        .bind(&metadata.scope.locator_hash)
        .bind(&metadata.turn_id)
        .bind(&metadata.operation_id)
        .bind(encode_chunk_index(chunk_index)?)
        .bind(message_id)
        .bind(content_hash)
        .bind(render_version)
        .bind(status)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_TURN_LEDGER_WRITE_FAILED",
                "Telegram turn ledger did not record exactly one message",
            ));
        }
        Ok(())
    }

    async fn mark_sending(&self, id: &str) -> AppResult<()> {
        let changed = sqlx::query(
            "UPDATE channel_deliveries
             SET status = 'sending', attempt_token = ?, updated_at = ?
             WHERE id = ? AND status IN ('pending', 'retry_pending')",
        )
        .bind(new_id())
        .bind(now_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_DELIVERY_STATE_STALE",
                "Telegram 投递状态已被其他尝试接管",
            ));
        }
        Ok(())
    }

    async fn mark_sent_result(&self, id: &str, message_id: i64) -> AppResult<()> {
        let changed = sqlx::query(
            "UPDATE channel_deliveries SET external_message_id = ?, status = 'sent', last_error = NULL, updated_at = ? WHERE id = ? AND status = 'sending'",
        )
        .bind(message_id.to_string())
        .bind(now_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_DELIVERY_STATE_STALE",
                "Telegram delivery confirmation no longer matches the active attempt",
            ));
        }
        self.rate_limiter.forget_pending(id).await;
        Ok(())
    }

    async fn mark_sent_best_effort(&self, id: &str, message_id: i64) {
        if let Err(error) = self.mark_sent_result(id, message_id).await {
            tracing::error!(delivery_id = id, code = %error.code, "telegram control delivery sent state persist failed");
        }
    }

    pub async fn replace_turn_chunk(
        &self,
        _turn_id: &str,
        _chunk_index: usize,
        _old_message_id: i64,
        _text: &str,
    ) -> AppResult<i64> {
        Err(AppError::conflict(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "Telegram turn replacement requires a frozen scope",
        ))
    }

    pub async fn replace_turn_chunk_scoped(
        &self,
        metadata: &TurnMetadata,
        target_revision: u64,
        chunk_index: usize,
        old_message_id: i64,
        text: &str,
    ) -> AppResult<i64> {
        let delivery_id = self
            .create_delivery("reply", Some(&metadata.operation_id))
            .await?;
        self.mark_sending(&delivery_id).await?;
        let new_message_id = match self.send_message(text, None).await {
            Ok(message_id) => message_id,
            Err(err) => {
                if err.code == "TELEGRAM_RATE_LIMITED" {
                    self.mark_retry_pending(&delivery_id, &err).await?;
                } else if err.code == "TELEGRAM_REQUEST_FAILED"
                    || err.code == "TELEGRAM_RESPONSE_INVALID"
                {
                    self.mark_unknown(&delivery_id, &err).await?;
                } else {
                    self.mark_failed(&delivery_id, &err).await?;
                }
                return Err(err);
            }
        };
        let next_revision = target_revision.checked_add(1).ok_or_else(|| {
            AppError::conflict(
                "TELEGRAM_TURN_REVISION_INVALID",
                "Telegram turn revision overflow",
            )
        })?;
        let content_hash = hex::encode(sha2::Sha256::digest(text.as_bytes()));
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(error) => {
                let persistence_error = AppError::from(error);
                return Err(self
                    .mark_unknown_after_persistence_failure(&delivery_id, &persistence_error)
                    .await);
            }
        };
        let updated = match sqlx::query(
            "UPDATE telegram_turn_messages
             SET message_id = ?, content_hash = ?, render_version = ?, status = 'sent', lifecycle = 'active', updated_at = ?
             WHERE account_id = ? AND internal_bot_id = ? AND numeric_bot_id = ?
               AND chat_id = ? AND locator_hash = ? AND turn_id = ?
               AND chunk_index = ? AND message_id = ? AND render_version <= ?
               AND status IN ('sent', 'active') AND lifecycle IN ('sent', 'active')",
        )
        .bind(new_message_id)
        .bind(content_hash)
        .bind(i64::try_from(next_revision).map_err(|_| {
            AppError::conflict("TELEGRAM_TURN_REVISION_INVALID", "Telegram turn revision is invalid")
        })?)
        .bind(now_rfc3339())
        .bind(&metadata.scope.account_id)
        .bind(&metadata.scope.internal_bot_id)
        .bind(metadata.scope.numeric_bot_id)
        .bind(metadata.scope.chat_id)
        .bind(&metadata.scope.locator_hash)
        .bind(&metadata.turn_id)
        .bind(encode_chunk_index(chunk_index)?)
        .bind(old_message_id)
        .bind(i64::try_from(target_revision).map_err(|_| {
            AppError::conflict("TELEGRAM_TURN_REVISION_INVALID", "Telegram turn revision is invalid")
        })?)
        .execute(&mut *tx)
        .await
        {
            Ok(updated) => updated,
            Err(error) => {
                let persistence_error = AppError::from(error);
                if let Err(rollback_error) = tx.rollback().await {
                    tracing::error!(error = %rollback_error, "telegram replacement ledger rollback failed");
                }
                return Err(self
                    .mark_unknown_after_persistence_failure(&delivery_id, &persistence_error)
                    .await);
            }
        };
        if updated.rows_affected() != 1 {
            if let Err(rollback_error) = tx.rollback().await {
                tracing::error!(error = %rollback_error, "telegram replacement ledger rollback failed");
            }
            let persistence_error = AppError::conflict(
                "TELEGRAM_TURN_LEDGER_STALE",
                "Telegram turn replacement revision or target no longer matches",
            );
            return Err(self
                .mark_unknown_after_persistence_failure(&delivery_id, &persistence_error)
                .await);
        }
        let delivery_update = match sqlx::query(
            "UPDATE channel_deliveries
             SET external_message_id = ?, status = 'sent', terminal_evidence_json = ?, updated_at = ?
             WHERE id = ? AND status = 'sending'",
        )
        .bind(new_message_id.to_string())
        .bind(json!({"replaced_message_id": old_message_id}).to_string())
        .bind(now_rfc3339())
        .bind(&delivery_id)
        .execute(&mut *tx)
        .await
        {
            Ok(updated) => updated,
            Err(error) => {
                if let Err(rollback_error) = tx.rollback().await {
                    tracing::error!(error = %rollback_error, "telegram replacement delivery rollback failed");
                }
                let persistence_error = AppError::from(error);
                return Err(self
                    .mark_unknown_after_persistence_failure(&delivery_id, &persistence_error)
                    .await);
            }
        };
        if delivery_update.rows_affected() != 1 {
            if let Err(rollback_error) = tx.rollback().await {
                tracing::error!(error = %rollback_error, "telegram replacement ledger rollback failed");
            }
            let persistence_error = AppError::conflict(
                "TELEGRAM_DELIVERY_STATE_STALE",
                "Telegram replacement delivery confirmation no longer matches",
            );
            return Err(self
                .mark_unknown_after_persistence_failure(&delivery_id, &persistence_error)
                .await);
        }
        if let Err(error) = tx.commit().await {
            let persistence_error = AppError::from(error);
            return Err(self
                .mark_unknown_after_persistence_failure(&delivery_id, &persistence_error)
                .await);
        }
        if let Err(error) = self
            .delete_message_in_chat(metadata.scope.chat_id, old_message_id)
            .await
        {
            let unknown = self
                .mark_unknown_after_persistence_failure(&delivery_id, &error)
                .await;
            return Err(unknown);
        }
        self.rate_limiter.forget_pending(&delivery_id).await;
        Ok(new_message_id)
    }

    pub async fn update_turn_chunk(
        &self,
        _turn_id: &str,
        _chunk_index: usize,
        _message_id: i64,
        _text: &str,
    ) -> AppResult<()> {
        Err(AppError::conflict(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "Telegram turn updates require a frozen scope",
        ))
    }

    pub async fn update_turn_chunk_scoped(
        &self,
        scope: &TurnScope,
        target_revision: u64,
        turn_id: &str,
        chunk_index: usize,
        message_id: i64,
        text: &str,
    ) -> AppResult<()> {
        let next_revision = target_revision.checked_add(1).ok_or_else(|| {
            AppError::conflict(
                "TELEGRAM_TURN_REVISION_INVALID",
                "Telegram turn revision overflow",
            )
        })?;
        let content_hash = hex::encode(sha2::Sha256::digest(text.as_bytes()));
        let updated = sqlx::query(
            "UPDATE telegram_turn_messages
             SET content_hash = ?, render_version = ?, status = 'sent', updated_at = ?
             WHERE account_id = ? AND internal_bot_id = ? AND numeric_bot_id = ?
               AND chat_id = ? AND locator_hash = ? AND turn_id = ?
               AND chunk_index = ? AND message_id = ? AND render_version <= ?
               AND status IN ('sent', 'active') AND lifecycle IN ('sent', 'active')",
        )
        .bind(content_hash)
        .bind(i64::try_from(next_revision).map_err(|_| {
            AppError::conflict(
                "TELEGRAM_TURN_REVISION_INVALID",
                "Telegram turn revision is invalid",
            )
        })?)
        .bind(now_rfc3339())
        .bind(&scope.account_id)
        .bind(&scope.internal_bot_id)
        .bind(scope.numeric_bot_id)
        .bind(scope.chat_id)
        .bind(&scope.locator_hash)
        .bind(turn_id)
        .bind(encode_chunk_index(chunk_index)?)
        .bind(message_id)
        .bind(i64::try_from(target_revision).map_err(|_| {
            AppError::conflict(
                "TELEGRAM_TURN_REVISION_INVALID",
                "Telegram turn revision is invalid",
            )
        })?)
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_TURN_LEDGER_STALE",
                "Telegram turn ledger no longer matches the active scoped message",
            ));
        }
        Ok(())
    }

    pub async fn assert_turn_revision(
        &self,
        scope: &TurnScope,
        turn_id: &str,
        expected_revision: u64,
    ) -> AppResult<()> {
        let stored: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(render_version) FROM telegram_turn_messages
             WHERE account_id = ? AND internal_bot_id = ? AND numeric_bot_id = ?
               AND chat_id = ? AND locator_hash = ? AND turn_id = ?
               AND lifecycle IN ('active', 'sent') AND status IN ('sent', 'active')",
        )
        .bind(&scope.account_id)
        .bind(&scope.internal_bot_id)
        .bind(scope.numeric_bot_id)
        .bind(scope.chat_id)
        .bind(&scope.locator_hash)
        .bind(turn_id)
        .fetch_one(&self.pool)
        .await?;
        let stored = stored.ok_or_else(|| {
            AppError::conflict(
                "TELEGRAM_TURN_LEDGER_STALE",
                "Telegram turn revision is missing",
            )
        })?;
        let expected = i64::try_from(expected_revision).map_err(|_| {
            AppError::conflict(
                "TELEGRAM_TURN_REVISION_INVALID",
                "Telegram turn revision is invalid",
            )
        })?;
        if stored != expected {
            return Err(AppError::conflict(
                "TELEGRAM_TURN_REVISION_CONFLICT",
                "Telegram turn revision changed concurrently",
            ));
        }
        Ok(())
    }

    pub async fn retire_turn_scoped(
        &self,
        scope: &TurnScope,
        turn_id: &str,
        operation_id: &str,
    ) -> AppResult<()> {
        if turn_id.trim().is_empty()
            || operation_id.trim().is_empty()
            || scope.chat_id != self.chat_id
        {
            return Err(AppError::bad_request(
                "TELEGRAM_TURN_SCOPE_INVALID",
                "Telegram turn retirement identity is invalid",
            ));
        }
        let changed = sqlx::query(
            "UPDATE telegram_turn_messages
             SET lifecycle = 'retired', status = 'retired', terminal_at = ?, updated_at = ?
             WHERE account_id = ? AND internal_bot_id = ? AND numeric_bot_id = ?
               AND chat_id = ? AND locator_hash = ? AND turn_id = ?
               AND operation_id != ? AND lifecycle IN ('active', 'sent')
               AND status IN ('sent', 'active')",
        )
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .bind(&scope.account_id)
        .bind(&scope.internal_bot_id)
        .bind(scope.numeric_bot_id)
        .bind(scope.chat_id)
        .bind(&scope.locator_hash)
        .bind(turn_id)
        .bind(operation_id)
        .execute(&self.pool)
        .await?;
        if changed.rows_affected() == 0 {
            return Err(AppError::conflict(
                "TELEGRAM_TURN_LEDGER_STALE",
                "Telegram turn retirement matched no active scoped message",
            ));
        }
        Ok(())
    }

    async fn link_generation(&self, id: &str, generation_run_id: &str) -> AppResult<()> {
        let changed = sqlx::query(
            "UPDATE channel_deliveries SET generation_run_id = ?, updated_at = ? WHERE id = ?",
        )
        .bind(generation_run_id)
        .bind(now_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_DELIVERY_STATE_STALE",
                "Telegram generation link no longer matches a delivery",
            ));
        }
        Ok(())
    }

    async fn mark_unknown_after_persistence_failure(
        &self,
        id: &str,
        persistence_error: &AppError,
    ) -> AppError {
        let unknown_code = "TELEGRAM_COMMIT_STATE_UNKNOWN";
        self.rate_limiter.forget_pending(id).await;
        match sqlx::query(
            "UPDATE channel_deliveries
             SET status = 'unknown', last_error = ?, terminal_at = ?, updated_at = ?
             WHERE id = ? AND status IN ('sending', 'sent')",
        )
        .bind(unknown_code)
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await
        {
            Ok(changed) if changed.rows_affected() == 1 => AppError::conflict(
                unknown_code,
                format!(
                    "persistence_error={}; delivery_state=unknown",
                    persistence_error.code
                ),
            ),
            Ok(_) => AppError::conflict(
                unknown_code,
                format!(
                    "persistence_error={}; unknown_state_persistence_not_confirmed",
                    persistence_error.code
                ),
            ),
            Err(unknown_error) => AppError::internal(format!(
                "persistence_error={}; unknown_state_error={unknown_error}",
                persistence_error.code
            )),
        }
    }
    async fn mark_unknown(&self, id: &str, err: &AppError) -> AppResult<()> {
        let changed = sqlx::query(
            "UPDATE channel_deliveries SET status = 'unknown', last_error = ?, terminal_at = ?, updated_at = ? WHERE id = ? AND status = 'sending'",
        )
        .bind(err.code)
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_DELIVERY_STATE_STALE",
                "Telegram unknown delivery state did not update exactly one attempt",
            ));
        }
        self.rate_limiter.forget_pending(id).await;
        Ok(())
    }

    async fn mark_retry_pending(&self, id: &str, err: &AppError) -> AppResult<()> {
        let retry_after_secs = retry_after_seconds(err);
        let next_attempt_at = retry_at_rfc3339(retry_after_secs)?;
        let changed = sqlx::query(
            "UPDATE channel_deliveries
             SET status = 'retry_pending', last_error = ?, next_attempt_at = ?,
                 attempt_token = ?, attempt_count = attempt_count + 1, updated_at = ?
             WHERE id = ? AND status = 'sending'",
        )
        .bind(err.code)
        .bind(&next_attempt_at)
        .bind(new_id())
        .bind(now_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_DELIVERY_STATE_STALE",
                "Telegram retry state did not update exactly one active attempt",
            ));
        }
        self.rate_limiter
            .enqueue_pending(id.to_string(), &self.bot_id, self.chat_id)
            .await;
        Ok(())
    }

    async fn mark_failed(&self, id: &str, err: &AppError) -> AppResult<()> {
        let changed = sqlx::query(
            "UPDATE channel_deliveries SET status = 'failed', last_error = ?, terminal_at = ?, updated_at = ? WHERE id = ? AND status = 'sending'",
        )
        .bind(err.code)
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_DELIVERY_STATE_STALE",
                "Telegram failed delivery state did not update exactly one attempt",
            ));
        }
        self.rate_limiter.forget_pending(id).await;
        Ok(())
    }

    async fn mark_status(&self, scope: &TurnScope, message_id: i64, status: &str) -> AppResult<()> {
        let changed = sqlx::query(
            "UPDATE telegram_turn_messages
             SET status = ?, lifecycle = ?, updated_at = ?
             WHERE account_id = ? AND internal_bot_id = ? AND numeric_bot_id = ?
               AND chat_id = ? AND locator_hash = ? AND message_id = ?
               AND status IN ('sent', 'active')",
        )
        .bind(status)
        .bind(if status == "deleted" || status == "revoked" {
            "retired"
        } else {
            "active"
        })
        .bind(now_rfc3339())
        .bind(&scope.account_id)
        .bind(&scope.internal_bot_id)
        .bind(scope.numeric_bot_id)
        .bind(scope.chat_id)
        .bind(&scope.locator_hash)
        .bind(message_id)
        .execute(&self.pool)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_TURN_LEDGER_STALE",
                "Telegram turn status update did not match exactly one scoped message",
            ));
        }
        Ok(())
    }

    async fn send_message(&self, text: &str, markup: Option<Value>) -> AppResult<i64> {
        let mut body = json!({"chat_id": self.chat_id, "text": text});
        if let Some(markup) = markup {
            body["reply_markup"] = markup;
        }
        let result = self.call("sendMessage", body).await?;
        result
            .pointer("/message_id")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                AppError::bad_gateway(
                    "TELEGRAM_RESPONSE_INVALID",
                    "sendMessage returned no message_id",
                )
            })
    }

    async fn edit_message(&self, message_id: i64, text: &str) -> AppResult<()> {
        let body = json!({
            "chat_id": self.chat_id,
            "message_id": message_id,
            "text": text,
        });
        match self.call("editMessageText", body).await {
            Ok(_) => Ok(()),
            Err(err) if err.message.contains("message is not modified") => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub async fn edit_text(&self, message_id: i64, text: &str) -> AppResult<()> {
        self.edit_message(message_id, text).await
    }

    pub fn bot_id(&self) -> &str {
        &self.bot_id
    }

    pub fn chat_id(&self) -> i64 {
        self.chat_id
    }

    pub fn numeric_bot_id(&self) -> AppResult<i64> {
        if self.numeric_bot_id <= 0 {
            return Err(AppError::service_unavailable(
                "TELEGRAM_BOT_SCOPE_UNAVAILABLE",
                "Telegram numeric bot scope is unavailable",
            ));
        }
        Ok(self.numeric_bot_id)
    }

    pub async fn delete_message(&self, message_id: i64) -> AppResult<()> {
        let body = json!({
            "chat_id": self.chat_id,
            "message_id": message_id,
        });
        self.call("deleteMessage", body).await.map(|_| ())
    }

    pub async fn answer_callback_query(
        &self,
        callback_query_id: &str,
        text: Option<&str>,
        show_alert: bool,
    ) -> AppResult<()> {
        let mut body = json!({
            "callback_query_id": callback_query_id,
            "show_alert": show_alert,
        });
        if let Some(text) = text {
            body["text"] = Value::String(text.to_string());
        }
        self.call("answerCallbackQuery", body).await.map(|_| ())
    }

    pub async fn execute_panel_effect(&self, effect: &PanelEffect) -> AppResult<Option<i64>> {
        match effect {
            PanelEffect::Create { chat_id, panel, .. } => {
                self.send_panel(*chat_id, panel).await.map(Some)
            }
            PanelEffect::Replace { slot, panel, .. } => {
                self.edit_panel(slot.chat_id, slot.message_id, panel)
                    .await?;
                Ok(Some(slot.message_id))
            }
            PanelEffect::Toast {
                callback_query_id: _,
                text: _,
                show_alert: _,
            } => Ok(None),
            PanelEffect::RetireKeyboard { slot } => {
                self.clear_reply_markup(slot.chat_id, slot.message_id)
                    .await?;
                Ok(Some(slot.message_id))
            }
            PanelEffect::DeleteChunk {
                chat_id,
                message_id,
                ..
            } => {
                self.delete_message_in_chat(*chat_id, *message_id).await?;
                Ok(None)
            }
            PanelEffect::FallbackSend { panel, .. } => {
                self.send_panel(self.chat_id, panel).await.map(Some)
            }
        }
    }

    pub async fn execute_redo_cleanup(
        &self,
        _chat_id: i64,
        _previous_ids: &[i64],
        _new_ids: &[i64],
    ) -> AppResult<()> {
        Err(AppError::conflict(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "Telegram redo cleanup requires a frozen scope",
        ))
    }

    pub async fn execute_redo_cleanup_scoped(
        &self,
        scope: &TurnScope,
        previous_ids: &[i64],
        new_ids: &[i64],
    ) -> AppResult<()> {
        let keep = new_ids.len().max(1);
        if previous_ids.len() <= keep || scope.chat_id != self.chat_id {
            return Ok(());
        }
        for message_id in previous_ids[keep..].iter().rev() {
            self.delete_message_in_chat(scope.chat_id, *message_id)
                .await?;
            self.mark_status(scope, *message_id, "deleted").await?;
        }
        Ok(())
    }

    async fn send_panel(&self, chat_id: i64, panel: &TelegramPanel) -> AppResult<i64> {
        let mut body = json!({
            "chat_id": chat_id,
            "text": panel.text.as_str(),
        });
        if let Some(parse_mode) = parse_mode_name(panel.parse_mode) {
            body["parse_mode"] = json!(parse_mode);
        }
        if !panel.keyboard.is_empty() {
            body["reply_markup"] = panel_markup(&panel.keyboard);
        }
        let result = self.call("sendMessage", body).await?;
        result
            .pointer("/message_id")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                AppError::bad_gateway(
                    "TELEGRAM_RESPONSE_INVALID",
                    "sendMessage returned no message_id",
                )
            })
    }

    async fn edit_panel(
        &self,
        chat_id: i64,
        message_id: i64,
        panel: &TelegramPanel,
    ) -> AppResult<()> {
        let mut body = json!({
            "chat_id": chat_id,
            "message_id": message_id,
            "text": panel.text.as_str(),
            "reply_markup": panel_markup(&panel.keyboard),
        });
        if let Some(parse_mode) = parse_mode_name(panel.parse_mode) {
            body["parse_mode"] = json!(parse_mode);
        }
        match self.call("editMessageText", body).await {
            Ok(_) => Ok(()),
            Err(err) if err.message.contains("message is not modified") => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn clear_reply_markup(&self, chat_id: i64, message_id: i64) -> AppResult<()> {
        self.call(
            "editMessageReplyMarkup",
            json!({
                "chat_id": chat_id,
                "message_id": message_id,
                "reply_markup": {"inline_keyboard": []},
            }),
        )
        .await
        .map(|_| ())
    }

    async fn delete_message_in_chat(&self, chat_id: i64, message_id: i64) -> AppResult<()> {
        self.call(
            "deleteMessage",
            json!({"chat_id": chat_id, "message_id": message_id}),
        )
        .await
        .map(|_| ())
    }

    pub async fn recover_pending_deliveries(&self) -> AppResult<usize> {
        let now = now_rfc3339();
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT id FROM channel_deliveries
             WHERE bot_id = ? AND CAST(chat_id AS INTEGER) = ? AND status IN ('pending', 'retry_pending')
               AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
             ORDER BY updated_at ASC LIMIT 128",
        )
        .bind(&self.bot_id)
        .bind(self.chat_id)
        .bind(&now)
        .fetch_all(&self.pool)
        .await?;
        for (delivery_id,) in &rows {
            self.rate_limiter
                .enqueue_pending(delivery_id.clone(), &self.bot_id, self.chat_id)
                .await;
        }
        Ok(rows.len())
    }

    pub async fn claim_due_delivery(
        &self,
        runtime_instance_id: &str,
        lease_for: Duration,
    ) -> AppResult<Option<(String, String)>> {
        if runtime_instance_id.trim().is_empty() || lease_for.is_zero() {
            return Err(AppError::bad_request(
                "TELEGRAM_DELIVERY_LEASE_INVALID",
                "Telegram delivery lease identity is invalid",
            ));
        }
        Err(AppError::service_unavailable(
            "TELEGRAM_DELIVERY_REPLAY_BLOCKED",
            "durable Telegram reply payload replay is unavailable; delivery remains blocked",
        ))
    }

    pub async fn take_due_delivery(&self) -> Option<String> {
        self.rate_limiter
            .take_due_for(&self.bot_id, self.chat_id)
            .await
            .map(|pending| pending.delivery_id)
    }

    pub async fn retry_pending_reply(
        &self,
        delivery_id: &str,
        _turn_id: &str,
        _chunk_index: usize,
        text: &str,
        markup: Option<Value>,
    ) -> AppResult<i64> {
        self.mark_sending(delivery_id).await?;
        match self.send_message(text, markup).await {
            Ok(message_id) => {
                if let Err(error) = self.mark_sent_result(delivery_id, message_id).await {
                    return Err(self
                        .mark_unknown_after_persistence_failure(delivery_id, &error)
                        .await);
                }
                Ok(message_id)
            }
            Err(err) => {
                if err.code == "TELEGRAM_RATE_LIMITED" {
                    self.mark_retry_pending(delivery_id, &err).await?;
                } else if err.code == "TELEGRAM_REQUEST_FAILED"
                    || err.code == "TELEGRAM_RESPONSE_INVALID"
                {
                    self.mark_unknown(delivery_id, &err).await?;
                } else {
                    self.mark_failed(delivery_id, &err).await?;
                }
                Err(err)
            }
        }
    }

    pub fn is_cooling_down(&self) -> bool {
        self.rate_limiter
            .is_cooling_down(&self.bot_id, self.chat_id)
    }

    pub fn cooldown_remaining(&self) -> Duration {
        self.rate_limiter
            .cooldown_remaining(&self.bot_id, self.chat_id)
    }

    pub async fn find_last_turn(
        &self,
        scope: &TurnScope,
    ) -> AppResult<(Option<String>, Vec<i64>, u64)> {
        let latest_turn: Option<(String,)> = sqlx::query_as(
            "SELECT t.turn_id FROM telegram_turn_messages t
             WHERE t.account_id = ? AND t.internal_bot_id = ? AND t.numeric_bot_id = ?
               AND t.chat_id = ? AND t.locator_hash = ?
               AND t.lifecycle IN ('active', 'sent') AND t.status IN ('sent', 'active')
             ORDER BY t.updated_at DESC, t.id DESC LIMIT 1",
        )
        .bind(&scope.account_id)
        .bind(&scope.internal_bot_id)
        .bind(scope.numeric_bot_id)
        .bind(scope.chat_id)
        .bind(&scope.locator_hash)
        .fetch_optional(&self.pool)
        .await?;
        let Some((turn_id,)) = latest_turn else {
            return Ok((None, Vec::new(), 0));
        };
        let rows: Vec<(i64,)> = sqlx::query_as(
            "SELECT t.message_id FROM telegram_turn_messages t
             WHERE t.account_id = ? AND t.internal_bot_id = ? AND t.numeric_bot_id = ?
               AND t.chat_id = ? AND t.locator_hash = ? AND t.turn_id = ?
               AND t.lifecycle IN ('active', 'sent') AND t.status IN ('sent', 'active')
             ORDER BY t.chunk_index ASC, t.id ASC",
        )
        .bind(&scope.account_id)
        .bind(&scope.internal_bot_id)
        .bind(scope.numeric_bot_id)
        .bind(scope.chat_id)
        .bind(&scope.locator_hash)
        .bind(&turn_id)
        .fetch_all(&self.pool)
        .await?;
        let revision: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(render_version) FROM telegram_turn_messages
             WHERE account_id = ? AND internal_bot_id = ? AND numeric_bot_id = ?
               AND chat_id = ? AND locator_hash = ? AND turn_id = ?
               AND lifecycle IN ('active', 'sent') AND status IN ('sent', 'active')",
        )
        .bind(&scope.account_id)
        .bind(&scope.internal_bot_id)
        .bind(scope.numeric_bot_id)
        .bind(scope.chat_id)
        .bind(&scope.locator_hash)
        .bind(&turn_id)
        .fetch_one(&self.pool)
        .await?;
        let revision = u64::try_from(
            revision
                .ok_or_else(|| AppError::internal("Telegram turn render revision is missing"))?,
        )
        .map_err(|_| {
            AppError::internal("Telegram turn render revision is outside the supported range")
        })?;
        Ok((
            Some(turn_id),
            rows.into_iter().map(|(id,)| id).collect(),
            revision,
        ))
    }

    pub async fn find_last_turn_message_ids(&self, scope: &TurnScope) -> AppResult<Vec<i64>> {
        Ok(self.find_last_turn(scope).await?.1)
    }

    pub async fn load_frozen_target(
        &self,
        operation_id: &str,
        scope: &TurnScope,
    ) -> AppResult<Option<FrozenTurnTarget>> {
        let row: Option<(String, String, String, i64, String, String, String)> = sqlx::query_as(
            "SELECT operation_id, target_turn_id, tail_fingerprint, target_revision, old_message_ids, status, scope
             FROM telegram_turn_targets
             WHERE operation_id = ? AND account_id = ? AND internal_bot_id = ?
               AND numeric_bot_id = ? AND chat_id = ? AND locator_hash = ?",
        )
        .bind(operation_id)
        .bind(&scope.account_id)
        .bind(&scope.internal_bot_id)
        .bind(scope.numeric_bot_id)
        .bind(scope.chat_id)
        .bind(&scope.locator_hash)
        .fetch_optional(&self.pool)
        .await?;
        let Some((
            operation_id,
            target_turn_id,
            tail_fingerprint,
            target_revision,
            old_ids,
            status,
            _stored_scope,
        )) = row
        else {
            return Ok(None);
        };
        let old_message_ids: Vec<i64> = serde_json::from_str(&old_ids).map_err(|_| {
            AppError::conflict(
                "TELEGRAM_TURN_TARGET_INVALID",
                "frozen Telegram target message IDs are invalid",
            )
        })?;
        if old_message_ids.iter().any(|message_id| *message_id <= 0)
            || tail_fingerprint.len() != 64
            || !tail_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || tail_fingerprint != tail_fingerprint.to_ascii_lowercase()
        {
            return Err(AppError::conflict(
                "TELEGRAM_TURN_TARGET_INVALID",
                "frozen Telegram target contains invalid identity data",
            ));
        }
        let target_revision = u64::try_from(target_revision).map_err(|_| {
            AppError::conflict(
                "TELEGRAM_TURN_TARGET_INVALID",
                "frozen Telegram target revision is invalid",
            )
        })?;
        Ok(Some(FrozenTurnTarget {
            operation_id,
            scope: scope.clone(),
            target_turn_id,
            tail_fingerprint,
            target_revision,
            old_message_ids,
            status,
        }))
    }

    pub async fn freeze_target(
        &self,
        operation_id: &str,
        scope: &TurnScope,
        target_turn_id: &str,
        tail_fingerprint: &str,
        target_revision: u64,
        old_message_ids: &[i64],
    ) -> AppResult<FrozenTurnTarget> {
        if operation_id.trim().is_empty()
            || target_turn_id.trim().is_empty()
            || tail_fingerprint.len() != 64
            || !tail_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || tail_fingerprint != tail_fingerprint.to_ascii_lowercase()
            || old_message_ids.iter().any(|message_id| *message_id <= 0)
        {
            return Err(AppError::bad_request(
                "TELEGRAM_TURN_TARGET_INVALID",
                "frozen Telegram target identity is invalid",
            ));
        }
        let revision = i64::try_from(target_revision).map_err(|_| {
            AppError::bad_request(
                "TELEGRAM_TURN_TARGET_INVALID",
                "frozen Telegram target revision is invalid",
            )
        })?;
        let old_message_ids = serde_json::to_string(old_message_ids).map_err(|_| {
            AppError::bad_request(
                "TELEGRAM_TURN_TARGET_INVALID",
                "frozen Telegram target IDs are invalid",
            )
        })?;
        let requested_message_ids: Vec<i64> =
            serde_json::from_str(&old_message_ids).map_err(|_| {
                AppError::bad_request(
                    "TELEGRAM_TURN_TARGET_INVALID",
                    "frozen Telegram target IDs are invalid",
                )
            })?;
        let scope_json = serde_json::to_string(&json!({
            "account_id": scope.account_id,
            "internal_bot_id": scope.internal_bot_id,
            "numeric_bot_id": scope.numeric_bot_id,
            "chat_id": scope.chat_id,
            "locator_hash": scope.locator_hash,
        }))
        .map_err(|_| AppError::internal("failed to serialize Telegram turn scope"))?;
        let now = now_rfc3339();
        let inserted = sqlx::query(
            "INSERT INTO telegram_turn_targets
                (operation_id, account_id, internal_bot_id, numeric_bot_id, chat_id, locator_hash,
                 target_turn_id, scope, tail_fingerprint, target_revision, old_message_ids, status, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'frozen', ?, ?)
             ON CONFLICT(operation_id) DO NOTHING",
        )
        .bind(operation_id)
        .bind(&scope.account_id)
        .bind(&scope.internal_bot_id)
        .bind(scope.numeric_bot_id)
        .bind(scope.chat_id)
        .bind(&scope.locator_hash)
        .bind(target_turn_id)
        .bind(scope_json)
        .bind(tail_fingerprint)
        .bind(revision)
        .bind(&old_message_ids)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() == 0 {
            let existing = self
                .load_frozen_target(operation_id, scope)
                .await?
                .ok_or_else(|| {
                    AppError::conflict(
                        "TELEGRAM_TURN_TARGET_IDENTITY_REUSED",
                        "frozen Telegram target identity was reused",
                    )
                })?;
            if existing.target_turn_id != target_turn_id
                || existing.tail_fingerprint != tail_fingerprint
                || existing.target_revision != target_revision
                || existing.old_message_ids != requested_message_ids
            {
                return Err(AppError::conflict(
                    "TELEGRAM_TURN_TARGET_IDENTITY_REUSED",
                    "frozen Telegram target identity was reused",
                ));
            }
            return Ok(existing);
        }
        self.load_frozen_target(operation_id, scope)
            .await?
            .ok_or_else(|| AppError::internal("frozen Telegram target disappeared after insert"))
    }

    pub async fn cas_target_status(
        &self,
        operation_id: &str,
        scope: &TurnScope,
        expected: &str,
        next: &str,
    ) -> AppResult<()> {
        let allowed = matches!(
            (expected, next),
            ("frozen", "applying")
                | ("applying", "applied")
                | ("applying", "failed")
                | ("applying", "unknown")
        );
        if !allowed {
            return Err(AppError::bad_request(
                "TELEGRAM_TURN_TARGET_STATUS_INVALID",
                "invalid frozen Telegram target status transition",
            ));
        }
        let changed = sqlx::query(
            "UPDATE telegram_turn_targets SET status = ?, updated_at = ?
             WHERE operation_id = ? AND account_id = ? AND internal_bot_id = ?
               AND numeric_bot_id = ? AND chat_id = ? AND locator_hash = ? AND status = ?",
        )
        .bind(next)
        .bind(now_rfc3339())
        .bind(operation_id)
        .bind(&scope.account_id)
        .bind(&scope.internal_bot_id)
        .bind(scope.numeric_bot_id)
        .bind(scope.chat_id)
        .bind(&scope.locator_hash)
        .bind(expected)
        .execute(&self.pool)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_TURN_TARGET_CAS_CONFLICT",
                "frozen Telegram target status changed concurrently",
            ));
        }
        Ok(())
    }

    pub async fn execute_revoke_effect(
        &self,
        _target_message_ids: &[i64],
        _tombstone: &str,
    ) -> AppResult<RevokeEffectResult> {
        Err(AppError::conflict(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "Telegram revoke cleanup requires a frozen scope",
        ))
    }

    pub async fn execute_revoke_effect_scoped(
        &self,
        scope: &TurnScope,
        target_message_ids: &[i64],
        tombstone: &str,
    ) -> AppResult<RevokeEffectResult> {
        if scope.chat_id != self.chat_id || target_message_ids.is_empty() {
            return Ok(RevokeEffectResult::default());
        }
        let mut effect = RevokeEffectResult::default();
        let first_id = target_message_ids[0];
        match self.edit_message(first_id, tombstone).await {
            Ok(()) => {
                self.mark_status(scope, first_id, "revoked").await?;
                effect.edited_tombstone_message_id = Some(first_id);
            }
            Err(_) => effect.failed_message_ids.push(first_id),
        }
        for extra_id in &target_message_ids[1..] {
            match self.delete_message_in_chat(scope.chat_id, *extra_id).await {
                Ok(()) => {
                    self.mark_status(scope, *extra_id, "deleted").await?;
                    effect.deleted_extra_message_ids.push(*extra_id);
                }
                Err(err) if is_not_found_error(&err) => {
                    self.mark_status(scope, *extra_id, "deleted").await?;
                    effect.deleted_extra_message_ids.push(*extra_id);
                }
                Err(_) => effect.failed_message_ids.push(*extra_id),
            }
        }
        Ok(effect)
    }

    async fn wait_for_cooldown(&self) {
        self.rate_limiter.wait(&self.bot_id, self.chat_id).await;
    }

    async fn set_cooldown(&self, retry_after_secs: u64) {
        self.rate_limiter
            .set_cooldown_secs(&self.bot_id, self.chat_id, retry_after_secs)
            .await;
    }

    async fn call(&self, method: &str, body: Value) -> AppResult<Value> {
        self.wait_for_cooldown().await;
        let url = format!("{}/bot{}/{method}", self.api_base, self.token);
        let response = self
            .client
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(|_| {
                AppError::bad_gateway("TELEGRAM_REQUEST_FAILED", "Telegram request failed")
            })?;
        let status = response.status();
        let payload: Value = response
            .json()
            .await
            .map_err(|err| AppError::bad_gateway("TELEGRAM_RESPONSE_INVALID", err.to_string()))?;
        if status.as_u16() == 429 || payload.get("error_code").and_then(Value::as_i64) == Some(429)
        {
            let retry_after = payload
                .pointer("/parameters/retry_after")
                .and_then(Value::as_u64)
                .unwrap_or(1);
            self.set_cooldown(retry_after).await;
            let description = safe_telegram_description(
                payload.get("description").and_then(Value::as_str),
                "Telegram API rate limited",
            );
            return Err(AppError::bad_gateway(
                "TELEGRAM_RATE_LIMITED",
                format!("{description}; retry_after={retry_after}"),
            ));
        }
        if !status.is_success() || payload.get("ok").and_then(Value::as_bool) != Some(true) {
            let description = safe_telegram_description(
                payload.get("description").and_then(Value::as_str),
                "Telegram API request failed",
            );
            return Err(AppError::bad_gateway("TELEGRAM_API_ERROR", description));
        }
        Ok(payload.get("result").cloned().unwrap_or(Value::Null))
    }
}

fn safe_telegram_description(value: Option<&str>, fallback: &str) -> String {
    value
        .and_then(crate::modules::bridge::redaction::redact_detail)
        .unwrap_or_else(|| fallback.to_string())
}

fn is_not_found_error(error: &AppError) -> bool {
    let message = error.message.to_ascii_lowercase();
    message.contains("not found")
        || message.contains("message to delete not found")
        || message.contains("message can't be deleted")
}

fn panel_markup(keyboard: &[Vec<InlineButton>]) -> Value {
    json!({
        "inline_keyboard": keyboard
            .iter()
            .map(|row| {
                row.iter()
                    .map(|button| {
                        json!({"text": button.label.as_str(), "callback_data": button.callback_data.as_str()})
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
    })
}

fn parse_mode_name(mode: PanelParseMode) -> Option<&'static str> {
    match mode {
        PanelParseMode::PlainText => None,
        PanelParseMode::MarkdownV2 => Some("MarkdownV2"),
    }
}

struct ProgressState {
    stream: StreamState,
    accumulated_text: String,
    finalized: bool,
}

pub struct TelegramProgressSink {
    delivery: TelegramDelivery,
    delivery_id: Option<String>,
    message_id: Option<i64>,
    metadata: TurnMetadata,
    render_version: Arc<Mutex<u64>>,
    state: Arc<Mutex<ProgressState>>,
}

impl TelegramProgressSink {
    pub async fn finalize(&self, text: &str) -> AppResult<()> {
        let should_finalize = {
            let mut state = self.state.lock().await;
            if state.finalized {
                false
            } else {
                state.finalized = true;
                state.stream.force_final(text, Instant::now());
                true
            }
        };
        if !should_finalize || text.trim().is_empty() {
            return Ok(());
        }
        let chunks = chunk_text(text, self.delivery.policy.chunk_size);
        let Some(first) = chunks.first() else {
            return Ok(());
        };
        let message_id = self.message_id.ok_or_else(|| {
            AppError::conflict(
                "TELEGRAM_DELIVERY_STATE_UNKNOWN",
                "stream finalization has no placeholder message",
            )
        })?;
        if let Err(error) = self.delivery.edit_message(message_id, first).await {
            if let Some(delivery_id) = self.delivery_id.as_deref() {
                let unknown = self
                    .delivery
                    .mark_unknown_after_persistence_failure(delivery_id, &error)
                    .await;
                return Err(unknown);
            }
            return Err(error);
        }
        let mut revision = self.render_version.lock().await;
        let expected_revision = *revision;
        if let Err(error) = self
            .delivery
            .update_turn_chunk_scoped(
                &self.metadata.scope,
                expected_revision,
                &self.metadata.turn_id,
                0,
                message_id,
                first,
            )
            .await
        {
            if let Some(delivery_id) = self.delivery_id.as_deref() {
                return Err(self
                    .delivery
                    .mark_unknown_after_persistence_failure(delivery_id, &error)
                    .await);
            }
            return Err(error);
        }
        *revision = expected_revision.checked_add(1).ok_or_else(|| {
            AppError::conflict(
                "TELEGRAM_TURN_REVISION_INVALID",
                "Telegram turn revision overflow",
            )
        })?;
        drop(revision);
        for (index, chunk) in chunks.into_iter().enumerate().skip(1) {
            tokio::time::sleep(self.delivery.inter_message_delay).await;
            let expected_revision = *self.render_version.lock().await;
            self.delivery
                .send_turn_chunk_scoped(&self.metadata, expected_revision, index, &chunk)
                .await?;
            let mut revision = self.render_version.lock().await;
            *revision = expected_revision.checked_add(1).ok_or_else(|| {
                AppError::conflict(
                    "TELEGRAM_TURN_REVISION_INVALID",
                    "Telegram turn revision overflow",
                )
            })?;
        }
        Ok(())
    }

    pub async fn fail(&self, message: &str) -> AppResult<()> {
        self.finalize(&format!("生成失败：{message}")).await
    }
}

#[async_trait]
impl ProgressSink for TelegramProgressSink {
    async fn emit(&self, event: ProgressEvent) -> AppResult<()> {
        match event {
            ProgressEvent::Delta { full_text, .. } => {
                let should_render = {
                    let mut state = self.state.lock().await;
                    state
                        .stream
                        .should_render(&full_text, &self.delivery.policy, Instant::now())
                };
                if should_render {
                    if let Some(message_id) = self.message_id {
                        let first = chunk_text(&full_text, self.delivery.policy.chunk_size)
                            .into_iter()
                            .next()
                            .unwrap_or_default();
                        if !first.is_empty() {
                            if let Err(err) = self.delivery.edit_message(message_id, &first).await {
                                tracing::warn!(code = %err.code, "telegram progress edit failed");
                            }
                        }
                    }
                }
            }
            ProgressEvent::Done { reply_text, .. } => {
                self.finalize(&reply_text).await?;
            }
            ProgressEvent::Error { message } => {
                self.fail(&message).await?;
            }
            ProgressEvent::Progress { completed, total } => {
                if let Some(message_id) = self.message_id {
                    let text = format!("正在压缩… {completed}/{total}");
                    if let Err(err) = self.delivery.edit_message(message_id, &text).await {
                        tracing::warn!(code = %err.code, "telegram compression progress edit failed");
                    }
                }
            }
            ProgressEvent::Started { operation_id, .. } => {
                if let Some(delivery_id) = self.delivery_id.as_deref() {
                    self.delivery
                        .link_generation(delivery_id, &operation_id)
                        .await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl crate::seams::bridge_progress::BridgeProgressSink for TelegramProgressSink {
    async fn emit(
        &self,
        event: crate::seams::bridge_progress::BridgeProgressEvent,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match event {
            crate::seams::bridge_progress::BridgeProgressEvent::Delta { full_text, .. } => {
                let (should_render, full_text) = {
                    let mut state = self.state.lock().await;
                    state.accumulated_text = full_text.clone();
                    let should = state.stream.should_render(
                        &full_text,
                        &self.delivery.policy,
                        Instant::now(),
                    );
                    (should, full_text)
                };
                if should_render {
                    if let Some(message_id) = self.message_id {
                        let first = chunk_text(&full_text, self.delivery.policy.chunk_size)
                            .into_iter()
                            .next()
                            .unwrap_or_default();
                        if !first.is_empty() {
                            if let Err(err) = self.delivery.edit_message(message_id, &first).await {
                                tracing::warn!(code = %err.code, "telegram bridge progress edit failed");
                            }
                        }
                    }
                }
            }
            crate::seams::bridge_progress::BridgeProgressEvent::Done { reply_text, .. } => {
                self.finalize(&reply_text)
                    .await
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
            }
            crate::seams::bridge_progress::BridgeProgressEvent::Error { safe_message, .. } => {
                self.fail(&safe_message)
                    .await
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
            }
            crate::seams::bridge_progress::BridgeProgressEvent::Progress { completed, total } => {
                if let Some(message_id) = self.message_id {
                    let text = format!("正在处理… {completed}/{total}");
                    if let Err(error) = self.delivery.edit_message(message_id, &text).await {
                        tracing::warn!(code = %error.code, "telegram progress edit failed");
                    }
                }
            }
            crate::seams::bridge_progress::BridgeProgressEvent::Started { .. } => {}
        }
        Ok(())
    }
}
