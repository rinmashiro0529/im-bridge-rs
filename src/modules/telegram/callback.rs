use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Digest;
use sqlx::SqlitePool;

use crate::clock::now_rfc3339;
use crate::error::{AppError, AppResult};
use crate::modules::telegram::panel::ModelPurpose;

pub const MAX_CALLBACK_DATA_BYTES: usize = 64;
pub const OPAQUE_CALLBACK_DATA_BYTES: usize = 35;
pub const OPAQUE_CALLBACK_HEX_BYTES: usize = 32;

pub type CallbackToken = CallbackAction;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallbackAction {
    Home,
    Help,
    Settings,
    Status,
    Now,
    Characters {
        page: usize,
    },
    SelectCharacter {
        page: usize,
        index: usize,
    },
    History {
        page: usize,
    },
    SelectHistory {
        page: usize,
        index: usize,
    },
    Recent {
        page: usize,
    },
    SelectRecent {
        page: usize,
        index: usize,
    },
    Providers {
        purpose: ModelPurpose,
        page: usize,
    },
    ProviderModels {
        purpose: ModelPurpose,
        provider_key: String,
        page: usize,
    },
    SelectModel {
        purpose: ModelPurpose,
        provider_key: String,
        model_index: usize,
    },
    SelectModelById {
        purpose: ModelPurpose,
        provider_key: String,
        model_id: String,
    },
    ResetModel {
        purpose: ModelPurpose,
    },
}

impl CallbackAction {
    /// Encode an action for an in-process panel template. Before a panel is sent,
    /// `bind_panel_callbacks` replaces this value with a durable opaque token.
    pub fn to_callback_data(&self) -> String {
        self.legacy_data()
    }

    pub fn serialize(&self) -> AppResult<String> {
        let value = self.to_callback_data();
        if value.len() > 4096 {
            return Err(AppError::bad_request(
                "TELEGRAM_CALLBACK_TOO_LONG",
                "callback template exceeds the internal limit",
            ));
        }
        Ok(value)
    }

    pub fn parse(data: &str) -> Option<Self> {
        parse_callback_data(data).ok()
    }

    fn legacy_data(&self) -> String {
        match self {
            Self::Home => "cb:home".into(),
            Self::Help => "cb:help".into(),
            Self::Settings => "cb:settings".into(),
            Self::Status => "cb:status".into(),
            Self::Now => "cb:now".into(),
            Self::Characters { page } => format!("cb:chars:{page}"),
            Self::SelectCharacter { page, index } => format!("cb:sel_char:{page}:{index}"),
            Self::History { page } => format!("cb:hist:{page}"),
            Self::SelectHistory { page, index } => format!("cb:sel_hist:{page}:{index}"),
            Self::Recent { page } => format!("cb:recent:{page}"),
            Self::SelectRecent { page, index } => format!("cb:sel_recent:{page}:{index}"),
            Self::Providers { purpose, page } => {
                format!("cb:prov:{}:{page}", purpose_token(*purpose))
            }
            Self::ProviderModels {
                purpose,
                provider_key,
                page,
            } => format!(
                "cb:p_mdl:{}:{}:{page}",
                purpose_token(*purpose),
                provider_key,
            ),
            Self::SelectModel {
                purpose,
                provider_key,
                model_index,
            } => format!(
                "cb:sel_mdl:{}:{}:{model_index}",
                purpose_token(*purpose),
                provider_key,
            ),
            Self::SelectModelById {
                purpose,
                provider_key,
                model_id,
            } => format!(
                "cb:sel_mdl_id:{}:{}:{}",
                purpose_token(*purpose),
                provider_key,
                model_id,
            ),
            Self::ResetModel { purpose } => format!("cb:rst_mdl:{}", purpose_token(*purpose)),
        }
    }
}

impl fmt::Display for CallbackAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_callback_data())
    }
}

pub fn serialize_callback(action: &CallbackAction) -> AppResult<String> {
    action.serialize()
}

pub fn parse_callback(data: &str) -> AppResult<CallbackAction> {
    parse_callback_data(data)
}

pub fn parse_callback_data(data: &str) -> AppResult<CallbackAction> {
    parse_legacy_callback_data(data)
}

fn validate_opaque_token(data: &str) -> AppResult<()> {
    if data.len() != OPAQUE_CALLBACK_DATA_BYTES
        || !data.is_ascii()
        || !data.starts_with("cb:")
        || !data[3..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(AppError::bad_request(
            "TELEGRAM_CALLBACK_INVALID",
            "callback token 无效或已过期",
        ));
    }
    Ok(())
}

fn purpose_token(purpose: ModelPurpose) -> &'static str {
    match purpose {
        ModelPurpose::Chat => "chat",
        ModelPurpose::Compression => "comp",
    }
}

fn parse_legacy_callback_data(data: &str) -> AppResult<CallbackAction> {
    if data.len() > 4096 {
        return Err(AppError::bad_request(
            "TELEGRAM_CALLBACK_TOO_LONG",
            "Telegram callback_data 超过 64 字节",
        ));
    }
    let parts: Vec<&str> = data.split(':').collect();
    let parse_usize = |value: Option<&&str>| {
        value
            .and_then(|raw| raw.parse::<usize>().ok())
            .ok_or_else(|| AppError::bad_request("TELEGRAM_CALLBACK_INVALID", "callback 参数无效"))
    };
    let purpose = |value: Option<&&str>| match value.copied() {
        Some("chat") => Ok(ModelPurpose::Chat),
        Some("comp") | Some("compression") => Ok(ModelPurpose::Compression),
        _ => Err(AppError::bad_request(
            "TELEGRAM_CALLBACK_INVALID",
            "callback 模型用途无效",
        )),
    };
    if parts.first().copied() != Some("cb") {
        return Err(AppError::bad_request(
            "TELEGRAM_CALLBACK_INVALID",
            "callback 前缀无效",
        ));
    }
    match parts.get(1).copied() {
        Some("home") if parts.len() == 2 => Ok(CallbackAction::Home),
        Some("help") if parts.len() == 2 => Ok(CallbackAction::Help),
        Some("settings") if parts.len() == 2 => Ok(CallbackAction::Settings),
        Some("status") if parts.len() == 2 => Ok(CallbackAction::Status),
        Some("now") if parts.len() == 2 => Ok(CallbackAction::Now),
        Some("chars") if parts.len() == 3 => Ok(CallbackAction::Characters {
            page: parse_usize(parts.get(2))?,
        }),
        Some("sel_char") if parts.len() == 4 => Ok(CallbackAction::SelectCharacter {
            page: parse_usize(parts.get(2))?,
            index: parse_usize(parts.get(3))?,
        }),
        Some("hist") if parts.len() == 3 => Ok(CallbackAction::History {
            page: parse_usize(parts.get(2))?,
        }),
        Some("sel_hist") if parts.len() == 4 => Ok(CallbackAction::SelectHistory {
            page: parse_usize(parts.get(2))?,
            index: parse_usize(parts.get(3))?,
        }),
        Some("recent") if parts.len() == 3 => Ok(CallbackAction::Recent {
            page: parse_usize(parts.get(2))?,
        }),
        Some("sel_recent") if parts.len() == 4 => Ok(CallbackAction::SelectRecent {
            page: parse_usize(parts.get(2))?,
            index: parse_usize(parts.get(3))?,
        }),
        Some("prov") if parts.len() == 4 => Ok(CallbackAction::Providers {
            purpose: purpose(parts.get(2))?,
            page: parse_usize(parts.get(3))?,
        }),
        Some("p_mdl") if parts.len() == 5 => Ok(CallbackAction::ProviderModels {
            purpose: purpose(parts.get(2))?,
            provider_key: parts[3].to_string(),
            page: parse_usize(parts.get(4))?,
        }),
        Some("sel_mdl") if parts.len() == 5 => Ok(CallbackAction::SelectModel {
            purpose: purpose(parts.get(2))?,
            provider_key: parts[3].to_string(),
            model_index: parse_usize(parts.get(4))?,
        }),
        Some("sel_mdl_id") if parts.len() == 5 => Ok(CallbackAction::SelectModelById {
            purpose: purpose(parts.get(2))?,
            provider_key: parts[3].to_string(),
            model_id: parts[4].to_string(),
        }),
        Some("rst_mdl") if parts.len() == 3 => Ok(CallbackAction::ResetModel {
            purpose: purpose(parts.get(2))?,
        }),
        _ => Err(AppError::bad_request(
            "TELEGRAM_CALLBACK_INVALID",
            "未知或格式错误的 callback",
        )),
    }
}

pub fn opaque_callback(data: &str) -> String {
    parse_legacy_callback_data(data)
        .map(|action| action.to_callback_data())
        .unwrap_or_else(|_| data.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallbackBinding {
    pub account_id: String,
    pub internal_bot_id: String,
    pub numeric_bot_id: i64,
    pub chat_id: i64,
    pub authorized_user_id: String,
    pub panel_id: String,
    pub message_id: i64,
    pub panel_revision: u64,
    pub catalog_revision: Option<String>,
    pub expires_at_unix: i64,
    pub action: CallbackAction,
    pub action_nonce: String,
}

fn token_digest(token: &str) -> String {
    hex::encode(sha2::Sha256::digest(token.as_bytes()))
}

#[derive(Clone)]
pub struct CallbackStore {
    pool: SqlitePool,
}

impl CallbackStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn issue(&self, binding: &CallbackBinding) -> AppResult<String> {
        let mut raw = [0_u8; 16];
        rand::thread_rng().fill_bytes(&mut raw);
        let token = format!("cb:{}", hex::encode(raw));
        validate_opaque_token(&token)?;
        let payload = serde_json::to_string(binding)
            .map_err(|err| AppError::internal(format!("callback binding encode failed: {err}")))?;
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO telegram_panel_effects
                (effect_id, panel_id, chat_id, effect_type, payload, status, action_nonce,
                 created_at, updated_at)
             VALUES (?, ?, ?, 'callback_token', ?, 'active', ?, ?, ?)",
        )
        .bind(token_digest(&token))
        .bind(&binding.panel_id)
        .bind(binding.chat_id)
        .bind(payload)
        .bind(&binding.action_nonce)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(token)
    }

    /// Atomically consume a token only after every scope and revision field
    /// matches the callback update. The token's action is never inferred from
    /// a provider name or a list index supplied by Telegram.
    // Every callback scope field is authenticated before atomic consumption.
    #[allow(clippy::too_many_arguments)]
    pub async fn consume(
        &self,
        token: &str,
        account_id: &str,
        internal_bot_id: &str,
        numeric_bot_id: i64,
        chat_id: i64,
        authorized_user_id: &str,
        panel_id: &str,
        message_id: i64,
        panel_revision: u64,
    ) -> AppResult<(CallbackAction, String)> {
        validate_opaque_token(token)?;
        if numeric_bot_id <= 0 || message_id <= 0 {
            return Err(AppError::bad_request(
                "TELEGRAM_CALLBACK_SCOPE_MISMATCH",
                "callback 已失效，请重新打开面板",
            ));
        }
        let mut tx = self.pool.begin().await?;
        let row: Option<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT payload, status, action_nonce FROM telegram_panel_effects
             WHERE effect_id = ? AND panel_id IS NOT NULL AND chat_id = ?",
        )
        .bind(token_digest(token))
        .bind(chat_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((payload, status, stored_nonce)) = row else {
            return Err(AppError::bad_request(
                "TELEGRAM_CALLBACK_INVALID",
                "callback 已失效",
            ));
        };
        if status != "active" {
            return Err(AppError::conflict(
                "TELEGRAM_CALLBACK_REPLAYED",
                "callback 已处理",
            ));
        }
        let binding: CallbackBinding = serde_json::from_str(&payload)
            .map_err(|_| AppError::internal("callback binding is invalid"))?;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        if binding.action_nonce.trim().is_empty()
            || stored_nonce.as_deref() != Some(binding.action_nonce.as_str())
            || binding.account_id != account_id
            || binding.internal_bot_id != internal_bot_id
            || binding.numeric_bot_id != numeric_bot_id
            || binding.chat_id != chat_id
            || binding.authorized_user_id != authorized_user_id
            || binding.panel_id != panel_id
            || binding.message_id != message_id
            || binding.panel_revision != panel_revision
            || binding.expires_at_unix <= now
        {
            return Err(AppError::bad_request(
                "TELEGRAM_CALLBACK_SCOPE_MISMATCH",
                "callback 已失效，请重新打开面板",
            ));
        }
        let changed = sqlx::query(
            "UPDATE telegram_panel_effects SET status = 'consumed', terminal_at = ?, updated_at = ?
             WHERE effect_id = ? AND status = 'active' AND payload = ? AND action_nonce = ?",
        )
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .bind(token_digest(token))
        .bind(&payload)
        .bind(&binding.action_nonce)
        .execute(&mut *tx)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_CALLBACK_REPLAYED",
                "callback 已处理",
            ));
        }
        let effect_id = crate::ids::new_id();
        let action_payload = serde_json::to_string(&binding.action)
            .map_err(|_| AppError::internal("callback action serialization failed"))?;
        let queued = sqlx::query(
            "INSERT INTO telegram_panel_effects
                (effect_id, panel_id, chat_id, effect_type, payload, status, attempt_token, created_at, updated_at)
             VALUES (?, ?, ?, 'callback_action', ?, 'pending', ?, ?, ?)",
        )
        .bind(&effect_id)
        .bind(&binding.panel_id)
        .bind(binding.chat_id)
        .bind(action_payload)
        .bind(&effect_id)
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .execute(&mut *tx)
        .await?;
        if queued.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_CALLBACK_ENQUEUE_FAILED",
                "callback action was not durably enqueued",
            ));
        }
        tx.commit().await?;
        Ok((binding.action, effect_id))
    }

    pub async fn claim_action_effect(&self, effect_id: &str) -> AppResult<()> {
        let changed = sqlx::query(
            "UPDATE telegram_panel_effects
             SET status = 'sending', updated_at = ?
             WHERE effect_id = ? AND attempt_token = ? AND effect_type = 'callback_action' AND status = 'pending'",
        )
        .bind(now_rfc3339())
        .bind(effect_id)
        .bind(effect_id)
        .execute(&self.pool)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_CALLBACK_EFFECT_CAS_CONFLICT",
                "callback action effect was already consumed",
            ));
        }
        Ok(())
    }

    pub async fn complete_action_effect(&self, effect_id: &str) -> AppResult<()> {
        self.update_action_effect(effect_id, "sent").await
    }

    pub async fn fail_action_effect(&self, effect_id: &str, unknown: bool) -> AppResult<()> {
        self.update_action_effect(effect_id, if unknown { "unknown" } else { "failed" })
            .await
    }

    async fn update_action_effect(&self, effect_id: &str, status: &str) -> AppResult<()> {
        let changed = sqlx::query(
            "UPDATE telegram_panel_effects
             SET status = ?, terminal_at = ?, updated_at = ?
             WHERE effect_id = ? AND attempt_token = ? AND effect_type = 'callback_action' AND status = 'sending'",
        )
        .bind(status)
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .bind(effect_id)
        .bind(effect_id)
        .execute(&self.pool)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::conflict(
                "TELEGRAM_CALLBACK_EFFECT_CAS_CONFLICT",
                "callback action effect terminal state changed concurrently",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct CallbackDedup {
    entries: Arc<Mutex<HashMap<String, Instant>>>,
    ttl: Duration,
}

impl Default for CallbackDedup {
    fn default() -> Self {
        Self::new(Duration::from_secs(10))
    }
}

impl CallbackDedup {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
            ttl,
        }
    }

    /// Return true exactly once for a query id/action nonce during the TTL.
    pub fn accept(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut entries = self.entries.lock().expect("callback dedup mutex poisoned");
        entries.retain(|_, timestamp| now.duration_since(*timestamp) < self.ttl);
        if entries.contains_key(key) {
            return false;
        }
        entries.insert(key.to_string(), now);
        true
    }

    pub fn accept_with_nonce(&self, query_id: &str, action_nonce: &str) -> bool {
        self.accept(&format!("{query_id}:{action_nonce}"))
    }

    pub fn check_and_record(&self, key: &str) -> bool {
        self.accept(key)
    }

    pub fn is_duplicate(&self, key: &str) -> bool {
        let now = Instant::now();
        let entries = self.entries.lock().expect("callback dedup mutex poisoned");
        entries
            .get(key)
            .is_some_and(|timestamp| now.duration_since(*timestamp) < self.ttl)
    }
}

pub(crate) fn decode_stored_action(value: &str) -> AppResult<CallbackAction> {
    parse_legacy_callback_data(value)
}

impl CallbackAction {
    // Retained for backward-compatible callback serialization during migration.
    #[allow(dead_code)]
    pub(crate) fn stored_data(&self) -> String {
        self.legacy_data()
    }
}

#[allow(dead_code)]
fn _binding_payload_example(binding: &CallbackBinding) -> serde_json::Value {
    json!({"panel_id": binding.panel_id, "action_nonce": binding.action_nonce})
}
