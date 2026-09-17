use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{json, Value};
use sha2::Digest;
use sqlx::SqlitePool;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::clock::now_rfc3339;
use crate::domain::channel::TelegramBot;
use crate::domain::identity::Actor;
use crate::error::{AppError, AppResult};
use crate::ids::new_id;
use crate::modules::bridge::channel_context::ChannelContextStore;
use crate::modules::bridge::engine::SidecarBridgeEngine;
use crate::modules::bridge::errors::{CommitState, StBridgeError, StErrorCode, StErrorStage};
use crate::modules::bridge::poller_ownership::{
    PollerLifecycle, PollerOwner, PollerOwnershipGuard, PollerOwnershipRegistry,
    PollerRuntimeBinding,
};
use crate::modules::identity::IdentityModule;
use crate::seams::secret_vault::SecretVault;
use crate::seams::st_bridge_engine::StBridgeEngine;

pub mod callback;
pub mod delivery;
pub mod delivery_queue;
pub mod dispatch;
pub mod error_render;
pub mod locks;
pub mod panel;
pub mod panel_store;
pub mod stream;

#[cfg(test)]
mod inbox_offset_tests;

#[cfg(test)]
mod inbox_recovery_tests;

const CODE_ALPHABET: &[u8] = b"23456789ABCDEFGHJKMNPQRSTUVWXYZ";
const CODE_LENGTH: usize = 6;

fn telegram_api_base() -> AppResult<String> {
    let value = std::env::var("IMBRIDGE_TELEGRAM_API_BASE")
        .unwrap_or_else(|_| "https://api.telegram.org".into());
    let value = value.trim_end_matches('/').to_string();
    crate::config::validate_service_url("IMBRIDGE_TELEGRAM_API_BASE", &value, true)?;
    let url = reqwest::Url::parse(&value).map_err(|_| {
        AppError::bad_request("CONFIG_INVALID", "IMBRIDGE_TELEGRAM_API_BASE is invalid")
    })?;
    let host = url.host_str().unwrap_or_default();
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if !loopback && !host.eq_ignore_ascii_case("api.telegram.org") {
        return Err(AppError::bad_request(
            "CONFIG_INVALID",
            "custom Telegram API hosts are disabled; use api.telegram.org or a loopback test endpoint",
        ));
    }
    Ok(value)
}

#[derive(Clone)]
pub struct TelegramServices {
    pub identity: IdentityModule,
    pub vault: Arc<dyn SecretVault>,
    pub bridge: Option<SidecarBridgeEngine>,
    pub channel: ChannelContextStore,
    pub ownership_registry: Option<Arc<dyn PollerOwnershipRegistry>>,
}

#[derive(Clone)]
pub struct TelegramModule {
    pool: SqlitePool,
    runtime: Arc<Mutex<HashMap<String, BotRuntime>>>,
    services: Arc<Mutex<Option<TelegramServices>>>,
    delivery_locks: Arc<locks::DeliveryLockManager>,
    panel_store: Arc<panel_store::PanelStore>,
    delivery_queue: Arc<delivery_queue::DeliveryCoordinator>,
    callback_store: Arc<callback::CallbackStore>,
    callback_dedup: Arc<callback::CallbackDedup>,
    ownership_registry: Arc<Mutex<Option<Arc<dyn PollerOwnershipRegistry>>>>,
    active_numeric_bots: Arc<Mutex<HashMap<i64, ActiveNumericBot>>>,
    bind_code_key: Arc<zeroize::Zeroizing<[u8; 32]>>,
}

#[derive(Clone)]
struct ActiveNumericBot {
    bot_id: String,
    runtime_instance_id: String,
    epoch: u64,
    lifecycle: PollerLifecycle,
}

struct BotRuntime {
    cancel: CancellationToken,
    join: Option<tokio::task::JoinHandle<AppResult<()>>>,
    ownership_guard: PollerOwnershipGuard,
    #[allow(dead_code)]
    observed_username: Option<String>,
    #[allow(dead_code)]
    last_error: Option<String>,
    status: String,
    numeric_bot_id: Option<i64>,
}

impl TelegramModule {
    pub fn new(pool: SqlitePool) -> Self {
        use rand::RngCore;
        let mut bind_code_key = [0_u8; 32];
        rand::thread_rng().fill_bytes(&mut bind_code_key);
        Self::new_with_bind_key(pool, bind_code_key)
    }

    pub fn new_with_bind_key(pool: SqlitePool, bind_code_key: [u8; 32]) -> Self {
        let panel_store = Arc::new(panel_store::PanelStore::new(pool.clone()));
        let delivery_queue = Arc::new(delivery_queue::DeliveryCoordinator::default());
        let callback_store = Arc::new(callback::CallbackStore::new(pool.clone()));
        Self {
            pool,
            runtime: Arc::new(Mutex::new(HashMap::new())),
            services: Arc::new(Mutex::new(None)),
            delivery_locks: Arc::new(locks::DeliveryLockManager::new(
                4096,
                std::time::Duration::from_secs(600),
            )),
            panel_store,
            delivery_queue,
            callback_store,
            callback_dedup: Arc::new(callback::CallbackDedup::default()),
            ownership_registry: Arc::new(Mutex::new(None)),
            active_numeric_bots: Arc::new(Mutex::new(HashMap::new())),
            bind_code_key: Arc::new(zeroize::Zeroizing::new(bind_code_key)),
        }
    }

    pub async fn attach_services(&self, services: TelegramServices) {
        if let Some(registry) = services.ownership_registry.clone() {
            *self.ownership_registry.lock().await = Some(registry);
        }
        *self.services.lock().await = Some(services);
    }

    pub async fn attach_ownership_registry(&self, registry: Arc<dyn PollerOwnershipRegistry>) {
        *self.ownership_registry.lock().await = Some(registry.clone());
        if let Some(services) = self.services.lock().await.as_mut() {
            services.ownership_registry = Some(registry);
        }
    }

    pub fn services(&self) -> Option<TelegramServices> {
        self.services
            .try_lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    pub async fn services_async(&self) -> Option<TelegramServices> {
        self.services.lock().await.clone()
    }

    pub fn panel_store(&self) -> &panel_store::PanelStore {
        &self.panel_store
    }

    pub fn callback_store(&self) -> Arc<callback::CallbackStore> {
        self.callback_store.clone()
    }

    pub fn accept_callback(&self, key: &str) -> bool {
        self.callback_dedup.accept(key)
    }

    pub async fn delivery_for(
        &self,
        bot_id: &str,
        chat_id: i64,
    ) -> AppResult<Option<crate::modules::telegram::delivery::TelegramDelivery>> {
        let Some(bot) = self.get_bot(bot_id).await? else {
            return Ok(None);
        };
        let Some(services) = self.services_async().await else {
            return Ok(None);
        };
        let Some(secret_id) = bot.token_secret_id.as_deref() else {
            return Ok(None);
        };
        let token = String::from_utf8(services.vault.get(secret_id).await?)
            .map_err(|_| AppError::internal("bot token is not utf-8"))?;
        let numeric_bot_id = self.numeric_bot_id(bot_id).await.ok_or_else(|| {
            AppError::service_unavailable(
                "TELEGRAM_BOT_SCOPE_UNAVAILABLE",
                "Telegram delivery requires an active numeric bot identity",
            )
        })?;
        let client = telegram_http_client()
            .map_err(|err| AppError::bad_gateway("TELEGRAM_REQUEST_FAILED", err.to_string()))?;
        Ok(Some(
            crate::modules::telegram::delivery::TelegramDelivery::new_with_coordinator(
                client,
                self.pool.clone(),
                token,
                telegram_api_base()?,
                bot_id.to_string(),
                numeric_bot_id,
                chat_id,
                bot.inter_message_delay_ms,
                bot.stream_min_interval_ms,
                bot.stream_min_delta_chars,
                bot.stream_first_render_chars,
                bot.stream_chunk_size,
                self.delivery_queue.clone(),
            ),
        ))
    }

    pub async fn resolve_bound_actor(
        &self,
        bot_id: &str,
        telegram_user_id: &str,
    ) -> AppResult<Option<Actor>> {
        let Some(services) = self.services_async().await else {
            return Ok(None);
        };
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT ei.account_id, tb.workspace_id
             FROM telegram_bindings tb
             JOIN external_identities ei ON ei.id = tb.external_identity_id
             WHERE tb.bot_id = ? AND ei.channel = 'telegram' AND ei.external_user_id = ? AND tb.revoked_at IS NULL
             ORDER BY tb.bound_at DESC LIMIT 1",
        )
        .bind(bot_id)
        .bind(telegram_user_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some((account_id, workspace_id)) = row else {
            return Ok(None);
        };
        let Some(account) = services.identity.get_by_id(&account_id).await? else {
            return Ok(None);
        };
        Ok(Some(
            services
                .identity
                .actor_in_workspace(account, &workspace_id)
                .await?,
        ))
    }

    pub async fn autostart_enabled(&self) -> AppResult<()> {
        let bots = sqlx::query_as::<_, BotRow>(
            "SELECT id, workspace_id, owner_account_id, token_secret_id, desired_enabled, observed_username, last_error,
                    inter_message_delay_ms, stream_min_interval_ms, stream_min_delta_chars, stream_first_render_chars, stream_chunk_size
             FROM telegram_bots WHERE desired_enabled = 1 AND token_secret_id IS NOT NULL",
        )
        .fetch_all(&self.pool)
        .await?;
        for bot in bots {
            let bot = bot.into_bot();
            if let Some(services) = self.services_async().await {
                if let Err(err) = self.start_bot(&bot, services.vault.as_ref()).await {
                    tracing::error!(bot_id = %bot.id, code = %err.code, "telegram autostart failed");
                }
            }
        }
        Ok(())
    }

    pub async fn list_bots(&self, workspace_id: &str) -> AppResult<Vec<TelegramBot>> {
        let rows = sqlx::query_as::<_, BotRow>(
            "SELECT id, workspace_id, owner_account_id, token_secret_id, desired_enabled, observed_username, last_error,
                    inter_message_delay_ms, stream_min_interval_ms, stream_min_delta_chars, stream_first_render_chars, stream_chunk_size
             FROM telegram_bots WHERE workspace_id = ?",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(BotRow::into_bot).collect())
    }

    pub async fn upsert_bot(
        &self,
        actor: &Actor,
        token_secret_id: Option<&str>,
        desired_enabled: bool,
    ) -> AppResult<TelegramBot> {
        let workspace_id = actor.require_workspace()?.to_string();
        let existing = self.list_bots(&workspace_id).await?;
        let now = now_rfc3339();
        if let Some(bot) = existing.into_iter().next() {
            sqlx::query(
                "UPDATE telegram_bots SET token_secret_id = COALESCE(?, token_secret_id), desired_enabled = ?, updated_at = ? WHERE id = ?",
            )
            .bind(token_secret_id)
            .bind(i64::from(desired_enabled))
            .bind(&now)
            .bind(&bot.id)
            .execute(&self.pool)
            .await?;
            return self
                .get_bot(&bot.id)
                .await?
                .ok_or_else(|| AppError::internal("bot missing"));
        }
        let id = new_id();
        sqlx::query(
            "INSERT INTO telegram_bots
                (id, workspace_id, owner_account_id, token_secret_id, desired_enabled, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(&workspace_id)
        .bind(&actor.account.id)
        .bind(token_secret_id)
        .bind(i64::from(desired_enabled))
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        self.get_bot(&id)
            .await?
            .ok_or_else(|| AppError::internal("bot missing after insert"))
    }

    pub async fn get_bot(&self, id: &str) -> AppResult<Option<TelegramBot>> {
        let row = sqlx::query_as::<_, BotRow>(
            "SELECT id, workspace_id, owner_account_id, token_secret_id, desired_enabled, observed_username, last_error,
                    inter_message_delay_ms, stream_min_interval_ms, stream_min_delta_chars, stream_first_render_chars, stream_chunk_size
             FROM telegram_bots WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(BotRow::into_bot))
    }

    pub async fn generate_bind_code(
        &self,
        bot_id: &str,
        account_id: &str,
    ) -> AppResult<BindCodeView> {
        let code = generate_code();
        let hmac = crate::adapters::secrets::encrypted_sqlite::bind_code_hmac(
            &self.bind_code_key[..],
            &code,
        )?;
        let now = now_rfc3339();
        let expires = time::OffsetDateTime::now_utc() + time::Duration::minutes(5);
        let expires_at = expires
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| now.clone());
        sqlx::query("DELETE FROM bind_codes WHERE bot_id = ? AND consumed_at IS NULL")
            .bind(bot_id)
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "INSERT INTO bind_codes (id, bot_id, target_account_id, code_hmac, expires_at, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(new_id())
        .bind(bot_id)
        .bind(account_id)
        .bind(&hmac)
        .bind(&expires_at)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(BindCodeView {
            code,
            expires_at,
            ttl_ms: 5 * 60 * 1000,
        })
    }

    pub async fn redeem_bind_code(
        &self,
        bot_id: &str,
        account_id: &str,
        raw_code: &str,
        telegram_user_id: &str,
    ) -> AppResult<String> {
        if self.is_locked(account_id, telegram_user_id).await? {
            return Ok("rate_limited".into());
        }
        let code = raw_code.trim().to_uppercase();
        let hmac = crate::adapters::secrets::encrypted_sqlite::bind_code_hmac(
            &self.bind_code_key[..],
            &code,
        )?;
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT id, expires_at FROM bind_codes WHERE bot_id = ? AND target_account_id = ? AND consumed_at IS NULL ORDER BY created_at DESC LIMIT 1",
        )
        .bind(bot_id)
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some((id, expires_at)) = row else {
            self.record_bind_failure(account_id, telegram_user_id)
                .await?;
            return Ok("invalid".into());
        };
        let stored: (String,) = sqlx::query_as("SELECT code_hmac FROM bind_codes WHERE id = ?")
            .bind(&id)
            .fetch_one(&self.pool)
            .await?;
        if !crate::adapters::secrets::encrypted_sqlite::hmac_eq(&stored.0, &hmac) {
            self.record_bind_failure(account_id, telegram_user_id)
                .await?;
            return Ok("invalid".into());
        }
        if expires_at <= now_rfc3339() {
            sqlx::query("DELETE FROM bind_codes WHERE id = ?")
                .bind(&id)
                .execute(&self.pool)
                .await?;
            self.record_bind_failure(account_id, telegram_user_id)
                .await?;
            return Ok("expired".into());
        }
        let existing_identity: Option<(String, String)> = sqlx::query_as(
            "SELECT id, account_id FROM external_identities
             WHERE channel = 'telegram' AND external_user_id = ?",
        )
        .bind(telegram_user_id)
        .fetch_optional(&self.pool)
        .await?;
        if matches!(&existing_identity, Some((_, owner)) if owner != account_id) {
            return Ok("identity_conflict".into());
        }
        let bot = self
            .get_bot(bot_id)
            .await?
            .ok_or_else(|| AppError::not_found("BOT_NOT_FOUND", "bot not found"))?;
        let now = now_rfc3339();
        let mut tx = self.pool.begin().await?;
        let consumed = sqlx::query(
            "UPDATE bind_codes SET consumed_at = ? WHERE id = ? AND consumed_at IS NULL",
        )
        .bind(&now)
        .bind(&id)
        .execute(&mut *tx)
        .await?;
        if consumed.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok("invalid".into());
        }
        let identity_id = if let Some((identity_id, _)) = existing_identity {
            sqlx::query("UPDATE external_identities SET verified_at = ? WHERE id = ?")
                .bind(&now)
                .bind(&identity_id)
                .execute(&mut *tx)
                .await?;
            identity_id
        } else {
            let identity_id = new_id();
            sqlx::query(
                "INSERT INTO external_identities
                    (id, account_id, channel, external_user_id, verified_at, created_at)
                 VALUES (?, ?, 'telegram', ?, ?, ?)",
            )
            .bind(&identity_id)
            .bind(account_id)
            .bind(telegram_user_id)
            .bind(&now)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
            identity_id
        };
        let binding_exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM telegram_bindings
             WHERE bot_id = ? AND external_identity_id = ? AND revoked_at IS NULL",
        )
        .bind(bot_id)
        .bind(&identity_id)
        .fetch_one(&mut *tx)
        .await?;
        if binding_exists == 0 {
            sqlx::query(
                "INSERT INTO telegram_bindings
                    (id, bot_id, external_identity_id, workspace_id, bound_at)
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(new_id())
            .bind(bot_id)
            .bind(&identity_id)
            .bind(&bot.workspace_id)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("DELETE FROM bind_attempts WHERE account_id = ? AND telegram_user_id = ?")
            .bind(account_id)
            .bind(telegram_user_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok("ok".into())
    }

    async fn is_locked(&self, account_id: &str, telegram_user_id: &str) -> AppResult<bool> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT locked_until FROM bind_attempts WHERE account_id = ? AND telegram_user_id = ?",
        )
        .bind(account_id)
        .bind(telegram_user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(matches!(row, Some((Some(until),)) if until > now_rfc3339()))
    }

    async fn record_bind_failure(&self, account_id: &str, telegram_user_id: &str) -> AppResult<()> {
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO bind_attempts (account_id, telegram_user_id, failures, window_start, locked_until)
             VALUES (?, ?, 1, ?, NULL)
             ON CONFLICT(account_id, telegram_user_id) DO UPDATE SET failures = failures + 1",
        )
        .bind(account_id)
        .bind(telegram_user_id)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        let failures: i64 = sqlx::query_scalar(
            "SELECT failures FROM bind_attempts WHERE account_id = ? AND telegram_user_id = ?",
        )
        .bind(account_id)
        .bind(telegram_user_id)
        .fetch_one(&self.pool)
        .await?;
        if failures >= 10 {
            let locked_until = (time::OffsetDateTime::now_utc() + time::Duration::minutes(15))
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| now.clone());
            sqlx::query(
                "UPDATE bind_attempts SET locked_until = ? WHERE account_id = ? AND telegram_user_id = ?",
            )
            .bind(locked_until)
            .bind(account_id)
            .bind(telegram_user_id)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    pub async fn ingest_update(
        &self,
        bot_id: &str,
        update_id: i64,
        raw: &serde_json::Value,
    ) -> AppResult<bool> {
        if update_id < 0 {
            return Err(AppError::bad_request(
                "TELEGRAM_UPDATE_ID_INVALID",
                "Telegram update id must be non-negative",
            ));
        }
        let now = now_rfc3339();
        let raw_json = raw.to_string();
        let raw_sha256 = hex::encode(sha2::Sha256::digest(raw_json.as_bytes()));
        let mut tx = self.pool.begin().await?;
        let existing: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT raw_update_json, raw_update_sha256 FROM telegram_updates
             WHERE bot_id = ? AND update_id = ?",
        )
        .bind(bot_id)
        .bind(update_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((existing_json, existing_sha256)) = existing {
            if existing_json != raw_json || existing_sha256.as_deref() != Some(raw_sha256.as_str())
            {
                return Err(AppError::conflict(
                    "TELEGRAM_UPDATE_IDENTITY_REUSED",
                    "Telegram update identity was reused with a different body",
                ));
            }
            tx.commit().await?;
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO telegram_updates
                (bot_id, update_id, raw_update_json, raw_update_sha256, status, received_at, updated_at)
             VALUES (?, ?, ?, ?, 'received', ?, ?)",
        )
        .bind(bot_id)
        .bind(update_id)
        .bind(&raw_json)
        .bind(&raw_sha256)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO telegram_bot_offsets (bot_id, next_offset, updated_at)
             VALUES (?, ?, ?)
             ON CONFLICT(bot_id) DO UPDATE SET next_offset = MAX(telegram_bot_offsets.next_offset, excluded.next_offset), updated_at = excluded.updated_at",
        )
        .bind(bot_id)
        .bind(update_id.checked_add(1).ok_or_else(|| {
            AppError::bad_request("TELEGRAM_UPDATE_ID_INVALID", "Telegram update id overflow")
        })?)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    pub async fn start_bot(&self, bot: &TelegramBot, vault: &dyn SecretVault) -> AppResult<()> {
        let secret_id = bot
            .token_secret_id
            .as_deref()
            .ok_or_else(|| AppError::bad_request("BOT_TOKEN_MISSING", "请先配置 bot token"))?;
        let token = String::from_utf8(vault.get(secret_id).await?)
            .map_err(|_| AppError::internal("bot token is not utf-8"))?;
        if self.runtime.lock().await.contains_key(&bot.id) {
            return Err(AppError::conflict("BOT_ALREADY_RUNNING", "Bot 已在运行"));
        }
        let client = telegram_http_client()
            .map_err(|err| AppError::bad_gateway("TELEGRAM_REQUEST_FAILED", err.to_string()))?;
        let me = telegram_api_request(&client, &telegram_api_base()?, &token, "getMe", json!({}))
            .await?;
        let numeric_bot_id = parse_numeric_bot_id(&me)?;
        let guard = self.claim_numeric_bot(numeric_bot_id, &bot.id).await?;
        if let Err(error) =
            sqlx::query("UPDATE telegram_bots SET desired_enabled = 1, updated_at = ? WHERE id = ?")
                .bind(now_rfc3339())
                .bind(&bot.id)
                .execute(&self.pool)
                .await
        {
            if let Err(cleanup_error) = self.finalize_poller_exit(&bot.id, &guard).await {
                tracing::error!(
                    bot_id = %bot.id,
                    runtime_instance_id = %guard.runtime_instance_id,
                    code = %cleanup_error.code,
                    "telegram poller startup cleanup failed"
                );
            }
            return Err(error.into());
        }
        let cancel = CancellationToken::new();
        let child = cancel.child_token();
        let bot_id = bot.id.clone();
        let pool = self.pool.clone();
        let module = self.clone();
        let task_guard = guard.clone();
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            if start_rx.await.is_ok() {
                poll_telegram_bot(module, pool, bot_id, token, child, Some(task_guard)).await
            } else {
                module.finalize_poller_exit(&bot_id, &task_guard).await
            }
        });
        self.runtime.lock().await.insert(
            bot.id.clone(),
            BotRuntime {
                cancel,
                join: Some(join),
                ownership_guard: guard,
                observed_username: None,
                last_error: None,
                status: "running".into(),
                numeric_bot_id: Some(numeric_bot_id),
            },
        );
        start_tx.send(()).map_err(|_| {
            AppError::service_unavailable(
                "TELEGRAM_POLLER_START_FAILED",
                "telegram poller failed to receive its start signal",
            )
        })?;
        Ok(())
    }

    pub async fn stop_bot(&self, bot_id: &str, disable: bool) -> AppResult<()> {
        let stopped_runtime = {
            let mut runtime = self.runtime.lock().await;
            if let Some(entry) = runtime.get_mut(bot_id) {
                if let Err(error) = entry.ownership_guard.begin_draining() {
                    tracing::error!(
                        bot_id,
                        runtime_instance_id = %entry.ownership_guard.runtime_instance_id,
                        error = %error,
                        "telegram poller begin draining failed during stop"
                    );
                    return Err(AppError::conflict("ST_POLLER_NOT_OWNER", error.to_string()));
                }
                entry.cancel.cancel();
                Some((entry.ownership_guard.clone(), entry.join.take()))
            } else {
                None
            }
        };
        let mut first_error = None;
        if let Some((guard, join)) = stopped_runtime {
            if let Some(active_entry) = self
                .active_numeric_bots
                .lock()
                .await
                .get_mut(&guard.numeric_bot_id)
            {
                if active_entry.runtime_instance_id == guard.runtime_instance_id {
                    active_entry.lifecycle = PollerLifecycle::Draining;
                }
            }
            if let Some(join) = join {
                match join.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::error!(
                            bot_id,
                            runtime_instance_id = %guard.runtime_instance_id,
                            code = %error.code,
                            "telegram poller finalizer failed"
                        );
                        first_error = Some(error);
                    }
                    Err(error) => {
                        tracing::error!(
                            bot_id,
                            runtime_instance_id = %guard.runtime_instance_id,
                            error = %error,
                            "telegram poller join failed"
                        );
                        first_error = Some(AppError::internal(format!(
                            "telegram poller join failed: {error}"
                        )));
                        if let Err(cleanup_error) = self.finalize_poller_exit(bot_id, &guard).await
                        {
                            tracing::error!(
                                bot_id,
                                runtime_instance_id = %guard.runtime_instance_id,
                                code = %cleanup_error.code,
                                "telegram poller fallback cleanup failed"
                            );
                        }
                    }
                }
            }
        }
        if disable {
            if let Err(error) = sqlx::query(
                "UPDATE telegram_bots SET desired_enabled = 0, updated_at = ? WHERE id = ?",
            )
            .bind(now_rfc3339())
            .bind(bot_id)
            .execute(&self.pool)
            .await
            {
                tracing::error!(bot_id, error = %error, "telegram bot disable update failed");
                if first_error.is_none() {
                    first_error = Some(error.into());
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn finalize_poller_exit(
        &self,
        bot_id: &str,
        guard: &PollerOwnershipGuard,
    ) -> AppResult<()> {
        let runtime_instance_id = &guard.runtime_instance_id;
        let mut first_error = None;
        if let Err(error) = guard.begin_draining() {
            tracing::error!(
                bot_id,
                runtime_instance_id = %runtime_instance_id,
                error = %error,
                "telegram poller begin draining failed"
            );
            first_error = Some(AppError::conflict(
                "ST_POLLER_NOT_OWNER",
                format!("poller begin draining failed: {error}"),
            ));
        }
        if let Err(error) = guard.mark_stopped() {
            tracing::error!(
                bot_id,
                runtime_instance_id = %runtime_instance_id,
                error = %error,
                "telegram poller mark stopped failed"
            );
            if first_error.is_none() {
                first_error = Some(AppError::internal(format!(
                    "poller mark stopped failed: {error}"
                )));
            }
        }
        if let Err(error) = guard.release_runtime().await {
            tracing::error!(
                bot_id,
                runtime_instance_id = %runtime_instance_id,
                error = %error,
                "telegram poller runtime release failed"
            );
            if first_error.is_none() {
                first_error = Some(AppError::internal(format!(
                    "poller runtime release failed: {error}"
                )));
            }
        }
        self.release_numeric_bot_instance(guard.numeric_bot_id, bot_id, runtime_instance_id)
            .await;
        let removed = {
            let mut runtime = self.runtime.lock().await;
            if runtime
                .get(bot_id)
                .map(|entry| {
                    entry.ownership_guard.runtime_instance_id.as_str()
                        == runtime_instance_id.as_str()
                })
                .unwrap_or(false)
            {
                runtime.remove(bot_id);
                true
            } else {
                false
            }
        };
        if !removed {
            tracing::debug!(
                bot_id,
                runtime_instance_id = %runtime_instance_id,
                "telegram poller runtime entry already replaced or absent"
            );
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub async fn shutdown_all(&self) -> AppResult<()> {
        let bot_ids: Vec<String> = self.runtime.lock().await.keys().cloned().collect();
        let mut first_error = None;
        for bot_id in bot_ids {
            if let Err(error) = self.stop_bot(&bot_id, false).await {
                tracing::error!(bot_id, code = %error.code, "telegram poller shutdown failed");
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub async fn claim_numeric_bot(
        &self,
        numeric_bot_id: i64,
        bot_id: &str,
    ) -> AppResult<PollerOwnershipGuard> {
        self.claim_numeric_bot_with_epoch(numeric_bot_id, bot_id, None)
            .await
    }

    pub async fn claim_numeric_bot_with_epoch(
        &self,
        numeric_bot_id: i64,
        bot_id: &str,
        expected_epoch: Option<u64>,
    ) -> AppResult<PollerOwnershipGuard> {
        if numeric_bot_id <= 0 {
            return Err(AppError::bad_gateway(
                "TELEGRAM_BOT_ID_INVALID",
                "Telegram bot numeric id is missing or invalid",
            ));
        }
        let runtime_instance_id = uuid::Uuid::new_v4().to_string();
        {
            let mut active = self.active_numeric_bots.lock().await;
            if active.contains_key(&numeric_bot_id) {
                return Err(AppError::conflict(
                    "BOT_NUMERIC_ID_ALREADY_RUNNING",
                    "该 Telegram Bot (numeric ID) 已在运行",
                ));
            }
            active.insert(
                numeric_bot_id,
                ActiveNumericBot {
                    bot_id: bot_id.to_string(),
                    runtime_instance_id: runtime_instance_id.clone(),
                    epoch: expected_epoch.unwrap_or(1),
                    lifecycle: PollerLifecycle::Starting,
                },
            );
        }
        let registry = match self.ownership_registry.lock().await.clone() {
            Some(registry) => registry,
            None => {
                self.release_numeric_bot_instance(numeric_bot_id, bot_id, &runtime_instance_id)
                    .await;
                return Err(AppError::conflict(
                    "ST_POLLER_NOT_OWNER",
                    "poller ownership rejected: poller ownership registry unavailable",
                ));
            }
        };
        let epoch = match expected_epoch {
            Some(epoch) => epoch,
            None => match registry.get_ownership(numeric_bot_id).await {
                Ok(Some(record)) => record.epoch,
                Ok(None) => {
                    self.release_numeric_bot_instance(numeric_bot_id, bot_id, &runtime_instance_id)
                        .await;
                    return Err(AppError::conflict(
                        "ST_POLLER_NOT_OWNER",
                        "poller ownership rejected: poller not registered for numeric bot id",
                    ));
                }
                Err(err) => {
                    self.release_numeric_bot_instance(numeric_bot_id, bot_id, &runtime_instance_id)
                        .await;
                    return Err(AppError::conflict(
                        "ST_POLLER_NOT_OWNER",
                        format!("poller ownership rejected: {err}"),
                    ));
                }
            },
        };
        if let Err(err) = registry
            .assert_can_start(numeric_bot_id, PollerOwner::RustBridge, epoch)
            .await
        {
            self.release_numeric_bot_instance(numeric_bot_id, bot_id, &runtime_instance_id)
                .await;
            return Err(AppError::conflict(
                "ST_POLLER_NOT_OWNER",
                format!("poller ownership rejected: {err}"),
            ));
        }
        let binding =
            PollerRuntimeBinding::new(bot_id.to_string(), runtime_instance_id.clone(), epoch)
                .running();
        if let Err(err) = registry
            .claim_runtime(numeric_bot_id, binding.clone())
            .await
        {
            self.release_numeric_bot_instance(numeric_bot_id, bot_id, &runtime_instance_id)
                .await;
            return Err(AppError::conflict(
                "ST_POLLER_NOT_OWNER",
                format!("poller runtime claim rejected: {err}"),
            ));
        }
        if let Some(active_entry) = self
            .active_numeric_bots
            .lock()
            .await
            .get_mut(&numeric_bot_id)
        {
            active_entry.epoch = epoch;
            active_entry.lifecycle = PollerLifecycle::Running;
        }
        Ok(PollerOwnershipGuard::new_with_binding(
            numeric_bot_id,
            PollerOwner::RustBridge,
            binding,
            registry,
        ))
    }

    pub async fn release_numeric_bot(&self, numeric_bot_id: i64, bot_id: &str) {
        let runtime_instance_id = {
            let active = self.active_numeric_bots.lock().await;
            active
                .get(&numeric_bot_id)
                .filter(|entry| entry.bot_id == bot_id)
                .map(|entry| entry.runtime_instance_id.clone())
        };
        if let Some(runtime_instance_id) = runtime_instance_id {
            self.release_numeric_bot_instance(numeric_bot_id, bot_id, &runtime_instance_id)
                .await;
        }
    }

    async fn release_numeric_bot_instance(
        &self,
        numeric_bot_id: i64,
        bot_id: &str,
        runtime_instance_id: &str,
    ) {
        let mut active = self.active_numeric_bots.lock().await;
        if active
            .get(&numeric_bot_id)
            .map(|entry| entry.bot_id == bot_id && entry.runtime_instance_id == runtime_instance_id)
            .unwrap_or(false)
        {
            active.remove(&numeric_bot_id);
        }
    }

    pub async fn runtime_status(&self, bot_id: &str) -> String {
        self.runtime
            .lock()
            .await
            .get(bot_id)
            .map(|item| item.status.clone())
            .unwrap_or_else(|| "stopped".into())
    }

    pub async fn numeric_bot_id(&self, bot_id: &str) -> Option<i64> {
        {
            let runtime = self.runtime.lock().await;
            if let Some(numeric_bot_id) = runtime
                .get(bot_id)
                .and_then(|runtime| runtime.numeric_bot_id)
            {
                if numeric_bot_id > 0 {
                    return Some(numeric_bot_id);
                }
            }
        }
        let active = self.active_numeric_bots.lock().await;
        for (numeric_bot_id, entry) in active.iter() {
            if *numeric_bot_id > 0
                && entry.bot_id == bot_id
                && entry.lifecycle == PollerLifecycle::Running
            {
                return Some(*numeric_bot_id);
            }
        }
        None
    }

    pub async fn latest_visible_error(
        &self,
        actor_id: &str,
        bot_id: &str,
        chat_id: &str,
    ) -> AppResult<Option<VisibleErrorRow>> {
        let row: Option<VisibleErrorRow> = sqlx::query_as(
            "SELECT code, stage, safe_message, id, commit_state, retryable, request_id, trace_id, safe_detail
             FROM bridge_error_events
             WHERE actor_id = ? AND bot_id = ? AND chat_id = ?
             ORDER BY created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(actor_id)
        .bind(bot_id)
        .bind(chat_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub fn help_text() -> String {
        [
            "可用命令：",
            "/start - 开始使用",
            "/chars - 选择角色",
            "/hist - 查看历史会话",
            "/new - 基于当前角色新建会话",
            "/now - 查看当前会话",
            "/settings - 查看模型设置",
            "/status - 查看连接状态",
            "/last - 查看最后一轮",
            "/redo - 重生成回复",
            "/undo - 在 SillyTavern 撤回最后一轮（Telegram 消息保持保留）",
            "/revoke - 撤回最后一轮并清理 Telegram 消息",
            "/recent - 查看最近使用的会话",
            "/model - 查看并切换当前可用模型",
            "/cmodel - 查看并切换压缩专用模型",
            "/compress - 压缩当前会话历史",
            "/error - 查看最近一次失败摘要",
            "/help - 查看帮助",
        ]
        .join("\n")
    }

    pub fn split_text(text: &str, max_length: usize) -> Vec<String> {
        let max_length = max_length.max(1);
        if text.encode_utf16().count() <= max_length {
            return vec![text.to_string()];
        }
        let mut chunks = Vec::new();
        let mut remaining = text;
        while remaining.encode_utf16().count() > max_length {
            let mut units = 0usize;
            let mut byte_limit = remaining.len();
            for (index, ch) in remaining.char_indices() {
                let next = units + ch.len_utf16();
                if next > max_length {
                    byte_limit = index;
                    break;
                }
                units = next;
            }
            if byte_limit == 0 {
                byte_limit = remaining
                    .char_indices()
                    .nth(1)
                    .map(|(index, _)| index)
                    .unwrap_or(remaining.len());
            }
            let prefix = &remaining[..byte_limit];
            let mut split = prefix.rfind("\n\n").unwrap_or(0);
            if prefix[..split].encode_utf16().count() < max_length / 2 {
                split = prefix.rfind('\n').unwrap_or(byte_limit);
            }
            if prefix[..split].encode_utf16().count() < max_length / 2 {
                split = byte_limit;
            }
            let (chunk, rest) = remaining.split_at(split);
            if !chunk.trim().is_empty() {
                chunks.push(chunk.trim_end().to_string());
            }
            remaining = rest.trim_start();
        }
        if !remaining.is_empty() {
            chunks.push(remaining.to_string());
        }
        chunks
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct VisibleErrorRow {
    pub code: String,
    pub stage: String,
    pub safe_message: String,
    pub id: i64,
    pub commit_state: String,
    pub retryable: i64,
    pub request_id: Option<String>,
    pub trace_id: Option<String>,
    pub safe_detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BindCodeView {
    pub code: String,
    pub expires_at: String,
    pub ttl_ms: i64,
}

#[derive(sqlx::FromRow)]
struct BotRow {
    id: String,
    workspace_id: String,
    owner_account_id: String,
    token_secret_id: Option<String>,
    desired_enabled: i64,
    observed_username: Option<String>,
    last_error: Option<String>,
    inter_message_delay_ms: i64,
    stream_min_interval_ms: i64,
    stream_min_delta_chars: i64,
    stream_first_render_chars: i64,
    stream_chunk_size: i64,
}

impl BotRow {
    fn into_bot(self) -> TelegramBot {
        TelegramBot {
            id: self.id,
            workspace_id: self.workspace_id,
            owner_account_id: self.owner_account_id,
            token_secret_id: self.token_secret_id,
            desired_enabled: self.desired_enabled != 0,
            observed_username: self.observed_username,
            last_error: self.last_error,
            inter_message_delay_ms: self.inter_message_delay_ms,
            stream_min_interval_ms: self.stream_min_interval_ms,
            stream_min_delta_chars: self.stream_min_delta_chars,
            stream_first_render_chars: self.stream_first_render_chars,
            stream_chunk_size: self.stream_chunk_size,
        }
    }
}

fn generate_code() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..CODE_LENGTH)
        .map(|_| CODE_ALPHABET[rng.gen_range(0..CODE_ALPHABET.len())] as char)
        .collect()
}

fn telegram_http_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(40))
        .build()
}

fn parse_numeric_bot_id(me: &Value) -> AppResult<i64> {
    match me.get("id").and_then(Value::as_i64) {
        Some(id) if id > 0 => Ok(id),
        _ => Err(AppError::bad_gateway(
            "TELEGRAM_BOT_ID_INVALID",
            "Telegram bot numeric id is missing or invalid",
        )),
    }
}

async fn poll_telegram_bot(
    module: TelegramModule,
    pool: SqlitePool,
    bot_id: String,
    token: String,
    cancel: CancellationToken,
    ownership_guard: Option<PollerOwnershipGuard>,
) -> AppResult<()> {
    let api_base = telegram_api_base()?;
    poll_telegram_bot_at(
        module,
        pool,
        bot_id,
        token,
        cancel,
        ownership_guard,
        api_base,
    )
    .await
}

async fn poll_telegram_bot_at(
    module: TelegramModule,
    pool: SqlitePool,
    bot_id: String,
    token: String,
    cancel: CancellationToken,
    ownership_guard: Option<PollerOwnershipGuard>,
    api_base: String,
) -> AppResult<()> {
    let client = match telegram_http_client() {
        Ok(client) => client,
        Err(err) => {
            let build_error = AppError::bad_gateway("TELEGRAM_REQUEST_FAILED", err.to_string());
            tracing::error!(error = %err, bot_id, "telegram client build failed");
            if let Some(guard) = ownership_guard.as_ref() {
                if let Err(cleanup_error) = module.finalize_poller_exit(&bot_id, guard).await {
                    tracing::error!(
                        bot_id,
                        runtime_instance_id = %guard.runtime_instance_id,
                        code = %cleanup_error.code,
                        "telegram poller client startup cleanup failed"
                    );
                    return Err(cleanup_error);
                }
            }
            return Err(build_error);
        }
    };
    if let Err(err) =
        configure_telegram_bot(&module, &pool, &client, &api_base, &token, &bot_id).await
    {
        set_poll_error(&module, &pool, &bot_id, &err.message).await;
    }
    let mut offset: i64 =
        sqlx::query_scalar("SELECT next_offset FROM telegram_bot_offsets WHERE bot_id = ?")
            .bind(&bot_id)
            .fetch_optional(&pool)
            .await?
            .unwrap_or(0);
    if let Some(guard) = ownership_guard.as_ref() {
        if let Err(err) = recover_bridge_operations(&module, &bot_id, guard).await {
            tracing::error!(bot_id, code = %err.code, "bridge operation recovery failed");
        }
    }
    if let Err(err) = recover_startup_updates(
        &module,
        &pool,
        &client,
        &token,
        &bot_id,
        ownership_guard.as_ref(),
    )
    .await
    {
        tracing::error!(bot_id, code = %err.code, "telegram inbox recovery failed");
    }
    let mut next_recovery_at = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut next_inbox_recovery_at = tokio::time::Instant::now();
    while !cancel.is_cancelled() {
        if let Some(guard) = ownership_guard.as_ref() {
            if let Err(err) = guard.assert_valid().await {
                tracing::warn!(
                    bot_id,
                    numeric_bot_id = guard.numeric_bot_id,
                    error = %err,
                    "poller ownership lost; cancelling getUpdates"
                );
                break;
            }
            if tokio::time::Instant::now() >= next_recovery_at {
                if let Err(err) = recover_bridge_operations(&module, &bot_id, guard).await {
                    tracing::error!(bot_id, code = %err.code, "bridge operation recovery failed");
                }
                next_recovery_at = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            }
        }
        // Every previous batch is drained before this point. Do not reset
        // processing rows here: a live or uncertain effect must not be stolen.
        if tokio::time::Instant::now() >= next_inbox_recovery_at {
            if let Err(err) = recover_pending_updates(
                &module,
                &pool,
                &client,
                &token,
                &bot_id,
                ownership_guard.as_ref(),
            )
            .await
            {
                tracing::error!(bot_id, code = %err.code, "telegram online inbox recovery failed");
            }
            next_inbox_recovery_at =
                tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        }
        // Ownership can change while recovery awaits storage or delivery work.
        // Revalidate immediately before the next network poll so a stale owner
        // never consumes another Telegram batch after detecting the takeover.
        if let Some(guard) = ownership_guard.as_ref() {
            if let Err(err) = guard.assert_valid().await {
                tracing::warn!(
                    bot_id,
                    numeric_bot_id = guard.numeric_bot_id,
                    error = %err,
                    "poller ownership lost after inbox recovery; cancelling getUpdates"
                );
                break;
            }
        }
        let url = format!("{api_base}/bot{token}/getUpdates");
        let offset_text = offset.to_string();
        let response = tokio::select! {
            _ = cancel.cancelled() => break,
            response = client
                .get(&url)
                .query(&[
                    ("timeout", "25"),
                    ("offset", offset_text.as_str()),
                    ("allowed_updates", "[\"message\",\"callback_query\"]"),
                ])
                .send() => response,
        };
        let Ok(response) = response else {
            set_poll_error(
                &module,
                &pool,
                &bot_id,
                "Telegram getUpdates request failed",
            )
            .await;
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        };
        let status = response.status();
        let Ok(payload) = response.json::<serde_json::Value>().await else {
            set_poll_error(&module, &pool, &bot_id, "Telegram returned invalid JSON").await;
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        };
        if !status.is_success() || payload.get("ok").and_then(Value::as_bool) != Some(true) {
            set_poll_error(&module, &pool, &bot_id, "Telegram getUpdates failed").await;
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        clear_poll_error(&module, &pool, &bot_id).await;
        let updates = payload
            .get("result")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let persisted = match persist_update_batch(&pool, &bot_id, updates, &mut offset).await {
            Ok(persisted) => persisted,
            Err(err) => {
                tracing::error!(bot_id, code = %err.code, "telegram inbox batch commit failed");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };
        let mut grouped: HashMap<String, Vec<(i64, Value)>> = HashMap::new();
        for (update_id, update) in persisted {
            let group = update
                .pointer("/message/chat/id")
                .or_else(|| update.pointer("/callback_query/message/chat/id"))
                .and_then(Value::as_i64)
                .map(|chat_id| format!("chat:{chat_id}"))
                .unwrap_or_else(|| format!("update:{update_id}"));
            grouped.entry(group).or_default().push((update_id, update));
        }
        let mut tasks = tokio::task::JoinSet::new();
        for (_, mut updates) in grouped {
            updates.sort_by_key(|(update_id, _)| *update_id);
            let module = module.clone();
            let pool = pool.clone();
            let client = client.clone();
            let token = token.clone();
            let bot_id = bot_id.clone();
            let ownership_guard = ownership_guard.clone();
            tasks.spawn(async move {
                for (update_id, update) in updates {
                    if let Err(err) = process_and_deliver(
                        &module,
                        &pool,
                        &client,
                        &token,
                        &bot_id,
                        update_id,
                        &update,
                        ownership_guard.as_ref(),
                    )
                    .await
                    {
                        tracing::error!(bot_id, update_id, code = %err.code, "telegram update processing failed");
                        if let Err(persistence_error) = sqlx::query(
                            "UPDATE telegram_updates SET status = 'failed', error_summary = ?, updated_at = ? WHERE bot_id = ? AND update_id = ?",
                        )
                        .bind(err.code)
                        .bind(now_rfc3339())
                        .bind(&bot_id)
                        .bind(update_id)
                        .execute(&pool)
                        .await
                        {
                            tracing::error!(bot_id, update_id, error = %persistence_error, "telegram update failure state persist failed");
                        }
                    }
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            if let Err(err) = result {
                tracing::error!(error = %err, "telegram update task panicked");
            }
        }
    }
    match ownership_guard.as_ref() {
        Some(guard) => {
            let result = module.finalize_poller_exit(&bot_id, guard).await;
            if let Err(error) = &result {
                tracing::error!(
                    bot_id,
                    runtime_instance_id = %guard.runtime_instance_id,
                    code = %error.code,
                    "telegram poller final cleanup failed"
                );
            }
            result
        }
        None => Ok(()),
    }
}

async fn persist_update_batch(
    pool: &SqlitePool,
    bot_id: &str,
    updates: Vec<Value>,
    offset: &mut i64,
) -> AppResult<Vec<(i64, Value)>> {
    let now = now_rfc3339();
    let mut tx = pool.begin().await?;
    let mut persisted = Vec::new();
    let mut candidate_offset = *offset;
    for update in updates {
        let Some(update_id) = update.get("update_id").and_then(Value::as_i64) else {
            continue;
        };
        if update_id < 0 {
            return Err(AppError::bad_request(
                "TELEGRAM_UPDATE_ID_INVALID",
                "Telegram update id must be non-negative",
            ));
        }
        let raw_json = update.to_string();
        let raw_sha256 = hex::encode(sha2::Sha256::digest(raw_json.as_bytes()));
        let existing: Option<(String, Option<String>, String)> = sqlx::query_as(
            "SELECT raw_update_json, raw_update_sha256, status FROM telegram_updates
             WHERE bot_id = ? AND update_id = ?",
        )
        .bind(bot_id)
        .bind(update_id)
        .fetch_optional(&mut *tx)
        .await?;
        match existing {
            Some((existing_json, existing_sha256, status)) => {
                if existing_json != raw_json
                    || existing_sha256.as_deref() != Some(raw_sha256.as_str())
                {
                    return Err(AppError::conflict(
                        "TELEGRAM_UPDATE_IDENTITY_REUSED",
                        "Telegram update identity was reused with a different body",
                    ));
                }
                if matches!(status.as_str(), "received" | "failed") {
                    persisted.push((update_id, update));
                }
            }
            None => {
                sqlx::query(
                    "INSERT INTO telegram_updates
                        (bot_id, update_id, raw_update_json, raw_update_sha256, status, received_at, updated_at)
                     VALUES (?, ?, ?, ?, 'received', ?, ?)",
                )
                .bind(bot_id)
                .bind(update_id)
                .bind(&raw_json)
                .bind(&raw_sha256)
                .bind(&now)
                .bind(&now)
                .execute(&mut *tx)
                .await?;
                persisted.push((update_id, update));
            }
        }
        let next_offset = update_id.checked_add(1).ok_or_else(|| {
            AppError::bad_request("TELEGRAM_UPDATE_ID_INVALID", "Telegram update id overflow")
        })?;
        candidate_offset = candidate_offset.max(next_offset);
    }
    sqlx::query(
        "INSERT INTO telegram_bot_offsets (bot_id, next_offset, updated_at)
         VALUES (?, ?, ?)
         ON CONFLICT(bot_id) DO UPDATE SET
            next_offset = MAX(telegram_bot_offsets.next_offset, excluded.next_offset),
            updated_at = excluded.updated_at",
    )
    .bind(bot_id)
    .bind(candidate_offset)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    // Publish only after both inbox rows and the durable offset have committed.
    *offset = candidate_offset;
    Ok(persisted)
}

async fn telegram_api_request(
    client: &reqwest::Client,
    api_base: &str,
    token: &str,
    method: &str,
    body: Value,
) -> AppResult<Value> {
    let url = format!("{api_base}/bot{token}/{method}");
    let response =
        client.post(url).json(&body).send().await.map_err(|_| {
            AppError::bad_gateway("TELEGRAM_REQUEST_FAILED", "Telegram request failed")
        })?;
    let status = response.status();
    let payload: Value = response.json().await.map_err(|_| {
        AppError::bad_gateway(
            "TELEGRAM_RESPONSE_INVALID",
            "Telegram returned invalid JSON",
        )
    })?;
    if !status.is_success() || payload.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(AppError::bad_gateway(
            "TELEGRAM_API_ERROR",
            "Telegram API request failed",
        ));
    }
    Ok(payload.get("result").cloned().unwrap_or(Value::Null))
}

async fn configure_telegram_bot(
    module: &TelegramModule,
    pool: &SqlitePool,
    client: &reqwest::Client,
    api_base: &str,
    token: &str,
    bot_id: &str,
) -> AppResult<()> {
    let me = telegram_api_request(client, api_base, token, "getMe", json!({})).await?;
    let username = me
        .get("username")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let commands = json!([
        {"command": "bind", "description": "首次使用：绑定账号"},
        {"command": "start", "description": "开始使用"},
        {"command": "chars", "description": "选择角色"},
        {"command": "hist", "description": "查看当前角色历史"},
        {"command": "new", "description": "新建会话"},
        {"command": "now", "description": "查看当前会话"},
        {"command": "settings", "description": "查看模型设置"},
        {"command": "status", "description": "查看连接状态"},
        {"command": "last", "description": "查看最后一轮"},
        {"command": "redo", "description": "重新生成回复"},
        {"command": "undo", "description": "在 SillyTavern 撤回最后一轮"},
        {"command": "revoke", "description": "撤回最后一轮并清理 Telegram 消息"},
        {"command": "recent", "description": "查看最近会话"},
        {"command": "model", "description": "切换对话模型"},
        {"command": "cmodel", "description": "切换压缩模型"},
        {"command": "compress", "description": "压缩当前会话"},
        {"command": "error", "description": "查看最近一次失败摘要"},
        {"command": "help", "description": "查看帮助"}
    ]);
    telegram_api_request(
        client,
        api_base,
        token,
        "setMyCommands",
        json!({"commands": commands}),
    )
    .await?;
    if let Err(error) = telegram_api_request(
        client,
        api_base,
        token,
        "setMyDescription",
        json!({"description": "IM Bridge：在 Telegram 中与角色对话。"}),
    )
    .await
    {
        tracing::warn!(code = %error.code, "telegram description update failed");
    }
    if let Err(error) = telegram_api_request(
        client,
        api_base,
        token,
        "setMyShortDescription",
        json!({"short_description": "Telegram 角色对话桥接服务"}),
    )
    .await
    {
        tracing::warn!(code = %error.code, "telegram short description update failed");
    }
    if let Some(runtime) = module.runtime.lock().await.get_mut(bot_id) {
        runtime.observed_username = username.clone();
    }
    sqlx::query(
        "UPDATE telegram_bots SET observed_username = ?, last_error = NULL, updated_at = ? WHERE id = ?",
    )
    .bind(username)
    .bind(now_rfc3339())
    .bind(bot_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_poll_error(module: &TelegramModule, pool: &SqlitePool, bot_id: &str, message: &str) {
    let message = crate::modules::bridge::redaction::redact_detail(message)
        .unwrap_or_else(|| "Telegram request failed".to_string());
    if let Some(runtime) = module.runtime.lock().await.get_mut(bot_id) {
        runtime.status = "error".into();
        runtime.last_error = Some(message.clone());
    }
    if let Err(error) =
        sqlx::query("UPDATE telegram_bots SET last_error = ?, updated_at = ? WHERE id = ?")
            .bind(&message)
            .bind(now_rfc3339())
            .bind(bot_id)
            .execute(pool)
            .await
    {
        tracing::error!(bot_id, error = %error, "telegram poll error state persist failed");
    }
}

async fn clear_poll_error(module: &TelegramModule, pool: &SqlitePool, bot_id: &str) {
    let should_clear = {
        let mut runtimes = module.runtime.lock().await;
        if let Some(runtime) = runtimes.get_mut(bot_id) {
            let changed = runtime.last_error.is_some() || runtime.status != "running";
            runtime.status = "running".into();
            runtime.last_error = None;
            changed
        } else {
            false
        }
    };
    if should_clear {
        if let Err(error) =
            sqlx::query("UPDATE telegram_bots SET last_error = NULL, updated_at = ? WHERE id = ?")
                .bind(now_rfc3339())
                .bind(bot_id)
                .execute(pool)
                .await
        {
            tracing::error!(bot_id, error = %error, "telegram poll error clear persist failed");
        }
    }
}

async fn recover_bridge_operations(
    module: &TelegramModule,
    bot_id: &str,
    ownership_guard: &PollerOwnershipGuard,
) -> AppResult<usize> {
    let services = module.services_async().await.ok_or_else(|| {
        AppError::service_unavailable(
            "BRIDGE_SERVICES_UNAVAILABLE",
            "bridge services are unavailable for operation recovery",
        )
    })?;
    let bridge = services.bridge.ok_or_else(|| {
        AppError::service_unavailable(
            "ST_WRITE_NOT_READY",
            "ST bridge is unavailable for operation recovery",
        )
    })?;
    let coordinator = bridge.operation_coordinator().ok_or_else(|| {
        AppError::service_unavailable(
            "ST_WRITE_NOT_READY",
            "operation coordinator is unavailable for recovery",
        )
    })?;
    let recovery = crate::modules::bridge::operation_recovery::OperationRecoveryCoordinator::new(
        coordinator.store().clone(),
    );
    recovery
        .recover_for_bot(coordinator, ownership_guard, bot_id)
        .await
        .map(|operations| operations.len())
}

// Run once before spawning delivery tasks. Keep reset failures inside the
// existing recoverable startup boundary; do not exit without poller cleanup.
async fn recover_startup_updates(
    module: &TelegramModule,
    pool: &SqlitePool,
    client: &reqwest::Client,
    token: &str,
    bot_id: &str,
    ownership_guard: Option<&PollerOwnershipGuard>,
) -> AppResult<()> {
    sqlx::query(
        "UPDATE telegram_updates SET status = 'received', updated_at = ? WHERE bot_id = ? AND status = 'processing'",
    )
    .bind(now_rfc3339())
    .bind(bot_id)
    .execute(pool)
    .await?;
    recover_pending_updates(module, pool, client, token, bot_id, ownership_guard).await
}

// Bounded due-work selection; the caller owns the bot and drains live batches.
// last failure time is durable; retries back off by 2s then 4s, with three
// total attempts. Exhausted rows stay visible as failed for manual inspection.
async fn pending_inbox_batch(
    pool: &SqlitePool,
    bot_id: &str,
    now_unix: i64,
) -> AppResult<Vec<(i64, String)>> {
    Ok(sqlx::query_as(
        "SELECT update_id, raw_update_json FROM telegram_updates
         WHERE bot_id = ? AND status IN ('received', 'failed') AND attempt_count < 3
           AND (attempt_count = 0 OR COALESCE(unixepoch(updated_at), 0)
                + CASE WHEN attempt_count <= 1 THEN 2 ELSE 4 END <= ?)
         ORDER BY update_id LIMIT 100",
    )
    .bind(bot_id)
    .bind(now_unix)
    .fetch_all(pool)
    .await?)
}

async fn recover_pending_updates(
    module: &TelegramModule,
    pool: &SqlitePool,
    client: &reqwest::Client,
    token: &str,
    bot_id: &str,
    ownership_guard: Option<&PollerOwnershipGuard>,
) -> AppResult<()> {
    let rows = pending_inbox_batch(
        pool,
        bot_id,
        time::OffsetDateTime::now_utc().unix_timestamp(),
    )
    .await?;
    for (update_id, raw) in rows {
        if let Some(guard) = ownership_guard {
            guard.assert_valid().await.map_err(|_| {
                AppError::conflict(
                    "POLLER_OWNERSHIP_LOST",
                    "inbox recovery ownership is no longer valid",
                )
            })?;
        }
        let update: Value = match serde_json::from_str(&raw) {
            Ok(update) => update,
            Err(_) => {
                // Quarantine one poison row without blocking every later update.
                sqlx::query(
                    "UPDATE telegram_updates SET status = 'failed', attempt_count = 3,
                     error_summary = 'TELEGRAM_INBOX_JSON_INVALID', updated_at = ?
                     WHERE bot_id = ? AND update_id = ? AND status IN ('received', 'failed')",
                )
                .bind(now_rfc3339())
                .bind(bot_id)
                .bind(update_id)
                .execute(pool)
                .await?;
                tracing::warn!(
                    bot_id,
                    update_id,
                    "invalid inbox JSON exhausted; manual review required"
                );
                continue;
            }
        };
        if let Err(err) = process_and_deliver(
            module,
            pool,
            client,
            token,
            bot_id,
            update_id,
            &update,
            ownership_guard,
        )
        .await
        {
            tracing::error!(bot_id, update_id, code = %err.code, "persisted telegram update retry failed");
            if let Err(persistence_error) = sqlx::query(
                "UPDATE telegram_updates SET status = 'failed', error_summary = ?, updated_at = ? WHERE bot_id = ? AND update_id = ?",
            )
            .bind(err.code)
            .bind(now_rfc3339())
            .bind(bot_id)
            .bind(update_id)
            .execute(pool)
            .await
            {
                tracing::error!(bot_id, update_id, error = %persistence_error, "persisted Telegram update failure state persist failed");
            }
        }
    }
    Ok(())
}

async fn mark_delivered_after_delivery(
    module: &TelegramModule,
    operation_id: &str,
) -> AppResult<()> {
    if operation_id.trim().is_empty() {
        return Err(AppError::bad_request(
            "TELEGRAM_OPERATION_ID_REQUIRED",
            "Telegram delivery requires an operation id",
        ));
    }
    let Some(services) = module.services_async().await else {
        return Err(AppError::service_unavailable(
            "BRIDGE_SERVICES_UNAVAILABLE",
            "bridge services are unavailable for delivery confirmation",
        ));
    };
    let Some(bridge) = services.bridge else {
        return Ok(());
    };
    let Some(coordinator) = bridge.operation_coordinator() else {
        return Ok(());
    };
    let Some(record) = coordinator.store().get_operation(operation_id).await? else {
        return Ok(());
    };
    let statuses: Vec<(String,)> = sqlx::query_as(
        "SELECT status FROM channel_deliveries WHERE turn_id = ? ORDER BY created_at ASC, id ASC",
    )
    .bind(operation_id)
    .fetch_all(coordinator.store().pool())
    .await?;
    if statuses.is_empty()
        || statuses
            .iter()
            .any(|(status,)| !matches!(status.as_str(), "sent" | "delivered"))
    {
        return Ok(());
    }
    if record.status == crate::modules::bridge::operations::BridgeOperationStatus::Committed
        && record.commit_state == crate::modules::bridge::operations::OperationCommitState::Applied
    {
        coordinator
            .mark_delivered(operation_id)
            .await
            .map_err(AppError::from_st)?;
    }
    Ok(())
}

fn replacement_slot_for_panel(
    active_slot: Option<&crate::modules::telegram::panel::PanelSlot>,
    callback_message_id: Option<i64>,
) -> Option<crate::modules::telegram::panel::PanelSlot> {
    if callback_message_id.is_some() {
        active_slot.cloned()
    } else {
        None
    }
}

// Update processing keeps the durable inbox, transport, bot, and ownership scope explicit.
#[allow(clippy::too_many_arguments)]
async fn process_and_deliver(
    module: &TelegramModule,
    pool: &SqlitePool,
    client: &reqwest::Client,
    token: &str,
    bot_id: &str,
    update_id: i64,
    update: &serde_json::Value,
    ownership_guard: Option<&PollerOwnershipGuard>,
) -> AppResult<()> {
    let claimed = sqlx::query(
        "UPDATE telegram_updates
         SET status = 'processing', attempt_count = attempt_count + 1, updated_at = ?
         WHERE bot_id = ? AND update_id = ? AND status IN ('received', 'failed')
           AND attempt_count < 3",
    )
    .bind(now_rfc3339())
    .bind(bot_id)
    .bind(update_id)
    .execute(pool)
    .await?;
    if claimed.rows_affected() == 0 {
        return Ok(());
    }
    let chat_id = update
        .pointer("/message/chat/id")
        .or_else(|| update.pointer("/callback_query/message/chat/id"))
        .and_then(Value::as_i64);
    let Some(chat_id) = chat_id else {
        mark_update_processed(pool, bot_id, update_id).await?;
        return Ok(());
    };
    let lock_key = format!("{bot_id}:{chat_id}");
    let _guard = match module.delivery_locks.acquire(lock_key).await {
        Ok(guard) => guard,
        Err(_) => {
            return Err(AppError::service_unavailable(
                "DELIVERY_LOCK_CAPACITY",
                "系统正忙，消息投递已受控排队，请稍后重试",
            ));
        }
    };
    let bot = module
        .get_bot(bot_id)
        .await?
        .ok_or_else(|| AppError::not_found("BOT_NOT_FOUND", "bot not found"))?;
    let numeric_bot_id = module.numeric_bot_id(bot_id).await.ok_or_else(|| {
        AppError::service_unavailable(
            "TELEGRAM_BOT_SCOPE_UNAVAILABLE",
            "Telegram delivery requires an active numeric bot identity",
        )
    })?;
    let delivery = crate::modules::telegram::delivery::TelegramDelivery::new_with_coordinator(
        client.clone(),
        pool.clone(),
        token.to_string(),
        telegram_api_base()?,
        bot_id.to_string(),
        numeric_bot_id,
        chat_id,
        bot.inter_message_delay_ms,
        bot.stream_min_interval_ms,
        bot.stream_min_delta_chars,
        bot.stream_first_render_chars,
        bot.stream_chunk_size,
        module.delivery_queue.clone(),
    );
    delivery.recover_pending_deliveries().await?;
    let replies = match crate::modules::telegram::dispatch::dispatch_update_with_delivery_and_guard(
        module,
        bot_id,
        update,
        Some(&delivery),
        ownership_guard,
    )
    .await
    {
        Ok(replies) => replies,
        Err(err) if !err.status.is_server_error() => {
            let text = if let Some(st_error) = st_error_from_app(&err) {
                if let Err(record_error) =
                    crate::modules::bridge::error_events::ErrorEventStore::new(pool.clone())
                        .record(
                            &st_error,
                            None,
                            Some(bot_id),
                            Some(&chat_id.to_string()),
                            Some(update_id),
                        )
                        .await
                {
                    tracing::error!(code = %record_error.code, "telegram error event persist failed");
                }
                crate::modules::telegram::error_render::render(&st_error)
            } else if err.code == crate::st_readiness::ST_WRITE_NOT_READY_CODE {
                format!(
                    "{}\n错误码：{}",
                    crate::st_readiness::ST_WRITE_NOT_READY_MESSAGE,
                    crate::st_readiness::ST_WRITE_NOT_READY_CODE
                )
            } else {
                format!("Telegram 处理失败\n错误码：{}", err.code)
            };
            delivery.send_reply(&text, None).await?;
            mark_update_processed(pool, bot_id, update_id).await?;
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    let mut first_reply = true;
    let callback_message_id = update
        .pointer("/callback_query/message/message_id")
        .and_then(Value::as_i64);
    let scope_user_id = update
        .pointer("/callback_query/from/id")
        .or_else(|| update.pointer("/message/from/id"))
        .and_then(Value::as_i64)
        .map(|id| id.to_string());
    let scope_actor = if let Some(user_id) = scope_user_id.as_deref() {
        module.resolve_bound_actor(bot_id, user_id).await?
    } else {
        None
    };
    let scope_account_id = scope_actor.as_ref().map(|actor| actor.account.id.clone());
    let active_slot = if let Some(account_id) = scope_account_id.as_deref() {
        if let Some(numeric_bot_id) = module.numeric_bot_id(bot_id).await {
            module
                .panel_store()
                .find_active_slot_for_scope(account_id, numeric_bot_id, chat_id)
                .await?
        } else {
            module
                .panel_store()
                .find_active_slot_for_account_chat(account_id, chat_id)
                .await?
        }
    } else {
        None
    };
    let business_turn = if update
        .pointer("/message/text")
        .and_then(Value::as_str)
        .filter(|text| !text.trim_start().starts_with('/'))
        .is_some()
    {
        let account_id = scope_account_id.as_deref().ok_or_else(|| {
            AppError::forbidden("Telegram business reply has no bound account scope")
        })?;
        let numeric_bot_id = module.numeric_bot_id(bot_id).await.ok_or_else(|| {
            AppError::service_unavailable(
                "TELEGRAM_BOT_SCOPE_UNAVAILABLE",
                "Telegram business reply has no numeric bot scope",
            )
        })?;
        let services = module.services_async().await.ok_or_else(|| {
            AppError::service_unavailable(
                "TELEGRAM_SERVICES_UNAVAILABLE",
                "Telegram business reply services are unavailable",
            )
        })?;
        let context_key = format!("{bot_id}:{chat_id}");
        let context = services.channel.load(account_id, &context_key).await?;
        let handle = context.handle.ok_or_else(|| {
            AppError::conflict(
                "TELEGRAM_TURN_SCOPE_INVALID",
                "Telegram turn handle is missing",
            )
        })?;
        let avatar = context.avatar.ok_or_else(|| {
            AppError::conflict(
                "TELEGRAM_TURN_SCOPE_INVALID",
                "Telegram turn avatar is missing",
            )
        })?;
        let chat_file = context.chat_file.ok_or_else(|| {
            AppError::conflict(
                "TELEGRAM_TURN_SCOPE_INVALID",
                "Telegram turn chat locator is missing",
            )
        })?;
        let locator_hash =
            crate::modules::bridge::st_ops::locator_hash(&handle, &avatar, &chat_file);
        let canonical_id = format!("tg:{bot_id}:{update_id}");
        let scope = crate::modules::telegram::delivery::TurnScope::new(
            account_id,
            bot_id,
            numeric_bot_id,
            chat_id,
            locator_hash,
        )?;
        Some(
            crate::modules::telegram::delivery::TurnMetadata::new_scoped(
                scope,
                canonical_id.clone(),
                canonical_id,
            )?,
        )
    } else {
        None
    };
    for reply in replies {
        if let Some(markup) = reply.markup.as_ref() {
            let keyboard = crate::modules::telegram::panel::keyboard_from_markup(markup)?;
            if let Some(panel) =
                crate::modules::telegram::panel::lookup_panel(&reply.text, &keyboard)
            {
                let slot = replacement_slot_for_panel(active_slot.as_ref(), callback_message_id);
                if callback_message_id.is_some()
                    && slot
                        .as_ref()
                        .map(|slot| Some(slot.message_id) != callback_message_id)
                        .unwrap_or(true)
                {
                    // A stale callback must not create a second message.
                    continue;
                }
                let Some(account_id) = scope_account_id.as_deref() else {
                    continue;
                };
                let Some(user_id) = scope_user_id.as_deref() else {
                    continue;
                };
                if let Some(slot) = slot {
                    let (effect, next_slot) = crate::modules::telegram::panel::plan_effect(
                        Some(&slot),
                        panel,
                        slot.panel_id.clone(),
                        slot.account_id.clone(),
                        slot.numeric_bot_id,
                        slot.chat_id,
                        600,
                    );
                    let crate::modules::telegram::panel::PanelEffect::Replace {
                        expected_revision,
                        panel: desired,
                        slot: effect_slot,
                    } = effect
                    else {
                        continue;
                    };
                    let desired = bind_panel_callbacks(
                        module,
                        &desired,
                        account_id,
                        user_id,
                        bot_id,
                        slot.numeric_bot_id,
                        slot.chat_id,
                        &slot.panel_id,
                        slot.message_id,
                        next_slot.revision,
                    )
                    .await?;
                    let claim_panel = desired.clone();
                    let effect = crate::modules::telegram::panel::PanelEffect::Replace {
                        slot: effect_slot,
                        expected_revision,
                        panel: desired,
                    };
                    let Some(effect_id) = module
                        .panel_store()
                        .claim_panel_revision(&slot, expected_revision, &claim_panel, &next_slot)
                        .await?
                    else {
                        continue;
                    };
                    module
                        .panel_store()
                        .mark_panel_revision_sending(&effect_id, &slot, expected_revision)
                        .await?;
                    match delivery.execute_panel_effect(&effect).await {
                        Ok(_) => {
                            if let Err(error) = module
                                .panel_store()
                                .complete_panel_replace(
                                    &effect_id,
                                    &slot,
                                    expected_revision,
                                    &next_slot,
                                )
                                .await
                            {
                                if let Err(state_error) = module
                                    .panel_store()
                                    .mark_panel_revision_failed(
                                        &effect_id,
                                        &slot,
                                        expected_revision,
                                    )
                                    .await
                                {
                                    return Err(AppError::internal(format!(
                                        "panel effect completion failed with {} and failure CAS failed with {}",
                                        error.code, state_error.code
                                    )));
                                }
                                return Err(error);
                            }
                        }
                        Err(err) => {
                            module
                                .panel_store()
                                .mark_panel_revision_failed(&effect_id, &slot, expected_revision)
                                .await?;
                            return Err(err);
                        }
                    }
                    continue;
                }
                let panel_id = crate::ids::new_id();
                let numeric_bot_id = module.numeric_bot_id(bot_id).await.ok_or_else(|| {
                    AppError::service_unavailable(
                        "TELEGRAM_BOT_SCOPE_UNAVAILABLE",
                        "Telegram panel reply has no numeric bot scope",
                    )
                })?;
                if numeric_bot_id <= 0 {
                    return Err(AppError::bad_request(
                        "TELEGRAM_CALLBACK_SCOPE_MISMATCH",
                        "Telegram Bot numeric ID 不可用",
                    ));
                }
                let mut initial_panel = panel.clone();
                initial_panel.keyboard.clear();
                let create_effect = crate::modules::telegram::panel::PanelEffect::Create {
                    panel_id: panel_id.clone(),
                    chat_id,
                    panel: initial_panel,
                };
                let Some(message_id) = delivery.execute_panel_effect(&create_effect).await? else {
                    return Err(AppError::bad_gateway(
                        "TELEGRAM_RESPONSE_INVALID",
                        "Telegram 创建面板未返回 message_id",
                    ));
                };
                if message_id <= 0 {
                    return Err(AppError::bad_gateway(
                        "TELEGRAM_RESPONSE_INVALID",
                        "Telegram 创建面板返回无效 message_id",
                    ));
                }
                let desired = bind_panel_callbacks(
                    module,
                    &panel,
                    account_id,
                    user_id,
                    bot_id,
                    numeric_bot_id,
                    chat_id,
                    &panel_id,
                    message_id,
                    0,
                )
                .await?;
                let new_slot = crate::modules::telegram::panel::PanelSlot {
                    panel_id: panel_id.clone(),
                    account_id: account_id.to_string(),
                    numeric_bot_id,
                    chat_id,
                    message_id,
                    kind: panel.kind.clone(),
                    revision: 0,
                    active: true,
                    expires_at_unix: time::OffsetDateTime::now_utc().unix_timestamp() + 600,
                };
                let edit_effect = crate::modules::telegram::panel::PanelEffect::Replace {
                    slot: new_slot.clone(),
                    expected_revision: 0,
                    panel: desired,
                };
                delivery.execute_panel_effect(&edit_effect).await?;
                if !account_id.is_empty() {
                    module.panel_store().save_slot(&new_slot).await?;
                    if callback_message_id.is_none() {
                        if let Some(old_slot) = active_slot.as_ref().filter(|slot| {
                            slot.panel_id != new_slot.panel_id
                                || slot.message_id != new_slot.message_id
                        }) {
                            let retire_effect =
                                crate::modules::telegram::panel::PanelEffect::RetireKeyboard {
                                    slot: old_slot.clone(),
                                };
                            if let Err(error) = delivery.execute_panel_effect(&retire_effect).await
                            {
                                tracing::warn!(
                                    panel_id = %old_slot.panel_id,
                                    message_id = old_slot.message_id,
                                    code = %error.code,
                                    "telegram old panel keyboard retirement failed"
                                );
                            }
                        }
                    }
                }
                continue;
            }
        }
        if let Some(generation) = reply.generation {
            if !matches!(
                generation.kind,
                crate::modules::telegram::dispatch::GenerationKind::Send
            ) {
                delivery
                    .send_reply(
                        &format!(
                            "{}\n错误码：{}",
                            crate::st_readiness::ST_WRITE_NOT_READY_MESSAGE,
                            crate::st_readiness::ST_WRITE_NOT_READY_CODE
                        ),
                        None,
                    )
                    .await?;
                first_reply = false;
                continue;
            }
            let turn = business_turn.as_ref().ok_or_else(|| {
                AppError::conflict(
                    "TELEGRAM_TURN_SCOPE_INVALID",
                    "Telegram generation turn metadata is missing",
                )
            })?;
            let Some(actor) = scope_actor.as_ref() else {
                return Err(AppError::forbidden(
                    "Telegram generation has no bound account scope",
                ));
            };
            let mut services = module.services_async().await.ok_or_else(|| {
                AppError::service_unavailable(
                    "TELEGRAM_SERVICES_UNAVAILABLE",
                    "Telegram generation services are unavailable",
                )
            })?;
            if let Some(guard) = ownership_guard {
                if let Some(bridge) = services.bridge.take() {
                    services.bridge =
                        Some(bridge.with_ownership_guard(std::sync::Arc::new(guard.clone())));
                }
            }
            let context = services
                .channel
                .load(&actor.account.id, &generation.context_key)
                .await?;
            let locator = crate::domain::st::StChatLocator {
                handle: context.handle.ok_or_else(|| {
                    AppError::conflict(
                        "TELEGRAM_TURN_SCOPE_INVALID",
                        "Telegram turn handle is missing",
                    )
                })?,
                avatar: context.avatar.ok_or_else(|| {
                    AppError::conflict(
                        "TELEGRAM_TURN_SCOPE_INVALID",
                        "Telegram turn avatar is missing",
                    )
                })?,
                character_name: context.character_name.unwrap_or_default(),
                chat_file: context.chat_file.ok_or_else(|| {
                    AppError::conflict(
                        "TELEGRAM_TURN_SCOPE_INVALID",
                        "Telegram turn chat locator is missing",
                    )
                })?,
            };
            let bridge = services
                .bridge
                .as_ref()
                .map(|engine| engine.with_context_key(&generation.context_key))
                .ok_or_else(|| {
                    AppError::from_st(StBridgeError::new(
                        StErrorCode::StWriteNotReady,
                        StErrorStage::Control,
                        crate::st_readiness::ST_WRITE_NOT_READY_MESSAGE,
                        false,
                        CommitState::NotStarted,
                    ))
                })?;
            let sink = std::sync::Arc::new(
                delivery
                    .stream_sink(turn.clone(), "已收到，正在继续当前会话。")
                    .await?,
            );
            let operation_id = turn.operation_id.clone();
            let result = bridge
                .execute_with_context_and_origin(
                    actor,
                    crate::seams::st_bridge_engine::StBridgeCommand::SendMessage {
                        locator,
                        text: generation.user_text.unwrap_or_default(),
                        client_operation_id: operation_id.clone(),
                        model_override: None,
                    },
                    Some(crate::seams::bridge_progress::BridgeExecutionContext::new(
                        operation_id.clone(),
                        Some(sink.clone()),
                    )),
                    crate::seams::st_bridge_engine::BridgeOperationOrigin {
                        internal_bot_id: bot_id.to_string(),
                        telegram_update_id: update_id,
                        channel_context_key: generation.context_key,
                    },
                )
                .await;
            match result {
                Ok(_) => {
                    mark_delivered_after_delivery(module, &operation_id).await?;
                    let statuses: Vec<(String,)> = sqlx::query_as(
                        "SELECT status FROM channel_deliveries WHERE turn_id = ? ORDER BY created_at ASC, id ASC",
                    )
                    .bind(&operation_id)
                    .fetch_all(pool)
                    .await?;
                    if statuses.is_empty()
                        || statuses
                            .iter()
                            .any(|(status,)| !matches!(status.as_str(), "sent" | "delivered"))
                    {
                        return Err(AppError::conflict(
                            "TELEGRAM_DELIVERY_INCOMPLETE",
                            "Telegram final delivery is not complete",
                        ));
                    }
                    continue;
                }
                Err(st_error) => {
                    if let Err(record_error) =
                        crate::modules::bridge::error_events::ErrorEventStore::new(pool.clone())
                            .record(
                                &st_error,
                                Some(&actor.account.id),
                                Some(bot_id),
                                Some(&chat_id.to_string()),
                                Some(update_id),
                            )
                            .await
                    {
                        tracing::error!(code = %record_error.code, "telegram error event persist failed");
                    }
                    sink.fail(&crate::modules::telegram::error_render::render(&st_error))
                        .await?;
                    mark_update_processed(pool, bot_id, update_id).await?;
                    return Ok(());
                }
            }
        }
        if !first_reply {
            tokio::time::sleep(std::time::Duration::from_millis(
                bot.inter_message_delay_ms.max(0) as u64,
            ))
            .await;
        }
        if let Some(turn) = business_turn.as_ref() {
            delivery
                .send_business_reply(turn, &reply.text, reply.markup)
                .await?;
            mark_delivered_after_delivery(module, &turn.operation_id).await?;
        } else {
            let operation_id = format!("tg:{bot_id}:{update_id}");
            delivery
                .send_operation_reply(&operation_id, &reply.text, reply.markup)
                .await?;
            mark_delivered_after_delivery(module, &operation_id).await?;
        }
        first_reply = false;
    }
    mark_update_processed(pool, bot_id, update_id).await
}

// Callback binding authenticates every actor, bot, chat, panel, and revision field.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn bind_panel_callbacks(
    module: &TelegramModule,
    panel: &crate::modules::telegram::panel::TelegramPanel,
    account_id: &str,
    user_id: &str,
    bot_id: &str,
    numeric_bot_id: i64,
    chat_id: i64,
    panel_id: &str,
    message_id: i64,
    panel_revision: u64,
) -> AppResult<crate::modules::telegram::panel::TelegramPanel> {
    let mut bound = panel.clone();
    for row in &mut bound.keyboard {
        for button in row {
            let Ok(action) =
                crate::modules::telegram::callback::decode_stored_action(&button.callback_data)
            else {
                continue;
            };
            let token = module
                .callback_store()
                .issue(&crate::modules::telegram::callback::CallbackBinding {
                    account_id: account_id.to_string(),
                    internal_bot_id: bot_id.to_string(),
                    numeric_bot_id,
                    chat_id,
                    authorized_user_id: user_id.to_string(),
                    panel_id: panel_id.to_string(),
                    message_id,
                    panel_revision,
                    catalog_revision: bound.catalog_revision.clone(),
                    expires_at_unix: time::OffsetDateTime::now_utc().unix_timestamp() + 600,
                    action,
                    action_nonce: crate::ids::new_id(),
                })
                .await?;
            button.callback_data = token;
        }
    }
    crate::modules::telegram::panel::remember_panel(&bound);
    Ok(bound)
}

fn st_error_from_app(error: &AppError) -> Option<StBridgeError> {
    error.st.as_deref().cloned().or_else(|| {
        StErrorCode::parse(error.code).map(|code| {
            StBridgeError::new(
                code,
                StErrorStage::Control,
                error.message.clone(),
                false,
                CommitState::NotStarted,
            )
        })
    })
}

async fn mark_update_processed(pool: &SqlitePool, bot_id: &str, update_id: i64) -> AppResult<()> {
    sqlx::query(
        "UPDATE telegram_updates SET status = 'processed', updated_at = ? WHERE bot_id = ? AND update_id = ?",
    )
    .bind(now_rfc3339())
    .bind(bot_id)
    .bind(update_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn active_slot() -> crate::modules::telegram::panel::PanelSlot {
        crate::modules::telegram::panel::PanelSlot {
            panel_id: "panel-current".into(),
            account_id: "account-1".into(),
            numeric_bot_id: 42,
            chat_id: 7,
            message_id: 9001,
            kind: crate::modules::telegram::panel::PanelKind::Home,
            revision: 3,
            active: true,
            expires_at_unix: 1,
        }
    }

    #[test]
    fn callback_uses_the_existing_active_slot_for_replacement() {
        let active = active_slot();
        let selected = replacement_slot_for_panel(Some(&active), Some(active.message_id));
        assert_eq!(selected, Some(active));
    }

    #[test]
    fn direct_message_does_not_select_an_existing_slot_for_replacement() {
        let active = active_slot();
        assert!(replacement_slot_for_panel(Some(&active), None).is_none());
    }

    #[test]
    fn callback_without_an_active_slot_cannot_select_replacement() {
        assert!(replacement_slot_for_panel(None, Some(9001)).is_none());
    }

    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex as StdMutex, OnceLock};

    use crate::adapters::secrets::encrypted_sqlite::EncryptedSqliteVault;
    use crate::adapters::sqlite::{connect_pool, migrate};
    use crate::domain::st::{
        StCapabilities, StChatLocator, StChatSnapshot, StCommitResult, StCommitStatus,
        StGenerationRequest, StGenerationResult, StGenerationSettings, StModelCatalog, StStatus,
        StWriteMode,
    };
    use crate::modules::bridge::channel_context::ChannelContextStore;
    use crate::modules::bridge::engine::SidecarBridgeEngine;
    use crate::modules::bridge::errors::{StErrorCode, StErrorStage, StResult};
    use crate::modules::bridge::operation_coordinator::OperationCoordinator;
    use crate::modules::bridge::operation_payload::OperationPayloadKeyProvider;
    use crate::modules::bridge::operation_store::OperationStore;
    use crate::modules::bridge::poller_ownership::{PollerOwner, SharedMemoryPollerRegistry};
    use crate::seams::secret_vault::SecretVault;
    use crate::seams::st_backend::StBackend;
    use crate::seams::st_operation_journal::MemoryStOperationJournal;
    use async_trait::async_trait;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[derive(Default)]
    struct GenerationTestBackend {
        probe: StdMutex<VecDeque<StResult<StStatus>>>,
        snapshots: StdMutex<VecDeque<StResult<StChatSnapshot>>>,
        settings: StdMutex<VecDeque<StResult<StGenerationSettings>>>,
        generations: StdMutex<VecDeque<StResult<StGenerationResult>>>,
        commits: StdMutex<VecDeque<StResult<StCommitResult>>>,
        backend_calls: AtomicUsize,
        generation_calls: AtomicUsize,
    }

    impl GenerationTestBackend {
        fn push_probe(&self, result: StResult<StStatus>) {
            self.probe.lock().unwrap().push_back(result);
        }

        fn push_snapshot(&self, result: StResult<StChatSnapshot>) {
            self.snapshots.lock().unwrap().push_back(result);
        }

        fn push_settings(&self, result: StResult<StGenerationSettings>) {
            self.settings.lock().unwrap().push_back(result);
        }

        fn push_generation(&self, result: StResult<StGenerationResult>) {
            self.generations.lock().unwrap().push_back(result);
        }

        fn push_commit(&self, result: StResult<StCommitResult>) {
            self.commits.lock().unwrap().push_back(result);
        }

        fn generation_calls(&self) -> usize {
            self.generation_calls.load(Ordering::SeqCst)
        }

        fn backend_calls(&self) -> usize {
            self.backend_calls.load(Ordering::SeqCst)
        }

        fn record_call(&self) {
            self.backend_calls.fetch_add(1, Ordering::SeqCst);
        }

        fn missing<T>(name: &'static str) -> StResult<T> {
            Err(crate::modules::bridge::errors::StBridgeError::boxed(
                StErrorCode::StTestScopeRequired,
                StErrorStage::Control,
                format!("missing generation test fixture: {name}"),
                false,
                crate::modules::bridge::errors::CommitState::NotStarted,
            ))
        }
    }

    #[async_trait]
    impl StBackend for GenerationTestBackend {
        async fn probe(&self) -> StResult<StStatus> {
            self.record_call();
            self.probe
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Self::missing("probe"))
        }

        async fn list_characters(&self) -> StResult<Vec<crate::domain::st::StCharacterSummary>> {
            self.record_call();
            Ok(vec![crate::domain::st::StCharacterSummary {
                avatar: "GenerationTest.png".into(),
                name: "GenerationTest".into(),
                ..Default::default()
            }])
        }

        async fn list_chats(
            &self,
            _avatar: &str,
        ) -> StResult<Vec<crate::domain::st::StChatSummary>> {
            self.record_call();
            Ok(Vec::new())
        }

        async fn snapshot(&self, _locator: &StChatLocator) -> StResult<StChatSnapshot> {
            self.record_call();
            self.snapshots
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Self::missing("snapshot"))
        }

        async fn list_models(&self) -> StResult<StModelCatalog> {
            self.record_call();
            Ok(StModelCatalog {
                models: Vec::new(),
                current_model: None,
            })
        }

        async fn generation_settings(&self) -> StResult<StGenerationSettings> {
            self.record_call();
            self.settings
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Self::missing("generation_settings"))
        }

        async fn stream_generate(
            &self,
            _request: StGenerationRequest,
            _progress: Option<Arc<dyn crate::seams::bridge_progress::BridgeProgressSink>>,
            _cancel: CancellationToken,
        ) -> StResult<StGenerationResult> {
            self.record_call();
            self.generation_calls.fetch_add(1, Ordering::SeqCst);
            self.generations
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Self::missing("stream_generate"))
        }

        async fn create_chat(
            &self,
            _command: crate::domain::st::CreateStChat,
        ) -> StResult<StCommitResult> {
            self.record_call();
            Self::missing("create_chat")
        }

        async fn commit(
            &self,
            _command: crate::domain::st::CommitStChat,
        ) -> StResult<StCommitResult> {
            self.record_call();
            self.commits
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Self::missing("commit"))
        }
    }

    struct GenerationHarness {
        _dir: TempDir,
        pool: sqlx::SqlitePool,
        tg: TelegramModule,
        bot_id: String,
        update_id: i64,
        update: Value,
        backend: Arc<GenerationTestBackend>,
        server: MockServer,
    }

    static TELEGRAM_TEST_ENV_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

    fn telegram_test_env_lock() -> &'static tokio::sync::Mutex<()> {
        TELEGRAM_TEST_ENV_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    fn generation_status() -> StStatus {
        StStatus {
            available: true,
            version: Some("1.16.0-test".into()),
            handle: "default-user".into(),
            capabilities: StCapabilities {
                mode: StWriteMode::TestWrite,
                snapshot: true,
                typed_mutations: true,
                integrity_rotation: true,
                operation_replay: true,
            },
        }
    }

    fn generation_locator() -> StChatLocator {
        StChatLocator {
            handle: "default-user".into(),
            avatar: "GenerationTest.png".into(),
            character_name: "GenerationTest".into(),
            chat_file: "IMBridge-Test-GenerationTest.jsonl".into(),
        }
    }

    fn generation_snapshot() -> StChatSnapshot {
        StChatSnapshot {
            locator: generation_locator(),
            parsed_chat: vec![
                serde_json::json!({"chat_metadata": {"integrity": "generation-int-1"}}),
                serde_json::json!({"is_user": true, "name": "User", "mes": "hello"}),
            ],
            source_sha256: "1".repeat(64),
            source_integrity: "generation-int-1".into(),
            source_byte_length: 100,
            source_message_count: 2,
        }
    }

    fn generation_settings() -> StGenerationSettings {
        StGenerationSettings {
            username: "User".into(),
            chat_completion_source: "test".into(),
            model: "generation-test-model".into(),
            custom_url: String::new(),
            custom_prompt_post_processing: String::new(),
            temperature: 0.7,
            top_p: 1.0,
            max_tokens: 128,
        }
    }

    fn generation_result(text: &str) -> StGenerationResult {
        StGenerationResult {
            text: text.into(),
            finish_reason: Some("stop".into()),
            usage: None,
        }
    }

    fn generation_commit() -> StCommitResult {
        StCommitResult {
            status: StCommitStatus::Applied,
            new_sha256: Some("2".repeat(64)),
            new_integrity: Some("generation-int-2".into()),
            byte_length: Some(160),
            message_count: Some(3),
        }
    }

    async fn setup_generation_harness(
        generation: StResult<StGenerationResult>,
        commit: StResult<StCommitResult>,
        send_status: u16,
        edit_status: u16,
        update_id: i64,
    ) -> GenerationHarness {
        let dir = tempfile::tempdir().unwrap();
        let pool = connect_pool(&dir.path().join("generation-test.db"))
            .await
            .unwrap();
        migrate(&pool).await.unwrap();
        let identity = crate::modules::identity::IdentityModule::new(pool.clone());
        let account = identity
            .bootstrap_admin("generation-test", "generation-test-pass", "Generation Test")
            .await
            .unwrap();
        let workspace_id = identity.default_workspace_id(&account.id).await.unwrap();
        let actor = identity
            .actor_in_workspace(account, &workspace_id)
            .await
            .unwrap();
        let tg = TelegramModule::new(pool.clone());
        let backend = Arc::new(GenerationTestBackend::default());
        for _ in 0..3 {
            backend.push_probe(Ok(generation_status()));
        }
        for _ in 0..3 {
            backend.push_snapshot(Ok(generation_snapshot()));
        }
        backend.push_settings(Ok(generation_settings()));
        backend.push_generation(generation);
        backend.push_commit(commit);

        let channel = ChannelContextStore::new(pool.clone());
        let registry = Arc::new(SharedMemoryPollerRegistry::new());
        registry.bootstrap_legacy(1).await.unwrap();
        registry
            .transfer(1, PollerOwner::LegacyPlugin, 1, PollerOwner::RustBridge)
            .await
            .unwrap();
        tg.attach_ownership_registry(registry.clone()).await;
        let bot = tg.upsert_bot(&actor, None, false).await.unwrap();
        let guard = tg.claim_numeric_bot(1, &bot.id).await.unwrap();
        let context_key = format!("{}:9", bot.id);
        let store = Arc::new(OperationStore::new(pool.clone()));
        let vault: Arc<dyn SecretVault> =
            Arc::new(EncryptedSqliteVault::new(pool.clone(), [9_u8; 32]));
        let provider = Arc::new(OperationPayloadKeyProvider::new(
            vault.clone(),
            store.clone(),
        ));
        let coordinator = Arc::new(OperationCoordinator::new_with_key_provider(
            backend.clone(),
            store,
            provider,
            Arc::new(MemoryStOperationJournal::new()),
        ));
        let engine = SidecarBridgeEngine::new(backend.clone(), channel.clone())
            .with_context_key(&context_key)
            .with_required_coordinator(coordinator)
            .with_ownership_guard(Arc::new(guard));
        tg.attach_services(TelegramServices {
            identity: identity.clone(),
            vault,
            bridge: Some(engine),
            channel: channel.clone(),
            ownership_registry: Some(registry),
        })
        .await;
        let code = tg
            .generate_bind_code(&bot.id, &actor.account.id)
            .await
            .unwrap();
        dispatch::dispatch_update_with_delivery(
            &tg,
            &bot.id,
            &serde_json::json!({"message": {"chat": {"id": 9}, "from": {"id": 4242}, "text": format!("/bind {}", code.code)}}),
            None,
        )
        .await
        .unwrap();
        assert!(tg
            .resolve_bound_actor(&bot.id, "4242")
            .await
            .unwrap()
            .is_some());
        channel
            .select_chat(
                &actor.account.id,
                &workspace_id,
                &context_key,
                &generation_locator(),
            )
            .await
            .unwrap();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/bottest-token/sendMessage"))
            .respond_with(ResponseTemplate::new(send_status).set_body_json(
                serde_json::json!({"ok": send_status < 300, "result": {"message_id": 7001}}),
            ))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/bottest-token/editMessageText"))
            .respond_with(
                ResponseTemplate::new(edit_status)
                    .set_body_json(serde_json::json!({"ok": edit_status < 300, "result": true})),
            )
            .mount(&server)
            .await;
        let update = serde_json::json!({
            "update_id": update_id,
            "message": {
                "chat": {"id": 9},
                "from": {"id": 4242},
                "text": "hello from generation test"
            }
        });
        tg.ingest_update(&bot.id, update_id, &update).await.unwrap();
        GenerationHarness {
            _dir: dir,
            pool,
            tg,
            bot_id: bot.id,
            update_id,
            update,
            backend,
            server,
        }
    }

    async fn run_generation_process(harness: &GenerationHarness) -> AppResult<()> {
        process_and_deliver(
            &harness.tg,
            &harness.pool,
            &reqwest::Client::new(),
            "test-token",
            &harness.bot_id,
            harness.update_id,
            &harness.update,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn ordinary_dispatch_returns_generation_intent_without_engine_call() {
        let harness = setup_generation_harness(
            Ok(generation_result("unused")),
            Ok(generation_commit()),
            200,
            200,
            7301,
        )
        .await;
        let replies = dispatch::dispatch_update_with_delivery(
            &harness.tg,
            &harness.bot_id,
            &harness.update,
            None,
        )
        .await
        .unwrap();
        assert_eq!(harness.backend.generation_calls(), 0);
        assert_eq!(harness.backend.backend_calls(), 0);
        assert_eq!(replies.len(), 1);
        let intent = replies[0].generation.as_ref().expect("send intent");
        assert_eq!(intent.kind, dispatch::GenerationKind::Send);
        assert_eq!(
            intent.user_text.as_deref(),
            Some("hello from generation test")
        );
        assert_eq!(intent.context_key, format!("{}:9", harness.bot_id));
    }

    #[tokio::test]
    async fn placeholder_failure_prevents_generation_call() {
        let _env = telegram_test_env_lock().lock().await;
        let harness = setup_generation_harness(
            Ok(generation_result("unused")),
            Ok(generation_commit()),
            500,
            200,
            7302,
        )
        .await;
        std::env::set_var("IMBRIDGE_TELEGRAM_API_BASE", harness.server.uri());
        let result = run_generation_process(&harness).await;
        std::env::remove_var("IMBRIDGE_TELEGRAM_API_BASE");
        assert!(result.is_err());
        assert_eq!(harness.backend.generation_calls(), 0);
        assert_eq!(harness.backend.backend_calls(), 0);
        let requests = harness.server.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path().ends_with("/sendMessage"))
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path().ends_with("/editMessageText"))
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn generation_failure_edits_same_placeholder_and_processes_update() {
        let _env = telegram_test_env_lock().lock().await;
        let generation_error = crate::modules::bridge::errors::StBridgeError::boxed(
            StErrorCode::StGenerateStreamInvalid,
            StErrorStage::Generation,
            "synthetic generation stream failure",
            true,
            crate::modules::bridge::errors::CommitState::NotStarted,
        );
        let harness = setup_generation_harness(
            Err(generation_error),
            Ok(generation_commit()),
            200,
            200,
            7303,
        )
        .await;
        std::env::set_var("IMBRIDGE_TELEGRAM_API_BASE", harness.server.uri());
        run_generation_process(&harness).await.unwrap();
        std::env::remove_var("IMBRIDGE_TELEGRAM_API_BASE");
        assert_eq!(harness.backend.generation_calls(), 1);
        let requests = harness.server.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path().ends_with("/sendMessage"))
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path().ends_with("/editMessageText"))
                .count(),
            1
        );
        let update_status: String = sqlx::query_scalar(
            "SELECT status FROM telegram_updates WHERE bot_id = ? AND update_id = ?",
        )
        .bind(&harness.bot_id)
        .bind(harness.update_id)
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(update_status, "processed");
        let operation: (String, String, Option<String>) = sqlx::query_as(
            "SELECT status, commit_state, error_code FROM bridge_operations WHERE id = ?",
        )
        .bind(format!("tg:{}:{}", harness.bot_id, harness.update_id))
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(operation.0, "failed");
        assert_eq!(operation.1, "not_started");
        assert_eq!(operation.2.as_deref(), Some("ST_GENERATE_STREAM_INVALID"));
    }

    #[tokio::test]
    async fn generation_success_finalizes_one_placeholder_and_marks_delivered() {
        let _env = telegram_test_env_lock().lock().await;
        let harness = setup_generation_harness(
            Ok(generation_result("final generated answer")),
            Ok(generation_commit()),
            200,
            200,
            7304,
        )
        .await;
        std::env::set_var("IMBRIDGE_TELEGRAM_API_BASE", harness.server.uri());
        run_generation_process(&harness).await.unwrap();
        std::env::remove_var("IMBRIDGE_TELEGRAM_API_BASE");
        let requests = harness.server.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path().ends_with("/sendMessage"))
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path().ends_with("/editMessageText"))
                .count(),
            1
        );
        let update_status: String = sqlx::query_scalar(
            "SELECT status FROM telegram_updates WHERE bot_id = ? AND update_id = ?",
        )
        .bind(&harness.bot_id)
        .bind(harness.update_id)
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(update_status, "processed");
        let operation: (String, String) =
            sqlx::query_as("SELECT status, commit_state FROM bridge_operations WHERE id = ?")
                .bind(format!("tg:{}:{}", harness.bot_id, harness.update_id))
                .fetch_one(&harness.pool)
                .await
                .unwrap();
        assert_eq!(operation, ("delivered".into(), "applied".into()));
        let delivery_status: (i64, String) = sqlx::query_as(
            "SELECT COUNT(*), MAX(status) FROM channel_deliveries WHERE turn_id = ?",
        )
        .bind(format!("tg:{}:{}", harness.bot_id, harness.update_id))
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(delivery_status, (1, "sent".into()));
    }
}
