use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use crate::clock::now_rfc3339;
use crate::domain::identity::Actor;
use crate::error::{AppError, AppResult};
use crate::ids::new_id;
use crate::modules::characters::CharacterModule;
use crate::modules::identity::IdentityModule;
use crate::modules::models::ModelModule;
use crate::modules::telegram::TelegramModule;
use crate::seams::legacy_source::{LegacyChatFile, LegacySource};
use crate::seams::secret_vault::SecretVault;

pub struct ImportReport {
    pub characters: u32,
    pub conversations: u32,
    pub messages: u32,
    pub bots: u32,
    pub bindings: u32,
    pub warnings: Vec<String>,
}

pub struct LegacyImporter {
    pool: SqlitePool,
    identity: IdentityModule,
    characters: CharacterModule,
    models: ModelModule,
}

impl LegacyImporter {
    pub fn new(
        pool: SqlitePool,
        identity: IdentityModule,
        characters: CharacterModule,
        models: ModelModule,
    ) -> Self {
        Self {
            pool,
            identity,
            characters,
            models,
        }
    }

    pub async fn import_source(
        &self,
        source: &dyn LegacySource,
        dry_run: bool,
    ) -> AppResult<ImportReport> {
        let mut report = ImportReport {
            characters: 0,
            conversations: 0,
            messages: 0,
            bots: 0,
            bindings: 0,
            warnings: Vec::new(),
        };
        for handle in source.list_handles().await? {
            let characters = source.list_characters(&handle).await?;
            let chats = source.list_chats(&handle).await?;
            if dry_run {
                report.characters += characters.len() as u32;
                report.conversations += chats.len() as u32;
                report.messages += chats
                    .iter()
                    .map(|chat| chat.lines.len().saturating_sub(1) as u32)
                    .sum::<u32>();
                continue;
            }
            let (account, workspace_id) = self
                .identity
                .ensure_legacy_account(&handle, &handle)
                .await?;
            if let Some(settings) = source.load_settings(&handle).await? {
                self.models
                    .update_prompt_user_name(&workspace_id, &settings.username)
                    .await?;
            }
            let actor = Actor {
                account: account.clone(),
                workspace_id: Some(workspace_id.clone()),
                workspace_role: Some(crate::domain::identity::WorkspaceRole::Owner),
            };
            for character in characters {
                let source_key = character_source_key(&handle, &character);
                if source_target(&self.pool, "st_character", &source_key)
                    .await?
                    .is_some()
                {
                    continue;
                }
                let checksum = crate::modules::characters::card_png::sha256_hex(&character.bytes);
                if let Some(character_id) =
                    existing_character_by_checksum(&self.pool, &workspace_id, &checksum).await?
                {
                    record_source_target(&self.pool, "st_character", &source_key, &character_id)
                        .await?;
                    continue;
                }
                let imported = self
                    .characters
                    .import_bytes(
                        &actor,
                        character
                            .path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or("character.png"),
                        &character.bytes,
                    )
                    .await?;
                record_source_target(&self.pool, "st_character", &source_key, &imported.id).await?;
                report.characters += 1;
            }
            for chat in chats {
                let source_key = chat_source_key(&handle, &chat);
                if source_target(&self.pool, "st_chat", &source_key)
                    .await?
                    .is_some()
                {
                    continue;
                }
                let checksum = crate::modules::characters::card_png::sha256_hex(
                    chat.lines.join("\n").as_bytes(),
                );
                if let Some(conversation_id) =
                    existing_chat_import(&self.pool, &handle, &chat.path, &checksum).await?
                {
                    record_source_target(&self.pool, "st_chat", &source_key, &conversation_id)
                        .await?;
                    continue;
                }
                match self
                    .import_chat(&workspace_id, &actor.account.id, &handle, &chat)
                    .await
                {
                    Ok((conversation_id, count)) => {
                        record_source_target(&self.pool, "st_chat", &source_key, &conversation_id)
                            .await?;
                        report.conversations += 1;
                        report.messages += count;
                    }
                    Err(err) => {
                        report
                            .warnings
                            .push(format!("{}: {}", chat.path.display(), err.message))
                    }
                }
            }
        }
        Ok(report)
    }

    pub async fn import_plugin_db(
        &self,
        plugin_db: &Path,
        telegram: &TelegramModule,
        vault: &dyn SecretVault,
        dry_run: bool,
    ) -> AppResult<ImportReport> {
        if !plugin_db.exists() {
            return Err(AppError::not_found(
                "PLUGIN_DB_NOT_FOUND",
                format!("plugin database not found: {}", plugin_db.display()),
            ));
        }
        let options = SqliteConnectOptions::new()
            .filename(plugin_db)
            .read_only(true)
            .foreign_keys(false);
        let legacy = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let mut report = ImportReport {
            characters: 0,
            conversations: 0,
            messages: 0,
            bots: 0,
            bindings: 0,
            warnings: Vec::new(),
        };
        if !table_exists(&legacy, "accounts").await? {
            report
                .warnings
                .push("plugin DB has no accounts table".into());
            return Ok(report);
        }
        let st_handle_expr = if column_exists(&legacy, "accounts", "st_user_handle").await? {
            "st_user_handle"
        } else {
            "NULL AS st_user_handle"
        };
        // Both fragments are compile-time choices, never legacy database contents.
        let account_rows =
            sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT account_id, display_name, ")
                .push(st_handle_expr)
                .push(" FROM accounts ORDER BY created_at")
                .build()
                .fetch_all(&legacy)
                .await?;
        let mut account_specs = Vec::new();
        for row in account_rows {
            let old_id: String = row.try_get("account_id")?;
            let display_name: Option<String> = row.try_get("display_name")?;
            let handle: Option<String> = row.try_get("st_user_handle")?;
            account_specs.push((
                old_id.clone(),
                handle
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or(old_id),
                display_name.unwrap_or_else(|| "Imported user".into()),
            ));
        }
        let config_rows = if table_exists(&legacy, "account_configs").await? {
            let inter_delay = optional_column(
                &legacy,
                "account_configs",
                "tg_inter_message_delay_ms",
                "1400 AS tg_inter_message_delay_ms",
            )
            .await?;
            let stream_interval = optional_column(
                &legacy,
                "account_configs",
                "tg_stream_min_interval_ms",
                "5000 AS tg_stream_min_interval_ms",
            )
            .await?;
            let stream_delta = optional_column(
                &legacy,
                "account_configs",
                "tg_stream_min_delta_chars",
                "700 AS tg_stream_min_delta_chars",
            )
            .await?;
            let advanced = optional_column(
                &legacy,
                "account_configs",
                "tg_advanced_json",
                "'{}' AS tg_advanced_json",
            )
            .await?;
            // optional_column only accepts static identifiers and fallback expressions.
            sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "SELECT account_id, telegram_bot_token, telegram_allowed_user_ids, ",
            )
            .push(inter_delay)
            .push(", ")
            .push(stream_interval)
            .push(", ")
            .push(stream_delta)
            .push(", ")
            .push(advanced)
            .push(" FROM account_configs")
            .build()
            .fetch_all(&legacy)
            .await?
        } else {
            report
                .warnings
                .push("plugin DB has no account_configs table".into());
            Vec::new()
        };
        let identity_rows = if table_exists(&legacy, "external_identities").await? {
            sqlx::query(
                "SELECT account_id, external_user_id FROM external_identities WHERE channel = 'telegram'",
            )
            .fetch_all(&legacy)
            .await?
        } else {
            Vec::new()
        };
        if dry_run {
            report.bots = config_rows
                .iter()
                .filter(|row| {
                    row.try_get::<Option<String>, _>("telegram_bot_token")
                        .ok()
                        .flatten()
                        .is_some_and(|token| !token.trim().is_empty())
                })
                .count() as u32;
            let mut owners_by_user: HashMap<String, HashSet<String>> = HashMap::new();
            for row in &config_rows {
                let owner = row
                    .try_get::<String, _>("account_id")
                    .unwrap_or_else(|_| "unknown".into());
                if let Ok(raw) = row.try_get::<String, _>("telegram_allowed_user_ids") {
                    for user in parse_legacy_user_ids(&raw) {
                        owners_by_user
                            .entry(user)
                            .or_default()
                            .insert(owner.clone());
                    }
                }
            }
            for row in &identity_rows {
                if let (Ok(owner), Ok(user)) = (
                    row.try_get::<String, _>("account_id"),
                    row.try_get::<String, _>("external_user_id"),
                ) {
                    owners_by_user.entry(user).or_default().insert(owner);
                }
            }
            for (user, owners) in owners_by_user {
                if owners.len() == 1 {
                    report.bindings += 1;
                } else {
                    report.warnings.push(format!(
                        "skipped Telegram user {user}: present under multiple legacy owners"
                    ));
                }
            }
            return Ok(report);
        }
        let mut actors = HashMap::new();
        for (old_id, handle, display_name) in account_specs {
            let (account, workspace_id) = self
                .identity
                .ensure_legacy_account(&handle, &display_name)
                .await?;
            let actor = self
                .identity
                .actor_in_workspace(account, &workspace_id)
                .await?;
            actors.insert(old_id, actor);
        }
        let mut bots_by_account = HashMap::new();
        let mut pending_bindings: HashMap<String, HashSet<String>> = HashMap::new();
        for row in config_rows {
            let old_account_id: String = row.try_get("account_id")?;
            let Some(actor) = actors.get(&old_account_id) else {
                report.warnings.push(format!(
                    "plugin config references unknown account {old_account_id}"
                ));
                continue;
            };
            let token: Option<String> = row.try_get("telegram_bot_token")?;
            if let Some(token) = token.filter(|value| !value.trim().is_empty()) {
                let workspace_id = actor.require_workspace()?;
                let existing_bot = telegram.list_bots(workspace_id).await?.into_iter().next();
                let existing_secret_id = existing_bot
                    .as_ref()
                    .and_then(|bot| bot.token_secret_id.clone());
                let same_secret = if let Some(secret_id) = existing_secret_id.as_deref() {
                    vault
                        .get(secret_id)
                        .await
                        .map(|bytes| bytes == token.as_bytes())
                        .unwrap_or(false)
                } else {
                    false
                };
                let (secret_id, replaced_secret_id) =
                    if let (true, Some(secret_id)) = (same_secret, existing_secret_id.clone()) {
                        (secret_id, None)
                    } else {
                        let secret = vault
                            .put(workspace_id, "telegram_bot_token", token.as_bytes())
                            .await?;
                        (secret.id, existing_secret_id)
                    };
                let bot = telegram.upsert_bot(actor, Some(&secret_id), false).await?;
                if let Some(old_secret_id) = replaced_secret_id {
                    if old_secret_id != secret_id {
                        vault.delete(&old_secret_id).await?;
                    }
                }
                let inter_delay: i64 = row.try_get("tg_inter_message_delay_ms")?;
                let stream_interval: i64 = row.try_get("tg_stream_min_interval_ms")?;
                let stream_delta: i64 = row.try_get("tg_stream_min_delta_chars")?;
                let advanced: String = row.try_get("tg_advanced_json")?;
                sqlx::query(
                    "UPDATE telegram_bots SET desired_enabled = 0,
                        inter_message_delay_ms = ?, stream_min_interval_ms = ?,
                        stream_min_delta_chars = ?, advanced_config_json = ?, updated_at = ?
                     WHERE id = ?",
                )
                .bind(inter_delay.max(0))
                .bind(stream_interval.max(0))
                .bind(stream_delta.max(1))
                .bind(advanced)
                .bind(now_rfc3339())
                .bind(&bot.id)
                .execute(&self.pool)
                .await?;
                bots_by_account.insert(old_account_id.clone(), bot.id);
                report.bots += 1;
            }
            let allowed: String = row.try_get("telegram_allowed_user_ids")?;
            pending_bindings
                .entry(old_account_id)
                .or_default()
                .extend(parse_legacy_user_ids(&allowed));
        }
        for row in identity_rows {
            let account_id: String = row.try_get("account_id")?;
            let user_id: String = row.try_get("external_user_id")?;
            pending_bindings
                .entry(account_id)
                .or_default()
                .insert(user_id);
        }
        let mut owners_by_user: HashMap<String, HashSet<String>> = HashMap::new();
        for (owner, users) in &pending_bindings {
            for user in users {
                owners_by_user
                    .entry(user.clone())
                    .or_default()
                    .insert(owner.clone());
            }
        }
        let conflicting_users: HashSet<String> = owners_by_user
            .into_iter()
            .filter_map(|(user, owners)| (owners.len() > 1).then_some(user))
            .collect();
        for user in &conflicting_users {
            report.warnings.push(format!(
                "skipped Telegram user {user}: present under multiple legacy owners"
            ));
        }
        for (old_account_id, users) in pending_bindings {
            let Some(actor) = actors.get(&old_account_id) else {
                continue;
            };
            let Some(bot_id) = bots_by_account.get(&old_account_id) else {
                if !users.is_empty() {
                    report.warnings.push(format!(
                        "skipped {} Telegram binding(s) for account {old_account_id}: no bot token",
                        users.len()
                    ));
                }
                continue;
            };
            for user_id in users {
                if conflicting_users.contains(&user_id) {
                    continue;
                }
                if import_telegram_binding(
                    &self.pool,
                    bot_id,
                    &actor.account.id,
                    actor.require_workspace()?,
                    &user_id,
                )
                .await?
                {
                    report.bindings += 1;
                } else {
                    report.warnings.push(format!(
                        "skipped Telegram user {user_id}: identity already belongs to another account"
                    ));
                }
            }
        }
        if report.bots > 0 {
            report.warnings.push(
                "Imported Bot tokens are encrypted but all Bots remain disabled; enable only after the old poller is stopped."
                    .into(),
            );
        }
        Ok(report)
    }

    async fn import_chat(
        &self,
        workspace_id: &str,
        account_id: &str,
        handle: &str,
        chat: &LegacyChatFile,
    ) -> AppResult<(String, u32)> {
        let header = chat
            .lines
            .first()
            .and_then(|line| serde_json::from_str::<Value>(line).ok())
            .unwrap_or(json!({}));
        let wanted_name = header
            .get("character_name")
            .and_then(Value::as_str)
            .unwrap_or(&chat.character_dir);
        let mut characters = self.characters.list(workspace_id).await?;
        let character = if let Some(index) = characters
            .iter()
            .position(|item| item.display_name == wanted_name)
        {
            characters.remove(index)
        } else {
            characters.pop().ok_or_else(|| {
                AppError::bad_request(
                    "CHARACTER_REQUIRED",
                    "no character available for chat import",
                )
            })?
        };
        let classification = if chat
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("")
            .contains(".pre_compress_")
        {
            "pre-compress-backup"
        } else {
            "primary"
        };
        let conversation_id = new_id();
        let now = now_rfc3339();
        let mut tx = self.pool.begin().await?;
        let title = header
            .get("character_name")
            .and_then(Value::as_str)
            .unwrap_or(&character.display_name)
            .to_string();
        sqlx::query(
            "INSERT INTO conversations
                (id, workspace_id, character_id, title, prompt_profile, revision, archived, legacy_locator, created_at, updated_at)
             VALUES (?, ?, ?, ?, 'legacy_bridge_v1', 0, ?, ?, ?, ?)",
        )
        .bind(&conversation_id)
        .bind(workspace_id)
        .bind(&character.id)
        .bind(&title)
        .bind(i64::from(classification != "primary"))
        .bind(chat.path.to_string_lossy().as_ref())
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO legacy_chat_archives
                (id, conversation_id, source_handle, source_path, source_checksum, raw_header_json, classification, imported_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(new_id())
        .bind(&conversation_id)
        .bind(handle)
        .bind(chat.path.to_string_lossy().as_ref())
        .bind(crate::modules::characters::card_png::sha256_hex(chat.lines.join("\n").as_bytes()))
        .bind(header.to_string())
        .bind(classification)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        let mut count = 0u32;
        let mut sequence = 0i64;
        let mut ordinal = 0i64;
        let mut current_turn: Option<(String, bool)> = None;
        for line in chat.lines.iter().skip(1) {
            let Ok(raw) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            sequence += 1;
            let is_user = raw.get("is_user").and_then(Value::as_bool).unwrap_or(false);
            let is_system = raw
                .get("is_system")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let role = if is_system {
                "system"
            } else if is_user {
                "user"
            } else {
                "assistant"
            };
            let turn_id = if is_system {
                None
            } else if is_user {
                ordinal += 1;
                let turn_id = new_id();
                sqlx::query(
                    "INSERT INTO turns
                        (id, conversation_id, ordinal, status, created_by, created_at, updated_at)
                     VALUES (?, ?, ?, 'pending', ?, ?, ?)",
                )
                .bind(&turn_id)
                .bind(&conversation_id)
                .bind(ordinal)
                .bind(account_id)
                .bind(&now)
                .bind(&now)
                .execute(&mut *tx)
                .await?;
                current_turn = Some((turn_id.clone(), false));
                Some(turn_id)
            } else {
                let turn_id = match current_turn.as_ref() {
                    Some((turn_id, false)) => turn_id.clone(),
                    _ => {
                        ordinal += 1;
                        let turn_id = new_id();
                        sqlx::query(
                            "INSERT INTO turns
                                (id, conversation_id, ordinal, status, created_by, created_at, updated_at)
                             VALUES (?, ?, ?, 'pending', ?, ?, ?)",
                        )
                        .bind(&turn_id)
                        .bind(&conversation_id)
                        .bind(ordinal)
                        .bind(account_id)
                        .bind(&now)
                        .bind(&now)
                        .execute(&mut *tx)
                        .await?;
                        turn_id
                    }
                };
                current_turn = Some((turn_id.clone(), true));
                Some(turn_id)
            };
            let message_id = new_id();
            let original = raw
                .get("mes")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let prompt = raw
                .pointer("/extra/display_text")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            sqlx::query(
                "INSERT INTO messages
                    (id, conversation_id, turn_id, sequence, role, content_original, prompt_content, status, metadata_json, source_raw_json, source_kind, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, 'st_jsonl', ?)",
            )
            .bind(&message_id)
            .bind(&conversation_id)
            .bind(turn_id.as_deref())
            .bind(sequence)
            .bind(role)
            .bind(&original)
            .bind(&prompt)
            .bind(raw.to_string())
            .bind(raw.to_string())
            .bind(&now)
            .execute(&mut *tx)
            .await?;
            if role == "assistant" {
                if let Some(turn_id) = turn_id.as_deref() {
                    sqlx::query(
                        "UPDATE turns SET status = 'complete', active_assistant_message_id = ?, updated_at = ? WHERE id = ?",
                    )
                    .bind(&message_id)
                    .bind(&now)
                    .bind(turn_id)
                    .execute(&mut *tx)
                    .await?;
                }
            }
            count += 1;
        }
        sqlx::query("UPDATE conversations SET revision = ?, updated_at = ? WHERE id = ?")
            .bind(ordinal)
            .bind(&now)
            .bind(&conversation_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok((conversation_id, count))
    }

    pub async fn export_workspace(&self, workspace_id: &str, output: &Path) -> AppResult<u32> {
        std::fs::create_dir_all(output)?;
        let conversations: Vec<(String, String, Option<String>, String)> = sqlx::query_as(
            "SELECT c.id, c.title, c.legacy_locator, ch.display_name
             FROM conversations c
             JOIN characters ch ON ch.id = c.character_id
             WHERE c.workspace_id = ?",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;
        let mut count = 0u32;
        for (id, title, locator, character_name) in conversations {
            let archive: Option<(String,)> = sqlx::query_as(
                "SELECT raw_header_json FROM legacy_chat_archives WHERE conversation_id = ? ORDER BY imported_at LIMIT 1",
            )
            .bind(&id)
            .fetch_optional(&self.pool)
            .await?;
            #[allow(clippy::type_complexity)]
            let messages: Vec<(String, String, Option<String>, Option<String>, String)> = sqlx::query_as(
                "SELECT role, content_original, prompt_content, source_raw_json, status FROM messages WHERE conversation_id = ? ORDER BY sequence",
            )
            .bind(&id)
            .fetch_all(&self.pool)
            .await?;
            let mut lines = Vec::new();
            lines.push(
                archive
                    .and_then(|row| serde_json::from_str::<Value>(&row.0).ok())
                    .unwrap_or(json!({
                        "chat_metadata": {"integrity": new_id()},
                        "character_name": title,
                    }))
                    .to_string(),
            );
            for (role, original, prompt, source_raw, status) in messages {
                if status != "active" {
                    continue;
                }
                let mut object = source_raw
                    .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
                    .unwrap_or(json!({}));
                if let Some(map) = object.as_object_mut() {
                    map.insert("mes".into(), json!(prompt.as_deref().unwrap_or(&original)));
                    map.insert("is_user".into(), json!(role == "user"));
                    map.insert("is_system".into(), json!(role == "system"));
                }
                lines.push(object.to_string());
            }
            let filename = locator
                .as_deref()
                .and_then(|path| Path::new(path).file_name())
                .and_then(|name| name.to_str())
                .map(safe_path_component)
                .unwrap_or_else(|| format!("{}-{}.jsonl", safe_path_component(&title), id));
            let character_dir = locator
                .as_deref()
                .and_then(|path| Path::new(path).parent())
                .and_then(|parent| parent.file_name())
                .and_then(|name| name.to_str())
                .map(safe_path_component)
                .unwrap_or_else(|| safe_path_component(&character_name));
            let target_dir = output.join(character_dir);
            std::fs::create_dir_all(&target_dir)?;
            std::fs::write(target_dir.join(filename), lines.join("\n") + "\n")?;
            count += 1;
        }
        Ok(count)
    }
}

pub struct FilesystemLegacySource {
    data_root: PathBuf,
}

impl FilesystemLegacySource {
    pub fn new(data_root: PathBuf) -> Self {
        Self { data_root }
    }
}

#[async_trait::async_trait]
impl LegacySource for FilesystemLegacySource {
    async fn list_handles(&self) -> AppResult<Vec<String>> {
        let mut handles = Vec::new();
        for entry in std::fs::read_dir(&self.data_root)? {
            let entry = entry?;
            if entry.path().is_dir() {
                handles.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        Ok(handles)
    }

    async fn list_characters(
        &self,
        handle: &str,
    ) -> AppResult<Vec<crate::seams::legacy_source::LegacyCharacterFile>> {
        let dir = self.data_root.join(handle).join("characters");
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("png")
                || path.extension().and_then(|ext| ext.to_str()) == Some("json")
            {
                files.push(crate::seams::legacy_source::LegacyCharacterFile {
                    handle: handle.to_string(),
                    bytes: std::fs::read(&path)?,
                    path,
                });
            }
        }
        Ok(files)
    }

    async fn list_chats(&self, handle: &str) -> AppResult<Vec<LegacyChatFile>> {
        let dir = self.data_root.join(handle).join("chats");
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        visit_jsonl(&dir, handle, &mut files)?;
        Ok(files)
    }

    async fn load_settings(
        &self,
        handle: &str,
    ) -> AppResult<Option<crate::seams::legacy_source::LegacySettings>> {
        let path = self.data_root.join(handle).join("settings.json");
        if !path.exists() {
            return Ok(None);
        }
        let raw: Value = serde_json::from_slice(&std::fs::read(path)?).unwrap_or(json!({}));
        Ok(Some(crate::seams::legacy_source::LegacySettings {
            handle: handle.to_string(),
            username: raw
                .get("username")
                .and_then(Value::as_str)
                .unwrap_or("User")
                .to_string(),
            raw,
        }))
    }
}

fn safe_path_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' | '\0'..='\u{1f}' => '_',
            _ => ch,
        })
        .collect();
    let sanitized = sanitized.trim().trim_matches('.');
    if sanitized.is_empty() || matches!(sanitized, "." | "..") {
        "unnamed".into()
    } else {
        sanitized.to_string()
    }
}

fn character_source_key(
    handle: &str,
    character: &crate::seams::legacy_source::LegacyCharacterFile,
) -> String {
    let filename = character
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("character");
    let checksum = crate::modules::characters::card_png::sha256_hex(&character.bytes);
    format!("{handle}:{filename}:{checksum}")
}

fn chat_source_key(handle: &str, chat: &LegacyChatFile) -> String {
    let filename = chat
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("chat.jsonl");
    let checksum =
        crate::modules::characters::card_png::sha256_hex(chat.lines.join("\n").as_bytes());
    format!("{handle}:{}:{filename}:{checksum}", chat.character_dir)
}

async fn existing_character_by_checksum(
    pool: &SqlitePool,
    workspace_id: &str,
    checksum: &str,
) -> AppResult<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT cr.character_id
         FROM character_revisions cr
         JOIN characters c ON c.id = cr.character_id
         WHERE c.workspace_id = ? AND cr.checksum = ?
         ORDER BY cr.created_at LIMIT 1",
    )
    .bind(workspace_id)
    .bind(checksum)
    .fetch_optional(pool)
    .await?)
}

async fn existing_chat_import(
    pool: &SqlitePool,
    handle: &str,
    path: &Path,
    checksum: &str,
) -> AppResult<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT conversation_id FROM legacy_chat_archives
         WHERE source_handle = ? AND source_path = ? AND source_checksum = ?
         ORDER BY imported_at LIMIT 1",
    )
    .bind(handle)
    .bind(path.to_string_lossy().as_ref())
    .bind(checksum)
    .fetch_optional(pool)
    .await?)
}

async fn source_target(
    pool: &SqlitePool,
    source_kind: &str,
    source_key: &str,
) -> AppResult<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT target_id FROM legacy_source_keys WHERE source_kind = ? AND source_key = ?",
    )
    .bind(source_kind)
    .bind(source_key)
    .fetch_optional(pool)
    .await?)
}

async fn record_source_target(
    pool: &SqlitePool,
    source_kind: &str,
    source_key: &str,
    target_id: &str,
) -> AppResult<()> {
    sqlx::query(
        "INSERT OR IGNORE INTO legacy_source_keys
            (source_kind, source_key, target_id, imported_at)
         VALUES (?, ?, ?, ?)",
    )
    .bind(source_kind)
    .bind(source_key)
    .bind(target_id)
    .bind(now_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

async fn column_exists(pool: &SqlitePool, table: &str, column: &str) -> AppResult<bool> {
    let exists: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?")
            .bind(table)
            .bind(column)
            .fetch_one(pool)
            .await?;
    Ok(exists > 0)
}

async fn optional_column(
    pool: &SqlitePool,
    table: &str,
    column: &'static str,
    fallback: &'static str,
) -> AppResult<&'static str> {
    Ok(if column_exists(pool, table, column).await? {
        column
    } else {
        fallback
    })
}

async fn table_exists(pool: &SqlitePool, name: &str) -> AppResult<bool> {
    let exists: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(name)
            .fetch_one(pool)
            .await?;
    Ok(exists > 0)
}

fn parse_legacy_user_ids(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<Value>>(raw)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| match value {
            Value::String(value) if !value.trim().is_empty() => Some(value),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        })
        .collect()
}

async fn import_telegram_binding(
    pool: &SqlitePool,
    bot_id: &str,
    account_id: &str,
    workspace_id: &str,
    telegram_user_id: &str,
) -> AppResult<bool> {
    let now = now_rfc3339();
    let existing: Option<(String, String)> = sqlx::query_as(
        "SELECT id, account_id FROM external_identities
         WHERE channel = 'telegram' AND external_user_id = ?",
    )
    .bind(telegram_user_id)
    .fetch_optional(pool)
    .await?;
    let identity_id = if let Some((identity_id, owner_account_id)) = existing {
        if owner_account_id != account_id {
            return Ok(false);
        }
        sqlx::query("UPDATE external_identities SET verified_at = ? WHERE id = ?")
            .bind(&now)
            .bind(&identity_id)
            .execute(pool)
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
        .execute(pool)
        .await?;
        identity_id
    };
    let exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM telegram_bindings
         WHERE bot_id = ? AND external_identity_id = ? AND revoked_at IS NULL",
    )
    .bind(bot_id)
    .bind(&identity_id)
    .fetch_one(pool)
    .await?;
    if exists == 0 {
        sqlx::query(
            "INSERT INTO telegram_bindings
                (id, bot_id, external_identity_id, workspace_id, bound_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(new_id())
        .bind(bot_id)
        .bind(identity_id)
        .bind(workspace_id)
        .bind(now)
        .execute(pool)
        .await?;
    }
    Ok(true)
}

fn visit_jsonl(dir: &Path, handle: &str, files: &mut Vec<LegacyChatFile>) -> AppResult<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            visit_jsonl(&path, handle, files)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            let content = std::fs::read_to_string(&path)?;
            files.push(LegacyChatFile {
                handle: handle.to_string(),
                character_dir: path
                    .parent()
                    .and_then(|parent| parent.file_name())
                    .and_then(|name| name.to_str())
                    .unwrap_or("")
                    .to_string(),
                path,
                lines: content.lines().map(ToOwned::to_owned).collect(),
            });
        }
    }
    Ok(())
}
