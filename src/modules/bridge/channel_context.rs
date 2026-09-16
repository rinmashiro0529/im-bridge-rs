use sqlx::SqlitePool;

use crate::clock::now_rfc3339;
use crate::domain::channel::StChannelLocator;
use crate::domain::st::StChatLocator;
use crate::error::AppResult;

type ChannelLocatorRow = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

#[derive(Clone)]
pub struct ChannelContextStore {
    pool: SqlitePool,
}

impl ChannelContextStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn load(&self, account_id: &str, context_key: &str) -> AppResult<StChannelLocator> {
        let row: Option<ChannelLocatorRow> = sqlx::query_as(
            "SELECT st_handle, st_character_avatar, st_character_name, st_chat_file, chat_model_id, compression_model_id
             FROM channel_contexts
             WHERE account_id = ? AND channel = 'telegram' AND external_context_key = ?",
        )
        .bind(account_id)
        .bind(context_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row
            .map(
                |(
                    handle,
                    avatar,
                    character_name,
                    chat_file,
                    chat_model_id,
                    compression_model_id,
                )| {
                    StChannelLocator {
                        handle,
                        avatar,
                        character_name,
                        chat_file,
                        chat_model_id,
                        compression_model_id,
                    }
                },
            )
            .unwrap_or_default())
    }

    pub async fn select_character(
        &self,
        account_id: &str,
        workspace_id: &str,
        context_key: &str,
        handle: &str,
        avatar: &str,
        character_name: &str,
    ) -> AppResult<()> {
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO channel_contexts
                (account_id, channel, external_context_key, workspace_id, st_handle, st_character_avatar, st_character_name, st_chat_file, updated_at)
             VALUES (?, 'telegram', ?, ?, ?, ?, ?, NULL, ?)
             ON CONFLICT(account_id, channel, external_context_key) DO UPDATE SET
                workspace_id = excluded.workspace_id,
                st_handle = excluded.st_handle,
                st_character_avatar = excluded.st_character_avatar,
                st_character_name = excluded.st_character_name,
                st_chat_file = NULL,
                updated_at = excluded.updated_at",
        )
        .bind(account_id)
        .bind(context_key)
        .bind(workspace_id)
        .bind(handle)
        .bind(avatar)
        .bind(character_name)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn select_chat(
        &self,
        account_id: &str,
        workspace_id: &str,
        context_key: &str,
        locator: &StChatLocator,
    ) -> AppResult<()> {
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO channel_contexts
                (account_id, channel, external_context_key, workspace_id, st_handle, st_character_avatar, st_character_name, st_chat_file, updated_at)
             VALUES (?, 'telegram', ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(account_id, channel, external_context_key) DO UPDATE SET
                workspace_id = excluded.workspace_id,
                st_handle = excluded.st_handle,
                st_character_avatar = excluded.st_character_avatar,
                st_character_name = excluded.st_character_name,
                st_chat_file = excluded.st_chat_file,
                updated_at = excluded.updated_at",
        )
        .bind(account_id)
        .bind(context_key)
        .bind(workspace_id)
        .bind(&locator.handle)
        .bind(&locator.avatar)
        .bind(&locator.character_name)
        .bind(&locator.chat_file)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_model_override(
        &self,
        account_id: &str,
        workspace_id: &str,
        context_key: &str,
        purpose: &str,
        model_id: Option<&str>,
    ) -> AppResult<()> {
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO channel_contexts
                (account_id, channel, external_context_key, workspace_id, chat_model_id, compression_model_id, updated_at)
             VALUES (?, 'telegram', ?, ?, ?, ?, ?)
             ON CONFLICT(account_id, channel, external_context_key) DO UPDATE SET
                workspace_id = COALESCE(excluded.workspace_id, channel_contexts.workspace_id),
                chat_model_id = CASE WHEN ? = 'chat' THEN excluded.chat_model_id ELSE channel_contexts.chat_model_id END,
                compression_model_id = CASE WHEN ? = 'compression' THEN excluded.compression_model_id ELSE channel_contexts.compression_model_id END,
                updated_at = excluded.updated_at",
        )
        .bind(account_id)
        .bind(context_key)
        .bind(workspace_id)
        .bind(if purpose == "chat" { model_id } else { None })
        .bind(if purpose == "compression" {
            model_id
        } else {
            None
        })
        .bind(&now)
        .bind(purpose)
        .bind(purpose)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_recent(
        &self,
        account_id: &str,
        limit: i64,
    ) -> AppResult<Vec<StChannelLocator>> {
        let rows: Vec<ChannelLocatorRow> = sqlx::query_as(
            "SELECT st_handle, st_character_avatar, st_character_name, st_chat_file, chat_model_id, compression_model_id
             FROM channel_contexts
             WHERE account_id = ? AND channel = 'telegram' AND st_chat_file IS NOT NULL
             ORDER BY updated_at DESC
             LIMIT ?",
        )
        .bind(account_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    handle,
                    avatar,
                    character_name,
                    chat_file,
                    chat_model_id,
                    compression_model_id,
                )| {
                    StChannelLocator {
                        handle,
                        avatar,
                        character_name,
                        chat_file,
                        chat_model_id,
                        compression_model_id,
                    }
                },
            )
            .collect())
    }
}
