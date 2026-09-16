use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::SqlitePool;

use crate::clock::now_rfc3339;
use crate::domain::conversation::{
    Conversation, GenerationRun, GenerationStatus, HistoryView, Message, MessageRole, MessageStatus,
};
use crate::domain::identity::Actor;
use crate::error::{AppError, AppResult};
use crate::ids::new_id;
use crate::modules::characters::CharacterModule;
use crate::modules::chat::history::history_items;
use crate::modules::chat::locks::ConversationLocks;
use crate::modules::chat::prompt::{
    build_system_prompt, normalize_assistant_reply, sanitize_compression, select_recent_messages,
    to_openai_messages, COMPRESSION_SYSTEM_PROMPT,
};
use crate::modules::models::ModelModule;
use crate::seams::llm_gateway::{LlmGateway, LlmRequest, ProgressEvent, ProgressSink};
use crate::seams::secret_vault::SecretVault;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChatCommand {
    StartConversation {
        character_id: String,
        channel: String,
        external_context_key: String,
        client_turn_id: Option<String>,
    },
    SelectCharacter {
        character_id: String,
        channel: String,
        external_context_key: String,
    },
    SelectConversation {
        conversation_id: String,
        channel: String,
        external_context_key: String,
    },
    SendMessage {
        conversation_id: String,
        text: String,
        channel: String,
        external_context_key: String,
        client_turn_id: Option<String>,
        expected_revision: Option<i64>,
        model_preset_id: Option<String>,
    },
    UndoLastTurn {
        conversation_id: String,
        channel: String,
        external_context_key: String,
        client_turn_id: Option<String>,
        expected_revision: Option<i64>,
    },
    RegenerateReply {
        conversation_id: String,
        channel: String,
        external_context_key: String,
        client_turn_id: Option<String>,
        expected_revision: Option<i64>,
        model_preset_id: Option<String>,
    },
    RetryGeneration {
        conversation_id: String,
        operation_id: String,
        channel: String,
        external_context_key: String,
        client_turn_id: Option<String>,
    },
    CompressHistory {
        conversation_id: String,
        channel: String,
        external_context_key: String,
        client_turn_id: Option<String>,
        keep_recent: usize,
        model_preset_id: Option<String>,
    },
    SetChannelModel {
        channel: String,
        external_context_key: String,
        purpose: String,
        model_preset_id: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChatQuery {
    ListConversations {
        workspace_id: Option<String>,
    },
    GetConversation {
        conversation_id: String,
    },
    GetHistory {
        conversation_id: String,
        known_revision: Option<i64>,
    },
    GetChannelContext {
        channel: String,
        external_context_key: String,
    },
    GetOperation {
        operation_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatOutcome {
    pub operation_id: String,
    pub conversation: Option<Conversation>,
    pub reply_text: Option<String>,
    pub revision: i64,
    pub status: String,
    pub in_progress: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatView {
    pub conversations: Vec<Conversation>,
    pub conversation: Option<Conversation>,
    pub history: Option<HistoryView>,
    pub operation: Option<GenerationRun>,
    pub channel_character_id: Option<String>,
    pub chat_model_preset_id: Option<String>,
    pub compression_model_preset_id: Option<String>,
}

#[async_trait]
pub trait ChatEngine: Send + Sync + 'static {
    async fn execute(
        &self,
        actor: &Actor,
        command: ChatCommand,
        progress: Option<&dyn ProgressSink>,
    ) -> AppResult<ChatOutcome>;

    async fn query(&self, actor: &Actor, query: ChatQuery) -> AppResult<ChatView>;
}

pub struct SqliteChatEngine {
    pool: SqlitePool,
    locks: ConversationLocks,
    characters: CharacterModule,
    models: ModelModule,
    llm: Arc<dyn LlmGateway>,
    vault: Option<Arc<dyn SecretVault>>,
}

impl SqliteChatEngine {
    pub fn new(
        pool: SqlitePool,
        characters: CharacterModule,
        models: ModelModule,
        llm: Arc<dyn LlmGateway>,
        vault: Option<Arc<dyn SecretVault>>,
    ) -> Self {
        Self {
            pool,
            locks: ConversationLocks::new(),
            characters,
            models,
            llm,
            vault,
        }
    }

    async fn get_conversation(&self, id: &str) -> AppResult<Conversation> {
        load_conversation(&self.pool, id)
            .await?
            .ok_or_else(|| AppError::not_found("CONVERSATION_NOT_FOUND", "conversation not found"))
    }

    async fn require_workspace_access(
        &self,
        actor: &Actor,
        conversation: &Conversation,
    ) -> AppResult<()> {
        if actor.account.is_system_admin {
            return Ok(());
        }
        let workspace_id = actor.require_workspace()?;
        if workspace_id != conversation.workspace_id {
            return Err(AppError::forbidden(
                "conversation is outside the current workspace",
            ));
        }
        Ok(())
    }

    async fn replay_or_start(
        &self,
        actor: &Actor,
        channel: &str,
        external_context_key: &str,
        operation_kind: &str,
        client_turn_id: Option<&str>,
    ) -> AppResult<Option<ChatOutcome>> {
        let Some(client_turn_id) = client_turn_id else {
            return Ok(None);
        };
        let existing = sqlx::query_as::<_, GenerationRow>(
            "SELECT id, conversation_id, turn_id, actor_id, channel, external_context_key, operation_kind, client_turn_id, status,
                    effective_character_revision_id, error_code, error_message, partial_content, result_json
             FROM generation_runs
             WHERE actor_id = ? AND channel = ? AND external_context_key = ? AND operation_kind = ? AND client_turn_id = ?",
        )
        .bind(&actor.account.id)
        .bind(channel)
        .bind(external_context_key)
        .bind(operation_kind)
        .bind(client_turn_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(existing) = existing else {
            return Ok(None);
        };
        let status = GenerationStatus::parse(&existing.status).unwrap_or(GenerationStatus::Failed);
        match status {
            GenerationStatus::Completed => {
                let conversation = self.get_conversation(&existing.conversation_id).await?;
                let reply = existing
                    .result_json
                    .as_ref()
                    .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                    .and_then(|value| {
                        value
                            .get("reply_text")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned)
                    });
                Ok(Some(ChatOutcome {
                    operation_id: existing.id,
                    revision: conversation.revision,
                    conversation: Some(conversation),
                    reply_text: reply,
                    status: "completed".into(),
                    in_progress: false,
                }))
            }
            GenerationStatus::Started | GenerationStatus::Streaming => Ok(Some(ChatOutcome {
                operation_id: existing.id,
                conversation: load_conversation(&self.pool, &existing.conversation_id).await?,
                reply_text: None,
                revision: 0,
                status: "in_progress".into(),
                in_progress: true,
            })),
            GenerationStatus::Failed | GenerationStatus::Interrupted => Err(AppError::conflict(
                "OPERATION_RETRY_REQUIRED",
                "previous operation failed; use RetryGeneration",
            )),
        }
    }

    async fn persist_channel_context(
        &self,
        actor: &Actor,
        channel: &str,
        external_context_key: &str,
        conversation: &Conversation,
    ) -> AppResult<()> {
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO channel_contexts
                (account_id, channel, external_context_key, workspace_id, character_id, conversation_id, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(account_id, channel, external_context_key) DO UPDATE SET
                workspace_id = excluded.workspace_id,
                character_id = excluded.character_id,
                conversation_id = excluded.conversation_id,
                updated_at = excluded.updated_at",
        )
        .bind(&actor.account.id)
        .bind(channel)
        .bind(external_context_key)
        .bind(&conversation.workspace_id)
        .bind(&conversation.character_id)
        .bind(&conversation.id)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn select_character_context(
        &self,
        actor: &Actor,
        character_id: &str,
        channel: &str,
        external_context_key: &str,
    ) -> AppResult<()> {
        let workspace_id = actor.require_workspace()?.to_string();
        let character = self
            .characters
            .get(character_id)
            .await?
            .ok_or_else(|| AppError::not_found("CHARACTER_NOT_FOUND", "character not found"))?;
        if character.workspace_id != workspace_id {
            return Err(AppError::forbidden(
                "character is outside the current workspace",
            ));
        }
        sqlx::query(
            "INSERT INTO channel_contexts
                (account_id, channel, external_context_key, workspace_id, character_id, conversation_id, updated_at)
             VALUES (?, ?, ?, ?, ?, NULL, ?)
             ON CONFLICT(account_id, channel, external_context_key) DO UPDATE SET
                workspace_id = excluded.workspace_id,
                character_id = excluded.character_id,
                conversation_id = NULL,
                updated_at = excluded.updated_at",
        )
        .bind(&actor.account.id)
        .bind(channel)
        .bind(external_context_key)
        .bind(&workspace_id)
        .bind(character_id)
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn channel_model(
        &self,
        actor: &Actor,
        channel: &str,
        external_context_key: &str,
        purpose: &str,
    ) -> AppResult<Option<String>> {
        let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT chat_model_preset_id, compression_model_preset_id
             FROM channel_contexts
             WHERE account_id = ? AND channel = ? AND external_context_key = ?",
        )
        .bind(&actor.account.id)
        .bind(channel)
        .bind(external_context_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(match purpose {
            "compression" => row.and_then(|item| item.1),
            _ => row.and_then(|item| item.0),
        })
    }

    async fn set_channel_model(
        &self,
        actor: &Actor,
        channel: &str,
        external_context_key: &str,
        purpose: &str,
        model_preset_id: Option<&str>,
    ) -> AppResult<()> {
        let workspace_id = actor.require_workspace()?.to_string();
        if !matches!(purpose, "chat" | "compression") {
            return Err(AppError::bad_request(
                "MODEL_PURPOSE_INVALID",
                "purpose must be chat or compression",
            ));
        }
        if let Some(preset_id) = model_preset_id {
            let preset = self.models.get_preset(preset_id).await?.ok_or_else(|| {
                AppError::not_found("MODEL_PRESET_NOT_FOUND", "model preset not found")
            })?;
            if preset.workspace_id != workspace_id && !actor.account.is_system_admin {
                return Err(AppError::forbidden(
                    "model preset is outside the current workspace",
                ));
            }
            if preset.purpose != purpose {
                return Err(AppError::bad_request(
                    "MODEL_PURPOSE_MISMATCH",
                    "model preset purpose does not match",
                ));
            }
        }
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO channel_contexts
                (account_id, channel, external_context_key, workspace_id, chat_model_preset_id, compression_model_preset_id, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(account_id, channel, external_context_key) DO UPDATE SET
                workspace_id = COALESCE(excluded.workspace_id, channel_contexts.workspace_id),
                chat_model_preset_id = CASE WHEN ? = 'chat' THEN excluded.chat_model_preset_id ELSE channel_contexts.chat_model_preset_id END,
                compression_model_preset_id = CASE WHEN ? = 'compression' THEN excluded.compression_model_preset_id ELSE channel_contexts.compression_model_preset_id END,
                updated_at = excluded.updated_at",
        )
        .bind(&actor.account.id)
        .bind(channel)
        .bind(external_context_key)
        .bind(&workspace_id)
        .bind(if purpose == "chat" {
            model_preset_id
        } else {
            None
        })
        .bind(if purpose == "compression" {
            model_preset_id
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

    async fn replay_channel_command(
        &self,
        actor: &Actor,
        channel: &str,
        external_context_key: &str,
        operation_kind: &str,
        client_turn_id: Option<&str>,
    ) -> AppResult<Option<ChatOutcome>> {
        let Some(client_turn_id) = client_turn_id else {
            return Ok(None);
        };
        let row: Option<(String, String, i64, String)> = sqlx::query_as(
            "SELECT operation_id, conversation_id, revision, result_json
             FROM channel_command_results
             WHERE actor_id = ? AND channel = ? AND external_context_key = ?
               AND operation_kind = ? AND client_turn_id = ?",
        )
        .bind(&actor.account.id)
        .bind(channel)
        .bind(external_context_key)
        .bind(operation_kind)
        .bind(client_turn_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some((operation_id, conversation_id, revision, result_json)) = row else {
            return Ok(None);
        };
        let conversation = self.get_conversation(&conversation_id).await?;
        self.require_workspace_access(actor, &conversation).await?;
        let result: Value = serde_json::from_str(&result_json).unwrap_or(json!({}));
        Ok(Some(ChatOutcome {
            operation_id,
            conversation: Some(conversation),
            reply_text: result
                .get("reply_text")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            revision,
            status: "completed".into(),
            in_progress: false,
        }))
    }

    async fn start_conversation(
        &self,
        actor: &Actor,
        character_id: &str,
        channel: &str,
        external_context_key: &str,
        client_turn_id: Option<&str>,
    ) -> AppResult<ChatOutcome> {
        if let Some(replay) = self
            .replay_channel_command(
                actor,
                channel,
                external_context_key,
                "start_conversation",
                client_turn_id,
            )
            .await?
        {
            if let Some(conversation) = replay.conversation.as_ref() {
                self.persist_channel_context(actor, channel, external_context_key, conversation)
                    .await?;
            }
            return Ok(replay);
        }
        let workspace_id = actor.require_workspace()?.to_string();
        let character = self
            .characters
            .get(character_id)
            .await?
            .ok_or_else(|| AppError::not_found("CHARACTER_NOT_FOUND", "character not found"))?;
        if character.workspace_id != workspace_id && !actor.account.is_system_admin {
            return Err(AppError::forbidden(
                "character is outside the current workspace",
            ));
        }
        let revision = self.characters.current_revision(character_id).await?;
        let settings = self.models.workspace_settings(&workspace_id).await?;
        let opening_source = if revision.normalized.first_mes.trim().is_empty() {
            format!(
                "你好，我是{}。我们开始新的故事吧。",
                revision.normalized.name
            )
        } else {
            revision.normalized.first_mes.clone()
        };
        let opening = crate::modules::chat::prompt::substitute_placeholders(
            &opening_source,
            &revision.normalized.name,
            &settings.prompt_user_name,
        );
        let operation_id = new_id();
        let conversation_id = new_id();
        let turn_id = new_id();
        let message_id = new_id();
        let now = now_rfc3339();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO conversations
                (id, workspace_id, character_id, pinned_character_revision_id, title, prompt_profile, revision, archived, created_at, updated_at)
             VALUES (?, ?, ?, NULL, ?, 'legacy_bridge_v1', 1, 0, ?, ?)",
        )
        .bind(&conversation_id)
        .bind(&workspace_id)
        .bind(character_id)
        .bind(&revision.normalized.name)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO turns (id, conversation_id, ordinal, status, created_by, active_assistant_message_id, created_at, updated_at)
             VALUES (?, ?, 1, 'complete', ?, ?, ?, ?)",
        )
        .bind(&turn_id)
        .bind(&conversation_id)
        .bind(&actor.account.id)
        .bind(&message_id)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO messages
                (id, conversation_id, turn_id, sequence, role, content_original, prompt_content, status, metadata_json, created_at)
             VALUES (?, ?, ?, 1, 'assistant', ?, NULL, 'active', ?, ?)",
        )
        .bind(&message_id)
        .bind(&conversation_id)
        .bind(&turn_id)
        .bind(&opening)
        .bind(json!({"name": revision.normalized.name, "send_date": now}).to_string())
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        if let Some(client_turn_id) = client_turn_id {
            sqlx::query(
                "INSERT INTO channel_command_results
                    (operation_id, actor_id, channel, external_context_key, operation_kind,
                     client_turn_id, conversation_id, revision, result_json, created_at)
                 VALUES (?, ?, ?, ?, 'start_conversation', ?, ?, 1, ?, ?)",
            )
            .bind(&operation_id)
            .bind(&actor.account.id)
            .bind(channel)
            .bind(external_context_key)
            .bind(client_turn_id)
            .bind(&conversation_id)
            .bind(json!({"reply_text": opening}).to_string())
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        let conversation = self.get_conversation(&conversation_id).await?;
        self.persist_channel_context(actor, channel, external_context_key, &conversation)
            .await?;
        Ok(ChatOutcome {
            operation_id,
            revision: conversation.revision,
            conversation: Some(conversation),
            reply_text: Some(opening),
            status: "completed".into(),
            in_progress: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_or_regenerate(
        &self,
        actor: &Actor,
        conversation_id: &str,
        text: Option<&str>,
        channel: &str,
        external_context_key: &str,
        client_turn_id: Option<&str>,
        expected_revision: Option<i64>,
        model_preset_id: Option<&str>,
        operation_kind: &str,
        progress: Option<&dyn ProgressSink>,
    ) -> AppResult<ChatOutcome> {
        if let Some(replay) = self
            .replay_or_start(
                actor,
                channel,
                external_context_key,
                operation_kind,
                client_turn_id,
            )
            .await?
        {
            return Ok(replay);
        }
        let _guard = self.locks.lock(conversation_id).await;
        let conversation = self.get_conversation(conversation_id).await?;
        self.require_workspace_access(actor, &conversation).await?;
        if let Some(expected) = expected_revision {
            if expected != conversation.revision {
                return Err(AppError::conflict(
                    "CONVERSATION_REVISION_CONFLICT",
                    "Conversation changed while the operation was running",
                ));
            }
        }

        let include_user = text.is_some() && operation_kind != "retry";

        let resolved_preset = match model_preset_id {
            Some(id) => Some(id.to_string()),
            None => {
                self.channel_model(actor, channel, external_context_key, "chat")
                    .await?
            }
        };
        let prepared = self
            .prepare_generation(
                actor,
                &conversation,
                text,
                channel,
                external_context_key,
                client_turn_id,
                resolved_preset.as_deref(),
                operation_kind,
                include_user,
            )
            .await?;

        if let Some(progress) = progress {
            progress
                .emit(ProgressEvent::Started {
                    operation_id: prepared.operation_id.clone(),
                    conversation_id: conversation.id.clone(),
                    revision: prepared.revision_after_a,
                })
                .await?;
        }

        let llm_result = self
            .llm
            .stream_chat(
                &prepared.provider,
                LlmRequest {
                    messages: prepared.messages.clone(),
                    stream: true,
                },
                progress,
            )
            .await;

        match llm_result {
            Ok(completion) => {
                let reply = normalize_assistant_reply(&prepared.character_name, &completion.text);
                self.complete_generation(
                    &prepared,
                    &reply,
                    GenerationStatus::Completed,
                    None,
                    progress,
                )
                .await
            }
            Err(err) => {
                let _ = self
                    .complete_generation(
                        &prepared,
                        "",
                        GenerationStatus::Failed,
                        Some(err.message.clone()),
                        progress,
                    )
                    .await;
                Err(err)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_generation(
        &self,
        actor: &Actor,
        conversation: &Conversation,
        text: Option<&str>,
        channel: &str,
        external_context_key: &str,
        client_turn_id: Option<&str>,
        model_preset_id: Option<&str>,
        operation_kind: &str,
        include_user: bool,
    ) -> AppResult<PreparedGeneration> {
        let settings = self
            .models
            .workspace_settings(&conversation.workspace_id)
            .await?;
        let revision = if let Some(pinned) = &conversation.pinned_character_revision_id {
            self.characters.get_revision(pinned).await?
        } else {
            self.characters
                .current_revision(&conversation.character_id)
                .await?
        };
        let mut provider = self
            .models
            .resolve_provider(
                &conversation.workspace_id,
                model_preset_id.or(conversation.default_model_preset_id.as_deref()),
                "chat",
            )
            .await?;
        if let Some(vault) = &self.vault {
            provider = self
                .models
                .hydrate_secrets(provider, vault.as_ref())
                .await?;
        }
        let mut history = load_active_messages(&self.pool, &conversation.id).await?;
        let supersedes_message_id = if operation_kind == "regenerate" {
            history
                .iter()
                .rposition(|message| message.role == MessageRole::Assistant)
                .map(|index| history.remove(index).id)
        } else {
            None
        };
        let recent = select_recent_messages(&history);
        let system_prompt = build_system_prompt(&revision.normalized, &settings.prompt_user_name);
        let messages = to_openai_messages(
            &system_prompt,
            &recent,
            &settings.prompt_user_name,
            if include_user { text } else { None },
        );

        let now = now_rfc3339();
        let operation_id = new_id();
        let turn_id = new_id();
        let user_message_id = new_id();
        let mut tx = self.pool.begin().await?;
        if let Some(message_id) = supersedes_message_id.as_deref() {
            sqlx::query(
                "UPDATE messages SET status = 'superseded' WHERE id = ? AND status = 'active'",
            )
            .bind(message_id)
            .execute(&mut *tx)
            .await?;
        }
        let next_ordinal: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(ordinal), 0) + 1 FROM turns WHERE conversation_id = ?",
        )
        .bind(&conversation.id)
        .fetch_one(&mut *tx)
        .await?;
        let next_sequence: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM messages WHERE conversation_id = ?",
        )
        .bind(&conversation.id)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO turns (id, conversation_id, ordinal, status, created_by, created_at, updated_at)
             VALUES (?, ?, ?, 'pending', ?, ?, ?)",
        )
        .bind(&turn_id)
        .bind(&conversation.id)
        .bind(next_ordinal)
        .bind(&actor.account.id)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        let mut sequence = next_sequence;
        if include_user {
            let user_text = text.unwrap_or_default();
            sqlx::query(
                "INSERT INTO messages
                    (id, conversation_id, turn_id, sequence, role, content_original, prompt_content, status, metadata_json, created_at)
                 VALUES (?, ?, ?, ?, 'user', ?, NULL, 'active', ?, ?)",
            )
            .bind(&user_message_id)
            .bind(&conversation.id)
            .bind(&turn_id)
            .bind(sequence)
            .bind(user_text)
            .bind(json!({"name": settings.prompt_user_name, "send_date": now}).to_string())
            .bind(&now)
            .execute(&mut *tx)
            .await?;
            sequence += 1;
        }
        sqlx::query(
            "INSERT INTO generation_runs
                (id, conversation_id, turn_id, actor_id, channel, external_context_key, operation_kind, client_turn_id, status,
                 effective_character_revision_id, provider_snapshot_json, request_id, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'started', ?, ?, ?, ?, ?)",
        )
        .bind(&operation_id)
        .bind(&conversation.id)
        .bind(&turn_id)
        .bind(&actor.account.id)
        .bind(channel)
        .bind(external_context_key)
        .bind(operation_kind)
        .bind(client_turn_id)
        .bind(&revision.id)
        .bind(serde_json::to_string(&json!({
            "provider_id": provider.id,
            "model": provider.model,
            "temperature": provider.temperature,
            "top_p": provider.top_p,
            "max_tokens": provider.max_tokens,
        })).unwrap_or_else(|_| "{}".into()))
        .bind(&operation_id)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        let revision_after_a = conversation.revision + 1;
        sqlx::query("UPDATE conversations SET revision = ?, updated_at = ? WHERE id = ?")
            .bind(revision_after_a)
            .bind(&now)
            .bind(&conversation.id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(PreparedGeneration {
            operation_id,
            conversation_id: conversation.id.clone(),
            turn_id,
            next_sequence: sequence,
            character_name: revision.normalized.name.clone(),
            provider,
            messages,
            revision_after_a,
            supersedes_message_id,
        })
    }

    async fn complete_generation(
        &self,
        prepared: &PreparedGeneration,
        reply: &str,
        status: GenerationStatus,
        error: Option<String>,
        progress: Option<&dyn ProgressSink>,
    ) -> AppResult<ChatOutcome> {
        let now = now_rfc3339();
        let assistant_id = new_id();
        let mut tx = self.pool.begin().await?;
        let current_revision: i64 =
            sqlx::query_scalar("SELECT revision FROM conversations WHERE id = ?")
                .bind(&prepared.conversation_id)
                .fetch_one(&mut *tx)
                .await?;
        if current_revision != prepared.revision_after_a {
            return Err(AppError::conflict(
                "CONVERSATION_REVISION_CONFLICT",
                "Conversation changed while the operation was running",
            ));
        }
        if status == GenerationStatus::Completed {
            sqlx::query(
                "INSERT INTO messages
                    (id, conversation_id, turn_id, sequence, role, content_original, prompt_content, status, supersedes_id, metadata_json, created_at)
                 VALUES (?, ?, ?, ?, 'assistant', ?, NULL, 'active', ?, ?, ?)",
            )
            .bind(&assistant_id)
            .bind(&prepared.conversation_id)
            .bind(&prepared.turn_id)
            .bind(prepared.next_sequence)
            .bind(reply)
            .bind(&prepared.supersedes_message_id)
            .bind(json!({"name": prepared.character_name, "send_date": now}).to_string())
            .bind(&now)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE turns SET status = 'complete', active_assistant_message_id = ?, updated_at = ? WHERE id = ?",
            )
            .bind(&assistant_id)
            .bind(&now)
            .bind(&prepared.turn_id)
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query("UPDATE turns SET status = 'failed', updated_at = ? WHERE id = ?")
                .bind(&now)
                .bind(&prepared.turn_id)
                .execute(&mut *tx)
                .await?;
            if let Some(message_id) = prepared.supersedes_message_id.as_deref() {
                sqlx::query(
                    "UPDATE messages SET status = 'active' WHERE id = ? AND status = 'superseded'",
                )
                .bind(message_id)
                .execute(&mut *tx)
                .await?;
            }
        }
        sqlx::query(
            "UPDATE generation_runs
             SET status = ?, error_message = ?, result_json = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(status.as_str())
        .bind(&error)
        .bind(json!({"reply_text": reply, "assistant_message_id": assistant_id}).to_string())
        .bind(&now)
        .bind(&prepared.operation_id)
        .execute(&mut *tx)
        .await?;
        let revision_after_b = prepared.revision_after_a + 1;
        sqlx::query("UPDATE conversations SET revision = ?, updated_at = ? WHERE id = ?")
            .bind(revision_after_b)
            .bind(&now)
            .bind(&prepared.conversation_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        if let Some(progress) = progress {
            if status == GenerationStatus::Completed {
                progress
                    .emit(ProgressEvent::Done {
                        reply_text: reply.to_string(),
                        conversation_revision: revision_after_b,
                    })
                    .await?;
            } else if let Some(message) = error.clone() {
                progress.emit(ProgressEvent::Error { message }).await?;
            }
        }
        let conversation = self.get_conversation(&prepared.conversation_id).await?;
        Ok(ChatOutcome {
            operation_id: prepared.operation_id.clone(),
            revision: conversation.revision,
            conversation: Some(conversation),
            reply_text: if status == GenerationStatus::Completed {
                Some(reply.to_string())
            } else {
                None
            },
            status: status.as_str().to_string(),
            in_progress: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn undo(
        &self,
        actor: &Actor,
        conversation_id: &str,
        channel: &str,
        external_context_key: &str,
        client_turn_id: Option<&str>,
        expected_revision: Option<i64>,
    ) -> AppResult<ChatOutcome> {
        if let Some(replay) = self
            .replay_channel_command(
                actor,
                channel,
                external_context_key,
                "undo_last_turn",
                client_turn_id,
            )
            .await?
        {
            return Ok(replay);
        }
        let _guard = self.locks.lock(conversation_id).await;
        let conversation = self.get_conversation(conversation_id).await?;
        self.require_workspace_access(actor, &conversation).await?;
        if let Some(expected) = expected_revision {
            if expected != conversation.revision {
                return Err(AppError::conflict(
                    "CONVERSATION_REVISION_CONFLICT",
                    "Conversation changed while the operation was running",
                ));
            }
        }
        let turn = last_undoable_turn(&self.pool, conversation_id).await?;
        let operation_id = new_id();
        let now = now_rfc3339();
        let mut tx = self.pool.begin().await?;
        sqlx::query("UPDATE turns SET status = 'revoked', updated_at = ? WHERE id = ?")
            .bind(&now)
            .bind(&turn.id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE messages SET status = 'revoked' WHERE turn_id = ? AND status = 'active'",
        )
        .bind(&turn.id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE conversations SET revision = revision + 1, updated_at = ? WHERE id = ?",
        )
        .bind(&now)
        .bind(conversation_id)
        .execute(&mut *tx)
        .await?;
        let revision = conversation.revision + 1;
        if let Some(client_turn_id) = client_turn_id {
            sqlx::query(
                "INSERT INTO channel_command_results
                    (operation_id, actor_id, channel, external_context_key, operation_kind,
                     client_turn_id, conversation_id, revision, result_json, created_at)
                 VALUES (?, ?, ?, ?, 'undo_last_turn', ?, ?, ?, '{}', ?)",
            )
            .bind(&operation_id)
            .bind(&actor.account.id)
            .bind(channel)
            .bind(external_context_key)
            .bind(client_turn_id)
            .bind(conversation_id)
            .bind(revision)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        let conversation = self.get_conversation(conversation_id).await?;
        Ok(ChatOutcome {
            operation_id,
            revision: conversation.revision,
            conversation: Some(conversation),
            reply_text: None,
            status: "completed".into(),
            in_progress: false,
        })
    }

    async fn compress(
        &self,
        actor: &Actor,
        conversation_id: &str,
        keep_recent: usize,
        model_preset_id: Option<&str>,
        progress: Option<&dyn ProgressSink>,
    ) -> AppResult<ChatOutcome> {
        let _guard = self.locks.lock(conversation_id).await;
        let conversation = self.get_conversation(conversation_id).await?;
        self.require_workspace_access(actor, &conversation).await?;
        let messages = load_active_messages(&self.pool, conversation_id).await?;
        let keep_from = if keep_recent == 0 {
            i64::MAX
        } else {
            messages
                .iter()
                .rev()
                .filter(|message| message.role != MessageRole::System)
                .take(keep_recent)
                .last()
                .map(|message| message.sequence)
                .unwrap_or(i64::MAX)
        };
        let mut targets = Vec::new();
        for message in &messages {
            if message.sequence >= keep_from {
                continue;
            }
            if message.role != MessageRole::Assistant {
                continue;
            }
            if message.prompt_content.is_some() {
                continue;
            }
            if message.content_original.starts_with("[CompressionDigest]") {
                continue;
            }
            targets.push(message.clone());
        }
        if let Some(progress) = progress {
            progress
                .emit(ProgressEvent::Progress {
                    completed: 0,
                    total: targets.len() as u32,
                })
                .await?;
        }
        if targets.is_empty() {
            return Ok(ChatOutcome {
                operation_id: new_id(),
                revision: conversation.revision,
                conversation: Some(conversation),
                reply_text: None,
                status: "completed".into(),
                in_progress: false,
            });
        }
        let mut provider = self
            .models
            .resolve_provider(&conversation.workspace_id, model_preset_id, "compression")
            .await?;
        if let Some(vault) = &self.vault {
            provider = self
                .models
                .hydrate_secrets(provider, vault.as_ref())
                .await?;
        }
        let mut compressed = 0u32;
        for (index, message) in targets.iter().enumerate() {
            let prev_user = messages
                .iter()
                .rev()
                .find(|candidate| {
                    candidate.sequence < message.sequence && candidate.role == MessageRole::User
                })
                .map(|candidate| candidate.content_original.clone());
            let payload = if let Some(prev) = prev_user {
                format!(
                    "角色名：{}\n（仅供参考的上文用户消息）：{}\n\n需要压缩的 AI 回复原文：\n{}",
                    conversation.title,
                    prev.chars().take(800).collect::<String>(),
                    message.content_original
                )
            } else {
                format!(
                    "角色名：{}\n\n需要压缩的 AI 回复原文：\n{}",
                    conversation.title, message.content_original
                )
            };
            let result = self
                .llm
                .stream_chat(
                    &provider,
                    LlmRequest {
                        messages: vec![
                            crate::seams::llm_gateway::LlmMessage {
                                role: "system".into(),
                                content: COMPRESSION_SYSTEM_PROMPT.into(),
                                name: None,
                            },
                            crate::seams::llm_gateway::LlmMessage {
                                role: "user".into(),
                                content: payload,
                                name: None,
                            },
                        ],
                        stream: false,
                    },
                    None,
                )
                .await?;
            let sanitized = sanitize_compression(&result.text);
            if !sanitized.is_empty() && sanitized.len() < message.content_original.len() {
                sqlx::query("UPDATE messages SET prompt_content = ? WHERE id = ?")
                    .bind(&sanitized)
                    .bind(&message.id)
                    .execute(&self.pool)
                    .await?;
                compressed += 1;
            }
            if let Some(progress) = progress {
                progress
                    .emit(ProgressEvent::Progress {
                        completed: (index + 1) as u32,
                        total: targets.len() as u32,
                    })
                    .await?;
            }
        }
        let now = now_rfc3339();
        sqlx::query(
            "UPDATE conversations SET revision = revision + 1, updated_at = ? WHERE id = ?",
        )
        .bind(&now)
        .bind(conversation_id)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "INSERT INTO compression_runs
                (id, conversation_id, model_snapshot_json, selected_message_ids_json, status, created_at, updated_at)
             VALUES (?, ?, ?, ?, 'completed', ?, ?)",
        )
        .bind(new_id())
        .bind(conversation_id)
        .bind(json!({"model": provider.model}).to_string())
        .bind(json!(targets.iter().map(|item| &item.id).collect::<Vec<_>>()).to_string())
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        let conversation = self.get_conversation(conversation_id).await?;
        let _ = compressed;
        Ok(ChatOutcome {
            operation_id: new_id(),
            revision: conversation.revision,
            conversation: Some(conversation),
            reply_text: None,
            status: "completed".into(),
            in_progress: false,
        })
    }
}

struct PreparedGeneration {
    operation_id: String,
    conversation_id: String,
    turn_id: String,
    next_sequence: i64,
    character_name: String,
    provider: crate::seams::llm_gateway::ResolvedProvider,
    messages: Vec<crate::seams::llm_gateway::LlmMessage>,
    revision_after_a: i64,
    supersedes_message_id: Option<String>,
}

#[async_trait]
impl ChatEngine for SqliteChatEngine {
    async fn execute(
        &self,
        actor: &Actor,
        command: ChatCommand,
        progress: Option<&dyn ProgressSink>,
    ) -> AppResult<ChatOutcome> {
        match command {
            ChatCommand::StartConversation {
                character_id,
                channel,
                external_context_key,
                client_turn_id,
            } => {
                self.start_conversation(
                    actor,
                    &character_id,
                    &channel,
                    &external_context_key,
                    client_turn_id.as_deref(),
                )
                .await
            }
            ChatCommand::SelectCharacter {
                character_id,
                channel,
                external_context_key,
            } => {
                self.select_character_context(
                    actor,
                    &character_id,
                    &channel,
                    &external_context_key,
                )
                .await?;
                Ok(ChatOutcome {
                    operation_id: new_id(),
                    revision: 0,
                    conversation: None,
                    reply_text: None,
                    status: "completed".into(),
                    in_progress: false,
                })
            }
            ChatCommand::SelectConversation {
                conversation_id,
                channel,
                external_context_key,
            } => {
                let conversation = self.get_conversation(&conversation_id).await?;
                self.require_workspace_access(actor, &conversation).await?;
                self.persist_channel_context(actor, &channel, &external_context_key, &conversation)
                    .await?;
                Ok(ChatOutcome {
                    operation_id: new_id(),
                    revision: conversation.revision,
                    conversation: Some(conversation),
                    reply_text: None,
                    status: "completed".into(),
                    in_progress: false,
                })
            }
            ChatCommand::SendMessage {
                conversation_id,
                text,
                channel,
                external_context_key,
                client_turn_id,
                expected_revision,
                model_preset_id,
            } => {
                if text.trim().is_empty() {
                    return Err(AppError::bad_request(
                        "EMPTY_MESSAGE",
                        "message text is required",
                    ));
                }
                self.send_or_regenerate(
                    actor,
                    &conversation_id,
                    Some(&text),
                    &channel,
                    &external_context_key,
                    client_turn_id.as_deref(),
                    expected_revision,
                    model_preset_id.as_deref(),
                    "send",
                    progress,
                )
                .await
            }
            ChatCommand::UndoLastTurn {
                conversation_id,
                channel,
                external_context_key,
                client_turn_id,
                expected_revision,
            } => {
                self.undo(
                    actor,
                    &conversation_id,
                    &channel,
                    &external_context_key,
                    client_turn_id.as_deref(),
                    expected_revision,
                )
                .await
            }
            ChatCommand::RegenerateReply {
                conversation_id,
                channel,
                external_context_key,
                client_turn_id,
                expected_revision,
                model_preset_id,
            } => {
                self.send_or_regenerate(
                    actor,
                    &conversation_id,
                    None,
                    &channel,
                    &external_context_key,
                    client_turn_id.as_deref(),
                    expected_revision,
                    model_preset_id.as_deref(),
                    "regenerate",
                    progress,
                )
                .await
            }
            ChatCommand::RetryGeneration {
                conversation_id,
                operation_id,
                channel,
                external_context_key,
                client_turn_id,
            } => {
                let run = load_generation(&self.pool, &operation_id)
                    .await?
                    .ok_or_else(|| {
                        AppError::not_found("OPERATION_NOT_FOUND", "operation not found")
                    })?;
                if run.conversation_id != conversation_id {
                    return Err(AppError::bad_request(
                        "OPERATION_MISMATCH",
                        "operation does not belong to conversation",
                    ));
                }
                if !matches!(
                    run.status,
                    GenerationStatus::Failed | GenerationStatus::Interrupted
                ) {
                    return Err(AppError::conflict(
                        "OPERATION_NOT_RETRYABLE",
                        "operation is not retryable",
                    ));
                }
                let last_user = last_active_user(&self.pool, &conversation_id).await?;
                let text = last_user.map(|message| message.content_original);
                self.send_or_regenerate(
                    actor,
                    &conversation_id,
                    text.as_deref(),
                    &channel,
                    &external_context_key,
                    client_turn_id.as_deref(),
                    None,
                    None,
                    "retry",
                    progress,
                )
                .await
            }
            ChatCommand::CompressHistory {
                conversation_id,
                keep_recent,
                model_preset_id,
                channel,
                external_context_key,
                ..
            } => {
                let preset = if model_preset_id.is_some() {
                    model_preset_id
                } else {
                    self.channel_model(actor, &channel, &external_context_key, "compression")
                        .await?
                };
                self.compress(
                    actor,
                    &conversation_id,
                    keep_recent,
                    preset.as_deref(),
                    progress,
                )
                .await
            }
            ChatCommand::SetChannelModel {
                channel,
                external_context_key,
                purpose,
                model_preset_id,
            } => {
                self.set_channel_model(
                    actor,
                    &channel,
                    &external_context_key,
                    &purpose,
                    model_preset_id.as_deref(),
                )
                .await?;
                Ok(ChatOutcome {
                    operation_id: new_id(),
                    conversation: None,
                    reply_text: None,
                    revision: 0,
                    status: "completed".into(),
                    in_progress: false,
                })
            }
        }
    }

    async fn query(&self, actor: &Actor, query: ChatQuery) -> AppResult<ChatView> {
        match query {
            ChatQuery::ListConversations { workspace_id } => {
                let workspace_id = workspace_id
                    .or_else(|| actor.workspace_id.clone())
                    .ok_or_else(|| {
                        AppError::bad_request("WORKSPACE_REQUIRED", "workspace is required")
                    })?;
                let rows = sqlx::query_as::<_, ConversationRow>(
                    "SELECT id, workspace_id, character_id, pinned_character_revision_id, title, prompt_profile, revision, default_model_preset_id, archived, legacy_locator
                     FROM conversations WHERE workspace_id = ? AND archived = 0 ORDER BY updated_at DESC",
                )
                .bind(&workspace_id)
                .fetch_all(&self.pool)
                .await?;
                Ok(ChatView {
                    conversations: rows
                        .into_iter()
                        .map(ConversationRow::into_conversation)
                        .collect(),
                    conversation: None,
                    history: None,
                    operation: None,
                    channel_character_id: None,
                    chat_model_preset_id: None,
                    compression_model_preset_id: None,
                })
            }
            ChatQuery::GetConversation { conversation_id } => {
                let conversation = self.get_conversation(&conversation_id).await?;
                self.require_workspace_access(actor, &conversation).await?;
                Ok(ChatView {
                    conversations: vec![conversation.clone()],
                    conversation: Some(conversation),
                    history: None,
                    operation: None,
                    channel_character_id: None,
                    chat_model_preset_id: None,
                    compression_model_preset_id: None,
                })
            }
            ChatQuery::GetHistory {
                conversation_id,
                known_revision,
            } => {
                let conversation = self.get_conversation(&conversation_id).await?;
                self.require_workspace_access(actor, &conversation).await?;
                let messages = load_active_messages(&self.pool, &conversation_id).await?;
                let items = history_items(&messages);
                let mode = if known_revision == Some(conversation.revision) {
                    "unchanged"
                } else {
                    "full"
                };
                Ok(ChatView {
                    conversations: vec![conversation.clone()],
                    conversation: Some(conversation.clone()),
                    history: Some(HistoryView {
                        conversation_id,
                        revision: conversation.revision,
                        mode: mode.into(),
                        items: if mode == "unchanged" {
                            Vec::new()
                        } else {
                            items
                        },
                    }),
                    operation: None,
                    channel_character_id: None,
                    chat_model_preset_id: None,
                    compression_model_preset_id: None,
                })
            }
            ChatQuery::GetChannelContext {
                channel,
                external_context_key,
            } => {
                let row: Option<(
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                )> = sqlx::query_as(
                    "SELECT conversation_id, character_id, chat_model_preset_id, compression_model_preset_id FROM channel_contexts WHERE account_id = ? AND channel = ? AND external_context_key = ?",
                )
                .bind(&actor.account.id)
                .bind(&channel)
                .bind(&external_context_key)
                .fetch_optional(&self.pool)
                .await?;
                let (
                    conversation,
                    channel_character_id,
                    chat_model_preset_id,
                    compression_model_preset_id,
                ) = if let Some((id, character_id, chat_model, compression_model)) = row {
                    let conversation = if let Some(id) = id {
                        load_conversation(&self.pool, &id).await?
                    } else {
                        None
                    };
                    (conversation, character_id, chat_model, compression_model)
                } else {
                    (None, None, None, None)
                };
                Ok(ChatView {
                    conversations: conversation.clone().into_iter().collect(),
                    conversation,
                    history: None,
                    operation: None,
                    channel_character_id,
                    chat_model_preset_id,
                    compression_model_preset_id,
                })
            }
            ChatQuery::GetOperation { operation_id } => Ok(ChatView {
                conversations: Vec::new(),
                conversation: None,
                history: None,
                operation: load_generation(&self.pool, &operation_id).await?,
                channel_character_id: None,
                chat_model_preset_id: None,
                compression_model_preset_id: None,
            }),
        }
    }
}

#[derive(sqlx::FromRow)]
struct ConversationRow {
    id: String,
    workspace_id: String,
    character_id: String,
    pinned_character_revision_id: Option<String>,
    title: String,
    prompt_profile: String,
    revision: i64,
    default_model_preset_id: Option<String>,
    archived: i64,
    legacy_locator: Option<String>,
}

impl ConversationRow {
    fn into_conversation(self) -> Conversation {
        Conversation {
            id: self.id,
            workspace_id: self.workspace_id,
            character_id: self.character_id,
            pinned_character_revision_id: self.pinned_character_revision_id,
            title: self.title,
            prompt_profile: self.prompt_profile,
            revision: self.revision,
            default_model_preset_id: self.default_model_preset_id,
            archived: self.archived != 0,
            legacy_locator: self.legacy_locator,
        }
    }
}

#[derive(sqlx::FromRow)]
struct MessageRow {
    id: String,
    conversation_id: String,
    turn_id: Option<String>,
    sequence: i64,
    role: String,
    content_original: String,
    prompt_content: Option<String>,
    status: String,
    supersedes_id: Option<String>,
    metadata_json: String,
    source_raw_json: Option<String>,
}

impl MessageRow {
    fn into_message(self) -> Message {
        Message {
            id: self.id,
            conversation_id: self.conversation_id,
            turn_id: self.turn_id,
            sequence: self.sequence,
            role: MessageRole::parse(&self.role).unwrap_or(MessageRole::Assistant),
            content_original: self.content_original,
            prompt_content: self.prompt_content,
            status: match self.status.as_str() {
                "revoked" => MessageStatus::Revoked,
                "superseded" => MessageStatus::Superseded,
                _ => MessageStatus::Active,
            },
            supersedes_id: self.supersedes_id,
            metadata: serde_json::from_str(&self.metadata_json).unwrap_or(json!({})),
            source_raw_json: self
                .source_raw_json
                .and_then(|raw| serde_json::from_str(&raw).ok()),
        }
    }
}

#[derive(sqlx::FromRow)]
struct GenerationRow {
    id: String,
    conversation_id: String,
    turn_id: Option<String>,
    actor_id: String,
    channel: String,
    external_context_key: String,
    operation_kind: String,
    client_turn_id: Option<String>,
    status: String,
    effective_character_revision_id: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
    partial_content: Option<String>,
    result_json: Option<String>,
}

impl GenerationRow {
    fn into_run(self) -> GenerationRun {
        GenerationRun {
            id: self.id,
            conversation_id: self.conversation_id,
            turn_id: self.turn_id,
            actor_id: self.actor_id,
            channel: self.channel,
            external_context_key: self.external_context_key,
            operation_kind: self.operation_kind,
            client_turn_id: self.client_turn_id,
            status: GenerationStatus::parse(&self.status).unwrap_or(GenerationStatus::Failed),
            effective_character_revision_id: self.effective_character_revision_id,
            error_code: self.error_code,
            error_message: self.error_message,
            partial_content: self.partial_content,
            result_json: self
                .result_json
                .and_then(|raw| serde_json::from_str(&raw).ok()),
        }
    }
}

async fn load_conversation(pool: &SqlitePool, id: &str) -> AppResult<Option<Conversation>> {
    let row = sqlx::query_as::<_, ConversationRow>(
        "SELECT id, workspace_id, character_id, pinned_character_revision_id, title, prompt_profile, revision, default_model_preset_id, archived, legacy_locator
         FROM conversations WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(ConversationRow::into_conversation))
}

async fn load_active_messages(pool: &SqlitePool, conversation_id: &str) -> AppResult<Vec<Message>> {
    let rows = sqlx::query_as::<_, MessageRow>(
        "SELECT id, conversation_id, turn_id, sequence, role, content_original, prompt_content, status, supersedes_id, metadata_json, source_raw_json
         FROM messages WHERE conversation_id = ? AND status = 'active' ORDER BY sequence",
    )
    .bind(conversation_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(MessageRow::into_message).collect())
}

async fn load_generation(pool: &SqlitePool, id: &str) -> AppResult<Option<GenerationRun>> {
    let row = sqlx::query_as::<_, GenerationRow>(
        "SELECT id, conversation_id, turn_id, actor_id, channel, external_context_key, operation_kind, client_turn_id, status,
                effective_character_revision_id, error_code, error_message, partial_content, result_json
         FROM generation_runs WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(GenerationRow::into_run))
}

async fn last_active_user(pool: &SqlitePool, conversation_id: &str) -> AppResult<Option<Message>> {
    let row = sqlx::query_as::<_, MessageRow>(
        "SELECT id, conversation_id, turn_id, sequence, role, content_original, prompt_content, status, supersedes_id, metadata_json, source_raw_json
         FROM messages WHERE conversation_id = ? AND status = 'active' AND role = 'user' ORDER BY sequence DESC LIMIT 1",
    )
    .bind(conversation_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(MessageRow::into_message))
}

struct UndoTurn {
    id: String,
}

async fn last_undoable_turn(pool: &SqlitePool, conversation_id: &str) -> AppResult<UndoTurn> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM turns WHERE conversation_id = ? AND status IN ('complete', 'failed', 'pending') ORDER BY ordinal DESC LIMIT 1",
    )
    .bind(conversation_id)
    .fetch_optional(pool)
    .await?;
    row.map(|value| UndoTurn { id: value.0 })
        .ok_or_else(|| AppError::bad_request("NO_LAST_TURN", "当前会话没有可删除的尾部对话。"))
}
