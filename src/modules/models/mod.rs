use serde_json::json;
use sqlx::SqlitePool;

use crate::clock::now_rfc3339;
use crate::domain::identity::Actor;
use crate::domain::model::{ModelPreset, ProviderProfile, WorkspaceSettings};
use crate::error::{AppError, AppResult};
use crate::ids::new_id;
use crate::seams::llm_gateway::ResolvedProvider;
use crate::seams::secret_vault::SecretVault;

#[derive(Clone)]
pub struct ModelModule {
    pool: SqlitePool,
}

impl ModelModule {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn workspace_settings(&self, workspace_id: &str) -> AppResult<WorkspaceSettings> {
        let row = sqlx::query_as::<_, SettingsRow>(
            "SELECT workspace_id, prompt_user_name, default_prompt_profile, default_chat_model_preset_id, default_compression_model_preset_id
             FROM workspace_settings WHERE workspace_id = ?",
        )
        .bind(workspace_id)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = row {
            return Ok(row.into_settings());
        }
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO workspace_settings (workspace_id, prompt_user_name, default_prompt_profile, updated_at)
             VALUES (?, 'User', 'legacy_bridge_v1', ?)",
        )
        .bind(workspace_id)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(WorkspaceSettings {
            workspace_id: workspace_id.to_string(),
            prompt_user_name: "User".into(),
            default_prompt_profile: "legacy_bridge_v1".into(),
            default_chat_model_preset_id: None,
            default_compression_model_preset_id: None,
        })
    }

    pub async fn update_prompt_user_name(&self, workspace_id: &str, name: &str) -> AppResult<()> {
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO workspace_settings (workspace_id, prompt_user_name, default_prompt_profile, updated_at)
             VALUES (?, ?, 'legacy_bridge_v1', ?)
             ON CONFLICT(workspace_id) DO UPDATE SET prompt_user_name = excluded.prompt_user_name, updated_at = excluded.updated_at",
        )
        .bind(workspace_id)
        .bind(name)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_provider(
        &self,
        actor: &Actor,
        name: &str,
        base_url: &str,
        api_key_secret_id: Option<&str>,
        custom_prompt_post_processing: &str,
    ) -> AppResult<ProviderProfile> {
        let workspace_id = actor.require_workspace()?.to_string();
        let id = new_id();
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO provider_profiles
                (id, workspace_id, name, kind, base_url, api_key_secret_id, custom_prompt_post_processing, enabled, created_at, updated_at)
             VALUES (?, ?, ?, 'openai_compatible', ?, ?, ?, 1, ?, ?)",
        )
        .bind(&id)
        .bind(&workspace_id)
        .bind(name)
        .bind(base_url)
        .bind(api_key_secret_id)
        .bind(custom_prompt_post_processing)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        self.get_provider(&id)
            .await?
            .ok_or_else(|| AppError::internal("provider missing after insert"))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_preset(
        &self,
        provider_id: &str,
        model_id: &str,
        label: &str,
        purpose: &str,
        temperature: f64,
        top_p: f64,
        max_tokens: i64,
    ) -> AppResult<ModelPreset> {
        if !matches!(purpose, "chat" | "compression") {
            return Err(AppError::bad_request(
                "MODEL_PURPOSE_INVALID",
                "purpose must be chat or compression",
            ));
        }
        if !temperature.is_finite()
            || !top_p.is_finite()
            || temperature < 0.0
            || top_p <= 0.0
            || max_tokens <= 0
        {
            return Err(AppError::bad_request(
                "MODEL_PARAMETERS_INVALID",
                "temperature, top_p and max_tokens are invalid",
            ));
        }
        let provider = self
            .get_provider(provider_id)
            .await?
            .ok_or_else(|| AppError::not_found("PROVIDER_NOT_FOUND", "provider not found"))?;
        let id = new_id();
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO model_presets
                (id, workspace_id, provider_profile_id, model_id, label, temperature, top_p, max_tokens, purpose, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(&provider.workspace_id)
        .bind(provider_id)
        .bind(model_id)
        .bind(label)
        .bind(temperature)
        .bind(top_p)
        .bind(max_tokens)
        .bind(purpose)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "UPDATE workspace_settings SET default_chat_model_preset_id = CASE WHEN ? = 'chat' THEN ? ELSE default_chat_model_preset_id END,
                 default_compression_model_preset_id = CASE WHEN ? = 'compression' THEN ? ELSE default_compression_model_preset_id END,
                 updated_at = ?
             WHERE workspace_id = ?",
        )
        .bind(purpose)
        .bind(&id)
        .bind(purpose)
        .bind(&id)
        .bind(&now)
        .bind(&provider.workspace_id)
        .execute(&self.pool)
        .await?;
        self.get_preset(&id)
            .await?
            .ok_or_else(|| AppError::internal("preset missing after insert"))
    }

    pub async fn list_providers(&self, workspace_id: &str) -> AppResult<Vec<ProviderProfile>> {
        let rows = sqlx::query_as::<_, ProviderRow>(
            "SELECT id, workspace_id, name, kind, base_url, api_key_secret_id, custom_headers_secret_id, custom_prompt_post_processing, enabled
             FROM provider_profiles WHERE workspace_id = ? ORDER BY created_at",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(ProviderRow::into_provider).collect())
    }

    pub async fn list_presets(&self, workspace_id: &str) -> AppResult<Vec<ModelPreset>> {
        let rows = sqlx::query_as::<_, PresetRow>(
            "SELECT id, workspace_id, provider_profile_id, model_id, label, temperature, top_p, max_tokens, hard_timeout_ms, idle_timeout_ms, purpose
             FROM model_presets WHERE workspace_id = ? ORDER BY created_at",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(PresetRow::into_preset).collect())
    }

    pub async fn get_provider(&self, id: &str) -> AppResult<Option<ProviderProfile>> {
        let row = sqlx::query_as::<_, ProviderRow>(
            "SELECT id, workspace_id, name, kind, base_url, api_key_secret_id, custom_headers_secret_id, custom_prompt_post_processing, enabled
             FROM provider_profiles WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(ProviderRow::into_provider))
    }

    pub async fn get_preset(&self, id: &str) -> AppResult<Option<ModelPreset>> {
        let row = sqlx::query_as::<_, PresetRow>(
            "SELECT id, workspace_id, provider_profile_id, model_id, label, temperature, top_p, max_tokens, hard_timeout_ms, idle_timeout_ms, purpose
             FROM model_presets WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(PresetRow::into_preset))
    }

    pub async fn resolve_provider(
        &self,
        workspace_id: &str,
        preset_id: Option<&str>,
        purpose: &str,
    ) -> AppResult<ResolvedProvider> {
        let settings = self.workspace_settings(workspace_id).await?;
        let preset_id = preset_id
            .map(ToOwned::to_owned)
            .or_else(|| {
                if purpose == "compression" {
                    settings.default_compression_model_preset_id.clone()
                } else {
                    settings.default_chat_model_preset_id.clone()
                }
            })
            .ok_or_else(|| {
                AppError::bad_request("MODEL_PRESET_MISSING", "no model preset configured")
            })?;
        let preset = self.get_preset(&preset_id).await?.ok_or_else(|| {
            AppError::not_found("MODEL_PRESET_NOT_FOUND", "model preset not found")
        })?;
        if preset.workspace_id != workspace_id {
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
        let provider = self
            .get_provider(&preset.provider_profile_id)
            .await?
            .ok_or_else(|| AppError::not_found("PROVIDER_NOT_FOUND", "provider not found"))?;
        if provider.workspace_id != workspace_id {
            return Err(AppError::forbidden(
                "provider is outside the current workspace",
            ));
        }
        Ok(ResolvedProvider {
            id: provider.id,
            base_url: provider.base_url,
            api_key: None,
            custom_headers: Vec::new(),
            custom_prompt_post_processing: provider.custom_prompt_post_processing,
            model: preset.model_id,
            temperature: preset.temperature,
            top_p: preset.top_p,
            max_tokens: preset.max_tokens,
            hard_timeout_ms: preset.hard_timeout_ms as u64,
            idle_timeout_ms: preset.idle_timeout_ms as u64,
        })
    }

    pub async fn hydrate_secrets(
        &self,
        mut provider: ResolvedProvider,
        vault: &dyn SecretVault,
    ) -> AppResult<ResolvedProvider> {
        let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT api_key_secret_id, custom_headers_secret_id FROM provider_profiles WHERE id = ?",
        )
        .bind(&provider.id)
        .fetch_optional(&self.pool)
        .await?;
        if let Some((api_key_secret_id, headers_secret_id)) = row {
            if let Some(secret_id) = api_key_secret_id {
                let bytes = vault.get(&secret_id).await?;
                provider.api_key = Some(String::from_utf8_lossy(&bytes).to_string());
            }
            if let Some(secret_id) = headers_secret_id {
                let bytes = vault.get(&secret_id).await?;
                if let Ok(map) =
                    serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&bytes)
                {
                    provider.custom_headers = map
                        .into_iter()
                        .filter_map(|(k, v)| v.as_str().map(|value| (k, value.to_string())))
                        .collect();
                }
            }
        }
        let _ = json!({});
        Ok(provider)
    }
}

#[derive(sqlx::FromRow)]
struct SettingsRow {
    workspace_id: String,
    prompt_user_name: String,
    default_prompt_profile: String,
    default_chat_model_preset_id: Option<String>,
    default_compression_model_preset_id: Option<String>,
}

impl SettingsRow {
    fn into_settings(self) -> WorkspaceSettings {
        WorkspaceSettings {
            workspace_id: self.workspace_id,
            prompt_user_name: self.prompt_user_name,
            default_prompt_profile: self.default_prompt_profile,
            default_chat_model_preset_id: self.default_chat_model_preset_id,
            default_compression_model_preset_id: self.default_compression_model_preset_id,
        }
    }
}

#[derive(sqlx::FromRow)]
struct ProviderRow {
    id: String,
    workspace_id: String,
    name: String,
    kind: String,
    base_url: String,
    api_key_secret_id: Option<String>,
    custom_headers_secret_id: Option<String>,
    custom_prompt_post_processing: String,
    enabled: i64,
}

impl ProviderRow {
    fn into_provider(self) -> ProviderProfile {
        ProviderProfile {
            id: self.id,
            workspace_id: self.workspace_id,
            name: self.name,
            kind: self.kind,
            base_url: self.base_url,
            api_key_secret_id: self.api_key_secret_id,
            custom_headers_secret_id: self.custom_headers_secret_id,
            custom_prompt_post_processing: self.custom_prompt_post_processing,
            enabled: self.enabled != 0,
        }
    }
}

#[derive(sqlx::FromRow)]
struct PresetRow {
    id: String,
    workspace_id: String,
    provider_profile_id: String,
    model_id: String,
    label: String,
    temperature: f64,
    top_p: f64,
    max_tokens: i64,
    hard_timeout_ms: i64,
    idle_timeout_ms: i64,
    purpose: String,
}

impl PresetRow {
    fn into_preset(self) -> ModelPreset {
        ModelPreset {
            id: self.id,
            workspace_id: self.workspace_id,
            provider_profile_id: self.provider_profile_id,
            model_id: self.model_id,
            label: self.label,
            temperature: self.temperature,
            top_p: self.top_p,
            max_tokens: self.max_tokens,
            hard_timeout_ms: self.hard_timeout_ms,
            idle_timeout_ms: self.idle_timeout_ms,
            purpose: self.purpose,
        }
    }
}
