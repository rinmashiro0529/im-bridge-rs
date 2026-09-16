use std::path::{Path, PathBuf};

use serde_json::Value;
use sqlx::SqlitePool;

use crate::clock::now_rfc3339;
use crate::domain::character::{Character, CharacterRevision, NormalizedCardFields};
use crate::domain::identity::Actor;
use crate::error::{AppError, AppResult};
use crate::ids::new_id;
use crate::modules::characters::card_png::{
    encode_png_with_card, parse_character_bytes, sha256_hex, ParsedCard,
};

#[derive(Clone)]
pub struct CharacterModule {
    pool: SqlitePool,
    assets_dir: PathBuf,
}

impl CharacterModule {
    pub fn new(pool: SqlitePool, assets_dir: PathBuf) -> Self {
        Self { pool, assets_dir }
    }

    pub async fn import_bytes(
        &self,
        actor: &Actor,
        filename: &str,
        bytes: &[u8],
    ) -> AppResult<Character> {
        let workspace_id = actor.require_workspace()?.to_string();
        let parsed = parse_character_bytes(bytes, filename)?;
        let normalized = NormalizedCardFields::from_raw(&parsed.raw);
        let now = now_rfc3339();
        let asset_id = self.store_asset(&workspace_id, filename, bytes).await?;
        let character_id = new_id();
        let revision_id = new_id();
        sqlx::query(
            "INSERT INTO characters (id, workspace_id, display_name, current_revision_id, archived, created_at, updated_at)
             VALUES (?, ?, ?, ?, 0, ?, ?)",
        )
        .bind(&character_id)
        .bind(&workspace_id)
        .bind(&normalized.name)
        .bind(&revision_id)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        self.insert_revision(
            &character_id,
            &revision_id,
            &parsed,
            &normalized,
            Some(&asset_id),
            &now,
        )
        .await?;
        self.get(&character_id)
            .await?
            .ok_or_else(|| AppError::internal("imported character missing"))
    }

    pub async fn import_existing_or_revision(
        &self,
        workspace_id: &str,
        filename: &str,
        bytes: &[u8],
        locator: &str,
    ) -> AppResult<Character> {
        let parsed = parse_character_bytes(bytes, filename)?;
        let existing: Option<(String,)> = sqlx::query_as(
            "SELECT c.id FROM characters c
             JOIN character_revisions r ON r.character_id = c.id
             WHERE c.workspace_id = ? AND r.checksum = ?
             LIMIT 1",
        )
        .bind(workspace_id)
        .bind(&parsed.checksum)
        .fetch_optional(&self.pool)
        .await?;
        if let Some((id,)) = existing {
            return self
                .get(&id)
                .await?
                .ok_or_else(|| AppError::internal("character vanished"));
        }
        let named: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM characters WHERE workspace_id = ? AND display_name = ? LIMIT 1",
        )
        .bind(workspace_id)
        .bind(NormalizedCardFields::from_raw(&parsed.raw).name)
        .fetch_optional(&self.pool)
        .await?;
        let normalized = NormalizedCardFields::from_raw(&parsed.raw);
        let now = now_rfc3339();
        let asset_id = self.store_asset(workspace_id, filename, bytes).await?;
        if let Some((character_id,)) = named {
            let revision_id = new_id();
            self.insert_revision(
                &character_id,
                &revision_id,
                &parsed,
                &normalized,
                Some(&asset_id),
                &now,
            )
            .await?;
            sqlx::query(
                "UPDATE characters SET current_revision_id = ?, updated_at = ? WHERE id = ?",
            )
            .bind(&revision_id)
            .bind(&now)
            .bind(&character_id)
            .execute(&self.pool)
            .await?;
            let _ = locator;
            return self
                .get(&character_id)
                .await?
                .ok_or_else(|| AppError::internal("character vanished"));
        }
        let dummy_actor = crate::domain::identity::Actor {
            account: crate::domain::identity::Account {
                id: "importer".into(),
                username: "importer".into(),
                display_name: "importer".into(),
                is_system_admin: true,
                disabled_at: None,
                legacy_st_handle: None,
            },
            workspace_id: Some(workspace_id.to_string()),
            workspace_role: Some(crate::domain::identity::WorkspaceRole::Owner),
        };
        self.import_bytes(&dummy_actor, filename, bytes).await
    }

    async fn insert_revision(
        &self,
        character_id: &str,
        revision_id: &str,
        parsed: &ParsedCard,
        normalized: &NormalizedCardFields,
        asset_id: Option<&str>,
        now: &str,
    ) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO character_revisions
                (id, character_id, source_format, spec, spec_version, normalized_fields_json, raw_card_json, raw_asset_id, checksum, compatibility_warnings_json, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(revision_id)
        .bind(character_id)
        .bind(&parsed.source_format)
        .bind(&parsed.spec)
        .bind(&parsed.spec_version)
        .bind(serde_json::to_string(normalized).unwrap_or_else(|_| "{}".into()))
        .bind(serde_json::to_string(&parsed.raw).unwrap_or_else(|_| "{}".into()))
        .bind(asset_id)
        .bind(&parsed.checksum)
        .bind(serde_json::to_string(&parsed.warnings).unwrap_or_else(|_| "[]".into()))
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn store_asset(
        &self,
        workspace_id: &str,
        filename: &str,
        bytes: &[u8],
    ) -> AppResult<String> {
        let digest = sha256_hex(bytes);
        let prefix = &digest[..2.min(digest.len())];
        let relative = format!("{prefix}/{digest}");
        let dest = self.assets_dir.join(prefix);
        std::fs::create_dir_all(&dest)?;
        std::fs::write(dest.join(&digest), bytes)?;
        let id = new_id();
        let now = now_rfc3339();
        let mime = if filename.to_ascii_lowercase().ends_with(".png") {
            "image/png"
        } else {
            "application/json"
        };
        sqlx::query(
            "INSERT INTO assets (id, workspace_id, sha256, mime, size_bytes, relative_path, original_filename, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(workspace_id, sha256) DO NOTHING",
        )
        .bind(&id)
        .bind(workspace_id)
        .bind(&digest)
        .bind(mime)
        .bind(bytes.len() as i64)
        .bind(&relative)
        .bind(filename)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        let existing: (String,) =
            sqlx::query_as("SELECT id FROM assets WHERE workspace_id = ? AND sha256 = ?")
                .bind(workspace_id)
                .bind(&digest)
                .fetch_one(&self.pool)
                .await?;
        Ok(existing.0)
    }

    pub async fn list(&self, workspace_id: &str) -> AppResult<Vec<Character>> {
        let rows = sqlx::query_as::<_, CharacterRow>(
            "SELECT id, workspace_id, display_name, current_revision_id, archived FROM characters WHERE workspace_id = ? ORDER BY display_name",
        )
        .bind(workspace_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(CharacterRow::into_character).collect())
    }

    pub async fn get(&self, id: &str) -> AppResult<Option<Character>> {
        let row = sqlx::query_as::<_, CharacterRow>(
            "SELECT id, workspace_id, display_name, current_revision_id, archived FROM characters WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(CharacterRow::into_character))
    }

    pub async fn current_revision(&self, character_id: &str) -> AppResult<CharacterRevision> {
        let character = self
            .get(character_id)
            .await?
            .ok_or_else(|| AppError::not_found("CHARACTER_NOT_FOUND", "character not found"))?;
        let revision_id = character.current_revision_id.ok_or_else(|| {
            AppError::not_found("REVISION_NOT_FOUND", "character has no revision")
        })?;
        self.get_revision(&revision_id).await
    }

    pub async fn get_revision(&self, revision_id: &str) -> AppResult<CharacterRevision> {
        let row = sqlx::query_as::<_, RevisionRow>(
            "SELECT id, character_id, source_format, spec, spec_version, normalized_fields_json, raw_card_json, checksum, compatibility_warnings_json
             FROM character_revisions WHERE id = ?",
        )
        .bind(revision_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::not_found("REVISION_NOT_FOUND", "character revision not found"))?;
        row.into_revision()
    }

    pub async fn export_json(&self, character_id: &str) -> AppResult<Value> {
        let revision = self.current_revision(character_id).await?;
        Ok(revision.raw_card_json)
    }

    pub async fn export_png(&self, character_id: &str) -> AppResult<Vec<u8>> {
        let character = self
            .get(character_id)
            .await?
            .ok_or_else(|| AppError::not_found("CHARACTER_NOT_FOUND", "character not found"))?;
        let revision = self.current_revision(character_id).await?;
        let asset_id: Option<(String, String)> = sqlx::query_as(
            "SELECT a.relative_path, a.mime FROM character_revisions r
             JOIN assets a ON a.id = r.raw_asset_id
             WHERE r.id = ?",
        )
        .bind(character.current_revision_id.as_deref().unwrap_or_default())
        .fetch_optional(&self.pool)
        .await?;
        let Some((relative, mime)) = asset_id else {
            return Err(AppError::bad_request(
                "PNG_ASSET_MISSING",
                "character has no PNG asset to export",
            ));
        };
        if mime != "image/png" {
            return Err(AppError::bad_request(
                "PNG_ASSET_MISSING",
                "character source is not PNG",
            ));
        }
        let bytes = std::fs::read(self.assets_dir.join(Path::new(&relative)))?;
        encode_png_with_card(&bytes, &revision.raw_card_json)
    }
}

#[derive(sqlx::FromRow)]
struct CharacterRow {
    id: String,
    workspace_id: String,
    display_name: String,
    current_revision_id: Option<String>,
    archived: i64,
}

impl CharacterRow {
    fn into_character(self) -> Character {
        Character {
            id: self.id,
            workspace_id: self.workspace_id,
            display_name: self.display_name,
            current_revision_id: self.current_revision_id,
            archived: self.archived != 0,
        }
    }
}

#[derive(sqlx::FromRow)]
struct RevisionRow {
    id: String,
    character_id: String,
    source_format: String,
    spec: Option<String>,
    spec_version: Option<String>,
    normalized_fields_json: String,
    raw_card_json: String,
    checksum: String,
    compatibility_warnings_json: String,
}

impl RevisionRow {
    fn into_revision(self) -> AppResult<CharacterRevision> {
        Ok(CharacterRevision {
            id: self.id,
            character_id: self.character_id,
            source_format: self.source_format,
            spec: self.spec,
            spec_version: self.spec_version,
            normalized: serde_json::from_str(&self.normalized_fields_json).unwrap_or_default(),
            raw_card_json: serde_json::from_str(&self.raw_card_json).unwrap_or(Value::Null),
            checksum: self.checksum,
            compatibility_warnings: serde_json::from_str(&self.compatibility_warnings_json)
                .unwrap_or_default(),
        })
    }
}
