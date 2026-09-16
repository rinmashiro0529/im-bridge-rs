use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Argon2, Params};
use rand::RngCore;
use sqlx::SqlitePool;

use crate::clock::now_rfc3339;
use crate::domain::identity::{Account, Actor, WorkspaceRole};
use crate::error::{AppError, AppResult};
use crate::ids::new_id;

const ARGON2_M_KIB: u32 = 19 * 1024;
const ARGON2_T: u32 = 2;
const ARGON2_P: u32 = 1;

#[derive(Clone)]
pub struct IdentityModule {
    pool: SqlitePool,
}

impl IdentityModule {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn argon2() -> AppResult<Argon2<'static>> {
        let params = Params::new(ARGON2_M_KIB, ARGON2_T, ARGON2_P, None)
            .map_err(|err| AppError::internal(format!("argon2 params: {err}")))?;
        Ok(Argon2::new(
            argon2::Algorithm::Argon2id,
            argon2::Version::V0x13,
            params,
        ))
    }

    fn username_is_valid(username: &str) -> bool {
        let username_len = username.chars().count();
        (1..=64).contains(&username_len)
            && username.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
            })
    }

    fn validate_account_fields(
        username: &str,
        password: &str,
        display_name: &str,
    ) -> AppResult<()> {
        let display_len = display_name.chars().count();
        if !Self::username_is_valid(username) {
            return Err(AppError::bad_request(
                "USERNAME_INVALID",
                "username must contain 1-64 ASCII letters, digits, dots, underscores, or hyphens",
            ));
        }
        if !(1..=128).contains(&display_len) {
            return Err(AppError::bad_request(
                "DISPLAY_NAME_INVALID",
                "display name must contain 1-128 characters",
            ));
        }
        if !(12..=1024).contains(&password.len()) {
            return Err(AppError::bad_request(
                "PASSWORD_INVALID",
                "password must contain 12-1024 bytes",
            ));
        }
        Ok(())
    }

    pub fn hash_password(password: &str) -> AppResult<String> {
        let argon2 = Self::argon2()?;
        let mut salt_bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut salt_bytes);
        let salt = SaltString::encode_b64(&salt_bytes)
            .map_err(|err| AppError::internal(format!("salt: {err}")))?;
        argon2
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|err| AppError::internal(format!("hash: {err}")))
    }

    pub fn verify_password(password: &str, hash: &str) -> AppResult<bool> {
        let parsed =
            PasswordHash::new(hash).map_err(|_| AppError::unauthorized("invalid credentials"))?;
        Ok(Self::argon2()?
            .verify_password(password.as_bytes(), &parsed)
            .is_ok())
    }

    pub async fn bootstrap_admin(
        &self,
        username: &str,
        password: &str,
        display_name: &str,
    ) -> AppResult<Account> {
        Self::validate_account_fields(username, password, display_name)?;
        if let Some(existing) = self.get_by_username(username).await? {
            return Ok(existing);
        }
        let id = new_id();
        let now = now_rfc3339();
        let hash = Self::hash_password(password)?;
        sqlx::query(
            "INSERT INTO accounts (id, username, display_name, password_hash, is_system_admin, created_at, updated_at)
             VALUES (?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&id)
        .bind(username)
        .bind(display_name)
        .bind(&hash)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        let workspace_id = new_id();
        sqlx::query(
            "INSERT INTO workspaces (id, name, created_by, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&workspace_id)
        .bind("Default")
        .bind(&id)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, account_id, role, created_at) VALUES (?, ?, 'owner', ?)",
        )
        .bind(&workspace_id)
        .bind(&id)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "INSERT INTO workspace_settings (workspace_id, prompt_user_name, default_prompt_profile, updated_at)
             VALUES (?, 'User', 'legacy_bridge_v1', ?)",
        )
        .bind(&workspace_id)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        self.get_by_id(&id)
            .await?
            .ok_or_else(|| AppError::internal("bootstrap account missing"))
    }

    pub async fn create_account(
        &self,
        actor: &Actor,
        username: &str,
        password: &str,
        display_name: &str,
        is_admin: bool,
    ) -> AppResult<Account> {
        if !actor.account.is_system_admin {
            return Err(AppError::forbidden("only system admin can create accounts"));
        }
        Self::validate_account_fields(username, password, display_name)?;
        let id = new_id();
        let now = now_rfc3339();
        let hash = Self::hash_password(password)?;
        let workspace_id = new_id();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO accounts (id, username, display_name, password_hash, is_system_admin, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(username)
        .bind(display_name)
        .bind(&hash)
        .bind(i64::from(is_admin))
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO workspaces (id, name, created_by, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&workspace_id)
        .bind(display_name)
        .bind(&id)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, account_id, role, created_at)
             VALUES (?, ?, 'owner', ?)",
        )
        .bind(&workspace_id)
        .bind(&id)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO workspace_settings
                (workspace_id, prompt_user_name, default_prompt_profile, updated_at)
             VALUES (?, ?, 'legacy_bridge_v1', ?)",
        )
        .bind(&workspace_id)
        .bind(display_name)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        self.get_by_id(&id)
            .await?
            .ok_or_else(|| AppError::internal("created account missing"))
    }

    pub async fn authenticate(&self, username: &str, password: &str) -> AppResult<Account> {
        if !Self::username_is_valid(username) || password.len() > 1024 {
            let _ = Self::hash_password("invalid-password-probe")?;
            return Err(AppError::unauthorized("invalid credentials"));
        }
        self.check_login_rate(username).await?;
        let row = sqlx::query_as::<_, AccountRow>(
            "SELECT id, username, display_name, password_hash, is_system_admin, disabled_at, legacy_st_handle
             FROM accounts WHERE username = ?",
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            // Perform the same expensive password work for an unknown username to reduce
            // account-enumeration timing differences.
            let _ = Self::hash_password(password)?;
            return Err(AppError::unauthorized("invalid credentials"));
        };
        if row.disabled_at.is_some() || !Self::verify_password(password, &row.password_hash)? {
            self.record_login_failure(username).await?;
            return Err(AppError::unauthorized("invalid credentials"));
        }
        self.clear_login_rate(username).await?;
        Ok(row.into_account())
    }

    pub async fn get_by_id(&self, id: &str) -> AppResult<Option<Account>> {
        let row = sqlx::query_as::<_, AccountRow>(
            "SELECT id, username, display_name, password_hash, is_system_admin, disabled_at, legacy_st_handle
             FROM accounts WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(AccountRow::into_account))
    }

    pub async fn get_by_username(&self, username: &str) -> AppResult<Option<Account>> {
        let row = sqlx::query_as::<_, AccountRow>(
            "SELECT id, username, display_name, password_hash, is_system_admin, disabled_at, legacy_st_handle
             FROM accounts WHERE username = ?",
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(AccountRow::into_account))
    }

    pub async fn list_accounts(&self) -> AppResult<Vec<Account>> {
        let rows = sqlx::query_as::<_, AccountRow>(
            "SELECT id, username, display_name, password_hash, is_system_admin, disabled_at, legacy_st_handle
             FROM accounts ORDER BY created_at",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(AccountRow::into_account).collect())
    }

    pub async fn actor_in_workspace(
        &self,
        account: Account,
        workspace_id: &str,
    ) -> AppResult<Actor> {
        if account.is_system_admin {
            return Ok(Actor {
                account,
                workspace_id: Some(workspace_id.to_string()),
                workspace_role: Some(WorkspaceRole::Owner),
            });
        }
        let role: Option<(String,)> = sqlx::query_as(
            "SELECT role FROM workspace_members WHERE workspace_id = ? AND account_id = ?",
        )
        .bind(workspace_id)
        .bind(&account.id)
        .fetch_optional(&self.pool)
        .await?;
        let role = role
            .and_then(|row| WorkspaceRole::parse(&row.0))
            .ok_or_else(|| AppError::forbidden("not a member of this workspace"))?;
        Ok(Actor {
            account,
            workspace_id: Some(workspace_id.to_string()),
            workspace_role: Some(role),
        })
    }

    pub async fn default_workspace_id(&self, account_id: &str) -> AppResult<String> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT workspace_id FROM workspace_members WHERE account_id = ? ORDER BY created_at LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|value| value.0)
            .ok_or_else(|| AppError::not_found("WORKSPACE_NOT_FOUND", "no workspace"))
    }

    pub async fn ensure_legacy_account(
        &self,
        handle: &str,
        display_name: &str,
    ) -> AppResult<(Account, String)> {
        if let Some(existing) = sqlx::query_as::<_, AccountRow>(
            "SELECT id, username, display_name, password_hash, is_system_admin, disabled_at, legacy_st_handle
             FROM accounts WHERE legacy_st_handle = ?",
        )
        .bind(handle)
        .fetch_optional(&self.pool)
        .await?
        {
            let workspace = self.default_workspace_id(&existing.id).await?;
            return Ok((existing.into_account(), workspace));
        }
        if let Some(existing) = self.get_by_username(handle).await? {
            sqlx::query("UPDATE accounts SET legacy_st_handle = ?, updated_at = ? WHERE id = ?")
                .bind(handle)
                .bind(now_rfc3339())
                .bind(&existing.id)
                .execute(&self.pool)
                .await?;
            let workspace = self.default_workspace_id(&existing.id).await?;
            return Ok((existing, workspace));
        }
        let password = format!("imported-{}", new_id());
        let account = self
            .bootstrap_like_user(handle, &password, display_name)
            .await?;
        sqlx::query("UPDATE accounts SET legacy_st_handle = ? WHERE id = ?")
            .bind(handle)
            .bind(&account.id)
            .execute(&self.pool)
            .await?;
        let workspace = self.default_workspace_id(&account.id).await?;
        Ok((account, workspace))
    }

    async fn bootstrap_like_user(
        &self,
        username: &str,
        password: &str,
        display_name: &str,
    ) -> AppResult<Account> {
        let id = new_id();
        let now = now_rfc3339();
        let hash = Self::hash_password(password)?;
        sqlx::query(
            "INSERT INTO accounts (id, username, display_name, password_hash, is_system_admin, created_at, updated_at)
             VALUES (?, ?, ?, ?, 0, ?, ?)",
        )
        .bind(&id)
        .bind(username)
        .bind(display_name)
        .bind(&hash)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        let workspace_id = new_id();
        sqlx::query(
            "INSERT INTO workspaces (id, name, created_by, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&workspace_id)
        .bind(display_name)
        .bind(&id)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "INSERT INTO workspace_members (workspace_id, account_id, role, created_at) VALUES (?, ?, 'owner', ?)",
        )
        .bind(&workspace_id)
        .bind(&id)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "INSERT INTO workspace_settings (workspace_id, prompt_user_name, default_prompt_profile, updated_at)
             VALUES (?, ?, 'legacy_bridge_v1', ?)",
        )
        .bind(&workspace_id)
        .bind(display_name)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        self.get_by_id(&id)
            .await?
            .ok_or_else(|| AppError::internal("imported account missing"))
    }

    async fn check_login_rate(&self, username: &str) -> AppResult<()> {
        let row: Option<(Option<String>,)> =
            sqlx::query_as("SELECT locked_until FROM login_rate_limits WHERE key = ?")
                .bind(username)
                .fetch_optional(&self.pool)
                .await?;
        if let Some((Some(locked_until),)) = row {
            if locked_until > now_rfc3339() {
                return Err(AppError::too_many("login temporarily locked"));
            }
        }
        Ok(())
    }

    async fn record_login_failure(&self, username: &str) -> AppResult<()> {
        let now = now_rfc3339();
        let window_cutoff = (time::OffsetDateTime::now_utc() - time::Duration::minutes(15))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| now.clone());
        sqlx::query(
            "INSERT INTO login_rate_limits (key, failures, window_start, locked_until)
             VALUES (?, 1, ?, NULL)
             ON CONFLICT(key) DO UPDATE SET
                 failures = CASE WHEN window_start < ? THEN 1 ELSE failures + 1 END,
                 window_start = CASE WHEN window_start < ? THEN excluded.window_start ELSE window_start END,
                 locked_until = CASE WHEN locked_until IS NOT NULL AND locked_until <= ? THEN NULL ELSE locked_until END",
        )
        .bind(username)
        .bind(&now)
        .bind(&window_cutoff)
        .bind(&window_cutoff)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        let failures: i64 =
            sqlx::query_scalar("SELECT failures FROM login_rate_limits WHERE key = ?")
                .bind(username)
                .fetch_one(&self.pool)
                .await?;
        if failures >= 10 {
            let locked_until = (time::OffsetDateTime::now_utc() + time::Duration::minutes(15))
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| now.clone());
            sqlx::query("UPDATE login_rate_limits SET locked_until = ? WHERE key = ?")
                .bind(locked_until)
                .bind(username)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn clear_login_rate(&self, username: &str) -> AppResult<()> {
        sqlx::query("DELETE FROM login_rate_limits WHERE key = ?")
            .bind(username)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct AccountRow {
    id: String,
    username: String,
    display_name: String,
    password_hash: String,
    is_system_admin: i64,
    disabled_at: Option<String>,
    legacy_st_handle: Option<String>,
}

impl AccountRow {
    fn into_account(self) -> Account {
        Account {
            id: self.id,
            username: self.username,
            display_name: self.display_name,
            is_system_admin: self.is_system_admin != 0,
            disabled_at: self.disabled_at,
            legacy_st_handle: self.legacy_st_handle,
        }
    }
}
