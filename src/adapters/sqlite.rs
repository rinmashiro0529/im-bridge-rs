use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;

use crate::error::AppResult;

pub async fn connect_pool(database_path: &Path) -> AppResult<SqlitePool> {
    if let Some(parent) = database_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let url = format!("sqlite://{}", database_path.display());
    let options = SqliteConnectOptions::from_str(&url)
        .map_err(|err| crate::error::AppError::internal(format!("invalid sqlite url: {err}")))?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await?;
    sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
        .execute(&pool)
        .await
        .ok();
    Ok(pool)
}

pub async fn backup_database(pool: &SqlitePool, output: &Path) -> AppResult<()> {
    if output.exists() {
        return Err(crate::error::AppError::conflict(
            "BACKUP_EXISTS",
            format!("backup output already exists: {}", output.display()),
        ));
    }
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    sqlx::query("VACUUM INTO ?")
        .bind(output.to_string_lossy().to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn migrate(pool: &SqlitePool) -> AppResult<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(|err| crate::error::AppError::internal(format!("migration failed: {err}")))
}
