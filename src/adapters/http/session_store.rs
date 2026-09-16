use async_trait::async_trait;
use sqlx::SqlitePool;
use time::OffsetDateTime;
use tower_sessions::session::{Id, Record};
use tower_sessions::session_store::{Error as StoreError, Result as StoreResult};
use tower_sessions::ExpiredDeletion;
use tower_sessions::SessionStore;

#[derive(Clone, Debug)]
pub struct SqliteSessionStore {
    pool: SqlitePool,
}

impl SqliteSessionStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SessionStore for SqliteSessionStore {
    async fn create(&self, record: &mut Record) -> StoreResult<()> {
        loop {
            let data =
                rmp_serde::to_vec(&*record).map_err(|err| StoreError::Encode(err.to_string()))?;
            let result = sqlx::query(
                "INSERT OR IGNORE INTO tower_sessions (id, data, expiry_date) VALUES (?, ?, ?)",
            )
            .bind(record.id.to_string())
            .bind(data)
            .bind(record.expiry_date.unix_timestamp())
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Backend(err.to_string()))?;
            if result.rows_affected() == 1 {
                return Ok(());
            }
            record.id = Id::default();
        }
    }

    async fn save(&self, record: &Record) -> StoreResult<()> {
        let data = rmp_serde::to_vec(record).map_err(|err| StoreError::Encode(err.to_string()))?;
        let expiry = record.expiry_date.unix_timestamp();
        sqlx::query(
            "INSERT INTO tower_sessions (id, data, expiry_date)
             VALUES (?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET data = excluded.data, expiry_date = excluded.expiry_date",
        )
        .bind(record.id.to_string())
        .bind(data)
        .bind(expiry)
        .execute(&self.pool)
        .await
        .map_err(|err| StoreError::Backend(err.to_string()))?;
        Ok(())
    }

    async fn load(&self, session_id: &Id) -> StoreResult<Option<Record>> {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT data FROM tower_sessions WHERE id = ? AND expiry_date > ?")
                .bind(session_id.to_string())
                .bind(now)
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| StoreError::Backend(err.to_string()))?;
        row.map(|data| {
            rmp_serde::from_slice(&data.0).map_err(|err| StoreError::Decode(err.to_string()))
        })
        .transpose()
    }

    async fn delete(&self, session_id: &Id) -> StoreResult<()> {
        sqlx::query("DELETE FROM tower_sessions WHERE id = ?")
            .bind(session_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Backend(err.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl ExpiredDeletion for SqliteSessionStore {
    async fn delete_expired(&self) -> StoreResult<()> {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        sqlx::query("DELETE FROM tower_sessions WHERE expiry_date <= ?")
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Backend(err.to_string()))?;
        Ok(())
    }
}
