use serde_json::Value;
use sqlx::SqlitePool;

use crate::clock::now_rfc3339;
use crate::error::AppResult;

#[derive(Clone)]
pub struct AuditModule {
    pool: SqlitePool,
}

impl AuditModule {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn record(
        &self,
        actor_id: Option<&str>,
        operation: &str,
        resource_type: Option<&str>,
        resource_id: Option<&str>,
        request_id: Option<&str>,
        result: &str,
        metadata: Value,
    ) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO audit_events
                (actor_id, operation, resource_type, resource_id, request_id, result, metadata_json, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(actor_id)
        .bind(operation)
        .bind(resource_type)
        .bind(resource_id)
        .bind(request_id)
        .bind(result)
        .bind(metadata.to_string())
        .bind(now_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list(&self, limit: i64) -> AppResult<Vec<serde_json::Value>> {
        #[allow(clippy::type_complexity)]
        let rows: Vec<(Option<String>, String, Option<String>, Option<String>, String, String, String)> = sqlx::query_as(
            "SELECT actor_id, operation, resource_type, resource_id, result, metadata_json, created_at
             FROM audit_events ORDER BY id DESC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "actorId": row.0,
                    "operation": row.1,
                    "resourceType": row.2,
                    "resourceId": row.3,
                    "result": row.4,
                    "metadata": serde_json::from_str::<Value>(&row.5).unwrap_or(Value::Null),
                    "createdAt": row.6,
                })
            })
            .collect())
    }
}
