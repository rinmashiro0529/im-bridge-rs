use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::AppResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretMeta {
    pub id: String,
    pub kind: String,
    pub fingerprint: String,
    pub configured: bool,
}

#[async_trait]
pub trait SecretVault: Send + Sync {
    async fn put(&self, owner_scope: &str, kind: &str, plaintext: &[u8]) -> AppResult<SecretMeta>;
    async fn get(&self, secret_id: &str) -> AppResult<Vec<u8>>;
    async fn delete(&self, secret_id: &str) -> AppResult<()>;
    async fn fingerprint(&self, secret_id: &str) -> AppResult<String>;
    async fn rotate_master_key(&self, new_key: &[u8], dry_run: bool) -> AppResult<u32>;
}
