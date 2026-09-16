use std::path::PathBuf;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::AppResult;

#[derive(Debug, Clone)]
pub struct LegacyCharacterFile {
    pub handle: String,
    pub path: PathBuf,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct LegacyChatFile {
    pub handle: String,
    pub character_dir: String,
    pub path: PathBuf,
    pub lines: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LegacySettings {
    pub handle: String,
    pub username: String,
    pub raw: Value,
}

#[async_trait]
pub trait LegacySource: Send + Sync {
    async fn list_handles(&self) -> AppResult<Vec<String>>;
    async fn list_characters(&self, handle: &str) -> AppResult<Vec<LegacyCharacterFile>>;
    async fn list_chats(&self, handle: &str) -> AppResult<Vec<LegacyChatFile>>;
    async fn load_settings(&self, handle: &str) -> AppResult<Option<LegacySettings>>;
}
