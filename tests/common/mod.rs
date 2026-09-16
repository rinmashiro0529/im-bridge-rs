#![allow(dead_code)]

pub mod st_backend;
pub mod st_bridge_engine;

use std::sync::Arc;

use im_bridge::adapters::llm::fake::FakeLlm;
use im_bridge::adapters::sqlite::{connect_pool, migrate};
use im_bridge::domain::identity::Actor;
use im_bridge::modules::characters::CharacterModule;
use im_bridge::modules::chat::SqliteChatEngine;
use im_bridge::modules::identity::IdentityModule;
use im_bridge::modules::models::ModelModule;
use tempfile::TempDir;

pub struct TestApp {
    pub _dir: TempDir,
    pub pool: sqlx::SqlitePool,
    pub identity: IdentityModule,
    pub characters: CharacterModule,
    pub models: ModelModule,
    pub chat: SqliteChatEngine,
    pub llm: Arc<FakeLlm>,
    pub actor: Actor,
}

pub async fn setup() -> TestApp {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.db");
    let pool = connect_pool(&db).await.unwrap();
    migrate(&pool).await.unwrap();
    let identity = IdentityModule::new(pool.clone());
    let account = identity
        .bootstrap_admin("alice", "secret-password", "Alice")
        .await
        .unwrap();
    let workspace = identity.default_workspace_id(&account.id).await.unwrap();
    let actor = identity
        .actor_in_workspace(account, &workspace)
        .await
        .unwrap();
    let characters = CharacterModule::new(pool.clone(), dir.path().join("assets"));
    let models = ModelModule::new(pool.clone());
    let llm = Arc::new(FakeLlm::new(vec!["The lighthouse is still on.".into()]));
    let chat = SqliteChatEngine::new(
        pool.clone(),
        characters.clone(),
        models.clone(),
        llm.clone(),
        None,
    );
    TestApp {
        _dir: dir,
        pool,
        identity,
        characters,
        models,
        chat,
        llm,
        actor,
    }
}

pub async fn seed_character_and_provider(app: &TestApp) -> (String, String) {
    let bytes = std::fs::read("fixtures/character_cards/v2.json").unwrap();
    let character = app
        .characters
        .import_bytes(&app.actor, "v2.json", &bytes)
        .await
        .unwrap();
    let provider = app
        .models
        .upsert_provider(&app.actor, "fake", "http://127.0.0.1:9", None, "")
        .await
        .unwrap();
    app.models
        .upsert_preset(&provider.id, "fake-model", "fake", "chat", 1.0, 1.0, 256)
        .await
        .unwrap();
    app.models
        .upsert_preset(
            &provider.id,
            "fake-model",
            "fake-compress",
            "compression",
            1.0,
            1.0,
            256,
        )
        .await
        .unwrap();
    (character.id, app.actor.workspace_id.clone().unwrap())
}
