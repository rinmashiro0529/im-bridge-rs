use std::sync::Arc;

use sqlx::SqlitePool;
use zeroize::Zeroize;

use crate::adapters::secrets::encrypted_sqlite::EncryptedSqliteVault;
use crate::adapters::sqlite::{connect_pool, migrate};
use crate::adapters::st::backend::ReqwestStBackend;
use crate::config::AppConfig;
use crate::error::AppResult;
use crate::modules::audit::AuditModule;
use crate::modules::bridge::channel_context::ChannelContextStore;
use crate::modules::bridge::operation_coordinator::OperationCoordinator;
use crate::modules::bridge::operation_payload::OperationPayloadKeyProvider;
use crate::modules::bridge::operation_store::OperationStore;
use crate::modules::characters::CharacterModule;
use crate::modules::identity::IdentityModule;
use crate::modules::migration::LegacyImporter;
use crate::modules::models::ModelModule;
use crate::modules::telegram::{TelegramModule, TelegramServices};
use crate::seams::secret_vault::SecretVault;
use crate::seams::st_backend::StBackend;
use crate::seams::st_operation_journal::UnavailableStOperationJournal;

pub struct AppState {
    pub config: AppConfig,
    pub pool: SqlitePool,
    pub identity: IdentityModule,
    pub characters: CharacterModule,
    pub models: ModelModule,
    pub telegram: TelegramModule,
    pub audit: AuditModule,
    pub importer: LegacyImporter,
    pub vault: Arc<dyn SecretVault>,
    pub st: Option<Arc<dyn StBackend>>,
    pub bridge: Option<Arc<dyn crate::seams::st_bridge_engine::StBridgeEngine>>,
}

impl AppState {
    pub async fn bootstrap(mut config: AppConfig, _use_fake_llm: bool) -> AppResult<Self> {
        config.validate()?;
        config.ensure_dirs()?;
        let pool = connect_pool(&config.database_path).await?;
        migrate(&pool).await?;
        let mut master_key = EncryptedSqliteVault::load_or_create_key(&config.master_key_path)?;
        let bind_code_key = crate::adapters::secrets::encrypted_sqlite::derive_subkey(
            &master_key,
            b"im-bridge/telegram-bind-code/v1",
        )?;
        let vault: Arc<dyn SecretVault> =
            Arc::new(EncryptedSqliteVault::new(pool.clone(), master_key));
        master_key.zeroize();
        verify_master_key(&pool, vault.as_ref()).await?;
        let identity = IdentityModule::new(pool.clone());
        let characters = CharacterModule::new(pool.clone(), config.assets_dir());
        let models = ModelModule::new(pool.clone());
        let connector_hmac_key = config.st.connector_hmac_key.take();
        let mut st_runtime_config = config.st.clone();
        st_runtime_config.connector_hmac_key = connector_hmac_key;
        let concrete_st: Option<Arc<ReqwestStBackend>> = if st_runtime_config.base_url.is_some() {
            Some(Arc::new(ReqwestStBackend::new(st_runtime_config)))
        } else {
            None
        };
        let st: Option<Arc<dyn StBackend>> = concrete_st
            .clone()
            .map(|backend| backend as Arc<dyn StBackend>);
        let connector = concrete_st
            .as_ref()
            .and_then(|backend| backend.connector_client());
        if config.st.write_mode().allows_write() && connector.is_none() {
            return Err(crate::error::AppError::service_unavailable(
                "ST_WRITE_NOT_READY",
                "write mode requires an authenticated Connector journal and key provider",
            ));
        }
        let ownership_registry = connector.clone().map(|client| {
            client as Arc<dyn crate::modules::bridge::poller_ownership::PollerOwnershipRegistry>
        });
        let channel = ChannelContextStore::new(pool.clone());
        let operation_store = Arc::new(OperationStore::new(pool.clone()));
        let sidecar = concrete_st.clone().map(|concrete_backend| {
            let backend: Arc<dyn StBackend> = concrete_backend.clone();
            let journal: Arc<dyn crate::seams::st_operation_journal::StOperationJournal> =
                match concrete_backend.connector_client() {
                    Some(client) => client,
                    None => Arc::new(UnavailableStOperationJournal),
                };
            let key_provider = Arc::new(OperationPayloadKeyProvider::new(
                vault.clone(),
                operation_store.clone(),
            ));
            let coordinator = Arc::new(OperationCoordinator::new_with_key_provider(
                backend.clone(),
                operation_store.clone(),
                key_provider,
                journal,
            ));
            crate::modules::bridge::engine::SidecarBridgeEngine::new(backend, channel.clone())
                .with_required_coordinator(coordinator)
        });
        let bridge = sidecar.clone().map(|engine| {
            Arc::new(engine) as Arc<dyn crate::seams::st_bridge_engine::StBridgeEngine>
        });
        let telegram = TelegramModule::new_with_bind_key(pool.clone(), bind_code_key);
        telegram
            .attach_services(TelegramServices {
                identity: identity.clone(),
                vault: vault.clone(),
                bridge: sidecar,
                channel,
                ownership_registry,
            })
            .await;
        let audit = AuditModule::new(pool.clone());
        let importer = LegacyImporter::new(
            pool.clone(),
            identity.clone(),
            characters.clone(),
            models.clone(),
        );
        Ok(Self {
            config,
            pool,
            identity,
            characters,
            models,
            telegram,
            audit,
            importer,
            vault,
            st,
            bridge,
        })
    }
}

async fn verify_master_key(pool: &SqlitePool, vault: &dyn SecretVault) -> AppResult<()> {
    const SENTINEL: &[u8] = b"im-bridge-master-key-check-v1";
    let existing: Option<String> = sqlx::query_scalar(
        "SELECT id FROM secrets WHERE owner_scope = 'system' AND kind = 'master_key_check' ORDER BY created_at LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    if let Some(secret_id) = existing {
        let plaintext = vault
            .get(&secret_id)
            .await
            .map_err(|_| crate::error::AppError::internal("master key verification failed"))?;
        if plaintext != SENTINEL {
            return Err(crate::error::AppError::internal(
                "master key verification failed",
            ));
        }
    } else {
        let existing_secret: Option<String> =
            sqlx::query_scalar("SELECT id FROM secrets ORDER BY created_at LIMIT 1")
                .fetch_optional(pool)
                .await?;
        if let Some(secret_id) = existing_secret {
            vault
                .get(&secret_id)
                .await
                .map_err(|_| crate::error::AppError::internal("master key verification failed"))?;
        }
        vault.put("system", "master_key_check", SENTINEL).await?;
    }
    Ok(())
}
