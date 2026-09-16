use std::sync::Arc;

use im_bridge::modules::bridge::poller_ownership::{
    PollerError, PollerOwner, PollerOwnershipGuard, PollerOwnershipRegistry, PollerRuntimeBinding,
    SharedMemoryPollerRegistry,
};
use im_bridge::modules::telegram::TelegramModule;

mod common;

const BOT_ID: i64 = 123456789;

fn expect_claim_error<T>(
    result: Result<T, im_bridge::AppError>,
    message: &str,
) -> im_bridge::AppError {
    match result {
        Ok(_) => panic!("{message}"),
        Err(error) => error,
    }
}

async fn rust_registry() -> SharedMemoryPollerRegistry {
    let registry = SharedMemoryPollerRegistry::new();
    registry
        .bootstrap_legacy(BOT_ID)
        .await
        .expect("bootstrap legacy");
    registry
        .transfer(
            BOT_ID,
            PollerOwner::LegacyPlugin,
            1,
            PollerOwner::RustBridge,
        )
        .await
        .expect("transfer to rust");
    registry
}

async fn telegram_module() -> (TelegramModule, common::TestApp) {
    let app = common::setup().await;
    (TelegramModule::new(app.pool.clone()), app)
}

#[tokio::test]
async fn runtime_start_fails_closed_without_registration() {
    let (module, _app) = telegram_module().await;
    let missing = expect_claim_error(
        module
            .claim_numeric_bot(BOT_ID, "bot-missing-registry")
            .await,
        "missing registry must fail closed",
    );
    assert_eq!(missing.code, "ST_POLLER_NOT_OWNER");
    assert!(missing.message.contains("unavailable"));

    let registry = SharedMemoryPollerRegistry::new();
    module.attach_ownership_registry(Arc::new(registry)).await;
    let error = expect_claim_error(
        module.claim_numeric_bot(BOT_ID, "bot-a").await,
        "unregistered numeric bot must fail closed",
    );
    assert_eq!(error.code, "ST_POLLER_NOT_OWNER");
    assert!(error.message.contains("not registered"));
}

#[tokio::test]
async fn runtime_start_fails_closed_when_legacy_plugin_owns() {
    let (module, _app) = telegram_module().await;
    let registry = SharedMemoryPollerRegistry::new();
    registry.bootstrap_legacy(BOT_ID).await.expect("bootstrap");
    module.attach_ownership_registry(Arc::new(registry)).await;
    let error = expect_claim_error(
        module.claim_numeric_bot(BOT_ID, "bot-a").await,
        "legacy owner must block rust",
    );
    assert_eq!(error.code, "ST_POLLER_NOT_OWNER");
    assert!(error.message.contains("not owner"));
}

#[tokio::test]
async fn runtime_start_succeeds_after_transfer_to_rust() {
    let (module, _app) = telegram_module().await;
    let registry = rust_registry().await;
    module
        .attach_ownership_registry(Arc::new(registry.clone()))
        .await;
    let guard = module
        .claim_numeric_bot(BOT_ID, "bot-a")
        .await
        .expect("rust owner with matching epoch");
    assert_eq!(guard.numeric_bot_id, BOT_ID);
    assert_eq!(guard.owner, PollerOwner::RustBridge);
    assert_eq!(guard.epoch, 2);
    guard.assert_valid().await.expect("still owner");
    module.release_numeric_bot(BOT_ID, "bot-a").await;
}

#[tokio::test]
async fn runtime_start_fails_with_stale_epoch() {
    let (module, _app) = telegram_module().await;
    let registry = rust_registry().await;
    module.attach_ownership_registry(Arc::new(registry)).await;
    let error = expect_claim_error(
        module
            .claim_numeric_bot_with_epoch(BOT_ID, "bot-a", Some(1))
            .await,
        "stale epoch must fail closed",
    );
    assert_eq!(error.code, "ST_POLLER_NOT_OWNER");
    assert!(error.message.contains("epoch mismatch"));
}

#[tokio::test]
async fn two_different_tokens_same_numeric_bot_id_fails_closed() {
    let (module, _app) = telegram_module().await;
    let registry = rust_registry().await;
    module.attach_ownership_registry(Arc::new(registry)).await;
    module
        .claim_numeric_bot(BOT_ID, "token-a")
        .await
        .expect("first token");
    let error = expect_claim_error(
        module.claim_numeric_bot(BOT_ID, "token-b").await,
        "same numeric bot id must be exclusive",
    );
    assert_eq!(error.code, "BOT_NUMERIC_ID_ALREADY_RUNNING");
    module.release_numeric_bot(BOT_ID, "token-a").await;
}

#[tokio::test]
async fn poller_loop_cancels_immediately_when_ownership_lost() {
    let registry = rust_registry().await;
    let binding = PollerRuntimeBinding::new("bot-a", "runtime-a", 2).running();
    registry
        .claim_runtime(BOT_ID, binding.clone())
        .await
        .expect("claim runtime");
    let guard = PollerOwnershipGuard::new_with_binding(
        BOT_ID,
        PollerOwner::RustBridge,
        binding,
        Arc::new(registry.clone()) as Arc<dyn PollerOwnershipRegistry>,
    );
    guard.assert_valid().await.expect("initial owner");
    registry
        .transfer(
            BOT_ID,
            PollerOwner::RustBridge,
            2,
            PollerOwner::LegacyPlugin,
        )
        .await
        .expect("takeover");
    let error = guard
        .assert_valid()
        .await
        .expect_err("lost ownership must cancel before next poll");
    assert_eq!(error, PollerError::NotOwner);

    let registry = rust_registry().await;
    let binding = PollerRuntimeBinding::new("bot-a", "runtime-stale", 2).running();
    registry
        .claim_runtime(BOT_ID, binding.clone())
        .await
        .expect("claim stale runtime");
    let stale = PollerOwnershipGuard::new_with_binding(
        BOT_ID,
        PollerOwner::RustBridge,
        binding,
        Arc::new(registry.clone()) as Arc<dyn PollerOwnershipRegistry>,
    );
    registry
        .transfer(
            BOT_ID,
            PollerOwner::RustBridge,
            2,
            PollerOwner::LegacyPlugin,
        )
        .await
        .expect("epoch bump");
    registry
        .transfer(
            BOT_ID,
            PollerOwner::LegacyPlugin,
            3,
            PollerOwner::RustBridge,
        )
        .await
        .expect("return to rust with new epoch");
    let error = stale
        .assert_valid()
        .await
        .expect_err("stale epoch must cancel immediately");
    assert_eq!(error, PollerError::EpochMismatch);
}

#[tokio::test]
async fn heartbeat_validates_without_mutating_ownership() {
    let registry = rust_registry().await;
    let before = registry
        .get_ownership(BOT_ID)
        .await
        .expect("get")
        .expect("present");
    registry
        .heartbeat(BOT_ID, PollerOwner::RustBridge, 2)
        .await
        .expect("heartbeat");
    let after = registry
        .get_ownership(BOT_ID)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(before.owner, after.owner);
    assert_eq!(before.epoch, after.epoch);
    assert_eq!(after.owner, PollerOwner::RustBridge);
    assert_eq!(after.epoch, 2);
}
