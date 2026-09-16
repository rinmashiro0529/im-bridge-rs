use im_bridge::modules::bridge::poller_ownership::{
    MemoryPollerRegistry, PollerError, PollerOwner, PollerOwnershipRecord,
};

const BOT_ID: i64 = 123456789;

#[test]
fn empty_registry_bootstraps_legacy_epoch_one() {
    let mut registry = MemoryPollerRegistry::default();
    let record = registry.bootstrap_legacy(BOT_ID).expect("first bootstrap");
    assert_eq!(record.telegram_bot_id, BOT_ID);
    assert_eq!(record.owner, PollerOwner::LegacyPlugin);
    assert_eq!(record.epoch, 1);
    let stored = registry.get(BOT_ID).expect("stored");
    assert_eq!(stored.owner, PollerOwner::LegacyPlugin);
    assert_eq!(stored.epoch, 1);
}

#[test]
fn second_bootstrap_is_permanently_closed() {
    let mut registry = MemoryPollerRegistry::default();
    registry.bootstrap_legacy(BOT_ID).expect("first bootstrap");
    let error = registry
        .bootstrap_legacy(BOT_ID + 1)
        .expect_err("auto claim closed");
    assert_eq!(error, PollerError::BootstrapClosed);
}

#[test]
fn heartbeat_does_not_change_owner_or_epoch() {
    let mut registry = MemoryPollerRegistry::default();
    registry.bootstrap_legacy(BOT_ID).expect("bootstrap");
    registry
        .heartbeat(BOT_ID, PollerOwner::LegacyPlugin, 1)
        .expect("matching heartbeat");
    let stored = registry.get(BOT_ID).expect("stored");
    assert_eq!(stored.owner, PollerOwner::LegacyPlugin);
    assert_eq!(stored.epoch, 1);
}

#[test]
fn transfer_legacy_to_rust_increments_epoch() {
    let mut registry = MemoryPollerRegistry::default();
    registry.bootstrap_legacy(BOT_ID).expect("bootstrap");
    let transferred = registry
        .transfer(
            BOT_ID,
            PollerOwner::LegacyPlugin,
            1,
            PollerOwner::RustBridge,
        )
        .expect("cas transfer");
    assert_eq!(transferred.owner, PollerOwner::RustBridge);
    assert_eq!(transferred.epoch, 2);
    let stored = registry.get(BOT_ID).expect("stored");
    assert_eq!(stored.owner, PollerOwner::RustBridge);
    assert_eq!(stored.epoch, 2);
}

#[test]
fn wrong_expected_epoch_is_mismatch_and_leaves_record() {
    let mut registry = MemoryPollerRegistry::default();
    registry.bootstrap_legacy(BOT_ID).expect("bootstrap");
    let error = registry
        .transfer(
            BOT_ID,
            PollerOwner::LegacyPlugin,
            99,
            PollerOwner::RustBridge,
        )
        .expect_err("epoch fencing");
    assert_eq!(error, PollerError::EpochMismatch);
    let stored = registry.get(BOT_ID).expect("unchanged");
    assert_eq!(stored.owner, PollerOwner::LegacyPlugin);
    assert_eq!(stored.epoch, 1);
}

#[test]
fn rust_runtime_cannot_start_while_legacy_owns() {
    let mut registry = MemoryPollerRegistry::default();
    registry.bootstrap_legacy(BOT_ID).expect("bootstrap");
    let error = registry
        .assert_can_start(BOT_ID, PollerOwner::RustBridge, 1)
        .expect_err("legacy still owns");
    assert_eq!(error, PollerError::NotOwner);
}

#[test]
fn ownership_record_has_only_numeric_bot_id_owner_and_epoch() {
    let record = PollerOwnershipRecord {
        telegram_bot_id: BOT_ID,
        owner: PollerOwner::LegacyPlugin,
        epoch: 1,
    };
    let PollerOwnershipRecord {
        telegram_bot_id,
        owner,
        epoch,
    } = record;
    assert_eq!(telegram_bot_id, BOT_ID);
    assert_eq!(owner, PollerOwner::LegacyPlugin);
    assert_eq!(epoch, 1);
    let error_text = format!(
        "{} {} {} {} {} {} {}",
        PollerError::BootstrapClosed,
        PollerError::NotRegistered,
        PollerError::NotOwner,
        PollerError::EpochMismatch,
        PollerError::InvalidBotId,
        PollerError::RegistryUnavailable,
        PollerError::Conflict
    );
    assert!(!error_text.to_ascii_lowercase().contains("token"));
    assert!(!error_text.to_ascii_lowercase().contains("username"));
}
