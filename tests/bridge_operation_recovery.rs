use std::sync::Arc;

use im_bridge::domain::st::StChatLocator;
use im_bridge::modules::bridge::operation_payload::{locator_hash, OperationPayloadEncryptor};
use im_bridge::modules::bridge::operation_recovery::OperationRecoveryCoordinator;
use im_bridge::modules::bridge::operation_store::{
    ClaimOperationRequest, OperationClaimError, OperationStore,
};
use im_bridge::modules::bridge::operations::{
    BridgeOperationStatus, OperationAad, OperationCommitState,
};

mod common;

fn locator() -> StChatLocator {
    StChatLocator {
        handle: "synthetic-handle".into(),
        avatar: "Synthetic.png".into(),
        character_name: "Synthetic Character".into(),
        chat_file: "synthetic.jsonl".into(),
    }
}

fn claim_request(
    actor_id: &str,
    bot_id: &str,
    update_id: i64,
    kind: &str,
) -> ClaimOperationRequest {
    ClaimOperationRequest {
        id: String::new(),
        actor_id: actor_id.to_string(),
        bot_id: bot_id.to_string(),
        telegram_update_id: update_id,
        channel_context_key: "telegram:synthetic".into(),
        operation_kind: kind.to_string(),
        locator: locator(),
        request_id: "req_synthetic".into(),
        trace_id: "trace_synthetic".into(),
    }
}

async fn seed_bot(pool: &sqlx::SqlitePool, actor_id: &str, workspace_id: &str, bot_id: &str) {
    sqlx::query(
        "INSERT INTO telegram_bots (id, workspace_id, owner_account_id, desired_enabled, created_at, updated_at)
         VALUES (?, ?, ?, 0, '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .bind(bot_id)
    .bind(workspace_id)
    .bind(actor_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_wrapped_secret(pool: &sqlx::SqlitePool, secret_id: &str) {
    sqlx::query(
        "INSERT INTO secrets (id, owner_scope, kind, key_version, nonce, ciphertext, fingerprint, created_at, updated_at)
         VALUES (?, 'test-scope', 'operation-dek', 1, x'00', x'00', 'fp_test', '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z')",
    )
    .bind(secret_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn put_generated_key_ref(store: &OperationStore, operation_id: &str, key_version: u32) {
    let secret_id = format!("wrapped-{operation_id}");
    seed_wrapped_secret(store.pool(), &secret_id).await;
    store
        .put_operation_key_reference(operation_id, &secret_id, key_version)
        .await
        .unwrap();
}

#[tokio::test]
async fn atomic_claim_rejects_duplicate_update_kind() {
    let app = common::setup().await;
    let workspace_id = app.actor.workspace_id.clone().unwrap();
    seed_bot(
        &app.pool,
        &app.actor.account.id,
        &workspace_id,
        "bot-synthetic",
    )
    .await;
    let store = OperationStore::new(app.pool.clone());
    let first = store
        .claim_operation(claim_request(
            &app.actor.account.id,
            "bot-synthetic",
            42,
            "append_turn",
        ))
        .await
        .expect("first claim");
    assert_eq!(first.status, BridgeOperationStatus::Received);
    assert_eq!(first.commit_state, OperationCommitState::NotStarted);
    assert_eq!(first.attempt_count, 1);

    let duplicate = store
        .claim_operation(claim_request(
            &app.actor.account.id,
            "bot-synthetic",
            42,
            "append_turn",
        ))
        .await
        .expect_err("duplicate claim");
    match duplicate {
        OperationClaimError::DuplicateClaim(existing) => {
            assert_eq!(existing.id, first.id);
            assert_eq!(existing.status, BridgeOperationStatus::Received);
        }
        OperationClaimError::Failed(err) => panic!("expected duplicate claim, got {err}"),
    }
}

#[tokio::test]
async fn full_cas_path_and_generationless_shortcut() {
    let app = common::setup().await;
    let workspace_id = app.actor.workspace_id.clone().unwrap();
    seed_bot(&app.pool, &app.actor.account.id, &workspace_id, "bot-cas").await;
    let store = OperationStore::new(app.pool.clone());
    let encryptor = OperationPayloadEncryptor::new([11u8; 32], 1);

    let generated_path = store
        .claim_operation(claim_request(
            &app.actor.account.id,
            "bot-cas",
            1,
            "append_turn",
        ))
        .await
        .unwrap();
    store
        .update_snapshot_ready(
            &generated_path.id,
            Some("sha-synthetic"),
            Some("int-synthetic"),
            Some(12),
            Some(3),
        )
        .await
        .unwrap();
    store.update_generating(&generated_path.id).await.unwrap();
    let aad = OperationAad::new(
        &generated_path.id,
        locator_hash("synthetic-handle", "Synthetic.png", "synthetic.jsonl"),
        "append_turn",
        &app.actor.account.id,
        "bot-cas",
    );
    let payload = encryptor.encrypt(b"synthetic-generated", &aad).unwrap();
    put_generated_key_ref(&store, &generated_path.id, payload.key_version).await;
    store
        .update_generated(&generated_path.id, &payload, "digest-synthetic")
        .await
        .unwrap();
    store.mark_committing(&generated_path.id).await.unwrap();
    store
        .mark_committed(&generated_path.id, "{\"status\":\"applied\"}")
        .await
        .unwrap();
    let delivered = store.mark_delivered(&generated_path.id).await.unwrap();
    assert_eq!(delivered.status, BridgeOperationStatus::Delivered);
    assert_eq!(delivered.commit_state, OperationCommitState::Applied);

    let shortcut = store
        .claim_operation(claim_request(
            &app.actor.account.id,
            "bot-cas",
            2,
            "undo_last_turn",
        ))
        .await
        .unwrap();
    store
        .update_snapshot_ready(&shortcut.id, None, None, None, None)
        .await
        .unwrap();
    let undo_aad = OperationAad::new(
        &shortcut.id,
        locator_hash("synthetic-handle", "Synthetic.png", "synthetic.jsonl"),
        "undo_last_turn",
        &app.actor.account.id,
        "bot-cas",
    );
    let undo_payload = encryptor.encrypt(b"synthetic-undo", &undo_aad).unwrap();
    put_generated_key_ref(&store, &shortcut.id, undo_payload.key_version).await;
    let generated = store
        .update_generated(&shortcut.id, &undo_payload, "digest-undo")
        .await
        .unwrap();
    assert_eq!(generated.status, BridgeOperationStatus::Generated);
}

#[tokio::test]
async fn cas_rejects_mismatched_status_and_committed_is_not_failed() {
    let app = common::setup().await;
    let workspace_id = app.actor.workspace_id.clone().unwrap();
    seed_bot(&app.pool, &app.actor.account.id, &workspace_id, "bot-guard").await;
    let store = OperationStore::new(app.pool.clone());
    let encryptor = OperationPayloadEncryptor::new([13u8; 32], 1);
    let record = store
        .claim_operation(claim_request(
            &app.actor.account.id,
            "bot-guard",
            9,
            "append_turn",
        ))
        .await
        .unwrap();
    let error = store
        .update_generating(&record.id)
        .await
        .expect_err("cannot skip snapshot_ready");
    assert_eq!(error.code, "BRIDGE_OPERATION_CAS_CONFLICT");

    store
        .update_snapshot_ready(&record.id, None, None, None, None)
        .await
        .unwrap();
    store.update_generating(&record.id).await.unwrap();
    let aad = OperationAad::new(
        &record.id,
        locator_hash("synthetic-handle", "Synthetic.png", "synthetic.jsonl"),
        "append_turn",
        &app.actor.account.id,
        "bot-guard",
    );
    let payload = encryptor.encrypt(b"synthetic-generated", &aad).unwrap();
    put_generated_key_ref(&store, &record.id, payload.key_version).await;
    store
        .update_generated(&record.id, &payload, "digest")
        .await
        .unwrap();
    store.mark_committing(&record.id).await.unwrap();
    store
        .mark_committed(&record.id, "{\"status\":\"applied\"}")
        .await
        .unwrap();
    let error = store
        .mark_failed(
            &record.id,
            "delivery",
            "TG_SEND_FAILED",
            "delivery lost",
            true,
        )
        .await
        .expect_err("committed must not degrade to failed");
    assert_eq!(error.code, "BRIDGE_OPERATION_CAS_CONFLICT");
    let still = store.get_operation(&record.id).await.unwrap().unwrap();
    assert_eq!(still.status, BridgeOperationStatus::Committed);
}

#[tokio::test]
async fn encrypted_payload_survives_store_round_trip() {
    let app = common::setup().await;
    let workspace_id = app.actor.workspace_id.clone().unwrap();
    seed_bot(
        &app.pool,
        &app.actor.account.id,
        &workspace_id,
        "bot-payload",
    )
    .await;
    let store = OperationStore::new(app.pool.clone());
    let encryptor = OperationPayloadEncryptor::new([17u8; 32], 3);
    let record = store
        .claim_operation(claim_request(
            &app.actor.account.id,
            "bot-payload",
            11,
            "append_turn",
        ))
        .await
        .unwrap();
    store
        .update_snapshot_ready(&record.id, None, None, None, None)
        .await
        .unwrap();
    let aad = OperationAad::new(
        &record.id,
        locator_hash("synthetic-handle", "Synthetic.png", "synthetic.jsonl"),
        "append_turn",
        &app.actor.account.id,
        "bot-payload",
    );
    let payload = encryptor.encrypt(b"synthetic-generated", &aad).unwrap();
    put_generated_key_ref(&store, &record.id, payload.key_version).await;
    store
        .update_generated(&record.id, &payload, "digest-store")
        .await
        .unwrap();
    let loaded = store.get_operation(&record.id).await.unwrap().unwrap();
    let stored = loaded.payload.expect("payload persisted");
    assert_eq!(stored.key_version, 3);
    let plaintext = encryptor.decrypt(&stored, &aad).unwrap();
    assert_eq!(plaintext, b"synthetic-generated");
}

#[tokio::test]
async fn recovery_finds_dangling_committing_and_dry_run_skips_unsafe_rows() {
    let app = common::setup().await;
    let workspace_id = app.actor.workspace_id.clone().unwrap();
    seed_bot(
        &app.pool,
        &app.actor.account.id,
        &workspace_id,
        "bot-recovery",
    )
    .await;
    let store = Arc::new(OperationStore::new(app.pool.clone()));
    let encryptor = OperationPayloadEncryptor::new([19u8; 32], 1);
    let coordinator = OperationRecoveryCoordinator::new(store.clone());

    let dangling = store
        .claim_operation(claim_request(
            &app.actor.account.id,
            "bot-recovery",
            21,
            "append_turn",
        ))
        .await
        .unwrap();
    store
        .update_snapshot_ready(&dangling.id, None, None, None, None)
        .await
        .unwrap();
    store.update_generating(&dangling.id).await.unwrap();
    let aad = OperationAad::new(
        &dangling.id,
        locator_hash("synthetic-handle", "Synthetic.png", "synthetic.jsonl"),
        "append_turn",
        &app.actor.account.id,
        "bot-recovery",
    );
    let payload = encryptor.encrypt(b"synthetic-generated", &aad).unwrap();
    put_generated_key_ref(&store, &dangling.id, payload.key_version).await;
    store
        .update_generated(&dangling.id, &payload, "digest")
        .await
        .unwrap();
    store.mark_committing(&dangling.id).await.unwrap();
    store.mark_commit_unknown(&dangling.id).await.unwrap();

    let found = coordinator
        .find_dangling_committing_operations()
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id, dangling.id);
    assert_eq!(found[0].commit_state, OperationCommitState::Unknown);
    let unknown = coordinator.find_unknown_commit_operations().await.unwrap();
    assert_eq!(unknown.len(), 1);

    let conflict = store
        .claim_operation(claim_request(
            &app.actor.account.id,
            "bot-recovery",
            22,
            "append_turn",
        ))
        .await
        .unwrap();
    store
        .update_snapshot_ready(&conflict.id, None, None, None, None)
        .await
        .unwrap();
    let conflict_payload = encryptor
        .encrypt(
            b"synthetic-generated",
            &OperationAad::new(
                &conflict.id,
                locator_hash("synthetic-handle", "Synthetic.png", "synthetic.jsonl"),
                "append_turn",
                &app.actor.account.id,
                "bot-recovery",
            ),
        )
        .unwrap();
    put_generated_key_ref(&store, &conflict.id, conflict_payload.key_version).await;
    store
        .update_generated(&conflict.id, &conflict_payload, "digest-conflict")
        .await
        .unwrap();
    store.mark_committing(&conflict.id).await.unwrap();
    store
        .mark_conflict(&conflict.id, "cas rejected")
        .await
        .unwrap();

    let interrupted = store
        .claim_operation(claim_request(
            &app.actor.account.id,
            "bot-recovery",
            23,
            "append_turn",
        ))
        .await
        .unwrap();
    store
        .update_snapshot_ready(&interrupted.id, None, None, None, None)
        .await
        .unwrap();
    store.update_generating(&interrupted.id).await.unwrap();
    store.mark_interrupted(&interrupted.id).await.unwrap();

    let delivered = store
        .claim_operation(claim_request(
            &app.actor.account.id,
            "bot-recovery",
            24,
            "append_turn",
        ))
        .await
        .unwrap();
    store
        .update_snapshot_ready(&delivered.id, None, None, None, None)
        .await
        .unwrap();
    let delivered_payload = encryptor
        .encrypt(
            b"synthetic-generated",
            &OperationAad::new(
                &delivered.id,
                locator_hash("synthetic-handle", "Synthetic.png", "synthetic.jsonl"),
                "append_turn",
                &app.actor.account.id,
                "bot-recovery",
            ),
        )
        .unwrap();
    put_generated_key_ref(&store, &delivered.id, delivered_payload.key_version).await;
    store
        .update_generated(&delivered.id, &delivered_payload, "digest-delivered")
        .await
        .unwrap();
    store.mark_committing(&delivered.id).await.unwrap();
    store
        .mark_committed(&delivered.id, "{\"status\":\"applied\"}")
        .await
        .unwrap();
    store.mark_delivered(&delivered.id).await.unwrap();
    sqlx::query("UPDATE bridge_operations SET created_at = '2026-01-01T00:00:00Z' WHERE id = ?")
        .bind(&delivered.id)
        .execute(&app.pool)
        .await
        .unwrap();

    let eligible = store
        .dry_run_retention_cleanup("2026-09-01T00:00:00Z")
        .await
        .unwrap();
    assert_eq!(eligible, vec![delivered.id.clone()]);
    assert!(!eligible.contains(&conflict.id));
    assert!(!eligible.contains(&interrupted.id));
    assert!(!eligible.contains(&dangling.id));
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bridge_operations")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(remaining, 4);
}
