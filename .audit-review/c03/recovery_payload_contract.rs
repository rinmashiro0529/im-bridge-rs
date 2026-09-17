use std::sync::Arc;

use im_bridge::adapters::secrets::encrypted_sqlite::EncryptedSqliteVault;
use im_bridge::domain::st::{
    CreateStChat, PollerRuntimeFence, StChatLocator, StMutation, StWriteScope,
};
use im_bridge::modules::bridge::operation_coordinator::OperationCoordinator;
use im_bridge::modules::bridge::operation_payload::{
    locator_hash, OperationPayloadEncryptor, OperationPayloadKeyProvider,
};
use im_bridge::modules::bridge::operation_store::{ClaimOperationRequest, OperationStore};
use im_bridge::modules::bridge::operations::{
    BridgeOperationRecord, OperationAad, OperationRecoveryPayload,
};
use im_bridge::seams::secret_vault::SecretVault;
use im_bridge::seams::st_operation_journal::UnavailableStOperationJournal;
use im_bridge::{AppError, AppResult};
use serde_json::json;
use sha2::{Digest, Sha256};

mod common;

struct Fixture {
    app: common::TestApp,
    backend: Arc<common::st_backend::ScriptedStBackend>,
    store: Arc<OperationStore>,
    key: Arc<OperationPayloadEncryptor>,
    record: BridgeOperationRecord,
    fence: PollerRuntimeFence,
}

impl Fixture {
    async fn new() -> Self {
        let app = common::setup().await;
        sqlx::query(
            "INSERT INTO telegram_bots
                (id, workspace_id, owner_account_id, desired_enabled, created_at, updated_at)
             VALUES ('bot-c03', ?, ?, 0, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        )
        .bind(app.actor.require_workspace().unwrap())
        .bind(&app.actor.account.id)
        .execute(&app.pool)
        .await
        .unwrap();
        let store = Arc::new(OperationStore::new(app.pool.clone()));
        let record = store
            .claim_operation(ClaimOperationRequest {
                id: "operation-c03".into(),
                actor_id: app.actor.account.id.clone(),
                bot_id: "bot-c03".into(),
                telegram_update_id: 1,
                channel_context_key: "context-c03".into(),
                operation_kind: "undo".into(),
                locator: StChatLocator {
                    handle: "synthetic-user".into(),
                    avatar: "Synthetic.png".into(),
                    character_name: "Synthetic".into(),
                    chat_file: "synthetic.jsonl".into(),
                },
                request_id: "request-c03".into(),
                trace_id: "trace-c03".into(),
            })
            .await
            .unwrap();
        Self {
            app,
            backend: Arc::new(common::st_backend::ScriptedStBackend::new()),
            store,
            key: Arc::new(OperationPayloadEncryptor::new([0x53; 32], 7)),
            record,
            fence: PollerRuntimeFence {
                telegram_bot_id: 123,
                owner: "rust_bridge".into(),
                epoch: 2,
                internal_bot_id: "bot-c03".into(),
                runtime_instance_id: "runtime-c03".into(),
            },
        }
    }

    fn fixed(&self, key: Option<Arc<OperationPayloadEncryptor>>) -> OperationCoordinator {
        OperationCoordinator::new(
            self.backend.clone(),
            self.store.clone(),
            key,
            Arc::new(UnavailableStOperationJournal),
        )
    }

    fn managed(&self, version: u32) -> OperationCoordinator {
        let vault: Arc<dyn SecretVault> = Arc::new(EncryptedSqliteVault::new(
            self.app.pool.clone(),
            [0x71; 32],
        ));
        OperationCoordinator::new_with_key_provider(
            self.backend.clone(),
            self.store.clone(),
            Arc::new(OperationPayloadKeyProvider::with_key_version(
                vault,
                self.store.clone(),
                version,
            )),
            Arc::new(UnavailableStOperationJournal),
        )
    }

    fn aad(&self) -> OperationAad {
        OperationAad::new_with_fence(
            self.record.id.clone(),
            locator_hash(
                &self.record.locator.handle,
                &self.record.locator.avatar,
                &self.record.locator.chat_file,
            ),
            self.record.operation_kind.clone(),
            self.record.actor_id.clone(),
            self.fence.clone(),
        )
    }

    fn payloads(&self) -> Vec<OperationRecoveryPayload> {
        let message = im_bridge::domain::st::StChatMessage {
            name: "Synthetic".into(),
            is_user: false,
            mes: "synthetic text".into(),
            send_date: None,
            extra: Default::default(),
        };
        let sha = "synthetic-source-sha".to_string();
        let integrity = "synthetic-integrity".to_string();
        vec![
            OperationRecoveryPayload::Append {
                mutation: StMutation::AppendTurn {
                    user_message: im_bridge::domain::st::StChatMessage {
                        is_user: true,
                        ..message.clone()
                    },
                    assistant_message: message.clone(),
                },
                expected_sha256: sha.clone(),
                expected_integrity: integrity.clone(),
                fence: self.fence.clone(),
            },
            OperationRecoveryPayload::Replace {
                mutation: StMutation::ReplaceLastAssistant {
                    expected_assistant_sha256: "synthetic-assistant-sha".into(),
                    assistant_message: message,
                },
                expected_sha256: sha.clone(),
                expected_integrity: integrity.clone(),
                fence: self.fence.clone(),
            },
            OperationRecoveryPayload::Undo {
                mutation: StMutation::UndoLastTurn {
                    expected_user_sha256: "synthetic-user-sha".into(),
                    expected_assistant_sha256: "synthetic-assistant-sha".into(),
                },
                expected_sha256: sha.clone(),
                expected_integrity: integrity.clone(),
                fence: self.fence.clone(),
            },
            OperationRecoveryPayload::Compress {
                mutation: StMutation::CompressMessages { patches: vec![] },
                expected_sha256: sha,
                expected_integrity: integrity,
                fence: self.fence.clone(),
            },
            OperationRecoveryPayload::Create {
                command: CreateStChat {
                    operation_id: self.record.id.clone(),
                    locator: self.record.locator.clone(),
                    opening_message: json!({"name": "Synthetic", "mes": "hello"}),
                    scope: StWriteScope::TestChat,
                    fence: self.fence.clone(),
                },
            },
        ]
    }

    fn encrypt_bytes(&self, key: &OperationPayloadEncryptor, bytes: &[u8]) -> BridgeOperationRecord {
        let mut record = self.record.clone();
        record.payload = Some(key.encrypt(bytes, &self.aad()).unwrap());
        record
    }

    fn encrypted(&self, payload: &OperationRecoveryPayload) -> BridgeOperationRecord {
        // Pretty JSON deliberately differs from compact serialization. Create's
        // digest authenticates these exact plaintext bytes, not a re-encoding.
        let bytes = serde_json::to_vec_pretty(payload).unwrap();
        let mut record = self.encrypt_bytes(&self.key, &bytes);
        record.mutation_digest = Some(match payload.mutation() {
            Some(mutation) => hex::encode(Sha256::digest(serde_json::to_vec(mutation).unwrap())),
            None => hex::encode(Sha256::digest(&bytes)),
        });
        record
    }

    async fn rejected(
        &self,
        record: &BridgeOperationRecord,
        fence: &PollerRuntimeFence,
        code: &str,
        status: u16,
        message: &str,
    ) {
        let coordinator = self.fixed(Some(self.key.clone()));
        assert_error(
            coordinator.decrypt_recovery_payload(record, "bot-c03", fence),
            code,
            status,
            message,
        );
        assert_error(
            coordinator.decrypt_recovery_payload_for_operation(record, fence).await,
            code,
            status,
            message,
        );
        assert!(self.backend.calls().is_empty());
    }
}

fn assert_error(result: AppResult<OperationRecoveryPayload>, code: &str, status: u16, message: &str) {
    let error: AppError = result.expect_err("recovery must fail closed");
    assert_eq!((error.code, error.status.as_u16(), error.message.as_str()), (code, status, message));
}

#[tokio::test]
async fn fixed_entrypoints_accept_all_variants_with_and_without_digest() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.fixed(Some(fixture.key.clone()));
    for payload in fixture.payloads() {
        let mut record = fixture.encrypted(&payload);
        let original = record.clone();
        for has_digest in [true, false] {
            if !has_digest {
                record.mutation_digest = None;
            }
            let before = record.clone();
            assert_eq!(coordinator.decrypt_recovery_payload(&record, "bot-c03", &fixture.fence).unwrap(), payload);
            assert_eq!(coordinator.decrypt_recovery_payload_for_operation(&record, &fixture.fence).await.unwrap(), payload);
            assert_eq!(record, before);
        }
        assert_eq!(record.payload, original.payload);
    }
    assert!(fixture.backend.calls().is_empty());
}

#[tokio::test]
async fn key_payload_identity_precedence_is_unchanged() {
    let fixture = Fixture::new().await;
    let mut record = fixture.record.clone();
    record.id.clear();
    let mut fence = fixture.fence.clone();
    fence.epoch = 0;
    let unavailable = fixture.fixed(None);
    for result in [
        unavailable.decrypt_recovery_payload(&record, "wrong-bot", &fence),
        unavailable.decrypt_recovery_payload_for_operation(&record, &fence).await,
    ] {
        assert_error(result, "PAYLOAD_KEY_UNAVAILABLE", 503, "operation payload key is unavailable");
    }
    fixture.rejected(&record, &fence, "PAYLOAD_MISSING", 400, "operation recovery payload is missing").await;
    for empty_id in [true, false] {
        let mut record = fixture.encrypted(&fixture.payloads()[2]);
        if empty_id { record.id = " ".into(); } else { record.bot_id = " ".into(); }
        fixture.rejected(&record, &fence, "PAYLOAD_AAD_INVALID", 400, "operation payload identity is incomplete").await;
    }
}

#[tokio::test]
async fn entry_specific_scope_rejections_preserve_messages() {
    let fixture = Fixture::new().await;
    let record = fixture.encrypted(&fixture.payloads()[2]);
    let coordinator = fixture.fixed(Some(fixture.key.clone()));
    for index in 0..6 {
        let mut fence = fixture.fence.clone();
        match index {
            0 => fence.telegram_bot_id = 0,
            1 => fence.owner = "legacy_plugin".into(),
            2 => fence.epoch = 0,
            3 => fence.internal_bot_id = " ".into(),
            4 => fence.runtime_instance_id = " ".into(),
            _ => fence.internal_bot_id = "other-bot".into(),
        }
        assert_error(coordinator.decrypt_recovery_payload(&record, "bot-c03", &fence), "PAYLOAD_AAD_INVALID", 409, "operation payload runtime fence does not match the historical operation");
        assert_error(coordinator.decrypt_recovery_payload_for_operation(&record, &fence).await, "PAYLOAD_AAD_INVALID", 409, "operation recovery fence does not match the historical operation");
    }
    for bot in ["", " ", "other-bot"] {
        assert_error(coordinator.decrypt_recovery_payload(&record, bot, &fixture.fence), "PAYLOAD_AAD_INVALID", 409, "operation payload runtime fence does not match the historical operation");
    }
}

#[tokio::test]
async fn authenticated_identity_fields_cannot_be_retargeted() {
    let fixture = Fixture::new().await;
    for index in 0..12 {
        let mut record = fixture.encrypted(&fixture.payloads()[2]);
        let mut fence = fixture.fence.clone();
        match index {
            0 => record.id.push_str("-other"),
            1 => record.actor_id.push_str("-other"),
            2 => record.operation_kind.push_str("-other"),
            3 => record.locator.handle.push_str("-other"),
            4 => record.locator.avatar.push_str("-other"),
            5 => record.locator.chat_file.push_str("-other"),
            6 => fence.telegram_bot_id += 1,
            7 => fence.epoch += 1,
            8 => fence.runtime_instance_id.push_str("-other"),
            9 => record.actor_id.clear(),
            10 => record.operation_kind.clear(),
            _ => {
                record.bot_id = "other-bot".into();
                fence.internal_bot_id = record.bot_id.clone();
                let coordinator = fixture.fixed(Some(fixture.key.clone()));
                assert_error(coordinator.decrypt_recovery_payload(&record, "other-bot", &fence), "PAYLOAD_DECRYPT_FAILED", 400, "operation payload could not be decrypted");
                assert_error(coordinator.decrypt_recovery_payload_for_operation(&record, &fence).await, "PAYLOAD_DECRYPT_FAILED", 400, "operation payload could not be decrypted");
                continue;
            }
        }
        fixture.rejected(&record, &fence, "PAYLOAD_DECRYPT_FAILED", 400, "operation payload could not be decrypted").await;
    }
}

#[tokio::test]
async fn cipher_nonce_key_and_version_tampering_is_rejected() {
    let fixture = Fixture::new().await;
    for index in 0..3 {
        let mut record = fixture.encrypted(&fixture.payloads()[2]);
        let encrypted = record.payload.as_mut().unwrap();
        match index {
            0 => encrypted.ciphertext[0] ^= 1,
            1 => encrypted.nonce[0] ^= 1,
            _ => encrypted.key_version += 1,
        }
        fixture.rejected(&record, &fixture.fence, "PAYLOAD_DECRYPT_FAILED", 400, "operation payload could not be decrypted").await;
    }
    let record = fixture.encrypted(&fixture.payloads()[2]);
    let wrong = fixture.fixed(Some(Arc::new(OperationPayloadEncryptor::new([0x54; 32], 7))));
    assert_error(wrong.decrypt_recovery_payload(&record, "bot-c03", &fixture.fence), "PAYLOAD_DECRYPT_FAILED", 400, "operation payload could not be decrypted");
    assert_error(wrong.decrypt_recovery_payload_for_operation(&record, &fixture.fence).await, "PAYLOAD_DECRYPT_FAILED", 400, "operation payload could not be decrypted");
}

#[tokio::test]
async fn invalid_json_is_rejected_before_digest() {
    let fixture = Fixture::new().await;
    let mut record = fixture.encrypt_bytes(&fixture.key, b"{not-json");
    record.mutation_digest = Some("wrong-digest".into());
    fixture.rejected(&record, &fixture.fence, "PAYLOAD_RECOVERY_INVALID", 400, "operation recovery payload is invalid").await;
}

#[tokio::test]
async fn invalid_payload_is_rejected_before_digest() {
    let fixture = Fixture::new().await;
    for index in 0..4 {
        let mut payload = fixture.payloads()[2].clone();
        let (code, message) = match &mut payload {
            OperationRecoveryPayload::Undo { mutation, expected_sha256, expected_integrity, fence } => match index {
                0 => {
                    *mutation = StMutation::CompressMessages { patches: vec![] };
                    ("PAYLOAD_RECOVERY_INVALID", "recovery payload mutation kind is not allowed")
                }
                1 => {
                    expected_sha256.clear();
                    ("PAYLOAD_RECOVERY_INVALID", "recovery snapshot identity is required")
                }
                2 => {
                    expected_integrity.clear();
                    ("PAYLOAD_RECOVERY_INVALID", "recovery snapshot identity is required")
                }
                _ => {
                    fence.epoch = 0;
                    ("PAYLOAD_FENCE_INVALID", "operation recovery payload runtime fence is invalid")
                }
            },
            _ => unreachable!(),
        };
        let mut record = fixture.encrypted(&payload);
        record.mutation_digest = Some("wrong-digest".into());
        fixture.rejected(&record, &fixture.fence, code, 400, message).await;
    }
    let mut payload = fixture.payloads().pop().unwrap();
    if let OperationRecoveryPayload::Create { command } = &mut payload { command.operation_id.clear(); }
    let record = fixture.encrypted(&payload);
    fixture.rejected(&record, &fixture.fence, "PAYLOAD_RECOVERY_INVALID", 400, "create operation id is required").await;
}

#[tokio::test]
async fn mismatched_digest_is_rejected_for_every_variant() {
    let fixture = Fixture::new().await;
    for payload in fixture.payloads() {
        let mut record = fixture.encrypted(&payload);
        record.mutation_digest = Some("wrong-digest".into());
        fixture.rejected(&record, &fixture.fence, "PAYLOAD_DIGEST_MISMATCH", 409, "operation recovery payload digest mismatch").await;
    }
}

#[tokio::test]
async fn managed_restore_uses_persisted_key_version_without_writes() {
    let fixture = Fixture::new().await;
    let writer = fixture.managed(7);
    let key = writer.payload_encryptor_for_operation(&fixture.record.id).await.unwrap();
    let payload = fixture.payloads()[2].clone();
    let bytes = serde_json::to_vec_pretty(&payload).unwrap();
    let record = fixture.encrypt_bytes(&key, &bytes);
    let digest = fixture.encrypted(&payload).mutation_digest.unwrap();
    fixture.store.update_snapshot_ready(&record.id, None, None, None, None).await.unwrap();
    fixture.store.update_generated(&record.id, record.payload.as_ref().unwrap(), &digest).await.unwrap();
    drop(writer);
    drop(key);
    let stored = fixture.store.get_operation(&record.id).await.unwrap().unwrap();
    let before = fixture.store.get_operation_key_reference(&record.id).await.unwrap().unwrap();
    let secrets_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM secrets").fetch_one(&fixture.app.pool).await.unwrap();
    let restored = fixture.managed(9);
    assert_eq!(restored.decrypt_recovery_payload_for_operation(&stored, &fixture.fence).await.unwrap(), payload);
    let after = fixture.store.get_operation_key_reference(&record.id).await.unwrap().unwrap();
    assert_eq!(before, after);
    assert_eq!(after.key_version, 7);
    let secrets_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM secrets").fetch_one(&fixture.app.pool).await.unwrap();
    assert_eq!(secrets_before, secrets_after);
    assert_eq!(fixture.store.get_operation(&record.id).await.unwrap().unwrap(), stored);
    assert_error(restored.decrypt_recovery_payload(&stored, "bot-c03", &fixture.fence), "PAYLOAD_KEY_UNAVAILABLE", 503, "operation payload key is unavailable");
    let mut tampered = stored.clone();
    tampered.mutation_digest = Some("wrong-digest".into());
    assert_error(restored.decrypt_recovery_payload_for_operation(&tampered, &fixture.fence).await, "PAYLOAD_DIGEST_MISMATCH", 409, "operation recovery payload digest mismatch");
    assert!(fixture.backend.calls().is_empty());
}

#[tokio::test]
async fn missing_reference_never_creates_a_key_and_precedes_payload_validation() {
    let fixture = Fixture::new().await;
    let managed = fixture.managed(7);
    let secrets_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM secrets").fetch_one(&fixture.app.pool).await.unwrap();
    let mut invalid_fence = fixture.fence.clone();
    invalid_fence.epoch = 0;
    for record in [fixture.record.clone(), fixture.encrypted(&fixture.payloads()[2])] {
        assert_error(managed.decrypt_recovery_payload_for_operation(&record, &invalid_fence).await, "PAYLOAD_KEY_REFERENCE_MISSING", 503, "operation payload key reference is missing");
        assert!(fixture.store.get_operation_key_reference(&record.id).await.unwrap().is_none());
    }
    let secrets_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM secrets").fetch_one(&fixture.app.pool).await.unwrap();
    assert_eq!(secrets_before, secrets_after);
    assert!(fixture.backend.calls().is_empty());
}
