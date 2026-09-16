use im_bridge::modules::bridge::operation_payload::{locator_hash, OperationPayloadEncryptor};
use im_bridge::modules::bridge::operations::OperationAad;

fn aad() -> OperationAad {
    OperationAad::new(
        "op-synthetic",
        "locator-hash-synthetic",
        "append_turn",
        "actor-synthetic",
        "bot-synthetic",
    )
}

#[test]
fn aad_encoding_is_deterministic_and_length_prefixed() {
    let encoded = aad().encode();
    let prefix = b"im-bridge/operation-payload\0";
    assert!(encoded.starts_with(prefix));
    assert_eq!(&encoded[prefix.len()..prefix.len() + 2], 1u16.to_be_bytes());
    assert_eq!(encoded, aad().encode());

    let mut cursor = prefix.len() + 2;
    for part in [
        "op-synthetic",
        "locator-hash-synthetic",
        "append_turn",
        "actor-synthetic",
        "bot-synthetic",
    ] {
        let len = u64::from_be_bytes(encoded[cursor..cursor + 8].try_into().unwrap()) as usize;
        cursor += 8;
        assert_eq!(len, part.len());
        assert_eq!(&encoded[cursor..cursor + len], part.as_bytes());
        cursor += len;
    }
    assert_eq!(cursor, encoded.len());
}

#[test]
fn encrypt_decrypt_round_trip() {
    let encryptor = OperationPayloadEncryptor::new([7u8; 32], 1);
    let payload = encryptor
        .encrypt(b"synthetic-mutation", &aad())
        .expect("encrypt");
    let plaintext = encryptor.decrypt(&payload, &aad()).expect("decrypt");
    assert_eq!(plaintext, b"synthetic-mutation");
}

#[test]
fn tampering_any_aad_field_fails_decrypt() {
    let encryptor = OperationPayloadEncryptor::new([7u8; 32], 1);
    let payload = encryptor.encrypt(b"synthetic-mutation", &aad()).unwrap();
    let original = aad();
    let mutations = [
        OperationAad {
            version: 2,
            ..original.clone()
        },
        OperationAad {
            operation_id: "other-op".into(),
            ..original.clone()
        },
        OperationAad {
            locator_hash: "other-locator".into(),
            ..original.clone()
        },
        OperationAad {
            operation_kind: "undo_last_turn".into(),
            ..original.clone()
        },
        OperationAad {
            actor_id: "other-actor".into(),
            ..original.clone()
        },
        OperationAad {
            bot_id: "other-bot".into(),
            ..original.clone()
        },
    ];
    for mutated in mutations {
        let error = encryptor
            .decrypt(&payload, &mutated)
            .expect_err("tampered aad must fail closed");
        assert_eq!(error.code, "PAYLOAD_DECRYPT_FAILED");
    }
}

#[test]
fn tampering_ciphertext_or_nonce_fails_decrypt() {
    let encryptor = OperationPayloadEncryptor::new([7u8; 32], 1);
    let mut payload = encryptor.encrypt(b"synthetic-mutation", &aad()).unwrap();
    payload.ciphertext[0] ^= 0x01;
    let error = encryptor
        .decrypt(&payload, &aad())
        .expect_err("tampered ciphertext must fail");
    assert_eq!(error.code, "PAYLOAD_DECRYPT_FAILED");

    let mut payload = encryptor.encrypt(b"synthetic-mutation", &aad()).unwrap();
    payload.nonce[0] ^= 0x01;
    let error = encryptor
        .decrypt(&payload, &aad())
        .expect_err("tampered nonce must fail");
    assert_eq!(error.code, "PAYLOAD_DECRYPT_FAILED");
}

#[test]
fn successive_encryptions_use_unique_nonces() {
    let encryptor = OperationPayloadEncryptor::new([7u8; 32], 1);
    let first = encryptor.encrypt(b"synthetic-mutation", &aad()).unwrap();
    let second = encryptor.encrypt(b"synthetic-mutation", &aad()).unwrap();
    assert_ne!(first.nonce, second.nonce);
    assert_ne!(first.ciphertext, second.ciphertext);
}

#[test]
fn locator_hash_is_deterministic_and_length_prefixed() {
    let first = locator_hash("handle", "avatar.png", "chat.jsonl");
    let second = locator_hash("handle", "avatar.png", "chat.jsonl");
    assert_eq!(first, second);
    assert_eq!(first.len(), 64);
    let swapped = locator_hash("handleavatar.png", "", "chat.jsonl");
    assert_ne!(first, swapped);
    let prefix_collision = locator_hash("ab", "c", "d");
    let other = locator_hash("a", "bc", "d");
    assert_ne!(prefix_collision, other);
}
