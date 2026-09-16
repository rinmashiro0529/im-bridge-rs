use std::collections::BTreeMap;

use im_bridge::domain::st::{StChatMessage, StMutation};
use im_bridge::modules::bridge::st_ops::{locator_hash, message_sha256, mutation_digest};
use serde_json::json;

const ASCII_LOCATOR: &str = "cd1b45e4f7166c908052a5a9cb7fb6ebedc06371afeb1e356c94e533fc7bda92";
const CJK_LOCATOR: &str = "85e4f0397f150d1c884f3d612944c241131e9cdc43e368f73a5643ec536bc25c";
const MESSAGE_HASH: &str = "424c61dda49c827e8b62769c66b9c6aec585e038f4fe2e1d4f124e05399c41ae";
const CJK_MESSAGE_HASH: &str = "b464fff85dd6f80532b68b49e944dbb48851c9e7036d2ee6f3ebe6962c10040c";
const MUTATION_DIGEST: &str = "1e115e1edee89a53109adba4b993b4e85f75438f056195df280784bedf858cb5";

#[test]
fn locator_hash_matches_length_prefixed_golden_fixtures() {
    assert_eq!(
        locator_hash("default-user", "Tokyo.png", "chat-1.jsonl"),
        ASCII_LOCATOR
    );
    assert_eq!(
        locator_hash("默认用户", "角色.png", "对话.jsonl"),
        CJK_LOCATOR
    );
}

#[test]
fn message_sha256_matches_compact_json_golden_fixtures() {
    assert_eq!(
        message_sha256(&json!({"is_user":true,"name":"User","mes":"hello"})),
        MESSAGE_HASH
    );
    assert_eq!(
        message_sha256(&json!({"is_user":false,"name":"角色","mes":"你好"})),
        CJK_MESSAGE_HASH
    );
}

#[test]
fn mutation_digest_matches_append_turn_golden_fixture() {
    let mutation = StMutation::AppendTurn {
        user_message: StChatMessage {
            name: "User".into(),
            is_user: true,
            mes: "hello".into(),
            send_date: None,
            extra: BTreeMap::new(),
        },
        assistant_message: StChatMessage {
            name: "Char".into(),
            is_user: false,
            mes: "hi".into(),
            send_date: None,
            extra: BTreeMap::new(),
        },
    };
    assert_eq!(mutation_digest(&mutation), MUTATION_DIGEST);
}
