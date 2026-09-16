use im_bridge::adapters::st::backend::ReqwestStBackend;
use im_bridge::adapters::st::decoder::{
    decode_character_summaries, decode_chat_messages, decode_chat_summaries, decode_model_catalog,
    decode_settings_model,
};
use im_bridge::config::StClientConfig;
use im_bridge::domain::st::{PollerRuntimeFence, StChatLocator};
use im_bridge::modules::bridge::errors::StErrorCode;
use im_bridge::seams::st_backend::StBackend;
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn st_config(base_url: String, mode: &str) -> StClientConfig {
    StClientConfig {
        base_url: Some(base_url),
        handle: "default-user".into(),
        host_header: None,
        timeout_ms: 5_000,
        generate_hard_timeout_ms: 5_000,
        generate_idle_timeout_ms: 5_000,
        mode: mode.into(),
        connector_hmac_key: None,
    }
}

async fn mount_csrf(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/csrf-token"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("set-cookie", "connect.sid=synthetic-session")
                .set_body_json(json!({"token": "synthetic-csrf"})),
        )
        .mount(server)
        .await;
}

#[test]
fn decoder_reads_synthetic_character_chat_settings_and_models() {
    let characters =
        serde_json::from_str(include_str!("fixtures/st/characters-list.json")).unwrap();
    let decoded = decode_character_summaries(&characters).unwrap();
    assert_eq!(decoded[0].avatar, "SyntheticCharacter.png");
    assert_eq!(decoded[0].name, "Synthetic Character");

    let chats = serde_json::from_str(include_str!("fixtures/st/chat-search.json")).unwrap();
    let decoded_chats = decode_chat_summaries(&chats).unwrap();
    assert_eq!(
        decoded_chats[0].chat_file,
        "SyntheticCharacter - 2026-09-04.jsonl"
    );

    let settings = serde_json::from_str(include_str!("fixtures/st/settings.json")).unwrap();
    assert_eq!(
        decode_settings_model(&settings).unwrap().as_deref(),
        Some("synthetic-model")
    );

    let models = serde_json::from_str(include_str!("fixtures/st/model-catalog.json")).unwrap();
    let catalog = decode_model_catalog(&models, Some("synthetic-model".into()));
    assert_eq!(catalog.models[0].id, "synthetic-model");

    let invalid =
        serde_json::from_str(include_str!("fixtures/st/chat-invalid-payload.json")).unwrap();
    let error = decode_chat_messages(&invalid).expect_err("non-array payload");
    assert_eq!(error.code, StErrorCode::StChatPayloadInvalid);
}

#[tokio::test]
async fn session_retries_once_on_403_and_then_reads_characters() {
    let server = MockServer::start().await;
    mount_csrf(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/characters/all"))
        .and(header("x-csrf-token", "synthetic-csrf"))
        .respond_with(ResponseTemplate::new(403))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/characters/all"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            include_str!("fixtures/st/characters-list.json"),
            "application/json",
        ))
        .mount(&server)
        .await;

    let backend = ReqwestStBackend::new(st_config(server.uri(), "read_only"));
    let characters = backend.list_characters().await.unwrap();
    assert_eq!(characters[0].name, "Synthetic Character");
}

#[tokio::test]
async fn disabled_mode_does_not_call_catalog_and_existing_chat_read_uses_search_confirmed_locator()
{
    let server = MockServer::start().await;
    mount_csrf(&server).await;
    let backend = ReqwestStBackend::new(st_config(server.uri(), "disabled"));
    let error = backend
        .list_characters()
        .await
        .expect_err("disabled mode must not read");
    assert_eq!(error.code, StErrorCode::StWriteNotReady);

    Mock::given(method("POST"))
        .and(path("/api/chats/search"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            include_str!("fixtures/st/chat-search.json"),
            "application/json",
        ))
        .mount(&server)
        .await;
    let readable = ReqwestStBackend::new(st_config(server.uri(), "read_only"));
    let chats = readable.list_chats("SyntheticCharacter.png").await.unwrap();
    assert_eq!(chats.len(), 1);
    let snapshot = readable
        .snapshot(&StChatLocator {
            handle: "default-user".into(),
            avatar: "SyntheticCharacter.png".into(),
            character_name: "Synthetic Character".into(),
            chat_file: chats[0].chat_file.clone(),
        })
        .await
        .expect_err("snapshot without connector HMAC must fail closed");
    assert_eq!(snapshot.code, StErrorCode::StConnectorUnavailable);
}

#[tokio::test]
async fn generate_and_commit_stay_frozen_in_read_only_mode() {
    let server = MockServer::start().await;
    mount_csrf(&server).await;
    let backend = ReqwestStBackend::new(st_config(server.uri(), "read_only"));
    let generate = backend
        .stream_generate(
            im_bridge::domain::st::StGenerationRequest {
                model_id: None,
                messages: Vec::new(),
                temperature: None,
                top_p: None,
                max_tokens: None,
            },
            None,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect_err("generate frozen");
    assert_eq!(generate.code, StErrorCode::StWriteNotReady);
    let commit = backend
        .commit(im_bridge::domain::st::CommitStChat {
            operation_id: "op".into(),
            locator: StChatLocator {
                handle: "default-user".into(),
                avatar: "Synthetic.png".into(),
                character_name: "Synthetic".into(),
                chat_file: "synthetic.jsonl".into(),
            },
            expected_sha256: "sha".into(),
            expected_integrity: "int".into(),
            mutation: im_bridge::domain::st::StMutation::UndoLastTurn {
                expected_user_sha256: "u".into(),
                expected_assistant_sha256: "a".into(),
            },
            fence: PollerRuntimeFence {
                telegram_bot_id: 1,
                owner: "rust_bridge".into(),
                epoch: 1,
                internal_bot_id: "synthetic-bot".into(),
                runtime_instance_id: "synthetic-runtime".into(),
            },
        })
        .await
        .expect_err("commit frozen");
    assert_eq!(commit.code, StErrorCode::StWriteNotReady);
}

#[tokio::test]
async fn html_error_body_does_not_become_structured_code() {
    let server = MockServer::start().await;
    mount_csrf(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/characters/all"))
        .respond_with(
            ResponseTemplate::new(500)
                .insert_header("content-type", "text/html")
                .set_body_string(include_str!("fixtures/st/error-html.html")),
        )
        .mount(&server)
        .await;
    let backend = ReqwestStBackend::new(st_config(server.uri(), "read_only"));
    let error = backend.list_characters().await.expect_err("html 500");
    assert_eq!(error.code, StErrorCode::StCharacterListFailed);
    assert_ne!(error.code.as_str(), "Synthetic Error");
}

#[test]
fn write_mode_requires_a_strong_connector_key() {
    let mut config = st_config("http://127.0.0.1:18000".into(), "test_write");
    config.connector_hmac_key = Some("short".into());
    assert!(config.validate().is_err());
    config.connector_hmac_key = Some("0123456789abcdef0123456789abcdef".into());
    assert!(config.validate().is_ok());
}

#[test]
fn non_loopback_plain_http_is_rejected() {
    assert!(
        im_bridge::config::validate_service_url("TEST_URL", "http://192.0.2.10:18000", true,)
            .is_err()
    );
    assert!(
        im_bridge::config::validate_service_url("TEST_URL", "http://127.0.0.1:18000", true,)
            .is_ok()
    );
}
