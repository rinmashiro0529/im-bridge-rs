use im_bridge::adapters::secrets::encrypted_sqlite::EncryptedSqliteVault;
use im_bridge::adapters::telegram::teloxide_runtime::TeloxideRuntime;
use im_bridge::domain::channel::TelegramBot;
use im_bridge::modules::telegram::TelegramModule;

#[tokio::test]
async fn explicit_transport_features_preserve_the_previous_client_capabilities() {
    // These methods intentionally require the features that the unused crate
    // previously enabled. Build requests only; do not contact external hosts.
    let client = reqwest::Client::builder()
        .no_proxy()
        .use_rustls_tls()
        .tls_built_in_native_certs(true)
        .tls_built_in_webpki_certs(true)
        .build()
        .unwrap();
    let request = client
        .post("https://example.invalid/upload")
        .multipart(reqwest::multipart::Form::new().text("field", "synthetic"))
        .build()
        .unwrap();
    assert_eq!(request.method(), reqwest::Method::POST);
    assert!(request.headers()[reqwest::header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("multipart/form-data; boundary="));
}

#[tokio::test]
async fn legacy_runtime_entrypoint_preserves_missing_token_rejection() {
    let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
    let module = TelegramModule::new(pool.clone());
    let vault = EncryptedSqliteVault::new(pool, [9_u8; 32]);
    let bot = TelegramBot {
        id: "transport-test-bot".into(),
        workspace_id: "transport-test-workspace".into(),
        owner_account_id: "transport-test-account".into(),
        token_secret_id: None,
        desired_enabled: false,
        observed_username: None,
        last_error: None,
        inter_message_delay_ms: 0,
        stream_min_interval_ms: 0,
        stream_min_delta_chars: 1,
        stream_first_render_chars: 1,
        stream_chunk_size: 4000,
    };
    let error = TeloxideRuntime::start(&module, &bot, &vault)
        .await
        .unwrap_err();
    assert_eq!(error.code, "BOT_TOKEN_MISSING");
    assert_eq!(module.runtime_status(&bot.id).await, "stopped");
}
