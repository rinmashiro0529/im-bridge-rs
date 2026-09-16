use im_bridge::modules::telegram::delivery::{TelegramDelivery, TurnMetadata, TurnScope};
use im_bridge::modules::telegram::TelegramModule;
use im_bridge::seams::llm_gateway::{ProgressEvent, ProgressSink};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

#[tokio::test]
async fn telegram_stream_sends_placeholder_edits_and_records_delivery() {
    let app = common::setup().await;
    let telegram = TelegramModule::new(app.pool.clone());
    let bot = telegram.upsert_bot(&app.actor, None, false).await.unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/sendMessage"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"ok": true, "result": {"message_id": 101}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/bottest-token/editMessageText"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"ok": true, "result": true})),
        )
        .mount(&server)
        .await;

    let delivery = TelegramDelivery::new(
        reqwest::Client::new(),
        app.pool.clone(),
        "test-token".into(),
        server.uri(),
        bot.id.clone(),
        1,
        42,
        0,
        0,
        1,
        1,
        3200,
    );
    let scope = TurnScope::new(
        app.actor.account.id.clone(),
        bot.id.clone(),
        1,
        42,
        "0000000000000000000000000000000000000000000000000000000000000000",
    )
    .unwrap();
    let metadata = TurnMetadata::new_scoped(scope, "tg:test:1", "tg:test:1").unwrap();
    let sink = delivery.stream_sink(metadata, "正在生成…").await.unwrap();
    sink.emit(ProgressEvent::Started {
        operation_id: "generation-1".into(),
        conversation_id: "conversation-1".into(),
        revision: 1,
    })
    .await
    .unwrap();
    sink.emit(ProgressEvent::Delta {
        text: "hello".into(),
        full_text: "hello".into(),
    })
    .await
    .unwrap();
    sink.emit(ProgressEvent::Done {
        reply_text: "hello world".into(),
        conversation_revision: 2,
    })
    .await
    .unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.url.path().ends_with("/sendMessage"))
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.url.path().ends_with("/editMessageText"))
            .count(),
        2
    );
    let row: (String, String, String) = sqlx::query_as(
        "SELECT status, external_message_id, generation_run_id FROM channel_deliveries
         WHERE bot_id = ? AND turn_id = 'tg:test:1'",
    )
    .bind(&bot.id)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(row.0, "sent");
    assert_eq!(row.1, "101");
    assert_eq!(row.2, "generation-1");
}
