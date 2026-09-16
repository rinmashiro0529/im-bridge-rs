use im_bridge::adapters::st::sse::{
    decode_st_generation_event, SseDecodeError, StGenerationEvent, StSseEvent, StSseStreamDecoder,
};
use im_bridge::domain::st::{StGenerationRequest, StGenerationSettings};
use im_bridge::modules::bridge::st_ops::{generate_payload, generate_stream_payload};

#[test]
fn sse_stream_decoder_complete_stream() {
    let payload = b"data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\" world!\"}}]}\n\ndata: [DONE]\n\n";
    let mut decoder = StSseStreamDecoder::new(4096, 16384);
    let events = decoder.push(payload).expect("push must succeed");
    assert_eq!(events.len(), 3);

    assert_eq!(
        events[0],
        StSseEvent::Message {
            event: None,
            data: "{\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}".into()
        }
    );
    assert_eq!(
        events[1],
        StSseEvent::Message {
            event: None,
            data: "{\"choices\":[{\"delta\":{\"content\":\" world!\"}}]}".into()
        }
    );
    assert_eq!(
        events[2],
        StSseEvent::Message {
            event: None,
            data: "[DONE]".into()
        }
    );

    assert!(decoder.finish().is_ok());
}

#[test]
fn sse_stream_decoder_byte_by_byte_consistency() {
    let payload = b": heartbeat\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"\xE4\xBD\xA0\xE5\xA5\xBD\"}}]}\r\n\r\ndata: [DONE]\n\n";

    // 1. One whole chunk
    let mut decoder_full = StSseStreamDecoder::new(4096, 16384);
    let events_full = decoder_full.push(payload).unwrap();
    assert!(decoder_full.finish().is_ok());

    // 2. Byte-by-byte feed
    let mut decoder_bytes = StSseStreamDecoder::new(4096, 16384);
    let mut events_bytes = Vec::new();
    for &b in payload.iter() {
        let chunk = [b];
        let mut evs = decoder_bytes.push(&chunk).unwrap();
        events_bytes.append(&mut evs);
    }
    assert!(decoder_bytes.finish().is_ok());

    assert_eq!(events_full, events_bytes);
    assert_eq!(events_full.len(), 3);
    assert_eq!(events_full[0], StSseEvent::Heartbeat);
}

#[test]
fn sse_stream_decoder_utf8_bom_stripping() {
    let with_bom = b"\xEF\xBB\xBFdata: hello\n\n";
    let mut decoder = StSseStreamDecoder::new(4096, 16384);
    let events = decoder.push(with_bom).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0],
        StSseEvent::Message {
            event: None,
            data: "hello".into()
        }
    );
}

#[test]
fn sse_stream_decoder_limits_line_and_event() {
    // Line too large
    let mut decoder_line = StSseStreamDecoder::new(10, 100);
    let long_line = b"data: 123456789012345\n\n";
    assert_eq!(
        decoder_line.push(long_line),
        Err(SseDecodeError::LineTooLarge)
    );

    // Event too large
    let mut decoder_event = StSseStreamDecoder::new(100, 20);
    let multi_data = b"data: 1234567890\ndata: 123456789012345\n\n";
    assert_eq!(
        decoder_event.push(multi_data),
        Err(SseDecodeError::EventTooLarge)
    );
}

#[test]
fn sse_stream_decoder_truncation_detection() {
    let mut decoder = StSseStreamDecoder::new(4096, 16384);
    decoder.push(b"data: incomplete").unwrap();
    assert_eq!(decoder.finish(), Err(SseDecodeError::Truncated));
}

#[test]
fn decode_generation_event_variants() {
    let delta =
        decode_st_generation_event("{\"choices\":[{\"delta\":{\"content\":\"chunk\"}}]}", 1);
    assert_eq!(
        delta,
        Ok(vec![StGenerationEvent::TextDelta {
            sequence: 1,
            text: "chunk".into()
        }])
    );

    let finished = decode_st_generation_event(
        "{\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}",
        2,
    );
    assert_eq!(
        finished,
        Ok(vec![StGenerationEvent::Finished {
            finish_reason: Some("stop".into())
        }])
    );

    let done = decode_st_generation_event("[DONE]", 3);
    assert_eq!(
        done,
        Ok(vec![StGenerationEvent::Finished {
            finish_reason: Some("stop".into())
        }])
    );

    let error = decode_st_generation_event("{\"error\":{\"message\":\"quota exceeded\"}}", 4);
    assert_eq!(
        error,
        Ok(vec![StGenerationEvent::Rejected {
            safe_code: "ST_GENERATION_FAILED".into()
        }])
    );
}

#[test]
fn reasoning_content_is_ignored_before_text() {
    let reasoning = decode_st_generation_event(
        r#"{"choices":[{"delta":{"reasoning_content":"private thought"}}]}"#,
        5,
    );
    assert_eq!(reasoning, Ok(vec![StGenerationEvent::Ignored]));

    let content = decode_st_generation_event(r#"{"choices":[{"delta":{"content":"visible"}}]}"#, 6);
    assert_eq!(
        content,
        Ok(vec![StGenerationEvent::TextDelta {
            sequence: 6,
            text: "visible".into(),
        }])
    );
    let done = decode_st_generation_event("[DONE]", 7).unwrap();
    assert_eq!(
        done,
        vec![StGenerationEvent::Finished {
            finish_reason: Some("stop".into()),
        }]
    );
}

#[test]
fn reasoning_and_content_same_delta_only_emits_content() {
    let events = decode_st_generation_event(
        r#"{"choices":[{"delta":{"reasoning_content":"private thought","content":"visible"}}]}"#,
        7,
    )
    .unwrap();
    assert_eq!(
        events,
        vec![StGenerationEvent::TextDelta {
            sequence: 7,
            text: "visible".into(),
        }]
    );
}

#[test]
fn usage_only_empty_choices_is_ignored_but_bare_empty_choices_are_invalid() {
    assert_eq!(
        decode_st_generation_event(
            r#"{"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[],"usage":{"total_tokens":1}}"#,
            8,
        ),
        Ok(vec![StGenerationEvent::Ignored])
    );
    assert_eq!(
        decode_st_generation_event(r#"{"choices":[]}"#, 9),
        Err(SseDecodeError::InvalidPayload)
    );
}

#[test]
fn unsupported_delta_shapes_remain_rejected() {
    assert_eq!(
        decode_st_generation_event(r#"{"choices":[{"delta":{"tool_calls":[]}}]}"#, 10,),
        Ok(vec![StGenerationEvent::Rejected {
            safe_code: "ST_GENERATION_UNSUPPORTED_OUTPUT".into(),
        }])
    );
    assert_eq!(
        decode_st_generation_event(r#"{"choices":[{"delta":{"unexpected":"value"}}]}"#, 11,),
        Err(SseDecodeError::UnsupportedOutput)
    );
    assert_eq!(
        decode_st_generation_event(
            r#"{"choices":[{"message":{"content":"not a stream"}}]}"#,
            12,
        ),
        Err(SseDecodeError::UnsupportedOutput)
    );
    assert_eq!(
        decode_st_generation_event(r#"{"choices":[{"delta":{"reasoning_content":42}}]}"#, 13,),
        Err(SseDecodeError::InvalidPayload)
    );
}

#[test]
fn generate_stream_payload_sets_stream_true() {
    let settings = StGenerationSettings {
        username: "User".into(),
        chat_completion_source: "custom".into(),
        model: "gpt-4".into(),
        custom_url: "http://localhost".into(),
        custom_prompt_post_processing: "".into(),
        temperature: 0.8,
        top_p: 0.9,
        max_tokens: 200,
    };
    let request = StGenerationRequest {
        model_id: None,
        messages: Vec::new(),
        temperature: None,
        top_p: None,
        max_tokens: None,
    };

    let non_stream = generate_payload(&settings, &request);
    assert_eq!(non_stream["stream"], false);

    let stream = generate_stream_payload(&settings, &request);
    assert_eq!(stream["stream"], true);
}
