use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};
use tokio::time::timeout;

use crate::error::{AppError, AppResult};
use crate::seams::llm_gateway::{
    LlmCompletion, LlmGateway, LlmRequest, ModelDescriptor, ProgressEvent, ProgressSink,
    ResolvedProvider,
};

pub struct OpenAiCompatibleGateway {
    client: reqwest::Client,
}

impl Default for OpenAiCompatibleGateway {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenAiCompatibleGateway {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .no_proxy()
                .build()
                .expect("reqwest client"),
        }
    }

    fn endpoint(provider: &ResolvedProvider, suffix: &str) -> String {
        format!(
            "{}/{}",
            provider.base_url.trim_end_matches('/'),
            suffix.trim_start_matches('/')
        )
    }

    fn headers(provider: &ResolvedProvider) -> AppResult<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(api_key) = &provider.api_key {
            let value = format!("Bearer {api_key}");
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&value)
                    .map_err(|_| AppError::internal("invalid api key header"))?,
            );
        }
        for (name, value) in &provider.custom_headers {
            let header_name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
                AppError::bad_request("INVALID_HEADER", format!("invalid header name {name}"))
            })?;
            let header_value = HeaderValue::from_str(value).map_err(|_| {
                AppError::bad_request("INVALID_HEADER", format!("invalid header value for {name}"))
            })?;
            headers.insert(header_name, header_value);
        }
        Ok(headers)
    }
}

#[async_trait]
impl LlmGateway for OpenAiCompatibleGateway {
    async fn list_models(&self, provider: &ResolvedProvider) -> AppResult<Vec<ModelDescriptor>> {
        let response = self
            .client
            .get(Self::endpoint(provider, "/v1/models"))
            .headers(Self::headers(provider)?)
            .timeout(Duration::from_secs(15))
            .send()
            .await;
        let Ok(response) = response else {
            return Ok(Vec::new());
        };
        if !response.status().is_success() {
            return Ok(Vec::new());
        }
        let payload: Value = response.json().await.unwrap_or(json!({}));
        let data = payload
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok(data
            .into_iter()
            .filter_map(|item| {
                let id = item.get("id")?.as_str()?.to_string();
                Some(ModelDescriptor {
                    id,
                    owned_by: item
                        .get("owned_by")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    description: item
                        .get("description")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                })
            })
            .collect())
    }

    async fn stream_chat(
        &self,
        provider: &ResolvedProvider,
        request: LlmRequest,
        progress: Option<&dyn ProgressSink>,
    ) -> AppResult<LlmCompletion> {
        let mut payload = json!({
            "model": provider.model,
            "messages": request.messages,
            "temperature": provider.temperature,
            "top_p": provider.top_p,
            "max_tokens": provider.max_tokens,
            "stream": request.stream,
        });
        if !provider.custom_prompt_post_processing.is_empty() {
            payload["custom_prompt_post_processing"] =
                json!(provider.custom_prompt_post_processing);
        }
        let response = self
            .client
            .post(Self::endpoint(provider, "/v1/chat/completions"))
            .headers(Self::headers(provider)?)
            .json(&payload)
            .send()
            .await
            .map_err(|_| AppError::bad_gateway("GENERATE_FAILED", "provider request failed"))?;
        if !response.status().is_success() {
            let status = response.status();
            return Err(AppError::bad_gateway(
                "GENERATE_FAILED",
                format!("provider returned HTTP {status}"),
            ));
        }
        if request.stream {
            parse_sse(response, provider, progress).await
        } else {
            let payload: Value = response.json().await.map_err(|_| {
                AppError::bad_gateway("GENERATE_FAILED", "provider returned invalid JSON")
            })?;
            extract_completion(&payload)
        }
    }
}

async fn parse_sse(
    response: reqwest::Response,
    provider: &ResolvedProvider,
    progress: Option<&dyn ProgressSink>,
) -> AppResult<LlmCompletion> {
    let mut stream = response.bytes_stream();
    let mut buffer = String::new();
    let mut full_text = String::new();
    let mut last_event = Instant::now();
    let hard_deadline = Instant::now() + Duration::from_millis(provider.hard_timeout_ms.max(1_000));
    let idle = Duration::from_millis(provider.idle_timeout_ms.max(1_000));
    loop {
        if Instant::now() > hard_deadline {
            return Err(AppError::bad_gateway(
                "GENERATE_TIMEOUT",
                "generation hard timeout",
            ));
        }
        let remaining_idle = idle.saturating_sub(last_event.elapsed());
        let chunk = timeout(remaining_idle, stream.next())
            .await
            .map_err(|_| AppError::bad_gateway("GENERATE_TIMEOUT", "generation idle timeout"))?;
        let Some(chunk) = chunk else {
            break;
        };
        let bytes = chunk
            .map_err(|_| AppError::bad_gateway("GENERATE_FAILED", "provider stream failed"))?;
        last_event = Instant::now();
        buffer.push_str(&String::from_utf8_lossy(&bytes).replace('\r', "\n"));
        while let Some(index) = buffer.find("\n\n") {
            let raw_event = buffer[..index].to_string();
            buffer = buffer[index + 2..].to_string();
            for line in raw_event.lines() {
                let line = line.trim();
                let Some(data) = line.strip_prefix("data:") else {
                    continue;
                };
                let data = data.trim();
                if data.is_empty() {
                    continue;
                }
                if data == "[DONE]" {
                    return Ok(LlmCompletion {
                        text: full_text,
                        finish_reason: Some("stop".into()),
                        usage_json: None,
                    });
                }
                let parsed: Value = serde_json::from_str(data).unwrap_or(json!({}));
                if let Some(delta) = extract_delta(&parsed) {
                    full_text.push_str(&delta);
                    if let Some(progress) = progress {
                        progress
                            .emit(ProgressEvent::Delta {
                                text: delta,
                                full_text: full_text.clone(),
                            })
                            .await?;
                    }
                }
                if parsed.get("error").is_some() {
                    return Err(AppError::bad_gateway(
                        "GENERATE_FAILED",
                        "provider returned an error event",
                    ));
                }
            }
        }
    }
    if full_text.trim().is_empty() {
        return Err(AppError::bad_gateway(
            "GENERATE_EMPTY",
            "生成响应中没有可用文本",
        ));
    }
    Ok(LlmCompletion {
        text: full_text,
        finish_reason: Some("stop".into()),
        usage_json: None,
    })
}

fn extract_delta(payload: &Value) -> Option<String> {
    payload
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|choice| choice.get("delta").or_else(|| choice.get("message")))
        .and_then(|message| message.get("content"))
        .and_then(content_to_text)
        .filter(|text| !text.is_empty())
}

fn extract_completion(payload: &Value) -> AppResult<LlmCompletion> {
    if payload.get("error").is_some() {
        return Err(AppError::bad_gateway(
            "GENERATE_FAILED",
            "provider returned an error response",
        ));
    }
    let text = extract_delta(payload).or_else(|| {
        payload
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(|choice| choice.get("message"))
            .and_then(|message| message.get("content"))
            .and_then(content_to_text)
    });
    let Some(text) = text.filter(|value| !value.trim().is_empty()) else {
        return Err(AppError::bad_gateway(
            "GENERATE_EMPTY",
            "生成响应中没有可用文本",
        ));
    };
    Ok(LlmCompletion {
        text,
        finish_reason: payload
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(|choice| choice.get("finish_reason"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        usage_json: payload.get("usage").cloned(),
    })
}

fn content_to_text(content: &Value) -> Option<String> {
    content.as_str().map(ToOwned::to_owned).or_else(|| {
        content.as_array().map(|parts| {
            parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<String>()
        })
    })
}
