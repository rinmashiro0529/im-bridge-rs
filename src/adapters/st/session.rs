use std::sync::Arc;
use std::time::Instant;

use reqwest::header::{HeaderMap, SET_COOKIE};
use reqwest::StatusCode;
use serde_json::Value;
use tokio::sync::Mutex;
use zeroize::Zeroize;

use crate::config::StClientConfig;
use crate::modules::bridge::error_mapper::{map_st_error, StErrorFacts, StFailureFacts};
use crate::modules::bridge::errors::{
    CommitState, StBridgeError, StErrorCode, StErrorStage, StResult,
};

#[derive(Clone)]
struct SessionState {
    csrf_token: String,
    cookie_header: String,
}

impl Drop for SessionState {
    fn drop(&mut self) {
        self.csrf_token.zeroize();
        self.cookie_header.zeroize();
    }
}

#[derive(Clone)]
pub struct StSessionManager {
    client: reqwest::Client,
    config: StClientConfig,
    session: Arc<Mutex<Option<SessionState>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryPolicy {
    Never,
    ReadOnlySessionRefresh,
}

struct JsonRequestOptions {
    retry_policy: RetryPolicy,
    timeout_ms: Option<u64>,
}

impl StSessionManager {
    pub fn new(client: reqwest::Client, config: StClientConfig) -> Self {
        Self {
            client,
            config,
            session: Arc::new(Mutex::new(None)),
        }
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    pub async fn invalidate(&self) {
        *self.session.lock().await = None;
    }

    pub async fn csrf_and_cookie(&self) -> StResult<(String, String)> {
        let session = self.ensure_session().await?;
        Ok((session.csrf_token.clone(), session.cookie_header.clone()))
    }

    pub fn handle(&self) -> &str {
        &self.config.handle
    }

    pub fn write_mode(&self) -> crate::domain::st::StWriteMode {
        self.config.write_mode()
    }

    pub fn timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.config.timeout_ms.max(1))
    }

    pub fn config(&self) -> &StClientConfig {
        &self.config
    }

    pub async fn handshake(&self) -> StResult<()> {
        self.ensure_session().await.map(|_| ())
    }

    pub async fn get_json(
        &self,
        path: &str,
        stage: StErrorStage,
        endpoint_class: &str,
    ) -> StResult<Value> {
        self.request_json(
            reqwest::Method::GET,
            path,
            None,
            stage,
            endpoint_class,
            JsonRequestOptions {
                retry_policy: RetryPolicy::ReadOnlySessionRefresh,
                timeout_ms: None,
            },
        )
        .await
    }

    pub async fn post_json(
        &self,
        path: &str,
        body: Value,
        stage: StErrorStage,
        endpoint_class: &str,
    ) -> StResult<Value> {
        self.request_json(
            reqwest::Method::POST,
            path,
            Some(body),
            stage,
            endpoint_class,
            JsonRequestOptions {
                retry_policy: RetryPolicy::Never,
                timeout_ms: None,
            },
        )
        .await
    }

    pub async fn post_json_readonly(
        &self,
        path: &str,
        body: Value,
        stage: StErrorStage,
        endpoint_class: &str,
    ) -> StResult<Value> {
        self.request_json(
            reqwest::Method::POST,
            path,
            Some(body),
            stage,
            endpoint_class,
            JsonRequestOptions {
                retry_policy: RetryPolicy::ReadOnlySessionRefresh,
                timeout_ms: None,
            },
        )
        .await
    }

    pub async fn post_json_timeout(
        &self,
        path: &str,
        body: Value,
        stage: StErrorStage,
        endpoint_class: &str,
        timeout_ms: u64,
    ) -> StResult<Value> {
        self.request_json(
            reqwest::Method::POST,
            path,
            Some(body),
            stage,
            endpoint_class,
            JsonRequestOptions {
                retry_policy: RetryPolicy::Never,
                timeout_ms: Some(timeout_ms),
            },
        )
        .await
    }

    pub async fn post_stream(
        &self,
        path: &str,
        body: Value,
        stage: StErrorStage,
        endpoint_class: &str,
        timeout_ms: u64,
    ) -> StResult<reqwest::Response> {
        let started = Instant::now();
        let session = self.ensure_session().await?;
        let timeout = std::time::Duration::from_millis(timeout_ms.max(1));
        let mut request = self
            .client
            .request(reqwest::Method::POST, self.url(path))
            .timeout(timeout)
            .header("x-csrf-token", &session.csrf_token)
            .header(reqwest::header::COOKIE, &session.cookie_header)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&body);
        if let Some(host) = &self.config.host_header {
            request = request.header(reqwest::header::HOST, host);
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => return Err(self.map_transport(error, stage, endpoint_class, started)),
        };
        let status = response.status();
        if !status.is_success() {
            let status_code = status.as_u16();
            let ct = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(ToOwned::to_owned);
            let bytes = response.bytes().await.ok().map(|b| b.to_vec());
            return Err(map_http(
                stage,
                endpoint_class,
                status_code,
                ct,
                bytes,
                started,
            ));
        }
        Ok(response)
    }

    async fn request_json(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
        stage: StErrorStage,
        endpoint_class: &str,
        options: JsonRequestOptions,
    ) -> StResult<Value> {
        let started = Instant::now();
        let session = self.ensure_session().await?;
        let timeout = options
            .timeout_ms
            .map(|value| std::time::Duration::from_millis(value.max(1)))
            .unwrap_or_else(|| self.timeout());
        let mut request = self
            .client
            .request(method.clone(), self.url(path))
            .timeout(timeout)
            .header("x-csrf-token", &session.csrf_token)
            .header(reqwest::header::COOKIE, &session.cookie_header);
        if let Some(host) = &self.config.host_header {
            request = request.header(reqwest::header::HOST, host);
        }
        if let Some(body) = &body {
            request = request.json(body);
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => return Err(self.map_transport(error, stage, endpoint_class, started)),
        };
        let status = response.status();
        if status == StatusCode::FORBIDDEN
            && matches!(options.retry_policy, RetryPolicy::ReadOnlySessionRefresh)
        {
            *self.session.lock().await = None;
            return Box::pin(self.request_json(
                method,
                path,
                body,
                stage,
                endpoint_class,
                JsonRequestOptions {
                    retry_policy: RetryPolicy::Never,
                    timeout_ms: options.timeout_ms,
                },
            ))
            .await;
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let bytes = response
            .bytes()
            .await
            .map_err(|error| self.map_transport(error, stage, endpoint_class, started))?;
        if !status.is_success() {
            return Err(map_http(
                stage,
                endpoint_class,
                status.as_u16(),
                content_type,
                Some(bytes.to_vec()),
                started,
            ));
        }
        serde_json::from_slice(&bytes).map_err(|_| {
            map_st_error(StErrorFacts {
                stage,
                endpoint_class: Some(endpoint_class.into()),
                operation_id: None,
                commit_state: CommitState::NotStarted,
                attempt: 1,
                duration_ms: Some(elapsed_ms(started)),
                failure: if stage == StErrorStage::Snapshot && endpoint_class.contains("settings") {
                    StFailureFacts::DecodeSettings
                } else {
                    StFailureFacts::DecodeInvalidPayload
                },
            })
        })
    }

    async fn ensure_session(&self) -> StResult<SessionState> {
        if let Some(existing) = self.session.lock().await.clone() {
            return Ok(existing);
        }
        let started = Instant::now();
        let mut request = self
            .client
            .get(self.url("/csrf-token"))
            .timeout(self.timeout());
        if let Some(host) = &self.config.host_header {
            request = request.header(reqwest::header::HOST, host);
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                return Err(self.map_transport(error, StErrorStage::Session, "csrf", started))
            }
        };
        let status = response.status();
        let cookies = cookie_header(response.headers());
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let bytes = response
            .bytes()
            .await
            .map_err(|error| self.map_transport(error, StErrorStage::Session, "csrf", started))?;
        if !status.is_success() {
            return Err(map_http(
                StErrorStage::Session,
                "csrf",
                status.as_u16(),
                content_type,
                Some(bytes.to_vec()),
                started,
            ));
        }
        let payload: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        let token = payload
            .get("token")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let Some(token) = token else {
            return Err(map_st_error(StErrorFacts {
                stage: StErrorStage::Session,
                endpoint_class: Some("csrf".into()),
                operation_id: None,
                commit_state: CommitState::NotStarted,
                attempt: 1,
                duration_ms: Some(elapsed_ms(started)),
                failure: StFailureFacts::Control {
                    code: StErrorCode::StCsrfMissing,
                },
            }));
        };
        if cookies.is_empty() && !allow_cookieless_e2e_session() {
            return Err(map_st_error(StErrorFacts {
                stage: StErrorStage::Session,
                endpoint_class: Some("csrf".into()),
                operation_id: None,
                commit_state: CommitState::NotStarted,
                attempt: 1,
                duration_ms: Some(elapsed_ms(started)),
                failure: StFailureFacts::Control {
                    code: StErrorCode::StSessionRejected,
                },
            }));
        }
        let state = SessionState {
            csrf_token: token.to_string(),
            cookie_header: cookies,
        };
        *self.session.lock().await = Some(state.clone());
        Ok(state)
    }

    pub fn url(&self, path: &str) -> String {
        let base = self
            .config
            .base_url
            .as_deref()
            .unwrap_or("http://127.0.0.1:18000")
            .trim_end_matches('/');
        format!("{base}{path}")
    }

    fn map_transport(
        &self,
        error: reqwest::Error,
        stage: StErrorStage,
        endpoint_class: &str,
        started: Instant,
    ) -> Box<StBridgeError> {
        let failure = if error.is_timeout() {
            StFailureFacts::Timeout
        } else {
            StFailureFacts::Connect
        };
        map_st_error(StErrorFacts {
            stage,
            endpoint_class: Some(endpoint_class.into()),
            operation_id: None,
            commit_state: CommitState::NotStarted,
            attempt: 1,
            duration_ms: Some(elapsed_ms(started)),
            failure,
        })
    }
}

fn allow_cookieless_e2e_session() -> bool {
    #[cfg(feature = "e2e-control")]
    {
        let run_id_present = std::env::var("IMBRIDGE_E2E_RUN_ID")
            .ok()
            .is_some_and(|value| !value.trim().is_empty());
        let control_token_present = std::env::var("IMBRIDGE_E2E_CONTROL_TOKEN")
            .ok()
            .is_some_and(|value| !value.is_empty());
        let fixture_matches = std::env::var("IMBRIDGE_E2E_FIXTURE_SENTINEL")
            .ok()
            .is_some_and(|value| value == "imbridge-sidecar-fixture-v2");
        run_id_present && control_token_present && fixture_matches
    }
    #[cfg(not(feature = "e2e-control"))]
    {
        false
    }
}

fn cookie_header(headers: &HeaderMap) -> String {
    headers
        .get_all(SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .map(|value| value.split(';').next().unwrap_or(value).trim())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join("; ")
}

fn map_http(
    stage: StErrorStage,
    endpoint_class: &str,
    status: u16,
    content_type: Option<String>,
    body: Option<Vec<u8>>,
    started: Instant,
) -> Box<StBridgeError> {
    map_st_error(StErrorFacts {
        stage,
        endpoint_class: Some(endpoint_class.into()),
        operation_id: None,
        commit_state: CommitState::NotStarted,
        attempt: 1,
        duration_ms: Some(elapsed_ms(started)),
        failure: StFailureFacts::Http {
            status,
            content_type,
            body,
        },
    })
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}
