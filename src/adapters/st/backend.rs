use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroize;

use crate::adapters::st::connector::ConnectorClient;
use crate::adapters::st::decoder::{
    decode_character_summaries, decode_chat_summaries, decode_generation_settings,
    decode_model_catalog, decode_settings_model,
};
use crate::adapters::st::session::StSessionManager;
use crate::config::StClientConfig;
use crate::domain::st::{
    CommitStChat, CreateStChat, HandshakeResponse, StCapabilities, StCharacterSummary,
    StChatLocator, StChatSnapshot, StChatSummary, StCommitResult, StGenerationRequest,
    StGenerationResult, StGenerationSettings, StModelCatalog, StStatus, StWriteMode, StWriteScope,
};
use crate::modules::bridge::error_mapper::{map_st_error, StErrorFacts, StFailureFacts};
use crate::modules::bridge::errors::{CommitState, StErrorCode, StErrorStage, StResult};
use crate::seams::st_backend::StBackend;

const SSE_MAX_LINE_BYTES: usize = 64 * 1024;
const SSE_MAX_EVENT_BYTES: usize = 256 * 1024;
const GENERATION_MAX_TEXT_BYTES: usize = 4 * 1024 * 1024;

fn valid_stream_content_type(value: Option<&str>) -> bool {
    match value {
        // Pinned ST 1.16.0 forwards the SSE body without forwarding Provider headers.
        None => true,
        Some(value) => value
            .split(';')
            .next()
            .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("text/event-stream")),
    }
}

pub struct ReqwestStBackend {
    session: StSessionManager,
    mode: StWriteMode,
    connector: Option<Arc<ConnectorClient>>,
}

impl ReqwestStBackend {
    pub fn new(mut config: StClientConfig) -> Self {
        let mode = config.write_mode();
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("st reqwest client");
        let mut connector_hmac_key = config.connector_hmac_key.take();
        let session = StSessionManager::new(client, config);
        let connector =
            ConnectorClient::new(session.clone(), connector_hmac_key.as_deref()).map(Arc::new);
        if let Some(key) = connector_hmac_key.as_mut() {
            key.zeroize();
        }
        Self {
            session,
            mode,
            connector,
        }
    }

    pub fn connector_client(&self) -> Option<Arc<ConnectorClient>> {
        self.connector.clone()
    }

    fn handle(&self) -> &str {
        self.session.handle()
    }

    fn not_ready<T>(&self, code: StErrorCode, stage: StErrorStage) -> StResult<T> {
        Err(map_st_error(StErrorFacts {
            stage,
            endpoint_class: Some("control".into()),
            operation_id: None,
            commit_state: CommitState::NotStarted,
            attempt: 1,
            duration_ms: None,
            failure: StFailureFacts::Control { code },
        }))
    }

    fn require_read(&self) -> StResult<()> {
        if self.mode.allows_read() {
            Ok(())
        } else {
            self.not_ready(StErrorCode::StWriteNotReady, StErrorStage::Control)
        }
    }

    fn status_without_connector(&self, available: bool) -> StStatus {
        StStatus {
            available,
            version: None,
            handle: self.handle().to_string(),
            capabilities: StCapabilities {
                mode: if self.mode == StWriteMode::ReadOnly {
                    StWriteMode::ReadOnly
                } else {
                    StWriteMode::Disabled
                },
                ..StCapabilities::default()
            },
        }
    }

    fn status_from_handshake(&self, handshake: &HandshakeResponse) -> StStatus {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0);
        let profile_matches =
            handshake.handle == self.handle() && !handshake.profile.trim().is_empty();
        let local_scope = match self.mode {
            StWriteMode::TestWrite => Some(StWriteScope::TestChat),
            StWriteMode::ProductionWrite => Some(StWriteScope::ProductionChat),
            _ => None,
        };
        let scope_matches = local_scope
            .map(|scope| handshake.supports_scope(scope))
            .unwrap_or(true);
        let capabilities_valid = handshake.is_valid_at(now)
            && profile_matches
            && handshake.integrity_enabled
            && handshake.snapshot
            && handshake.typed_mutations
            && handshake.integrity_rotation
            && handshake.operation_replay
            && handshake.durable_write
            && scope_matches;
        let effective = self.mode.effective(handshake.connector_mode);
        let ready_mode =
            if self.mode.allows_write() && effective.allows_write() && capabilities_valid {
                effective
            } else if self.mode == StWriteMode::ReadOnly {
                StWriteMode::ReadOnly
            } else {
                StWriteMode::Disabled
            };
        StStatus {
            available: true,
            version: Some(handshake.version.clone()),
            handle: handshake.handle.clone(),
            capabilities: StCapabilities {
                mode: ready_mode,
                snapshot: handshake.snapshot,
                typed_mutations: handshake.typed_mutations,
                integrity_rotation: handshake.integrity_rotation,
                operation_replay: handshake.operation_replay,
            },
        }
    }

    async fn require_write(&self, scope: Option<StWriteScope>) -> StResult<StStatus> {
        let status = self.probe().await?;
        if !status.capabilities.mode.allows_write() {
            return self.not_ready(StErrorCode::StWriteNotReady, StErrorStage::Control);
        }
        if let Some(scope) = scope {
            if scope.mode() != status.capabilities.mode {
                return self.not_ready(StErrorCode::StTestScopeRequired, StErrorStage::Control);
            }
        }
        Ok(status)
    }

    fn generation_error(
        &self,
        code: StErrorCode,
    ) -> Box<crate::modules::bridge::errors::StBridgeError> {
        map_st_error(StErrorFacts {
            stage: StErrorStage::Generation,
            endpoint_class: Some("generation".into()),
            operation_id: None,
            commit_state: CommitState::NotStarted,
            attempt: 1,
            duration_ms: None,
            failure: StFailureFacts::Control { code },
        })
    }
}

#[async_trait]
impl StBackend for ReqwestStBackend {
    async fn probe(&self) -> StResult<StStatus> {
        let status = if self.session.handle().trim().is_empty() {
            self.status_without_connector(false)
        } else {
            if let Err(error) = self.session.handshake().await {
                if matches!(
                    error.code,
                    StErrorCode::StConnectFailed | StErrorCode::StRequestTimeout
                ) {
                    self.status_without_connector(false)
                } else {
                    crate::st_readiness::update(false, false);
                    return Err(error);
                }
            } else {
                match self.connector.as_ref() {
                    None => self.status_without_connector(true),
                    Some(connector) => match connector.probe().await {
                        Ok(handshake) => self.status_from_handshake(&handshake),
                        Err(error)
                            if matches!(
                                error.code,
                                StErrorCode::StConnectFailed
                                    | StErrorCode::StRequestTimeout
                                    | StErrorCode::StConnectorUnavailable
                            ) =>
                        {
                            self.status_without_connector(false)
                        }
                        Err(_) => StStatus {
                            available: true,
                            version: None,
                            handle: self.handle().to_string(),
                            capabilities: StCapabilities::default(),
                        },
                    },
                }
            }
        };
        crate::st_readiness::update(status.available, status.capabilities.mode.allows_write());
        Ok(status)
    }

    async fn list_characters(&self) -> StResult<Vec<StCharacterSummary>> {
        self.require_read()?;
        let payload = self
            .session
            .post_json_readonly(
                "/api/characters/all",
                json!({}),
                StErrorStage::Catalog,
                "characters",
            )
            .await?;
        decode_character_summaries(&payload)
    }

    async fn list_chats(&self, avatar: &str) -> StResult<Vec<StChatSummary>> {
        self.require_read()?;
        let payload = self
            .session
            .post_json_readonly(
                "/api/chats/search",
                json!({"avatar_url": avatar, "query": ""}),
                StErrorStage::Catalog,
                "chats",
            )
            .await?;
        decode_chat_summaries(&payload)
    }

    async fn snapshot(&self, locator: &StChatLocator) -> StResult<StChatSnapshot> {
        self.require_read()?;
        if locator.handle != self.handle() {
            return Err(map_st_error(StErrorFacts {
                stage: StErrorStage::Snapshot,
                endpoint_class: Some("connector".into()),
                operation_id: None,
                commit_state: CommitState::NotStarted,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::Control {
                    code: StErrorCode::StChatLocatorRejected,
                },
            }));
        }
        let Some(connector) = &self.connector else {
            return Err(map_st_error(StErrorFacts {
                stage: StErrorStage::Snapshot,
                endpoint_class: Some("connector".into()),
                operation_id: None,
                commit_state: CommitState::NotStarted,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::ConnectorUnavailable,
            }));
        };
        connector.snapshot(locator).await
    }

    async fn list_models(&self) -> StResult<StModelCatalog> {
        self.require_read()?;
        let settings = self
            .session
            .post_json_readonly(
                "/api/settings/get",
                json!({}),
                StErrorStage::Snapshot,
                "settings",
            )
            .await?;
        let current_model = decode_settings_model(&settings)?;
        let mut status_body = json!({"chat_completion_source": "custom"});
        if let Some(settings_text) = settings.get("settings").and_then(Value::as_str) {
            if let Ok(parsed) = serde_json::from_str::<Value>(settings_text) {
                if let Some(source) = parsed
                    .get("oai_settings")
                    .and_then(|item| item.get("chat_completion_source"))
                    .and_then(Value::as_str)
                {
                    status_body["chat_completion_source"] = json!(source);
                }
                if let Some(url) = parsed
                    .get("oai_settings")
                    .and_then(|item| item.get("custom_url"))
                    .and_then(Value::as_str)
                {
                    status_body["custom_url"] = json!(url);
                }
            }
        }
        let payload = self
            .session
            .post_json_readonly(
                "/api/backends/chat-completions/status",
                status_body,
                StErrorStage::Catalog,
                "models",
            )
            .await?;
        Ok(decode_model_catalog(&payload, current_model))
    }

    async fn generation_settings(&self) -> StResult<StGenerationSettings> {
        self.require_read()?;
        let settings = self
            .session
            .post_json_readonly(
                "/api/settings/get",
                json!({}),
                StErrorStage::Snapshot,
                "settings",
            )
            .await?;
        decode_generation_settings(&settings)
    }

    async fn stream_generate(
        &self,
        request: StGenerationRequest,
        progress: Option<Arc<dyn crate::seams::bridge_progress::BridgeProgressSink>>,
        cancel: CancellationToken,
    ) -> StResult<StGenerationResult> {
        let started = tokio::time::Instant::now();
        self.require_write(None).await?;
        let settings = self.generation_settings().await?;
        let payload = crate::modules::bridge::st_ops::generate_stream_payload(&settings, &request);
        let hard_timeout =
            Duration::from_millis(self.session.config().generate_hard_timeout_ms.max(1));
        let idle_timeout =
            Duration::from_millis(self.session.config().generate_idle_timeout_ms.max(1));
        let hard_deadline = started + hard_timeout;
        let mut response = tokio::select! {
            _ = cancel.cancelled() => {
                return Err(self.generation_error(StErrorCode::StGenerateRejected));
            }
            result = tokio::time::timeout_at(
                hard_deadline,
                self.session.post_stream(
                    "/api/backends/chat-completions/generate",
                    payload,
                    StErrorStage::Generation,
                    "generation",
                    hard_timeout.as_millis() as u64,
                ),
            ) => result,
        }
        .map_err(|_| self.generation_error(StErrorCode::StGenerateHardTimeout))??;

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_ascii_lowercase());
        let valid_content_type = valid_stream_content_type(content_type.as_deref());
        if !valid_content_type {
            return Err(self.generation_error(StErrorCode::StGenerateStreamInvalid));
        }

        let (progress_tx, worker_cancel, worker_join) = if let Some(progress_target) = progress {
            let (tx, mut rx) = tokio::sync::watch::channel::<
                Option<crate::seams::bridge_progress::BridgeProgressEvent>,
            >(None);
            let worker_cancel = cancel.child_token();
            let worker_cancel_for_task = worker_cancel.clone();
            let join = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = worker_cancel_for_task.cancelled() => break,
                        changed = rx.changed() => {
                            if changed.is_err() {
                                break;
                            }
                            let Some(event) = rx.borrow().clone() else {
                                continue;
                            };
                            tokio::select! {
                                _ = worker_cancel_for_task.cancelled() => break,
                                result = progress_target.emit(event) => {
                                    if let Err(error) = result {
                                        tracing::warn!(error = %error, "progress sink rejected an update");
                                    }
                                }
                            }
                        }
                    }
                }
            });
            (Some(tx), Some(worker_cancel), Some(join))
        } else {
            (None, None, None)
        };

        let generation_result = async {
            let mut decoder = crate::adapters::st::sse::StSseStreamDecoder::new(
                SSE_MAX_LINE_BYTES,
                SSE_MAX_EVENT_BYTES,
            );
            let mut accumulated_text = String::new();
            let mut finish_reason = None;
            let mut terminal = None;
            let mut sequence: u64 = 0;
            let mut idle_deadline = tokio::time::Instant::now() + idle_timeout;

            'stream: loop {
                let chunk = tokio::select! {
                    _ = cancel.cancelled() => {
                        return Err(self.generation_error(StErrorCode::StGenerateRejected));
                    }
                    _ = tokio::time::sleep_until(hard_deadline) => {
                        return Err(self.generation_error(StErrorCode::StGenerateHardTimeout));
                    }
                    _ = tokio::time::sleep_until(idle_deadline) => {
                        return Err(self.generation_error(StErrorCode::StGenerateIdleTimeout));
                    }
                    result = response.chunk() => {
                        match result {
                            Ok(chunk) => chunk,
                            Err(error) => {
                                let code = if error.is_timeout() {
                                    StErrorCode::StGenerateIdleTimeout
                                } else {
                                    StErrorCode::StGenerateStreamInvalid
                                };
                                return Err(self.generation_error(code));
                            }
                        }
                    }
                };
                let Some(chunk) = chunk else {
                    break;
                };
                let events = decoder
                    .push(&chunk)
                    .map_err(|_| self.generation_error(StErrorCode::StGenerateStreamInvalid))?;
                if !events.is_empty() {
                    idle_deadline = tokio::time::Instant::now() + idle_timeout;
                }
                for event in events {
                    let crate::adapters::st::sse::StSseEvent::Message { event, data } = event
                    else {
                        continue;
                    };
                    if event
                        .as_deref()
                        .is_some_and(|name| name.eq_ignore_ascii_case("error"))
                    {
                        return Err(self.generation_error(StErrorCode::StGenerateRejected));
                    }
                    let stream_done = data.trim() == "[DONE]";
                    sequence = sequence.checked_add(1).ok_or_else(|| {
                        self.generation_error(StErrorCode::StGenerateStreamInvalid)
                    })?;
                    let decoded =
                        crate::adapters::st::sse::decode_st_generation_event(&data, sequence)
                            .map_err(|_| {
                                self.generation_error(StErrorCode::StGenerateStreamInvalid)
                            })?;
                    for gen_event in decoded {
                        if terminal.is_some() {
                            match gen_event {
                                crate::adapters::st::sse::StGenerationEvent::Ignored => {}
                                crate::adapters::st::sse::StGenerationEvent::Finished {
                                    finish_reason: reason,
                                } if reason.as_deref() == Some("stop")
                                    || finish_reason.as_deref() == reason.as_deref() => {}
                                crate::adapters::st::sse::StGenerationEvent::Finished {
                                    ..
                                }
                                | crate::adapters::st::sse::StGenerationEvent::TextDelta {
                                    ..
                                } => {
                                    return Err(
                                        self.generation_error(StErrorCode::StGenerateStreamInvalid)
                                    );
                                }
                                crate::adapters::st::sse::StGenerationEvent::Rejected {
                                    ..
                                } => {
                                    return Err(
                                        self.generation_error(StErrorCode::StGenerateRejected)
                                    );
                                }
                            }
                            continue;
                        }
                        match gen_event {
                            crate::adapters::st::sse::StGenerationEvent::Ignored => {}
                            crate::adapters::st::sse::StGenerationEvent::TextDelta {
                                text, ..
                            } => {
                                let next_len = accumulated_text
                                    .len()
                                    .checked_add(text.len())
                                    .ok_or_else(|| {
                                        self.generation_error(StErrorCode::StGenerateStreamInvalid)
                                    })?;
                                if next_len > GENERATION_MAX_TEXT_BYTES {
                                    return Err(
                                        self.generation_error(StErrorCode::StGenerateStreamInvalid)
                                    );
                                }
                                accumulated_text.push_str(&text);
                                if let Some(progress_tx) = progress_tx.as_ref() {
                                    let progress_event =
                                        crate::seams::bridge_progress::BridgeProgressEvent::Delta {
                                            sequence,
                                            text,
                                            full_text: accumulated_text.clone(),
                                        };
                                    if progress_tx.send(Some(progress_event)).is_err() {
                                        tracing::warn!("progress relay worker is unavailable");
                                    }
                                }
                            }
                            crate::adapters::st::sse::StGenerationEvent::Finished {
                                finish_reason: reason,
                            } => {
                                finish_reason = reason.clone();
                                terminal =
                                    Some(crate::adapters::st::sse::StGenerationEvent::Finished {
                                        finish_reason: reason,
                                    });
                            }
                            crate::adapters::st::sse::StGenerationEvent::Rejected { .. } => {
                                return Err(self.generation_error(StErrorCode::StGenerateRejected));
                            }
                        }
                    }
                    if stream_done {
                        break 'stream;
                    }
                }
            }

            decoder
                .finish()
                .map_err(|_| self.generation_error(StErrorCode::StGenerateStreamInvalid))?;
            let Some(terminal) = terminal else {
                return Err(self.generation_error(StErrorCode::StGenerateStreamInvalid));
            };
            if !terminal.is_accepted_finish() {
                return Err(self.generation_error(StErrorCode::StGenerateStreamInvalid));
            }
            if accumulated_text.trim().is_empty() {
                return Err(self.generation_error(StErrorCode::StGenerateEmpty));
            }

            Ok(StGenerationResult {
                text: accumulated_text,
                finish_reason,
                usage: None,
            })
        }
        .await;

        if let Some(worker_cancel) = worker_cancel {
            worker_cancel.cancel();
        }
        drop(progress_tx);
        if let Some(join) = worker_join {
            if let Err(error) = join.await {
                tracing::warn!(error = %error, "progress relay worker join failed");
            }
        }
        generation_result
    }

    async fn create_chat(&self, command: CreateStChat) -> StResult<StCommitResult> {
        let status = self.require_write(Some(command.scope)).await?;
        if status.capabilities.mode == StWriteMode::TestWrite
            && !command.locator.chat_file.starts_with("IMBridge-Test-")
        {
            return self.not_ready(StErrorCode::StTestScopeRequired, StErrorStage::Control);
        }
        let Some(connector) = &self.connector else {
            return Err(map_st_error(StErrorFacts {
                stage: StErrorStage::Commit,
                endpoint_class: Some("connector".into()),
                operation_id: Some(command.operation_id.clone()),
                commit_state: CommitState::NotStarted,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::ConnectorUnavailable,
            }));
        };
        connector.create_chat(&command).await
    }

    async fn commit(&self, command: CommitStChat) -> StResult<StCommitResult> {
        let status = self.require_write(None).await?;
        if status.capabilities.mode == StWriteMode::TestWrite
            && !command.locator.chat_file.starts_with("IMBridge-Test-")
        {
            return self.not_ready(StErrorCode::StTestScopeRequired, StErrorStage::Control);
        }
        let Some(connector) = &self.connector else {
            return Err(map_st_error(StErrorFacts {
                stage: StErrorStage::Commit,
                endpoint_class: Some("connector".into()),
                operation_id: Some(command.operation_id.clone()),
                commit_state: CommitState::NotStarted,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::ConnectorUnavailable,
            }));
        };
        connector.commit(&command).await
    }
}

#[cfg(test)]
mod tests {
    use super::valid_stream_content_type;

    #[test]
    fn stream_content_type_accepts_sse_and_pinned_st_header_omission() {
        assert!(valid_stream_content_type(None));
        assert!(valid_stream_content_type(Some("text/event-stream")));
        assert!(valid_stream_content_type(Some(
            "Text/Event-Stream; charset=utf-8"
        )));
    }

    #[test]
    fn stream_content_type_rejects_explicit_non_sse_media_types() {
        assert!(!valid_stream_content_type(Some("application/json")));
        assert!(!valid_stream_content_type(Some("application/octet-stream")));
        assert!(!valid_stream_content_type(Some("")));
    }
}
