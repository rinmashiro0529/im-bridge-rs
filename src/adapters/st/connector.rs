use futures_util::StreamExt;
use rand::{distributions::Alphanumeric, Rng};
use serde::Deserialize;
use serde_json::{json, Value};
use zeroize::Zeroizing;

use crate::domain::st::{HandshakeResponse, StWriteMode, StWriteScope};

const MAX_CONNECTOR_BODY_BYTES: usize = 4 * 1024 * 1024;
const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SNAPSHOT_MESSAGES: u64 = 1_000_000;
const SHA256_HEX_LENGTH: usize = 64;

use crate::adapters::st::session::StSessionManager;
use crate::domain::st::{
    CommitStChat, CreateStChat, StChatLocator, StChatSnapshot, StCommitResult, StCommitStatus,
};
use crate::modules::bridge::connector_hmac::{
    canonical_string, sign, ConnectorHmacRequest, ALLOWED_CONTENT_TYPE,
};
use crate::modules::bridge::error_mapper::{map_st_error, StErrorFacts, StFailureFacts};
use crate::modules::bridge::errors::{
    CommitState, StBridgeError, StErrorCode, StErrorStage, StResult,
};
use crate::modules::bridge::poller_ownership::{
    PollerError, PollerOwner, PollerOwnershipRecord, PollerOwnershipRegistry, PollerRuntimeBinding,
};
use crate::seams::st_operation_journal::{StOperationJournalRecord, StOperationJournalStatus};

pub struct ConnectorClient {
    session: StSessionManager,
    hmac_key: Zeroizing<Vec<u8>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotWire {
    parsed_chat: Vec<Value>,
    source_sha256: String,
    source_integrity: String,
    source_byte_length: u64,
    source_message_count: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommitWire {
    status: String,
    new_sha256: Option<String>,
    new_integrity: Option<String>,
    byte_length: Option<u64>,
    message_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateWire {
    status: String,
    chat_file: Option<String>,
    #[serde(alias = "newSha256")]
    sha256: Option<String>,
    #[serde(alias = "newIntegrity")]
    integrity: Option<String>,
    byte_length: Option<u64>,
    message_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProbeCapabilitiesWire {
    mode: StWriteMode,
    snapshot: bool,
    typed_mutations: bool,
    integrity_rotation: bool,
    operation_replay: bool,
    durable_write: bool,
    #[serde(default)]
    scopes: Vec<StWriteScope>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProbeWire {
    version: String,
    handle: String,
    profile: String,
    #[serde(default, alias = "connectorMode")]
    mode: Option<StWriteMode>,
    #[serde(default)]
    scopes: Option<Vec<StWriteScope>>,
    #[serde(default)]
    snapshot: Option<bool>,
    #[serde(default)]
    integrity_enabled: Option<bool>,
    #[serde(default)]
    typed_mutations: Option<bool>,
    #[serde(default)]
    integrity_rotation: Option<bool>,
    #[serde(default)]
    operation_replay: Option<bool>,
    #[serde(default)]
    durable_write: Option<bool>,
    handshake_at_unix: i64,
    expires_at_unix: i64,
    #[serde(default)]
    capabilities: Option<ProbeCapabilitiesWire>,
}

impl ProbeWire {
    fn into_handshake(self) -> Option<HandshakeResponse> {
        let capability = self.capabilities;
        let mode = self
            .mode
            .or_else(|| capability.as_ref().map(|value| value.mode))?;
        let scopes = self
            .scopes
            .or_else(|| capability.as_ref().map(|value| value.scopes.clone()))?;
        let snapshot = self
            .snapshot
            .or_else(|| capability.as_ref().map(|value| value.snapshot))?;
        let integrity_enabled = self.integrity_enabled?;
        let typed_mutations = self
            .typed_mutations
            .or_else(|| capability.as_ref().map(|value| value.typed_mutations))?;
        let integrity_rotation = self
            .integrity_rotation
            .or_else(|| capability.as_ref().map(|value| value.integrity_rotation))?;
        let operation_replay = self
            .operation_replay
            .or_else(|| capability.as_ref().map(|value| value.operation_replay))?;
        let durable_write = self
            .durable_write
            .or_else(|| capability.as_ref().map(|value| value.durable_write))?;
        Some(HandshakeResponse {
            version: self.version,
            handle: self.handle,
            profile: self.profile,
            connector_mode: mode,
            scopes,
            snapshot,
            integrity_enabled,
            typed_mutations,
            integrity_rotation,
            operation_replay,
            durable_write,
            handshake_at_unix: self.handshake_at_unix,
            expires_at_unix: self.expires_at_unix,
        })
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == SHA256_HEX_LENGTH && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn invalid_payload(
    stage: StErrorStage,
    operation_id: Option<&str>,
    commit_state: CommitState,
) -> Box<crate::modules::bridge::errors::StBridgeError> {
    let mut error = map_st_error(StErrorFacts {
        stage,
        endpoint_class: Some("connector".into()),
        operation_id: operation_id.map(ToOwned::to_owned),
        commit_state,
        attempt: 1,
        duration_ms: None,
        failure: StFailureFacts::DecodeInvalidPayload,
    });
    error.operation_id = operation_id.map(ToOwned::to_owned);
    error
}

fn unknown_commit(operation_id: &str) -> Box<crate::modules::bridge::errors::StBridgeError> {
    let mut error = map_st_error(StErrorFacts {
        stage: StErrorStage::Commit,
        endpoint_class: Some("connector".into()),
        operation_id: Some(operation_id.to_string()),
        commit_state: CommitState::Unknown,
        attempt: 1,
        duration_ms: None,
        failure: StFailureFacts::Control {
            code: crate::modules::bridge::errors::StErrorCode::StCommitStateUnknown,
        },
    });
    error.operation_id = Some(operation_id.to_string());
    error
}

fn validate_integrity(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256
}

fn validate_snapshot_wire(wire: &SnapshotWire) -> bool {
    if wire.parsed_chat.is_empty()
        || wire.parsed_chat.len() as u64 > MAX_SNAPSHOT_MESSAGES
        || wire.source_byte_length == 0
        || wire.source_byte_length > MAX_SNAPSHOT_BYTES
        || wire.source_message_count == 0
        || wire.source_message_count != wire.parsed_chat.len() as u64
        || !is_sha256(&wire.source_sha256)
        || !validate_integrity(&wire.source_integrity)
    {
        return false;
    }
    let Some(header) = wire.parsed_chat.first().and_then(Value::as_object) else {
        return false;
    };
    let Some(metadata) = header.get("chat_metadata").and_then(Value::as_object) else {
        return false;
    };
    metadata
        .get("integrity")
        .and_then(Value::as_str)
        .is_some_and(|integrity| {
            integrity == wire.source_integrity && validate_integrity(integrity)
        })
}

fn validate_after_fields(
    sha256: Option<&str>,
    integrity: Option<&str>,
    byte_length: Option<u64>,
    message_count: Option<u64>,
) -> bool {
    is_sha256(sha256.unwrap_or(""))
        && validate_integrity(integrity.unwrap_or(""))
        && byte_length.is_some_and(|value| value > 0 && value <= MAX_SNAPSHOT_BYTES)
        && message_count.is_some_and(|value| value > 0 && value <= MAX_SNAPSHOT_MESSAGES)
}

#[derive(Debug, Deserialize)]
struct PollerOwnershipEnvelope {
    record: Option<PollerOwnershipRecord>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OperationJournalWire {
    operation_id: String,
    locator_hash: String,
    mutation_digest: String,
    status: StOperationJournalStatus,
    before_sha256: Option<String>,
    before_integrity: Option<String>,
    after_sha256: Option<String>,
    after_integrity: Option<String>,
    after_byte_length: Option<u64>,
    after_message_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationJournalEnvelope {
    record: Option<OperationJournalWire>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OperationJournalPayload {
    Envelope(OperationJournalEnvelope),
    Record(OperationJournalWire),
}

impl ConnectorClient {
    pub fn new(session: StSessionManager, key: Option<&str>) -> Option<Self> {
        let key = key?.trim();
        if key.len() < 32 {
            return None;
        }
        Some(Self {
            session,
            hmac_key: Zeroizing::new(key.as_bytes().to_vec()),
        })
    }

    pub async fn probe(&self) -> StResult<HandshakeResponse> {
        let payload = self
            .get(
                "/api/plugins/st-im-bridge/connector/v1/probe",
                "",
                StErrorStage::Control,
                None,
                false,
            )
            .await?;
        let wire: ProbeWire = serde_json::from_value(payload)
            .map_err(|_| invalid_payload(StErrorStage::Control, None, CommitState::NotStarted))?;
        let Some(handshake) = wire.into_handshake() else {
            return Err(invalid_payload(
                StErrorStage::Control,
                None,
                CommitState::NotStarted,
            ));
        };
        if handshake.version.trim().is_empty()
            || handshake.handle.trim().is_empty()
            || handshake.profile.trim().is_empty()
            || handshake.handshake_at_unix <= 0
            || handshake.expires_at_unix <= handshake.handshake_at_unix
        {
            return Err(invalid_payload(
                StErrorStage::Control,
                None,
                CommitState::NotStarted,
            ));
        }
        Ok(handshake)
    }

    pub async fn snapshot(&self, locator: &StChatLocator) -> StResult<StChatSnapshot> {
        let body = serde_json::to_vec(&json!({
            "handle": locator.handle,
            "avatar": locator.avatar,
            "chatFile": locator.chat_file,
        }))
        .map_err(|_| invalid_payload(StErrorStage::Snapshot, None, CommitState::NotStarted))?;
        let payload = self
            .post(
                "/api/plugins/st-im-bridge/connector/v1/chats/snapshot",
                body,
                StErrorStage::Snapshot,
                None,
                false,
            )
            .await?;
        let wire: SnapshotWire = serde_json::from_value(payload)
            .map_err(|_| invalid_payload(StErrorStage::Snapshot, None, CommitState::NotStarted))?;
        if !validate_snapshot_wire(&wire) {
            return Err(invalid_payload(
                StErrorStage::Snapshot,
                None,
                CommitState::NotStarted,
            ));
        }
        Ok(StChatSnapshot {
            locator: locator.clone(),
            parsed_chat: wire.parsed_chat,
            source_sha256: wire.source_sha256,
            source_integrity: wire.source_integrity,
            source_byte_length: wire.source_byte_length,
            source_message_count: wire.source_message_count,
        })
    }

    pub async fn create_chat(&self, command: &CreateStChat) -> StResult<StCommitResult> {
        let body = serde_json::to_vec(&json!({
            "operationId": command.operation_id,
            "handle": command.locator.handle,
            "avatar": command.locator.avatar,
            "chatFile": command.locator.chat_file,
            "openingMessage": command.opening_message,
            "fence": command.fence,
        }))
        .map_err(|_| unknown_commit(&command.operation_id))?;
        let payload = self
            .post(
                "/api/plugins/st-im-bridge/connector/v1/chats/create",
                body,
                StErrorStage::Commit,
                Some(&command.operation_id),
                true,
            )
            .await?;
        let wire: CreateWire =
            serde_json::from_value(payload).map_err(|_| unknown_commit(&command.operation_id))?;
        match wire.status.as_str() {
            "not_applied" | "failed" => {
                return Ok(StCommitResult {
                    status: StCommitStatus::NotApplied,
                    new_sha256: wire.sha256,
                    new_integrity: wire.integrity,
                    byte_length: wire.byte_length,
                    message_count: wire.message_count,
                });
            }
            "unknown" => {
                return Ok(StCommitResult {
                    status: StCommitStatus::Unknown,
                    new_sha256: wire.sha256,
                    new_integrity: wire.integrity,
                    byte_length: wire.byte_length,
                    message_count: wire.message_count,
                });
            }
            "applied" | "already_applied" => {}
            _ => return Err(unknown_commit(&command.operation_id)),
        }
        let status = if wire.status == "already_applied" {
            StCommitStatus::AlreadyApplied
        } else {
            StCommitStatus::Applied
        };
        let (
            Some(chat_file),
            Some(sha256),
            Some(integrity),
            Some(byte_length),
            Some(message_count),
        ) = (
            wire.chat_file,
            wire.sha256,
            wire.integrity,
            wire.byte_length,
            wire.message_count,
        )
        else {
            return Err(unknown_commit(&command.operation_id));
        };
        if chat_file.trim().is_empty()
            || chat_file != command.locator.chat_file
            || !validate_after_fields(
                Some(&sha256),
                Some(&integrity),
                Some(byte_length),
                Some(message_count),
            )
        {
            return Err(unknown_commit(&command.operation_id));
        }
        Ok(StCommitResult {
            status,
            new_sha256: Some(sha256),
            new_integrity: Some(integrity),
            byte_length: Some(byte_length),
            message_count: Some(message_count),
        })
    }

    pub async fn commit(&self, command: &CommitStChat) -> StResult<StCommitResult> {
        let body = serde_json::to_vec(&json!({
            "operationId": command.operation_id,
            "handle": command.locator.handle,
            "avatar": command.locator.avatar,
            "chatFile": command.locator.chat_file,
            "expectedSha256": command.expected_sha256,
            "expectedIntegrity": command.expected_integrity,
            "mutation": command.mutation,
            "fence": command.fence,
        }))
        .map_err(|_| unknown_commit(&command.operation_id))?;
        let payload = self
            .post(
                "/api/plugins/st-im-bridge/connector/v1/chats/commit",
                body,
                StErrorStage::Commit,
                Some(&command.operation_id),
                true,
            )
            .await?;
        let wire: CommitWire =
            serde_json::from_value(payload).map_err(|_| unknown_commit(&command.operation_id))?;
        let status = match wire.status.as_str() {
            "applied" => StCommitStatus::Applied,
            "already_applied" => StCommitStatus::AlreadyApplied,
            "not_applied" | "failed" => StCommitStatus::NotApplied,
            "unknown" => StCommitStatus::Unknown,
            _ => return Err(unknown_commit(&command.operation_id)),
        };
        if matches!(
            status,
            StCommitStatus::Applied | StCommitStatus::AlreadyApplied
        ) && !validate_after_fields(
            wire.new_sha256.as_deref(),
            wire.new_integrity.as_deref(),
            wire.byte_length,
            wire.message_count,
        ) {
            return Err(unknown_commit(&command.operation_id));
        }
        Ok(StCommitResult {
            status,
            new_sha256: wire.new_sha256,
            new_integrity: wire.new_integrity,
            byte_length: wire.byte_length,
            message_count: wire.message_count,
        })
    }

    pub async fn get_poller_ownership(
        &self,
        telegram_bot_id: i64,
    ) -> StResult<Option<PollerOwnershipRecord>> {
        if telegram_bot_id <= 0 {
            return Err(map_st_error(StErrorFacts {
                stage: StErrorStage::Control,
                endpoint_class: Some("connector".into()),
                operation_id: None,
                commit_state: CommitState::NotStarted,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::Control {
                    code: crate::modules::bridge::errors::StErrorCode::StPollerNotOwner,
                },
            }));
        }
        let query = format!("telegramBotId={telegram_bot_id}");
        let payload = self
            .get(
                "/api/plugins/st-im-bridge/connector/v1/poller-ownership",
                &query,
                StErrorStage::Control,
                None,
                false,
            )
            .await?;
        let envelope: PollerOwnershipEnvelope = serde_json::from_value(payload)
            .map_err(|_| invalid_payload(StErrorStage::Control, None, CommitState::NotStarted))?;
        Ok(envelope.record)
    }

    async fn get(
        &self,
        path: &str,
        query: &str,
        stage: StErrorStage,
        operation_id: Option<&str>,
        mutation: bool,
    ) -> StResult<Value> {
        self.request_once(
            "GET",
            path,
            query,
            Vec::new(),
            true,
            stage,
            operation_id,
            mutation,
        )
        .await
    }

    async fn post(
        &self,
        path: &str,
        body: Vec<u8>,
        stage: StErrorStage,
        operation_id: Option<&str>,
        mutation: bool,
    ) -> StResult<Value> {
        self.request_once("POST", path, "", body, true, stage, operation_id, mutation)
            .await
    }

    // Request authentication requires the complete canonical request context.
    #[allow(clippy::too_many_arguments)]
    async fn request_once(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Vec<u8>,
        retry_on_403: bool,
        stage: StErrorStage,
        operation_id: Option<&str>,
        mutation: bool,
    ) -> StResult<Value> {
        let commit_state = if mutation {
            CommitState::Unknown
        } else {
            CommitState::NotStarted
        };
        let (csrf_token, cookie_header) =
            self.session.csrf_and_cookie().await.map_err(|mut error| {
                error.operation_id = operation_id.map(ToOwned::to_owned);
                error.commit_state = if mutation {
                    CommitState::NotApplied
                } else {
                    CommitState::NotStarted
                };
                error
            })?;
        let timestamp = time::OffsetDateTime::now_utc().unix_timestamp();
        let nonce: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();
        let request = ConnectorHmacRequest {
            method,
            path,
            query,
            timestamp_unix: timestamp,
            nonce: &nonce,
            content_type: ALLOWED_CONTENT_TYPE,
            body: &body,
        };
        let canonical = canonical_string(&request).map_err(|_| {
            let mut error = map_st_error(StErrorFacts {
                stage,
                endpoint_class: Some("connector".into()),
                operation_id: operation_id.map(ToOwned::to_owned),
                commit_state: CommitState::NotApplied,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::ConnectorAuth,
            });
            error.operation_id = operation_id.map(ToOwned::to_owned);
            error
        })?;
        let signature = sign(&self.hmac_key[..], &canonical).map_err(|_| {
            let mut error = map_st_error(StErrorFacts {
                stage,
                endpoint_class: Some("connector".into()),
                operation_id: operation_id.map(ToOwned::to_owned),
                commit_state: CommitState::NotApplied,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::ConnectorAuth,
            });
            error.operation_id = operation_id.map(ToOwned::to_owned);
            error
        })?;
        let url = if query.is_empty() {
            self.session.url(path)
        } else {
            format!("{}?{query}", self.session.url(path))
        };
        let method = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|_| invalid_payload(stage, operation_id, CommitState::NotApplied))?;
        let mut request = self
            .session
            .client()
            .request(method.clone(), url)
            .timeout(self.session.timeout());
        if let Some(host) = self.session.config().host_header.as_deref() {
            request = request.header(reqwest::header::HOST, host);
        }
        let response = request
            .header("content-type", ALLOWED_CONTENT_TYPE)
            .header("x-csrf-token", csrf_token)
            .header(reqwest::header::COOKIE, cookie_header)
            .header("x-im-bridge-timestamp", timestamp.to_string())
            .header("x-im-bridge-nonce", &nonce)
            .header("x-im-bridge-signature", signature)
            .body(body.clone())
            .send()
            .await
            .map_err(|_| {
                let mut error = map_st_error(StErrorFacts {
                    stage,
                    endpoint_class: Some("connector".into()),
                    operation_id: operation_id.map(ToOwned::to_owned),
                    commit_state,
                    attempt: 1,
                    duration_ms: None,
                    failure: StFailureFacts::ConnectorUnavailable,
                });
                error.operation_id = operation_id.map(ToOwned::to_owned);
                error
            })?;
        let status = response.status();
        if status == reqwest::StatusCode::FORBIDDEN && retry_on_403 && !mutation {
            self.session.invalidate().await;
            return Box::pin(self.request_once(
                method.as_str(),
                path,
                query,
                body,
                false,
                stage,
                operation_id,
                mutation,
            ))
            .await;
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let content_length = response.content_length();
        if content_length.is_some_and(|length| length > MAX_CONNECTOR_BODY_BYTES as u64) {
            return Err(unknown_or_not_applied(
                stage,
                operation_id,
                mutation,
                StFailureFacts::Control {
                    code: crate::modules::bridge::errors::StErrorCode::StCommitPayloadTooLarge,
                },
            ));
        }
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::with_capacity(
            content_length
                .unwrap_or(0)
                .min(MAX_CONNECTOR_BODY_BYTES as u64) as usize,
        );
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| {
                unknown_or_not_applied(
                    stage,
                    operation_id,
                    mutation,
                    StFailureFacts::ConnectorUnavailable,
                )
            })?;
            let next_len = bytes.len().checked_add(chunk.len()).ok_or_else(|| {
                unknown_or_not_applied(
                    stage,
                    operation_id,
                    mutation,
                    StFailureFacts::Control {
                        code: crate::modules::bridge::errors::StErrorCode::StCommitPayloadTooLarge,
                    },
                )
            })?;
            if next_len > MAX_CONNECTOR_BODY_BYTES {
                return Err(unknown_or_not_applied(
                    stage,
                    operation_id,
                    mutation,
                    StFailureFacts::Control {
                        code: crate::modules::bridge::errors::StErrorCode::StCommitPayloadTooLarge,
                    },
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            let rejection_state = if mutation {
                if status.is_client_error() {
                    CommitState::NotApplied
                } else {
                    CommitState::Unknown
                }
            } else {
                CommitState::NotStarted
            };
            return Err(map_st_error(StErrorFacts {
                stage,
                endpoint_class: Some("connector".into()),
                operation_id: operation_id.map(ToOwned::to_owned),
                commit_state: rejection_state,
                attempt: 1,
                duration_ms: None,
                failure: StFailureFacts::Http {
                    status: status.as_u16(),
                    content_type,
                    body: Some(bytes),
                },
            }));
        }
        serde_json::from_slice(&bytes).map_err(|_| {
            if mutation {
                unknown_commit(operation_id.unwrap_or("unknown-operation"))
            } else {
                invalid_payload(stage, operation_id, CommitState::NotStarted)
            }
        })
    }
}

fn unknown_or_not_applied(
    stage: StErrorStage,
    operation_id: Option<&str>,
    mutation: bool,
    failure: StFailureFacts,
) -> Box<crate::modules::bridge::errors::StBridgeError> {
    let commit_state = if mutation {
        CommitState::Unknown
    } else {
        CommitState::NotStarted
    };
    let mut error = map_st_error(StErrorFacts {
        stage,
        endpoint_class: Some("connector".into()),
        operation_id: operation_id.map(ToOwned::to_owned),
        commit_state,
        attempt: 1,
        duration_ms: None,
        failure,
    });
    error.operation_id = operation_id.map(ToOwned::to_owned);
    error
}

#[async_trait::async_trait]
impl PollerOwnershipRegistry for ConnectorClient {
    async fn get_ownership(
        &self,
        telegram_bot_id: i64,
    ) -> Result<Option<PollerOwnershipRecord>, PollerError> {
        if telegram_bot_id <= 0 {
            return Err(PollerError::InvalidBotId);
        }
        self.get_poller_ownership(telegram_bot_id)
            .await
            .map_err(map_poller_error)
    }

    async fn assert_can_start(
        &self,
        telegram_bot_id: i64,
        runtime_owner: PollerOwner,
        runtime_epoch: u64,
    ) -> Result<(), PollerError> {
        let record = self.get_ownership(telegram_bot_id).await?;
        match record {
            None => Err(PollerError::NotRegistered),
            Some(record) if record.owner != runtime_owner => Err(PollerError::NotOwner),
            Some(record) if record.epoch != runtime_epoch => Err(PollerError::EpochMismatch),
            Some(_) => Ok(()),
        }
    }

    async fn assert_fence(
        &self,
        telegram_bot_id: i64,
        owner: PollerOwner,
        binding: &PollerRuntimeBinding,
    ) -> Result<(), PollerError> {
        if owner != PollerOwner::RustBridge
            || telegram_bot_id <= 0
            || binding.internal_bot_id.trim().is_empty()
            || binding.runtime_instance_id.trim().is_empty()
            || binding.epoch == 0
            || !binding.lifecycle.accepts_writes()
        {
            return Err(PollerError::LifecycleInvalid);
        }
        let body = serde_json::to_vec(&json!({
            "telegramBotId": telegram_bot_id,
            "internalBotId": binding.internal_bot_id,
            "runtimeInstanceId": binding.runtime_instance_id,
            "epoch": binding.epoch,
            "lifecycle": binding.lifecycle,
            "leaseUntil": Value::Null,
        }))
        .map_err(|_| PollerError::Conflict)?;
        self.post(
            "/api/plugins/st-im-bridge/connector/v1/poller-runtime/assert",
            body,
            StErrorStage::Control,
            None,
            false,
        )
        .await
        .map(|_| ())
        .map_err(map_poller_error)
    }

    async fn heartbeat(
        &self,
        telegram_bot_id: i64,
        owner: PollerOwner,
        epoch: u64,
    ) -> Result<(), PollerError> {
        let body = serde_json::to_vec(&json!({
            "telegramBotId": telegram_bot_id,
            "owner": owner,
            "epoch": epoch,
        }))
        .map_err(|_| PollerError::Conflict)?;
        self.post(
            "/api/plugins/st-im-bridge/connector/v1/poller-ownership/heartbeat",
            body,
            StErrorStage::Control,
            None,
            false,
        )
        .await
        .map(|_| ())
        .map_err(map_poller_error)
    }

    async fn claim_runtime(
        &self,
        telegram_bot_id: i64,
        binding: PollerRuntimeBinding,
    ) -> Result<(), PollerError> {
        if telegram_bot_id <= 0
            || binding.internal_bot_id.trim().is_empty()
            || binding.runtime_instance_id.trim().is_empty()
            || binding.epoch == 0
            || !binding.lifecycle.can_own()
        {
            return Err(PollerError::LifecycleInvalid);
        }
        let body = serde_json::to_vec(&json!({
            "telegramBotId": telegram_bot_id,
            "internalBotId": binding.internal_bot_id,
            "runtimeInstanceId": binding.runtime_instance_id,
            "epoch": binding.epoch,
            "lifecycle": binding.lifecycle,
            "leaseUntil": Value::Null,
        }))
        .map_err(|_| PollerError::Conflict)?;
        self.post(
            "/api/plugins/st-im-bridge/connector/v1/poller-runtime/claim",
            body,
            StErrorStage::Control,
            None,
            false,
        )
        .await
        .map(|_| ())
        .map_err(map_poller_error)
    }

    async fn release_runtime(
        &self,
        telegram_bot_id: i64,
        runtime_instance_id: &str,
    ) -> Result<(), PollerError> {
        if telegram_bot_id <= 0 || runtime_instance_id.trim().is_empty() {
            return Err(PollerError::InvalidBotId);
        }
        let body = serde_json::to_vec(&json!({
            "telegramBotId": telegram_bot_id,
            "runtimeInstanceId": runtime_instance_id,
        }))
        .map_err(|_| PollerError::Conflict)?;
        self.post(
            "/api/plugins/st-im-bridge/connector/v1/poller-runtime/release",
            body,
            StErrorStage::Control,
            None,
            false,
        )
        .await
        .map(|_| ())
        .map_err(map_poller_error)
    }
}

// The bridge error type is boxed by the shared ST result contract.
#[allow(clippy::boxed_local)]
fn map_poller_error(error: Box<StBridgeError>) -> PollerError {
    match error.code {
        StErrorCode::StPollerNotOwner => PollerError::NotOwner,
        StErrorCode::StPollerEpochMismatch => PollerError::EpochMismatch,
        StErrorCode::StChatLocatorRejected => PollerError::InvalidBotId,
        _ => PollerError::RegistryUnavailable,
    }
}

#[async_trait::async_trait]
impl crate::seams::st_operation_journal::StOperationJournal for ConnectorClient {
    async fn lookup(
        &self,
        operation_id: &str,
    ) -> crate::error::AppResult<Option<crate::seams::st_operation_journal::StOperationJournalRecord>>
    {
        let query = format!("operationId={operation_id}");
        let value = match self
            .get(
                "/api/plugins/st-im-bridge/connector/v1/operations",
                &query,
                StErrorStage::Control,
                Some(operation_id),
                false,
            )
            .await
        {
            Ok(value) => value,
            Err(error)
                if error.code == crate::modules::bridge::errors::StErrorCode::StChatNotFound
                    || error.upstream_http_status == Some(404) =>
            {
                return Ok(None);
            }
            Err(error) => {
                return Err(crate::error::AppError::bad_gateway(
                    "ST_OPERATION_JOURNAL_LOOKUP_FAILED",
                    format!("failed to lookup operation journal: {error}"),
                ));
            }
        };
        if value.is_null() {
            return Ok(None);
        }
        let payload: OperationJournalPayload = serde_json::from_value(value).map_err(|error| {
            crate::error::AppError::bad_request(
                "ST_JOURNAL_DECODE_FAILED",
                format!("invalid journal payload: {error}"),
            )
        })?;
        let wire = match payload {
            OperationJournalPayload::Envelope(envelope) => envelope.record,
            OperationJournalPayload::Record(record) => Some(record),
        };
        let Some(wire) = wire else {
            return Ok(None);
        };
        if matches!(wire.status, StOperationJournalStatus::Applied)
            && !validate_after_fields(
                wire.after_sha256.as_deref(),
                wire.after_integrity.as_deref(),
                wire.after_byte_length,
                wire.after_message_count,
            )
        {
            return Err(crate::error::AppError::bad_request(
                "ST_JOURNAL_DECODE_FAILED",
                "applied journal record is missing valid after-image fields",
            ));
        }
        Ok(Some(StOperationJournalRecord {
            operation_id: wire.operation_id,
            locator_hash: wire.locator_hash,
            mutation_digest: wire.mutation_digest,
            status: wire.status,
            before_sha256: wire.before_sha256,
            before_integrity: wire.before_integrity,
            after_sha256: wire.after_sha256,
            after_integrity: wire.after_integrity,
            after_byte_length: wire.after_byte_length,
            after_message_count: wire.after_message_count,
        }))
    }
}
