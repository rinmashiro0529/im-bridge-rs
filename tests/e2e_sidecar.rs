#![cfg(feature = "e2e-control")]

//! Gate C tests for the approved isolated sidecar stack.
//!
//! These tests deliberately have no in-process backend, WireMock server, journal,
//! or fallback endpoint. The Rust runtime polls fake Telegram, while observations
//! come from the fake services' read-only trace and durable-ledger controls.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::Client;
use serde_json::{json, Value};

const SIDECAR_CHARACTER: &str = include_str!("fixtures/sidecar/TestCharacter.json");
const SIDECAR_CHAT: &str = include_str!("fixtures/sidecar/chat_fixture.jsonl");
const SIDECAR_SETTINGS: &str = include_str!("fixtures/sidecar/settings.json");
const SENTINEL: &str = "imbridge-sidecar-fixture-v2";
const E2E_INTERNAL_BOT_ID: &str = "test-bot";
const MAX_WAIT: Duration = Duration::from_secs(90);
const CONTROL_TOKEN_HEADER: &str = "x-imbridge-e2e-control-token";
const MAX_CONTROL_TOKEN_LENGTH: usize = 256;
const MAX_SCENARIO_ID_LENGTH: usize = 128;

#[derive(Debug)]
struct HarnessError(String);

impl std::fmt::Display for HarnessError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for HarnessError {}

type E2eResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn boxed_harness_error(error: HarnessError) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(error)
}

#[derive(Clone, Debug)]
struct OperationIdentity {
    external_id: String,
    canonical_id: String,
}

impl OperationIdentity {
    fn for_update(external_id: &str, update: &Value) -> E2eResult<Self> {
        Ok(Self {
            external_id: external_id.to_owned(),
            canonical_id: canonical_operation_id(update)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeOperationStatus {
    Received,
    SnapshotReady,
    Generating,
    Generated,
    Committing,
    Committed,
    Delivered,
    Conflict,
    Failed,
    Interrupted,
}

impl RuntimeOperationStatus {
    fn parse(operation_id: &str, field: &str, raw: &str) -> E2eResult<Self> {
        let status = match raw {
            "received" => Self::Received,
            "snapshot_ready" => Self::SnapshotReady,
            "generating" => Self::Generating,
            "generated" => Self::Generated,
            "committing" => Self::Committing,
            "committed" => Self::Committed,
            "delivered" => Self::Delivered,
            "conflict" => Self::Conflict,
            "failed" => Self::Failed,
            "interrupted" => Self::Interrupted,
            _ => {
                return Err(Box::new(HarnessError(format!(
                    "operation {operation_id} probe field {field} has unknown runtime status {raw:?}"
                ))));
            }
        };
        Ok(status)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitState {
    NotStarted,
    NotApplied,
    Applied,
    Unknown,
}

impl CommitState {
    fn parse(operation_id: &str, field: &str, raw: &str) -> E2eResult<Self> {
        let state = match raw {
            "not_started" => Self::NotStarted,
            "not_applied" => Self::NotApplied,
            "applied" => Self::Applied,
            "unknown" => Self::Unknown,
            _ => {
                return Err(Box::new(HarnessError(format!(
                    "operation {operation_id} probe field {field} has unknown commit state {raw:?}"
                ))));
            }
        };
        Ok(state)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryEffectStatus {
    Pending,
    RetryAt,
    Sent,
    Delivered,
    Failed,
    Unknown,
}

impl DeliveryEffectStatus {
    fn parse(operation_id: &str, field: &str, raw: &str) -> E2eResult<Self> {
        let status = match raw {
            "pending" => Self::Pending,
            "retry_at" | "retry_pending" => Self::RetryAt,
            "sent" => Self::Sent,
            "delivered" => Self::Delivered,
            "failed" => Self::Failed,
            "unknown" => Self::Unknown,
            _ => {
                return Err(Box::new(HarnessError(format!(
                    "operation {operation_id} probe field {field} has unknown delivery status {raw:?}"
                ))));
            }
        };
        Ok(status)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationExpectation {
    CommittedAndDelivered,
    ReconciledAndDelivered,
    CommittingUnknown,
    // Reserved for the isolated negative-commit scenario.
    #[allow(dead_code)]
    RejectedNotApplied,
    RetryPending,
}

#[derive(Debug)]
struct OperationProbe<'a> {
    value: &'a Value,
    runtime_status: Option<RuntimeOperationStatus>,
    commit_state: CommitState,
}

impl<'a> OperationProbe<'a> {
    fn parse(operation_id: &str, value: &'a Value) -> E2eResult<Self> {
        let commit_raw = required_probe_string(value, operation_id, "commit_state")?;
        let commit_state = CommitState::parse(operation_id, "commit_state", commit_raw)?;
        let runtime_status = if let Some(runtime_value) = value.get("runtime_status") {
            let runtime_raw = runtime_value.as_str().ok_or_else(|| {
                Box::new(HarnessError(format!(
                    "operation {operation_id} probe field runtime_status must be a string"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
            Some(RuntimeOperationStatus::parse(
                operation_id,
                "runtime_status",
                runtime_raw,
            )?)
        } else if let Some(status_value) = value.get("status") {
            let status_raw = status_value.as_str().ok_or_else(|| {
                Box::new(HarnessError(format!(
                    "operation {operation_id} probe field status must be a string"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })?;
            match status_raw {
                "received" | "snapshot_ready" | "generating" | "generated" | "committing"
                | "committed" | "delivered" | "conflict" | "failed" | "interrupted" => Some(
                    RuntimeOperationStatus::parse(operation_id, "status", status_raw)?,
                ),
                "unknown" | "retry_at" | "retry_pending" | "pending" | "sent"
                | "already_applied" | "applied" => None,
                _ => Some(RuntimeOperationStatus::parse(
                    operation_id,
                    "status",
                    status_raw,
                )?),
            }
        } else {
            None
        };
        Ok(Self {
            value,
            runtime_status,
            commit_state,
        })
    }

    fn require_runtime_status(&self, operation_id: &str) -> E2eResult<RuntimeOperationStatus> {
        self.runtime_status.ok_or_else(|| {
            Box::new(HarnessError(format!(
                "operation {operation_id} probe does not expose a typed runtime_status"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })
    }

    fn require_delivery_status(&self, operation_id: &str) -> E2eResult<DeliveryEffectStatus> {
        let (field, raw_value) = if let Some(value) = self.value.get("delivery_status") {
            ("delivery_status", value)
        } else if let Some(value) = self
            .value
            .get("delivery")
            .and_then(|delivery| delivery.get("status"))
        {
            ("delivery.status", value)
        } else {
            return Err(Box::new(HarnessError(format!(
                "operation {operation_id} probe omitted required delivery_status/delivery.status"
            ))));
        };
        let raw = raw_value.as_str().ok_or_else(|| {
            Box::new(HarnessError(format!(
                "operation {operation_id} probe field {field} must be a string"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        DeliveryEffectStatus::parse(operation_id, field, raw)
    }
}

impl OperationExpectation {
    fn matches(self, operation_id: &str, probe: &OperationProbe<'_>) -> E2eResult<bool> {
        let runtime_status = match self {
            Self::RetryPending => None,
            _ => Some(probe.require_runtime_status(operation_id)?),
        };
        match self {
            Self::CommittedAndDelivered => {
                if !matches!(
                    runtime_status,
                    Some(RuntimeOperationStatus::Committed | RuntimeOperationStatus::Delivered)
                ) || probe.commit_state != CommitState::Applied
                {
                    return Ok(false);
                }
                Ok(probe.require_delivery_status(operation_id)? == DeliveryEffectStatus::Delivered)
            }
            Self::ReconciledAndDelivered => {
                if runtime_status != Some(RuntimeOperationStatus::Delivered)
                    || probe.commit_state != CommitState::Applied
                {
                    return Ok(false);
                }
                Ok(probe.require_delivery_status(operation_id)? == DeliveryEffectStatus::Delivered)
            }
            Self::CommittingUnknown => Ok(runtime_status
                == Some(RuntimeOperationStatus::Committing)
                && probe.commit_state == CommitState::Unknown),
            Self::RejectedNotApplied => Ok(matches!(
                runtime_status,
                Some(RuntimeOperationStatus::Conflict | RuntimeOperationStatus::Failed)
            ) && probe.commit_state == CommitState::NotApplied),
            Self::RetryPending => Ok(matches!(
                probe.require_delivery_status(operation_id)?,
                DeliveryEffectStatus::Pending | DeliveryEffectStatus::RetryAt,
            )),
        }
    }
}

fn required_probe_string<'a>(
    value: &'a Value,
    operation_id: &str,
    field: &str,
) -> E2eResult<&'a str> {
    let raw_value = value.get(field).ok_or_else(|| {
        Box::new(HarnessError(format!(
            "operation {operation_id} probe omitted required field {field}"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    raw_value.as_str().ok_or_else(|| {
        Box::new(HarnessError(format!(
            "operation {operation_id} probe field {field} must be a string"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })
}

#[derive(Clone)]
struct HarnessConfig {
    run_id: String,
    scenario_id: String,
    control_token: String,
    runtime_url: String,
    st_url: String,
    connector_url: String,
    fault_proxy_url: String,
    provider_url: String,
    telegram_url: String,
}

impl HarnessConfig {
    fn from_env(scenario_id: &str) -> E2eResult<Self> {
        let run_id = required_env("IMBRIDGE_E2E_RUN_ID")?;
        let scenario_id = required_scenario_id(scenario_id)?;
        if run_id.contains('/') || run_id.contains(' ') {
            return Err(Box::new(HarnessError(
                "IMBRIDGE_E2E_RUN_ID must not contain spaces or '/'".into(),
            )));
        }
        Ok(Self {
            run_id,
            scenario_id,
            control_token: required_control_token("IMBRIDGE_E2E_CONTROL_TOKEN")?,
            runtime_url: approved_endpoint("IMBRIDGE_E2E_RUNTIME_URL")?,
            st_url: approved_endpoint("IMBRIDGE_E2E_ST_URL")?,
            connector_url: approved_endpoint("IMBRIDGE_E2E_CONNECTOR_URL")?,
            fault_proxy_url: approved_endpoint("IMBRIDGE_E2E_FAULT_PROXY_URL")?,
            provider_url: approved_endpoint("IMBRIDGE_E2E_PROVIDER_URL")?,
            telegram_url: approved_endpoint("IMBRIDGE_E2E_TELEGRAM_URL")?,
        })
    }
}

#[derive(Clone)]
struct Harness {
    config: HarnessConfig,
    client: Client,
}

impl Harness {
    fn connect(scenario_id: &str) -> E2eResult<Self> {
        let config = HarnessConfig::from_env(scenario_id)?;
        let client = Client::builder().no_proxy().build()?;
        Ok(Self { config, client })
    }

    async fn require_ready(&self) -> E2eResult<()> {
        for (name, base) in [
            ("runtime", &self.config.runtime_url),
            ("ST", &self.config.st_url),
            ("Connector", &self.config.connector_url),
            ("fault-proxy", &self.config.fault_proxy_url),
            ("fake Provider", &self.config.provider_url),
            ("fake Telegram", &self.config.telegram_url),
        ] {
            let response = self.client.get(format!("{base}/health")).send().await?;
            if !response.status().is_success() {
                return Err(Box::new(HarnessError(format!(
                    "{name} health check failed with {}",
                    response.status()
                ))));
            }
        }
        let sentinel: Value = self
            .get(&self.config.st_url, "/_e2e/fixture")
            .await
            .map_err(|error| HarnessError(format!("ST fixture sentinel unavailable: {error}")))?;
        if sentinel["sentinel"] != SENTINEL {
            return Err(Box::new(HarnessError(
                "ST fixture sentinel does not identify the approved sidecar volume".into(),
            )));
        }
        Ok(())
    }

    fn get_request(&self, base: &str, path: &str) -> E2eResult<reqwest::RequestBuilder> {
        let control = is_control_path(path);
        let url = if control {
            scoped_get_url(base, path, &self.config.run_id, &self.config.scenario_id)?
        } else {
            reqwest::Url::parse(&format!("{base}{path}"))?
        };
        let request = self.client.get(url);
        Ok(if control {
            request.header(CONTROL_TOKEN_HEADER, &self.config.control_token)
        } else {
            request
        })
    }

    fn post_request(
        &self,
        base: &str,
        path: &str,
        body: Value,
    ) -> E2eResult<reqwest::RequestBuilder> {
        let control = is_control_path(path);
        let body = scoped_post_body(path, body, &self.config.run_id, &self.config.scenario_id)?;
        let request = self
            .client
            .post(reqwest::Url::parse(&format!("{base}{path}"))?)
            .json(&body);
        Ok(if control {
            request.header(CONTROL_TOKEN_HEADER, &self.config.control_token)
        } else {
            request
        })
    }

    async fn get(&self, base: &str, path: &str) -> E2eResult<Value> {
        let response = self.get_request(base, path)?.send().await?;
        decode_json(response, path).await
    }

    async fn post(&self, base: &str, path: &str, body: Value) -> E2eResult<Value> {
        let response = self.post_request(base, path, body)?.send().await?;
        decode_json(response, path).await
    }

    async fn enqueue_update(&self, external_operation_id: &str, update: Value) -> E2eResult<Value> {
        let identity = OperationIdentity::for_update(external_operation_id, &update)?;
        self.post(
            &self.config.telegram_url,
            "/_e2e/updates",
            json!({
                "run_id": self.config.run_id,
                "operation_id": &identity.external_id,
                "external_id": &identity.external_id,
                "canonical_id": &identity.canonical_id,
                "external_operation_id": &identity.external_id,
                "canonical_operation_id": &identity.canonical_id,
                "update": update,
            }),
        )
        .await
    }

    async fn enqueue_callback(
        &self,
        external_operation_id: &str,
        callback: Value,
    ) -> E2eResult<Value> {
        let identity = OperationIdentity::for_update(external_operation_id, &callback)?;
        self.post(
            &self.config.telegram_url,
            "/_e2e/callbacks",
            json!({
                "run_id": self.config.run_id,
                "operation_id": &identity.external_id,
                "external_id": &identity.external_id,
                "canonical_id": &identity.canonical_id,
                "external_operation_id": &identity.external_id,
                "canonical_operation_id": &identity.canonical_id,
                "update": callback,
            }),
        )
        .await
    }

    async fn operation(&self, external_operation_id: &str) -> E2eResult<Value> {
        self.get(
            &self.config.connector_url,
            &format!("/_e2e/operations/{external_operation_id}"),
        )
        .await
    }

    async fn wait_for_operation(
        &self,
        external_operation_id: &str,
        expected: OperationExpectation,
    ) -> E2eResult<Value> {
        let deadline = Instant::now() + MAX_WAIT;
        loop {
            let state = self.operation(external_operation_id).await?;
            let probe = OperationProbe::parse(external_operation_id, &state)?;
            if expected.matches(external_operation_id, &probe)? {
                return Ok(state);
            }
            if Instant::now() >= deadline {
                return Err(Box::new(HarnessError(format!(
                    "operation {external_operation_id} did not reach {expected:?}; last state: {state}"
                ))));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn telegram_trace(&self) -> E2eResult<Value> {
        self.get(&self.config.telegram_url, "/_e2e/trace").await
    }

    async fn connector_journal(&self) -> E2eResult<Value> {
        self.get(&self.config.connector_url, "/_e2e/journal").await
    }

    async fn provider_trace(&self) -> E2eResult<Value> {
        self.get(&self.config.provider_url, "/_e2e/trace").await
    }

    async fn st_snapshot(&self) -> E2eResult<Value> {
        self.get(&self.config.st_url, "/_e2e/snapshot").await
    }

    async fn fault_trace(&self) -> E2eResult<Value> {
        self.get(&self.config.fault_proxy_url, "/_fault/trace")
            .await
    }

    async fn configure_fault(&self, rules: Value) -> E2eResult<Value> {
        self.post(
            &self.config.fault_proxy_url,
            "/_fault/configure",
            json!({"reset": true, "rules": rules}),
        )
        .await
    }

    async fn configure_telegram(&self, config: Value) -> E2eResult<Value> {
        self.post(&self.config.telegram_url, "/_e2e/configure", config)
            .await
    }

    async fn configure_provider(&self, config: Value) -> E2eResult<Value> {
        self.post(&self.config.provider_url, "/_e2e/configure", config)
            .await
    }

    async fn install_barrier(
        &self,
        barrier: &str,
        operation_ids: &[&str],
        expected_participants: &[&str],
    ) -> E2eResult<Value> {
        self.post(
            &self.config.runtime_url,
            "/_e2e/barriers/install",
            json!({
                "barrier": barrier,
                "operation_ids": operation_ids,
                "expected_participants": expected_participants,
            }),
        )
        .await
    }

    async fn wait_for_barrier_reached(
        &self,
        barrier: &str,
        operation_ids: &[&str],
        expected_participants: &[&str],
    ) -> E2eResult<Value> {
        let path = format!("/_e2e/barriers/status?barrier={barrier}");
        let deadline = Instant::now() + MAX_WAIT;
        loop {
            let state = self.get(&self.config.runtime_url, &path).await?;
            if barrier_is_reached(&state, barrier, operation_ids, expected_participants)? {
                return Ok(state);
            }
            if Instant::now() >= deadline {
                return Err(Box::new(HarnessError(format!(
                    "barrier {barrier} did not reach the expected participants; last state: {state}"
                ))));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn release_barrier(
        &self,
        barrier: &str,
        operation_ids: &[&str],
        expected_participants: &[&str],
    ) -> E2eResult<Value> {
        self.post(
            &self.config.runtime_url,
            "/_e2e/barriers/release",
            json!({
                "barrier": barrier,
                "operation_ids": operation_ids,
                "expected_participants": expected_participants,
            }),
        )
        .await
    }

    async fn install_st_barrier(
        &self,
        barrier: &str,
        operation_ids: &[&str],
        expected_participants: &[&str],
    ) -> E2eResult<Value> {
        self.post(
            &self.config.st_url,
            "/_e2e/barriers/install",
            json!({
                "barrier": barrier,
                "operation_ids": operation_ids,
                "expected_participants": expected_participants,
            }),
        )
        .await
    }

    async fn wait_for_st_barrier_reached(
        &self,
        barrier: &str,
        operation_ids: &[&str],
        expected_participants: &[&str],
        required_reached: &[&str],
    ) -> E2eResult<Value> {
        let path = format!("/_e2e/barriers/status?barrier={barrier}");
        let deadline = Instant::now() + MAX_WAIT;
        loop {
            let state = self.get(&self.config.st_url, &path).await?;
            if barrier_has_reached(
                &state,
                barrier,
                operation_ids,
                expected_participants,
                required_reached,
            )? {
                return Ok(state);
            }
            if Instant::now() >= deadline {
                return Err(Box::new(HarnessError(format!(
                    "ST barrier {barrier} did not reach {required_reached:?}; last state: {state}"
                ))));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn release_st_barrier(
        &self,
        barrier: &str,
        operation_ids: &[&str],
        expected_participants: &[&str],
    ) -> E2eResult<Value> {
        self.post(
            &self.config.st_url,
            "/_e2e/barriers/release",
            json!({
                "barrier": barrier,
                "operation_ids": operation_ids,
                "expected_participants": expected_participants,
            }),
        )
        .await
    }

    async fn seed_turn(&self, seed_id: &str, chat_id: i64) -> E2eResult<Value> {
        self.post(
            &self.config.runtime_url,
            "/_e2e/seed-turn",
            json!({
                "run_id": self.config.run_id,
                "seed_id": seed_id,
                "chat_id": chat_id,
                "fixture_sentinel": SENTINEL,
            }),
        )
        .await
    }

    async fn restart_runtime(&self) -> E2eResult<Value> {
        self.post(
            &self.config.runtime_url,
            "/_e2e/restart",
            json!({"run_id": self.config.run_id, "preserve_volumes": true}),
        )
        .await
    }
}

fn barrier_is_reached(
    state: &Value,
    barrier: &str,
    operation_ids: &[&str],
    expected_participants: &[&str],
) -> E2eResult<bool> {
    barrier_has_reached(
        state,
        barrier,
        operation_ids,
        expected_participants,
        expected_participants,
    )
}

fn barrier_has_reached(
    state: &Value,
    barrier: &str,
    operation_ids: &[&str],
    expected_participants: &[&str],
    required_reached: &[&str],
) -> E2eResult<bool> {
    let ok = state.get("ok").and_then(Value::as_bool).ok_or_else(|| {
        Box::new(HarnessError(format!(
            "barrier {barrier} status omitted boolean ok"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    if !ok {
        return Err(Box::new(HarnessError(format!(
            "barrier {barrier} status reported ok=false"
        ))));
    }
    let wired = state.get("wired").and_then(Value::as_bool).ok_or_else(|| {
        Box::new(HarnessError(format!(
            "barrier {barrier} status omitted boolean wired"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    let status = state.get("status").and_then(Value::as_str).ok_or_else(|| {
        Box::new(HarnessError(format!(
            "barrier {barrier} status omitted string status"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    if !wired || status == "blocked" {
        return Err(Box::new(HarnessError(format!(
            "barrier {barrier} is not wired (status={status:?}, wired={wired})"
        ))));
    }

    let expected_operation_ids = barrier_expected_ids(operation_ids, barrier, "operation_ids")?;
    let expected_participants =
        barrier_expected_ids(expected_participants, barrier, "expected_participants")?;
    let required_reached = barrier_expected_ids(required_reached, barrier, "required_reached")?;
    if required_reached.iter().any(|participant| {
        !expected_participants
            .iter()
            .any(|expected| expected == participant)
    }) {
        return Err(Box::new(HarnessError(format!(
            "barrier {barrier} required reached participants are outside the installed expected set"
        ))));
    }
    let declared_operation_ids =
        barrier_string_list(state.get("operation_ids"), barrier, "operation_ids")?;
    if declared_operation_ids != expected_operation_ids {
        return Err(Box::new(HarnessError(format!(
            "barrier {barrier} returned operation_ids outside the installed scope"
        ))));
    }
    let declared_participants = barrier_string_list(
        state.get("expected_participants"),
        barrier,
        "expected_participants",
    )?;
    if declared_participants != expected_participants {
        return Err(Box::new(HarnessError(format!(
            "barrier {barrier} returned expected_participants outside the installed scope"
        ))));
    }
    let reached = state
        .get("reached_participants")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            Box::new(HarnessError(format!(
                "barrier {barrier} status omitted object reached_participants"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
    if reached.keys().any(|operation_id| {
        !expected_operation_ids
            .iter()
            .any(|expected| expected == operation_id)
    }) {
        return Err(Box::new(HarnessError(format!(
            "barrier {barrier} returned an unexpected reached operation"
        ))));
    }
    for operation_id in &expected_operation_ids {
        let Some(participants) = reached.get(operation_id) else {
            return Ok(false);
        };
        let reached_participants =
            barrier_string_list(Some(participants), barrier, "reached_participants")?;
        if reached_participants.iter().any(|participant| {
            !expected_participants
                .iter()
                .any(|expected| expected == participant)
        }) {
            return Err(Box::new(HarnessError(format!(
                "barrier {barrier} returned an unexpected reached participant"
            ))));
        }
        if required_reached.iter().any(|participant| {
            !reached_participants
                .iter()
                .any(|reached| reached == participant)
        }) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn barrier_expected_ids(values: &[&str], barrier: &str, field: &str) -> E2eResult<Vec<String>> {
    if values.is_empty() || values.iter().any(|value| value.is_empty()) {
        return Err(Box::new(HarnessError(format!(
            "barrier {barrier} has empty {field}"
        ))));
    }
    let mut values = values
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    values.sort_unstable();
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(Box::new(HarnessError(format!(
            "barrier {barrier} has duplicate {field}"
        ))));
    }
    Ok(values)
}

fn barrier_string_list(
    value: Option<&Value>,
    barrier: &str,
    field: &str,
) -> E2eResult<Vec<String>> {
    let values = value.and_then(Value::as_array).ok_or_else(|| {
        Box::new(HarnessError(format!(
            "barrier {barrier} field {field} must be an array"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    let mut values = values
        .iter()
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                Box::new(HarnessError(format!(
                    "barrier {barrier} field {field} must contain only strings"
                ))) as Box<dyn std::error::Error + Send + Sync>
            })
        })
        .collect::<E2eResult<Vec<_>>>()?;
    values.sort_unstable();
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(Box::new(HarnessError(format!(
            "barrier {barrier} field {field} contains duplicate participants"
        ))));
    }
    Ok(values)
}

fn required_env(name: &str) -> E2eResult<String> {
    let value = std::env::var(name).map_err(|_| {
        HarnessError(format!(
            "{name} is required; Gate C is blocked instead of falling back to a local mock"
        ))
    })?;
    let value = value.trim().trim_end_matches('/').to_string();
    if value.is_empty() {
        return Err(Box::new(HarnessError(format!(
            "{name} is empty; Gate C is blocked"
        ))));
    }
    Ok(value)
}

fn required_scenario_id(scenario_id: &str) -> E2eResult<String> {
    let value = scenario_id.trim();
    if value.is_empty()
        || value.len() > MAX_SCENARIO_ID_LENGTH
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(Box::new(HarnessError(
            "scenario_id is empty or outside the bounded ID format".into(),
        )));
    }
    Ok(value.to_owned())
}

fn required_control_token(name: &str) -> E2eResult<String> {
    let value = std::env::var(name).map_err(|_| {
        HarnessError(format!(
            "{name} is required; E2E control requests are blocked"
        ))
    })?;
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(Box::new(HarnessError(format!(
            "{name} is empty; E2E control requests are blocked"
        ))));
    }
    if value.len() > MAX_CONTROL_TOKEN_LENGTH {
        return Err(Box::new(HarnessError(format!(
            "{name} exceeds the {}-byte maximum",
            MAX_CONTROL_TOKEN_LENGTH
        ))));
    }
    Ok(value)
}

fn is_control_path(path: &str) -> bool {
    path.starts_with("/_e2e/") || path.starts_with("/_fault/")
}

fn scoped_get_url(
    base: &str,
    path: &str,
    run_id: &str,
    scenario_id: &str,
) -> E2eResult<reqwest::Url> {
    let mut url = reqwest::Url::parse(&format!("{base}{path}"))?;
    let mut query_run_id = None;
    let mut query_scenario_id = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "run_id" => {
                if query_run_id.is_some() {
                    return Err(Box::new(HarnessError(format!(
                        "control GET {path} contains duplicate run_id query parameters"
                    ))));
                }
                query_run_id = Some(value.into_owned());
            }
            "scenario_id" => {
                if query_scenario_id.is_some() {
                    return Err(Box::new(HarnessError(format!(
                        "control GET {path} contains duplicate scenario_id query parameters"
                    ))));
                }
                query_scenario_id = Some(value.into_owned());
            }
            _ => {}
        }
    }
    if let Some(query_run_id) = query_run_id {
        if query_run_id != run_id {
            return Err(Box::new(HarnessError(format!(
                "control GET {path} run_id does not match the active run scope"
            ))));
        }
    } else {
        url.query_pairs_mut().append_pair("run_id", run_id);
    }
    if let Some(query_scenario_id) = query_scenario_id {
        if query_scenario_id != scenario_id {
            return Err(Box::new(HarnessError(format!(
                "control GET {path} scenario_id does not match the active scenario scope"
            ))));
        }
    } else {
        url.query_pairs_mut()
            .append_pair("scenario_id", scenario_id);
    }
    Ok(url)
}

fn scoped_post_body(path: &str, body: Value, run_id: &str, scenario_id: &str) -> E2eResult<Value> {
    if !is_control_path(path) {
        return Ok(body);
    }
    let mut object = body.as_object().cloned().ok_or_else(|| {
        Box::new(HarnessError(format!(
            "control POST {path} requires a JSON object body"
        ))) as Box<dyn std::error::Error + Send + Sync>
    })?;
    ensure_scope_field(&mut object, path, "run_id", run_id)?;
    ensure_scope_field(&mut object, path, "scenario_id", scenario_id)?;
    Ok(Value::Object(object))
}

fn ensure_scope_field(
    object: &mut serde_json::Map<String, Value>,
    path: &str,
    field: &str,
    expected: &str,
) -> E2eResult<()> {
    if let Some(existing) = object.get(field) {
        let existing = existing.as_str().ok_or_else(|| {
            Box::new(HarnessError(format!(
                "control POST {path} {field} must be a string"
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;
        if existing != expected {
            return Err(Box::new(HarnessError(format!(
                "control POST {path} {field} does not match the active scope"
            ))));
        }
    } else {
        object.insert(field.into(), Value::String(expected.to_owned()));
    }
    Ok(())
}

fn approved_endpoint(name: &str) -> E2eResult<String> {
    let value = required_env(name)?;
    let parsed = reqwest::Url::parse(&value)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| HarnessError(format!("{name} has no host")))?;
    let approved_host = matches!(
        host,
        "localhost"
            | "127.0.0.1"
            | "::1"
            | "rust-runner"
            | "runtime-control"
            | "sillytavern"
            | "connector"
            | "connector-control"
            | "fault-proxy"
            | "fake-provider"
            | "fake-telegram"
    );
    if parsed.scheme() != "http" || parsed.username() != "" || parsed.password().is_some() {
        return Err(Box::new(HarnessError(format!(
            "{name} must be an http endpoint without credentials"
        ))));
    }
    if !approved_host || parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(Box::new(HarnessError(format!(
            "{name} is outside the approved isolated sidecar endpoints"
        ))));
    }
    Ok(value)
}

async fn decode_json(response: reqwest::Response, path: &str) -> E2eResult<Value> {
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(Box::new(HarnessError(format!("{path} returned {status}"))));
    }
    Ok(serde_json::from_str(&body)?)
}

fn message_update(
    run_id: &str,
    scenario_id: &str,
    update_id: i64,
    chat_id: i64,
    text: &str,
) -> Value {
    json!({
        "update_id": update_id,
        "message": {
            "message_id": update_id + 1000,
            "from": {"id": 7001, "is_bot": false, "first_name": "E2E"},
            "chat": {"id": chat_id, "type": "private"},
            "date": 1_725_000_000,
            "text": text,
        },
        "imbridge": {
            "run_id": run_id,
            "scenario_id": scenario_id,
            "fixture_sentinel": SENTINEL,
        },
    })
}

fn callback_update(
    run_id: &str,
    scenario_id: &str,
    update_id: i64,
    callback_id: &str,
    chat_id: i64,
    data: &str,
) -> Value {
    json!({
        "update_id": update_id,
        "callback_query": {
            "id": callback_id,
            "from": {"id": 7001, "is_bot": false, "first_name": "E2E"},
            "message": {
                "message_id": update_id + 1000,
                "chat": {"id": chat_id, "type": "private"},
                "date": 1_725_000_000,
                "text": "sidecar panel",
            },
            "data": data,
        },
        "imbridge": {
            "run_id": run_id,
            "scenario_id": scenario_id,
            "fixture_sentinel": SENTINEL,
        },
    })
}

fn canonical_operation_id(update: &Value) -> E2eResult<String> {
    let update_id = update["update_id"]
        .as_i64()
        .ok_or_else(|| HarnessError("Telegram update omitted numeric update_id".into()))?;
    Ok(format!("tg:{E2E_INTERNAL_BOT_ID}:{update_id}"))
}

fn operation_records_for_identity<'a>(
    value: &'a Value,
    identity: &OperationIdentity,
) -> Vec<&'a Value> {
    operation_records_by_ids(
        value,
        &[
            identity.external_id.as_str(),
            identity.canonical_id.as_str(),
        ],
    )
}

fn operation_records_by_ids<'a>(value: &'a Value, operation_ids: &[&str]) -> Vec<&'a Value> {
    let mut records = Vec::new();
    collect_operation_records(value, operation_ids, &mut records);
    records
}

fn collect_operation_records<'a>(
    value: &'a Value,
    operation_ids: &[&str],
    records: &mut Vec<&'a Value>,
) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_operation_records(item, operation_ids, records);
            }
        }
        Value::Object(map) => {
            let matches_operation = [
                "operation_id",
                "operationId",
                "external_id",
                "canonical_id",
                "external_operation_id",
                "externalOperationId",
                "canonical_operation_id",
                "canonicalOperationId",
                "target_op",
                "targetOperationId",
            ]
            .iter()
            .filter_map(|key| map.get(*key).and_then(Value::as_str))
            .any(|candidate| operation_ids.contains(&candidate));
            let looks_like_record = map.contains_key("event")
                || map.contains_key("method")
                || map.contains_key("path")
                || map.contains_key("status");
            if matches_operation && looks_like_record {
                records.push(value);
            }
            for child in map.values() {
                collect_operation_records(child, operation_ids, records);
            }
        }
        _ => {}
    }
}

fn records_with_method_for_identity<'a>(
    value: &'a Value,
    method: &str,
    identity: &OperationIdentity,
) -> Vec<&'a Value> {
    operation_records_for_identity(value, identity)
        .into_iter()
        .filter(|record| record["method"].as_str() == Some(method))
        .collect()
}

fn successful_message_ids(value: &Value, identity: &OperationIdentity, method: &str) -> Vec<i64> {
    records_with_method_for_identity(value, method, identity)
        .into_iter()
        .filter(|record| record.get("ok").and_then(Value::as_bool) == Some(true))
        .filter_map(|record| record.get("message_id").and_then(Value::as_i64))
        .collect()
}

fn active_message_ids(value: &Value) -> E2eResult<Vec<i64>> {
    let messages = value
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            Box::new(HarnessError(
                "fake Telegram trace omitted raw messages".into(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;
    Ok(messages
        .iter()
        .filter(|message| message.get("status").and_then(Value::as_str) == Some("active"))
        .filter_map(|message| message.get("message_id").and_then(Value::as_i64))
        .collect())
}

fn max_provider_in_flight(value: &Value) -> E2eResult<i64> {
    let samples = value
        .get("concurrency_samples")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            Box::new(HarnessError(
                "fake Provider trace omitted raw concurrency samples".into(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;
    samples
        .iter()
        .filter_map(|sample| sample.get("in_flight").and_then(Value::as_i64))
        .max()
        .ok_or_else(|| {
            Box::new(HarnessError(
                "fake Provider trace contained no concurrency samples".into(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })
}

fn find_string_by_key<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    match value {
        Value::Array(items) => items.iter().find_map(|item| find_string_by_key(item, key)),
        Value::Object(map) => map
            .get(key)
            .and_then(Value::as_str)
            .or_else(|| map.values().find_map(|item| find_string_by_key(item, key))),
        _ => None,
    }
}

async fn wait_for_telegram_method(
    harness: &Harness,
    identity: &OperationIdentity,
    method: &str,
) -> E2eResult<Value> {
    let deadline = Instant::now() + MAX_WAIT;
    loop {
        let trace = harness.telegram_trace().await?;
        if !records_with_method_for_identity(&trace, method, identity).is_empty() {
            return Ok(trace);
        }
        if Instant::now() >= deadline {
            return Err(Box::new(HarnessError(format!(
                "Telegram did not record {method} for {}",
                identity.external_id
            ))));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn field_string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn assert_event_order_for_identity(
    value: &Value,
    identity: &OperationIdentity,
    expected: &[&str],
) -> E2eResult<()> {
    let records = operation_records_for_identity(value, identity);
    let mut cursor = 0usize;
    for event in expected {
        let Some(offset) = records[cursor..]
            .iter()
            .position(|record| field_string(record, "event") == Some(event))
        else {
            return Err(Box::new(HarnessError(format!(
                "operation {} is missing ordered event {event:?}",
                identity.external_id
            ))));
        };
        cursor += offset + 1;
    }
    Ok(())
}

fn assert_fixture_contract() -> E2eResult<()> {
    let character: Value = serde_json::from_str(SIDECAR_CHARACTER)?;
    let settings: Value = serde_json::from_str(SIDECAR_SETTINGS)?;
    let chat_lines: Vec<Value> = SIDECAR_CHAT
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    if character["name"] != "TestCharacter"
        || settings["oai_settings"]["custom_url"] != "http://fake-provider:8080/v1"
        || settings["imbridge_test_profile"]["sentinel"] != SENTINEL
        || chat_lines.len() != 3
    {
        return Err(Box::new(HarnessError(
            "sidecar fixture contract does not match the approved synthetic profile".into(),
        )));
    }
    Ok(())
}

async fn send_message_and_wait(
    harness: &Harness,
    operation_id: &str,
    update_id: i64,
    chat_id: i64,
    text: &str,
) -> E2eResult<Value> {
    harness
        .enqueue_update(
            operation_id,
            message_update(
                &harness.config.run_id,
                &harness.config.scenario_id,
                update_id,
                chat_id,
                text,
            ),
        )
        .await?;
    harness
        .wait_for_operation(operation_id, OperationExpectation::CommittedAndDelivered)
        .await
}

#[tokio::test]
async fn e2e_send_message_full_lifecycle() -> E2eResult<()> {
    assert_fixture_contract()?;
    let harness = Harness::connect("send-message-full-lifecycle")?;
    harness.require_ready().await?;
    let operation_id = "gatec-send-001";
    let send_identity = OperationIdentity::for_update(
        operation_id,
        &message_update(
            &harness.config.run_id,
            &harness.config.scenario_id,
            10_001,
            70001,
            "Hello test",
        ),
    )?;
    let before = harness.st_snapshot().await?;
    let provider_before = harness.provider_trace().await?;

    send_message_and_wait(&harness, operation_id, 10_001, 70001, "Hello test").await?;

    let telegram_before_callback = harness.telegram_trace().await?;
    let callback_data =
        find_string_by_key(&telegram_before_callback, "callback_data").ok_or_else(|| {
            HarnessError("real Telegram send trace did not expose a callback token".into())
        })?;
    let callback_operation_id = "gatec-send-callback-001";
    let callback_update = callback_update(
        &harness.config.run_id,
        &harness.config.scenario_id,
        10_101,
        "gatec-callback-001",
        70001,
        callback_data,
    );
    let callback_identity = OperationIdentity::for_update(callback_operation_id, &callback_update)?;
    harness
        .enqueue_callback(callback_operation_id, callback_update)
        .await?;
    let _telegram_after_callback =
        wait_for_telegram_method(&harness, &callback_identity, "answerCallbackQuery").await?;

    let journal = harness.connector_journal().await?;
    let telegram = harness.telegram_trace().await?;
    let provider = harness.provider_trace().await?;
    let after = harness.st_snapshot().await?;
    let journal_records = operation_records_for_identity(&journal, &send_identity);
    if !journal_records.iter().any(|record| {
        matches!(
            field_string(record, "status"),
            Some("committed" | "applied" | "delivered")
        )
    }) {
        return Err(boxed_harness_error(HarnessError(
            "send operation has no durable committed record".into(),
        )));
    }
    if records_with_method_for_identity(&telegram, "sendMessage", &send_identity).is_empty()
        || records_with_method_for_identity(&telegram, "editMessageText", &send_identity).is_empty()
    {
        return Err(boxed_harness_error(HarnessError(
            "send lifecycle did not produce real Telegram send and final edit traces".into(),
        )));
    }
    assert_event_order_for_identity(&telegram, &send_identity, &["placeholder", "delta", "done"])?;
    if operation_records_for_identity(&provider, &send_identity)
        .len()
        .saturating_sub(operation_records_for_identity(&provider_before, &send_identity).len())
        != 1
    {
        return Err(boxed_harness_error(HarnessError(
            "Provider trace does not show exactly one generation for the operation".into(),
        )));
    }
    let before_sha = before["source_sha256"]
        .as_str()
        .ok_or_else(|| HarnessError("ST before snapshot omitted source_sha256".into()))?;
    let after_sha = after["source_sha256"]
        .as_str()
        .ok_or_else(|| HarnessError("ST after snapshot omitted source_sha256".into()))?;
    if before_sha == after_sha || after["source_integrity"] == before["source_integrity"] {
        return Err(boxed_harness_error(HarnessError(
            "real ST snapshot did not prove SHA/integrity rotation".into(),
        )));
    }
    if !journal_records.iter().any(|record| {
        record["before_sha256"].as_str() == Some(before_sha)
            && record["after_sha256"].as_str() == Some(after_sha)
            && record["backup_preimage_sha256"].as_str() == Some(before_sha)
    }) {
        return Err(boxed_harness_error(HarnessError(
            "durable commit journal omitted the before-image backup evidence".into(),
        )));
    }
    if before["non_target_sha256"] != after["non_target_sha256"] {
        return Err(boxed_harness_error(HarnessError(
            "send changed a non-target chat/file hash".into(),
        )));
    }
    Ok(())
}

#[tokio::test]
async fn e2e_new_chat_full_lifecycle() -> E2eResult<()> {
    let harness = Harness::connect("new-chat-full-lifecycle")?;
    harness.require_ready().await?;
    let operation_id = "gatec-new-001";
    let update = message_update(
        &harness.config.run_id,
        &harness.config.scenario_id,
        10_002,
        70002,
        "/new",
    );
    let identity = OperationIdentity::for_update(operation_id, &update)?;
    harness.enqueue_update(operation_id, update.clone()).await?;
    let state = harness
        .wait_for_operation(operation_id, OperationExpectation::CommittedAndDelivered)
        .await?;
    let journal = harness.connector_journal().await?;
    let records = operation_records_for_identity(&journal, &identity);
    if !records.iter().any(|record| {
        field_string(record, "operation_kind") == Some("create")
            && field_string(record, "status")
                .is_some_and(|status| matches!(status, "committed" | "applied" | "delivered"))
    }) {
        return Err(boxed_harness_error(HarnessError(
            "new-chat operation lacks an applied create journal record".into(),
        )));
    }
    let locator = state["locator"]
        .as_object()
        .ok_or_else(|| HarnessError("new-chat result omitted the selected real locator".into()))?;
    let chat_file = locator["chat_file"].as_str().unwrap_or_default();
    if !chat_file.starts_with("IMBridge-Test-") {
        return Err(boxed_harness_error(HarnessError(
            "new-chat selected locator does not carry the approved test marker".into(),
        )));
    }
    harness.enqueue_update(operation_id, update).await?;
    let journal_after_duplicate = harness.connector_journal().await?;
    if operation_records_for_identity(&journal_after_duplicate, &identity).len() != records.len() {
        return Err(boxed_harness_error(HarnessError(
            "duplicate /new update created another journal record".into(),
        )));
    }
    Ok(())
}

#[tokio::test]
async fn e2e_undo_and_revoke_lifecycle() -> E2eResult<()> {
    let harness = Harness::connect("undo-and-revoke-lifecycle")?;
    harness.require_ready().await?;
    let turn_operation_id = "gatec-turn-001";
    let turn_update = message_update(
        &harness.config.run_id,
        &harness.config.scenario_id,
        10_003,
        70003,
        "build a turn",
    );
    let turn_identity = OperationIdentity::for_update(turn_operation_id, &turn_update)?;
    send_message_and_wait(&harness, turn_operation_id, 10_003, 70003, "build a turn").await?;
    let before_undo = harness.telegram_trace().await?;

    let undo_operation_id = "gatec-undo-001";
    let undo_update = message_update(
        &harness.config.run_id,
        &harness.config.scenario_id,
        10_004,
        70003,
        "/undo",
    );
    let undo_identity = OperationIdentity::for_update(undo_operation_id, &undo_update)?;
    harness
        .enqueue_update(undo_operation_id, undo_update)
        .await?;
    harness
        .wait_for_operation(
            undo_operation_id,
            OperationExpectation::CommittedAndDelivered,
        )
        .await?;
    let after_undo = harness.telegram_trace().await?;
    if records_with_method_for_identity(&after_undo, "sendMessage", &turn_identity).len()
        != records_with_method_for_identity(&before_undo, "sendMessage", &turn_identity).len()
        || !records_with_method_for_identity(&after_undo, "deleteMessage", &undo_identity)
            .is_empty()
        || !records_with_method_for_identity(&after_undo, "editMessageText", &undo_identity)
            .is_empty()
    {
        return Err(boxed_harness_error(HarnessError(
            "undo performed Telegram cleanup instead of preserving the old turn".into(),
        )));
    }

    harness.seed_turn("revoke-fixture-001", 70003).await?;
    let revoke_operation_id = "gatec-revoke-001";
    let revoke_update = message_update(
        &harness.config.run_id,
        &harness.config.scenario_id,
        10_005,
        70003,
        "/revoke",
    );
    let revoke_identity = OperationIdentity::for_update(revoke_operation_id, &revoke_update)?;
    harness
        .enqueue_update(revoke_operation_id, revoke_update)
        .await?;
    harness
        .wait_for_operation(
            revoke_operation_id,
            OperationExpectation::CommittedAndDelivered,
        )
        .await?;
    let telegram = harness.telegram_trace().await?;
    let journal = harness.connector_journal().await?;
    if records_with_method_for_identity(&telegram, "editMessageText", &revoke_identity).len() != 1
        || records_with_method_for_identity(&telegram, "deleteMessage", &revoke_identity).len() != 2
        || operation_records_for_identity(&journal, &revoke_identity)
            .iter()
            .all(|record| field_string(record, "operation_kind") != Some("revoke"))
    {
        return Err(boxed_harness_error(HarnessError(
            "revoke cleanup was not a distinct post-commit effect for the fixed turn".into(),
        )));
    }
    Ok(())
}

#[tokio::test]
async fn e2e_redo_in_place_and_cleanup() -> E2eResult<()> {
    let harness = Harness::connect("redo-in-place-cleanup")?;
    harness.require_ready().await?;
    harness
        .configure_provider(json!({
            "run_id": harness.config.run_id,
            "script": [1, 10, 2, 1],
            "chunk_size_utf16": 3200,
        }))
        .await?;
    send_message_and_wait(&harness, "gatec-redo-seed", 10_006, 70004, "seed redo").await?;
    for (index, operation_id) in ["gatec-redo-10", "gatec-redo-2", "gatec-redo-1"]
        .into_iter()
        .enumerate()
    {
        harness
            .enqueue_update(
                operation_id,
                message_update(
                    &harness.config.run_id,
                    &harness.config.scenario_id,
                    10_007 + index as i64,
                    70004,
                    "/redo",
                ),
            )
            .await?;
        harness
            .wait_for_operation(operation_id, OperationExpectation::CommittedAndDelivered)
            .await?;
    }
    let redo_identity = OperationIdentity::for_update(
        "gatec-redo-1",
        &message_update(
            &harness.config.run_id,
            &harness.config.scenario_id,
            10_009,
            70004,
            "/redo",
        ),
    )?;
    let telegram = harness.telegram_trace().await?;
    let send_ids = successful_message_ids(&telegram, &redo_identity, "sendMessage");
    let active_ids = active_message_ids(&telegram)?;
    let delete_ids = successful_message_ids(&telegram, &redo_identity, "deleteMessage");
    if send_ids.is_empty()
        || active_ids.first() != send_ids.first()
        || active_ids.len() != 1
        || delete_ids.windows(2).any(|pair| pair[0] <= pair[1])
    {
        return Err(boxed_harness_error(HarnessError(
            "redo did not reconcile raw Telegram sends, active messages, and reverse cleanup order"
                .into(),
        )));
    }
    Ok(())
}

#[tokio::test]
async fn e2e_compress_chat_lifecycle() -> E2eResult<()> {
    let harness = Harness::connect("compress-chat-lifecycle")?;
    harness.require_ready().await?;
    harness
        .configure_provider(json!({
            "run_id": harness.config.run_id,
            "script": ["compressed history"],
            "progress_persisted": false,
        }))
        .await?;
    let operation_id = "gatec-compress-001";
    let update = message_update(
        &harness.config.run_id,
        &harness.config.scenario_id,
        10_010,
        70005,
        "/compress",
    );
    let identity = OperationIdentity::for_update(operation_id, &update)?;
    harness.enqueue_update(operation_id, update).await?;
    harness
        .wait_for_operation(operation_id, OperationExpectation::CommittedAndDelivered)
        .await?;
    let journal = harness.connector_journal().await?;
    let provider = harness.provider_trace().await?;
    let journal_records = operation_records_for_identity(&journal, &identity);
    let provider_records = operation_records_for_identity(&provider, &identity);
    if journal_records
        .iter()
        .filter(|record| field_string(record, "event") == Some("commit"))
        .count()
        != 1
        || provider_records.iter().any(|record| {
            field_string(record, "event") == Some("progress") && record["persisted"] == true
        })
    {
        return Err(boxed_harness_error(HarnessError(
            "compress did not produce exactly one durable commit or persisted progress".into(),
        )));
    }
    Ok(())
}

#[tokio::test]
async fn e2e_duplicate_update_idempotency() -> E2eResult<()> {
    let harness = Harness::connect("duplicate-update-idempotency")?;
    harness.require_ready().await?;
    let operation_id = "gatec-duplicate-001";
    let update = message_update(
        &harness.config.run_id,
        &harness.config.scenario_id,
        10_011,
        70006,
        "duplicate guarded",
    );
    let identity = OperationIdentity::for_update(operation_id, &update)?;
    harness.enqueue_update(operation_id, update.clone()).await?;
    harness.enqueue_update(operation_id, update).await?;
    harness
        .wait_for_operation(operation_id, OperationExpectation::CommittedAndDelivered)
        .await?;
    let provider = harness.provider_trace().await?;
    let journal = harness.connector_journal().await?;
    let telegram = harness.telegram_trace().await?;
    if operation_records_for_identity(&provider, &identity)
        .iter()
        .filter(|record| field_string(record, "event") == Some("generation"))
        .count()
        != 1
        || operation_records_for_identity(&journal, &identity)
            .iter()
            .filter(|record| field_string(record, "event") == Some("mutation"))
            .count()
            != 1
        || records_with_method_for_identity(&telegram, "sendMessage", &identity).len() != 1
    {
        return Err(boxed_harness_error(HarnessError(
            "duplicate update was not short-circuited by the durable inbox".into(),
        )));
    }

    let conflicting = message_update(
        &harness.config.run_id,
        &harness.config.scenario_id,
        10_011,
        70006,
        "different digest",
    );
    let response = harness
        .enqueue_update("gatec-duplicate-conflict", conflicting)
        .await?;
    if response["status"] != "rejected" && response["error"] != "identity_reused" {
        return Err(boxed_harness_error(HarnessError(
            "same update_id with a different digest was not rejected".into(),
        )));
    }
    Ok(())
}

#[tokio::test]
async fn e2e_same_locator_concurrency() -> E2eResult<()> {
    let harness = Harness::connect("same-locator-concurrency")?;
    harness.require_ready().await?;
    // Keep both request builders alive through join, rather than borrowing a
    // temporary clone (T01). Both requests still enter the real runtime.
    let first_operation_id = "gatec-concurrency-1";
    let first_update = message_update(
        &harness.config.run_id,
        &harness.config.scenario_id,
        10_012,
        70007,
        "first concurrent",
    );
    let first_identity = OperationIdentity::for_update(first_operation_id, &first_update)?;
    let first_body = json!({
        "run_id": harness.config.run_id,
        "operation_id": &first_identity.external_id,
        "external_id": &first_identity.external_id,
        "canonical_id": &first_identity.canonical_id,
        "external_operation_id": &first_identity.external_id,
        "canonical_operation_id": &first_identity.canonical_id,
        "update": first_update,
    });
    let first_request =
        harness.post_request(&harness.config.telegram_url, "/_e2e/updates", first_body)?;
    let second_operation_id = "gatec-concurrency-2";
    let second_update = message_update(
        &harness.config.run_id,
        &harness.config.scenario_id,
        10_013,
        70008,
        "second concurrent",
    );
    let second_identity = OperationIdentity::for_update(second_operation_id, &second_update)?;
    let second_body = json!({
        "run_id": harness.config.run_id,
        "operation_id": &second_identity.external_id,
        "external_id": &second_identity.external_id,
        "canonical_id": &second_identity.canonical_id,
        "external_operation_id": &second_identity.external_id,
        "canonical_operation_id": &second_identity.canonical_id,
        "update": second_update,
    });
    let barrier_operation_ids = [
        first_identity.canonical_id.as_str(),
        second_identity.canonical_id.as_str(),
    ];
    let barrier_participants = [
        first_identity.canonical_id.as_str(),
        second_identity.canonical_id.as_str(),
    ];
    harness
        .install_barrier(
            "same-locator-commit",
            &barrier_operation_ids,
            &barrier_participants,
        )
        .await?;
    let second_request =
        harness.post_request(&harness.config.telegram_url, "/_e2e/updates", second_body)?;
    let first = tokio::spawn(async move { first_request.send().await });
    let second = tokio::spawn(async move { second_request.send().await });
    let (first, second) = tokio::join!(first, second);
    first??;
    second??;
    harness
        .wait_for_barrier_reached(
            "same-locator-commit",
            &barrier_operation_ids,
            &barrier_participants,
        )
        .await?;
    harness
        .release_barrier(
            "same-locator-commit",
            &barrier_operation_ids,
            &barrier_participants,
        )
        .await?;
    harness
        .wait_for_operation(
            "gatec-concurrency-1",
            OperationExpectation::CommittedAndDelivered,
        )
        .await?;
    harness
        .wait_for_operation(
            "gatec-concurrency-2",
            OperationExpectation::CommittedAndDelivered,
        )
        .await?;
    let journal = harness.connector_journal().await?;
    let provider = harness.provider_trace().await?;
    if max_provider_in_flight(&provider)? != 1
        || operation_records_for_identity(&journal, &first_identity).is_empty()
        || operation_records_for_identity(&journal, &second_identity).is_empty()
    {
        return Err(boxed_harness_error(HarnessError(
            "same-locator operations did not serialize according to raw Provider samples and Connector events".into(),
        )));
    }
    Ok(())
}

#[tokio::test]
async fn e2e_fault_commit_response_lost_recovery() -> E2eResult<()> {
    let harness = Harness::connect("fault-commit-response-lost")?;
    harness.require_ready().await?;
    let operation_id = "gatec-lost-response-001";
    let update = message_update(
        &harness.config.run_id,
        &harness.config.scenario_id,
        10_014,
        70008,
        "lost response",
    );
    let identity = OperationIdentity::for_update(operation_id, &update)?;
    harness
        .configure_fault(json!([{
            "rule_id": "drop-commit-response-once",
            "run_id": harness.config.run_id,
            "operation_id": &identity.canonical_id,
            "external_id": &identity.external_id,
            "canonical_id": &identity.canonical_id,
            "path": "/api/plugins/st-im-bridge/connector/v1/chats/commit",
            "method": "POST",
            "cut_point": "drop_commit_response",
            "remaining_hits": 1,
        }]))
        .await?;
    harness.enqueue_update(operation_id, update).await?;
    harness
        .wait_for_operation(operation_id, OperationExpectation::CommittingUnknown)
        .await?;
    harness.restart_runtime().await?;
    harness
        .wait_for_operation(operation_id, OperationExpectation::ReconciledAndDelivered)
        .await?;
    let fault_trace = harness.fault_trace().await?;
    let provider = harness.provider_trace().await?;
    let journal = harness.connector_journal().await?;
    if operation_records_for_identity(&fault_trace, &identity)
        .iter()
        .filter(|record| field_string(record, "event") == Some("cut"))
        .count()
        != 1
        || operation_records_for_identity(&provider, &identity)
            .iter()
            .filter(|record| field_string(record, "event") == Some("generation"))
            .count()
            != 1
        || operation_records_for_identity(&journal, &identity)
            .iter()
            .filter(|record| field_string(record, "event") == Some("mutation"))
            .count()
            != 1
        || journal["replay"]["outcome"] != "AlreadyApplied"
    {
        return Err(boxed_harness_error(HarnessError(
            "lost commit response did not reconcile the same durable operation ID".into(),
        )));
    }
    Ok(())
}

#[tokio::test]
async fn e2e_fault_st_web_stale_save_cas_conflict() -> E2eResult<()> {
    let harness = Harness::connect("fault-stale-save-cas")?;
    harness.require_ready().await?;
    let fixture = harness.get(&harness.config.st_url, "/_e2e/fixture").await?;
    let fixture_sha = fixture
        .get("source_sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| HarnessError("ST fixture omitted source_sha256".into()))?
        .to_owned();
    let seeded = harness.seed_turn("stale-save-runtime-001", 70009).await?;
    if seeded.get("ok").and_then(Value::as_bool) != Some(true)
        || seeded.get("seed_wired").and_then(Value::as_bool) != Some(true)
        || seeded.get("bot_id").and_then(Value::as_str) != Some("test-bot")
        || seeded.get("numeric_bot_id").and_then(Value::as_i64) != Some(900_000_001)
        || seeded.get("poller_status").and_then(Value::as_str) != Some("running")
        || seeded.get("channel_context_key").and_then(Value::as_str) != Some("test-bot:70009")
    {
        return Err(boxed_harness_error(HarnessError(format!(
            "seed-turn did not start the isolated poller: {seeded}"
        ))));
    }
    harness
        .configure_provider(json!({
            "run_id": harness.config.run_id,
            "operation_id": "tg:test-bot:10015",
            "script": ["connector race response"],
            "chunk_size_utf16": 3200,
        }))
        .await?;
    let operation_id = "gatec-stale-save-001";
    let stale_barrier_operation_ids = ["tg:test-bot:10015"];
    let stale_barrier_participants = ["connector", "native_reader"];
    let connector_only = ["connector"];
    harness
        .install_st_barrier(
            "connector-recheck-before-rename",
            &stale_barrier_operation_ids,
            &stale_barrier_participants,
        )
        .await?;
    harness
        .enqueue_update(
            operation_id,
            message_update(
                &harness.config.run_id,
                &harness.config.scenario_id,
                10_015,
                70009,
                "stale browser save",
            ),
        )
        .await?;
    harness
        .wait_for_st_barrier_reached(
            "connector-recheck-before-rename",
            &stale_barrier_operation_ids,
            &stale_barrier_participants,
            &connector_only,
        )
        .await?;
    let native_harness = harness.clone();
    let native_task = tokio::spawn(async move {
        native_harness
            .post(
                &native_harness.config.st_url,
                "/_e2e/native-save",
                json!({
                    "run_id": native_harness.config.run_id,
                    "operation_id": "native-web-save-001",
                    "connector_operation_id": "tg:test-bot:10015",
                    "barrier": "connector-recheck-before-rename",
                    "mutation": {"mes": "native web update"},
                }),
            )
            .await
    });
    let wait_full = harness
        .wait_for_st_barrier_reached(
            "connector-recheck-before-rename",
            &stale_barrier_operation_ids,
            &stale_barrier_participants,
            &stale_barrier_participants,
        )
        .await;
    if let Err(error) = wait_full {
        native_task.abort();
        let _ = native_task.await;
        let _ = harness
            .release_st_barrier(
                "connector-recheck-before-rename",
                &stale_barrier_operation_ids,
                &stale_barrier_participants,
            )
            .await;
        return Err(error);
    }
    let released = harness
        .release_st_barrier(
            "connector-recheck-before-rename",
            &stale_barrier_operation_ids,
            &stale_barrier_participants,
        )
        .await;
    if let Err(error) = released {
        native_task.abort();
        let _ = native_task.await;
        return Err(error);
    }
    let native_join = native_task
        .await
        .map_err(|error| HarnessError(format!("native-save task join failed: {error}")))?;
    let native_save_result = native_join?;
    let state = harness
        .wait_for_operation(operation_id, OperationExpectation::CommittedAndDelivered)
        .await?;
    let snapshot = harness.st_snapshot().await?;
    let native_outcome = native_save_result
        .get("outcome")
        .and_then(Value::as_str)
        .ok_or_else(|| HarnessError("native-save omitted outcome".into()))?;
    let native_stale_overwrite = native_save_result
        .get("stale_overwrite")
        .and_then(Value::as_bool);
    let native_serialization = native_save_result
        .get("serialization")
        .and_then(Value::as_str)
        .ok_or_else(|| HarnessError("native-save omitted serialization".into()))?;
    let native_read_sha = native_save_result
        .get("read_sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| HarnessError("native-save omitted read_sha256".into()))?;
    let native_final_sha = native_save_result
        .get("final_sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| HarnessError("native-save omitted final_sha256".into()))?;
    let snapshot_sha = snapshot
        .get("source_sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| HarnessError("snapshot omitted source_sha256".into()))?;
    let snapshot_native = snapshot
        .get("native_save")
        .cloned()
        .ok_or_else(|| HarnessError("snapshot omitted native_save evidence".into()))?;
    let connector_record = state
        .pointer("/sources/connector")
        .cloned()
        .ok_or_else(|| {
            HarnessError("operation observation omitted connector source record".into())
        })?;
    let after_sha = connector_record
        .get("afterSha256")
        .or_else(|| connector_record.get("after_sha256"))
        .and_then(Value::as_str)
        .ok_or_else(|| HarnessError("connector journal omitted afterSha256".into()))?;
    let before_sha = connector_record
        .get("beforeSha256")
        .or_else(|| connector_record.get("before_sha256"))
        .and_then(Value::as_str)
        .ok_or_else(|| HarnessError("connector journal omitted beforeSha256".into()))?;
    if state["commit_state"] != "applied"
        || state["connector_status"] != "applied"
        || native_outcome != "rejected_stale"
        || native_stale_overwrite != Some(false)
        || native_serialization != "shared"
        || native_read_sha != fixture_sha
        || native_final_sha == native_read_sha
        || snapshot_sha != native_final_sha
        || snapshot_native.get("outcome").and_then(Value::as_str) != Some("rejected_stale")
        || snapshot_native
            .get("stale_overwrite")
            .and_then(Value::as_bool)
            != Some(false)
        || snapshot_native.get("read_sha256").and_then(Value::as_str) != Some(native_read_sha)
        || snapshot_native.get("final_sha256").and_then(Value::as_str) != Some(native_final_sha)
        || after_sha != native_final_sha
        || before_sha != native_read_sha
    {
        return Err(boxed_harness_error(HarnessError(format!(
            "native ST save race did not produce the required shared-serialization CAS result: state={state}, native={native_save_result}, snapshot={snapshot}"
        ))));
    }
    Ok(())
}

#[tokio::test]
async fn e2e_fault_telegram_429_rate_limit_and_cooldown() -> E2eResult<()> {
    let harness = Harness::connect("fault-telegram-429-cooldown")?;
    harness.require_ready().await?;
    let operation_id = "gatec-telegram-429-001";
    let update = message_update(
        &harness.config.run_id,
        &harness.config.scenario_id,
        10_016,
        70010,
        "critical final message",
    );
    let identity = OperationIdentity::for_update(operation_id, &update)?;
    harness
        .configure_telegram(json!({
            "run_id": harness.config.run_id,
            "fault": {
                "method": "sendMessage",
                "operation_id": &identity.canonical_id,
                "external_id": &identity.external_id,
                "canonical_id": &identity.canonical_id,
                "status": 429,
                "retry_after": 7,
                "then": "success",
            },
        }))
        .await?;
    harness.enqueue_update(operation_id, update).await?;
    let first = harness
        .wait_for_operation(operation_id, OperationExpectation::RetryPending)
        .await?;
    let first_request_at = first["delivery"]["first_request_at_unix_ms"]
        .as_i64()
        .ok_or_else(|| HarnessError("429 operation omitted first request timestamp".into()))?;
    let deadline = first_request_at + 7_000;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let during_cooldown = harness.telegram_trace().await?;
    let premature = records_with_method_for_identity(&during_cooldown, "sendMessage", &identity)
        .into_iter()
        .filter_map(|record| record["at_unix_ms"].as_i64())
        .filter(|timestamp| *timestamp < deadline)
        .count();
    if premature != 1 {
        return Err(boxed_harness_error(HarnessError(format!(
            "Telegram sent {premature} requests before retry_after cooldown expired"
        ))));
    }
    harness
        .wait_for_operation(operation_id, OperationExpectation::CommittedAndDelivered)
        .await?;
    let final_trace = harness.telegram_trace().await?;
    let sends = records_with_method_for_identity(&final_trace, "sendMessage", &identity);
    if sends.len() != 2
        || sends
            .last()
            .and_then(|record| record["at_unix_ms"].as_i64())
            .is_some_and(|timestamp| timestamp < deadline)
        || active_message_ids(&final_trace)?.is_empty()
    {
        return Err(boxed_harness_error(HarnessError(
            "critical Telegram delivery did not reliably arrive after the shared cooldown".into(),
        )));
    }
    Ok(())
}

#[allow(dead_code)]
fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
