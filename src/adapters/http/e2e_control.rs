use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use axum::extract::{Extension, Path, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;
use tower_http::limit::RequestBodyLimitLayer;

use crate::adapters::secrets::encrypted_sqlite::hmac_eq;
use crate::bootstrap::AppState;
use crate::clock::now_rfc3339;
use crate::domain::st::StChatLocator;
use crate::error::{AppError, AppResult};
use crate::modules::bridge::channel_context::ChannelContextStore;

const CONTROL_TOKEN_HEADER: &str = "x-imbridge-e2e-control-token";
const MAX_BODY_BYTES: usize = 64 * 1024;
const MAX_ID_BYTES: usize = 128;
const MAX_OPERATION_IDS: usize = 256;
const MAX_EXPECTED_PARTICIPANTS: usize = 64;
const MAX_BARRIER_EVENTS: usize = 1024;
const MAX_ACTIVE_BARRIERS: usize = 256;
const MAX_SENTINEL_BYTES: usize = 4096;

type DeliveryObservationRow = (String, Option<String>, Option<String>, Option<String>);

const BARRIER_NAMES: &[&str] = &[
    "before_st_publish",
    "after_st_publish_before_response",
    "before_telegram_send",
    "after_telegram_send_before_ledger",
    "same-locator-commit",
    "connector-recheck-before-rename",
];

const FIXED_BOT_ID: &str = "test-bot";
const FIXED_BOT_TOKEN: &[u8] = b"test-token";
const FIXED_TELEGRAM_USER_ID: &str = "7001";
const FIXED_NUMERIC_BOT_ID: i64 = 900_000_001;
const FIXED_ST_HANDLE: &str = "default-user";
const FIXED_ST_AVATAR: &str = "TestCharacter.png";
const FIXED_ST_CHARACTER_NAME: &str = "TestCharacter";
const FIXED_ST_CHAT_FILE: &str = "IMBridge-Test-fixture";
const FIXED_ACCOUNT_DISPLAY_NAME: &str = "E2E User";
const FIXED_BOT_TOKEN_KIND: &str = "telegram_bot_token";

#[derive(Clone)]
struct E2eControlState {
    app: Arc<AppState>,
    scope: Arc<VerifiedScope>,
    runtime_instance_id: String,
    barriers: Arc<Mutex<HashMap<BarrierKey, BarrierState>>>,
}

struct VerifiedScope {
    run_id: String,
    control_token: String,
    fixture_sentinel: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BarrierKey {
    run_id: String,
    scenario_id: String,
    barrier: String,
}

#[derive(Clone)]
struct BarrierState {
    scenario_id: String,
    operation_ids: BTreeSet<String>,
    expected_participants: BTreeSet<String>,
    installed_participants: BTreeMap<String, BTreeSet<String>>,
    reached_participants: BTreeMap<String, BTreeSet<String>>,
    released_participants: BTreeMap<String, BTreeSet<String>>,
    events: Vec<BarrierEvent>,
    next_event_sequence: u64,
    status: &'static str,
    wired: bool,
}

#[derive(Clone, Serialize)]
struct BarrierEvent {
    sequence: u64,
    event: &'static str,
    operation_ids: Vec<String>,
    participants: Vec<String>,
}

#[derive(Serialize)]
struct BarrierSnapshot {
    ok: bool,
    barrier_id: String,
    scenario_id: String,
    status: &'static str,
    wired: bool,
    operation_ids: Vec<String>,
    expected_participants: Vec<String>,
    installed_participants: BTreeMap<String, Vec<String>>,
    reached_participants: BTreeMap<String, Vec<String>>,
    released_participants: BTreeMap<String, Vec<String>>,
    event_sequence: u64,
    events: Vec<BarrierEvent>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeQuery {
    run_id: String,
    #[serde(default)]
    scenario_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BarrierStatusQuery {
    run_id: String,
    scenario_id: String,
    barrier: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BarrierRequest {
    run_id: String,
    scenario_id: String,
    barrier: String,
    #[serde(default, alias = "operationIds")]
    operation_ids: Vec<String>,
    #[serde(default, alias = "expectedParticipants")]
    expected_participants: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedTurnRequest {
    run_id: String,
    scenario_id: String,
    seed_id: String,
    chat_id: i64,
    fixture_sentinel: String,
}

pub fn mount_isolated_e2e_routes(api: Router, app: Arc<AppState>) -> Router {
    let Some(scope) = verified_scope() else {
        return api;
    };
    let state = E2eControlState {
        app,
        scope,
        runtime_instance_id: uuid::Uuid::new_v4().to_string(),
        barriers: Arc::new(Mutex::new(HashMap::new())),
    };
    let controls = Router::new()
        .route("/_e2e/operations/{operation_id}", get(operation_status))
        .route("/_e2e/health/live", get(live))
        .route("/_e2e/health/ready", get(ready))
        .route("/_e2e/barriers/install", post(install_barrier))
        .route("/_e2e/barriers/release", post(release_barrier))
        .route("/_e2e/barriers/status", get(barrier_status))
        .route("/_e2e/seed-turn", post(seed_turn))
        .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
        .layer(Extension(state));
    api.merge(controls)
}

async fn operation_status(
    Extension(state): Extension<E2eControlState>,
    Path(operation_id): Path<String>,
    Query(query): Query<ScopeQuery>,
    headers: HeaderMap,
) -> AppResult<Json<serde_json::Value>> {
    authenticate(&headers, &query.run_id, &state)?;
    if !valid_id(&operation_id) {
        return Err(AppError::bad_request(
            "E2E_OPERATION_ID_INVALID",
            "operation id is empty or outside the bounded ID format",
        ));
    }
    if query
        .scenario_id
        .as_deref()
        .is_some_and(|scenario| !valid_id(scenario))
    {
        return Err(AppError::bad_request(
            "E2E_SCENARIO_ID_INVALID",
            "scenario_id is empty or outside the bounded ID format",
        ));
    }
    let operation: Option<(String, String, String, String, i64, String, String)> = sqlx::query_as(
        "SELECT id, status, commit_state, bot_id, telegram_update_id, channel_context_key, request_meta_json
         FROM bridge_operations WHERE id = ?",
    )
    .bind(&operation_id)
    .fetch_optional(&state.app.pool)
    .await?;
    let Some((
        id,
        runtime_status,
        commit_state,
        bot_id,
        telegram_update_id,
        context_key,
        request_meta_json,
    )) = operation
    else {
        return Err(AppError::not_found(
            "E2E_OPERATION_NOT_FOUND",
            "operation was not found in the runtime ledger",
        ));
    };
    let scenario_id = query.scenario_id.as_deref().ok_or_else(|| {
        AppError::conflict(
            "E2E_OBSERVATION_NOT_READY",
            "operation observation requires a bound scenario scope",
        )
    })?;
    let metadata: serde_json::Value = serde_json::from_str(&request_meta_json).map_err(|_| {
        AppError::conflict(
            "E2E_OBSERVATION_NOT_READY",
            "operation request metadata is not valid JSON",
        )
    })?;
    let metadata_scenario = metadata
        .get("scenario_id")
        .and_then(serde_json::Value::as_str);
    let update_scope_matches = if metadata_scenario.is_none() {
        let raw_update: Option<String> = sqlx::query_scalar(
            "SELECT raw_update_json FROM telegram_updates WHERE bot_id = ? AND update_id = ?",
        )
        .bind(&bot_id)
        .bind(telegram_update_id)
        .fetch_optional(&state.app.pool)
        .await?;
        raw_update
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .and_then(|update| {
                update
                    .pointer("/imbridge/scenario_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .as_deref()
            == Some(scenario_id)
    } else {
        false
    };
    if metadata_scenario != Some(scenario_id) && !update_scope_matches {
        return Err(AppError::conflict(
            "E2E_OBSERVATION_NOT_READY",
            "operation scenario scope is not durably bound",
        ));
    }
    let deliveries: Vec<DeliveryObservationRow> = sqlx::query_as(
        "SELECT status, external_message_id, terminal_at, terminal_evidence_json
         FROM channel_deliveries WHERE turn_id = ? ORDER BY created_at ASC, id ASC",
    )
    .bind(&id)
    .fetch_all(&state.app.pool)
    .await?;
    let ledger: Vec<(i64, i64, String, String, String)> = sqlx::query_as(
        "SELECT chunk_index, message_id, status, lifecycle, locator_hash
         FROM telegram_turn_messages WHERE operation_id = ?
         ORDER BY chunk_index ASC, id ASC",
    )
    .bind(&id)
    .fetch_all(&state.app.pool)
    .await?;
    let delivery_status = if deliveries
        .iter()
        .any(|(status, _, _, _)| status == "unknown")
    {
        "unknown"
    } else if deliveries
        .iter()
        .any(|(status, _, _, _)| status == "retry_pending")
    {
        "retry_at"
    } else if deliveries
        .iter()
        .any(|(status, _, _, _)| status == "pending" || status == "sending")
    {
        "pending"
    } else if deliveries
        .iter()
        .any(|(status, _, _, _)| status == "failed")
    {
        "failed"
    } else if !deliveries.is_empty()
        && deliveries
            .iter()
            .all(|(status, _, _, _)| matches!(status.as_str(), "sent" | "delivered"))
    {
        "delivered"
    } else {
        "pending"
    };
    let effects = deliveries
        .into_iter()
        .map(|(status, message_id, terminal_at, evidence)| {
            json!({
                "status": status,
                "message_id": message_id,
                "terminal_at": terminal_at,
                "terminal_evidence": evidence,
            })
        })
        .collect::<Vec<_>>();
    if matches!(runtime_status.as_str(), "committed" | "delivered") && effects.is_empty() {
        return Err(AppError::conflict(
            "E2E_OPERATION_EVIDENCE_INCOMPLETE",
            "committed operation has no durable delivery effect evidence",
        ));
    }
    let runtime_turn_ledger = ledger
        .into_iter()
        .map(
            |(chunk_index, message_id, status, lifecycle, locator_hash)| {
                json!({
                    "chunk_index": chunk_index,
                    "message_id": message_id,
                    "status": status,
                    "lifecycle": lifecycle,
                    "locator_hash": locator_hash,
                })
            },
        )
        .collect::<Vec<_>>();
    let terminal_evidence = effects
        .iter()
        .filter_map(|effect| effect.get("terminal_evidence").cloned())
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "ok": true,
        "run_id": state.scope.run_id,
        "scenario_id": scenario_id,
        "operation_id": id,
        "runtime_status": runtime_status,
        "commit_state": commit_state,
        "delivery_status": delivery_status,
        "effects": effects,
        "terminal_evidence": terminal_evidence,
        "runtime_turn_ledger": runtime_turn_ledger,
        "bot_id": bot_id,
        "channel_context_key": context_key,
    })))
}

fn verified_scope() -> Option<Arc<VerifiedScope>> {
    let run_id = match required_env("IMBRIDGE_E2E_RUN_ID") {
        Some(value) if valid_run_id(&value) => value,
        _ => {
            tracing::warn!(
                code = "E2E_SCOPE_INVALID",
                "isolated E2E control routes disabled"
            );
            return None;
        }
    };
    let Some(control_token) = bounded_env("IMBRIDGE_E2E_CONTROL_TOKEN", MAX_SENTINEL_BYTES) else {
        tracing::warn!(
            code = "E2E_CONTROL_TOKEN_MISSING",
            "isolated E2E control routes disabled"
        );
        return None;
    };
    let Some(fixture_sentinel) = bounded_env("IMBRIDGE_E2E_FIXTURE_SENTINEL", MAX_SENTINEL_BYTES)
    else {
        tracing::warn!(
            code = "E2E_FIXTURE_SENTINEL_MISSING",
            "isolated E2E control routes disabled"
        );
        return None;
    };
    Some(Arc::new(VerifiedScope {
        run_id,
        control_token,
        fixture_sentinel,
    }))
}

fn required_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn bounded_env(name: &str, max_bytes: usize) -> Option<String> {
    let value = required_env(name)?;
    (value.len() <= max_bytes).then_some(value)
}

fn valid_run_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':' | b'.'))
}

fn authenticate(
    headers: &HeaderMap,
    requested_run_id: &str,
    state: &E2eControlState,
) -> AppResult<()> {
    let provided_token = headers
        .get(CONTROL_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if provided_token.len() > MAX_SENTINEL_BYTES
        || !hmac_eq(&state.scope.control_token, provided_token)
    {
        return Err(AppError::new(
            "E2E_CONTROL_UNAUTHORIZED",
            "E2E control token missing or invalid",
            StatusCode::UNAUTHORIZED,
        ));
    }
    if !valid_run_id(requested_run_id) || !hmac_eq(&state.scope.run_id, requested_run_id) {
        return Err(AppError::new(
            "E2E_RUN_ID_MISMATCH",
            "E2E run_id does not match this runtime",
            StatusCode::FORBIDDEN,
        ));
    }
    Ok(())
}

async fn live(
    Extension(state): Extension<E2eControlState>,
    Query(query): Query<ScopeQuery>,
    headers: HeaderMap,
) -> AppResult<Json<serde_json::Value>> {
    authenticate(&headers, &query.run_id, &state)?;
    validate_optional_scenario_id(query.scenario_id.as_deref())?;
    Ok(Json(json!({
        "ok": true,
        "feature": "e2e-control",
        "runtime_instance_id": state.runtime_instance_id.clone(),
        "runIdMatched": true,
        "fixtureConfigured": true,
        "fixture_ready": false,
        "seed_wired": false,
        "status": "live"
    })))
}

async fn ready(
    Extension(state): Extension<E2eControlState>,
    Query(query): Query<ScopeQuery>,
    headers: HeaderMap,
) -> AppResult<axum::response::Response> {
    authenticate(&headers, &query.run_id, &state)?;
    validate_optional_scenario_id(query.scenario_id.as_deref())?;
    sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&state.app.pool)
        .await
        .map_err(|_| {
            AppError::service_unavailable("E2E_RUNTIME_NOT_READY", "runtime database is not ready")
        })?;
    if isolated_seed_is_ready(&state).await? {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "ok": true,
                "feature": "e2e-control",
                "runtime_instance_id": state.runtime_instance_id.clone(),
                "runIdMatched": true,
                "fixtureConfigured": true,
                "fixture_ready": true,
                "seed_wired": true,
                "wired": true,
                "status": "ready",
            })),
        )
            .into_response());
    }
    Ok((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "ok": false,
            "feature": "e2e-control",
            "runtime_instance_id": state.runtime_instance_id.clone(),
            "runIdMatched": true,
            "fixtureConfigured": true,
            "fixture_ready": false,
            "seed_wired": false,
            "wired": false,
            "status": "blocked",
            "error": "E2E_RUNTIME_NOT_READY",
        })),
    )
        .into_response())
}

async fn install_barrier(
    Extension(state): Extension<E2eControlState>,
    headers: HeaderMap,
    Json(request): Json<BarrierRequest>,
) -> AppResult<Json<BarrierSnapshot>> {
    authenticate(&headers, &request.run_id, &state)?;
    let barrier = parse_barrier(&request.barrier)?;
    if !valid_id(&request.scenario_id) {
        return Err(AppError::bad_request(
            "E2E_SCENARIO_ID_INVALID",
            "scenario_id is empty or outside the bounded ID format",
        ));
    }
    let scenario_id = request.scenario_id.clone();
    let operation_ids = validate_ids(
        request.operation_ids,
        MAX_OPERATION_IDS,
        "E2E_BARRIER_OPERATION_IDS_INVALID",
    )?;
    let expected_participants = validate_ids(
        request.expected_participants,
        MAX_EXPECTED_PARTICIPANTS,
        "E2E_BARRIER_PARTICIPANTS_INVALID",
    )?;
    let key = barrier_key_for(&state, &scenario_id, barrier);
    let mut barriers = state.barriers.lock().await;
    if barriers.contains_key(&key) {
        return Err(AppError::conflict(
            "E2E_BARRIER_ALREADY_INSTALLED",
            "barrier is already installed for this scenario",
        ));
    }
    if barriers.len() >= MAX_ACTIVE_BARRIERS {
        return Err(AppError::conflict(
            "E2E_BARRIER_LIMIT",
            "runtime barrier state reached its configured bound",
        ));
    }
    let operation_ids = operation_ids.into_iter().collect::<BTreeSet<_>>();
    let expected_participants = expected_participants.into_iter().collect::<BTreeSet<_>>();
    let installed_participants = operation_ids
        .iter()
        .map(|id| (id.clone(), BTreeSet::new()))
        .collect();
    let reached_participants = operation_ids
        .iter()
        .map(|id| (id.clone(), BTreeSet::new()))
        .collect();
    let released_participants = operation_ids
        .iter()
        .map(|id| (id.clone(), BTreeSet::new()))
        .collect();
    let mut barrier_state = BarrierState {
        scenario_id,
        operation_ids,
        expected_participants,
        installed_participants,
        reached_participants,
        released_participants,
        events: Vec::new(),
        next_event_sequence: 1,
        status: "installed_not_wired",
        wired: false,
    };
    barrier_state.record_event("installed")?;
    let snapshot = barrier_state.snapshot(barrier, "installed_not_wired");
    barriers.insert(key, barrier_state);
    Ok(Json(snapshot))
}

async fn release_barrier(
    Extension(state): Extension<E2eControlState>,
    headers: HeaderMap,
    Json(request): Json<BarrierRequest>,
) -> AppResult<Json<BarrierSnapshot>> {
    authenticate(&headers, &request.run_id, &state)?;
    let barrier = parse_barrier(&request.barrier)?;
    if !valid_id(&request.scenario_id) {
        return Err(AppError::bad_request(
            "E2E_SCENARIO_ID_INVALID",
            "scenario_id is empty or outside the bounded ID format",
        ));
    }
    let key = barrier_key_for(&state, &request.scenario_id, barrier);
    let requested_operation_ids = validate_ids(
        request.operation_ids,
        MAX_OPERATION_IDS,
        "E2E_BARRIER_OPERATION_IDS_INVALID",
    )?
    .into_iter()
    .collect::<BTreeSet<_>>();
    let requested_expected_participants = validate_ids(
        request.expected_participants,
        MAX_EXPECTED_PARTICIPANTS,
        "E2E_BARRIER_PARTICIPANTS_INVALID",
    )?
    .into_iter()
    .collect::<BTreeSet<_>>();
    let mut barriers = state.barriers.lock().await;
    let barrier_state = barriers.get_mut(&key).ok_or_else(|| {
        AppError::not_found(
            "E2E_BARRIER_NOT_INSTALLED",
            "barrier is not installed for this scenario",
        )
    })?;
    if barrier_state.operation_ids != requested_operation_ids {
        return Err(AppError::conflict(
            "E2E_BARRIER_OPERATION_IDS_MISMATCH",
            "barrier operation_ids do not match the installed scope",
        ));
    }
    if requested_expected_participants != barrier_state.expected_participants {
        return Err(AppError::conflict(
            "E2E_BARRIER_PARTICIPANTS_MISMATCH",
            "barrier expected_participants do not match the installed scope",
        ));
    }
    barrier_state.record_event("release_blocked_not_wired")?;
    barrier_state.status = "blocked";
    Ok(Json(barrier_state.snapshot(barrier, "blocked")))
}

async fn barrier_status(
    Extension(state): Extension<E2eControlState>,
    Query(query): Query<BarrierStatusQuery>,
    headers: HeaderMap,
) -> AppResult<Json<BarrierSnapshot>> {
    authenticate(&headers, &query.run_id, &state)?;
    let barrier = parse_barrier(&query.barrier)?;
    if !valid_id(&query.scenario_id) {
        return Err(AppError::bad_request(
            "E2E_SCENARIO_ID_INVALID",
            "scenario_id is empty or outside the bounded ID format",
        ));
    }
    let key = barrier_key_for(&state, &query.scenario_id, barrier);
    let barriers = state.barriers.lock().await;
    let barrier_state = barriers.get(&key).ok_or_else(|| {
        AppError::not_found(
            "E2E_BARRIER_NOT_INSTALLED",
            "barrier is not installed for this scenario",
        )
    })?;
    Ok(Json(barrier_state.snapshot(barrier, barrier_state.status)))
}

async fn seed_turn(
    Extension(state): Extension<E2eControlState>,
    headers: HeaderMap,
    Json(request): Json<SeedTurnRequest>,
) -> AppResult<Json<serde_json::Value>> {
    authenticate(&headers, &request.run_id, &state)?;
    if !valid_id(&request.scenario_id) {
        return Err(AppError::bad_request(
            "E2E_SCENARIO_ID_INVALID",
            "scenario_id is empty or outside the bounded ID format",
        ));
    }
    if !valid_id(&request.seed_id) {
        return Err(AppError::bad_request(
            "E2E_SEED_ID_INVALID",
            "seed_id is empty or outside the bounded ID format",
        ));
    }
    if request.chat_id == 0 {
        return Err(AppError::bad_request(
            "E2E_CHAT_ID_INVALID",
            "chat_id must be non-zero",
        ));
    }
    if request.fixture_sentinel.is_empty()
        || request.fixture_sentinel.len() > MAX_SENTINEL_BYTES
        || !hmac_eq(&state.scope.fixture_sentinel, &request.fixture_sentinel)
    {
        return Err(AppError::forbidden("fixture sentinel missing or invalid"));
    }

    let (account, workspace_id) = state
        .app
        .identity
        .ensure_legacy_account(FIXED_ST_HANDLE, FIXED_ACCOUNT_DISPLAY_NAME)
        .await?;
    ensure_fixed_e2e_bot(&state, &account.id, &workspace_id).await?;
    let bind = state
        .app
        .telegram
        .generate_bind_code(FIXED_BOT_ID, &account.id)
        .await?;
    let redeemed = state
        .app
        .telegram
        .redeem_bind_code(
            FIXED_BOT_ID,
            &account.id,
            &bind.code,
            FIXED_TELEGRAM_USER_ID,
        )
        .await?;
    if redeemed != "ok" {
        return Err(AppError::conflict(
            "E2E_SEED_BIND_FAILED",
            "isolated telegram bind did not complete",
        ));
    }

    let context_key = format!("{FIXED_BOT_ID}:{}", request.chat_id);
    let locator = StChatLocator {
        handle: FIXED_ST_HANDLE.to_owned(),
        avatar: FIXED_ST_AVATAR.to_owned(),
        character_name: FIXED_ST_CHARACTER_NAME.to_owned(),
        chat_file: FIXED_ST_CHAT_FILE.to_owned(),
    };
    ChannelContextStore::new(state.app.pool.clone())
        .select_chat(&account.id, &workspace_id, &context_key, &locator)
        .await?;

    let bot = state
        .app
        .telegram
        .get_bot(FIXED_BOT_ID)
        .await?
        .ok_or_else(|| {
            AppError::conflict("E2E_SEED_BOT_MISSING", "fixed isolated bot is missing")
        })?;
    let poller_status = state.app.telegram.runtime_status(FIXED_BOT_ID).await;
    match poller_status.as_str() {
        "stopped" => {
            state
                .app
                .telegram
                .start_bot(&bot, state.app.vault.as_ref())
                .await?;
        }
        "running" => {}
        _ => {
            return Err(AppError::conflict(
                "E2E_SEED_POLLER_NOT_READY",
                "isolated poller is not in a startable state",
            ));
        }
    }
    let poller_status = state.app.telegram.runtime_status(FIXED_BOT_ID).await;
    let numeric_bot_id = state.app.telegram.numeric_bot_id(FIXED_BOT_ID).await;
    if poller_status != "running" || numeric_bot_id != Some(FIXED_NUMERIC_BOT_ID) {
        return Err(AppError::conflict(
            "E2E_SEED_POLLER_NOT_READY",
            "isolated poller did not reach the required running identity",
        ));
    }

    Ok(Json(json!({
        "ok": true,
        "seed_wired": true,
        "seed_id": request.seed_id,
        "scenario_id": request.scenario_id,
        "bot_id": FIXED_BOT_ID,
        "numeric_bot_id": FIXED_NUMERIC_BOT_ID,
        "account_id": account.id,
        "workspace_id": workspace_id,
        "channel_context_key": context_key,
        "poller_status": "running",
        "locator": {
            "handle": locator.handle,
            "avatar": locator.avatar,
            "character_name": locator.character_name,
            "chat_file": locator.chat_file,
        },
    })))
}

async fn ensure_fixed_e2e_bot(
    state: &E2eControlState,
    account_id: &str,
    workspace_id: &str,
) -> AppResult<crate::domain::channel::TelegramBot> {
    if let Some(existing) = state.app.telegram.get_bot(FIXED_BOT_ID).await? {
        let token_secret_id = existing
            .token_secret_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if existing.workspace_id != workspace_id
            || existing.owner_account_id != account_id
            || token_secret_id.is_none()
        {
            return Err(AppError::conflict(
                "E2E_SEED_BOT_CONFLICT",
                "fixed isolated bot identity does not match this run",
            ));
        }
        return Ok(existing);
    }

    let secret = state
        .app
        .vault
        .put(workspace_id, FIXED_BOT_TOKEN_KIND, FIXED_BOT_TOKEN)
        .await?;
    let now = now_rfc3339();
    let inserted = sqlx::query(
        "INSERT INTO telegram_bots
            (id, workspace_id, owner_account_id, token_secret_id, desired_enabled, created_at, updated_at)
         VALUES (?, ?, ?, ?, 0, ?, ?)",
    )
    .bind(FIXED_BOT_ID)
    .bind(workspace_id)
    .bind(account_id)
    .bind(&secret.id)
    .bind(&now)
    .bind(&now)
    .execute(&state.app.pool)
    .await;
    if inserted.is_err() {
        let _ = state.app.vault.delete(&secret.id).await;
        return Err(AppError::conflict(
            "E2E_SEED_BOT_CONFLICT",
            "fixed isolated bot could not be created",
        ));
    }
    state
        .app
        .telegram
        .get_bot(FIXED_BOT_ID)
        .await?
        .ok_or_else(|| AppError::conflict("E2E_SEED_BOT_MISSING", "fixed isolated bot is missing"))
}

async fn isolated_seed_is_ready(state: &E2eControlState) -> AppResult<bool> {
    let Some(bot) = state.app.telegram.get_bot(FIXED_BOT_ID).await? else {
        return Ok(false);
    };
    if bot
        .token_secret_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        return Ok(false);
    }
    if state.app.telegram.runtime_status(FIXED_BOT_ID).await != "running"
        || state.app.telegram.numeric_bot_id(FIXED_BOT_ID).await != Some(FIXED_NUMERIC_BOT_ID)
    {
        return Ok(false);
    }
    let bindings: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM telegram_bindings
         WHERE bot_id = ? AND revoked_at IS NULL",
    )
    .bind(FIXED_BOT_ID)
    .fetch_one(&state.app.pool)
    .await
    .map_err(|_| {
        AppError::service_unavailable("E2E_RUNTIME_NOT_READY", "runtime database is not ready")
    })?;
    if bindings < 1 {
        return Ok(false);
    }
    let contexts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM channel_contexts
         WHERE account_id = ?
           AND channel = 'telegram'
           AND st_handle = ?
           AND st_character_avatar = ?
           AND st_character_name = ?
           AND st_chat_file = ?",
    )
    .bind(&bot.owner_account_id)
    .bind(FIXED_ST_HANDLE)
    .bind(FIXED_ST_AVATAR)
    .bind(FIXED_ST_CHARACTER_NAME)
    .bind(FIXED_ST_CHAT_FILE)
    .fetch_one(&state.app.pool)
    .await
    .map_err(|_| {
        AppError::service_unavailable("E2E_RUNTIME_NOT_READY", "runtime database is not ready")
    })?;
    Ok(contexts >= 1)
}

fn validate_optional_scenario_id(scenario_id: Option<&str>) -> AppResult<()> {
    if let Some(scenario_id) = scenario_id {
        if !valid_id(scenario_id) {
            return Err(AppError::bad_request(
                "E2E_SCENARIO_ID_INVALID",
                "scenario_id is empty or outside the bounded ID format",
            ));
        }
    }
    Ok(())
}

fn parse_barrier(value: &str) -> AppResult<&'static str> {
    BARRIER_NAMES
        .iter()
        .copied()
        .find(|allowed| *allowed == value)
        .ok_or_else(|| {
            AppError::bad_request(
                "E2E_BARRIER_INVALID",
                "barrier is outside the fixed E2E barrier allowlist",
            )
        })
}

fn barrier_key_for(state: &E2eControlState, scenario_id: &str, barrier: &str) -> BarrierKey {
    BarrierKey {
        run_id: state.scope.run_id.clone(),
        scenario_id: scenario_id.to_owned(),
        barrier: barrier.to_owned(),
    }
}

fn validate_ids(values: Vec<String>, max: usize, code: &'static str) -> AppResult<Vec<String>> {
    if values.is_empty() || values.len() > max {
        return Err(AppError::bad_request(
            code,
            "ID collection is empty or exceeds the configured bound",
        ));
    }
    let mut unique = BTreeSet::new();
    for value in values {
        if !valid_id(&value) || !unique.insert(value) {
            return Err(AppError::bad_request(
                code,
                "ID collection contains an invalid or duplicate ID",
            ));
        }
    }
    Ok(unique.into_iter().collect())
}

impl BarrierState {
    fn record_event(&mut self, event: &'static str) -> AppResult<()> {
        if self.events.len() >= MAX_BARRIER_EVENTS {
            return Err(AppError::conflict(
                "E2E_BARRIER_EVENT_LIMIT",
                "barrier event history reached its configured bound",
            ));
        }
        let sequence = self.next_event_sequence;
        self.next_event_sequence = sequence.checked_add(1).ok_or_else(|| {
            AppError::conflict(
                "E2E_BARRIER_EVENT_LIMIT",
                "barrier event sequence reached its configured bound",
            )
        })?;
        self.events.push(BarrierEvent {
            sequence,
            event,
            operation_ids: self.operation_ids.iter().cloned().collect(),
            participants: Vec::new(),
        });
        Ok(())
    }

    fn snapshot(&self, barrier: &str, status: &'static str) -> BarrierSnapshot {
        BarrierSnapshot {
            ok: true,
            barrier_id: barrier.to_owned(),
            scenario_id: self.scenario_id.clone(),
            status,
            wired: self.wired,
            operation_ids: self.operation_ids.iter().cloned().collect(),
            expected_participants: self.expected_participants.iter().cloned().collect(),
            installed_participants: self
                .installed_participants
                .iter()
                .map(|(operation_id, participants)| {
                    (operation_id.clone(), participants.iter().cloned().collect())
                })
                .collect(),
            reached_participants: self
                .reached_participants
                .iter()
                .map(|(operation_id, participants)| {
                    (operation_id.clone(), participants.iter().cloned().collect())
                })
                .collect(),
            released_participants: self
                .released_participants
                .iter()
                .map(|(operation_id, participants)| {
                    (operation_id.clone(), participants.iter().cloned().collect())
                })
                .collect(),
            event_sequence: self.events.last().map(|event| event.sequence).unwrap_or(0),
            events: self.events.clone(),
        }
    }
}
