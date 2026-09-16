use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{from_fn, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use time::Duration;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::timeout::TimeoutLayer;
use tower_sessions::{Expiry, Session, SessionManagerLayer};
use zeroize::Zeroize;

use crate::adapters::http::session_store::SqliteSessionStore;

use crate::bootstrap::AppState;
use crate::domain::identity::Actor;
use crate::error::{AppError, AppResult};

#[derive(Deserialize)]
#[serde(transparent)]
struct SensitiveString(String);

impl SensitiveString {
    fn expose(&self) -> &str {
        &self.0
    }
}

impl Drop for SensitiveString {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Clone)]
pub struct HttpState {
    pub app: Arc<AppState>,
    password_hash_budget: Arc<tokio::sync::Semaphore>,
}

pub fn router(app: Arc<AppState>) -> Router {
    let cookie_name = if app.config.cookie_secure {
        "__Host-imbridge-session"
    } else {
        "imbridge-session"
    };
    let session_store = SqliteSessionStore::new(app.pool.clone());
    let session_layer = SessionManagerLayer::new(session_store)
        .with_name(cookie_name)
        .with_secure(app.config.cookie_secure)
        .with_http_only(true)
        .with_same_site(tower_sessions::cookie::SameSite::Lax)
        .with_expiry(Expiry::OnInactivity(Duration::hours(
            app.config.session_ttl_hours.max(1),
        )));

    let api = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/api/v1/csrf", get(csrf_token))
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/auth/logout", post(logout))
        .route("/api/v1/auth/me", get(me))
        .route("/api/v1/accounts", get(list_accounts).post(create_account))
        .route(
            "/api/v1/characters",
            get(native_backend_retired).post(native_backend_retired),
        )
        .route("/api/v1/characters/{id}", get(native_backend_retired))
        .route(
            "/api/v1/models/providers",
            get(native_backend_retired).post(native_backend_retired),
        )
        .route(
            "/api/v1/models/presets",
            get(native_backend_retired).post(native_backend_retired),
        )
        .route(
            "/api/v1/conversations",
            get(native_backend_retired).post(native_backend_retired),
        )
        .route("/api/v1/conversations/{id}", get(native_backend_retired))
        .route(
            "/api/v1/conversations/{id}/history",
            get(native_backend_retired),
        )
        .route(
            "/api/v1/conversations/{id}/messages",
            post(native_backend_retired),
        )
        .route(
            "/api/v1/conversations/{id}/undo",
            post(native_backend_retired),
        )
        .route(
            "/api/v1/conversations/{id}/redo",
            post(native_backend_retired),
        )
        .route(
            "/api/v1/conversations/{id}/compress",
            post(native_backend_retired),
        )
        .route("/api/v1/telegram/bots", get(list_bots).post(upsert_bot))
        .route("/api/v1/telegram/bots/{id}/start", post(start_bot))
        .route("/api/v1/telegram/bots/{id}/stop", post(stop_bot))
        .route(
            "/api/v1/telegram/bots/{id}/bind-code",
            post(create_bind_code),
        )
        .route("/api/v1/audit", get(list_audit))
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/style.css", get(style_css));

    let api = api
        .layer(from_fn(csrf_middleware))
        .with_state(HttpState {
            app: app.clone(),
            password_hash_budget: Arc::new(tokio::sync::Semaphore::new(4)),
        })
        .layer(session_layer)
        .layer(RequestBodyLimitLayer::new(64 * 1024))
        .layer(ConcurrencyLimitLayer::new(64))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            std::time::Duration::from_secs(30),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::REFERRER_POLICY,
            HeaderValue::from_static("no-referrer"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::X_FRAME_OPTIONS,
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(
                "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'; object-src 'none'",
            ),
        ));

    #[cfg(feature = "e2e-control")]
    let api = crate::adapters::http::e2e_control::mount_isolated_e2e_routes(api, app);

    api
}

async fn csrf_middleware(session: Session, req: Request, next: Next) -> Result<Response, AppError> {
    let method = req.method().as_str().to_string();
    require_csrf(&session, req.headers(), &method).await?;
    Ok(next.run(req).await)
}

async fn live() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"ok": true})))
}

async fn csrf_token(session: Session) -> AppResult<Json<Value>> {
    let token = ensure_csrf(&session).await?;
    Ok(Json(json!({"token": token})))
}

async fn ensure_csrf(session: &Session) -> AppResult<String> {
    if let Some(existing) = session
        .get::<String>("csrf_token")
        .await
        .map_err(|_| AppError::internal("session read failed"))?
    {
        return Ok(existing);
    }
    let token = crate::ids::new_id();
    session
        .insert("csrf_token", token.clone())
        .await
        .map_err(|_| AppError::internal("session write failed"))?;
    Ok(token)
}

fn is_safe_method(method: &str) -> bool {
    matches!(method, "GET" | "HEAD" | "OPTIONS")
}

async fn require_csrf(session: &Session, headers: &HeaderMap, method: &str) -> AppResult<()> {
    if is_safe_method(method) {
        return Ok(());
    }
    let expected = ensure_csrf(session).await?;
    let provided = headers
        .get("x-csrf-token")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if crate::adapters::secrets::encrypted_sqlite::hmac_eq(&expected, provided) {
        Ok(())
    } else {
        Err(AppError::forbidden("CSRF token missing or invalid"))
    }
}

async fn ready(State(state): State<HttpState>) -> impl IntoResponse {
    match sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&state.app.pool)
        .await
    {
        Ok(_) => {
            let st_configured = state.app.config.st.base_url.is_some();
            let st_mode = state.app.config.st.mode.clone();
            let write_ready = match state.app.st.as_ref() {
                Some(st) => st
                    .probe()
                    .await
                    .map(|status| status.write_ready())
                    .unwrap_or(false),
                None => false,
            };
            let required_write = state.app.config.st.write_mode().allows_write();
            let status = if required_write && !write_ready {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::OK
            };
            (
                status,
                Json(json!({
                    "ok": status == StatusCode::OK,
                    "st": {
                        "configured": st_configured,
                        "mode": st_mode,
                        "writeReady": write_ready
                    }
                })),
            )
                .into_response()
        }
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"ok": false, "error": {"code": "DB_UNAVAILABLE", "message": "database not ready"}})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct LoginBody {
    username: String,
    password: SensitiveString,
}

async fn login(
    State(state): State<HttpState>,
    session: Session,
    Json(body): Json<LoginBody>,
) -> AppResult<Json<Value>> {
    let _permit = state
        .password_hash_budget
        .clone()
        .try_acquire_owned()
        .map_err(|_| AppError::too_many("password verification capacity is busy"))?;
    let account = state
        .app
        .identity
        .authenticate(&body.username, body.password.expose())
        .await?;
    session
        .insert("account_id", account.id.clone())
        .await
        .map_err(|_| AppError::internal("session write failed"))?;
    session
        .cycle_id()
        .await
        .map_err(|_| AppError::internal("session rotate failed"))?;
    let workspace_id = state
        .app
        .identity
        .default_workspace_id(&account.id)
        .await
        .ok();
    Ok(Json(json!({
        "account": account,
        "workspaceId": workspace_id,
    })))
}

async fn logout(session: Session) -> AppResult<Json<Value>> {
    session
        .flush()
        .await
        .map_err(|_| AppError::internal("session flush failed"))?;
    Ok(Json(json!({"ok": true})))
}

async fn me(State(state): State<HttpState>, session: Session) -> AppResult<Json<Value>> {
    let actor = current_actor(&state, &session).await?;
    Ok(Json(json!({
        "account": actor.account,
        "workspaceId": actor.workspace_id,
    })))
}

#[derive(Deserialize)]
struct CreateAccountBody {
    username: String,
    password: SensitiveString,
    display_name: String,
    #[serde(default)]
    is_admin: bool,
}

async fn create_account(
    State(state): State<HttpState>,
    session: Session,
    Json(body): Json<CreateAccountBody>,
) -> AppResult<Json<Value>> {
    let actor = current_actor(&state, &session).await?;
    let _permit = state
        .password_hash_budget
        .clone()
        .try_acquire_owned()
        .map_err(|_| AppError::too_many("password hashing capacity is busy"))?;
    let account = state
        .app
        .identity
        .create_account(
            &actor,
            &body.username,
            body.password.expose(),
            &body.display_name,
            body.is_admin,
        )
        .await?;
    Ok(Json(json!(account)))
}

async fn list_accounts(State(state): State<HttpState>, session: Session) -> AppResult<Json<Value>> {
    let actor = current_actor(&state, &session).await?;
    if !actor.account.is_system_admin {
        return Err(AppError::forbidden("admin required"));
    }
    Ok(Json(
        json!({"items": state.app.identity.list_accounts().await?}),
    ))
}

async fn native_backend_retired() -> AppResult<Json<Value>> {
    Err(AppError::gone(
        crate::st_readiness::ST_WRITE_NOT_READY_CODE,
        crate::st_readiness::ST_WRITE_NOT_READY_MESSAGE,
    ))
}

async fn list_bots(State(state): State<HttpState>, session: Session) -> AppResult<Json<Value>> {
    let actor = current_actor(&state, &session).await?;
    require_workspace_manager(&actor)?;
    let items = state
        .app
        .telegram
        .list_bots(actor.require_workspace()?)
        .await?;
    let mut payload = Vec::new();
    for bot in items {
        payload.push(json!({
            "bot": bot,
            "status": state.app.telegram.runtime_status(&bot.id).await,
        }));
    }
    Ok(Json(json!({"items": payload})))
}

#[derive(Deserialize)]
struct BotBody {
    #[serde(default)]
    token: Option<SensitiveString>,
    #[serde(default)]
    desired_enabled: bool,
}

async fn upsert_bot(
    State(state): State<HttpState>,
    session: Session,
    Json(body): Json<BotBody>,
) -> AppResult<Json<Value>> {
    let actor = current_actor(&state, &session).await?;
    require_workspace_manager(&actor)?;
    let result = async {
        let secret_id = if let Some(token) = body
            .token
            .as_ref()
            .map(SensitiveString::expose)
            .filter(|value| !value.is_empty())
        {
            if !(16..=256).contains(&token.len())
                || token
                    .chars()
                    .any(|character| character.is_whitespace() || character.is_control())
            {
                return Err(AppError::bad_request(
                    "TELEGRAM_TOKEN_INVALID",
                    "Telegram bot token must contain 16-256 non-whitespace characters",
                ));
            }
            Some(
                state
                    .app
                    .vault
                    .put(
                        actor.require_workspace()?,
                        "telegram_bot_token",
                        token.as_bytes(),
                    )
                    .await?
                    .id,
            )
        } else {
            None
        };
        state
            .app
            .telegram
            .upsert_bot(&actor, secret_id.as_deref(), body.desired_enabled)
            .await
    }
    .await;
    let bot = result?;
    Ok(Json(json!(bot)))
}

async fn start_bot(
    State(state): State<HttpState>,
    session: Session,
    Path(id): Path<String>,
) -> AppResult<Json<Value>> {
    let actor = current_actor(&state, &session).await?;
    require_workspace_manager(&actor)?;
    let bot = state
        .app
        .telegram
        .get_bot(&id)
        .await?
        .ok_or_else(|| AppError::not_found("BOT_NOT_FOUND", "bot not found"))?;
    require_resource_workspace(&actor, &bot.workspace_id)?;
    state
        .app
        .telegram
        .start_bot(&bot, state.app.vault.as_ref())
        .await?;
    Ok(Json(
        json!({"ok": true, "status": state.app.telegram.runtime_status(&id).await}),
    ))
}

async fn stop_bot(
    State(state): State<HttpState>,
    session: Session,
    Path(id): Path<String>,
) -> AppResult<Json<Value>> {
    let actor = current_actor(&state, &session).await?;
    require_workspace_manager(&actor)?;
    let bot = state
        .app
        .telegram
        .get_bot(&id)
        .await?
        .ok_or_else(|| AppError::not_found("BOT_NOT_FOUND", "bot not found"))?;
    require_resource_workspace(&actor, &bot.workspace_id)?;
    state.app.telegram.stop_bot(&id, true).await?;
    Ok(Json(json!({"ok": true})))
}

async fn create_bind_code(
    State(state): State<HttpState>,
    session: Session,
    Path(id): Path<String>,
) -> AppResult<Json<Value>> {
    let actor = current_actor(&state, &session).await?;
    require_workspace_manager(&actor)?;
    let bot = state
        .app
        .telegram
        .get_bot(&id)
        .await?
        .ok_or_else(|| AppError::not_found("BOT_NOT_FOUND", "bot not found"))?;
    require_resource_workspace(&actor, &bot.workspace_id)?;
    let code = state
        .app
        .telegram
        .generate_bind_code(&id, &bot.owner_account_id)
        .await?;
    Ok(Json(json!({
        "code": code.code,
        "expiresAt": code.expires_at,
        "ttlMs": code.ttl_ms,
    })))
}

async fn list_audit(State(state): State<HttpState>, session: Session) -> AppResult<Json<Value>> {
    let actor = current_actor(&state, &session).await?;
    if !actor.account.is_system_admin {
        return Err(AppError::forbidden("system admin required"));
    }
    Ok(Json(json!({"items": state.app.audit.list(100).await?})))
}

async fn index() -> Response {
    asset("index.html", "text/html; charset=utf-8")
}

async fn app_js() -> Response {
    asset("app.js", "text/javascript; charset=utf-8")
}

async fn style_css() -> Response {
    asset("style.css", "text/css; charset=utf-8")
}

fn asset(path: &str, mime: &'static str) -> Response {
    match crate::adapters::http::static_ui::WebAssets::get(path) {
        Some(file) => {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, mime.parse().unwrap());
            (headers, file.data.to_vec()).into_response()
        }
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

fn require_workspace_manager(actor: &Actor) -> AppResult<()> {
    if actor.can_manage_workspace() {
        Ok(())
    } else {
        Err(AppError::forbidden("workspace admin required"))
    }
}

fn require_resource_workspace(actor: &Actor, workspace_id: &str) -> AppResult<()> {
    if actor.account.is_system_admin || actor.require_workspace()? == workspace_id {
        Ok(())
    } else {
        Err(AppError::forbidden(
            "resource is outside the current workspace",
        ))
    }
}

async fn current_actor(state: &HttpState, session: &Session) -> AppResult<Actor> {
    let account = if let Some(account_id) = session
        .get::<String>("account_id")
        .await
        .map_err(|_| AppError::internal("session read failed"))?
    {
        state
            .app
            .identity
            .get_by_id(&account_id)
            .await?
            .ok_or_else(|| AppError::unauthorized("login required"))?
    } else {
        return Err(AppError::unauthorized("login required"));
    };
    if account.disabled_at.is_some() {
        return Err(AppError::forbidden("account disabled"));
    }
    let workspace_id = state.app.identity.default_workspace_id(&account.id).await?;
    state
        .app
        .identity
        .actor_in_workspace(account, &workspace_id)
        .await
}

#[allow(dead_code)]
fn _body_type(_: Body) {}
