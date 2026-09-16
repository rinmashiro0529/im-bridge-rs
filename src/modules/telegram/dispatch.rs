use serde_json::{json, Value};

use crate::domain::identity::Actor;
use crate::domain::st::StChatLocator;
use crate::error::{AppError, AppResult};
use crate::modules::bridge::engine::SidecarBridgeEngine;
use crate::modules::bridge::errors::{CommitState, StBridgeError, StErrorCode, StErrorStage};
use crate::modules::telegram::callback::CallbackAction;
use crate::modules::telegram::delivery::{RevokeEffectResult, TelegramDelivery};
use crate::modules::telegram::error_render;
use crate::modules::telegram::panel::{
    render_help, render_home, render_provider_models, render_providers, render_settings,
    render_status, ModelPurpose, TelegramPanel,
};
use crate::modules::telegram::TelegramModule;
use crate::seams::st_bridge_engine::{
    BridgeOperationOrigin, ModelOverrideKind, StBridgeCommand, StBridgeCommandEnvelope,
    StBridgeEngine, StBridgeQuery, StBridgeView,
};

const PAGE_SIZE: usize = 8;

#[derive(Debug, Clone)]
pub struct TelegramReply {
    pub text: String,
    pub markup: Option<Value>,
    pub generation: Option<GenerationIntent>,
}

#[derive(Debug, Clone)]
pub struct GenerationIntent {
    pub kind: GenerationKind,
    pub conversation_id: String,
    pub context_key: String,
    pub user_text: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationKind {
    Send,
    Redo,
    Compress,
}

pub async fn dispatch_update(
    module: &TelegramModule,
    bot_id: &str,
    update: &Value,
) -> AppResult<Vec<TelegramReply>> {
    dispatch_update_with_delivery(module, bot_id, update, None).await
}

pub async fn dispatch_update_with_delivery(
    module: &TelegramModule,
    bot_id: &str,
    update: &Value,
    delivery: Option<&TelegramDelivery>,
) -> AppResult<Vec<TelegramReply>> {
    dispatch_update_with_delivery_and_guard(module, bot_id, update, delivery, None).await
}

pub async fn dispatch_update_with_delivery_and_guard(
    module: &TelegramModule,
    bot_id: &str,
    update: &Value,
    delivery: Option<&TelegramDelivery>,
    ownership_guard: Option<&crate::modules::bridge::poller_ownership::PollerOwnershipGuard>,
) -> AppResult<Vec<TelegramReply>> {
    let Some(mut services) = module.services_async().await else {
        return Ok(Vec::new());
    };
    if let Some(guard) = ownership_guard {
        if let Some(bridge) = services.bridge.take() {
            services.bridge = Some(bridge.with_ownership_guard(std::sync::Arc::new(guard.clone())));
        }
    }
    if let Some(callback) = update.get("callback_query") {
        let data = callback
            .get("data")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let callback_id = callback
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if callback_id.trim().is_empty() {
            return Err(AppError::bad_request(
                "TELEGRAM_CALLBACK_INVALID",
                "callback query id is required",
            ));
        }
        if let Some(delivery) = delivery {
            // Telegram clients show a spinner until this ACK arrives.  Keep it
            // before actor lookup or catalog I/O so slow ST requests cannot
            // make an otherwise valid button look broken.
            if let Err(err) = delivery
                .answer_callback_query(callback_id, None, false)
                .await
            {
                tracing::warn!(code = %err.code, "telegram callback ACK failed");
            }
        }
        let user_id = callback
            .pointer("/from/id")
            .and_then(Value::as_i64)
            .map(|id| id.to_string())
            .ok_or_else(|| AppError::bad_request("TG_USER_MISSING", "callback has no user"))?;
        let chat_id = callback
            .pointer("/message/chat/id")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        let message_id = callback
            .pointer("/message/message_id")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        return handle_callback(
            module, bot_id, &user_id, chat_id, message_id, data, delivery,
        )
        .await;
    }
    let Some(message) = update.get("message") else {
        return Ok(Vec::new());
    };
    let chat_id = message
        .pointer("/chat/id")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let user_id = message
        .pointer("/from/id")
        .and_then(Value::as_i64)
        .map(|id| id.to_string());
    let text = message
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let Some(user_id) = user_id else {
        return Ok(vec![TelegramReply {
            text: "无法识别 Telegram 用户。".into(),
            markup: None,
            generation: None,
        }]);
    };
    let command_token = text.split_whitespace().next().unwrap_or_default();
    if command_token.split('@').next() == Some("/bind") {
        let code = text[command_token.len()..].trim();
        if !code.is_empty() {
            return handle_bind(module, bot_id, &user_id, code).await;
        }
        return Ok(vec![TelegramReply {
            text: "用法：/bind <验证码>".into(),
            markup: None,
            generation: None,
        }]);
    }
    let actor = match module.resolve_bound_actor(bot_id, &user_id).await? {
        Some(actor) => actor,
        None => {
            return Ok(vec![TelegramReply {
                text: "未授权：请先在网页生成验证码，然后发送 /bind <验证码>。".into(),
                markup: None,
                generation: None,
            }]);
        }
    };
    let context_key = format!("{bot_id}:{chat_id}");
    let update_id = update
        .get("update_id")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let operation_origin = BridgeOperationOrigin {
        internal_bot_id: bot_id.to_string(),
        telegram_update_id: update_id,
        channel_context_key: context_key.clone(),
    };
    let client_turn_id = update
        .get("update_id")
        .and_then(Value::as_i64)
        .map(|update_id| format!("tg:{bot_id}:{update_id}"));
    let command = first_command(&text);
    match command {
        "/start" => Ok(vec![panel_reply(render_home())]),
        "/help" => Ok(vec![panel_reply(render_help())]),
        "/settings" => settings_panel(&services, &actor, &context_key).await,
        "/status" => Ok(vec![panel_reply(render_status(
            &module.runtime_status(bot_id).await,
            "Telegram channel",
            module.numeric_bot_id(bot_id).await.unwrap_or_default(),
            None,
        ))]),
        "/chars" | "/characters" => list_characters(&services, &actor, &context_key, 0).await,
        "/hist" | "/history" => list_history(&services, &actor, &context_key, 0).await,
        "/new" => {
            start_new(
                &services,
                &actor,
                &context_key,
                client_turn_id,
                operation_origin.clone(),
            )
            .await
        }
        "/now" | "/current" => current_state(&services, &actor, &context_key).await,
        "/last" => last_turn(&services, &actor, &context_key).await,
        "/undo" => {
            undo_turn(
                module,
                bot_id,
                chat_id,
                &services,
                &actor,
                &context_key,
                client_turn_id,
                delivery,
                operation_origin.clone(),
            )
            .await
        }
        "/revoke" => {
            revoke_turn(
                module,
                bot_id,
                chat_id,
                &services,
                &actor,
                &context_key,
                client_turn_id,
                delivery,
                operation_origin.clone(),
            )
            .await
        }
        "/redo" => {
            redo_turn(
                &services,
                &actor,
                &context_key,
                client_turn_id,
                delivery,
                operation_origin.clone(),
            )
            .await
        }
        "/recent" => recent_conversations(&services, &actor, &context_key).await,
        "/model" => list_models(&services, &actor, &context_key, "chat", 0).await,
        "/cmodel" => list_models(&services, &actor, &context_key, "compression", 0).await,
        "/compress" => {
            compress_history(
                &services,
                &actor,
                &context_key,
                client_turn_id,
                operation_origin.clone(),
            )
            .await
        }
        "/error" => last_error_summary(module, &actor, bot_id, chat_id).await,
        _ if text.starts_with('/') => Ok(vec![TelegramReply {
            text: "未知命令。发送 /help 查看可用命令。".into(),
            markup: None,
            generation: None,
        }]),
        _ => {
            send_chat(
                &services,
                &actor,
                &context_key,
                client_turn_id,
                &text,
                operation_origin.clone(),
            )
            .await
        }
    }
}

async fn handle_bind(
    module: &TelegramModule,
    bot_id: &str,
    user_id: &str,
    code: &str,
) -> AppResult<Vec<TelegramReply>> {
    let bot = module
        .get_bot(bot_id)
        .await?
        .ok_or_else(|| AppError::not_found("BOT_NOT_FOUND", "bot not found"))?;
    let outcome = module
        .redeem_bind_code(bot_id, &bot.owner_account_id, code, user_id)
        .await?;
    let text = match outcome.as_str() {
        "ok" => "绑定成功。现在可以使用 /chars 选择角色。",
        "expired" => "验证码已过期，请在网页重新生成。",
        "rate_limited" => "尝试次数过多，请 15 分钟后再试。",
        "identity_conflict" => "此 Telegram 用户已绑定到另一个账号，请先在原账号撤销绑定。",
        _ => "验证码无效。",
    };
    Ok(vec![TelegramReply {
        text: text.into(),
        markup: None,
        generation: None,
    }])
}

async fn handle_callback(
    module: &TelegramModule,
    bot_id: &str,
    user_id: &str,
    chat_id: i64,
    message_id: i64,
    data: &str,
    delivery: Option<&TelegramDelivery>,
) -> AppResult<Vec<TelegramReply>> {
    let Some(services) = module.services_async().await else {
        return Ok(Vec::new());
    };
    let Some(actor) = module.resolve_bound_actor(bot_id, user_id).await? else {
        return Ok(vec![TelegramReply {
            text: "未授权。".into(),
            markup: None,
            generation: None,
        }]);
    };
    let context_key = format!("{bot_id}:{chat_id}");
    if data.starts_with("cb:") {
        let numeric_bot_id = module.numeric_bot_id(bot_id).await.ok_or_else(|| {
            AppError::bad_request(
                "TELEGRAM_CALLBACK_SCOPE_MISMATCH",
                "callback 已失效，请重新打开面板",
            )
        })?;
        if numeric_bot_id <= 0 || message_id <= 0 {
            return Err(AppError::bad_request(
                "TELEGRAM_CALLBACK_SCOPE_MISMATCH",
                "callback 已失效，请重新打开面板",
            ));
        }
        let slot = module
            .panel_store()
            .find_active_slot_for_scope(&actor.account.id, numeric_bot_id, chat_id)
            .await?
            .ok_or_else(|| {
                AppError::bad_request(
                    "TELEGRAM_CALLBACK_INVALID",
                    "callback 已失效，请重新打开面板",
                )
            })?;
        let (action, effect_id) = module
            .callback_store()
            .consume(
                data,
                &actor.account.id,
                bot_id,
                numeric_bot_id,
                chat_id,
                user_id,
                &slot.panel_id,
                message_id,
                slot.revision,
            )
            .await?;
        module
            .callback_store()
            .claim_action_effect(&effect_id)
            .await?;
        let replies = match handle_panel_callback(
            module,
            bot_id,
            &actor,
            &services,
            &context_key,
            action,
        )
        .await
        {
            Ok(replies) => {
                if let Err(error) = module
                    .callback_store()
                    .complete_action_effect(&effect_id)
                    .await
                {
                    if let Err(state_error) = module
                        .callback_store()
                        .fail_action_effect(&effect_id, true)
                        .await
                    {
                        return Err(AppError::internal(format!(
                            "callback action terminal persist failed with {} and unknown CAS failed with {}",
                            error.code, state_error.code
                        )));
                    }
                    return Err(error);
                }
                replies
            }
            Err(error) => {
                module
                    .callback_store()
                    .fail_action_effect(&effect_id, false)
                    .await?;
                return Err(error);
            }
        };
        if let Some(delivery) = delivery {
            for reply in &replies {
                if apply_panel_reply(
                    module, bot_id, &actor, user_id, chat_id, message_id, reply, delivery,
                )
                .await?
                {
                    return Ok(Vec::new());
                }
            }
        }
        return Ok(replies);
    }
    // Legacy callbacks are intentionally not interpreted as commands. They
    // contain no server-side scope/revision binding and can only display the
    // generic expired-selection message.
    Ok(vec![TelegramReply {
        text: "选择已失效，请重新打开面板。".into(),
        markup: None,
        generation: None,
    }])
}

async fn handle_panel_callback(
    module: &TelegramModule,
    bot_id: &str,
    actor: &Actor,
    services: &crate::modules::telegram::TelegramServices,
    context_key: &str,
    action: CallbackAction,
) -> AppResult<Vec<TelegramReply>> {
    match action {
        CallbackAction::Home => Ok(vec![panel_reply(render_home())]),
        CallbackAction::Help => Ok(vec![panel_reply(render_help())]),
        CallbackAction::Settings => settings_panel(services, actor, context_key).await,
        CallbackAction::Status => Ok(vec![panel_reply(render_status(
            &module.runtime_status(bot_id).await,
            "Telegram channel",
            module.numeric_bot_id(bot_id).await.unwrap_or_default(),
            None,
        ))]),
        CallbackAction::Characters { page } => {
            list_characters(services, actor, context_key, page).await
        }
        CallbackAction::History { page } => list_history(services, actor, context_key, page).await,
        CallbackAction::Recent { page } => recent_panel(services, actor, context_key, page).await,
        CallbackAction::Now => current_state(services, actor, context_key).await,
        CallbackAction::SelectCharacter { page, index } => {
            select_character(services, actor, context_key, page, index).await
        }
        CallbackAction::SelectHistory { page, index } => {
            open_history(services, actor, context_key, page, index).await
        }
        CallbackAction::SelectRecent { page, index } => {
            open_recent(services, actor, context_key, page * PAGE_SIZE + index).await
        }
        CallbackAction::Providers { purpose, page } => {
            list_models(services, actor, context_key, purpose.short_name(), page).await
        }
        CallbackAction::ProviderModels {
            purpose,
            provider_key,
            page,
        } => list_provider_models(services, actor, context_key, purpose, &provider_key, page).await,
        CallbackAction::SelectModel {
            purpose,
            provider_key,
            model_index,
        } => {
            select_provider_model(
                services,
                actor,
                context_key,
                purpose,
                &provider_key,
                model_index,
            )
            .await
        }
        CallbackAction::SelectModelById {
            purpose,
            provider_key,
            model_id,
        } => {
            select_provider_model_by_id(
                services,
                actor,
                context_key,
                purpose,
                &provider_key,
                &model_id,
            )
            .await
        }
        CallbackAction::ResetModel { purpose } => {
            require_bridge(services, context_key)?
                .execute(
                    actor,
                    StBridgeCommand::SetModelOverride {
                        kind: model_override_kind(purpose),
                        model_id: None,
                    },
                )
                .await
                .map_err(st_err)?;
            list_models(services, actor, context_key, purpose.short_name(), 0).await
        }
    }
}

// Panel replacement requires explicit actor, Telegram scope, message, and delivery context.
#[allow(clippy::too_many_arguments)]
async fn apply_panel_reply(
    module: &TelegramModule,
    bot_id: &str,
    actor: &Actor,
    user_id: &str,
    chat_id: i64,
    message_id: i64,
    reply: &TelegramReply,
    delivery: &TelegramDelivery,
) -> AppResult<bool> {
    let Some(markup) = reply.markup.as_ref() else {
        return Ok(false);
    };
    let keyboard = crate::modules::telegram::panel::keyboard_from_markup(markup)?;
    let Some(panel) = crate::modules::telegram::panel::lookup_panel(&reply.text, &keyboard) else {
        return Ok(false);
    };
    let slot = if let Some(numeric_bot_id) = module.numeric_bot_id(bot_id).await {
        module
            .panel_store()
            .find_active_slot_for_scope(&actor.account.id, numeric_bot_id, chat_id)
            .await?
    } else {
        module
            .panel_store()
            .find_active_slot_for_account_chat(&actor.account.id, chat_id)
            .await?
    };
    let Some(slot) = slot else {
        return Ok(false);
    };
    if slot.message_id != message_id {
        return Ok(true);
    }
    let (effect, next_slot) = crate::modules::telegram::panel::plan_effect(
        Some(&slot),
        panel,
        slot.panel_id.clone(),
        slot.account_id.clone(),
        slot.numeric_bot_id,
        slot.chat_id,
        600,
    );
    let crate::modules::telegram::panel::PanelEffect::Replace {
        expected_revision,
        panel: desired,
        slot: effect_slot,
    } = effect
    else {
        return Ok(true);
    };
    let desired = crate::modules::telegram::bind_panel_callbacks(
        module,
        &desired,
        &actor.account.id,
        user_id,
        bot_id,
        slot.numeric_bot_id,
        slot.chat_id,
        &slot.panel_id,
        slot.message_id,
        next_slot.revision,
    )
    .await?;
    let claim_panel = desired.clone();
    let effect = crate::modules::telegram::panel::PanelEffect::Replace {
        slot: effect_slot,
        expected_revision,
        panel: desired,
    };
    let Some(effect_id) = module
        .panel_store()
        .claim_panel_revision(&slot, expected_revision, &claim_panel, &next_slot)
        .await?
    else {
        return Ok(true);
    };
    module
        .panel_store()
        .mark_panel_revision_sending(&effect_id, &slot, expected_revision)
        .await?;
    match delivery.execute_panel_effect(&effect).await {
        Ok(_) => {
            if let Err(error) = module
                .panel_store()
                .complete_panel_replace(&effect_id, &slot, expected_revision, &next_slot)
                .await
            {
                if let Err(state_error) = module
                    .panel_store()
                    .mark_panel_revision_failed(&effect_id, &slot, expected_revision)
                    .await
                {
                    return Err(AppError::internal(format!(
                        "panel effect completion failed with {} and failure CAS failed with {}",
                        error.code, state_error.code
                    )));
                }
                return Err(error);
            }
        }
        Err(err) => {
            module
                .panel_store()
                .mark_panel_revision_failed(&effect_id, &slot, expected_revision)
                .await?;
            return Err(err);
        }
    }
    Ok(true)
}

async fn settings_panel(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
) -> AppResult<Vec<TelegramReply>> {
    let view = require_bridge(services, context_key)?
        .query(actor, StBridgeQuery::GetModels)
        .await
        .map_err(st_err)?;
    let (chat, comp) = match view {
        StBridgeView::Models {
            override_chat,
            override_compression,
            ..
        } => (override_chat, override_compression),
        _ => (None, None),
    };
    Ok(vec![panel_reply(render_settings(
        chat.as_deref(),
        comp.as_deref(),
    ))])
}

fn model_override_kind(purpose: ModelPurpose) -> ModelOverrideKind {
    match purpose {
        ModelPurpose::Chat => ModelOverrideKind::Chat,
        ModelPurpose::Compression => ModelOverrideKind::Compression,
    }
}

fn panel_reply(panel: TelegramPanel) -> TelegramReply {
    crate::modules::telegram::panel::remember_panel(&panel);
    let markup = Some(json!({
        "inline_keyboard": panel
            .keyboard
            .iter()
            .map(|row| row.iter().map(|button| json!({
                "text": button.label.as_str(),
                "callback_data": button.callback_data.as_str(),
            })).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
    }));
    TelegramReply {
        text: panel.text,
        markup,
        generation: None,
    }
}

async fn recent_panel(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    _context_key: &str,
    page: usize,
) -> AppResult<Vec<TelegramReply>> {
    let items = services.channel.list_recent(&actor.account.id, 64).await?;
    Ok(vec![panel_reply(
        crate::modules::telegram::panel::render_recent(&items, page),
    )])
}

fn grouped_model_catalog(
    catalog: &crate::domain::st::StModelCatalog,
) -> std::collections::BTreeMap<String, Vec<crate::domain::st::StModelSummary>> {
    let mut grouped = std::collections::BTreeMap::new();
    for model in &catalog.models {
        grouped
            .entry(
                model
                    .owned_by
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| "未分类".into()),
            )
            .or_insert_with(Vec::new)
            .push(model.clone());
    }
    grouped
}

async fn list_provider_models(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    purpose: ModelPurpose,
    provider_key: &str,
    page: usize,
) -> AppResult<Vec<TelegramReply>> {
    let view = require_bridge(services, context_key)?
        .query(actor, StBridgeQuery::GetModels)
        .await
        .map_err(st_err)?;
    let StBridgeView::Models {
        catalog,
        override_chat,
        override_compression,
    } = view
    else {
        return Ok(Vec::new());
    };
    let grouped = grouped_model_catalog(&catalog);
    let Some(key) = resolve_provider_key(&grouped, provider_key) else {
        return Ok(vec![TelegramReply {
            text: "厂商选择已失效，请重新打开模型面板。".into(),
            markup: None,
            generation: None,
        }]);
    };
    let models = grouped.get(&key).cloned().unwrap_or_default();
    let total_pages = models.len().div_ceil(PAGE_SIZE).max(1);
    let current = match purpose {
        ModelPurpose::Chat => override_chat,
        ModelPurpose::Compression => override_compression,
    };
    Ok(vec![panel_reply(render_provider_models(
        purpose,
        &key,
        &models,
        current.as_deref(),
        page,
        total_pages,
    ))])
}

fn resolve_provider_key(
    grouped: &std::collections::BTreeMap<String, Vec<crate::domain::st::StModelSummary>>,
    token: &str,
) -> Option<String> {
    grouped.contains_key(token).then(|| token.to_string())
}

async fn select_provider_model(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    purpose: ModelPurpose,
    provider_key: &str,
    model_index: usize,
) -> AppResult<Vec<TelegramReply>> {
    let view = require_bridge(services, context_key)?
        .query(actor, StBridgeQuery::GetModels)
        .await
        .map_err(st_err)?;
    let StBridgeView::Models { catalog, .. } = view else {
        return Ok(Vec::new());
    };
    let grouped = grouped_model_catalog(&catalog);
    let Some(key) = resolve_provider_key(&grouped, provider_key) else {
        return Ok(vec![TelegramReply {
            text: "厂商选择已失效，请重新打开模型面板。".into(),
            markup: None,
            generation: None,
        }]);
    };
    let Some(model) = grouped.get(&key).and_then(|models| models.get(model_index)) else {
        return Ok(vec![TelegramReply {
            text: "模型选择已失效，请重新打开模型面板。".into(),
            markup: None,
            generation: None,
        }]);
    };
    require_bridge(services, context_key)?
        .execute(
            actor,
            StBridgeCommand::SetModelOverride {
                kind: model_override_kind(purpose),
                model_id: Some(model.id.clone()),
            },
        )
        .await
        .map_err(st_err)?;
    list_provider_models(services, actor, context_key, purpose, &key, 0).await
}

async fn select_provider_model_by_id(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    purpose: ModelPurpose,
    provider_key: &str,
    model_id: &str,
) -> AppResult<Vec<TelegramReply>> {
    let view = require_bridge(services, context_key)?
        .query(actor, StBridgeQuery::GetModels)
        .await
        .map_err(st_err)?;
    let StBridgeView::Models { catalog, .. } = view else {
        return Ok(Vec::new());
    };
    let grouped = grouped_model_catalog(&catalog);
    let Some(models) = grouped.get(provider_key) else {
        return Ok(vec![TelegramReply {
            text: "厂商选择已失效，请重新打开模型面板。".into(),
            markup: None,
            generation: None,
        }]);
    };
    let Some(model) = models.iter().find(|model| model.id == model_id) else {
        return Ok(vec![TelegramReply {
            text: "模型选择已失效，请重新打开模型面板。".into(),
            markup: None,
            generation: None,
        }]);
    };
    require_bridge(services, context_key)?
        .execute(
            actor,
            StBridgeCommand::SetModelOverride {
                kind: model_override_kind(purpose),
                model_id: Some(model.id.clone()),
            },
        )
        .await
        .map_err(st_err)?;
    list_provider_models(services, actor, context_key, purpose, provider_key, 0).await
}

fn require_bridge(
    services: &crate::modules::telegram::TelegramServices,
    context_key: &str,
) -> AppResult<SidecarBridgeEngine> {
    services
        .bridge
        .as_ref()
        .map(|engine| engine.with_context_key(context_key))
        .ok_or_else(|| {
            AppError::from_st(StBridgeError::new(
                StErrorCode::StWriteNotReady,
                StErrorStage::Control,
                crate::st_readiness::ST_WRITE_NOT_READY_MESSAGE,
                false,
                CommitState::NotStarted,
            ))
        })
}

fn st_err(error: impl Into<Box<StBridgeError>>) -> AppError {
    AppError::from_st(error)
}

async fn list_characters(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    page: usize,
) -> AppResult<Vec<TelegramReply>> {
    let view = require_bridge(services, context_key)?
        .query(actor, StBridgeQuery::ListCharacters)
        .await
        .map_err(st_err)?;
    let StBridgeView::Characters(items) = view else {
        return Err(st_err(StBridgeError::new(
            StErrorCode::StCharacterListFailed,
            StErrorStage::Catalog,
            "character list unavailable",
            false,
            CommitState::NotStarted,
        )));
    };
    Ok(vec![panel_reply(
        crate::modules::telegram::panel::render_characters(&items, page, items.len()),
    )])
}

async fn select_character(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    page: usize,
    index: usize,
) -> AppResult<Vec<TelegramReply>> {
    let engine = require_bridge(services, context_key)?;
    let view = engine
        .query(actor, StBridgeQuery::ListCharacters)
        .await
        .map_err(st_err)?;
    let StBridgeView::Characters(items) = view else {
        return Err(st_err(StBridgeError::new(
            StErrorCode::StCharacterListFailed,
            StErrorStage::Catalog,
            "character list unavailable",
            false,
            CommitState::NotStarted,
        )));
    };
    let start = page * PAGE_SIZE;
    let Some(character) = items.get(start + index) else {
        return Ok(vec![TelegramReply {
            text: "角色选择已失效，请重新发送 /chars。".into(),
            markup: None,
            generation: None,
        }]);
    };
    require_bridge(services, context_key)?
        .execute(
            actor,
            StBridgeCommand::SelectCharacter {
                avatar: character.avatar.clone(),
                character_name: character.name.clone(),
            },
        )
        .await
        .map_err(st_err)?;
    list_history(services, actor, context_key, 0).await
}

async fn list_history(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    page: usize,
) -> AppResult<Vec<TelegramReply>> {
    let locator = services
        .channel
        .load(&actor.account.id, context_key)
        .await?;
    let Some(avatar) = locator.avatar.as_deref() else {
        return Ok(vec![TelegramReply {
            text: "当前还没有选择角色。请先使用 /chars。".into(),
            markup: None,
            generation: None,
        }]);
    };
    let view = require_bridge(services, context_key)?
        .query(
            actor,
            StBridgeQuery::ListCharacterChats {
                avatar: avatar.to_string(),
            },
        )
        .await
        .map_err(st_err)?;
    let StBridgeView::Chats(chats) = view else {
        return Err(st_err(StBridgeError::new(
            StErrorCode::StChatListFailed,
            StErrorStage::Catalog,
            "chat list unavailable",
            false,
            CommitState::NotStarted,
        )));
    };
    Ok(vec![panel_reply(
        crate::modules::telegram::panel::render_history(
            locator.character_name.as_deref().unwrap_or("未选择"),
            &chats,
            page,
            chats.len(),
        ),
    )])
}

async fn open_history(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    page: usize,
    index: usize,
) -> AppResult<Vec<TelegramReply>> {
    let locator = services
        .channel
        .load(&actor.account.id, context_key)
        .await?;
    let Some(avatar) = locator.avatar.clone() else {
        return Ok(vec![TelegramReply {
            text: "当前还没有选择角色。请先使用 /chars。".into(),
            markup: None,
            generation: None,
        }]);
    };
    let view = require_bridge(services, context_key)?
        .query(
            actor,
            StBridgeQuery::ListCharacterChats {
                avatar: avatar.clone(),
            },
        )
        .await
        .map_err(st_err)?;
    let StBridgeView::Chats(chats) = view else {
        return Err(st_err(StBridgeError::new(
            StErrorCode::StChatListFailed,
            StErrorStage::Catalog,
            "chat list unavailable",
            false,
            CommitState::NotStarted,
        )));
    };
    let start = page * PAGE_SIZE;
    let Some(chat) = chats.get(start + index) else {
        return Ok(vec![TelegramReply {
            text: "会话选择已失效，请重新发送 /hist。".into(),
            markup: None,
            generation: None,
        }]);
    };
    require_bridge(services, context_key)?
        .execute(
            actor,
            StBridgeCommand::SelectChat {
                locator: StChatLocator {
                    handle: locator.handle.unwrap_or_else(|| "default-user".into()),
                    avatar,
                    character_name: locator.character_name.unwrap_or_default(),
                    chat_file: chat.chat_file.clone(),
                },
            },
        )
        .await
        .map_err(st_err)?;
    current_state(services, actor, context_key).await
}

async fn start_new(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    client_turn_id: Option<String>,
    origin: BridgeOperationOrigin,
) -> AppResult<Vec<TelegramReply>> {
    let locator = require_selected_character(services, actor, context_key).await?;
    let outcome = require_bridge(services, context_key)?
        .execute_enveloped(
            actor,
            StBridgeCommandEnvelope {
                command: StBridgeCommand::StartChat {
                    locator,
                    client_operation_id: client_turn_id.unwrap_or_default(),
                },
                origin,
            },
        )
        .await
        .map_err(st_err)?;
    Ok(vec![TelegramReply {
        text: outcome.reply_text.unwrap_or_else(|| "已新建会话。".into()),
        markup: None,
        generation: None,
    }])
}

fn st_locator_from_context(
    context: &crate::domain::channel::StChannelLocator,
) -> AppResult<StChatLocator> {
    let avatar = context.avatar.clone().ok_or_else(|| {
        AppError::bad_request(
            "CHARACTER_NOT_SELECTED",
            "当前还没有选择角色。请先使用 /chars。",
        )
    })?;
    let chat_file = context.chat_file.clone().ok_or_else(|| {
        AppError::bad_request(
            "SESSION_NOT_SELECTED",
            "当前没有绑定角色和会话。请先使用 /chars。",
        )
    })?;
    Ok(StChatLocator {
        handle: context
            .handle
            .clone()
            .unwrap_or_else(|| "default-user".into()),
        avatar,
        character_name: context.character_name.clone().unwrap_or_default(),
        chat_file,
    })
}

async fn current_state(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
) -> AppResult<Vec<TelegramReply>> {
    let context = services
        .channel
        .load(&actor.account.id, context_key)
        .await?;
    let Ok(locator) = st_locator_from_context(&context) else {
        return Ok(vec![TelegramReply {
            text: "当前还没有绑定角色和会话。请先使用 /chars。".into(),
            markup: None,
            generation: None,
        }]);
    };
    let view = require_bridge(services, context_key)?
        .query(
            actor,
            StBridgeQuery::GetHistory {
                locator: locator.clone(),
            },
        )
        .await
        .map_err(st_err)?;
    let preview = match view {
        StBridgeView::History { preview } if !preview.trim().is_empty() => Some(preview),
        _ => None,
    };
    let chunks = preview
        .as_deref()
        .map(crate::modules::telegram::panel::split_current_preview)
        .unwrap_or_default();
    let preview_status =
        (!chunks.is_empty()).then(|| format!("完整最近内容已在上方分 {} 段发送。", chunks.len()));
    let mut replies = chunks
        .into_iter()
        .map(|text| TelegramReply {
            text,
            markup: None,
            generation: None,
        })
        .collect::<Vec<_>>();
    replies.push(panel_reply(
        crate::modules::telegram::panel::render_current(
            &locator.character_name,
            &locator.chat_file,
            None,
            preview_status.as_deref(),
        ),
    ));
    Ok(replies)
}

async fn last_turn(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
) -> AppResult<Vec<TelegramReply>> {
    let context = services
        .channel
        .load(&actor.account.id, context_key)
        .await?;
    let locator = st_locator_from_context(&context)?;
    let view = require_bridge(services, context_key)?
        .query(actor, StBridgeQuery::GetLastTurn { locator })
        .await
        .map_err(st_err)?;
    let mut lines = vec!["最后一轮：".to_string(), String::new()];
    match view {
        StBridgeView::LastTurn {
            user, assistant, ..
        } => {
            if user.is_none() && assistant.is_none() {
                lines.push("当前会话还没有可查看的尾部对话。".into());
            }
            if let Some(text) = user {
                lines.push("用户：".into());
                lines.push(text);
                lines.push(String::new());
            }
            if let Some(text) = assistant {
                lines.push("角色：".into());
                lines.push(text);
            }
        }
        _ => lines.push("当前会话还没有可查看的尾部对话。".into()),
    }
    Ok(vec![TelegramReply {
        text: lines.join("\n").trim().to_string(),
        markup: None,
        generation: None,
    }])
}

#[allow(clippy::too_many_arguments)]
async fn undo_turn(
    module: &TelegramModule,
    bot_id: &str,
    chat_id: i64,
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    client_turn_id: Option<String>,
    delivery: Option<&TelegramDelivery>,
    origin: BridgeOperationOrigin,
) -> AppResult<Vec<TelegramReply>> {
    let locator = require_selected_chat(services, actor, context_key).await?;
    let operation_id = client_turn_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            AppError::bad_request(
                "TELEGRAM_OPERATION_ID_REQUIRED",
                "undo requires a canonical operation id",
            )
        })?;
    let numeric_bot_id = module.numeric_bot_id(bot_id).await.ok_or_else(|| {
        AppError::service_unavailable(
            "TELEGRAM_BOT_SCOPE_UNAVAILABLE",
            "Telegram undo has no numeric bot scope",
        )
    })?;
    let scope = crate::modules::telegram::delivery::TurnScope::new(
        actor.account.id.clone(),
        bot_id,
        numeric_bot_id,
        chat_id,
        crate::modules::bridge::st_ops::locator_hash(
            &locator.handle,
            &locator.avatar,
            &locator.chat_file,
        ),
    )?;
    let delivery = delivery.ok_or_else(|| {
        AppError::service_unavailable(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "undo requires a Telegram delivery scope",
        )
    })?;
    let bridge = require_bridge(services, context_key)?;
    let claimed = bridge
        .claim_operation_with_origin(
            actor,
            &operation_id,
            crate::modules::bridge::operations::KIND_UNDO,
            &locator,
            origin.clone(),
        )
        .await
        .map_err(st_err)?;
    if matches!(
        claimed.record.status,
        crate::modules::bridge::operations::BridgeOperationStatus::Delivered
    ) && matches!(
        claimed.record.commit_state,
        crate::modules::bridge::operations::OperationCommitState::Applied
    ) {
        return Ok(Vec::new());
    }
    let target = if let Some(target) = delivery.load_frozen_target(&operation_id, &scope).await? {
        match target.status.as_str() {
            "applied" => return Ok(Vec::new()),
            "applying" | "unknown" => {
                return Err(AppError::conflict(
                    "TELEGRAM_TURN_TARGET_IN_PROGRESS",
                    "undo target is already being reconciled",
                ));
            }
            "failed" => {
                return Err(AppError::conflict(
                    "TELEGRAM_TURN_TARGET_FAILED",
                    "undo target has a recorded terminal failure",
                ));
            }
            "frozen" => target,
            _ => {
                return Err(AppError::conflict(
                    "TELEGRAM_TURN_TARGET_INVALID",
                    "undo target status is invalid",
                ))
            }
        }
    } else {
        let view = bridge
            .query(
                actor,
                StBridgeQuery::GetLastTurn {
                    locator: locator.clone(),
                },
            )
            .await
            .map_err(st_err)?;
        let tail_fingerprint = match view {
            StBridgeView::LastTurn {
                tail_fingerprint: Some(fingerprint),
                ..
            } if fingerprint.len() == 64
                && fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit())
                && fingerprint == fingerprint.to_ascii_lowercase() =>
            {
                fingerprint
            }
            _ => {
                return Err(AppError::conflict(
                    "TELEGRAM_TURN_FINGERPRINT_INVALID",
                    "undo requires a valid frozen ST tail fingerprint",
                ))
            }
        };
        let (target_turn_id, old_message_ids, target_revision) =
            delivery.find_last_turn(&scope).await?;
        let target_turn_id = target_turn_id.ok_or_else(|| {
            AppError::conflict(
                "TELEGRAM_TURN_TARGET_MISSING",
                "undo has no active Telegram turn target",
            )
        })?;
        delivery
            .freeze_target(
                &operation_id,
                &scope,
                &target_turn_id,
                &tail_fingerprint,
                target_revision,
                &old_message_ids,
            )
            .await?
    };
    delivery
        .cas_target_status(&operation_id, &scope, "frozen", "applying")
        .await?;
    if let Err(error) = delivery
        .assert_turn_revision(&scope, &target.target_turn_id, target.target_revision)
        .await
    {
        delivery
            .cas_target_status(&operation_id, &scope, "applying", "failed")
            .await?;
        return Err(error);
    }
    let outcome = match bridge
        .execute_enveloped(
            actor,
            StBridgeCommandEnvelope {
                command: StBridgeCommand::UndoLastTurn {
                    locator,
                    client_operation_id: operation_id.clone(),
                },
                origin,
            },
        )
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            let next = if error.commit_state == CommitState::NotApplied {
                "failed"
            } else {
                "unknown"
            };
            delivery
                .cas_target_status(&operation_id, &scope, "applying", next)
                .await?;
            return Err(st_err(error));
        }
    };
    if !outcome.confirmed_commit {
        delivery
            .cas_target_status(&operation_id, &scope, "applying", "unknown")
            .await?;
        return Err(AppError::conflict(
            "ST_COMMIT_STATE_UNKNOWN",
            "SillyTavern undo commit was not confirmed",
        ));
    }
    if let Err(error) = delivery
        .retire_turn_scoped(&scope, &target.target_turn_id, &operation_id)
        .await
    {
        if let Err(state_error) = delivery
            .cas_target_status(&operation_id, &scope, "applying", "unknown")
            .await
        {
            return Err(AppError::internal(format!(
                "turn retirement failed with {} and target unknown CAS failed with {}",
                error.code, state_error.code
            )));
        }
        return Err(error);
    }
    if let Err(error) = delivery
        .cas_target_status(&operation_id, &scope, "applying", "applied")
        .await
    {
        if let Err(state_error) = delivery
            .cas_target_status(&operation_id, &scope, "applying", "unknown")
            .await
        {
            return Err(AppError::internal(format!(
                "turn target applied CAS failed with {} and unknown CAS failed with {}",
                error.code, state_error.code
            )));
        }
        return Err(error);
    }
    let removed = outcome
        .removed_safe_content
        .unwrap_or_else(|| "最后一轮".into());
    Ok(vec![TelegramReply {
        text: format!("已在 SillyTavern 撤回最后一轮（Telegram 消息保持保留）：{removed}"),
        markup: None,
        generation: None,
    }])
}

#[allow(clippy::too_many_arguments)]
async fn revoke_turn(
    module: &TelegramModule,
    bot_id: &str,
    chat_id: i64,
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    client_turn_id: Option<String>,
    delivery: Option<&TelegramDelivery>,
    origin: BridgeOperationOrigin,
) -> AppResult<Vec<TelegramReply>> {
    let locator = require_selected_chat(services, actor, context_key).await?;
    let operation_id = client_turn_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            AppError::bad_request(
                "TELEGRAM_OPERATION_ID_REQUIRED",
                "revoke requires a canonical operation id",
            )
        })?;
    let numeric_bot_id = module.numeric_bot_id(bot_id).await.ok_or_else(|| {
        AppError::service_unavailable(
            "TELEGRAM_BOT_SCOPE_UNAVAILABLE",
            "Telegram revoke has no numeric bot scope",
        )
    })?;
    let scope = crate::modules::telegram::delivery::TurnScope::new(
        actor.account.id.clone(),
        bot_id,
        numeric_bot_id,
        chat_id,
        crate::modules::bridge::st_ops::locator_hash(
            &locator.handle,
            &locator.avatar,
            &locator.chat_file,
        ),
    )?;
    let bridge = require_bridge(services, context_key)?;
    let Some(delivery) = delivery else {
        return Err(AppError::service_unavailable(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "revoke requires a Telegram delivery scope",
        ));
    };
    let claimed = bridge
        .claim_operation_with_origin(
            actor,
            &operation_id,
            crate::modules::bridge::operations::KIND_REVOKE,
            &locator,
            origin.clone(),
        )
        .await
        .map_err(st_err)?;
    if matches!(
        claimed.record.status,
        crate::modules::bridge::operations::BridgeOperationStatus::Delivered
    ) && matches!(
        claimed.record.commit_state,
        crate::modules::bridge::operations::OperationCommitState::Applied
    ) {
        return Ok(Vec::new());
    }
    let target = if let Some(target) = delivery.load_frozen_target(&operation_id, &scope).await? {
        match target.status.as_str() {
            "applied" => return Ok(Vec::new()),
            "applying" | "unknown" => {
                return Err(AppError::conflict(
                    "TELEGRAM_TURN_TARGET_IN_PROGRESS",
                    "revoke target is already being reconciled",
                ));
            }
            "failed" => {
                return Err(AppError::conflict(
                    "TELEGRAM_TURN_TARGET_FAILED",
                    "revoke target has a recorded terminal failure",
                ));
            }
            "frozen" => target,
            _ => {
                return Err(AppError::conflict(
                    "TELEGRAM_TURN_TARGET_INVALID",
                    "revoke target status is invalid",
                ))
            }
        }
    } else {
        let view = bridge
            .query(
                actor,
                StBridgeQuery::GetLastTurn {
                    locator: locator.clone(),
                },
            )
            .await
            .map_err(st_err)?;
        let tail_fingerprint = match view {
            StBridgeView::LastTurn {
                tail_fingerprint: Some(fingerprint),
                ..
            } if fingerprint.len() == 64
                && fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit())
                && fingerprint == fingerprint.to_ascii_lowercase() =>
            {
                fingerprint
            }
            _ => {
                return Err(AppError::conflict(
                    "TELEGRAM_TURN_FINGERPRINT_INVALID",
                    "revoke requires a valid frozen ST tail fingerprint",
                ))
            }
        };
        let (target_turn_id, target_message_ids, target_revision) =
            delivery.find_last_turn(&scope).await?;
        let target_turn_id = target_turn_id.ok_or_else(|| {
            AppError::conflict(
                "TELEGRAM_TURN_TARGET_MISSING",
                "revoke has no active Telegram turn target",
            )
        })?;
        delivery
            .freeze_target(
                &operation_id,
                &scope,
                &target_turn_id,
                &tail_fingerprint,
                target_revision,
                &target_message_ids,
            )
            .await?
    };
    delivery
        .cas_target_status(&operation_id, &scope, "frozen", "applying")
        .await?;
    if let Err(error) = delivery
        .assert_turn_revision(&scope, &target.target_turn_id, target.target_revision)
        .await
    {
        delivery
            .cas_target_status(&operation_id, &scope, "applying", "failed")
            .await?;
        return Err(error);
    }
    let outcome = match bridge
        .execute_enveloped(
            actor,
            StBridgeCommandEnvelope {
                command: StBridgeCommand::RevokeLastTurn {
                    locator,
                    client_operation_id: operation_id.clone(),
                },
                origin,
            },
        )
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            let next = if error.commit_state == CommitState::NotApplied {
                "failed"
            } else {
                "unknown"
            };
            delivery
                .cas_target_status(&operation_id, &scope, "applying", next)
                .await?;
            return Err(st_err(error));
        }
    };

    if !outcome.confirmed_commit {
        delivery
            .cas_target_status(&operation_id, &scope, "applying", "unknown")
            .await?;
        return Err(AppError::conflict(
            "ST_COMMIT_STATE_UNKNOWN",
            "SillyTavern revoke commit was not confirmed",
        ));
    }
    let target_message_ids = &target.old_message_ids;
    let effect = if target_message_ids.is_empty() {
        RevokeEffectResult::default()
    } else {
        match delivery
            .execute_revoke_effect_scoped(&scope, target_message_ids, "[已撤回]")
            .await
        {
            Ok(effect) => effect,
            Err(error) => {
                delivery
                    .cas_target_status(&operation_id, &scope, "applying", "unknown")
                    .await?;
                return Err(error);
            }
        }
    };
    if !effect.failed_message_ids.is_empty() {
        delivery
            .cas_target_status(&operation_id, &scope, "applying", "unknown")
            .await?;
        return Err(AppError::conflict(
            "TELEGRAM_REVOKE_CLEANUP_UNKNOWN",
            "Telegram revoke cleanup did not reach a confirmed terminal state",
        ));
    }
    if let Err(error) = delivery
        .cas_target_status(&operation_id, &scope, "applying", "applied")
        .await
    {
        if let Err(state_error) = delivery
            .cas_target_status(&operation_id, &scope, "applying", "unknown")
            .await
        {
            return Err(AppError::internal(format!(
                "turn target applied CAS failed with {} and unknown CAS failed with {}",
                error.code, state_error.code
            )));
        }
        return Err(error);
    }
    let removed = outcome
        .removed_safe_content
        .unwrap_or_else(|| "最后一轮".into());
    let text = if target_message_ids.is_empty() {
        format!("已撤回最后一轮（SillyTavern 已回退，但无可确认 Telegram 目标）：{removed}")
    } else if effect.failed_message_ids.is_empty() {
        format!("已撤回最后一轮（SillyTavern 已回退，消息已清理）：{removed}")
    } else {
        format!(
            "已在 SillyTavern 撤回最后一轮，但 Telegram 消息清理部分失败（将只重试清理）：{removed}"
        )
    };

    Ok(vec![TelegramReply {
        text,
        markup: None,
        generation: None,
    }])
}

async fn turn_scope_for_delivery(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    delivery: &TelegramDelivery,
) -> AppResult<crate::modules::telegram::delivery::TurnScope> {
    let context = services
        .channel
        .load(&actor.account.id, context_key)
        .await?;
    let handle = context.handle.ok_or_else(|| {
        AppError::conflict(
            "TELEGRAM_TURN_SCOPE_INVALID",
            "Telegram turn handle is missing",
        )
    })?;
    let avatar = context.avatar.ok_or_else(|| {
        AppError::conflict(
            "TELEGRAM_TURN_SCOPE_INVALID",
            "Telegram turn avatar is missing",
        )
    })?;
    let chat_file = context.chat_file.ok_or_else(|| {
        AppError::conflict(
            "TELEGRAM_TURN_SCOPE_INVALID",
            "Telegram turn chat locator is missing",
        )
    })?;
    let locator_hash = crate::modules::bridge::st_ops::locator_hash(&handle, &avatar, &chat_file);
    let numeric_bot_id = delivery.numeric_bot_id()?;
    crate::modules::telegram::delivery::TurnScope::new(
        actor.account.id.clone(),
        delivery.bot_id(),
        numeric_bot_id,
        delivery.chat_id(),
        locator_hash,
    )
}

async fn redo_turn(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    client_turn_id: Option<String>,
    delivery: Option<&TelegramDelivery>,
    origin: BridgeOperationOrigin,
) -> AppResult<Vec<TelegramReply>> {
    let locator = require_selected_chat(services, actor, context_key).await?;
    let operation_id = client_turn_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            AppError::bad_request(
                "TELEGRAM_OPERATION_ID_REQUIRED",
                "redo requires a canonical operation id",
            )
        })?;
    let bridge = require_bridge(services, context_key)?;
    let scope = if let Some(delivery) = delivery {
        Some(turn_scope_for_delivery(services, actor, context_key, delivery).await?)
    } else {
        None
    };
    let claimed = bridge
        .claim_operation_with_origin(
            actor,
            &operation_id,
            crate::modules::bridge::operations::KIND_REGENERATE,
            &locator,
            origin.clone(),
        )
        .await
        .map_err(st_err)?;
    if matches!(
        claimed.record.status,
        crate::modules::bridge::operations::BridgeOperationStatus::Delivered
    ) && matches!(
        claimed.record.commit_state,
        crate::modules::bridge::operations::OperationCommitState::Applied
    ) {
        return Ok(Vec::new());
    }
    let frozen_target = if let (Some(delivery), Some(scope)) = (delivery, scope.as_ref()) {
        if let Some(target) = delivery.load_frozen_target(&operation_id, scope).await? {
            match target.status.as_str() {
                "applied" => return Ok(Vec::new()),
                "applying" | "unknown" => {
                    return Err(AppError::conflict(
                        "TELEGRAM_TURN_TARGET_IN_PROGRESS",
                        "redo target is already being reconciled",
                    ));
                }
                "failed" => {
                    return Err(AppError::conflict(
                        "TELEGRAM_TURN_TARGET_FAILED",
                        "redo target has a recorded terminal failure",
                    ));
                }
                "frozen" => target,
                _ => {
                    return Err(AppError::conflict(
                        "TELEGRAM_TURN_TARGET_INVALID",
                        "redo target has an unknown status",
                    ));
                }
            }
        } else {
            let view = bridge
                .query(
                    actor,
                    StBridgeQuery::GetLastTurn {
                        locator: locator.clone(),
                    },
                )
                .await
                .map_err(st_err)?;
            let tail_fingerprint = match view {
                StBridgeView::LastTurn {
                    tail_fingerprint: Some(fingerprint),
                    ..
                } if fingerprint.len() == 64
                    && fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit())
                    && fingerprint == fingerprint.to_ascii_lowercase() =>
                {
                    fingerprint
                }
                _ => {
                    return Err(AppError::conflict(
                        "TELEGRAM_TURN_FINGERPRINT_INVALID",
                        "redo requires a valid frozen ST tail fingerprint",
                    ));
                }
            };
            let (target_turn_id, old_message_ids, target_revision) =
                delivery.find_last_turn(scope).await?;
            let target_turn_id = target_turn_id.ok_or_else(|| {
                AppError::conflict(
                    "TELEGRAM_TURN_TARGET_MISSING",
                    "redo has no active Telegram turn target",
                )
            })?;
            delivery
                .freeze_target(
                    &operation_id,
                    scope,
                    &target_turn_id,
                    &tail_fingerprint,
                    target_revision,
                    &old_message_ids,
                )
                .await?
        }
    } else {
        return Err(AppError::service_unavailable(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "redo requires a Telegram delivery scope",
        ));
    };
    let scope = scope.ok_or_else(|| {
        AppError::service_unavailable(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "redo requires a Telegram delivery scope",
        )
    })?;
    delivery
        .ok_or_else(|| {
            AppError::service_unavailable(
                "TELEGRAM_TURN_SCOPE_REQUIRED",
                "redo delivery is unavailable",
            )
        })?
        .cas_target_status(&operation_id, &scope, "frozen", "applying")
        .await?;
    let outcome = match bridge
        .execute_enveloped(
            actor,
            StBridgeCommandEnvelope {
                command: StBridgeCommand::RegenerateReply {
                    locator,
                    client_operation_id: operation_id.clone(),
                    model_override: None,
                },
                origin,
            },
        )
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            let next = if error.commit_state == CommitState::NotApplied {
                "failed"
            } else {
                "unknown"
            };
            delivery
                .ok_or_else(|| {
                    AppError::service_unavailable(
                        "TELEGRAM_TURN_SCOPE_REQUIRED",
                        "redo delivery is unavailable",
                    )
                })?
                .cas_target_status(&operation_id, &scope, "applying", next)
                .await?;
            return Err(st_err(error));
        }
    };
    if !outcome.confirmed_commit {
        if let Some(delivery) = delivery {
            delivery
                .cas_target_status(&operation_id, &scope, "applying", "unknown")
                .await?;
        }
        return Err(AppError::conflict(
            "ST_COMMIT_STATE_UNKNOWN",
            "SillyTavern redo commit was not confirmed",
        ));
    }
    let text = outcome.reply_text.unwrap_or_default();
    let chunks = TelegramModule::split_text(&text, 3200);
    let desired = if chunks.is_empty() {
        vec!["生成结果为空".to_string()]
    } else {
        chunks
    };
    let metadata = crate::modules::telegram::delivery::TurnMetadata::new_scoped(
        scope.clone(),
        frozen_target.target_turn_id.clone(),
        operation_id.clone(),
    )?;
    let delivery = delivery.ok_or_else(|| {
        AppError::service_unavailable(
            "TELEGRAM_TURN_SCOPE_REQUIRED",
            "redo delivery is unavailable",
        )
    })?;
    let overlap = frozen_target.old_message_ids.len().min(desired.len());
    let mut target_revision = frozen_target.target_revision;
    let mut active_ids = frozen_target.old_message_ids[..overlap].to_vec();
    for (index, (&message_id, desired_text)) in frozen_target
        .old_message_ids
        .iter()
        .zip(desired.iter())
        .take(overlap)
        .enumerate()
    {
        if let Err(error) = delivery
            .assert_turn_revision(&scope, &frozen_target.target_turn_id, target_revision)
            .await
        {
            delivery
                .cas_target_status(&operation_id, &scope, "applying", "failed")
                .await?;
            return Err(error);
        }
        match delivery.edit_text(message_id, desired_text).await {
            Ok(()) => {
                if let Err(error) = delivery
                    .update_turn_chunk_scoped(
                        &scope,
                        target_revision,
                        &frozen_target.target_turn_id,
                        index,
                        message_id,
                        desired_text,
                    )
                    .await
                {
                    delivery
                        .cas_target_status(&operation_id, &scope, "applying", "unknown")
                        .await?;
                    return Err(error);
                }
                target_revision = target_revision.checked_add(1).ok_or_else(|| {
                    AppError::conflict(
                        "TELEGRAM_TURN_REVISION_INVALID",
                        "Telegram turn revision overflow",
                    )
                })?;
            }
            Err(err) if index == 0 && is_known_uneditable(&err) => {
                let replacement = match delivery
                    .replace_turn_chunk_scoped(
                        &metadata,
                        target_revision,
                        index,
                        message_id,
                        desired_text,
                    )
                    .await
                {
                    Ok(replacement) => replacement,
                    Err(error) => {
                        delivery
                            .cas_target_status(&operation_id, &scope, "applying", "unknown")
                            .await?;
                        return Err(error);
                    }
                };
                active_ids[0] = replacement;
                target_revision = target_revision.checked_add(1).ok_or_else(|| {
                    AppError::conflict(
                        "TELEGRAM_TURN_REVISION_INVALID",
                        "Telegram turn revision overflow",
                    )
                })?;
            }
            Err(err) => {
                delivery
                    .cas_target_status(&operation_id, &scope, "applying", "unknown")
                    .await?;
                return Err(err);
            }
        }
    }
    if desired.len() > frozen_target.old_message_ids.len() {
        for (index, chunk) in desired
            .iter()
            .enumerate()
            .skip(frozen_target.old_message_ids.len())
        {
            if let Err(error) = delivery
                .assert_turn_revision(&scope, &frozen_target.target_turn_id, target_revision)
                .await
            {
                delivery
                    .cas_target_status(&operation_id, &scope, "applying", "failed")
                    .await?;
                return Err(error);
            }
            let message_id = match delivery
                .send_turn_chunk_scoped(&metadata, target_revision, index, chunk)
                .await
            {
                Ok(message_id) => message_id,
                Err(error) => {
                    delivery
                        .cas_target_status(&operation_id, &scope, "applying", "unknown")
                        .await?;
                    return Err(error);
                }
            };
            active_ids.push(message_id);
            target_revision = target_revision.checked_add(1).ok_or_else(|| {
                AppError::conflict(
                    "TELEGRAM_TURN_REVISION_INVALID",
                    "Telegram turn revision overflow",
                )
            })?;
        }
    }
    if frozen_target.old_message_ids.len() > desired.len() {
        if let Err(error) = delivery
            .assert_turn_revision(&scope, &frozen_target.target_turn_id, target_revision)
            .await
        {
            delivery
                .cas_target_status(&operation_id, &scope, "applying", "failed")
                .await?;
            return Err(error);
        }
        if let Err(error) = delivery
            .execute_redo_cleanup_scoped(&scope, &frozen_target.old_message_ids, &active_ids)
            .await
        {
            delivery
                .cas_target_status(&operation_id, &scope, "applying", "unknown")
                .await?;
            return Err(error);
        }
    }
    if let Err(error) = delivery
        .cas_target_status(&operation_id, &scope, "applying", "applied")
        .await
    {
        if let Err(state_error) = delivery
            .cas_target_status(&operation_id, &scope, "applying", "unknown")
            .await
        {
            return Err(AppError::internal(format!(
                "redo target applied CAS failed with {} and unknown CAS failed with {}",
                error.code, state_error.code
            )));
        }
        return Err(error);
    }
    let coordinator = bridge.operation_coordinator().ok_or_else(|| {
        AppError::service_unavailable(
            "ST_WRITE_NOT_READY",
            "redo delivery confirmation coordinator is unavailable",
        )
    })?;
    coordinator
        .mark_delivered(&operation_id)
        .await
        .map_err(st_err)?;
    Ok(Vec::new())
}

async fn recent_conversations(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
) -> AppResult<Vec<TelegramReply>> {
    recent_panel(services, actor, context_key, 0).await
}

async fn open_recent(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    index: usize,
) -> AppResult<Vec<TelegramReply>> {
    let recent = services
        .channel
        .list_recent(&actor.account.id, PAGE_SIZE as i64)
        .await?;
    let Some(item) = recent.get(index) else {
        return Ok(vec![TelegramReply {
            text: "最近会话选择已失效，请重新发送 /recent。".into(),
            markup: None,
            generation: None,
        }]);
    };
    let Some(avatar) = item.avatar.as_deref() else {
        return Ok(vec![TelegramReply {
            text: "最近会话选择已失效，请重新发送 /recent。".into(),
            markup: None,
            generation: None,
        }]);
    };
    let Some(chat_file) = item.chat_file.as_deref() else {
        return Ok(vec![TelegramReply {
            text: "最近会话选择已失效，请重新发送 /recent。".into(),
            markup: None,
            generation: None,
        }]);
    };
    let view = require_bridge(services, context_key)?
        .query(
            actor,
            StBridgeQuery::ListCharacterChats {
                avatar: avatar.to_string(),
            },
        )
        .await
        .map_err(st_err)?;
    let StBridgeView::Chats(chats) = view else {
        return Err(st_err(StBridgeError::new(
            StErrorCode::StChatListFailed,
            StErrorStage::Catalog,
            "chat list unavailable",
            false,
            CommitState::NotStarted,
        )));
    };
    if !chats.iter().any(|chat| chat.chat_file == chat_file) {
        return Ok(vec![TelegramReply {
            text: "该最近会话在 SillyTavern 中已不存在。".into(),
            markup: None,
            generation: None,
        }]);
    }
    require_bridge(services, context_key)?
        .execute(
            actor,
            StBridgeCommand::SelectChat {
                locator: StChatLocator {
                    handle: item.handle.clone().unwrap_or_else(|| "default-user".into()),
                    avatar: avatar.to_string(),
                    character_name: item.character_name.clone().unwrap_or_default(),
                    chat_file: chat_file.to_string(),
                },
            },
        )
        .await
        .map_err(st_err)?;
    current_state(services, actor, context_key).await
}

async fn list_models(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    purpose: &str,
    page: usize,
) -> AppResult<Vec<TelegramReply>> {
    let view = require_bridge(services, context_key)?
        .query(actor, StBridgeQuery::GetModels)
        .await
        .map_err(st_err)?;
    let StBridgeView::Models { catalog, .. } = view else {
        return Err(st_err(StBridgeError::new(
            StErrorCode::StSettingsInvalid,
            StErrorStage::Catalog,
            "model catalog unavailable",
            false,
            CommitState::NotStarted,
        )));
    };
    let purpose = if purpose == "compression" || purpose == "comp" {
        ModelPurpose::Compression
    } else {
        ModelPurpose::Chat
    };
    let grouped = grouped_model_catalog(&catalog);
    let providers = grouped.keys().cloned().collect::<Vec<_>>();
    let total_pages = providers.len().div_ceil(PAGE_SIZE).max(1);
    Ok(vec![panel_reply(render_providers(
        purpose,
        &providers,
        page,
        total_pages,
    ))])
}

// Retained until the legacy indexed model callback migration is complete.
#[allow(dead_code)]
async fn select_model(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    purpose: &str,
    index: usize,
) -> AppResult<Vec<TelegramReply>> {
    let view = require_bridge(services, context_key)?
        .query(actor, StBridgeQuery::GetModels)
        .await
        .map_err(st_err)?;
    let StBridgeView::Models { catalog, .. } = view else {
        return Err(st_err(StBridgeError::new(
            StErrorCode::StSettingsInvalid,
            StErrorStage::Catalog,
            "model catalog unavailable",
            false,
            CommitState::NotStarted,
        )));
    };
    let Some(model) = catalog.models.get(index) else {
        return Ok(vec![TelegramReply {
            text: "模型选择已失效，请重新发送 /model。".into(),
            markup: None,
            generation: None,
        }]);
    };
    require_bridge(services, context_key)?
        .execute(
            actor,
            StBridgeCommand::SetModelOverride {
                kind: if purpose == "compression" {
                    ModelOverrideKind::Compression
                } else {
                    ModelOverrideKind::Chat
                },
                model_id: Some(model.id.clone()),
            },
        )
        .await
        .map_err(st_err)?;
    let kind = if purpose == "compression" {
        "压缩模型"
    } else {
        "对话模型"
    };
    Ok(vec![TelegramReply {
        text: format!("已切换{kind}：{}。后续发送将使用该模型 ID。", model.id),
        markup: None,
        generation: None,
    }])
}

async fn compress_history(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    client_turn_id: Option<String>,
    origin: BridgeOperationOrigin,
) -> AppResult<Vec<TelegramReply>> {
    let locator = require_selected_chat(services, actor, context_key).await?;
    let outcome = require_bridge(services, context_key)?
        .execute_enveloped(
            actor,
            StBridgeCommandEnvelope {
                command: StBridgeCommand::CompressChat {
                    locator,
                    client_operation_id: client_turn_id.unwrap_or_default(),
                },
                origin,
            },
        )
        .await
        .map_err(st_err)?;
    Ok(vec![TelegramReply {
        text: outcome.reply_text.unwrap_or_default(),
        markup: None,
        generation: None,
    }])
}

async fn send_chat(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
    client_turn_id: Option<String>,
    text: &str,
    _origin: BridgeOperationOrigin,
) -> AppResult<Vec<TelegramReply>> {
    require_selected_chat(services, actor, context_key).await?;
    Ok(vec![TelegramReply {
        text: String::new(),
        markup: None,
        generation: Some(GenerationIntent {
            kind: GenerationKind::Send,
            conversation_id: client_turn_id
                .clone()
                .unwrap_or_else(|| context_key.to_string()),
            context_key: context_key.to_string(),
            user_text: Some(text.to_string()),
        }),
    }])
}

async fn require_selected_character(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
) -> AppResult<StChatLocator> {
    let context = services
        .channel
        .load(&actor.account.id, context_key)
        .await?;
    let avatar = context.avatar.clone().ok_or_else(|| {
        AppError::bad_request(
            "CHARACTER_NOT_SELECTED",
            "当前还没有选择角色。请先使用 /chars。",
        )
    })?;
    Ok(StChatLocator {
        handle: context.handle.unwrap_or_else(|| "default-user".into()),
        avatar,
        character_name: context.character_name.unwrap_or_default(),
        chat_file: context.chat_file.unwrap_or_default(),
    })
}

async fn require_selected_chat(
    services: &crate::modules::telegram::TelegramServices,
    actor: &Actor,
    context_key: &str,
) -> AppResult<StChatLocator> {
    let context = services
        .channel
        .load(&actor.account.id, context_key)
        .await?;
    st_locator_from_context(&context)
}

async fn last_error_summary(
    module: &TelegramModule,
    actor: &Actor,
    bot_id: &str,
    chat_id: i64,
) -> AppResult<Vec<TelegramReply>> {
    let row = module
        .latest_visible_error(&actor.account.id, bot_id, &chat_id.to_string())
        .await?;
    let Some(row) = row else {
        return Ok(vec![TelegramReply {
            text: "当前会话没有可查看的错误记录。".into(),
            markup: None,
            generation: None,
        }]);
    };
    let parsed_code = StErrorCode::parse(&row.code).unwrap_or(StErrorCode::StConnectFailed);
    let parsed_stage = StErrorStage::parse(&row.stage).unwrap_or(StErrorStage::Control);
    let parsed_commit = CommitState::parse(&row.commit_state).unwrap_or(CommitState::NotStarted);
    let mut error = StBridgeError::new(
        parsed_code,
        parsed_stage,
        row.safe_message,
        row.retryable != 0,
        parsed_commit,
    );
    error.safe_detail = row.safe_detail;
    error.request_id = row.request_id.unwrap_or_default();
    error.trace_id = row.trace_id.unwrap_or_default();
    Ok(vec![TelegramReply {
        text: error_render::render(&error),
        markup: None,
        generation: None,
    }])
}

fn is_known_uneditable(error: &AppError) -> bool {
    let message = error.message.to_ascii_lowercase();
    message.contains("not found")
        || message.contains("cannot edit")
        || message.contains("can't be edited")
        || message.contains("message to edit")
}

fn first_command(text: &str) -> &str {
    text.split_whitespace()
        .next()
        .and_then(|token| token.split('@').next())
        .unwrap_or(text)
}
