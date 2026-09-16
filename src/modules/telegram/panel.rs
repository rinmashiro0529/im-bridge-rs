use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use crate::domain::channel::StChannelLocator;
use crate::domain::st::{StCharacterSummary, StChatSummary, StModelSummary};
use crate::error::{AppError, AppResult};

/// Compatibility aliases for the names used by the panel specification.
pub type StChatFileSummary = StChatSummary;
pub type ModelInfo = StModelSummary;

/// The model catalog can be used for two independent purposes.  Keeping the
/// purpose in the panel token prevents a compression selection from changing
/// the chat model (and vice versa).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelPurpose {
    Chat,
    Compression,
}

impl ModelPurpose {
    pub fn short_name(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Compression => "comp",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Chat => "聊天",
            Self::Compression => "压缩",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PanelKind {
    Home,
    Help,
    Settings,
    Status,
    Characters {
        page: usize,
    },
    History {
        page: usize,
    },
    Recent {
        page: usize,
    },
    CurrentState,
    Providers {
        purpose: ModelPurpose,
        page: usize,
    },
    ProviderModels {
        purpose: ModelPurpose,
        provider_key: String,
        page: usize,
    },
    OperationProgress {
        operation_id: String,
    },
}

impl PanelKind {
    pub fn breadcrumb(&self) -> String {
        match self {
            Self::Home => "首页".into(),
            Self::Help => "首页 > 帮助".into(),
            Self::Settings => "首页 > 设置".into(),
            Self::Status => "首页 > 状态".into(),
            Self::Characters { .. } => "首页 > 角色列表".into(),
            Self::History { .. } => "首页 > 历史会话".into(),
            Self::Recent { .. } => "首页 > 最近会话".into(),
            Self::CurrentState => "首页 > 当前会话".into(),
            Self::Providers { purpose, .. } => {
                format!("首页 > {}模型 > 厂商选择", purpose.display_name())
            }
            Self::ProviderModels {
                purpose,
                provider_key,
                ..
            } => {
                format!(
                    "首页 > {}模型 > {}",
                    purpose.display_name(),
                    redact_label(provider_key)
                )
            }
            Self::OperationProgress { .. } => "首页 > 操作进度".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PanelParseMode {
    PlainText,
    MarkdownV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InlineButton {
    pub label: String,
    pub callback_data: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelegramPanel {
    pub kind: PanelKind,
    pub revision: u64,
    pub catalog_revision: Option<String>,
    pub text: String,
    pub keyboard: Vec<Vec<InlineButton>>,
    pub parse_mode: PanelParseMode,
}

fn panel_registry() -> &'static Mutex<HashMap<String, TelegramPanel>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, TelegramPanel>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn panel_key(text: &str, keyboard: &[Vec<InlineButton>]) -> String {
    format!(
        "{}:{}",
        text,
        serde_json::to_string(keyboard).unwrap_or_default()
    )
}

/// Retain a rendered panel long enough for the delivery worker to perform the
/// corresponding in-place edit. The panel text is not used as authorization;
/// the active slot and revision are checked by PanelStore before the edit.
pub(crate) fn remember_panel(panel: &TelegramPanel) {
    if let Ok(mut registry) = panel_registry().lock() {
        if registry.len() >= 256 {
            if let Some(key) = registry.keys().next().cloned() {
                registry.remove(&key);
            }
        }
        registry.insert(panel_key(&panel.text, &panel.keyboard), panel.clone());
    }
}

pub(crate) fn lookup_panel(text: &str, keyboard: &[Vec<InlineButton>]) -> Option<TelegramPanel> {
    panel_registry()
        .lock()
        .ok()
        .and_then(|registry| registry.get(&panel_key(text, keyboard)).cloned())
}

pub fn keyboard_from_markup(markup: &serde_json::Value) -> AppResult<Vec<Vec<InlineButton>>> {
    let rows = markup
        .get("inline_keyboard")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            AppError::bad_request("TELEGRAM_PANEL_MARKUP_INVALID", "Telegram 面板键盘格式无效")
        })?;
    rows.iter()
        .map(|row| {
            let buttons = row.as_array().ok_or_else(|| {
                AppError::bad_request(
                    "TELEGRAM_PANEL_MARKUP_INVALID",
                    "Telegram 面板按钮行格式无效",
                )
            })?;
            buttons
                .iter()
                .map(|button| {
                    let object = button.as_object().ok_or_else(|| {
                        AppError::bad_request(
                            "TELEGRAM_PANEL_MARKUP_INVALID",
                            "Telegram 面板按钮格式无效",
                        )
                    })?;
                    let label = object
                        .get("text")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            AppError::bad_request(
                                "TELEGRAM_PANEL_MARKUP_INVALID",
                                "Telegram 面板按钮缺少文本",
                            )
                        })?;
                    let callback_data = object
                        .get("callback_data")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            AppError::bad_request(
                                "TELEGRAM_PANEL_MARKUP_INVALID",
                                "Telegram 面板按钮缺少回调令牌",
                            )
                        })?;
                    Ok(InlineButton {
                        label: label.to_string(),
                        callback_data: callback_data.to_string(),
                    })
                })
                .collect()
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PanelSlot {
    pub panel_id: String,
    pub account_id: String,
    pub numeric_bot_id: i64,
    pub chat_id: i64,
    pub message_id: i64,
    pub kind: PanelKind,
    pub revision: u64,
    pub active: bool,
    pub expires_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PanelEffect {
    Create {
        panel_id: String,
        chat_id: i64,
        panel: TelegramPanel,
    },
    Replace {
        slot: PanelSlot,
        expected_revision: u64,
        panel: TelegramPanel,
    },
    Toast {
        callback_query_id: String,
        text: String,
        show_alert: bool,
    },
    RetireKeyboard {
        slot: PanelSlot,
    },
    DeleteChunk {
        chat_id: i64,
        message_id: i64,
        operation_id: String,
    },
    FallbackSend {
        old_slot: PanelSlot,
        panel: TelegramPanel,
        effect_id: String,
    },
}

/// Plan a panel mutation without performing I/O.  The caller can persist the
/// returned slot only after the corresponding effect succeeds.
pub fn plan_effect(
    current_slot: Option<&PanelSlot>,
    next_panel: TelegramPanel,
    panel_id: String,
    account_id: String,
    numeric_bot_id: i64,
    chat_id: i64,
    ttl_secs: i64,
) -> (PanelEffect, PanelSlot) {
    let expires_at_unix = time::OffsetDateTime::now_utc().unix_timestamp() + ttl_secs.max(0);
    if let Some(slot) = current_slot.filter(|slot| slot.active) {
        let next_revision = slot.revision.saturating_add(1);
        let mut next_panel = next_panel;
        next_panel.revision = next_revision;
        let updated = PanelSlot {
            panel_id: slot.panel_id.clone(),
            account_id: slot.account_id.clone(),
            numeric_bot_id: slot.numeric_bot_id,
            chat_id: slot.chat_id,
            message_id: slot.message_id,
            kind: next_panel.kind.clone(),
            revision: slot.revision.saturating_add(1),
            active: true,
            expires_at_unix,
        };
        (
            PanelEffect::Replace {
                slot: slot.clone(),
                expected_revision: slot.revision,
                panel: next_panel,
            },
            updated,
        )
    } else {
        let slot = PanelSlot {
            panel_id: panel_id.clone(),
            account_id,
            numeric_bot_id,
            chat_id,
            message_id: 0,
            kind: next_panel.kind.clone(),
            revision: 0,
            active: true,
            expires_at_unix,
        };
        (
            PanelEffect::Create {
                panel_id,
                chat_id,
                panel: next_panel,
            },
            slot,
        )
    }
}

fn is_protected_text(value: &str) -> bool {
    let lowered = value.to_ascii_lowercase();
    lowered.contains("http://")
        || lowered.contains("https://")
        || lowered.contains("bearer ")
        || lowered.contains("token=")
        || lowered.contains("api_key=")
        || lowered.contains("secret")
}

fn redact_label(value: &str) -> String {
    let value = value.trim();
    if is_protected_text(value) {
        return "受保护对象".into();
    }
    value.chars().take(96).collect()
}

pub fn split_current_preview(value: &str) -> Vec<String> {
    let value = value.trim();
    if value.is_empty() {
        return Vec::new();
    }
    if is_protected_text(value) {
        return vec!["受保护对象".into()];
    }
    crate::modules::telegram::TelegramModule::split_text(value, 3200)
}

fn panel(kind: PanelKind, text: String, keyboard: Vec<Vec<InlineButton>>) -> TelegramPanel {
    TelegramPanel {
        kind,
        revision: 0,
        catalog_revision: None,
        text,
        keyboard,
        parse_mode: PanelParseMode::PlainText,
    }
}

fn button(label: impl Into<String>, callback_data: impl Into<String>) -> InlineButton {
    let callback_data = crate::modules::telegram::callback::opaque_callback(&callback_data.into());
    InlineButton {
        label: label.into(),
        callback_data,
    }
}

fn home_button() -> Vec<InlineButton> {
    vec![button("返回首页", "cb:home")]
}

fn provider_models_callback(purpose: ModelPurpose, provider_key: &str, page: usize) -> String {
    crate::modules::telegram::callback::CallbackAction::ProviderModels {
        purpose,
        provider_key: provider_key.to_string(),
        page,
    }
    .to_callback_data()
}

fn select_model_callback(purpose: ModelPurpose, provider_key: &str, model_id: &str) -> String {
    crate::modules::telegram::callback::CallbackAction::SelectModelById {
        purpose,
        provider_key: provider_key.to_string(),
        model_id: model_id.to_string(),
    }
    .to_callback_data()
}

fn page_buttons(
    page: usize,
    total_pages: usize,
    previous_token: impl Fn(usize) -> String,
    next_token: impl Fn(usize) -> String,
) -> Vec<InlineButton> {
    let mut row = Vec::new();
    if page > 0 {
        row.push(button("上一页", previous_token(page - 1)));
    }
    if page + 1 < total_pages {
        row.push(button("下一页", next_token(page + 1)));
    }
    row
}

fn total_pages(total: usize, page_size: usize) -> usize {
    total.div_ceil(page_size).max(1)
}

pub fn render_home() -> TelegramPanel {
    panel(
        PanelKind::Home,
        "首页\n请选择一个功能：".into(),
        vec![
            vec![button("角色", "cb:chars:0"), button("历史", "cb:hist:0")],
            vec![button("最近", "cb:recent:0"), button("当前", "cb:now")],
            vec![button("设置", "cb:settings"), button("状态", "cb:status")],
            vec![button("帮助", "cb:help")],
        ],
    )
}

pub fn render_help() -> TelegramPanel {
    let text = [
        "首页 > 帮助",
        "",
        "可用命令：",
        "/start - 打开首页",
        "/help - 查看帮助",
        "/chars - 选择角色",
        "/hist - 查看历史会话",
        "/recent - 查看最近会话",
        "/now - 查看当前会话",
        "/model - 切换聊天模型",
        "/cmodel - 切换压缩模型",
        "/settings - 查看模型设置",
        "/status - 查看连接状态",
        "/redo - 重新生成回复",
        "/undo - 撤回最后一轮（保留消息）",
        "/revoke - 撤回并清理消息",
        "/compress - 压缩当前会话",
        "/error - 查看最近错误",
    ]
    .join("\n");
    panel(PanelKind::Help, text, vec![home_button()])
}

pub fn render_settings(chat_override: Option<&str>, comp_override: Option<&str>) -> TelegramPanel {
    let chat = chat_override
        .map(redact_label)
        .unwrap_or_else(|| "默认".into());
    let comp = comp_override
        .map(redact_label)
        .unwrap_or_else(|| "默认".into());
    let text = format!("首页 > 设置\n\n聊天模型覆盖：{chat}\n压缩模型覆盖：{comp}");
    panel(
        PanelKind::Settings,
        text,
        vec![
            vec![button("聊天模型", "cb:prov:chat:0")],
            vec![button("压缩模型", "cb:prov:comp:0")],
            home_button(),
        ],
    )
}

pub fn render_status(
    readiness: &str,
    locator_summary: &str,
    numeric_bot_id: i64,
    last_error: Option<&str>,
) -> TelegramPanel {
    let safe_locator = redact_label(locator_summary);
    let mut text = format!(
        "首页 > 状态\n\n就绪状态：{readiness}\nBot numeric ID：{numeric_bot_id}\n定位器：{safe_locator}"
    );
    if let Some(error) = last_error.filter(|value| !value.trim().is_empty()) {
        text.push_str(&format!("\n最近错误：{}", redact_label(error)));
    }
    panel(
        PanelKind::Status,
        text,
        vec![vec![button("刷新", "cb:status")], home_button()],
    )
}

pub fn render_characters(chars: &[StCharacterSummary], page: usize, total: usize) -> TelegramPanel {
    const PAGE_SIZE: usize = 8;
    let pages = total_pages(total.max(chars.len()), PAGE_SIZE);
    let page = page.min(pages - 1);
    let mut text = format!(
        "首页 > 角色列表\n\n请选择角色（第 {} / {} 页）：",
        page + 1,
        pages
    );
    if chars.is_empty() {
        text.push_str("\n\n当前没有可见角色。请刷新后重试。");
    }
    let mut rows = Vec::new();
    let mut current = Vec::new();
    for (index, character) in chars.iter().enumerate() {
        let safe_name = redact_label(&character.name);
        text.push_str(&format!("\n{}. {safe_name}", index + 1));
        current.push(button(
            format!("{} {safe_name}", index + 1),
            format!("cb:sel_char:{page}:{index}"),
        ));
        if current.len() == 2 {
            rows.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        rows.push(current);
    }
    let nav = page_buttons(
        page,
        pages,
        |value| format!("cb:chars:{value}"),
        |value| format!("cb:chars:{value}"),
    );
    if !nav.is_empty() {
        rows.push(nav);
    }
    rows.push(home_button());
    panel(PanelKind::Characters { page }, text, rows)
}

pub fn render_history(
    character_name: &str,
    chats: &[StChatFileSummary],
    page: usize,
    total: usize,
) -> TelegramPanel {
    const PAGE_SIZE: usize = 8;
    let pages = total_pages(total.max(chats.len()), PAGE_SIZE);
    let page = page.min(pages - 1);
    let safe_character_name = redact_label(character_name);
    let mut text = format!(
        "首页 > 历史会话\n角色：{safe_character_name}\n\n请选择会话（第 {} / {} 页）：",
        page + 1,
        pages
    );
    if chats.is_empty() {
        text.push_str("\n\n当前角色还没有历史会话。发送 /new 新建会话。");
    }
    let mut rows = Vec::new();
    for (index, chat) in chats.iter().enumerate() {
        let title = chat.title.as_deref().unwrap_or(&chat.chat_file);
        let safe_title = redact_label(title);
        text.push_str(&format!("\n{}. {safe_title}", index + 1));
        rows.push(vec![button(
            safe_title,
            format!("cb:sel_hist:{page}:{index}"),
        )]);
    }
    let nav = page_buttons(
        page,
        pages,
        |value| format!("cb:hist:{value}"),
        |value| format!("cb:hist:{value}"),
    );
    if !nav.is_empty() {
        rows.push(nav);
    }
    rows.push(home_button());
    panel(PanelKind::History { page }, text, rows)
}

pub fn render_recent(items: &[StChannelLocator], page: usize) -> TelegramPanel {
    const PAGE_SIZE: usize = 8;
    let pages = total_pages(items.len(), PAGE_SIZE);
    let page = page.min(pages - 1);
    let start = page * PAGE_SIZE;
    let visible = items.iter().skip(start).take(PAGE_SIZE);
    let mut text = format!(
        "首页 > 最近会话\n\n最近会话（第 {} / {} 页）：",
        page + 1,
        pages
    );
    if items.is_empty() {
        text.push_str("\n\n还没有最近会话。");
    }
    let mut rows = Vec::new();
    for (index, item) in visible.enumerate() {
        let label = item
            .chat_file
            .as_deref()
            .or(item.character_name.as_deref())
            .map(redact_label)
            .unwrap_or_else(|| "未命名会话".into());
        text.push_str(&format!("\n{}. {label}", start + index + 1));
        rows.push(vec![button(label, format!("cb:sel_recent:{page}:{index}"))]);
    }
    let nav = page_buttons(
        page,
        pages,
        |value| format!("cb:recent:{value}"),
        |value| format!("cb:recent:{value}"),
    );
    if !nav.is_empty() {
        rows.push(nav);
    }
    rows.push(home_button());
    panel(PanelKind::Recent { page }, text, rows)
}

pub fn render_current(
    character_name: &str,
    chat_file: &str,
    message_count: Option<usize>,
    preview: Option<&str>,
) -> TelegramPanel {
    let count = message_count
        .map(|value| value.to_string())
        .unwrap_or_else(|| "未知".into());
    let safe_character_name = redact_label(character_name);
    let safe_chat_file = redact_label(chat_file);
    let safe_preview = preview
        .filter(|value| !value.trim().is_empty())
        .map(redact_label)
        .unwrap_or_else(|| "暂无消息".into());
    let text = format!(
        "首页 > 当前会话\n\n角色：{safe_character_name}\n会话：{safe_chat_file}\n消息数：{count}\n\n最近内容：\n{safe_preview}"
    );
    panel(
        PanelKind::CurrentState,
        text,
        vec![
            vec![button("刷新", "cb:now")],
            vec![button("历史", "cb:hist:0")],
            home_button(),
        ],
    )
}

pub fn render_providers(
    purpose: ModelPurpose,
    providers: &[String],
    page: usize,
    total_pages: usize,
) -> TelegramPanel {
    const PAGE_SIZE: usize = 8;
    let pages = total_pages.max(1);
    let page = page.min(pages - 1);
    let start = page * PAGE_SIZE;
    let visible = providers.iter().skip(start).take(PAGE_SIZE);
    let mut text = format!(
        "首页 > {}模型 > 厂商选择\n\n请选择厂商（第 {} / {} 页）：",
        purpose.display_name(),
        page + 1,
        pages
    );
    if providers.is_empty() {
        text.push_str("\n\n当前没有可用模型厂商。请刷新后重试。");
    }
    let mut rows = Vec::new();
    let mut current = Vec::new();
    for (index, provider) in visible.enumerate() {
        let absolute = start + index;
        let safe_provider = redact_label(provider);
        text.push_str(&format!("\n{}. {safe_provider}", absolute + 1));
        current.push(button(
            safe_provider,
            crate::modules::telegram::callback::CallbackAction::ProviderModels {
                purpose,
                provider_key: provider.to_string(),
                page,
            }
            .to_callback_data(),
        ));
        if current.len() == 2 {
            rows.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        rows.push(current);
    }
    rows.push(vec![button(
        "重置模型",
        format!("cb:rst_mdl:{}", purpose.short_name()),
    )]);
    let nav = page_buttons(
        page,
        pages,
        |value| format!("cb:prov:{}:{value}", purpose.short_name()),
        |value| format!("cb:prov:{}:{value}", purpose.short_name()),
    );
    if !nav.is_empty() {
        rows.push(nav);
    }
    rows.push(home_button());
    panel(PanelKind::Providers { purpose, page }, text, rows)
}

pub fn render_provider_models(
    purpose: ModelPurpose,
    provider_key: &str,
    models: &[ModelInfo],
    current_override: Option<&str>,
    page: usize,
    total_pages: usize,
) -> TelegramPanel {
    const PAGE_SIZE: usize = 8;
    let pages = total_pages.max(1);
    let page = page.min(pages - 1);
    let start = page * PAGE_SIZE;
    let visible = models.iter().skip(start).take(PAGE_SIZE);
    let safe_provider_key = redact_label(provider_key);
    let mut text = format!(
        "首页 > {}模型 > {safe_provider_key}\n\n请选择模型（第 {} / {} 页）：",
        purpose.display_name(),
        page + 1,
        pages
    );
    if models.is_empty() {
        text.push_str("\n\n该厂商当前没有可用模型。请返回厂商列表。");
    }
    let mut rows = Vec::new();
    for (index, model) in visible.enumerate() {
        let absolute = start + index;
        let selected = current_override == Some(model.id.as_str());
        let marker = if selected { " [当前]" } else { "" };
        let safe_model_id = redact_label(&model.id);
        text.push_str(&format!("\n{}. {}{marker}", absolute + 1, safe_model_id));
        rows.push(vec![button(
            format!("{}{}", safe_model_id, marker),
            select_model_callback(purpose, provider_key, &model.id),
        )]);
    }
    let nav = page_buttons(
        page,
        pages,
        |value| provider_models_callback(purpose, provider_key, value),
        |value| provider_models_callback(purpose, provider_key, value),
    );
    if !nav.is_empty() {
        rows.push(nav);
    }
    rows.push(vec![button(
        "返回厂商",
        format!("cb:prov:{}:0", purpose.short_name()),
    )]);
    rows.push(home_button());
    panel(
        PanelKind::ProviderModels {
            purpose,
            provider_key: provider_key.into(),
            page,
        },
        text,
        rows,
    )
}
