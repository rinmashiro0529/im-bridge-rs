CREATE TABLE IF NOT EXISTS telegram_panel_slots (
    panel_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    numeric_bot_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1,
    expires_at_unix INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS telegram_panel_slots_active_chat
    ON telegram_panel_slots(numeric_bot_id, chat_id)
    WHERE active = 1;

CREATE TABLE IF NOT EXISTS telegram_panel_effects (
    effect_id TEXT PRIMARY KEY,
    panel_id TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    effect_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS telegram_panel_effects_panel_idx
    ON telegram_panel_effects(panel_id);

CREATE TABLE IF NOT EXISTS telegram_turn_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    turn_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    render_version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS telegram_turn_messages_chat_turn_idx
    ON telegram_turn_messages(chat_id, turn_id);

CREATE INDEX IF NOT EXISTS telegram_turn_messages_msg_idx
    ON telegram_turn_messages(chat_id, message_id);
