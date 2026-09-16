ALTER TABLE telegram_updates ADD COLUMN raw_update_sha256 TEXT;

ALTER TABLE channel_deliveries ADD COLUMN next_attempt_at TEXT;
ALTER TABLE channel_deliveries ADD COLUMN attempt_token TEXT;
ALTER TABLE channel_deliveries ADD COLUMN lease_until TEXT;
ALTER TABLE channel_deliveries ADD COLUMN expected_revision INTEGER;
ALTER TABLE channel_deliveries ADD COLUMN next_revision INTEGER;
ALTER TABLE channel_deliveries ADD COLUMN terminal_at TEXT;
ALTER TABLE channel_deliveries ADD COLUMN terminal_evidence_json TEXT;

ALTER TABLE telegram_panel_effects ADD COLUMN action_nonce TEXT;
ALTER TABLE telegram_panel_effects ADD COLUMN next_attempt_at TEXT;
ALTER TABLE telegram_panel_effects ADD COLUMN attempt_token TEXT;
ALTER TABLE telegram_panel_effects ADD COLUMN lease_until TEXT;
ALTER TABLE telegram_panel_effects ADD COLUMN expected_revision INTEGER;
ALTER TABLE telegram_panel_effects ADD COLUMN next_revision INTEGER;
ALTER TABLE telegram_panel_effects ADD COLUMN terminal_at TEXT;
ALTER TABLE telegram_panel_effects ADD COLUMN terminal_evidence_json TEXT;

CREATE UNIQUE INDEX telegram_panel_effects_action_nonce_idx
    ON telegram_panel_effects(action_nonce)
    WHERE action_nonce IS NOT NULL;

ALTER TABLE telegram_turn_messages ADD COLUMN account_id TEXT;
ALTER TABLE telegram_turn_messages ADD COLUMN internal_bot_id TEXT;
ALTER TABLE telegram_turn_messages ADD COLUMN numeric_bot_id INTEGER;
ALTER TABLE telegram_turn_messages ADD COLUMN locator_hash TEXT;
ALTER TABLE telegram_turn_messages ADD COLUMN lifecycle TEXT;
ALTER TABLE telegram_turn_messages ADD COLUMN terminal_at TEXT;

CREATE UNIQUE INDEX telegram_turn_messages_complete_scope_idx
    ON telegram_turn_messages(
        account_id,
        internal_bot_id,
        numeric_bot_id,
        chat_id,
        locator_hash,
        turn_id,
        chunk_index
    )
    WHERE account_id IS NOT NULL
      AND internal_bot_id IS NOT NULL
      AND numeric_bot_id IS NOT NULL
      AND locator_hash IS NOT NULL;

CREATE TABLE telegram_turn_targets (
    operation_id TEXT PRIMARY KEY REFERENCES bridge_operations(id) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    internal_bot_id TEXT NOT NULL,
    numeric_bot_id INTEGER NOT NULL CHECK (numeric_bot_id > 0),
    chat_id INTEGER NOT NULL,
    locator_hash TEXT NOT NULL,
    target_turn_id TEXT NOT NULL,
    scope TEXT NOT NULL,
    tail_fingerprint TEXT NOT NULL,
    target_revision INTEGER NOT NULL CHECK (target_revision >= 0),
    old_message_ids TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('frozen', 'applying', 'applied', 'failed', 'unknown')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX telegram_turn_targets_scope_idx
    ON telegram_turn_targets(
        account_id,
        internal_bot_id,
        numeric_bot_id,
        chat_id,
        locator_hash,
        target_turn_id
    );
