CREATE TABLE channel_command_results (
    operation_id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    external_context_key TEXT NOT NULL,
    operation_kind TEXT NOT NULL,
    client_turn_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL,
    result_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE (actor_id, channel, external_context_key, operation_kind, client_turn_id)
);

CREATE INDEX channel_command_results_context_idx
    ON channel_command_results(actor_id, channel, external_context_key, created_at);
