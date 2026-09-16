ALTER TABLE channel_contexts ADD COLUMN st_handle TEXT;
ALTER TABLE channel_contexts ADD COLUMN st_character_avatar TEXT;
ALTER TABLE channel_contexts ADD COLUMN st_character_name TEXT;
ALTER TABLE channel_contexts ADD COLUMN st_chat_file TEXT;
ALTER TABLE channel_contexts ADD COLUMN chat_model_id TEXT;
ALTER TABLE channel_contexts ADD COLUMN compression_model_id TEXT;

CREATE TABLE bridge_operations (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES accounts(id),
    bot_id TEXT NOT NULL REFERENCES telegram_bots(id),
    telegram_update_id INTEGER NOT NULL,
    channel_context_key TEXT NOT NULL,
    operation_kind TEXT NOT NULL,
    st_handle TEXT,
    st_character_avatar TEXT,
    st_chat_file TEXT,
    status TEXT NOT NULL CHECK (status IN (
        'received', 'snapshot_ready', 'generating', 'generated',
        'committing', 'committed', 'delivered',
        'conflict', 'failed', 'interrupted'
    )),
    source_sha256 TEXT,
    source_integrity TEXT,
    source_size INTEGER,
    message_count INTEGER,
    request_meta_json TEXT NOT NULL DEFAULT '{}',
    operation_payload_ciphertext BLOB,
    operation_payload_nonce BLOB,
    operation_payload_key_version INTEGER,
    mutation_digest TEXT,
    connector_result_json TEXT,
    error_stage TEXT,
    error_code TEXT,
    error_summary TEXT,
    upstream_http_status INTEGER,
    upstream_code TEXT,
    retryable INTEGER NOT NULL DEFAULT 0 CHECK (retryable IN (0, 1)),
    commit_state TEXT NOT NULL DEFAULT 'not_started'
        CHECK (commit_state IN ('not_started', 'not_applied', 'applied', 'unknown')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    request_id TEXT,
    trace_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (bot_id, telegram_update_id, operation_kind)
);

CREATE INDEX bridge_operations_trace_idx ON bridge_operations(trace_id);
CREATE INDEX bridge_operations_created_idx ON bridge_operations(created_at);
CREATE INDEX bridge_operations_actor_bot_context_idx
    ON bridge_operations(actor_id, bot_id, channel_context_key, created_at);

CREATE TABLE bridge_error_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT REFERENCES bridge_operations(id),
    actor_id TEXT,
    bot_id TEXT,
    chat_id TEXT,
    telegram_update_id INTEGER,
    stage TEXT NOT NULL CHECK (stage IN (
        'control', 'connect', 'session', 'catalog', 'snapshot',
        'prompt', 'generation', 'commit', 'delivery'
    )),
    code TEXT NOT NULL,
    safe_message TEXT NOT NULL,
    safe_detail TEXT,
    upstream_http_status INTEGER,
    upstream_code TEXT,
    endpoint_class TEXT,
    retryable INTEGER NOT NULL CHECK (retryable IN (0, 1)),
    commit_state TEXT NOT NULL CHECK (commit_state IN (
        'not_started', 'not_applied', 'applied', 'unknown'
    )),
    attempt INTEGER NOT NULL,
    duration_ms INTEGER,
    request_id TEXT,
    trace_id TEXT,
    body_sha256 TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX bridge_error_events_operation_idx ON bridge_error_events(operation_id);
CREATE INDEX bridge_error_events_trace_idx ON bridge_error_events(trace_id);
CREATE INDEX bridge_error_events_created_idx ON bridge_error_events(created_at);
CREATE INDEX bridge_error_events_actor_bot_chat_idx
    ON bridge_error_events(actor_id, bot_id, chat_id, created_at);

CREATE TRIGGER bridge_error_events_no_update
BEFORE UPDATE ON bridge_error_events
BEGIN
    SELECT RAISE(ABORT, 'bridge_error_events are append-only');
END;

CREATE TRIGGER bridge_error_events_no_delete
BEFORE DELETE ON bridge_error_events
BEGIN
    SELECT RAISE(ABORT, 'bridge_error_events are append-only');
END;
