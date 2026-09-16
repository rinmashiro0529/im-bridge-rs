PRAGMA foreign_keys = ON;

CREATE TABLE accounts (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    is_system_admin INTEGER NOT NULL DEFAULT 0,
    disabled_at TEXT,
    legacy_st_handle TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE workspaces (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES accounts(id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE workspace_members (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'member')),
    created_at TEXT NOT NULL,
    PRIMARY KEY (workspace_id, account_id)
);

CREATE TABLE api_tokens (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    token_fingerprint TEXT NOT NULL,
    label TEXT NOT NULL,
    scopes_json TEXT NOT NULL,
    expires_at TEXT,
    revoked_at TEXT,
    last_used_at TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE secrets (
    id TEXT PRIMARY KEY,
    owner_scope TEXT NOT NULL,
    kind TEXT NOT NULL,
    key_version INTEGER NOT NULL,
    nonce BLOB NOT NULL,
    ciphertext BLOB NOT NULL,
    fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE assets (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    sha256 TEXT NOT NULL,
    mime TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    relative_path TEXT NOT NULL,
    original_filename TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (workspace_id, sha256)
);

CREATE TABLE characters (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    display_name TEXT NOT NULL,
    current_revision_id TEXT,
    archived INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE character_revisions (
    id TEXT PRIMARY KEY,
    character_id TEXT NOT NULL REFERENCES characters(id) ON DELETE CASCADE,
    source_format TEXT NOT NULL,
    spec TEXT,
    spec_version TEXT,
    normalized_fields_json TEXT NOT NULL,
    raw_card_json TEXT NOT NULL,
    raw_asset_id TEXT REFERENCES assets(id),
    checksum TEXT NOT NULL,
    compatibility_warnings_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL
);

CREATE INDEX character_revisions_character_idx ON character_revisions(character_id, created_at);

CREATE TABLE workspace_settings (
    workspace_id TEXT PRIMARY KEY REFERENCES workspaces(id) ON DELETE CASCADE,
    prompt_user_name TEXT NOT NULL DEFAULT 'User',
    default_prompt_profile TEXT NOT NULL DEFAULT 'legacy_bridge_v1',
    default_chat_model_preset_id TEXT,
    default_compression_model_preset_id TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE provider_profiles (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    base_url TEXT NOT NULL,
    api_key_secret_id TEXT REFERENCES secrets(id),
    custom_headers_secret_id TEXT REFERENCES secrets(id),
    custom_prompt_post_processing TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    model_cache_json TEXT,
    model_cache_updated_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE model_presets (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    provider_profile_id TEXT NOT NULL REFERENCES provider_profiles(id) ON DELETE CASCADE,
    model_id TEXT NOT NULL,
    label TEXT NOT NULL,
    temperature REAL NOT NULL DEFAULT 1,
    top_p REAL NOT NULL DEFAULT 1,
    max_tokens INTEGER NOT NULL DEFAULT 1024,
    hard_timeout_ms INTEGER NOT NULL DEFAULT 900000,
    idle_timeout_ms INTEGER NOT NULL DEFAULT 90000,
    purpose TEXT NOT NULL CHECK (purpose IN ('chat', 'compression')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE conversations (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    character_id TEXT NOT NULL REFERENCES characters(id),
    pinned_character_revision_id TEXT REFERENCES character_revisions(id),
    title TEXT NOT NULL,
    prompt_profile TEXT NOT NULL DEFAULT 'legacy_bridge_v1',
    revision INTEGER NOT NULL DEFAULT 0,
    default_model_preset_id TEXT REFERENCES model_presets(id),
    archived INTEGER NOT NULL DEFAULT 0,
    legacy_locator TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX conversations_workspace_idx ON conversations(workspace_id, updated_at);

CREATE TABLE turns (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'complete', 'failed', 'revoked')),
    created_by TEXT NOT NULL,
    active_assistant_message_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (conversation_id, ordinal)
);

CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    turn_id TEXT REFERENCES turns(id),
    sequence INTEGER NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('system', 'user', 'assistant')),
    content_original TEXT NOT NULL,
    prompt_content TEXT,
    status TEXT NOT NULL CHECK (status IN ('active', 'revoked', 'superseded')),
    supersedes_id TEXT REFERENCES messages(id),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    source_raw_json TEXT,
    source_kind TEXT,
    source_checksum TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (conversation_id, sequence)
);

CREATE INDEX messages_conversation_status_idx ON messages(conversation_id, status, sequence);

CREATE TABLE legacy_chat_archives (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    source_handle TEXT,
    source_path TEXT,
    source_checksum TEXT NOT NULL,
    raw_header_json TEXT,
    raw_jsonl_asset_id TEXT REFERENCES assets(id),
    classification TEXT NOT NULL CHECK (classification IN ('primary', 'pre-compress-backup', 'other')),
    imported_at TEXT NOT NULL
);

CREATE TABLE generation_runs (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    turn_id TEXT REFERENCES turns(id),
    actor_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    external_context_key TEXT NOT NULL,
    operation_kind TEXT NOT NULL,
    client_turn_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('started', 'streaming', 'completed', 'failed', 'interrupted')),
    effective_character_revision_id TEXT,
    provider_snapshot_json TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    usage_json TEXT,
    partial_content TEXT,
    request_id TEXT,
    trace_id TEXT,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX generation_runs_idempotency
    ON generation_runs(actor_id, channel, external_context_key, operation_kind, client_turn_id)
    WHERE client_turn_id IS NOT NULL;

CREATE INDEX generation_runs_conversation_idx ON generation_runs(conversation_id, created_at);

CREATE TABLE compression_runs (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    model_snapshot_json TEXT NOT NULL,
    selected_message_ids_json TEXT NOT NULL,
    before_prompt_size INTEGER,
    after_prompt_size INTEGER,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE channel_contexts (
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    external_context_key TEXT NOT NULL,
    workspace_id TEXT REFERENCES workspaces(id),
    character_id TEXT REFERENCES characters(id),
    conversation_id TEXT REFERENCES conversations(id),
    chat_model_preset_id TEXT REFERENCES model_presets(id),
    compression_model_preset_id TEXT REFERENCES model_presets(id),
    updated_at TEXT NOT NULL,
    PRIMARY KEY (account_id, channel, external_context_key)
);

CREATE TABLE telegram_bots (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    owner_account_id TEXT NOT NULL REFERENCES accounts(id),
    token_secret_id TEXT REFERENCES secrets(id),
    desired_enabled INTEGER NOT NULL DEFAULT 0,
    observed_username TEXT,
    last_error TEXT,
    inter_message_delay_ms INTEGER NOT NULL DEFAULT 1400,
    stream_min_interval_ms INTEGER NOT NULL DEFAULT 5000,
    stream_min_delta_chars INTEGER NOT NULL DEFAULT 700,
    stream_first_render_chars INTEGER NOT NULL DEFAULT 300,
    stream_chunk_size INTEGER NOT NULL DEFAULT 3200,
    degraded_mode INTEGER NOT NULL DEFAULT 0,
    advanced_config_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE external_identities (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    verified_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (channel, external_user_id)
);

CREATE TABLE telegram_bindings (
    id TEXT PRIMARY KEY,
    bot_id TEXT NOT NULL REFERENCES telegram_bots(id) ON DELETE CASCADE,
    external_identity_id TEXT NOT NULL REFERENCES external_identities(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    bound_at TEXT NOT NULL,
    revoked_at TEXT
);

CREATE UNIQUE INDEX telegram_bindings_active
    ON telegram_bindings(bot_id, external_identity_id)
    WHERE revoked_at IS NULL;

CREATE TABLE bind_codes (
    id TEXT PRIMARY KEY,
    bot_id TEXT NOT NULL REFERENCES telegram_bots(id) ON DELETE CASCADE,
    target_account_id TEXT NOT NULL REFERENCES accounts(id),
    code_hmac TEXT NOT NULL,
    encrypted_code_secret_id TEXT REFERENCES secrets(id),
    expires_at TEXT NOT NULL,
    consumed_at TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE bind_attempts (
    account_id TEXT NOT NULL,
    telegram_user_id TEXT NOT NULL,
    failures INTEGER NOT NULL DEFAULT 0,
    window_start TEXT NOT NULL,
    locked_until TEXT,
    PRIMARY KEY (account_id, telegram_user_id)
);

CREATE TABLE telegram_updates (
    bot_id TEXT NOT NULL REFERENCES telegram_bots(id) ON DELETE CASCADE,
    update_id INTEGER NOT NULL,
    raw_update_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('received', 'processing', 'processed', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    lease_until TEXT,
    next_retry_at TEXT,
    error_summary TEXT,
    received_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (bot_id, update_id)
);

CREATE TABLE telegram_bot_offsets (
    bot_id TEXT PRIMARY KEY REFERENCES telegram_bots(id) ON DELETE CASCADE,
    next_offset INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE channel_deliveries (
    id TEXT PRIMARY KEY,
    bot_id TEXT NOT NULL REFERENCES telegram_bots(id) ON DELETE CASCADE,
    chat_id TEXT NOT NULL,
    external_message_id TEXT,
    message_kind TEXT NOT NULL,
    turn_id TEXT,
    generation_run_id TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id TEXT,
    operation TEXT NOT NULL,
    resource_type TEXT,
    resource_id TEXT,
    request_id TEXT,
    trace_id TEXT,
    result TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TRIGGER audit_events_no_update
BEFORE UPDATE ON audit_events
BEGIN
    SELECT RAISE(ABORT, 'audit_events are append-only');
END;

CREATE TRIGGER audit_events_no_delete
BEFORE DELETE ON audit_events
BEGIN
    SELECT RAISE(ABORT, 'audit_events are append-only');
END;

CREATE TABLE legacy_import_jobs (
    id TEXT PRIMARY KEY,
    source_fingerprint TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('dry-run', 'commit')),
    stats_json TEXT NOT NULL,
    warnings_json TEXT NOT NULL,
    result TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE legacy_mappings (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES legacy_import_jobs(id) ON DELETE CASCADE,
    source_kind TEXT NOT NULL,
    source_key TEXT NOT NULL,
    target_id TEXT NOT NULL,
    UNIQUE (source_kind, source_key)
);

CREATE TABLE login_rate_limits (
    key TEXT PRIMARY KEY,
    failures INTEGER NOT NULL DEFAULT 0,
    window_start TEXT NOT NULL,
    locked_until TEXT
);

CREATE TABLE tower_sessions (
    id TEXT PRIMARY KEY NOT NULL,
    data BLOB NOT NULL,
    expiry_date INTEGER NOT NULL
);
