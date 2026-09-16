CREATE TABLE legacy_source_keys (
    source_kind TEXT NOT NULL,
    source_key TEXT NOT NULL,
    target_id TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    PRIMARY KEY (source_kind, source_key)
);
