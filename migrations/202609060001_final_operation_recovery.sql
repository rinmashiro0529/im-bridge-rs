CREATE TABLE operation_key_refs (
    operation_id TEXT PRIMARY KEY REFERENCES bridge_operations(id) ON DELETE CASCADE,
    wrapped_secret_id TEXT NOT NULL REFERENCES secrets(id),
    key_version INTEGER NOT NULL CHECK (key_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE operation_execution_leases (
    operation_id TEXT PRIMARY KEY REFERENCES bridge_operations(id) ON DELETE CASCADE,
    instance_id TEXT NOT NULL,
    lease_generation INTEGER NOT NULL CHECK (lease_generation > 0),
    lease_until TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX operation_execution_leases_expiry_idx
    ON operation_execution_leases(lease_until, operation_id);
